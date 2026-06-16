//! Per-channel concurrent delivery of gateway events.
//!
//! The gateway read loop must never block on downstream delivery (a slow
//! downstream would stall heartbeats and get the bot dropped as a zombie), so
//! forwarding runs on separate tasks fed through bounded channels.
//!
//! ## Sharded, keyed lanes
//!
//! A single serial forwarder caps a busy bot at `1 / request-latency` events per
//! second. Instead we run `FORWARD_SHARDS` worker tasks ("lanes"), each draining
//! its own bounded channel and forwarding strictly in order. Events are routed to
//! a lane by hashing a **key** derived from the payload (`channel_id`, falling
//! back to `guild_id`, falling back to a constant for keyless events):
//!
//! * same key ⇒ same lane ⇒ delivered **in order** (per-channel ordering), and
//! * different keys spread across lanes ⇒ up to `FORWARD_SHARDS` concurrent
//!   in-flight POSTs (throughput).
//!
//! Note this preserves order *per channel*, not globally across a bot: events in
//! different channels may interleave. Since every event for a bot targets the
//! same downstream URL, concurrency only pipelines that URL — per-channel order
//! is the meaningful guarantee.
//!
//! When the [`Dispatcher`] is dropped its lane senders close, and each worker
//! drains its remaining queue and exits — giving a clean shutdown on bot removal.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Instant;

use serde_json::Value;
use tokio::sync::mpsc;
use tracing::{Instrument, Span, debug, error, warn};

use crate::metrics;
use crate::webhook::WebhookClient;

/// Number of concurrent delivery lanes per bot when `FORWARD_SHARDS` is unset.
const DEFAULT_SHARDS: usize = 8;

/// Per-lane channel capacity when `FORWARD_BUFFER` is unset.
const DEFAULT_SHARD_BUFFER: usize = 256;

/// Environment variable overriding the number of delivery lanes per bot.
const SHARDS_ENV: &str = "FORWARD_SHARDS";

/// Environment variable overriding each lane's channel capacity.
const BUFFER_ENV: &str = "FORWARD_BUFFER";

/// One dispatch event queued for downstream delivery: the gateway event name
/// (the envelope's `t`, e.g. `MESSAGE_CREATE`) plus the `d` payload. The `d`
/// payload is the request body; the name is forwarded in the `X-Discord-Event`
/// header (and tags the delivery logs).
struct ForwardEvent {
    name: String,
    data: Value,
}

/// Routes events to a fixed set of ordered delivery lanes. Cloneable handle is
/// not needed — the gateway holds it by reference and drops it to shut lanes down.
pub struct Dispatcher {
    lanes: Vec<mpsc::Sender<ForwardEvent>>,
    project_id: String,
}

impl Dispatcher {
    /// Spawn `FORWARD_SHARDS` delivery workers, each sharing `webhook`'s
    /// connection pool, and return a handle that routes events to them.
    ///
    /// Workers inherit the caller's tracing span so their logs stay tagged with
    /// the bot's `project_id`.
    pub fn spawn(webhook: WebhookClient, project_id: String) -> Self {
        let shards = shard_count();
        let buffer = shard_buffer();
        let mut lanes = Vec::with_capacity(shards);
        for lane in 0..shards {
            let (tx, rx) = mpsc::channel::<ForwardEvent>(buffer);
            tokio::spawn(
                worker(lane, rx, webhook.clone(), project_id.clone()).instrument(Span::current()),
            );
            lanes.push(tx);
        }
        debug!(shards, buffer, "spawned delivery lanes");
        Self { lanes, project_id }
    }

    /// Queue `data` (the event's `d` payload) for delivery, ordered per channel.
    ///
    /// Non-blocking: if the target lane is saturated the event is dropped rather
    /// than stalling the gateway read loop, and the drop is counted so saturation
    /// is observable instead of silent.
    pub fn enqueue(&self, name: String, data: Value) {
        metrics::event_received(&self.project_id);
        let lane = shard_for(&data, self.lanes.len());
        if self.lanes[lane]
            .try_send(ForwardEvent { name, data })
            .is_err()
        {
            metrics::event_dropped(&self.project_id);
            warn!(lane, "delivery lane saturated; dropping event");
        }
    }
}

/// Drain one lane, forwarding each event downstream in order.
async fn worker(
    lane: usize,
    mut rx: mpsc::Receiver<ForwardEvent>,
    webhook: WebhookClient,
    project_id: String,
) {
    while let Some(ForwardEvent { name, data }) = rx.recv().await {
        debug!(lane, event = %name, "forwarding event downstream");
        let start = Instant::now();
        let (attempts, result) = webhook.forward(&name, &data).await;
        metrics::forward_latency(&project_id, start.elapsed().as_secs_f64());
        // Record retries on both outcomes: a failed forward performs the *most*
        // retries, so only counting them on success inverts the metric.
        metrics::forward_retries(&project_id, u64::from(attempts.saturating_sub(1)));
        match result {
            Ok(()) => {
                metrics::event_forwarded(&project_id);
                debug!(lane, event = %name, attempts, "event forwarded downstream");
            }
            Err(err) => {
                metrics::forward_failure(&project_id);
                error!(lane, event = %name, attempts, error = %err, "forwarding event downstream failed");
            }
        }
    }
    debug!(lane, "delivery lane closed; worker shutting down");
}

/// Pick the lane for `data` by hashing its routing key. Keyless events all map
/// to lane 0 so they stay mutually ordered.
fn shard_for(data: &Value, lanes: usize) -> usize {
    match routing_key(data) {
        Some(key) => (hash(key) % lanes as u64) as usize,
        None => 0,
    }
}

/// The value events are ordered by: a channel where present, else the guild.
/// Both are Discord snowflake strings.
fn routing_key(data: &Value) -> Option<&str> {
    data.get("channel_id")
        .and_then(Value::as_str)
        .or_else(|| data.get("guild_id").and_then(Value::as_str))
}

/// Deterministic hash of a routing key. [`DefaultHasher`] uses fixed keys, so
/// the same string always maps to the same lane within and across runs.
fn hash(key: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

/// Number of delivery lanes, from `FORWARD_SHARDS` or [`DEFAULT_SHARDS`], floored
/// at 1 so there is always somewhere to route.
fn shard_count() -> usize {
    env_usize(SHARDS_ENV).unwrap_or(DEFAULT_SHARDS).max(1)
}

/// Per-lane buffer, from `FORWARD_BUFFER` or [`DEFAULT_SHARD_BUFFER`], floored at 1.
fn shard_buffer() -> usize {
    env_usize(BUFFER_ENV).unwrap_or(DEFAULT_SHARD_BUFFER).max(1)
}

/// Parse a positive `usize` from environment variable `name`, ignoring unset or
/// malformed values (the default applies instead).
fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok()?.trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn routes_by_channel_then_guild() {
        let by_channel = json!({ "channel_id": "111", "guild_id": "999" });
        assert_eq!(routing_key(&by_channel), Some("111"));

        let by_guild = json!({ "guild_id": "999" });
        assert_eq!(routing_key(&by_guild), Some("999"));

        let keyless = json!({ "user": { "id": "5" } });
        assert_eq!(routing_key(&keyless), None);
    }

    #[test]
    fn same_channel_maps_to_same_lane() {
        let a = json!({ "channel_id": "12345", "content": "hi" });
        let b = json!({ "channel_id": "12345", "content": "there" });
        assert_eq!(shard_for(&a, 8), shard_for(&b, 8));
    }

    #[test]
    fn hash_is_deterministic() {
        assert_eq!(hash("12345"), hash("12345"));
    }

    #[test]
    fn keyless_events_share_lane_zero() {
        let keyless = json!({ "foo": "bar" });
        assert_eq!(shard_for(&keyless, 8), 0);
    }

    #[test]
    fn lane_index_is_in_range() {
        for id in 0..1000u64 {
            let v = json!({ "channel_id": id.to_string() });
            assert!(shard_for(&v, 8) < 8);
        }
    }
}
