//! Optional Prometheus metrics.
//!
//! Instrumentation is wired in unconditionally via the [`metrics`] facade, but a
//! recorder is only installed when `METRICS_ADDR` is set. With no recorder
//! installed every `counter!`/`histogram!`/`gauge!` call is a cheap no-op, so
//! the default (unset) deployment pays effectively nothing.
//!
//! When `METRICS_ADDR` (e.g. `0.0.0.0:9100`) is set, [`init`] installs the
//! Prometheus recorder and spawns its built-in HTTP listener, exposing the usual
//! `/metrics` scrape endpoint. Call it once, after the Tokio runtime is up.
//!
//! Most series are labeled with the bot's `project_id` so per-bot behaviour stays
//! distinguishable in aggregated dashboards.

use std::net::SocketAddr;

use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::PrometheusBuilder;
use tracing::{error, info, warn};

/// Environment variable selecting the Prometheus listener bind address. Unset
/// (or empty) disables metrics entirely.
const METRICS_ADDR_ENV: &str = "METRICS_ADDR";

// Metric names. Counters end in `_total` per Prometheus convention; the latency
// series is a histogram in seconds.
const EVENTS_RECEIVED: &str = "middleman_events_received_total";
const EVENTS_FORWARDED: &str = "middleman_events_forwarded_total";
const EVENTS_DROPPED: &str = "middleman_events_dropped_total";
const FORWARD_FAILURES: &str = "middleman_forward_failures_total";
const FORWARD_RETRIES: &str = "middleman_forward_retries_total";
const FORWARD_LATENCY: &str = "middleman_forward_latency_seconds";
const GATEWAY_RECONNECTS: &str = "middleman_gateway_reconnects_total";
const GATEWAY_RESUMES: &str = "middleman_gateway_resumes_total";
const ACTIVE_BOTS: &str = "middleman_active_bots";

/// Install the Prometheus recorder + HTTP listener if `METRICS_ADDR` is set.
///
/// A missing/empty var is the normal "metrics off" case and is logged at debug
/// level only. A set-but-unusable value (bad address, bind failure) is warned
/// about but never fatal — losing metrics must not take the service down.
pub fn init() {
    let addr = match std::env::var(METRICS_ADDR_ENV) {
        Ok(v) if !v.trim().is_empty() => v.trim().to_string(),
        _ => {
            info!("METRICS_ADDR unset; Prometheus metrics disabled");
            return;
        }
    };

    let socket: SocketAddr = match addr.parse() {
        Ok(socket) => socket,
        Err(err) => {
            warn!(error = %err, %addr, "invalid METRICS_ADDR; metrics disabled");
            return;
        }
    };

    match PrometheusBuilder::new()
        .with_http_listener(socket)
        .install()
    {
        Ok(()) => info!(%socket, "serving Prometheus metrics at /metrics"),
        Err(err) => error!(error = %err, %socket, "failed to start metrics listener; metrics disabled"),
    }
}

/// An event was received from the gateway and queued for delivery.
pub fn event_received(project_id: &str) {
    counter!(EVENTS_RECEIVED, "project_id" => project_id.to_owned()).increment(1);
}

/// An event was successfully delivered downstream.
pub fn event_forwarded(project_id: &str) {
    counter!(EVENTS_FORWARDED, "project_id" => project_id.to_owned()).increment(1);
}

/// An event was dropped because its delivery lane was saturated.
pub fn event_dropped(project_id: &str) {
    counter!(EVENTS_DROPPED, "project_id" => project_id.to_owned()).increment(1);
}

/// A forward attempt ultimately failed (all retries exhausted or permanent).
pub fn forward_failure(project_id: &str) {
    counter!(FORWARD_FAILURES, "project_id" => project_id.to_owned()).increment(1);
}

/// Record `n` retries performed for a single forward (0 if it succeeded first
/// try). A no-op when `n` is 0.
pub fn forward_retries(project_id: &str, n: u64) {
    if n > 0 {
        counter!(FORWARD_RETRIES, "project_id" => project_id.to_owned()).increment(n);
    }
}

/// Observe the wall-clock latency of a completed forward (including retries).
pub fn forward_latency(project_id: &str, secs: f64) {
    histogram!(FORWARD_LATENCY, "project_id" => project_id.to_owned()).record(secs);
}

/// The gateway reconnected after a transient connection error.
pub fn gateway_reconnect(project_id: &str) {
    counter!(GATEWAY_RECONNECTS, "project_id" => project_id.to_owned()).increment(1);
}

/// The gateway resumed an existing session.
pub fn gateway_resume(project_id: &str) {
    counter!(GATEWAY_RESUMES, "project_id" => project_id.to_owned()).increment(1);
}

/// Set the number of bots currently supervised.
pub fn set_active_bots(n: usize) {
    gauge!(ACTIVE_BOTS).set(n as f64);
}
