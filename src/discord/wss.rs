//! Discord gateway (WSS) connection management.
//!
//! One Discord bot maps to one [`spawn`] call, which drives a *supervisor*
//! that keeps a gateway connection alive forever — reconnecting and resuming
//! across drops so the middleman has effectively no downtime.
//!
//! ## Tasks, not threads
//!
//! Per bot we run exactly two cheap async tasks (not OS threads):
//!
//! * **gateway task** — connects, runs the heartbeat timer and socket reads in
//!   a single [`tokio::select!`] loop, and reconnects/resumes on failure.
//! * **forwarder task** — drains an [`mpsc`] channel of event payloads and
//!   POSTs them downstream via [`WebhookClient`].
//!
//! Forwarding is decoupled from the read loop on purpose: [`WebhookClient`]
//! retries with backoff and can block for seconds, and we must never let that
//! stall heartbeats (Discord would drop us as a zombie). The gateway task only
//! ever does a non-blocking `try_send` into the channel.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::dispatch::Dispatcher;
use crate::metrics;
use crate::model::Client;
use crate::webhook::WebhookClient;

/// Discord gateway entrypoint. `v=10` + JSON encoding (no compression — keeps
/// the hot path dependency-light; revisit if bandwidth ever matters).
const GATEWAY_URL: &str = "wss://gateway.discord.gg/?v=10&encoding=json";

/// Query string appended to a `resume_gateway_url` when resuming a session.
const GATEWAY_QUERY: &str = "?v=10&encoding=json";

/// Gateway intents we subscribe to. `MESSAGE_CONTENT` is privileged and must be
/// enabled for the application in the Discord developer portal, or Identify is
/// rejected with a `4014` close code.
///
/// * `GUILD_MESSAGES`   `1 << 9`
/// * `DIRECT_MESSAGES`  `1 << 12`
/// * `MESSAGE_CONTENT`  `1 << 15`
const INTENTS: u64 = (1 << 9) | (1 << 12) | (1 << 15);

/// Gateway opcodes. See the Discord gateway documentation.
mod op {
    pub const DISPATCH: u64 = 0;
    pub const HEARTBEAT: u64 = 1;
    pub const IDENTIFY: u64 = 2;
    pub const RESUME: u64 = 6;
    pub const RECONNECT: u64 = 7;
    pub const INVALID_SESSION: u64 = 9;
    pub const HELLO: u64 = 10;
    pub const HEARTBEAT_ACK: u64 = 11;
}

/// Errors are internal to the supervisor; it logs them and reconnects, so a
/// boxed error keeps the gateway code free of bespoke `From` plumbing.
type Error = Box<dyn std::error::Error + Send + Sync>;

/// Spawn a supervised gateway connection for `client`'s bot, forwarding every
/// received event's data to `webhook`.
///
/// Returns immediately; the returned [`JoinHandle`] resolves only if the
/// supervisor itself is dropped (it otherwise loops forever). Call this once
/// per bot — N bots = N independent supervisors on the shared runtime.
///
/// Cancelling `token` stops the supervisor: it breaks out of the reconnect loop,
/// drops the connection, and closes the delivery lanes (which drain and exit).
///
/// [`JoinHandle`]: tokio::task::JoinHandle
pub fn spawn(
    client: Client,
    webhook: WebhookClient,
    token: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(run(client, webhook, token))
}

/// The supervisor loop. Owns the persistent session state and the forwarder
/// task, and reconnects forever.
///
/// All logs emitted by this supervisor and its connections carry the bot's
/// `project_id` via the instrumented span, so concurrent bots stay
/// distinguishable in the aggregated logs.
#[tracing::instrument(name = "bot", skip_all, fields(project_id = %client.project_id))]
async fn run(client: Client, webhook: WebhookClient, token: CancellationToken) {
    info!(target = %webhook.url(), "starting gateway supervisor");

    // The dispatcher (and its delivery lanes) outlives individual connections:
    // gateway drops/resumes do not interrupt in-flight delivery. Dropping it on
    // exit closes the lanes so their workers drain and shut down cleanly.
    let dispatcher = Dispatcher::spawn(webhook, client.project_id.clone());

    let mut session: Option<Session> = None;
    let mut backoff = Backoff::new();

    loop {
        // Race every blocking await against cancellation so a removed bot stops
        // promptly instead of finishing a backoff or a whole connection first.
        let outcome = tokio::select! {
            biased;
            _ = token.cancelled() => break,
            outcome = connect_once(&client, &dispatcher, &mut session, &mut backoff) => outcome,
        };

        match outcome {
            // Clean signal to reconnect with the existing session.
            Ok(Disconnect::Resume) => {
                debug!("connection ended; will resume existing session");
                metrics::gateway_resume(&client.project_id);
                backoff.reset();
            }
            // Session is dead; drop it and Identify fresh after a short wait.
            Ok(Disconnect::Reidentify) => {
                info!("session invalidated; re-identifying with a fresh session");
                metrics::gateway_reconnect(&client.project_id);
                session = None;
                backoff.reset();
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_secs(2)) => {}
                }
            }
            // Transient failure: keep the session so the next attempt resumes,
            // and grow the backoff to avoid hammering the gateway.
            Err(err) => {
                metrics::gateway_reconnect(&client.project_id);
                warn!(error = %err, "gateway connection error; reconnecting after backoff");
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = backoff.sleep() => {}
                }
            }
        }
    }

    info!("gateway supervisor stopped");
}

/// Outcome of a single connection that ended without an error.
enum Disconnect {
    /// Reconnect and resume the existing session (op 7, zombie, socket close).
    Resume,
    /// Session is unrecoverable; reconnect with a fresh Identify (op 9, false).
    Reidentify,
}

/// What to do after handling one decoded frame.
enum Flow {
    /// Keep pumping.
    Continue,
    /// Gateway requested an immediate heartbeat (op 1).
    Beat,
    /// End this connection with the given disposition.
    End(Disconnect),
}

/// Persistent session info needed to resume after a drop.
struct Session {
    /// Opaque session id from the `READY` event.
    id: String,
    /// Host to reconnect to when resuming (from `resume_gateway_url`).
    resume_url: String,
    /// Last sequence number seen, replayed on resume and sent in heartbeats.
    seq: Option<u64>,
}

/// Open one gateway connection and pump it until it ends.
///
/// On a fresh start (`session` is `None`) we connect to [`GATEWAY_URL`] and
/// Identify; otherwise we reconnect to the session's `resume_url` and Resume.
async fn connect_once(
    client: &Client,
    dispatcher: &Dispatcher,
    session: &mut Option<Session>,
    backoff: &mut Backoff,
) -> Result<Disconnect, Error> {
    let resuming = session.is_some();
    let url = match session.as_ref() {
        Some(s) => format!("{}{}", s.resume_url.trim_end_matches('/'), GATEWAY_QUERY),
        None => GATEWAY_URL.to_string(),
    };

    debug!(%url, resuming, "opening gateway connection");
    let (mut ws, _) = connect_async(&url).await?;

    // The gateway opens with Hello (op 10) carrying the heartbeat interval.
    let hello = recv_json(&mut ws).await?;
    let interval_ms = hello["d"]["heartbeat_interval"]
        .as_u64()
        .ok_or("hello missing heartbeat_interval")?;

    // Resume an existing session, or Identify a new one.
    match session.as_ref() {
        Some(s) => {
            ws.send(text(json!({
                "op": op::RESUME,
                "d": { "token": client.bot_token, "session_id": s.id, "seq": s.seq },
            })))
            .await?;
        }
        None => ws.send(text(identify(&client.bot_token))).await?,
    }

    let seq = session.as_ref().and_then(|s| s.seq);
    Connection {
        ws,
        dispatcher,
        session,
        backoff,
        seq,
        awaiting_ack: false,
        interval: Duration::from_millis(interval_ms),
    }
    .pump()
    .await
}

/// Live state for a single connection, pumped by [`Connection::pump`].
struct Connection<'a, S> {
    ws: tokio_tungstenite::WebSocketStream<S>,
    dispatcher: &'a Dispatcher,
    session: &'a mut Option<Session>,
    backoff: &'a mut Backoff,
    seq: Option<u64>,
    awaiting_ack: bool,
    interval: Duration,
}

impl<S> Connection<'_, S>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    /// Run the heartbeat timer and socket reads in a single `select!` loop.
    async fn pump(mut self) -> Result<Disconnect, Error> {
        // First heartbeat is jittered per the gateway spec to avoid a thundering
        // herd of bots heartbeating in lockstep after a mass reconnect.
        tokio::time::sleep(jitter(self.interval)).await;
        let mut heartbeat = tokio::time::interval(self.interval);

        loop {
            tokio::select! {
                _ = heartbeat.tick() => {
                    // No ACK since the last beat ⇒ the connection is a zombie.
                    if self.awaiting_ack {
                        warn!("heartbeat not acked; treating connection as a zombie and reconnecting");
                        return Ok(Disconnect::Resume);
                    }
                    self.send_heartbeat().await?;
                }
                msg = self.ws.next() => {
                    let Some(msg) = msg else {
                        return Ok(Disconnect::Resume); // stream ended
                    };
                    match msg? {
                        Message::Text(t) => match self.handle(serde_json::from_str(t.as_str())?)? {
                            Flow::Continue => {}
                            Flow::Beat => self.send_heartbeat().await?,
                            Flow::End(d) => return Ok(d),
                        },
                        Message::Close(_) => return Ok(Disconnect::Resume),
                        // Ping/Pong are handled by tungstenite; binary is unused.
                        _ => {}
                    }
                }
            }
        }
    }

    /// Handle one decoded gateway frame.
    fn handle(&mut self, v: Value) -> Result<Flow, Error> {
        match v["op"].as_u64() {
            Some(op::DISPATCH) => {
                if let Some(s) = v["s"].as_u64() {
                    self.seq = Some(s);
                    if let Some(sess) = self.session.as_mut() {
                        sess.seq = Some(s);
                    }
                }
                match v["t"].as_str() {
                    Some("READY") => {
                        let id = v["d"]["session_id"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string();
                        info!(session_id = %id, "gateway session established (READY)");
                        *self.session = Some(Session {
                            id,
                            resume_url: v["d"]["resume_gateway_url"]
                                .as_str()
                                .unwrap_or_default()
                                .to_string(),
                            seq: self.seq,
                        });
                        self.backoff.reset();
                    }
                    Some("RESUMED") => {
                        info!("gateway session resumed");
                        self.backoff.reset();
                    }
                    _ => {}
                }
                self.dispatch(v);
                Ok(Flow::Continue)
            }
            Some(op::HEARTBEAT) => Ok(Flow::Beat),
            Some(op::RECONNECT) => {
                debug!("gateway requested reconnect (op 7)");
                Ok(Flow::End(Disconnect::Resume))
            }
            Some(op::INVALID_SESSION) => {
                let resumable = v["d"].as_bool().unwrap_or(false);
                warn!(resumable, "gateway reported invalid session (op 9)");
                Ok(Flow::End(if resumable {
                    Disconnect::Resume
                } else {
                    Disconnect::Reidentify
                }))
            }
            Some(op::HEARTBEAT_ACK) => {
                self.awaiting_ack = false;
                Ok(Flow::Continue)
            }
            Some(op::HELLO) => Ok(Flow::Continue), // already consumed before the loop
            _ => Ok(Flow::Continue),
        }
    }

    /// Forward an event's data downstream — only the `d` payload as the body,
    /// never the op/seq envelope. The event type (`t`) is carried alongside so
    /// the forwarder can tag it on the request. Hands off to the [`Dispatcher`],
    /// which routes by channel and never blocks the gateway read loop.
    fn dispatch(&self, v: Value) {
        let Value::Object(mut obj) = v else { return };
        let name = obj
            .get("t")
            .and_then(Value::as_str)
            .unwrap_or("UNKNOWN")
            .to_string();
        let Some(data) = obj.remove("d") else { return };
        if data.is_null() {
            return;
        }
        self.dispatcher.enqueue(name, data);
    }

    /// Send a heartbeat (op 1) carrying the last sequence number and mark the
    /// connection as awaiting its ACK for zombie detection.
    async fn send_heartbeat(&mut self) -> Result<(), Error> {
        self.ws
            .send(text(json!({ "op": op::HEARTBEAT, "d": self.seq })))
            .await?;
        self.awaiting_ack = true;
        Ok(())
    }
}

/// Read frames until a JSON text frame arrives, decoding it. A close/EOF before
/// any text frame is an error (the caller will reconnect).
async fn recv_json<S>(ws: &mut tokio_tungstenite::WebSocketStream<S>) -> Result<Value, Error>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        match ws.next().await {
            Some(Ok(Message::Text(t))) => return Ok(serde_json::from_str(t.as_str())?),
            Some(Ok(Message::Close(_))) | None => return Err("connection closed".into()),
            Some(Ok(_)) => continue,
            Some(Err(e)) => return Err(e.into()),
        }
    }
}

/// Build the Identify (op 2) payload for `token`.
fn identify(token: &str) -> Value {
    json!({
        "op": op::IDENTIFY,
        "d": {
            "token": token,
            "intents": INTENTS,
            "properties": {
                "os": std::env::consts::OS,
                "browser": "discord-middleman",
                "device": "discord-middleman",
            },
        },
    })
}

/// Wrap a JSON value in a websocket text frame.
fn text(v: Value) -> Message {
    Message::text(v.to_string())
}

/// A pseudo-random fraction of `interval`, used to jitter the first heartbeat.
/// Derived from the wall clock to avoid pulling in an RNG crate; uniformity
/// across bots is good enough for spreading the herd.
fn jitter(interval: Duration) -> Duration {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let frac = (nanos % 1_000_000) as f64 / 1_000_000.0;
    interval.mul_f64(frac)
}

/// Exponential reconnect backoff, capped, reset on a healthy connection.
struct Backoff {
    current: Duration,
}

impl Backoff {
    const BASE: Duration = Duration::from_secs(1);
    const MAX: Duration = Duration::from_secs(60);

    fn new() -> Self {
        Self {
            current: Self::BASE,
        }
    }

    fn reset(&mut self) {
        self.current = Self::BASE;
    }

    async fn sleep(&mut self) {
        let delay = self.current;
        self.current = (self.current * 2).min(Self::MAX);
        tokio::time::sleep(delay).await;
    }
}
