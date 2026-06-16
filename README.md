# discord-relay

A small, always-on bridge that streams **Discord gateway events** into **Fusor**
(and on to **Spectrum**). It opens a WebSocket to Discord for each configured
bot, keeps that connection alive forever (heartbeats, resume, reconnect-with-
backoff), and forwards every dispatched event downstream over HTTP.

```
                 wss (gateway)             https (super-webhook)
  Discord  ───────────────────▶  relay     ───────────────────▶  Fusor  ──▶  Spectrum
                                     │
                                     ├─ resolves each project's slug via the
                                     │  Spectrum control plane (HTTP GET)
                                     └─ forwards the event `d` payload (type in
                                        an X-Discord-Event header), signed with
                                        the per-bot webhook secret
```

## Why it exists

Discord only delivers gateway events over a persistent WebSocket — there's no
"push me your events at this URL" option. Fusor/Spectrum want those events as
plain webhook POSTs. This service is the glue: it holds the WSS connection open
and turns each event into a downstream HTTP request, with effectively no
downtime.

## How it works

For every bot in `config.json`, the relay:

1. **Resolves the Spectrum slug.** It GETs the project from the Spectrum control
   plane (`{SPECTRUM_CLOUD_URL}/projects/{projectId}/`) using the project's
   `projectId` / `projectSecret` as HTTP Basic auth, and reads `data.slug` from
   the response. This is a pure lookup — nothing is modified. See
   [`src/spectrum.rs`](src/spectrum.rs).
2. **Connects to the Discord gateway** (`wss://gateway.discord.gg/?v=10&encoding=json`)
   and Identifies with the bot token. A single supervised task runs the
   heartbeat timer and socket reads in one `select!` loop, resuming across drops
   and reconnecting with exponential backoff. See [`src/discord/wss.rs`](src/discord/wss.rs).
3. **Forwards events downstream.** Each gateway *dispatch* event's `d` payload
   (never the op/seq envelope) is POSTed to the Fusor super-webhook edge at
   `https://{slug}.{domain}/discord`, with the per-bot secret in the
   `X-Webhook-Secret` header and the event type (the envelope's `t`, e.g.
   `MESSAGE_CREATE`) in the `X-Discord-Event` header — the body stays the raw
   `d` payload, but the type rides alongside so the downstream can tell
   otherwise near-identical create/update/delete payloads apart. Delivery runs on separate tasks with bounded
   buffering and retry/backoff, so a slow downstream can never stall Discord
   heartbeats (which would get the bot dropped as a zombie). See
   [`src/dispatch.rs`](src/dispatch.rs) and [`src/webhook.rs`](src/webhook.rs).

Each bot is fully independent where N bots means N supervisors on the shared Tokio
runtime, all sharing a single HTTP connection pool. Every log line is tagged with
the bot's `projectId` so concurrent bots stay distinguishable.

### Delivery: per-channel ordering, cross-channel concurrency

Within a bot, events are delivered over `FORWARD_SHARDS` concurrent **lanes**.
Each event is routed to a lane by hashing its `channel_id` (falling back to
`guild_id`, then a constant for keyless events), so events for the same channel
always take the same lane and arrive **in order**, while different channels are
delivered in parallel. This lifts the old one-at-a-time throughput ceiling
without reordering any single channel's messages. Note ordering is *per channel*,
not global across the bot. If a lane's bounded buffer fills (downstream wedged),
events are dropped rather than stalling heartbeats — drops are counted (see
metrics) instead of being silent.

### Live config reload

Sending **SIGHUP** re-reads `config.json` and reconciles the running bots:
new `projectId`s are started, removed ones are stopped, and bots whose
credentials changed are restarted — unchanged bots keep running (and keep their
gateway session) untouched, so a reload never needlessly re-Identifies them. A
malformed config on reload is logged and ignored, leaving the current set
running. **SIGTERM**/**SIGINT** stop all bots gracefully and exit.

### Required Discord intents

The bot subscribes to `GUILD_MESSAGES`, `DIRECT_MESSAGES`, and the privileged
`MESSAGE_CONTENT` intent. **`MESSAGE_CONTENT` must be enabled** for the
application in the Discord developer portal, or Identify is rejected with close
code `4014`.

## Configuration

The service reads `config.json` from the working directory at startup:

```json
{
  "bots": [
    {
      "webhookSecret": "userDefinedSecret",
      "projectId": "projectId",
      "projectSecret": "projSecret",
      "botToken": "discordbottoken"
    }
  ]
}
```

| Field           | Description                                                                 |
| --------------- | --------------------------------------------------------------------------- |
| `webhookSecret` | User-defined secret sent as `X-Webhook-Secret` on every forwarded request.  |
| `projectId`     | Spectrum project ID (also the Basic-auth username for slug resolution).     |
| `projectSecret` | Spectrum project secret (Basic-auth password).                              |
| `botToken`      | Discord bot token used to open the gateway connection.                      |

All secret fields are redacted in logs.

### Environment variables

| Variable                 | Default                         | Purpose                                                                  |
| ------------------------ | ------------------------------- | ------------------------------------------------------------------------ |
| `SPECTRUM_CLOUD_URL`     | `https://spectrum.photon.codes` | Base URL of the Spectrum control plane (slug resolution).                |
| `SPECTRUM_SUPER_WEBHOOK` | `spctrm.dev`                    | Base domain of the Fusor super-webhook edge; URLs are `{slug}.{domain}`. |
| `RUST_LOG`               | `info`                          | Log filter, e.g. `discord_relay=debug,reqwest=warn`.                 |
| `LOG_FORMAT`             | human-readable                  | Set to `json` for one JSON object per line (ship to a log aggregator).   |
| `FORWARD_SHARDS`         | `8`                             | Concurrent delivery lanes per bot (per-channel keyed). Floored at 1.     |
| `FORWARD_BUFFER`         | `256`                           | Buffered events per lane before new events are dropped. Floored at 1.    |
| `METRICS_ADDR`           | unset                           | If set (e.g. `0.0.0.0:9100`), serve Prometheus metrics at `/metrics`.    |

### Metrics

Metrics are off by default. Set `METRICS_ADDR` to a bind address to expose a
Prometheus `/metrics` endpoint. Series (most labeled by `project_id`):

| Metric                                | Type      | Meaning                                          |
| ------------------------------------- | --------- | ------------------------------------------------ |
| `relay_events_received_total`         | counter   | Dispatch events queued for delivery.             |
| `relay_events_forwarded_total`        | counter   | Events delivered downstream successfully.        |
| `relay_events_dropped_total`          | counter   | Events dropped due to a saturated lane.          |
| `relay_forward_failures_total`        | counter   | Forwards that failed after exhausting retries.   |
| `relay_forward_retries_total`         | counter   | Retry attempts across all forwards.              |
| `relay_forward_latency_seconds`       | histogram | Per-forward latency (including retries).         |
| `relay_gateway_reconnects_total`      | counter   | Fresh reconnects / re-identifies.                |
| `relay_gateway_resumes_total`         | counter   | Session resumes after a drop.                    |
| `relay_active_bots`                   | gauge     | Bots currently supervised.                       |

## Running

Requires a recent Rust toolchain (using edition 2024).

```sh
# build
cargo build --release

# run (expects ./config.json in the working directory)
cargo run --release
```

Run the tests with:

```sh
cargo test
```

## Project layout

| Path                 | Responsibility                                                         |
| -------------------- | ---------------------------------------------------------------------- |
| `src/main.rs`        | Entry point: init, build shared HTTP client, signal/reload loop.       |
| `src/supervisor.rs`  | Bot lifecycle: drive each bot, diff config on reload, graceful stop.   |
| `src/model.rs`       | `Config` / `Client` types.                                             |
| `src/discord/wss.rs` | Discord gateway supervisor: connect, heartbeat, resume, dispatch.      |
| `src/dispatch.rs`    | Per-channel sharded delivery lanes (ordering + concurrency).           |
| `src/spectrum.rs`    | Spectrum control-plane client (project slug resolution).               |
| `src/webhook.rs`     | Downstream forwarder: signs and POSTs events, retries with backoff.    |
| `src/metrics.rs`     | Optional Prometheus metrics (enabled via `METRICS_ADDR`).              |
| `src/logging.rs`     | `tracing` setup driven by `RUST_LOG` / `LOG_FORMAT`.                   |
| `docs/PAYLOAD.md`    | Config payload example and design notes.                               |
