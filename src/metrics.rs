//! Optional OpenTelemetry metrics.
//!
//! Instrumentation is wired in unconditionally through this module's thin
//! wrappers, but the instruments are only created when an OTLP endpoint is
//! configured. With metrics disabled (the default) every helper is a cheap early
//! return, so an unconfigured deployment pays effectively nothing.
//!
//! When `OTEL_EXPORTER_OTLP_ENDPOINT` (e.g. `http://localhost:4317`) is set,
//! [`init`] builds an OTLP/gRPC exporter, installs a meter provider with a
//! periodic reader, and creates the instruments. Measurements are then pushed to
//! the configured collector on the reader's interval. Call it once, after the
//! Tokio runtime is up, and call [`shutdown`] on exit to flush anything still
//! buffered.
//!
//! Most series carry the bot's `project_id` attribute so per-bot behaviour stays
//! distinguishable in aggregated dashboards.

use std::sync::OnceLock;

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Gauge, Histogram, MeterProvider};
use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use tracing::{error, info};

/// Environment variable selecting the OTLP collector endpoint. Unset (or empty)
/// disables metrics entirely. Once an endpoint is configured the SDK also honours
/// the other standard `OTEL_*` variables (headers, protocol, timeout, …).
const OTLP_ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";

/// Logical service name reported as a resource attribute on every metric.
const SERVICE_NAME: &str = "discord-relay";

// Instrument names. OpenTelemetry uses dotted namespaces and derives the
// monotonic `_total` suffix at export time, so unlike the old Prometheus names
// these carry no `_total`. The latency series is a histogram in seconds.
const EVENTS_RECEIVED: &str = "relay.events.received";
const EVENTS_FORWARDED: &str = "relay.events.forwarded";
const EVENTS_DROPPED: &str = "relay.events.dropped";
const FORWARD_FAILURES: &str = "relay.forward.failures";
const FORWARD_RETRIES: &str = "relay.forward.retries";
const FORWARD_LATENCY: &str = "relay.forward.latency";
const GATEWAY_RECONNECTS: &str = "relay.gateway.reconnects";
const GATEWAY_RESUMES: &str = "relay.gateway.resumes";
const ACTIVE_BOTS: &str = "relay.active_bots";

/// The instruments plus the provider that owns them. Created once by [`init`] and
/// stored in [`INSTRUMENTS`]; absence is the "metrics off" case. Keeping the
/// provider here both keeps the exporter alive and gives [`shutdown`] something to
/// flush.
struct Metrics {
    events_received: Counter<u64>,
    events_forwarded: Counter<u64>,
    events_dropped: Counter<u64>,
    forward_failures: Counter<u64>,
    forward_retries: Counter<u64>,
    forward_latency: Histogram<f64>,
    gateway_reconnects: Counter<u64>,
    gateway_resumes: Counter<u64>,
    active_bots: Gauge<u64>,
    provider: SdkMeterProvider,
}

static INSTRUMENTS: OnceLock<Metrics> = OnceLock::new();

/// Tag a measurement with the originating bot's project.
fn project(project_id: &str) -> [KeyValue; 1] {
    [KeyValue::new("project_id", project_id.to_owned())]
}

/// Install the OpenTelemetry meter provider + OTLP exporter if an endpoint is set.
///
/// A missing/empty `OTEL_EXPORTER_OTLP_ENDPOINT` is the normal "metrics off" case
/// and is logged at info level only. A set-but-unusable value (exporter build
/// failure) is reported but never fatal — losing metrics must not take the
/// service down.
pub fn init() {
    let endpoint = match std::env::var(OTLP_ENDPOINT_ENV) {
        Ok(v) if !v.trim().is_empty() => v.trim().to_string(),
        _ => {
            info!("OTEL_EXPORTER_OTLP_ENDPOINT unset; OpenTelemetry metrics disabled");
            return;
        }
    };

    let exporter = match MetricExporter::builder()
        .with_tonic()
        .with_endpoint(&endpoint)
        .build()
    {
        Ok(exporter) => exporter,
        Err(err) => {
            error!(error = %err, %endpoint, "failed to build OTLP metric exporter; metrics disabled");
            return;
        }
    };

    let provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter)
        .with_resource(Resource::builder().with_service_name(SERVICE_NAME).build())
        .build();

    // Make the provider the process-wide default so any transitive library
    // instrumentation reports through the same exporter.
    opentelemetry::global::set_meter_provider(provider.clone());

    let meter = provider.meter(SERVICE_NAME);
    let metrics = Metrics {
        events_received: meter.u64_counter(EVENTS_RECEIVED).build(),
        events_forwarded: meter.u64_counter(EVENTS_FORWARDED).build(),
        events_dropped: meter.u64_counter(EVENTS_DROPPED).build(),
        forward_failures: meter.u64_counter(FORWARD_FAILURES).build(),
        forward_retries: meter.u64_counter(FORWARD_RETRIES).build(),
        forward_latency: meter.f64_histogram(FORWARD_LATENCY).with_unit("s").build(),
        gateway_reconnects: meter.u64_counter(GATEWAY_RECONNECTS).build(),
        gateway_resumes: meter.u64_counter(GATEWAY_RESUMES).build(),
        active_bots: meter.u64_gauge(ACTIVE_BOTS).build(),
        provider,
    };

    if INSTRUMENTS.set(metrics).is_err() {
        error!("OpenTelemetry metrics already initialised");
        return;
    }
    info!(%endpoint, "exporting OpenTelemetry metrics via OTLP/gRPC");
}

/// Flush and shut down the exporter, draining any buffered measurements. A no-op
/// when metrics were never enabled. Call once during graceful shutdown.
pub fn shutdown() {
    if let Some(m) = INSTRUMENTS.get()
        && let Err(err) = m.provider.shutdown()
    {
        error!(error = %err, "failed to flush OpenTelemetry metrics on shutdown");
    }
}

/// An event was received from the gateway and queued for delivery.
pub fn event_received(project_id: &str) {
    if let Some(m) = INSTRUMENTS.get() {
        m.events_received.add(1, &project(project_id));
    }
}

/// An event was successfully delivered downstream.
pub fn event_forwarded(project_id: &str) {
    if let Some(m) = INSTRUMENTS.get() {
        m.events_forwarded.add(1, &project(project_id));
    }
}

/// An event was dropped because its delivery lane was saturated.
pub fn event_dropped(project_id: &str) {
    if let Some(m) = INSTRUMENTS.get() {
        m.events_dropped.add(1, &project(project_id));
    }
}

/// A forward attempt ultimately failed (all retries exhausted or permanent).
pub fn forward_failure(project_id: &str) {
    if let Some(m) = INSTRUMENTS.get() {
        m.forward_failures.add(1, &project(project_id));
    }
}

/// Record `n` retries performed for a single forward (0 if it succeeded first
/// try). A no-op when `n` is 0.
pub fn forward_retries(project_id: &str, n: u64) {
    if n == 0 {
        return;
    }
    if let Some(m) = INSTRUMENTS.get() {
        m.forward_retries.add(n, &project(project_id));
    }
}

/// Observe the wall-clock latency of a completed forward (including retries).
pub fn forward_latency(project_id: &str, secs: f64) {
    if let Some(m) = INSTRUMENTS.get() {
        m.forward_latency.record(secs, &project(project_id));
    }
}

/// The gateway reconnected after a transient connection error.
pub fn gateway_reconnect(project_id: &str) {
    if let Some(m) = INSTRUMENTS.get() {
        m.gateway_reconnects.add(1, &project(project_id));
    }
}

/// The gateway resumed an existing session.
pub fn gateway_resume(project_id: &str) {
    if let Some(m) = INSTRUMENTS.get() {
        m.gateway_resumes.add(1, &project(project_id));
    }
}

/// Set the number of bots currently supervised.
pub fn set_active_bots(n: usize) {
    if let Some(m) = INSTRUMENTS.get() {
        m.active_bots.record(n as u64, &[]);
    }
}
