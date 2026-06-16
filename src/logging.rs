//! Process-wide logging/telemetry setup built on [`tracing`].
//!
//! Production deployments get structured, leveled logs that are controlled
//! entirely by the environment, so operators can dial verbosity or switch to
//! machine-readable output without a code change or redeploy:
//!
//! * `RUST_LOG` — standard [`EnvFilter`] syntax, e.g. `info` or
//!   `discord_relay=debug,reqwest=warn`. Falls back to [`DEFAULT_FILTER`]
//!   when unset or empty.
//! * `LOG_FORMAT` — `json` emits one JSON object per line (ship these straight
//!   to a log aggregator); anything else (or unset) emits human-readable lines,
//!   with ANSI colour only when stdout is a TTY.
//!
//! Call [`init`] exactly once, as early in `main` as possible. It is safe to
//! call more than once — subsequent calls are quietly ignored, which keeps
//! tests that each initialize logging from panicking.

use std::io::IsTerminal;

use tracing_subscriber::{EnvFilter, fmt, prelude::*};

/// Filter applied when `RUST_LOG` is unset or empty. `info` everywhere keeps
/// the signal-to-noise ratio sane out of the box; bump it per-target via
/// `RUST_LOG` when debugging (e.g. `discord_relay=debug`).
const DEFAULT_FILTER: &str = "info";

/// Environment variable selecting the output encoding (`json` vs human).
const LOG_FORMAT_ENV: &str = "LOG_FORMAT";

/// Install the global tracing subscriber, configured from the environment.
///
/// See the [module docs](self) for the `RUST_LOG` / `LOG_FORMAT` knobs.
pub fn init() {
    let filter = match std::env::var(EnvFilter::DEFAULT_ENV) {
        // `parse_lossy` keeps us alive on a typo'd directive instead of
        // silently dropping every log: bad directives are skipped, the rest
        // still apply.
        Ok(v) if !v.trim().is_empty() => EnvFilter::builder().parse_lossy(v),
        _ => EnvFilter::new(DEFAULT_FILTER),
    };

    let json = std::env::var(LOG_FORMAT_ENV)
        .map(|v| v.eq_ignore_ascii_case("json"))
        .unwrap_or(false);

    let registry = tracing_subscriber::registry().with(filter);

    // `try_init` (rather than `init`) so a second call — e.g. from a test — is a
    // no-op error we can ignore instead of a panic.
    if json {
        registry
            .with(
                fmt::layer()
                    .json()
                    .with_current_span(true)
                    .with_span_list(true),
            )
            .try_init()
            .ok();
    } else {
        let ansi = std::io::stdout().is_terminal();
        registry
            .with(fmt::layer().with_ansi(ansi).with_target(true))
            .try_init()
            .ok();
    }
}
