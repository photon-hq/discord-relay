mod discord;
mod dispatch;
mod logging;
mod metrics;
mod model;
mod spectrum;
mod supervisor;
mod webhook;

use std::time::Duration;

use model::Config;
use reqwest::Client as HttpClient;
use supervisor::Supervisor;
use tokio::signal::unix::{SignalKind, signal};
use tracing::{error, info, warn};

/// Per-request timeout for the shared HTTP client. Both slug resolution and event
/// forwarding want fast failure over a hung request.
const HTTP_TIMEOUT: Duration = Duration::from_secs(15);

#[tokio::main]
async fn main() {
    
    let _ = dotenvy::dotenv();

    logging::init();
    metrics::init();

    let http = match HttpClient::builder().timeout(HTTP_TIMEOUT).build() {
        Ok(http) => http,
        Err(err) => {
            error!(error = %err, "failed to build shared HTTP client");
            std::process::exit(1);
        }
    };

    let Some(config) = load_config() else {
        std::process::exit(1);
    };
    if config.bots.is_empty() {
        warn!("no bots configured; waiting for a SIGHUP reload to add some");
    }

    let mut supervisor = Supervisor::new(http);
    supervisor.reconcile(config).await;

    // Signal-driven control loop:
    //   SIGHUP           -> reload config.json and reconcile the bot set
    //   SIGTERM / SIGINT -> stop all bots and exit
    let mut sighup = install(SignalKind::hangup());
    let mut sigterm = install(SignalKind::terminate());
    let mut sigint = install(SignalKind::interrupt());

    loop {
        tokio::select! {
            _ = sighup.recv() => {
                info!("SIGHUP received; reloading config.json");
                match load_config() {
                    Some(config) => supervisor.reconcile(config).await,
                    None => warn!("config reload failed; keeping the current bot set"),
                }
            }
            _ = sigterm.recv() => {
                info!("SIGTERM received; shutting down");
                break;
            }
            _ = sigint.recv() => {
                info!("SIGINT received; shutting down");
                break;
            }
        }
    }

    supervisor.shutdown().await;
}

/// Read and parse `config.json` from the working directory. Returns `None` on any
/// read/parse error (already logged), leaving the caller to decide whether that's
/// fatal (startup) or recoverable (reload).
fn load_config() -> Option<Config> {
    let contents = match std::fs::read_to_string("config.json") {
        Ok(contents) => contents,
        Err(err) => {
            error!(error = %err, "failed to read config.json");
            return None;
        }
    };

    match serde_json::from_str::<Config>(&contents) {
        Ok(config) => {
            info!(bots = config.bots.len(), "configuration loaded");
            Some(config)
        }
        Err(err) => {
            error!(error = %err, "failed to parse config.json");
            None
        }
    }
}

/// Install a handler for `kind`, aborting startup if the OS refuses (a process
/// that can't hear SIGTERM can't be managed cleanly).
fn install(kind: SignalKind) -> tokio::signal::unix::Signal {
    match signal(kind) {
        Ok(sig) => sig,
        Err(err) => {
            error!(error = %err, "failed to install signal handler");
            std::process::exit(1);
        }
    }
}
