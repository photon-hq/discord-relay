//! Bot lifecycle management and live config reconciliation.
//!
//! [`Supervisor`] owns the set of running bots, keyed by `project_id`, and the
//! shared HTTP connection pool they all forward through. [`Supervisor::reconcile`]
//! diffs a freshly loaded [`Config`] against what's running and applies the
//! minimum change — start new bots, stop removed ones, restart changed ones, and
//! leave unchanged bots (and their live gateway sessions) alone. This is what
//! makes SIGHUP hot-reload non-disruptive.
//!
//! Each bot runs under its own [`CancellationToken`]; stopping a bot cancels its
//! token, which unwinds the gateway supervisor and drains its delivery lanes.

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Duration;

use reqwest::Client as HttpClient;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::discord;
use crate::metrics;
use crate::model::{Client, Config};
use crate::spectrum::{self, SpectrumClient};
use crate::webhook::WebhookClient;

/// Platform label used to address the Fusor super-webhook edge
/// (`https://{slug}.{domain}/{platform}`).
const PLATFORM: &str = "discord";

/// Backoff bounds for retrying control-plane slug resolution at startup. A
/// transient Spectrum hiccup must not permanently kill a bot, so we keep
/// retrying rather than giving up — the gateway is useless without a slug.
const SLUG_BACKOFF_BASE: Duration = Duration::from_secs(1);
const SLUG_BACKOFF_MAX: Duration = Duration::from_secs(60);

/// A running bot: the task driving it, the token that stops it, and a hash of the
/// config it was started with (so a later reconcile can detect changes).
struct BotHandle {
    join: JoinHandle<()>,
    cancel: CancellationToken,
    config_hash: u64,
}

/// Owns and reconciles the set of running bots.
pub struct Supervisor {
    http: HttpClient,
    bots: HashMap<String, BotHandle>,
}

impl Supervisor {
    /// Create an empty supervisor sharing `http` across every bot it spawns.
    pub fn new(http: HttpClient) -> Self {
        Self {
            http,
            bots: HashMap::new(),
        }
    }

    /// Reconcile the running set to match `config`.
    ///
    /// Bots are keyed by `project_id`: a new id is started, a missing id is
    /// stopped, and an id whose credentials changed is restarted. Unchanged bots
    /// are left running untouched so a reload never needlessly re-Identifies them.
    pub async fn reconcile(&mut self, config: Config) {
        // Collapse to one client per project_id (last wins) so duplicates don't
        // fight over the same registry slot.
        let mut desired: HashMap<String, Client> = HashMap::new();
        for client in config.bots {
            if desired.insert(client.project_id.clone(), client).is_some() {
                warn!("duplicate project_id in config; keeping the last entry");
            }
        }

        // Stop bots that vanished or whose credentials changed.
        let to_stop: Vec<String> = self
            .bots
            .iter()
            .filter(|(id, handle)| {
                desired
                    .get(*id)
                    .is_none_or(|client| config_hash(client) != handle.config_hash)
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in to_stop {
            if let Some(handle) = self.bots.remove(&id) {
                info!(project_id = %id, "stopping bot");
                stop(handle).await;
            }
        }

        // Start everything not already running (new, or just stopped to restart).
        for (id, client) in desired {
            if self.bots.contains_key(&id) {
                continue; // unchanged, still running
            }
            info!(project_id = %id, "starting bot");
            let cancel = CancellationToken::new();
            let config_hash = config_hash(&client);
            let join = tokio::spawn(run_bot(self.http.clone(), client, cancel.clone()));
            self.bots.insert(
                id,
                BotHandle {
                    join,
                    cancel,
                    config_hash,
                },
            );
        }

        metrics::set_active_bots(self.bots.len());
        info!(active = self.bots.len(), "reconciled bot set");
    }

    /// Cancel every bot and wait for them to stop. Used on shutdown.
    pub async fn shutdown(&mut self) {
        info!(active = self.bots.len(), "shutting down all bots");
        for (_, handle) in self.bots.drain() {
            stop(handle).await;
        }
        metrics::set_active_bots(0);
    }
}

/// Cancel a bot and await its task, logging a panic but never propagating it.
async fn stop(handle: BotHandle) {
    handle.cancel.cancel();
    if let Err(err) = handle.join.await {
        error!(error = %err, "bot task panicked during shutdown");
    }
}

/// Hash the credential-bearing fields of a [`Client`]. `project_id` is the
/// registry key (not hashed); a change to any secret/token forces a restart.
fn config_hash(client: &Client) -> u64 {
    let mut hasher = DefaultHasher::new();
    client.webhook_secret.hash(&mut hasher);
    client.project_secret.hash(&mut hasher);
    client.bot_token.hash(&mut hasher);
    hasher.finish()
}

/// Drive a single bot end to end: resolve its Spectrum slug (which also
/// authenticates the project credentials), build the downstream forwarder on the
/// shared pool, then hand off to the gateway supervisor — which connects,
/// identifies, and forwards events until `token` is cancelled.
#[tracing::instrument(name = "bot", skip_all, fields(project_id = %client.project_id))]
async fn run_bot(http: HttpClient, client: Client, token: CancellationToken) {
    // Resolve the slug, but bail immediately if the bot is cancelled mid-retry
    // (e.g. removed from config while the control plane is down).
    let slug = tokio::select! {
        biased;
        _ = token.cancelled() => {
            info!("cancelled before slug resolution");
            return;
        }
        slug = resolve_slug(&http, &client) => match slug {
            Some(slug) => slug,
            None => return,
        }
    };
    info!(%slug, "resolved spectrum slug");

    // Build the downstream forwarder on the shared connection pool.
    let webhook =
        match WebhookClient::for_slug_with_client(http, &slug, PLATFORM, &client.webhook_secret) {
            Ok(webhook) => webhook,
            Err(err) => {
                error!(error = %err, %slug, "failed to build downstream webhook client");
                return;
            }
        };

    // Hand off to the gateway supervisor; it runs until `token` is cancelled.
    if let Err(err) = discord::wss::spawn(client, webhook, token).await {
        error!(error = %err, "gateway supervisor task panicked");
    }
}

/// Resolve the Spectrum slug for `client` on the shared `http` pool, retrying
/// transient failures with capped exponential backoff. Returns `None` if the
/// project is misconfigured (a non-success HTTP status), since retrying won't help.
async fn resolve_slug(http: &HttpClient, client: &Client) -> Option<String> {
    let spectrum = SpectrumClient::with_client(
        http.clone(),
        spectrum::spectrum_cloud_url(),
        &client.project_id,
        &client.project_secret,
    );

    let mut backoff = SLUG_BACKOFF_BASE;
    loop {
        match spectrum.get_project_slug().await {
            Ok(slug) => return Some(slug),
            // A non-success status (or a project with no slug) means the request
            // itself is the problem (credentials / unknown / unconfigured
            // project). Retrying is pointless.
            Err(
                err @ (spectrum::SpectrumError::Status { .. } | spectrum::SpectrumError::MissingSlug),
            ) => {
                error!(error = %err, "slug resolution rejected; check project credentials");
                return None;
            }
            // Transport / URL issues may be transient: keep trying so a brief
            // control-plane outage doesn't permanently sideline the bot.
            Err(err) => {
                warn!(error = %err, backoff = ?backoff, "slug resolution failed; retrying after backoff");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(SLUG_BACKOFF_MAX);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client(webhook: &str, secret: &str, token: &str) -> Client {
        Client {
            webhook_secret: webhook.into(),
            project_id: "proj".into(),
            project_secret: secret.into(),
            bot_token: token.into(),
        }
    }

    #[test]
    fn config_hash_ignores_project_id() {
        let mut a = client("w", "s", "t");
        let mut b = client("w", "s", "t");
        a.project_id = "one".into();
        b.project_id = "two".into();
        assert_eq!(config_hash(&a), config_hash(&b));
    }

    #[test]
    fn config_hash_changes_with_credentials() {
        let base = client("w", "s", "t");
        assert_ne!(config_hash(&base), config_hash(&client("w2", "s", "t")));
        assert_ne!(config_hash(&base), config_hash(&client("w", "s2", "t")));
        assert_ne!(config_hash(&base), config_hash(&client("w", "s", "t2")));
    }
}
