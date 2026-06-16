use serde::{Deserialize, Serialize};
use veil::Redact;

/// Top-level configuration read from `config.json`.
///
/// Holds one [`Client`] per Discord bot the middleman should drive:
/// ```json
/// {
///   "bots": [
///     {
///       "webhookSecret": "userDefinedSecret",
///       "projectId": "projectId",
///       "projectSecret": "projSecret",
///       "botToken": "discordbottoken"
///     }
///   ]
/// }
/// ```
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    /// The bots to run, one supervisor per entry.
    pub bots: Vec<Client>,
}

/// Configuration for a single Discord bot the middleman drives.
///
/// Mirrors one entry of the `bots` array documented in `docs/PAYLOAD.md`:
/// ```json
/// {
///   "webhookSecret": "userDefinedSecret",
///   "projectId": "projectId",
///   "projectSecret": "projSecret",
///   "botToken": "discordbottoken"
/// }
/// ```
/// Secret fields are redacted so they don't leak sensitive data
/// when the `Client` is logged.
#[derive(Clone, Serialize, Deserialize, Redact)]
#[serde(rename_all = "camelCase")]
pub struct Client {
    /// User-defined secret used to authenticate incoming webhook calls.
    #[redact(fixed = 8)]
    pub webhook_secret: String,
    /// Spectrum project identifier.
    pub project_id: String,
    /// Spectrum project secret.
    #[redact(fixed = 8)]
    pub project_secret: String,
    /// Discord bot token used to open the gateway connection.
    #[redact(fixed = 8)]
    pub bot_token: String,
}
