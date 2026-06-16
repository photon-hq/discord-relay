//! Forwarding of Discord gateway events to a downstream fusor url.
//!
//! The relay receives a [`crate::model::Client`] describing where events
//! should be delivered (e.g. `https://<slug>.spectrm.dev/discord`) along with
//! the user-defined `webhookSecret`. [`WebhookClient`] wraps a pooled HTTP
//! client and posts JSON payloads to that endpoint, attaching the secret on
//! every request and retrying transient failures with exponential backoff.

use std::time::Duration;

use reqwest::{Client, StatusCode, Url};
use serde::Serialize;
use veil::Redact;

/// HTTP header used to authenticate forwarded requests against the downstream
/// endpoint. The downstream service is expected to compare this against the
/// `webhookSecret` it was configured with.
const WEBHOOK_SECRET_HEADER: &str = "X-Webhook-Secret";

/// HTTP header carrying the Discord gateway event name (the envelope's `t`,
/// e.g. `MESSAGE_CREATE`). The body is the full `{ op, t, s, d }` gateway frame,
/// so the type is also available there; this header lets downstream clients
/// route on the event type without parsing the body.
const DISCORD_EVENT_HEADER: &str = "X-Discord-Event";

/// Default per-request timeout. Forwarding should be fast; a hung downstream
/// must not block the gateway pump indefinitely.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(15);

/// Default number of attempts (1 initial try + retries) for a single forward.
const DEFAULT_MAX_ATTEMPTS: u32 = 4;

/// Base delay for exponential backoff between retries.
const DEFAULT_BACKOFF_BASE: Duration = Duration::from_millis(250);

/// Upper bound on a single inter-retry sleep, including server-requested
/// `Retry-After` waits. Caps a pathological header or a large tuned backoff so a
/// delivery lane can't stall indefinitely (and the delay can't overflow).
const MAX_RETRY_DELAY: Duration = Duration::from_secs(60);

/// Base domain of the Fusor "super webhook" edge. Events are delivered to
/// `https://{slug}.{domain}/{platform}`, where Fusor forwards them on to
/// Spectrum. This is the only baked-in default — override per-environment via
/// the [`SUPER_WEBHOOK_DOMAIN_ENV`] env var or an explicit `domain` argument.
const DEFAULT_SUPER_WEBHOOK_DOMAIN: &str = "spctrm.dev";

/// Environment variable that overrides [`DEFAULT_SUPER_WEBHOOK_DOMAIN`].
const SUPER_WEBHOOK_DOMAIN_ENV: &str = "SPECTRUM_SUPER_WEBHOOK";

/// URL scheme used when assembling super-webhook URLs from parts.
const DEFAULT_SCHEME: &str = "https";

/// Resolve the configured super-webhook base domain.
///
/// Returns the value of `SPECTRUM_SUPER_WEBHOOK` if it is set and non-empty,
/// otherwise [`DEFAULT_SUPER_WEBHOOK_DOMAIN`]. Surrounding whitespace and a
/// trailing slash are trimmed so the env value is forgiving to set.
pub fn super_webhook_domain() -> String {
    match std::env::var(SUPER_WEBHOOK_DOMAIN_ENV) {
        Ok(v) if !v.trim().is_empty() => v.trim().trim_end_matches('/').to_string(),
        _ => DEFAULT_SUPER_WEBHOOK_DOMAIN.to_string(),
    }
}

/// Assemble a delivery URL for `slug` + `platform` against `domain`, i.e.
/// `https://{slug}.{domain}/{platform}`. Each part is trimmed of surrounding
/// whitespace and stray slashes so callers don't have to normalize them.
pub fn super_webhook_url(slug: &str, platform: &str, domain: &str) -> String {
    let slug = slug.trim().trim_matches('/');
    let domain = domain.trim().trim_matches('/');
    let platform = platform.trim().trim_matches('/');
    format!("{DEFAULT_SCHEME}://{slug}.{domain}/{platform}")
}

/// Errors that can occur while forwarding an event.
#[derive(Debug)]
pub enum WebhookError {
    /// The supplied target URL could not be parsed.
    InvalidUrl(url_error::Error),
    /// The request could not be built, sent, or timed out at the transport
    /// layer (DNS, TLS, connection, read timeout, ...).
    Transport(reqwest::Error),
    /// The downstream returned a non-success status. Carries the status, a
    /// best-effort snippet of the response body for diagnostics, and any
    /// server-requested `Retry-After` delay (present on `429`).
    Status {
        status: StatusCode,
        body: String,
        retry_after: Option<Duration>,
    },
}

impl std::fmt::Display for WebhookError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WebhookError::InvalidUrl(e) => write!(f, "invalid webhook url: {e}"),
            WebhookError::Transport(e) => write!(f, "transport error: {e}"),
            WebhookError::Status { status, body, .. } => {
                write!(f, "downstream returned {status}: {body}")
            }
        }
    }
}

impl std::error::Error for WebhookError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WebhookError::InvalidUrl(e) => Some(e),
            WebhookError::Transport(e) => Some(e),
            WebhookError::Status { .. } => None,
        }
    }
}

/// Local alias so callers don't need a separate `url` dependency in scope.
mod url_error {
    pub use reqwest::Url as _Url;
    /// URL parse error surfaced by `reqwest::Url`.
    pub type Error = <_Url as std::str::FromStr>::Err;
}

/// A client that forwards JSON payloads to a single downstream webhook URL.
///
/// Cheap to [`clone`](Clone) — the underlying connection pool is shared, so a
/// single instance can be handed to many concurrent tasks.
#[derive(Redact, Clone)]
pub struct WebhookClient {
    http: Client,
    url: Url,
    #[redact(fixed = 8)]
    secret: String,
    max_attempts: u32,
    backoff_base: Duration,
}

impl WebhookClient {
    /// Create a client targeting `url` (e.g. `https://<slug>.spectrm.dev/discord`),
    /// authenticating with `secret`, building its own connection pool.
    ///
    /// Bots run through [`for_slug_with_client`](Self::for_slug_with_client) so
    /// they share one pool; this self-contained variant is kept for tests and
    /// standalone use. Returns [`WebhookError::InvalidUrl`] if `url` is invalid.
    #[allow(dead_code)]
    pub fn new(url: impl AsRef<str>, secret: impl Into<String>) -> Result<Self, WebhookError> {
        let http = Client::builder()
            .timeout(DEFAULT_TIMEOUT)
            .build()
            .map_err(WebhookError::Transport)?;
        Self::with_client(http, url, secret)
    }

    /// Like [`for_slug_with_client`](Self::for_slug_with_client) but with an
    /// explicit base `domain` and its own connection pool, bypassing the
    /// environment lookup. Kept for tests and standalone use.
    #[allow(dead_code)]
    pub fn for_slug_with_domain(
        slug: &str,
        platform: &str,
        domain: &str,
        secret: impl Into<String>,
    ) -> Result<Self, WebhookError> {
        Self::new(super_webhook_url(slug, platform, domain), secret)
    }

    /// Target the Fusor super-webhook edge for a `slug`/`platform`, reusing an
    /// existing [`reqwest::Client`] so every bot shares a single connection pool
    /// instead of each building its own. The base domain is resolved from the
    /// environment (`SPECTRUM_SUPER_WEBHOOK`, falling back to the default).
    ///
    /// e.g. `for_slug_with_client(http, "my-slug", "discord", secret)` targets
    /// `https://my-slug.spctrm.dev/discord`.
    pub fn for_slug_with_client(
        http: Client,
        slug: &str,
        platform: &str,
        secret: impl Into<String>,
    ) -> Result<Self, WebhookError> {
        Self::with_client(
            http,
            super_webhook_url(slug, platform, &super_webhook_domain()),
            secret,
        )
    }

    /// Like [`new`](Self::new) but reuses an existing [`reqwest::Client`], which
    /// is useful when many endpoints share one connection pool.
    pub fn with_client(
        http: Client,
        url: impl AsRef<str>,
        secret: impl Into<String>,
    ) -> Result<Self, WebhookError> {
        let url = url
            .as_ref()
            .parse::<Url>()
            .map_err(WebhookError::InvalidUrl)?;
        Ok(Self {
            http,
            url,
            secret: secret.into(),
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            backoff_base: DEFAULT_BACKOFF_BASE,
        })
    }

    /// Override the maximum number of attempts per forward (must be >= 1).
    ///
    /// Reserved for per-bot retry tuning; bots currently use the defaults.
    #[allow(dead_code)]
    pub fn with_max_attempts(mut self, attempts: u32) -> Self {
        self.max_attempts = attempts.max(1);
        self
    }

    /// Override the base backoff delay between retries.
    ///
    /// Reserved for per-bot retry tuning; bots currently use the defaults.
    #[allow(dead_code)]
    pub fn with_backoff_base(mut self, base: Duration) -> Self {
        self.backoff_base = base;
        self
    }

    /// The target URL events are forwarded to.
    pub fn url(&self) -> &Url {
        &self.url
    }

    /// Forward `payload` as a JSON `POST` to the configured URL, tagging it with
    /// the gateway `event` name via the [`DISCORD_EVENT_HEADER`].
    ///
    /// Retries on transport errors and `5xx`/`429` responses using exponential
    /// backoff. `4xx` responses (other than `429`) are treated as permanent and
    /// returned immediately, since retrying a malformed/unauthorized request
    /// won't help.
    ///
    /// Always returns the number of attempts made (1 means it succeeded — or
    /// failed permanently — on the first try) alongside the outcome, so the
    /// caller can record retry metrics on both success and failure.
    pub async fn forward<T: Serialize + ?Sized>(
        &self,
        event: &str,
        payload: &T,
    ) -> (u32, Result<(), WebhookError>) {
        let mut attempt = 0;
        loop {
            attempt += 1;
            match self.send_once(event, payload).await {
                Ok(()) => return (attempt, Ok(())),
                Err(err) => {
                    if !is_retryable(&err) || attempt >= self.max_attempts {
                        return (attempt, Err(err));
                    }
                    // Honour a server-requested `Retry-After` (429); otherwise
                    // exponential backoff (base * 2^(attempt-1)). Both are capped
                    // so a lane can't stall and the delay can't overflow.
                    let delay = match &err {
                        WebhookError::Status {
                            retry_after: Some(after),
                            ..
                        } => *after,
                        _ => self.backoff_base * 2u32.saturating_pow(attempt - 1),
                    }
                    .min(MAX_RETRY_DELAY);
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    /// Perform a single forward attempt without retries.
    async fn send_once<T: Serialize + ?Sized>(
        &self,
        event: &str,
        payload: &T,
    ) -> Result<(), WebhookError> {
        let response = self
            .http
            .post(self.url.clone())
            .header(WEBHOOK_SECRET_HEADER, &self.secret)
            .header(DISCORD_EVENT_HEADER, event)
            .json(payload)
            .send()
            .await
            .map_err(WebhookError::Transport)?;

        let status = response.status();
        if status.is_success() {
            return Ok(());
        }

        // Grab any Retry-After before consuming the response for its body.
        let retry_after = parse_retry_after(response.headers());

        // Capture a bounded snippet of the body for diagnostics.
        let body = response
            .text()
            .await
            .map(|b| truncate(&b, 512))
            .unwrap_or_else(|_| "<unreadable body>".to_string());

        Err(WebhookError::Status {
            status,
            body,
            retry_after,
        })
    }
}

/// Parse a `Retry-After` header expressed in whole seconds. The HTTP-date form
/// is not handled (downstreams send delta-seconds); a missing or unparseable
/// header yields `None`, falling back to exponential backoff.
fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    let secs: u64 = headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()?;
    Some(Duration::from_secs(secs))
}

/// Decide whether an error is worth retrying.
fn is_retryable(err: &WebhookError) -> bool {
    match err {
        // Connection/timeout issues are typically transient.
        WebhookError::Transport(_) => true,
        // Retry on rate limiting and server-side errors only.
        WebhookError::Status { status, .. } => {
            *status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
        }
        WebhookError::InvalidUrl(_) => false,
    }
}

/// Truncate a string to at most `max` bytes on a char boundary.
fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &s[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_url() {
        let err = WebhookClient::new("not a url", "secret").unwrap_err();
        assert!(matches!(err, WebhookError::InvalidUrl(_)));
    }

    #[test]
    fn assembles_super_webhook_url() {
        assert_eq!(
            super_webhook_url("my-slug", "discord", "spctrm.dev"),
            "https://my-slug.spctrm.dev/discord"
        );
        // Stray slashes / whitespace are normalized away.
        assert_eq!(
            super_webhook_url(" my-slug ", "/discord/", "spctrm.dev/"),
            "https://my-slug.spctrm.dev/discord"
        );
    }

    #[test]
    fn for_slug_builds_expected_target() {
        let client =
            WebhookClient::for_slug_with_domain("my-slug", "discord", "spctrm.dev", "secret")
                .unwrap();
        assert_eq!(client.url().as_str(), "https://my-slug.spctrm.dev/discord");
    }

    #[test]
    fn domain_falls_back_to_default_when_unset() {
        // Note: relies on the env var being unset in the test environment.
        unsafe { std::env::remove_var(SUPER_WEBHOOK_DOMAIN_ENV) };
        assert_eq!(super_webhook_domain(), DEFAULT_SUPER_WEBHOOK_DOMAIN);
    }

    #[test]
    fn accepts_slug_url() {
        let client = WebhookClient::new("https://my-slug.spectrm.dev/discord", "secret").unwrap();
        assert_eq!(client.url().host_str(), Some("my-slug.spectrm.dev"));
    }

    #[test]
    fn client_4xx_is_permanent() {
        let err = WebhookError::Status {
            status: StatusCode::UNAUTHORIZED,
            body: String::new(),
            retry_after: None,
        };
        assert!(!is_retryable(&err));
    }

    #[test]
    fn server_5xx_and_429_are_retryable() {
        assert!(is_retryable(&WebhookError::Status {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            body: String::new(),
            retry_after: None,
        }));
        assert!(is_retryable(&WebhookError::Status {
            status: StatusCode::TOO_MANY_REQUESTS,
            body: String::new(),
            retry_after: None,
        }));
    }

    #[test]
    fn truncate_respects_char_boundary() {
        let s = "é".repeat(10); // 2 bytes each
        let out = truncate(&s, 5);
        assert!(out.ends_with('…'));
    }

    #[test]
    fn parses_retry_after_seconds() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(reqwest::header::RETRY_AFTER, "5".parse().unwrap());
        assert_eq!(parse_retry_after(&headers), Some(Duration::from_secs(5)));
    }

    #[test]
    fn ignores_missing_or_non_numeric_retry_after() {
        assert_eq!(parse_retry_after(&reqwest::header::HeaderMap::new()), None);

        // The HTTP-date form is intentionally not parsed.
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::RETRY_AFTER,
            "Wed, 21 Oct 2025 07:28:00 GMT".parse().unwrap(),
        );
        assert_eq!(parse_retry_after(&headers), None);
    }

    #[test]
    fn max_attempts_floored_at_one() {
        let client = WebhookClient::new("https://x.spectrm.dev/discord", "s")
            .unwrap()
            .with_max_attempts(0);
        assert_eq!(client.max_attempts, 1);
    }
}
