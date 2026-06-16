//! Spectrum cloud API client.
//!
//! Everything that talks to the Spectrum control plane lives here. Right now
//! that means resolving a project's **slug** — the subdomain label the Fusor
//! super-webhook edge is addressed by (`https://{slug}.{domain}/{platform}`,
//! see [`crate::webhook`]).
//!
//! The slug is read by GETting the project, authenticating with the project's
//! `projectId` / `projectSecret` via HTTP Basic auth. This is purely a lookup —
//! no modifications. Equivalent to:
//!
//! ```sh
//! curl -sS \
//!   -H "Authorization: Basic $(printf '%s' "${PROJECT_ID}:${PROJECT_SECRET}" | base64)" \
//!   "${SPECTRUM_CLOUD_URL:-https://spectrum.photon.codes}/projects/${PROJECT_ID}/"
//! ```
//!
//! The response wraps the project in a `{ "succeed": ..., "data": { ... } }`
//! envelope; we pull `data.slug` out of it.
//!
//! Nothing is baked in beyond the default cloud URL: the base URL is overridable
//! via [`SPECTRUM_CLOUD_URL_ENV`], and the project credentials are supplied by
//! the caller.

use std::time::Duration;

use reqwest::{Client, StatusCode, Url};
use serde::Deserialize;
use veil::Redact;

/// Default base URL of the Spectrum cloud control plane. This is the only
/// baked-in default — override per-environment via [`SPECTRUM_CLOUD_URL_ENV`]
/// or an explicit base URL passed to [`SpectrumClient::with_base_url`].
const DEFAULT_SPECTRUM_CLOUD_URL: &str = "https://spectrum.photon.codes";

/// Environment variable that overrides [`DEFAULT_SPECTRUM_CLOUD_URL`].
const SPECTRUM_CLOUD_URL_ENV: &str = "SPECTRUM_CLOUD_URL";

/// Default per-request timeout. Control-plane calls happen at startup and must
/// not hang the process indefinitely.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(15);

/// Resolve the configured Spectrum cloud base URL.
///
/// Returns the value of `SPECTRUM_CLOUD_URL` if it is set and non-empty,
/// otherwise [`DEFAULT_SPECTRUM_CLOUD_URL`]. Surrounding whitespace and a
/// trailing slash are trimmed so the env value is forgiving to set.
pub fn spectrum_cloud_url() -> String {
    match std::env::var(SPECTRUM_CLOUD_URL_ENV) {
        Ok(v) if !v.trim().is_empty() => v.trim().trim_end_matches('/').to_string(),
        _ => DEFAULT_SPECTRUM_CLOUD_URL.to_string(),
    }
}

/// Assemble the project endpoint for `project_id` against `base_url`, i.e.
/// `{base_url}/projects/{project_id}/`. `base_url` is trimmed of a trailing
/// slash and `project_id` of surrounding whitespace/slashes so callers don't
/// have to normalize them.
fn project_endpoint(base_url: &str, project_id: &str) -> String {
    let base = base_url.trim().trim_end_matches('/');
    let project_id = project_id.trim().trim_matches('/');
    format!("{base}/projects/{project_id}/")
}

/// Errors that can occur while talking to the Spectrum cloud API.
#[derive(Debug)]
pub enum SpectrumError {
    /// The resolved endpoint could not be parsed as a URL.
    InvalidUrl(<Url as std::str::FromStr>::Err),
    /// The request could not be built, sent, or timed out at the transport
    /// layer (DNS, TLS, connection, read timeout, ...).
    Transport(reqwest::Error),
    /// The API returned a non-success status. Carries the status and a
    /// best-effort snippet of the response body for diagnostics.
    Status { status: StatusCode, body: String },
    /// The response was a success but carried no `data.slug`, so there's
    /// nothing to address the downstream edge with.
    MissingSlug,
}

impl std::fmt::Display for SpectrumError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpectrumError::InvalidUrl(e) => write!(f, "invalid spectrum url: {e}"),
            SpectrumError::Transport(e) => write!(f, "transport error: {e}"),
            SpectrumError::Status { status, body } => {
                write!(f, "spectrum returned {status}: {body}")
            }
            SpectrumError::MissingSlug => write!(f, "project response carried no slug"),
        }
    }
}

impl std::error::Error for SpectrumError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SpectrumError::InvalidUrl(e) => Some(e),
            SpectrumError::Transport(e) => Some(e),
            SpectrumError::Status { .. } | SpectrumError::MissingSlug => None,
        }
    }
}

/// Response envelope for the project GET. The API wraps the project in a
/// `{ "succeed": ..., "data": { ... } }` shape; we only need `data.slug`.
#[derive(Deserialize)]
struct ProjectResponse {
    #[serde(default)]
    data: Option<ProjectData>,
}

/// The `data` object of a project response. Only the fields we consume are
/// modeled; the rest (`id`, `name`, `profile`, ...) are ignored.
#[derive(Deserialize)]
struct ProjectData {
    #[serde(default)]
    slug: Option<String>,
}

/// A client for a single Spectrum project's control-plane operations.
///
/// Cheap to [`clone`](Clone) — the underlying connection pool is shared.
#[derive(Redact, Clone)]
pub struct SpectrumClient {
    http: Client,
    base_url: String,
    project_id: String,
    #[redact(fixed = 8)]
    project_secret: String,
}

impl SpectrumClient {
    /// Create a client for `project_id` / `project_secret`, resolving the base
    /// URL from the environment (`SPECTRUM_CLOUD_URL`, falling back to
    /// [`DEFAULT_SPECTRUM_CLOUD_URL`]), building its own connection pool.
    ///
    /// Bots resolve slugs through [`with_client`](Self::with_client) to share one
    /// pool; this self-contained variant is kept for tests and standalone use.
    #[allow(dead_code)]
    pub fn new(
        project_id: impl Into<String>,
        project_secret: impl Into<String>,
    ) -> Result<Self, SpectrumError> {
        Self::with_base_url(spectrum_cloud_url(), project_id, project_secret)
    }

    /// Like [`new`](Self::new) but with an explicit `base_url`, bypassing the
    /// environment lookup entirely. Kept for tests and standalone use.
    #[allow(dead_code)]
    pub fn with_base_url(
        base_url: impl Into<String>,
        project_id: impl Into<String>,
        project_secret: impl Into<String>,
    ) -> Result<Self, SpectrumError> {
        let http = Client::builder()
            .timeout(DEFAULT_TIMEOUT)
            .build()
            .map_err(SpectrumError::Transport)?;
        Ok(Self::with_client(
            http,
            base_url,
            project_id,
            project_secret,
        ))
    }

    /// Like [`with_base_url`](Self::with_base_url) but reuses an existing
    /// [`reqwest::Client`], useful when many clients share one connection pool.
    pub fn with_client(
        http: Client,
        base_url: impl Into<String>,
        project_id: impl Into<String>,
        project_secret: impl Into<String>,
    ) -> Self {
        Self {
            http,
            base_url: base_url.into(),
            project_id: project_id.into(),
            project_secret: project_secret.into(),
        }
    }

    /// Look up this project's slug.
    ///
    /// Issues `GET {base_url}/projects/{project_id}/` with the project's
    /// Basic-auth credentials and returns `data.slug` from the response
    /// envelope. This is a pure read — nothing is modified.
    ///
    /// Returns [`SpectrumError::Status`] for any non-success response (bad
    /// credentials, unknown project, ...), since those are permanent and a blind
    /// retry won't help, and [`SpectrumError::MissingSlug`] if a successful
    /// response carries no slug.
    pub async fn get_project_slug(&self) -> Result<String, SpectrumError> {
        let endpoint = project_endpoint(&self.base_url, &self.project_id);
        let url = endpoint.parse::<Url>().map_err(SpectrumError::InvalidUrl)?;

        let response = self
            .http
            .get(url)
            .basic_auth(&self.project_id, Some(&self.project_secret))
            .send()
            .await
            .map_err(SpectrumError::Transport)?;

        let status = response.status();
        if !status.is_success() {
            // Capture a bounded snippet of the body for diagnostics.
            let body = response
                .text()
                .await
                .map(|b| truncate(&b, 512))
                .unwrap_or_else(|_| "<unreadable body>".to_string());
            return Err(SpectrumError::Status { status, body });
        }

        response
            .json::<ProjectResponse>()
            .await
            .map_err(SpectrumError::Transport)?
            .data
            .and_then(|d| d.slug)
            .filter(|s| !s.trim().is_empty())
            .ok_or(SpectrumError::MissingSlug)
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
    fn assembles_project_endpoint() {
        assert_eq!(
            project_endpoint("https://spectrum.photon.codes", "proj-123"),
            "https://spectrum.photon.codes/projects/proj-123/"
        );
        // Stray trailing slash / whitespace are normalized away.
        assert_eq!(
            project_endpoint("https://spectrum.photon.codes/", " /proj-123/ "),
            "https://spectrum.photon.codes/projects/proj-123/"
        );
    }

    #[test]
    fn cloud_url_falls_back_to_default() {
        // Ensure the override is unset so we observe the baked-in default.
        unsafe {
            std::env::remove_var(SPECTRUM_CLOUD_URL_ENV);
        }
        assert_eq!(spectrum_cloud_url(), DEFAULT_SPECTRUM_CLOUD_URL);
    }
}
