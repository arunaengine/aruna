use axum::http::Method;
use http::HeaderMap;
use http::HeaderName;
use http::HeaderValue;
use http::header;
use std::time::Duration;
use tower_http::cors::{AllowOrigin, CorsLayer};

const CORS_MAX_AGE: Duration = Duration::from_secs(3600);
const S3_ALLOWED_METHODS: &str = "GET,HEAD,PUT,POST,DELETE,OPTIONS";
const S3_DEFAULT_ALLOWED_HEADERS: &str = "authorization,content-type,content-md5,range,\
     x-amz-content-sha256,x-amz-date,x-amz-security-token,x-amz-user-agent";
const S3_EXPOSED_HEADERS: &str = "etag,content-range,accept-ranges,content-length,last-modified,\
     x-amz-request-id,x-amz-version-id,x-amz-delete-marker,aruna-source-content-type,\
     aruna-source-etag,aruna-source-last-modified,aruna-last-refresh";
pub(crate) const S3_PREFLIGHT_VARY: &[HeaderName] = &[
    header::ORIGIN,
    header::ACCESS_CONTROL_REQUEST_METHOD,
    header::ACCESS_CONTROL_REQUEST_HEADERS,
];

/// Origins of the first-party Aruna Desktop shell, which serves the portal
/// build from its own fixed origin. Which of the two a request carries depends
/// on the platform webview.
pub const DESKTOP_ORIGINS: [&str; 2] = ["tauri://localhost", "http://tauri.localhost"];

/// Allowed cross-origin request origins, shared by the REST and S3 interfaces.
/// An empty configuration denies all cross-origin access to S3 (no CORS headers
/// are emitted); a literal `*` entry allows every origin.
///
/// The REST API additionally admits [`DESKTOP_ORIGINS`], including when nothing
/// is configured, so the desktop app reaches realms it did not deploy itself.
/// That is safe because REST authenticates by bearer token and never by cookie,
/// which makes the origin allowlist defense in depth rather than the
/// authorization boundary; operators can still opt out with `DESKTOP_CORS=off`.
#[derive(Clone, Debug)]
pub struct CorsConfig {
    allowed_origins: Vec<String>,
    allow_any: bool,
    allow_desktop: bool,
}

impl Default for CorsConfig {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

impl CorsConfig {
    pub fn new(origins: impl IntoIterator<Item = String>) -> Self {
        let mut allowed_origins = Vec::new();
        let mut allow_any = false;
        for origin in origins {
            let origin = origin.trim().trim_end_matches('/');
            if origin.is_empty() {
                continue;
            }
            if origin == "*" {
                allow_any = true;
            } else {
                allowed_origins.push(origin.to_string());
            }
        }
        Self {
            allowed_origins,
            allow_any,
            allow_desktop: true,
        }
    }

    /// Operator escape hatch for the desktop admission (`DESKTOP_CORS=off`).
    pub fn with_desktop(mut self, allow_desktop: bool) -> Self {
        self.allow_desktop = allow_desktop;
        self
    }

    /// Whether any origin is configured. The desktop admission is REST-only and
    /// deliberately does not enable the S3 interface.
    pub fn is_enabled(&self) -> bool {
        self.allow_any || !self.allowed_origins.is_empty()
    }

    pub fn allows(&self, origin: &HeaderValue) -> bool {
        if self.allow_any {
            return true;
        }
        origin
            .to_str()
            .map(|origin| {
                self.allowed_origins
                    .iter()
                    .any(|allowed| allowed == origin.trim_end_matches('/'))
            })
            .unwrap_or(false)
    }

    fn rest_origins(&self) -> Vec<HeaderValue> {
        let desktop: &[&str] = if self.allow_desktop {
            &DESKTOP_ORIGINS
        } else {
            &[]
        };
        self.allowed_origins
            .iter()
            .map(String::as_str)
            .chain(desktop.iter().copied())
            .filter_map(|origin| HeaderValue::from_str(origin).ok())
            .collect()
    }

    pub(crate) fn mcp_origins(&self) -> Vec<String> {
        if self.allow_any {
            return Vec::new();
        }
        let desktop: &[&str] = if self.allow_desktop {
            &DESKTOP_ORIGINS
        } else {
            &[]
        };
        self.allowed_origins
            .iter()
            .cloned()
            .chain(desktop.iter().map(|origin| (*origin).to_string()))
            .collect()
    }

    pub fn rest_layer(&self) -> Option<CorsLayer> {
        let allow_origin = if self.allow_any {
            AllowOrigin::any()
        } else {
            let origins = self.rest_origins();
            if origins.is_empty() {
                return None;
            }
            AllowOrigin::list(origins)
        };
        Some(
            CorsLayer::new()
                .allow_origin(allow_origin)
                .allow_methods([
                    Method::GET,
                    Method::POST,
                    Method::PUT,
                    Method::PATCH,
                    Method::DELETE,
                    Method::HEAD,
                    Method::OPTIONS,
                ])
                .allow_headers([
                    header::AUTHORIZATION,
                    header::CONTENT_TYPE,
                    HeaderName::from_static("mcp-method"),
                    HeaderName::from_static("mcp-name"),
                    HeaderName::from_static("mcp-protocol-version"),
                    HeaderName::from_static("mcp-session-id"),
                ])
                // The portal is always cross-origin now, so its retry backoff
                // and download filenames depend on these being readable.
                .expose_headers([header::CONTENT_DISPOSITION, header::RETRY_AFTER])
                .max_age(CORS_MAX_AGE),
        )
    }

    fn allow_origin_value(&self, origin: &HeaderValue) -> HeaderValue {
        if self.allow_any {
            HeaderValue::from_static("*")
        } else {
            origin.clone()
        }
    }

    /// Headers for an S3 preflight response. Returns `None` when the origin is
    /// not allowed; the preflight is then answered without CORS headers.
    pub fn s3_preflight_headers(
        &self,
        origin: &HeaderValue,
        requested_headers: Option<&HeaderValue>,
    ) -> Option<HeaderMap> {
        if !self.allows(origin) {
            return None;
        }
        let mut headers = HeaderMap::new();
        headers.insert(
            header::ACCESS_CONTROL_ALLOW_ORIGIN,
            self.allow_origin_value(origin),
        );
        headers.insert(
            header::ACCESS_CONTROL_ALLOW_METHODS,
            HeaderValue::from_static(S3_ALLOWED_METHODS),
        );
        headers.insert(
            header::ACCESS_CONTROL_ALLOW_HEADERS,
            requested_headers
                .cloned()
                .unwrap_or_else(|| HeaderValue::from_static(S3_DEFAULT_ALLOWED_HEADERS)),
        );
        headers.insert(
            header::ACCESS_CONTROL_MAX_AGE,
            HeaderValue::from(CORS_MAX_AGE.as_secs()),
        );
        append_vary_headers(&mut headers, S3_PREFLIGHT_VARY);
        Some(headers)
    }

    /// Adds CORS headers to a normal (non-preflight) S3 response when the
    /// request origin is allowed.
    pub fn apply_s3_response_headers(&self, origin: Option<&HeaderValue>, headers: &mut HeaderMap) {
        let Some(origin) = origin else {
            return;
        };
        if !self.allows(origin) {
            return;
        }
        headers.insert(
            header::ACCESS_CONTROL_ALLOW_ORIGIN,
            self.allow_origin_value(origin),
        );
        headers.insert(
            header::ACCESS_CONTROL_EXPOSE_HEADERS,
            HeaderValue::from_static(S3_EXPOSED_HEADERS),
        );
        append_vary_headers(headers, &[header::ORIGIN]);
    }
}

pub(crate) fn append_vary_headers(headers: &mut HeaderMap, values: &[HeaderName]) {
    let mut vary_values = headers
        .get(header::VARY)
        .and_then(|value| value.to_str().ok())
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|entry| !entry.is_empty())
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    for value in values {
        if !vary_values
            .iter()
            .any(|existing| existing.eq_ignore_ascii_case(value.as_str()))
        {
            vary_values.push(value.as_str().to_string());
        }
    }

    if !vary_values.is_empty()
        && let Ok(value) = HeaderValue::from_str(&vary_values.join(", "))
    {
        headers.insert(header::VARY, value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rest_exposes_disposition() {
        use axum::{Router, body::Body, http::Request, routing::get};
        use tower::ServiceExt;

        let config = CorsConfig::new(vec!["http://portal.test".to_string()]);
        let app = Router::new()
            .route("/", get(|| async {}))
            .layer(config.rest_layer().unwrap());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/")
                    .header(header::ORIGIN, "http://portal.test")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            response
                .headers()
                .get(header::ACCESS_CONTROL_EXPOSE_HEADERS)
                .unwrap(),
            "content-disposition,retry-after"
        );
    }

    async fn rest_allow_origin(config: &CorsConfig, origin: &'static str) -> Option<String> {
        use axum::{Router, body::Body, http::Request, routing::get};
        use tower::ServiceExt;

        let app = Router::new()
            .route("/", get(|| async {}))
            .layer(config.rest_layer()?);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/")
                    .header(header::ORIGIN, origin)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        response
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .map(|value| value.to_str().unwrap().to_string())
    }

    #[tokio::test]
    async fn empty_admits_desktop() {
        // Realms that configured no portal origin must still serve the app.
        let config = CorsConfig::default();
        for origin in DESKTOP_ORIGINS {
            assert_eq!(
                rest_allow_origin(&config, origin).await.as_deref(),
                Some(origin)
            );
        }
        assert!(
            rest_allow_origin(&config, "http://evil.test")
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn list_admits_desktop() {
        let config = CorsConfig::new(vec!["http://portal.test".to_string()]);
        assert_eq!(
            rest_allow_origin(&config, "http://portal.test")
                .await
                .as_deref(),
            Some("http://portal.test")
        );
        assert_eq!(
            rest_allow_origin(&config, "tauri://localhost")
                .await
                .as_deref(),
            Some("tauri://localhost")
        );
        assert!(
            rest_allow_origin(&config, "http://evil.test")
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn desktop_hatch_off() {
        let config = CorsConfig::new(vec!["http://portal.test".to_string()]).with_desktop(false);
        assert!(
            rest_allow_origin(&config, "tauri://localhost")
                .await
                .is_none()
        );
        assert_eq!(
            rest_allow_origin(&config, "http://portal.test")
                .await
                .as_deref(),
            Some("http://portal.test")
        );
        assert!(
            CorsConfig::default()
                .with_desktop(false)
                .rest_layer()
                .is_none()
        );
    }

    #[test]
    fn s3_denies_desktop() {
        // The desktop admission covers the bearer-only REST API, not S3.
        let config = CorsConfig::new(vec!["http://portal.test".to_string()]);
        let origin = HeaderValue::from_static("tauri://localhost");
        assert!(!config.allows(&origin));
        assert!(config.s3_preflight_headers(&origin, None).is_none());
        let mut headers = HeaderMap::new();
        config.apply_s3_response_headers(Some(&origin), &mut headers);
        assert!(headers.is_empty());
    }

    #[test]
    fn empty_denies_foreign() {
        let config = CorsConfig::default();
        assert!(!config.is_enabled());
        assert!(!config.allows(&HeaderValue::from_static("http://portal.test")));
        assert!(
            config
                .s3_preflight_headers(&HeaderValue::from_static("http://portal.test"), None)
                .is_none()
        );
        let mut headers = HeaderMap::new();
        config.apply_s3_response_headers(
            Some(&HeaderValue::from_static("http://portal.test")),
            &mut headers,
        );
        assert!(headers.is_empty());
    }

    #[test]
    fn listed_origin_is_allowed_and_reflected() {
        let config = CorsConfig::new(vec!["http://portal.test".to_string()]);
        let origin = HeaderValue::from_static("http://portal.test");
        assert!(config.allows(&origin));
        assert!(!config.allows(&HeaderValue::from_static("http://evil.test")));

        let headers = config.s3_preflight_headers(&origin, None).unwrap();
        assert_eq!(
            headers.get(header::ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(),
            &origin
        );

        let mut response_headers = HeaderMap::new();
        config.apply_s3_response_headers(Some(&origin), &mut response_headers);
        assert_eq!(
            response_headers
                .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .unwrap(),
            &origin
        );
        assert!(
            response_headers
                .get(header::ACCESS_CONTROL_EXPOSE_HEADERS)
                .unwrap()
                .to_str()
                .unwrap()
                .contains("etag")
        );
    }

    #[test]
    fn wildcard_allows_any_origin() {
        let config = CorsConfig::new(vec!["*".to_string()]);
        let origin = HeaderValue::from_static("http://anywhere.test");
        assert!(config.allows(&origin));
        let headers = config.s3_preflight_headers(&origin, None).unwrap();
        assert_eq!(
            headers.get(header::ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(),
            "*"
        );
    }

    #[test]
    fn preflight_echoes_requested_headers() {
        let config = CorsConfig::new(vec!["http://portal.test".to_string()]);
        let requested = HeaderValue::from_static("authorization,x-amz-meta-custom");
        let headers = config
            .s3_preflight_headers(
                &HeaderValue::from_static("http://portal.test"),
                Some(&requested),
            )
            .unwrap();
        assert_eq!(
            headers.get(header::ACCESS_CONTROL_ALLOW_HEADERS).unwrap(),
            &requested
        );
        assert_eq!(
            headers.get(header::VARY).unwrap(),
            "origin, access-control-request-method, access-control-request-headers"
        );
    }
}
