use axum::http::Method;
use http::HeaderMap;
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

/// Allowed cross-origin request origins, shared by the REST and S3 interfaces.
/// An empty configuration denies all cross-origin access (no CORS headers are
/// emitted); a literal `*` entry allows every origin.
#[derive(Clone, Debug, Default)]
pub struct CorsConfig {
    allowed_origins: Vec<String>,
    allow_any: bool,
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
        }
    }

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

    pub fn rest_layer(&self) -> Option<CorsLayer> {
        if !self.is_enabled() {
            return None;
        }
        let allow_origin = if self.allow_any {
            AllowOrigin::any()
        } else {
            AllowOrigin::list(
                self.allowed_origins
                    .iter()
                    .filter_map(|origin| origin.parse().ok()),
            )
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
                .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE])
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
        headers.insert(header::VARY, HeaderValue::from_static("origin"));
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
        headers.append(header::VARY, HeaderValue::from_static("origin"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_config_denies_everything() {
        let config = CorsConfig::default();
        assert!(!config.is_enabled());
        assert!(!config.allows(&HeaderValue::from_static("http://portal.test")));
        assert!(config.rest_layer().is_none());
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
    }
}
