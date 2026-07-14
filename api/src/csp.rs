use crate::server_state::ServerState;
use aruna_operations::driver::drive;
use aruna_operations::get_realm_config::GetRealmConfigOperation;
use axum::extract::{Request, State};
use axum::http::{HeaderName, HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, error, warn};
use url::{Host, Url};

/// The portal loads its webfont stylesheet from Google Fonts and the font files
/// from the matching static origin.
const FONT_STYLE_ORIGIN: &str = "https://fonts.googleapis.com";
const FONT_FILE_ORIGIN: &str = "https://fonts.gstatic.com";

/// Anti-clickjacking policy every response carries when it has no stricter one.
const BASELINE_CSP: &str = "frame-ancestors 'none'";

/// OIDC origins change rarely; a point-read behind this window keeps the common
/// portal response off the storage path.
const OIDC_ORIGIN_TTL: Duration = Duration::from_secs(60);
const OIDC_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(5);

const CROSS_ORIGIN_OPENER_POLICY: HeaderName =
    HeaderName::from_static("cross-origin-opener-policy");

/// Extra origins the portal document may connect to, on top of this node's own
/// REST origin, its S3 interface and the realm's OIDC providers. Needed when a
/// deployment expects the portal to reach other nodes (e.g. their S3 endpoints).
#[derive(Clone, Debug, Default)]
pub struct PortalCspConfig {
    extra_connect_origins: Vec<String>,
}

impl PortalCspConfig {
    pub fn new(origins: impl IntoIterator<Item = String>) -> Self {
        let mut extra_connect_origins = Vec::new();
        for origin in origins {
            if let Some(origin) = normalize_origin(&origin) {
                extra_connect_origins.push(origin);
            }
        }
        Self {
            extra_connect_origins,
        }
    }
}

/// Origins the served policy allows, resolved per response.
#[derive(Clone, Debug, Default)]
struct ResolvedOrigins {
    connect: BTreeSet<String>,
    img: BTreeSet<String>,
}

#[derive(Default)]
struct OidcOriginCache {
    origins: BTreeSet<String>,
    token_origins: BTreeMap<String, String>,
    refreshed_at: Option<Instant>,
}

#[derive(Default)]
struct S3OriginCache {
    base_url: Option<String>,
    origin: Option<String>,
}

#[derive(Deserialize)]
struct OidcDiscoveryDocument {
    token_endpoint: String,
}

#[derive(Clone)]
pub(crate) struct PortalSecurity {
    state: Arc<ServerState>,
    config: Arc<PortalCspConfig>,
    client: reqwest::Client,
    s3_cache: Arc<RwLock<S3OriginCache>>,
    oidc_cache: Arc<RwLock<OidcOriginCache>>,
}

impl PortalSecurity {
    pub(crate) fn new(state: Arc<ServerState>, config: PortalCspConfig) -> Self {
        Self {
            state,
            config: Arc::new(config),
            client: reqwest::Client::new(),
            s3_cache: Arc::new(RwLock::new(S3OriginCache::default())),
            oidc_cache: Arc::new(RwLock::new(OidcOriginCache::default())),
        }
    }

    async fn resolve(&self) -> ResolvedOrigins {
        let s3 = self.s3_origin().await;
        let mut connect = BTreeSet::new();
        connect.extend(s3.iter().cloned());
        connect.extend(self.oidc_origins().await);
        connect.extend(self.config.extra_connect_origins.iter().cloned());
        ResolvedOrigins {
            connect,
            img: s3.into_iter().collect(),
        }
    }

    async fn s3_origin(&self) -> Option<String> {
        let s3 = self.state.interface_state().await.s3?;
        {
            let cache = self.s3_cache.read().await;
            if cache.base_url.as_ref() == Some(&s3.base_url) {
                return cache.origin.clone();
            }
        }

        let mut cache = self.s3_cache.write().await;
        if cache.base_url.as_ref() == Some(&s3.base_url) {
            return cache.origin.clone();
        }
        let origin = normalize_origin(&s3.base_url);
        cache.base_url = Some(s3.base_url);
        cache.origin = origin.clone();
        origin
    }

    /// Cached OIDC origins; a stale window triggers a point-read, and a failed
    /// read reuses the last-known-good set so a loaded portal keeps its IdP.
    async fn oidc_origins(&self) -> BTreeSet<String> {
        if let Some(cached) = self.fresh_oidc().await {
            return cached;
        }

        let mut cache = self.oidc_cache.write().await;
        if cache
            .refreshed_at
            .is_some_and(|refreshed_at| refreshed_at.elapsed() < OIDC_ORIGIN_TTL)
        {
            return cache.origins.clone();
        }

        match drive(
            GetRealmConfigOperation::new(self.state.get_realm_id()),
            &self.state.get_ctx(),
        )
        .await
        {
            Ok(config) => {
                let mut origins = BTreeSet::new();
                let mut token_origins = BTreeMap::new();
                for provider in config.oidc_providers {
                    origins.extend(normalize_origin(&provider.issuer));
                    origins.extend(normalize_origin(&provider.discovery_url));
                    let token_origin = match self.oidc_token_endpoint(&provider.discovery_url).await
                    {
                        Ok(token_endpoint) => normalize_origin(&token_endpoint),
                        Err(error) => {
                            debug!(
                                error = %error,
                                discovery_url = %provider.discovery_url,
                                "Portal CSP reuses cached OIDC token endpoint"
                            );
                            cache.token_origins.get(&provider.discovery_url).cloned()
                        }
                    };
                    if let Some(token_origin) = token_origin {
                        origins.insert(token_origin.clone());
                        token_origins.insert(provider.discovery_url, token_origin);
                    }
                }
                cache.origins = origins.clone();
                cache.token_origins = token_origins;
                cache.refreshed_at = Some(Instant::now());
                origins
            }
            Err(error) => {
                debug!(error = %error, "Portal CSP reuses cached OIDC origins");
                cache.refreshed_at = Some(Instant::now());
                cache.origins.clone()
            }
        }
    }

    async fn fresh_oidc(&self) -> Option<BTreeSet<String>> {
        let cache = self.oidc_cache.read().await;
        let refreshed_at = cache.refreshed_at?;
        (refreshed_at.elapsed() < OIDC_ORIGIN_TTL).then(|| cache.origins.clone())
    }

    async fn oidc_token_endpoint(&self, discovery_url: &str) -> reqwest::Result<String> {
        let discovery = self
            .client
            .get(discovery_url)
            .timeout(OIDC_DISCOVERY_TIMEOUT)
            .send()
            .await?
            .error_for_status()?
            .json::<OidcDiscoveryDocument>()
            .await?;
        Ok(discovery.token_endpoint)
    }
}

pub(crate) async fn portal_security_headers(
    State(security): State<PortalSecurity>,
    request: Request,
    next: Next,
) -> Response {
    let policy = content_security_policy(&security.resolve().await);
    let policy = match HeaderValue::from_str(&policy) {
        Ok(policy) => policy,
        Err(error) => {
            error!(error = %error, policy, "Portal CSP rejected; refusing to serve");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_SECURITY_POLICY, policy);
    headers.insert(
        CROSS_ORIGIN_OPENER_POLICY,
        HeaderValue::from_static("same-origin"),
    );
    response
}

/// Headers every response carries, regardless of route: swagger and api-docs
/// serve inline-script HTML on this origin and still need nosniff and the
/// anti-clickjacking baseline. The strict portal policy stays portal-only.
pub(crate) async fn baseline_security_headers(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    if !headers.contains_key(header::CONTENT_SECURITY_POLICY) {
        headers.insert(
            header::CONTENT_SECURITY_POLICY,
            HeaderValue::from_static(BASELINE_CSP),
        );
    }
    response
}

/// `script-src` carries `'wasm-unsafe-eval'` because the portal hashes profile
/// artifacts with hash-wasm, which compiles a WebAssembly module; it permits no
/// JavaScript eval. Inline scripts and styles are not allowed.
fn content_security_policy(origins: &ResolvedOrigins) -> String {
    let connect_src = directive("connect-src 'self'", &origins.connect);
    let img_src = directive("img-src 'self' data: blob:", &origins.img);

    [
        "default-src 'self'",
        "base-uri 'self'",
        "object-src 'none'",
        "frame-src 'none'",
        "frame-ancestors 'none'",
        "form-action 'self'",
        "script-src 'self' 'wasm-unsafe-eval'",
        &format!("style-src 'self' {FONT_STYLE_ORIGIN}"),
        &img_src,
        &format!("font-src 'self' data: {FONT_FILE_ORIGIN}"),
        &connect_src,
    ]
    .join("; ")
}

fn directive(base: &str, origins: &BTreeSet<String>) -> String {
    let mut directive = String::from(base);
    for origin in origins {
        directive.push(' ');
        directive.push_str(origin);
    }
    directive
}

/// Origins may broaden `connect-src`/`img-src`, so only https (or http on a
/// loopback host, for local development) is accepted; anything else is dropped.
fn normalize_origin(value: &str) -> Option<String> {
    let url = Url::parse(value.trim()).ok()?;
    if !is_secure_origin(&url) {
        warn!(origin = %value, "Portal CSP ignores insecure origin");
        return None;
    }
    let origin = url.origin().ascii_serialization();
    (origin != "null").then_some(origin)
}

fn is_secure_origin(url: &Url) -> bool {
    match url.scheme() {
        "https" => true,
        "http" => is_loopback_host(url.host()),
        _ => false,
    }
}

fn is_loopback_host(host: Option<Host<&str>>) -> bool {
    match host {
        Some(Host::Ipv4(ip)) => ip.is_loopback(),
        Some(Host::Ipv6(ip)) => ip.is_loopback(),
        Some(Host::Domain(domain)) => domain.eq_ignore_ascii_case("localhost"),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::{ResolvedOrigins, content_security_policy, normalize_origin};

    fn origins(connect: &[&str], img: &[&str]) -> ResolvedOrigins {
        ResolvedOrigins {
            connect: connect.iter().map(|s| s.to_string()).collect(),
            img: img.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn origins_drop_noise() {
        assert_eq!(
            normalize_origin("https://issuer.test/realms/aruna/.well-known"),
            Some("https://issuer.test".to_string())
        );
        assert_eq!(
            normalize_origin(" http://127.0.0.1:9000/ "),
            Some("http://127.0.0.1:9000".to_string())
        );
        assert_eq!(normalize_origin("not-a-url"), None);
        assert_eq!(normalize_origin("data:text/html,x"), None);
    }

    #[test]
    fn rejects_insecure_origins() {
        // Only https, or http on a loopback host, may broaden the policy.
        assert_eq!(normalize_origin("http://issuer.test"), None);
        assert_eq!(normalize_origin("ws://issuer.test"), None);
        assert_eq!(normalize_origin("ftp://issuer.test"), None);
        assert_eq!(
            normalize_origin("https://issuer.test"),
            Some("https://issuer.test".to_string())
        );
        assert_eq!(
            normalize_origin("http://localhost:8080"),
            Some("http://localhost:8080".to_string())
        );
        assert_eq!(
            normalize_origin("http://[::1]:9000"),
            Some("http://[::1]:9000".to_string())
        );
    }

    #[test]
    fn policy_pins_needs() {
        let policy = content_security_policy(&ResolvedOrigins::default());

        assert!(policy.contains("default-src 'self'"));
        assert!(policy.contains("script-src 'self' 'wasm-unsafe-eval'"));
        assert!(policy.contains("style-src 'self' https://fonts.googleapis.com"));
        assert!(policy.contains("font-src 'self' data: https://fonts.gstatic.com"));
        assert!(policy.contains("img-src 'self' data: blob:"));
        assert!(policy.contains("connect-src 'self'"));
        assert!(policy.contains("frame-ancestors 'none'"));
        assert!(policy.contains("object-src 'none'"));
        assert!(policy.contains("base-uri 'self'"));
        assert!(policy.contains("form-action 'self'"));
        assert!(!policy.contains("unsafe-inline"));
        assert!(!policy.contains("'unsafe-eval'"));
    }

    #[test]
    fn connect_src_lists() {
        let policy = content_security_policy(&origins(
            &["https://issuer.test", "http://127.0.0.1:9000"],
            &[],
        ));

        assert!(policy.ends_with("connect-src 'self' http://127.0.0.1:9000 https://issuer.test"));
    }

    #[test]
    fn img_src_allows_s3() {
        let policy = content_security_policy(&origins(&["https://s3.test"], &["https://s3.test"]));

        assert!(policy.contains("img-src 'self' data: blob: https://s3.test"));
    }
}
