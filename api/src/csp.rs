use crate::server_state::ServerState;
use aruna_operations::driver::drive;
use aruna_operations::get_realm_config::GetRealmConfigOperation;
use axum::extract::{Request, State};
use axum::http::{HeaderName, HeaderValue, header};
use axum::middleware::Next;
use axum::response::Response;
use std::collections::BTreeSet;
use std::sync::Arc;
use tracing::debug;
use url::Url;

/// The portal loads its webfont stylesheet from Google Fonts and the font files
/// from the matching static origin.
const FONT_STYLE_ORIGIN: &str = "https://fonts.googleapis.com";
const FONT_FILE_ORIGIN: &str = "https://fonts.gstatic.com";

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

#[derive(Clone)]
pub(crate) struct PortalSecurity {
    state: Arc<ServerState>,
    config: Arc<PortalCspConfig>,
}

impl PortalSecurity {
    pub(crate) fn new(state: Arc<ServerState>, config: PortalCspConfig) -> Self {
        Self {
            state,
            config: Arc::new(config),
        }
    }

    async fn connect_origins(&self) -> BTreeSet<String> {
        let mut origins = BTreeSet::new();

        if let Some(s3) = self.state.interface_state().await.s3
            && let Some(origin) = normalize_origin(&s3.base_url)
        {
            origins.insert(origin);
        }

        match drive(
            GetRealmConfigOperation::new(self.state.get_realm_id()),
            &self.state.get_ctx(),
        )
        .await
        {
            Ok(config) => {
                for provider in config.oidc_providers {
                    origins.extend(normalize_origin(&provider.issuer));
                    origins.extend(normalize_origin(&provider.discovery_url));
                }
            }
            // The realm config is unavailable until it syncs; sign-in needs it
            // anyway, so the portal keeps booting with the baseline policy.
            Err(error) => debug!(error = %error, "Portal CSP omits realm OIDC origins"),
        }

        origins.extend(self.config.extra_connect_origins.iter().cloned());
        origins
    }
}

pub(crate) async fn portal_security_headers(
    State(security): State<PortalSecurity>,
    request: Request,
    next: Next,
) -> Response {
    let policy = content_security_policy(&security.connect_origins().await);
    let mut response = next.run(request).await;
    let headers = response.headers_mut();

    if let Ok(policy) = HeaderValue::from_str(&policy) {
        headers.insert(header::CONTENT_SECURITY_POLICY, policy);
    }
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    headers.insert(
        CROSS_ORIGIN_OPENER_POLICY,
        HeaderValue::from_static("same-origin"),
    );
    response
}

/// `script-src` carries `'wasm-unsafe-eval'` because the portal hashes profile
/// artifacts with hash-wasm, which compiles a WebAssembly module; it permits no
/// JavaScript eval. Inline scripts and styles are not allowed.
fn content_security_policy(connect_origins: &BTreeSet<String>) -> String {
    let mut connect_src = String::from("connect-src 'self'");
    for origin in connect_origins {
        connect_src.push(' ');
        connect_src.push_str(origin);
    }

    [
        "default-src 'self'",
        "base-uri 'self'",
        "object-src 'none'",
        "frame-src 'none'",
        "frame-ancestors 'none'",
        "form-action 'self'",
        "script-src 'self' 'wasm-unsafe-eval'",
        &format!("style-src 'self' {FONT_STYLE_ORIGIN}"),
        "img-src 'self' data:",
        &format!("font-src 'self' data: {FONT_FILE_ORIGIN}"),
        &connect_src,
    ]
    .join("; ")
}

fn normalize_origin(value: &str) -> Option<String> {
    let origin = Url::parse(value.trim())
        .ok()?
        .origin()
        .ascii_serialization();
    (origin != "null").then_some(origin)
}

#[cfg(test)]
mod tests {
    use super::{content_security_policy, normalize_origin};
    use std::collections::BTreeSet;

    #[test]
    fn origins_drop_path_and_scheme_noise() {
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
    fn policy_pins_portal_needs() {
        let policy = content_security_policy(&BTreeSet::new());

        assert!(policy.contains("default-src 'self'"));
        assert!(policy.contains("script-src 'self' 'wasm-unsafe-eval'"));
        assert!(policy.contains("style-src 'self' https://fonts.googleapis.com"));
        assert!(policy.contains("font-src 'self' data: https://fonts.gstatic.com"));
        assert!(policy.contains("img-src 'self' data:"));
        assert!(policy.contains("connect-src 'self'"));
        assert!(policy.contains("frame-ancestors 'none'"));
        assert!(policy.contains("object-src 'none'"));
        assert!(policy.contains("base-uri 'self'"));
        assert!(policy.contains("form-action 'self'"));
        assert!(!policy.contains("unsafe-inline"));
        assert!(!policy.contains("'unsafe-eval'"));
    }

    #[test]
    fn connect_src_lists_origins() {
        let origins = BTreeSet::from([
            "https://issuer.test".to_string(),
            "http://127.0.0.1:9000".to_string(),
        ]);

        let policy = content_security_policy(&origins);

        assert!(policy.ends_with("connect-src 'self' http://127.0.0.1:9000 https://issuer.test"));
    }
}
