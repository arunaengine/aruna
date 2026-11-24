//! The browser policy of applet responses: content security, framing and caching headers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use axum::http::header::{self, HeaderMap, HeaderName, HeaderValue};

/// Everything an applet may load comes from its own origin. Styles allow inline use because
/// component libraries add style elements at run time; scripts never do.
const CONTENT_POLICY: &str = "default-src 'self'; script-src 'self'; \
    style-src 'self' 'unsafe-inline'; img-src 'self' data: blob:; font-src 'self' data:; \
    connect-src 'self'; worker-src 'self' blob:; object-src 'none'; base-uri 'self'; \
    form-action 'self'";

/// Who may frame a response.
#[derive(Clone, Copy, Debug)]
pub enum Frame<'a> {
    /// Only the portal, and only for a preview session of the creator or an approver.
    Preview(&'a str),
    /// The applet's own embedding origins from the host decision; empty means nobody.
    Embed(&'a [String]),
}

/// Sets the security headers every applet response carries.
pub fn secure(headers: &mut HeaderMap, frame: Frame<'_>) {
    let ancestors = match frame {
        Frame::Preview(portal) => origin(portal).map(str::to_string),
        Frame::Embed(origins) => {
            let valid: Vec<&str> = origins.iter().filter_map(|o| origin(o)).collect();
            (!valid.is_empty()).then(|| valid.join(" "))
        }
    };
    let policy = format!(
        "{CONTENT_POLICY}; frame-ancestors {}",
        ancestors.as_deref().unwrap_or("'none'")
    );
    if let Ok(policy) = HeaderValue::from_str(&policy) {
        headers.insert(header::CONTENT_SECURITY_POLICY, policy);
    }
    match ancestors {
        Some(_) => headers.remove(header::X_FRAME_OPTIONS),
        None => headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY")),
    };
    let fixed = [
        (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        (header::REFERRER_POLICY, "same-origin"),
        (
            HeaderName::from_static("cross-origin-opener-policy"),
            "same-origin",
        ),
        (
            HeaderName::from_static("cross-origin-resource-policy"),
            "same-origin",
        ),
    ];
    for (name, value) in fixed {
        headers.insert(name, HeaderValue::from_static(value));
    }
}

/// `text` when it is exactly a web origin, so it cannot widen or break the policy.
pub fn origin(text: &str) -> Option<&str> {
    let parsed = url::Url::parse(text).ok()?;
    let web = matches!(parsed.scheme(), "https" | "http");
    (web && parsed.origin().ascii_serialization() == text).then_some(text)
}

/// How a response may be cached. `shared` holds only for the live version of a public applet;
/// everything a visitor's session decides stays private.
pub fn caching(path: &str, shared: bool) -> &'static str {
    match (shared, hashed(path)) {
        (true, true) => "public, max-age=31536000, immutable",
        (true, false) => "public, no-cache",
        (false, true) => "private, max-age=31536000, immutable",
        (false, false) => "private, no-cache",
    }
}

/// Whether the file name carries a content hash, as bundlers write `index-B2x9cQ4f.js`.
fn hashed(path: &str) -> bool {
    let name = path.rsplit('/').next().unwrap_or_default();
    let Some((stem, _)) = name.rsplit_once('.') else {
        return false;
    };
    let Some((_, hash)) = stem.rsplit_once(['-', '.']) else {
        return false;
    };
    let mixed = hash
        .bytes()
        .any(|b| b.is_ascii_digit() || b.is_ascii_uppercase());
    (8..=32).contains(&hash.len())
        && mixed
        && hash.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(frame: Frame<'_>) -> (String, Option<String>) {
        let mut headers = HeaderMap::new();
        secure(&mut headers, frame);
        let read = |name| {
            (headers.get(name))
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
        };
        let policy = read(header::CONTENT_SECURITY_POLICY).expect("policy is set");
        (policy, read(header::X_FRAME_OPTIONS))
    }

    // Without embedding origins and without a preview session nobody may frame the applet.
    #[test]
    fn never_frameable_default() {
        let (csp, frame) = policy(Frame::Embed(&[]));
        assert!(csp.ends_with("frame-ancestors 'none'"));
        assert_eq!(frame.as_deref(), Some("DENY"));
        for part in [
            "default-src 'self'",
            "script-src 'self';",
            "object-src 'none'",
        ] {
            assert!(csp.contains(part), "{part} missing from {csp}");
        }
        assert!(csp.contains("connect-src 'self';") && csp.contains("base-uri 'self'"));
        let invalid = ["*".to_string(), "https://a.test; script-src *".to_string()];
        let (csp, frame) = policy(Frame::Embed(&invalid));
        assert!(csp.ends_with("frame-ancestors 'none'"));
        assert_eq!(frame.as_deref(), Some("DENY"));
    }

    #[test]
    fn embed_lists_origins() {
        let origins = [
            "https://lab.test".to_string(),
            "https://b.test:8443".to_string(),
        ];
        let (csp, frame) = policy(Frame::Embed(&origins));
        assert!(csp.ends_with("frame-ancestors https://lab.test https://b.test:8443"));
        assert!(frame.is_none());
    }

    #[test]
    fn preview_names_portal() {
        let (csp, frame) = policy(Frame::Preview("https://portal.test"));
        assert!(csp.ends_with("frame-ancestors https://portal.test"));
        assert!(frame.is_none());
        let (csp, frame) = policy(Frame::Preview("https://portal.test/path"));
        assert!(csp.ends_with("frame-ancestors 'none'"));
        assert_eq!(frame.as_deref(), Some("DENY"));
    }

    #[test]
    fn caching_follows_hash() {
        let immutable = "public, max-age=31536000, immutable";
        assert_eq!(caching("/assets/index-B2x9cQ4f.js", true), immutable);
        assert_eq!(
            caching("/assets/index-B2x9cQ4f.css", false),
            "private, max-age=31536000, immutable"
        );
        assert_eq!(caching("/index.html", true), "public, no-cache");
        assert_eq!(caching("/assets/logo-dark.svg", true), "public, no-cache");
        assert_eq!(caching("/data.json", false), "private, no-cache");
    }
}
