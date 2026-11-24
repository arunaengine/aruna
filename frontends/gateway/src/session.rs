//! Seals visitor sessions into host-only cookies, reads them back and checks return paths.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::http::header::{COOKIE, HeaderMap, HeaderValue};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use serde::{Deserialize, Serialize};

/// A visitor's own session, outside any portal preview.
pub const VISITOR: &str = "__Host-aruna-visitor";
/// A preview session of the creator or an approver; always partitioned.
pub const PREVIEW: &str = "__Host-aruna-preview";
/// The sign-in state that ties a callback to the browser that started it.
pub const STATE: &str = "__Host-aruna-state";
/// Set on top-level page loads; browsers never send it from a cross-site frame.
pub const TOP: &str = "__Host-aruna-top";
/// Every pod derives the same sealing key from the shared edge key.
const KEY_CONTEXT: &str = "aruna applet gateway 2026-11 session cookie";
const NONCE_BYTES: usize = 24;

/// The version or build a preview session looks at instead of the decision's version.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Pick {
    Version(u32),
    Build(String),
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Session {
    pub applet_id: String,
    /// The node's visitor token, bound to this one applet.
    pub token: String,
    pub preview: bool,
    /// Unix seconds.
    pub expires: u64,
    pub pick: Option<Pick>,
}

// Shows everything except the token.
impl fmt::Debug for Session {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Session")
            .field("applet_id", &self.applet_id)
            .field("preview", &self.preview)
            .field("expires", &self.expires)
            .field("pick", &self.pick)
            .finish_non_exhaustive()
    }
}

/// Seals and opens sessions; the host is the associated data, so a cookie opens only there.
pub struct Sealer {
    key: [u8; 32],
}

impl Sealer {
    pub fn new(edge_key: &str) -> Self {
        Self {
            key: blake3::derive_key(KEY_CONTEXT, edge_key.as_bytes()),
        }
    }

    pub fn seal(&self, host: &str, session: &Session) -> Option<String> {
        let plain = serde_json::to_vec(session).ok()?;
        let nonce: [u8; NONCE_BYTES] = rand::random();
        let payload = Payload {
            msg: &plain,
            aad: host.as_bytes(),
        };
        let sealed = self.cipher().encrypt(&XNonce::from(nonce), payload).ok()?;
        Some(URL_SAFE_NO_PAD.encode([nonce.as_slice(), &sealed].concat()))
    }

    /// The session in `text` when it was sealed for `host` and has not expired at `now`.
    pub fn open(&self, host: &str, text: &str, now: u64) -> Option<Session> {
        let bytes = URL_SAFE_NO_PAD.decode(text).ok()?;
        let (nonce, sealed) = bytes.split_at_checked(NONCE_BYTES)?;
        let nonce: [u8; NONCE_BYTES] = nonce.try_into().ok()?;
        let payload = Payload {
            msg: sealed,
            aad: host.as_bytes(),
        };
        let plain = self.cipher().decrypt(&XNonce::from(nonce), payload).ok()?;
        let session: Session = serde_json::from_slice(&plain).ok()?;
        (session.expires > now).then_some(session)
    }

    fn cipher(&self) -> XChaCha20Poly1305 {
        XChaCha20Poly1305::new((&self.key).into())
    }
}

/// How a cookie travels across sites.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Site {
    /// A top-level visit.
    Lax,
    /// Inside a frame: kept in the partition of the framing site only.
    Framed,
    Strict,
}

/// A `Set-Cookie` value; the `__Host-` names keep it host-only, so sibling applet hosts can
/// neither set nor overwrite it.
pub fn set_cookie(name: &str, value: &str, max_age: u64, site: Site) -> Option<HeaderValue> {
    let same_site = match site {
        Site::Lax => "SameSite=Lax",
        Site::Framed => "SameSite=None; Partitioned",
        Site::Strict => "SameSite=Strict",
    };
    let cookie =
        format!("{name}={value}; Path=/; Max-Age={max_age}; Secure; HttpOnly; {same_site}");
    HeaderValue::from_str(&cookie).ok()
}

/// Every value the request sends for cookie `name`.
pub fn cookies<'a>(headers: &'a HeaderMap, name: &'a str) -> impl Iterator<Item = &'a str> {
    (headers.get_all(COOKIE).iter())
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(';'))
        .filter_map(move |pair| {
            let (key, value) = pair.trim().split_once('=')?;
            (key == name).then_some(value)
        })
}

/// Whether the browser loads this response into a frame.
pub fn framed(headers: &HeaderMap) -> bool {
    headers
        .get("sec-fetch-dest")
        .is_some_and(|dest| dest == "iframe")
}

/// `raw` when it is a path on this host, else the applet root; it never leaves the host.
pub fn return_path(raw: Option<&str>) -> String {
    match raw {
        Some(path)
            if path.starts_with('/')
                && !path.starts_with("//")
                && path.len() <= 2048
                && path.bytes().all(|b| b.is_ascii_graphic() && b != b'\\') =>
        {
            path.to_string()
        }
        _ => "/".to_string(),
    }
}

/// Compares two secrets without stopping at the first difference.
pub fn same_secret(left: &str, right: &str) -> bool {
    let difference = (left.bytes().zip(right.bytes())).fold(0u8, |acc, (l, r)| acc | (l ^ r));
    left.len() == right.len() && difference == 0
}

pub fn now_seconds() -> u64 {
    (SystemTime::now().duration_since(UNIX_EPOCH)).map_or(0, |since| since.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOST: &str = "bakta.apps.test";

    fn session(expires: u64) -> Session {
        Session {
            applet_id: "01K8W1B3D5F7H9KMQ1S3V5X7Z2".into(),
            token: "visitor-token".into(),
            preview: true,
            expires,
            pick: Some(Pick::Build("01K8W1B3D5F7H9KMQ1S3V5X7Z3".into())),
        }
    }

    #[test]
    fn sealed_session_opens() {
        let sealer = Sealer::new("edge-secret");
        let text = sealer.seal(HOST, &session(100)).expect("sealed");
        assert!(!text.contains("visitor-token"));
        assert_eq!(sealer.open(HOST, &text, 99), Some(session(100)));
        assert_ne!(sealer.seal(HOST, &session(100)), Some(text));
    }

    // A changed byte, another host, another edge key or a passed expiry all fail to open.
    #[test]
    fn rejects_tampered_cookies() {
        let sealer = Sealer::new("edge-secret");
        let text = sealer.seal(HOST, &session(100)).expect("sealed");
        assert!(sealer.open("other.apps.test", &text, 0).is_none());
        assert!(Sealer::new("other-secret").open(HOST, &text, 0).is_none());
        assert!(sealer.open(HOST, &text, 100).is_none());
        let mut bytes = URL_SAFE_NO_PAD.decode(&text).expect("base64");
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        assert!(
            sealer
                .open(HOST, &URL_SAFE_NO_PAD.encode(&bytes), 0)
                .is_none()
        );
        assert!(sealer.open(HOST, "", 0).is_none());
        assert!(sealer.open(HOST, "not base64!", 0).is_none());
    }

    #[test]
    fn cookie_attributes_match() {
        let read = |site| set_cookie(VISITOR, "v", 60, site).expect("valid");
        let lax = read(Site::Lax);
        assert_eq!(
            lax,
            "__Host-aruna-visitor=v; Path=/; Max-Age=60; Secure; HttpOnly; SameSite=Lax"
        );
        let framed = read(Site::Framed);
        assert!(
            framed
                .to_str()
                .expect("ascii")
                .ends_with("SameSite=None; Partitioned")
        );
        let mut headers = HeaderMap::new();
        headers.insert(
            COOKIE,
            HeaderValue::from_static("a=1; __Host-aruna-visitor=x"),
        );
        headers.append(COOKIE, HeaderValue::from_static("__Host-aruna-visitor=y"));
        assert_eq!(cookies(&headers, VISITOR).collect::<Vec<_>>(), ["x", "y"]);
        assert_eq!(cookies(&headers, PREVIEW).count(), 0);
    }

    #[test]
    fn return_stays_local() {
        assert_eq!(return_path(Some("/runs/1?tab=a")), "/runs/1?tab=a");
        for bad in [
            "//evil.test",
            "/\\evil.test",
            "https://evil.test",
            "runs",
            "/a b",
            "",
        ] {
            assert_eq!(
                return_path(Some(bad)),
                "/",
                "{bad} must not be a return path"
            );
        }
        assert_eq!(return_path(None), "/");
        assert!(same_secret("abc", "abc"));
        assert!(!same_secret("abc", "abd") && !same_secret("abc", "abcd"));
    }
}
