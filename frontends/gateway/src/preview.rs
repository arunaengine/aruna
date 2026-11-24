//! Preview sessions only: the injected inspect and variant helper, the source map and the pick.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use axum::body::Body;
use axum::http::StatusCode;
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE, HeaderValue, SET_COOKIE};
use axum::response::Response;
use reqwest::Method;
use tracing::warn;

use crate::Gateway;
use crate::pages::{bare, not_found, redirect, unavailable};
use crate::policy;
use crate::session::{PREVIEW, Pick, Session, Site, now_seconds, set_cookie};
use crate::upstream::Upstream;
use crate::visit::{Visit, plain_id, query_params};

/// The helper is served by the gateway and never stored with an applet's files.
const HELPER: &str = include_str!("preview.js");
/// The builder's side file; outside a preview session it never leaves the gateway.
pub const SOURCE_MAP: &str = "aruna-source-map.json";

/// `html` with the helper script before `</head>`, else before `</body>`, else at the end.
pub fn inject(html: &[u8], portal: &str) -> Vec<u8> {
    let portal = (portal.replace('&', "&amp;").replace('"', "&quot;"))
        .replace('<', "&lt;")
        .replace('>', "&gt;");
    let tag = format!(
        r#"<script src="/_aruna/preview-helper.js" data-portal="{portal}" defer></script>"#
    );
    let lower = html.to_ascii_lowercase();
    let find = |needle: &[u8]| lower.windows(needle.len()).position(|w| w == needle);
    let at = find(b"</head>")
        .or_else(|| find(b"</body>"))
        .unwrap_or(html.len());
    [&html[..at], tag.as_bytes(), &html[at..]].concat()
}

pub fn helper(visit: &Visit) -> Response {
    if visit.preview.is_none() {
        return not_found(visit.frame());
    }
    let mut response = Response::new(Body::from(HELPER));
    let headers = response.headers_mut();
    let script = HeaderValue::from_static("text/javascript; charset=utf-8");
    headers.insert(CONTENT_TYPE, script);
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    policy::secure(headers, visit.frame());
    response
}

/// The source map of the previewed files: the node's own route first, then the side file.
pub async fn source_map(gateway: &Gateway, visit: &Visit) -> Response {
    let frame = visit.frame();
    let (Some(token), Some(route), Some(_)) = (visit.token(), visit.files_route(), &visit.preview)
    else {
        return not_found(frame);
    };
    let side_file = Upstream::with_query(&format!("{route}/file"), &[("path", SOURCE_MAP)]);
    for path in [format!("{route}/source-map"), side_file] {
        let sent = (gateway
            .upstream
            .request(visit.answer.node, Method::GET, &path))
        .header("x-aruna-visitor", token);
        let response = match sent.send().await {
            Ok(response) if response.status() == StatusCode::NOT_FOUND => continue,
            Ok(response) if response.status().is_success() => response,
            Ok(response) => return bare(response.status(), frame),
            Err(error) => {
                warn!(%error, "reading the preview source map failed");
                return unavailable(frame);
            }
        };
        let mut reply = Response::new(Body::from_stream(response.bytes_stream()));
        let headers = reply.headers_mut();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
        policy::secure(headers, frame);
        return reply;
    }
    not_found(frame)
}

/// Parses an optional `version` or `build` preview pick from query parameters.
pub(crate) fn parse_pick(
    params: &std::collections::HashMap<String, String>,
) -> Result<Option<Pick>, ()> {
    let version = params.get("version").map(String::as_str);
    match (version, params.get("build")) {
        (Some(text), None) if text.len() <= 10 && text.bytes().all(|b| b.is_ascii_digit()) => text
            .parse()
            .map(|version| Some(Pick::Version(version)))
            .map_err(|_| ()),
        (None, Some(build)) if plain_id(build) => Ok(Some(Pick::Build(build.clone()))),
        (None, None) => Ok(None),
        _ => Err(()),
    }
}

/// Reseals the preview session with the version or build to look at, or with none.
pub fn select(gateway: &Gateway, visit: &Visit, query: Option<&str>) -> Response {
    let frame = visit.frame();
    let Some(session) = &visit.preview else {
        return not_found(frame);
    };
    let params = query_params(query);
    let Ok(pick) = parse_pick(&params) else {
        return bare(StatusCode::BAD_REQUEST, frame);
    };
    let session = Session {
        pick,
        ..session.clone()
    };
    let max_age = session.expires.saturating_sub(now_seconds());
    let cookie = (gateway.sealer.seal(&visit.host, &session))
        .and_then(|sealed| set_cookie(PREVIEW, &sealed, max_age, Site::Framed));
    let Some(cookie) = cookie else {
        return bare(StatusCode::INTERNAL_SERVER_ERROR, frame);
    };
    let mut response = redirect(StatusCode::SEE_OTHER, "/", frame);
    response.headers_mut().append(SET_COOKIE, cookie);
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    const TAG: &str = r#"<script src="/_aruna/preview-helper.js" data-portal="https://portal.test" defer></script>"#;

    #[test]
    fn inject_before_head() {
        let html = b"<html><HEAD><title>x</title></Head><body></body></html>";
        let expected = format!("<html><HEAD><title>x</title>{TAG}</Head><body></body></html>");
        assert_eq!(inject(html, "https://portal.test"), expected.as_bytes());
        let body_only = b"<p>hi</p></body>";
        assert_eq!(
            inject(body_only, "https://portal.test"),
            format!("<p>hi</p>{TAG}</body>").as_bytes()
        );
        assert_eq!(
            inject(b"<p>hi</p>", "https://portal.test"),
            format!("<p>hi</p>{TAG}").as_bytes()
        );
    }

    #[test]
    fn inject_escapes_portal() {
        let injected = inject(b"", "https://p.test\"><script>");
        let text = String::from_utf8(injected).expect("utf-8");
        assert!(text.contains("data-portal=\"https://p.test&quot;&gt;&lt;script&gt;\""));
    }
}
