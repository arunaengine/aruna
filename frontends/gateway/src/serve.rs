//! Serves an applet's pinned files with their browser policy, or the page that says why not.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use axum::body::Body;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::http::header::{
    ACCEPT, CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE, ETAG, HeaderMap, HeaderValue,
    IF_NONE_MATCH, SET_COOKIE, VARY,
};
use axum::response::Response;
use reqwest::Method;
use tracing::warn;

use crate::Gateway;
use crate::pages::{not_found, page, redirect, unavailable};
use crate::policy;
use crate::preview::{SOURCE_MAP, inject};
use crate::session::{Site, TOP, set_cookie};
use crate::upstream::{Access, Serving, Upstream};
use crate::visit::Visit;

/// Bytes of an HTML file the gateway reads whole to add the preview helper.
const MAX_HTML: usize = 16 << 20;
/// How long the top-level marker lives; any top-level page load renews it.
const TOP_SECONDS: u64 = 86_400;

pub async fn file(gateway: &Gateway, visit: &Visit, request: Request) -> Response {
    let decision = &visit.answer.decision;
    let frame = visit.frame();
    let (status, title, message) = match decision.serving {
        Serving::Live | Serving::Preview => (StatusCode::OK, "", ""),
        Serving::Gone => (
            StatusCode::GONE,
            "This applet was taken down",
            "It no longer serves any content.",
        ),
        Serving::Paused => (
            StatusCode::SERVICE_UNAVAILABLE,
            "This applet is paused",
            "Its owners paused it for now. Try again later.",
        ),
        Serving::FeaturePaused => (
            StatusCode::SERVICE_UNAVAILABLE,
            "Applets are paused",
            "Applets are switched off here for now. Try again later.",
        ),
    };
    if status != StatusCode::OK {
        return page(status, title, message, frame);
    }
    let path = request.uri().path();
    let headers = request.headers();
    if visit.preview.is_none()
        && path
            .trim_start_matches('/')
            .eq_ignore_ascii_case(SOURCE_MAP)
    {
        return not_found(frame);
    }
    let restricted = decision.serving == Serving::Preview || decision.access != Access::Public;
    if restricted && visit.token().is_none() {
        return signin_needed(visit, &request);
    }
    let Some(route) = visit.files_route() else {
        let message = "This applet has no version to show yet.";
        return page(
            StatusCode::NOT_FOUND,
            "Nothing released yet",
            message,
            frame,
        );
    };
    let mut fetched = fetch(gateway, visit, &route, path).await;
    // Client-side routes have no file of their own; the app's index page renders them.
    let last = path.rsplit('/').next().unwrap_or_default();
    let fallback = navigation(headers) && !last.contains('.');
    if fallback && matches!(&fetched, Ok(r) if r.status() == StatusCode::NOT_FOUND) {
        fetched = fetch(gateway, visit, &route, "/").await;
    }
    let upstream = match fetched {
        Ok(upstream) => upstream,
        Err(error) => {
            warn!(%error, host = %visit.host, "reading an applet file failed");
            return unavailable(frame);
        }
    };
    match upstream.status() {
        status if status.is_success() => respond(gateway, visit, headers, path, upstream).await,
        StatusCode::FORBIDDEN | StatusCode::UNAUTHORIZED if visit.token().is_none() => {
            signin_needed(visit, &request)
        }
        StatusCode::FORBIDDEN => {
            let message = "Your account may not open this applet.";
            page(
                StatusCode::FORBIDDEN,
                "Not available to you",
                message,
                frame,
            )
        }
        StatusCode::NOT_FOUND => not_found(frame),
        status => {
            warn!(%status, host = %visit.host, "the node refused an applet file");
            unavailable(frame)
        }
    }
}

async fn fetch(
    gateway: &Gateway,
    visit: &Visit,
    route: &str,
    path: &str,
) -> Result<reqwest::Response, reqwest::Error> {
    let file = Upstream::with_query(&format!("{route}/file"), &[("path", path)]);
    let mut sent = gateway
        .upstream
        .request(visit.answer.node, Method::GET, &file);
    if let Some(token) = visit.token() {
        sent = sent.header("x-aruna-visitor", token);
    }
    sent.send().await
}

async fn respond(
    gateway: &Gateway,
    visit: &Visit,
    headers: &HeaderMap,
    path: &str,
    mut upstream: reqwest::Response,
) -> Response {
    let frame = visit.frame();
    let passed = |name| upstream.headers().get(&name).cloned();
    let (kind, tag, length) = (passed(CONTENT_TYPE), passed(ETAG), passed(CONTENT_LENGTH));
    let html = (kind.as_ref()).is_some_and(|kind| kind.as_bytes().starts_with(b"text/html"));
    let portal = visit
        .portal
        .as_deref()
        .filter(|_| visit.preview.is_some() && html);
    let unchanged = (tag.as_ref()).is_some_and(|tag| matches_tag(headers, tag));
    let mut response = match portal {
        Some(portal) => {
            let mut bytes = Vec::new();
            loop {
                match upstream.chunk().await {
                    Ok(Some(chunk)) if bytes.len() + chunk.len() <= MAX_HTML => {
                        bytes.extend_from_slice(&chunk)
                    }
                    Ok(None) => break,
                    _ => return unavailable(frame),
                }
            }
            Response::new(Body::from(inject(&bytes, portal)))
        }
        None if unchanged => {
            let mut response = Response::new(Body::empty());
            *response.status_mut() = StatusCode::NOT_MODIFIED;
            response
        }
        None => {
            let mut response = Response::new(Body::from_stream(upstream.bytes_stream()));
            if let Some(length) = length {
                response.headers_mut().insert(CONTENT_LENGTH, length);
            }
            response
        }
    };
    let out = response.headers_mut();
    if let Some(kind) = kind {
        out.insert(CONTENT_TYPE, kind);
    }
    let caching = match portal {
        Some(_) => "no-store",
        None => policy::caching(path, visit.shared()),
    };
    out.insert(CACHE_CONTROL, HeaderValue::from_static(caching));
    if let Some(tag) = tag.filter(|_| portal.is_none()) {
        out.insert(ETAG, tag);
    }
    if !visit.shared() {
        out.insert(VARY, HeaderValue::from_static("Cookie"));
    }
    let decision = &visit.answer.decision;
    let top_load = headers
        .get("sec-fetch-dest")
        .is_some_and(|dest| dest == "document");
    let marked = decision.guests && !decision.guests_in_frames && top_load && html;
    if let Some(marker) = set_cookie(TOP, "1", TOP_SECONDS, Site::Strict).filter(|_| marked) {
        out.append(SET_COOKIE, marker);
        if portal.is_none() {
            out.insert(CACHE_CONTROL, HeaderValue::from_static("private, no-cache"));
        }
    }
    // Guest-enabled HTML never enters shared caches so new visitors always hit the gateway to mint
    // their host-bound guest cookie before concurrent runtime calls.
    if decision.guests && html {
        if portal.is_none() {
            out.insert(CACHE_CONTROL, HeaderValue::from_static("private, no-cache"));
        }
        out.insert(VARY, HeaderValue::from_static("Cookie"));
        if visit.token().is_none() {
            let (_, guest_cookie) = crate::runtime::ensure_guest(gateway, visit, headers, top_load);
            if let Some(cookie) = guest_cookie {
                out.append(SET_COOKIE, cookie);
            }
        }
    }
    policy::secure(out, frame);
    response
}

/// Sends a page load to sign-in; other requests only learn that a session is missing.
fn signin_needed(visit: &Visit, request: &Request) -> Response {
    let decision = &visit.answer.decision;
    let frame = visit.frame();
    if decision.custom_domain || visit.portal.is_none() {
        let message = "Open this applet at its own Aruna address to sign in.";
        return page(StatusCode::UNAUTHORIZED, "Sign-in needed", message, frame);
    }
    if !navigation(request.headers()) {
        let message = "This applet needs a signed-in visitor.";
        return page(StatusCode::UNAUTHORIZED, "Sign-in needed", message, frame);
    }
    let back = request.uri().path_and_query().map_or("/", |p| p.as_str());
    let mut start = url::form_urlencoded::Serializer::new(String::new());
    start.append_pair("return", back);
    if decision.serving == Serving::Preview {
        start.append_pair("preview", "1");
    }
    let location = format!("/_aruna/auth/start?{}", start.finish());
    redirect(StatusCode::FOUND, &location, frame)
}

/// Whether the browser navigates to this response rather than loading a part of a page.
fn navigation(headers: &HeaderMap) -> bool {
    match headers.get("sec-fetch-mode") {
        Some(mode) => mode == "navigate",
        None => (headers.get(ACCEPT))
            .and_then(|accept| accept.to_str().ok())
            .is_some_and(|accept| accept.contains("text/html")),
    }
}

fn matches_tag(headers: &HeaderMap, tag: &HeaderValue) -> bool {
    (headers.get_all(IF_NONE_MATCH).iter())
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|candidate| {
            let candidate = candidate.trim();
            candidate == "*" || candidate.trim_start_matches("W/").as_bytes() == tag.as_bytes()
        })
}
