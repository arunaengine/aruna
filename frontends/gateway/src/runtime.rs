//! Routes `/_aruna/` requests: sign-in, preview helpers and the proxied applet runtime calls.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::net::{IpAddr, SocketAddr};

use axum::body::Body;
use axum::extract::{ConnectInfo, Request};
use axum::http::header::{
    ACCEPT, CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE, ETAG, HeaderMap, HeaderValue, SET_COOKIE,
};
use axum::http::{Method, StatusCode};
use axum::response::Response;
use tracing::warn;

use crate::pages::{bare, unavailable};
use crate::session::{
    GUEST, GUEST_TTL_SECS, GuestSession, Site, TOP, cookies, framed, new_guest_id, now_seconds,
    set_cookie,
};
use crate::upstream::Serving;
use crate::visit::{Visit, plain_id};
use crate::{Gateway, policy, preview, signin};

/// The runtime routes an applet page may call on its own origin.
const ROUTES: [&str; 4] = ["session", "uploads", "submissions", "runs"];

pub async fn route(gateway: &Gateway, visit: &Visit, rest: &str, request: Request) -> Response {
    let query = request.uri().query();
    let headers = request.headers();
    match rest {
        "auth/start" => signin::start(visit, headers, query),
        "auth/callback" => signin::callback(gateway, visit, headers, query).await,
        "auth/logout" => signin::logout(visit),
        "auth/preview-frame" => signin::preview_frame(visit),
        "auth/preview-frame.js" => signin::preview_script(visit),
        "auth/popup.js" => signin::popup_script(visit),
        "auth/popup-done.js" => signin::done_script(visit),
        "preview/select" => preview::select(gateway, visit, query),
        "preview-helper.js" => preview::helper(visit),
        "preview-source-map.json" => preview::source_map(gateway, visit).await,
        _ if runtime_path(rest) => proxy(gateway, visit, rest, request).await,
        _ => bare(StatusCode::NOT_FOUND, visit.frame()),
    }
}

fn runtime_path(rest: &str) -> bool {
    let parts: Vec<&str> = rest.split('/').collect();
    match parts.as_slice() {
        [route] => ROUTES.contains(route),
        ["runs", run] | ["runs", run, "results"] => plain_id(run),
        _ => false,
    }
}

/// Forwards one runtime call with the edge key and the visitor's token. The node decides each
/// call; these checks only stop what it would refuse anyway.
async fn proxy(gateway: &Gateway, visit: &Visit, rest: &str, request: Request) -> Response {
    let frame = visit.frame();
    let decision = &visit.answer.decision;
    if !matches!(decision.serving, Serving::Live | Serving::Preview) {
        return bare(StatusCode::SERVICE_UNAVAILABLE, frame);
    }
    let headers = request.headers();
    let reads = matches!(*request.method(), Method::GET | Method::HEAD);
    if !reads && !same_origin(headers, &visit.host) {
        return bare(StatusCode::FORBIDDEN, frame);
    }
    let top = cookies(headers, TOP).next().is_some();
    let token = visit.token();
    if token.is_none() && !decision.guests {
        return bare(StatusCode::UNAUTHORIZED, frame);
    }
    // A Strict cookie never travels from a cross-site frame, so a guest there lacks it.
    if token.is_none() && !decision.guests_in_frames && !top {
        return bare(StatusCode::FORBIDDEN, frame);
    }
    let Some(applet) = visit.applet_route() else {
        return unavailable(frame);
    };
    let path = match request.uri().query() {
        Some(query) => format!("{applet}/{rest}?{query}"),
        None => format!("{applet}/{rest}"),
    };
    let method = request.method().clone();
    let mut sent = (gateway.upstream.request(visit.answer.node, method, &path))
        .header("x-aruna-host", &visit.host)
        .header("x-aruna-top", if top { "1" } else { "0" });
    let mut guest_cookie: Option<HeaderValue> = None;
    if let Some(token) = token {
        sent = sent.header("x-aruna-visitor", token);
    } else {
        let now = now_seconds();
        let applet_id = decision.applet_id.as_str();
        let guest_id = cookies(headers, GUEST)
            .find_map(|text| gateway.sealer.open_guest(&visit.host, text, now))
            .filter(|guest| guest.applet_id == applet_id)
            .map(|guest| guest.id)
            .unwrap_or_else(|| {
                let id = new_guest_id();
                let guest = GuestSession {
                    applet_id: applet_id.to_owned(),
                    id: id.clone(),
                    expires: now.saturating_add(GUEST_TTL_SECS),
                };
                if let Some(sealed) = gateway.sealer.seal_guest(&visit.host, &guest) {
                    let site = match framed(headers) || !top {
                        true => Site::Framed,
                        false => Site::Lax,
                    };
                    guest_cookie = set_cookie(GUEST, &sealed, GUEST_TTL_SECS, site);
                }
                id
            });
        sent = sent.header("x-aruna-guest", guest_id);
    }
    if let Some(client) = client_ip(gateway, &request) {
        sent = sent.header("x-forwarded-for", client.to_string());
    }
    for name in [CONTENT_TYPE, CONTENT_LENGTH, ACCEPT] {
        if let Some(value) = headers.get(&name) {
            sent = sent.header(name, value);
        }
    }
    let body = reqwest::Body::wrap_stream(request.into_body().into_data_stream());
    let upstream = match sent.body(body).send().await {
        Ok(upstream) => upstream,
        Err(error) => {
            warn!(%error, host = %visit.host, "an applet runtime call failed");
            return unavailable(frame);
        }
    };
    let mut response = Response::new(Body::empty());
    *response.status_mut() = upstream.status();
    let out = response.headers_mut();
    for name in [CONTENT_TYPE, CONTENT_LENGTH, CACHE_CONTROL, ETAG] {
        if let Some(value) = upstream.headers().get(&name) {
            out.insert(name, value.clone());
        }
    }
    if !out.contains_key(CACHE_CONTROL) {
        out.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    }
    if let Some(cookie) = guest_cookie {
        out.append(SET_COOKIE, cookie);
    }
    policy::secure(out, frame);
    *response.body_mut() = Body::from_stream(upstream.bytes_stream());
    response
}

/// A state-changing call must come from the applet's own page.
fn same_origin(headers: &HeaderMap, host: &str) -> bool {
    let site = headers.get("sec-fetch-site");
    let origin = headers
        .get("origin")
        .and_then(|origin| origin.to_str().ok());
    match (site, origin) {
        (Some(site), _) => site == "same-origin",
        (None, Some(origin)) => origin == format!("https://{host}"),
        (None, None) => false,
    }
}

/// The client: the socket peer, or the last hop the trusted proxy added.
fn client_ip(gateway: &Gateway, request: &Request) -> Option<IpAddr> {
    let peer = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()?
        .0
        .ip();
    if gateway.settings.trusted_proxy != Some(peer) {
        return Some(peer);
    }
    let forwarded = request.headers().get("x-forwarded-for")?.to_str().ok()?;
    forwarded
        .rsplit(',')
        .next()?
        .trim()
        .parse()
        .ok()
        .or(Some(peer))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_paths_allowed() {
        for path in [
            "session",
            "uploads",
            "submissions",
            "runs",
            "runs/r-1",
            "runs/r_1/results",
        ] {
            assert!(runtime_path(path), "{path} is a runtime route");
        }
        for path in [
            "",
            "admin",
            "runs/",
            "runs/../x",
            "runs/a/b",
            "runs/a/results/x",
            "session/x",
        ] {
            assert!(!runtime_path(path), "{path} is no runtime route");
        }
    }

    #[test]
    fn writes_need_origin() {
        let mut headers = HeaderMap::new();
        assert!(!same_origin(&headers, "a.apps.test"));
        headers.insert("origin", HeaderValue::from_static("https://a.apps.test"));
        assert!(same_origin(&headers, "a.apps.test"));
        assert!(!same_origin(&headers, "b.apps.test"));
        headers.insert("sec-fetch-site", HeaderValue::from_static("cross-site"));
        assert!(!same_origin(&headers, "a.apps.test"));
    }
}
