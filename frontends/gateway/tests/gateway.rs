//! Runs the gateway against a fake node on loopback ports: serving, sign-in and previews.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::time::{SystemTime, UNIX_EPOCH};

use aruna_gateway::settings::Settings;
use aruna_gateway::{Gateway, router};
use axum::extract::{Path, Query};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use reqwest::header::{CACHE_CONTROL, CONTENT_SECURITY_POLICY, LOCATION, SET_COOKIE};

const KEY: &str = "edge-secret";
const PUBLIC_ID: &str = "01K8W1B3D5F7H9KMQ1S3V5X7Z2";
const PRIVATE_ID: &str = "01K8W1B3D5F7H9KMQ1S3V5X7Z3";
const PAGE: &str = "<html><head><title>t</title></head><body>hi</body></html>";

async fn host(headers: HeaderMap, Path(host): Path<String>) -> axum::response::Response {
    if headers.get("x-aruna-edge-key").is_none_or(|key| key != KEY) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let (id, access, serving, guests) = match host.as_str() {
        "pub.apps.test" => (PUBLIC_ID, "public", "live", false),
        "guest.apps.test" => (PUBLIC_ID, "public", "live", true),
        "private.apps.test" => (PRIVATE_ID, "private", "live", false),
        "gone.apps.test" => (PUBLIC_ID, "public", "gone", false),
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    Json(serde_json::json!({
        "applet_id": id, "group_id": "g", "host_node": "n", "custom_domain": false,
        "serving": serving, "version": 1, "access": access, "frame_ancestors": [],
        "guests": guests, "guests_in_frames": false,
        "portal_origin": "https://portal.test", "max_age_s": 30
    }))
    .into_response()
}

async fn file(
    headers: HeaderMap,
    Path((id, _version)): Path<(String, u32)>,
    Query(query): Query<HashMap<String, String>>,
) -> axum::response::Response {
    if id == PRIVATE_ID
        && headers
            .get("x-aruna-visitor")
            .is_none_or(|t| t != "visitor-token")
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    let (kind, body) = match query.get("path").map(String::as_str) {
        Some("/" | "/index.html") => ("text/html; charset=utf-8", PAGE),
        Some("/assets/app-B2x9cQ4f.js") => ("text/javascript", "console.log(1)"),
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    ([("content-type", kind), ("etag", "\"abc\"")], body).into_response()
}

async fn redeem(Json(body): Json<serde_json::Value>) -> axum::response::Response {
    let expires = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs()
        + 3600;
    let preview = match body["code"].as_str() {
        Some("good") => false,
        Some("preview") => true,
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    Json(serde_json::json!({
        "applet_id": PRIVATE_ID, "visitor_token": "visitor-token", "expires_at": expires,
        "preview": preview, "portal_origin": "https://portal.test"
    }))
    .into_response()
}

async fn serve(app: Router) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("address");
    let service = app.into_make_service_with_connect_info::<SocketAddr>();
    tokio::spawn(async move { axum::serve(listener, service).await });
    address
}

/// A fake node and a gateway in front of it; returns the gateway's base URL.
async fn start() -> String {
    let node = Router::new()
        .route("/edge/v1/hosts/{host}", get(host))
        .route("/edge/v1/applets/{id}/versions/{version}/file", get(file))
        .route("/edge/v1/signin/redeem", post(redeem));
    let node = serve(node).await;
    let settings = Settings {
        edge_key: KEY.to_string(),
        upstreams: vec![format!("http://{node}")],
        listen: "127.0.0.1:0".parse().expect("address"),
        trusted_proxy: None,
        host_cache: NonZeroUsize::new(16).expect("non-zero"),
    };
    let gateway = Gateway::new(settings).expect("gateway");
    format!("http://{}", serve(router(gateway)).await)
}

fn client() -> reqwest::Client {
    let none = reqwest::redirect::Policy::none();
    reqwest::Client::builder()
        .redirect(none)
        .build()
        .expect("client")
}

async fn visit(base: &str, host: &str, path: &str, extra: &[(&str, &str)]) -> reqwest::Response {
    let mut sent = client().get(format!("{base}{path}")).header("host", host);
    for (name, value) in extra {
        sent = sent.header(*name, *value);
    }
    sent.send().await.expect("gateway answers")
}

fn header(response: &reqwest::Response, name: impl reqwest::header::AsHeaderName) -> String {
    let value = response
        .headers()
        .get(name)
        .map(|v| v.to_str().expect("ascii"));
    value.unwrap_or_default().to_string()
}

/// The `name=value` part of the response cookie `name`.
fn cookie(response: &reqwest::Response, name: &str) -> String {
    (response.headers().get_all(SET_COOKIE).iter())
        .map(|value| value.to_str().expect("ascii"))
        .find(|value| value.starts_with(&format!("{name}=")) && !value.contains("Max-Age=0"))
        .map(|value| value.split(';').next().unwrap_or_default().to_string())
        .unwrap_or_default()
}

#[tokio::test]
async fn public_files_served() {
    let base = start().await;
    let page = visit(&base, "pub.apps.test", "/", &[]).await;
    assert_eq!(page.status(), StatusCode::OK);
    assert!(header(&page, CONTENT_SECURITY_POLICY).ends_with("frame-ancestors 'none'"));
    assert_eq!(header(&page, "x-frame-options"), "DENY");
    assert_eq!(header(&page, CACHE_CONTROL), "public, no-cache");
    assert_eq!(page.text().await.expect("body"), PAGE);
    let cached = visit(&base, "pub.apps.test", "/", &[("if-none-match", "\"abc\"")]).await;
    assert_eq!(cached.status(), StatusCode::NOT_MODIFIED);
    let asset = visit(&base, "pub.apps.test", "/assets/app-B2x9cQ4f.js", &[]).await;
    assert_eq!(
        header(&asset, CACHE_CONTROL),
        "public, max-age=31536000, immutable"
    );
    let route = visit(
        &base,
        "pub.apps.test",
        "/runs/1",
        &[("sec-fetch-mode", "navigate")],
    )
    .await;
    assert_eq!(route.text().await.expect("body"), PAGE);
    let hidden = visit(&base, "pub.apps.test", "/aruna-source-map.json", &[]).await;
    assert_eq!(hidden.status(), StatusCode::NOT_FOUND);
    let helper = visit(&base, "pub.apps.test", "/_aruna/preview-helper.js", &[]).await;
    assert_eq!(helper.status(), StatusCode::NOT_FOUND);
    let unknown = visit(&base, "nobody.apps.test", "/", &[]).await;
    assert_eq!(unknown.status(), StatusCode::NOT_FOUND);
    let gone = visit(&base, "gone.apps.test", "/", &[]).await;
    assert_eq!(gone.status(), StatusCode::GONE);
    let check = |domain: &str| format!("/_edge/tls-check?domain={domain}");
    let known = visit(&base, "any.test", &check("pub.apps.test"), &[]).await;
    assert_eq!(known.status(), StatusCode::OK);
    let missing = visit(&base, "any.test", &check("nobody.apps.test"), &[]).await;
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
}

/// Signs in on the private host with `code` and returns the session cookie.
async fn sign_in(base: &str, code: &str, name: &str) -> String {
    let navigate = [("sec-fetch-mode", "navigate")];
    let page = visit(base, "private.apps.test", "/", &navigate).await;
    assert_eq!(page.status(), StatusCode::FOUND);
    let start = header(&page, LOCATION);
    assert_eq!(start, "/_aruna/auth/start?return=%2F");
    let started = visit(base, "private.apps.test", &start, &[]).await;
    let portal = url::Url::parse(&header(&started, LOCATION)).expect("portal URL");
    assert_eq!(portal.path(), "/applets/signin");
    let params: HashMap<_, _> = portal.query_pairs().into_owned().collect();
    assert_eq!(params["applet"], PRIVATE_ID);
    assert_eq!(params["host"], "private.apps.test");
    assert_eq!(params["node"], "n");
    let state = cookie(&started, "__Host-aruna-state");
    let callback =
        |state: &str| format!("/_aruna/auth/callback?code={code}&state={state}&return=/");
    let forged = visit(
        base,
        "private.apps.test",
        &callback("forged"),
        &[("cookie", &state)],
    )
    .await;
    assert_eq!(forged.status(), StatusCode::BAD_REQUEST);
    let callback = callback(&params["state"]);
    let back = visit(base, "private.apps.test", &callback, &[("cookie", &state)]).await;
    assert_eq!(back.status(), StatusCode::SEE_OTHER);
    assert_eq!(header(&back, LOCATION), "/");
    cookie(&back, name)
}

#[tokio::test]
async fn private_needs_signin() {
    let base = start().await;
    let session = sign_in(&base, "good", "__Host-aruna-visitor").await;
    assert!(!session.is_empty());
    let page = visit(&base, "private.apps.test", "/", &[("cookie", &session)]).await;
    assert_eq!(page.status(), StatusCode::OK);
    assert_eq!(header(&page, CACHE_CONTROL), "private, no-cache");
    assert_eq!(header(&page, "x-frame-options"), "DENY");
    let elsewhere = visit(&base, "pub.apps.test", "/", &[("cookie", &session)]).await;
    assert_eq!(elsewhere.status(), StatusCode::OK);
    let with = [("cookie", session.as_str())];
    let helper = visit(
        &base,
        "private.apps.test",
        "/_aruna/preview-helper.js",
        &with,
    )
    .await;
    assert_eq!(helper.status(), StatusCode::NOT_FOUND);
    let write = client()
        .post(format!("{base}/_aruna/runs"))
        .header("host", "private.apps.test");
    let write = write
        .header("cookie", &session)
        .send()
        .await
        .expect("answer");
    assert_eq!(write.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn preview_frames_portal() {
    let base = start().await;
    let session = sign_in(&base, "preview", "__Host-aruna-preview").await;
    let with = [("cookie", session.as_str())];
    let page = visit(&base, "private.apps.test", "/", &with).await;
    assert!(
        header(&page, CONTENT_SECURITY_POLICY).ends_with("frame-ancestors https://portal.test")
    );
    assert!(page.headers().get("x-frame-options").is_none());
    assert_eq!(header(&page, CACHE_CONTROL), "no-store");
    let body = page.text().await.expect("body");
    assert!(body.contains(r#"src="/_aruna/preview-helper.js" data-portal="https://portal.test""#));
    let helper = visit(
        &base,
        "private.apps.test",
        "/_aruna/preview-helper.js",
        &with,
    )
    .await;
    assert_eq!(helper.status(), StatusCode::OK);
    let bad = visit(
        &base,
        "private.apps.test",
        "/_aruna/preview/select?version=+1",
        &with,
    )
    .await;
    assert_eq!(bad.status(), StatusCode::BAD_REQUEST);
    let picked = visit(
        &base,
        "private.apps.test",
        "/_aruna/preview/select?version=1",
        &with,
    )
    .await;
    assert_eq!(picked.status(), StatusCode::SEE_OTHER);
    let resealed = cookie(&picked, "__Host-aruna-preview");
    assert!(!resealed.is_empty() && resealed != session);
    let unsigned = visit(
        &base,
        "private.apps.test",
        "/_aruna/preview/select?version=1",
        &[],
    )
    .await;
    assert_eq!(unsigned.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn preview_frame_handshake() {
    let base = start().await;
    let frame = visit(
        &base,
        "private.apps.test",
        "/_aruna/auth/preview-frame",
        &[("sec-fetch-dest", "iframe")],
    )
    .await;
    assert_eq!(frame.status(), StatusCode::OK);
    assert!(
        header(&frame, CONTENT_SECURITY_POLICY).ends_with("frame-ancestors https://portal.test")
    );
    assert!(frame.headers().get("x-frame-options").is_none());
    let raw_cookie = header(&frame, SET_COOKIE);
    assert!(raw_cookie.contains("SameSite=None; Partitioned"));
    let state_cookie = cookie(&frame, "__Host-aruna-state");
    let state_value = state_cookie
        .strip_prefix("__Host-aruna-state=")
        .expect("state cookie");
    let html = frame.text().await.expect("html");
    assert!(html.contains("/_aruna/auth/preview-frame.js"));
    assert!(html.contains(&format!(r#"data-state="{state_value}""#)));
    assert!(html.contains(r#"data-node="n""#));
    let script = visit(
        &base,
        "private.apps.test",
        "/_aruna/auth/preview-frame.js",
        &[],
    )
    .await;
    assert_eq!(script.status(), StatusCode::OK);
    assert!(
        script
            .text()
            .await
            .expect("js")
            .contains("aruna:preview-signin")
    );
    let callback =
        format!("/_aruna/auth/callback?code=preview&state={state_value}&return=/&version=1");
    let back = visit(
        &base,
        "private.apps.test",
        &callback,
        &[("cookie", &state_cookie), ("sec-fetch-dest", "iframe")],
    )
    .await;
    assert_eq!(back.status(), StatusCode::SEE_OTHER);
    assert_eq!(header(&back, LOCATION), "/");
    let preview = cookie(&back, "__Host-aruna-preview");
    assert!(!preview.is_empty());
}

#[tokio::test]
async fn embedded_popup_signin() {
    let base = start().await;
    let launcher = visit(
        &base,
        "private.apps.test",
        "/_aruna/auth/start?return=/runs",
        &[("sec-fetch-dest", "iframe")],
    )
    .await;
    assert_eq!(launcher.status(), StatusCode::OK);
    let raw_frame = header(&launcher, SET_COOKIE);
    assert!(raw_frame.contains("SameSite=None; Partitioned"));
    let frame_cookie = cookie(&launcher, "__Host-aruna-state");
    let state = frame_cookie
        .strip_prefix("__Host-aruna-state=")
        .expect("frame state");
    let html = launcher.text().await.expect("launcher html");
    assert!(html.contains("/_aruna/auth/popup.js"));
    assert!(html.contains(&format!(r#"data-state="{state}""#)));

    let popup_start = format!("/_aruna/auth/start?popup=1&state={state}&return=/runs");
    let redirect = visit(&base, "private.apps.test", &popup_start, &[]).await;
    assert_eq!(redirect.status(), StatusCode::FOUND);
    assert_eq!(
        header(&redirect, "cross-origin-opener-policy"),
        "unsafe-none"
    );
    let portal = url::Url::parse(&header(&redirect, LOCATION)).expect("portal URL");
    let params: HashMap<_, _> = portal.query_pairs().into_owned().collect();
    assert_eq!(params["state"], state);
    assert_eq!(params["node"], "n");
    assert_eq!(params["preview"], "0");
    let popup_cookie = cookie(&redirect, "__Host-aruna-state");

    let cb = format!("/_aruna/auth/callback?code=good&state={state}&return=/runs");
    let handoff = visit(
        &base,
        "private.apps.test",
        &cb,
        &[("cookie", &popup_cookie)],
    )
    .await;
    assert_eq!(handoff.status(), StatusCode::OK);
    assert_eq!(
        header(&handoff, "cross-origin-opener-policy"),
        "unsafe-none"
    );
    assert!(cookie(&handoff, "__Host-aruna-visitor").is_empty());
    let handoff_body = handoff.text().await.expect("handoff html");
    assert!(handoff_body.contains("/_aruna/auth/popup-done.js"));
    assert!(handoff_body.contains(r#"data-code="good""#));

    let redeemed = visit(
        &base,
        "private.apps.test",
        &cb,
        &[("cookie", &frame_cookie), ("sec-fetch-dest", "iframe")],
    )
    .await;
    assert_eq!(redeemed.status(), StatusCode::SEE_OTHER);
    assert_eq!(header(&redeemed, LOCATION), "/runs");
    let visitor = cookie(&redeemed, "__Host-aruna-visitor");
    assert!(!visitor.is_empty());
    assert!(
        redeemed
            .headers()
            .get_all(SET_COOKIE)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .any(|v| v.starts_with("__Host-aruna-visitor=") && v.contains("Partitioned"))
    );
}

#[tokio::test]
async fn guests_stay_private() {
    let base = start().await;
    let first = visit(
        &base,
        "guest.apps.test",
        "/",
        &[("sec-fetch-dest", "document")],
    )
    .await;
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(header(&first, CACHE_CONTROL), "private, no-cache");
    let guest_cookie = cookie(&first, "__Host-aruna-guest");
    assert!(!guest_cookie.is_empty());

    // A returning visitor with a guest cookie still gets private, no-cache on guest HTML.
    let second = visit(&base, "guest.apps.test", "/", &[("cookie", &guest_cookie)]).await;
    assert_eq!(second.status(), StatusCode::OK);
    assert_eq!(header(&second, CACHE_CONTROL), "private, no-cache");
    assert!(cookie(&second, "__Host-aruna-guest").is_empty());
}
