//! Signs visitors in through the portal and out again, on the applet's own host.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use axum::body::Body;
use axum::http::StatusCode;
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE, HeaderMap, HeaderValue, SET_COOKIE};
use axum::response::{Html, IntoResponse, Response};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use reqwest::Method;
use serde::Deserialize;
use tracing::warn;

use crate::Gateway;
use crate::pages::{bare, not_found, page, redirect, unavailable};
use crate::policy::{self, Frame};
use crate::preview::parse_pick;
use crate::session::{
    PREVIEW, STATE, Session, Site, VISITOR, cookies, framed, now_seconds, return_path, same_secret,
    set_cookie,
};
use crate::visit::{Visit, plain_id, query_params};

/// How long a started sign-in may take before its state cookie is gone.
const STATE_SECONDS: u64 = 600;
/// Shortest pop-up state accepted from the query string; matches a 24-byte base64url token.
const MIN_POPUP_STATE: usize = 32;
/// Marks a top-level pop-up state cookie so callback hands the code back to the framing applet.
const POPUP_PREFIX: &str = "popup:";
const PREVIEW_FRAME_JS: &str = include_str!("preview_frame.js");
const POPUP_JS: &str = include_str!("popup.js");
const POPUP_DONE_JS: &str = include_str!("popup_done.js");

#[derive(Deserialize)]
struct Redeemed {
    applet_id: String,
    visitor_token: String,
    expires_at: u64,
    preview: bool,
}

fn escape_attr(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn html_page(body: String, frame: Frame<'_>, opener: bool) -> Response {
    let mut response = (StatusCode::OK, Html(body)).into_response();
    let headers = response.headers_mut();
    policy::secure(headers, frame);
    policy::no_referrer(headers);
    if opener {
        policy::allow_opener(headers);
    }
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn script_page(source: &'static str, frame: Frame<'_>, opener: bool) -> Response {
    let mut response = Response::new(Body::from(source));
    let headers = response.headers_mut();
    let kind = HeaderValue::from_static("text/javascript; charset=utf-8");
    headers.insert(CONTENT_TYPE, kind);
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    policy::secure(headers, frame);
    if opener {
        policy::allow_opener(headers);
    }
    response
}

fn clear_state(headers: &mut HeaderMap) {
    for clear in [Site::Lax, Site::Framed] {
        if let Some(cookie) = set_cookie(STATE, "", 0, clear) {
            headers.append(SET_COOKIE, cookie);
        }
    }
}

fn unavailable_signin(frame: Frame<'_>) -> Response {
    let message = "Open this applet at its own Aruna address to sign in.";
    page(
        StatusCode::NOT_FOUND,
        "Sign-in is not available here",
        message,
        frame,
    )
}

/// Portal builder preview iframe handshake: sets a partitioned state cookie and posts it to the
/// portal so the portal mints the preview code without framing `/applets/signin`.
pub fn preview_frame(visit: &Visit) -> Response {
    let Some(portal) = (visit.portal.as_deref()).filter(|_| !visit.answer.decision.custom_domain)
    else {
        return unavailable_signin(visit.frame());
    };
    let state = URL_SAFE_NO_PAD.encode(rand::random::<[u8; 24]>());
    let body = format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><title>Preview</title>\
         <script src=\"/_aruna/auth/preview-frame.js\" data-portal=\"{}\" data-applet=\"{}\" \
         data-host=\"{}\" data-node=\"{}\" data-state=\"{}\" defer></script></head>\
         <body></body></html>",
        escape_attr(portal),
        escape_attr(&visit.answer.decision.applet_id),
        escape_attr(&visit.host),
        escape_attr(&visit.answer.decision.host_node),
        escape_attr(&state),
    );
    let mut response = html_page(body, Frame::Preview(portal), false);
    if let Some(cookie) = set_cookie(STATE, &state, STATE_SECONDS, Site::Framed) {
        response.headers_mut().append(SET_COOKIE, cookie);
    }
    response
}

pub fn preview_script(visit: &Visit) -> Response {
    let Some(portal) = (visit.portal.as_deref()).filter(|_| !visit.answer.decision.custom_domain)
    else {
        return not_found(visit.frame());
    };
    script_page(PREVIEW_FRAME_JS, Frame::Preview(portal), false)
}

pub fn popup_script(visit: &Visit) -> Response {
    script_page(POPUP_JS, visit.frame(), false)
}

pub fn done_script(visit: &Visit) -> Response {
    script_page(POPUP_DONE_JS, visit.frame(), true)
}

fn popup_launcher(visit: &Visit, back: &str) -> Response {
    let state = URL_SAFE_NO_PAD.encode(rand::random::<[u8; 24]>());
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    query
        .append_pair("popup", "1")
        .append_pair("state", &state)
        .append_pair("return", back);
    let popup_url = format!("/_aruna/auth/start?{}", query.finish());
    let body = format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\
         <title>Sign in</title><style>:root{{color-scheme:light dark;font-family:system-ui,sans-serif;}}\
         body{{margin:0;min-height:100vh;display:grid;place-items:center;background:Canvas;color:CanvasText;}}\
         main{{max-width:26rem;padding:2rem 1.5rem;text-align:center;}}\
         button{{font:inherit;padding:0.6rem 1.2rem;border-radius:0.5rem;cursor:pointer;}}</style>\
         <script src=\"/_aruna/auth/popup.js\" data-popup-url=\"{}\" data-state=\"{}\" \
         data-return-path=\"{}\" defer></script></head><body><main>\
         <h1>Sign in to continue</h1><p>Sign-in opens in a new window so this embedded applet \
         can keep your session inside this frame.</p>\
         <button id=\"aruna-signin\" type=\"button\">Sign in with Aruna</button>\
         </main></body></html>",
        escape_attr(&popup_url),
        escape_attr(&state),
        escape_attr(back),
    );
    let mut response = html_page(body, visit.frame(), false);
    if let Some(cookie) = set_cookie(STATE, &state, STATE_SECONDS, Site::Framed) {
        response.headers_mut().append(SET_COOKIE, cookie);
    }
    response
}

/// Remembers a fresh state in a cookie and sends the browser to the portal's sign-in page, or
/// serves the framed handshake/pop-up launcher when loaded inside an iframe.
pub fn start(visit: &Visit, headers: &HeaderMap, query: Option<&str>) -> Response {
    let frame = visit.frame();
    let portal = (visit.portal.as_deref()).filter(|_| !visit.answer.decision.custom_domain);
    let target =
        portal.and_then(|portal| url::Url::parse(portal).ok()?.join("/applets/signin").ok());
    let Some(mut target) = target else {
        return unavailable_signin(frame);
    };
    let params = query_params(query);
    let preview = params.get("preview").is_some_and(|flag| flag == "1");
    let popup = params.get("popup").is_some_and(|flag| flag == "1");
    let back = return_path(params.get("return").map(String::as_str));
    if framed(headers) {
        return match preview {
            true => preview_frame(visit),
            false => popup_launcher(visit, &back),
        };
    }
    let state = match popup {
        true => params
            .get("state")
            .filter(|value| value.len() >= MIN_POPUP_STATE && plain_id(value))
            .cloned()
            .unwrap_or_else(|| URL_SAFE_NO_PAD.encode(rand::random::<[u8; 24]>())),
        false => URL_SAFE_NO_PAD.encode(rand::random::<[u8; 24]>()),
    };
    target
        .query_pairs_mut()
        .append_pair("applet", &visit.answer.decision.applet_id)
        .append_pair("host", &visit.host)
        .append_pair("node", &visit.answer.decision.host_node)
        .append_pair("return", &back)
        .append_pair("state", &state)
        .append_pair("preview", if preview { "1" } else { "0" });
    let (cookie_value, site) = match (popup, preview) {
        (true, _) => (format!("{POPUP_PREFIX}{state}"), Site::Lax),
        (false, true) => (state, Site::Framed),
        (false, false) => (state, Site::Lax),
    };
    let mut response = redirect(StatusCode::FOUND, target.as_str(), frame);
    policy::no_referrer(response.headers_mut());
    if popup {
        policy::allow_opener(response.headers_mut());
    }
    if let Some(cookie) = set_cookie(STATE, &cookie_value, STATE_SECONDS, site) {
        response.headers_mut().append(SET_COOKIE, cookie);
    }
    response
}

fn popup_handoff(code: &str, state: &str) -> Response {
    let body = format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><title>Signed in</title>\
         <script src=\"/_aruna/auth/popup-done.js\" data-code=\"{}\" data-state=\"{}\" defer>\
         </script></head><body><p>Sign-in complete. You can close this window.</p></body></html>",
        escape_attr(code),
        escape_attr(state),
    );
    let mut response = html_page(body, Frame::Embed(&[]), true);
    clear_state(response.headers_mut());
    response
}

/// Trades the portal's one-use code for the visitor's session, if the state matches.
pub async fn callback(
    gateway: &Gateway,
    visit: &Visit,
    headers: &HeaderMap,
    query: Option<&str>,
) -> Response {
    let frame = visit.frame();
    let params = query_params(query);
    let expired = || {
        let message = "Start the sign-in again from the applet.";
        let mut response = page(StatusCode::BAD_REQUEST, "Sign-in expired", message, frame);
        policy::no_referrer(response.headers_mut());
        response
    };
    let (Some(code), Some(state)) = (params.get("code"), params.get("state")) else {
        return expired();
    };
    let popup_cookie = format!("{POPUP_PREFIX}{state}");
    if cookies(headers, STATE).any(|cookie| same_secret(cookie, &popup_cookie)) {
        return popup_handoff(code, state);
    }
    if !cookies(headers, STATE).any(|cookie| same_secret(cookie, state)) {
        return expired();
    }
    let body = serde_json::json!({ "code": code, "host": visit.host });
    let sent =
        (gateway
            .upstream
            .request(visit.answer.node, Method::POST, "/edge/v1/signin/redeem"))
        .json(&body)
        .send()
        .await;
    let redeemed = match sent {
        Ok(response) if response.status() == StatusCode::NOT_FOUND => return expired(),
        Ok(response) => match response.error_for_status() {
            Ok(response) => response.json::<Redeemed>().await,
            Err(error) => Err(error),
        },
        Err(error) => Err(error),
    };
    let redeemed = match redeemed {
        Ok(redeemed) if redeemed.applet_id == visit.answer.decision.applet_id => redeemed,
        Ok(_) => return expired(),
        Err(error) => {
            warn!(%error, host = %visit.host, "redeeming a sign-in code failed");
            return unavailable(frame);
        }
    };
    let pick = match redeemed.preview {
        true => match parse_pick(&params) {
            Ok(pick) => pick,
            Err(()) => return bare(StatusCode::BAD_REQUEST, frame),
        },
        false => None,
    };
    let (name, site) = match (redeemed.preview, framed(headers)) {
        (true, _) => (PREVIEW, Site::Framed),
        (false, true) => (VISITOR, Site::Framed),
        (false, false) => (VISITOR, Site::Lax),
    };
    let session = Session {
        applet_id: redeemed.applet_id,
        token: redeemed.visitor_token,
        preview: redeemed.preview,
        expires: redeemed.expires_at,
        pick,
    };
    let max_age = session.expires.saturating_sub(now_seconds());
    let cookie = (gateway.sealer.seal(&visit.host, &session))
        .and_then(|sealed| set_cookie(name, &sealed, max_age, site));
    let Some(cookie) = cookie else {
        return unavailable(frame);
    };
    let back = return_path(params.get("return").map(String::as_str));
    let mut response = redirect(StatusCode::SEE_OTHER, &back, frame);
    let out = response.headers_mut();
    policy::no_referrer(out);
    out.append(SET_COOKIE, cookie);
    clear_state(out);
    response
}

/// Clears every session cookie of the host, partitioned or not.
pub fn logout(visit: &Visit) -> Response {
    let mut response = redirect(StatusCode::SEE_OTHER, "/", visit.frame());
    let out = response.headers_mut();
    policy::no_referrer(out);
    for (name, site) in [
        (VISITOR, Site::Lax),
        (VISITOR, Site::Framed),
        (PREVIEW, Site::Framed),
    ] {
        if let Some(cookie) = set_cookie(name, "", 0, site) {
            out.append(SET_COOKIE, cookie);
        }
    }
    response
}
