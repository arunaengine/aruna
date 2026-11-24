//! Signs visitors in through the portal and out again, on the applet's own host.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use axum::http::StatusCode;
use axum::http::header::{HeaderMap, SET_COOKIE};
use axum::response::Response;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use reqwest::Method;
use serde::Deserialize;
use tracing::warn;

use crate::Gateway;
use crate::pages::{page, redirect, unavailable};
use crate::session::{
    PREVIEW, STATE, Session, Site, VISITOR, cookies, framed, now_seconds, return_path, same_secret,
    set_cookie,
};
use crate::visit::{Visit, query_params};

/// How long a started sign-in may take before its state cookie is gone.
const STATE_SECONDS: u64 = 600;

#[derive(Deserialize)]
struct Redeemed {
    applet_id: String,
    visitor_token: String,
    expires_at: u64,
    preview: bool,
}

/// Remembers a fresh state in a cookie and sends the browser to the portal's sign-in page.
pub fn start(visit: &Visit, headers: &HeaderMap, query: Option<&str>) -> Response {
    let frame = visit.frame();
    let portal = (visit.portal.as_deref()).filter(|_| !visit.answer.decision.custom_domain);
    let target =
        portal.and_then(|portal| url::Url::parse(portal).ok()?.join("/applets/signin").ok());
    let Some(mut target) = target else {
        let message = "Open this applet at its own Aruna address to sign in.";
        return page(
            StatusCode::NOT_FOUND,
            "Sign-in is not available here",
            message,
            frame,
        );
    };
    let params = query_params(query);
    let preview = params.get("preview").is_some_and(|flag| flag == "1");
    let state = URL_SAFE_NO_PAD.encode(rand::random::<[u8; 24]>());
    target
        .query_pairs_mut()
        .append_pair("applet", &visit.answer.decision.applet_id)
        .append_pair("host", &visit.host)
        .append_pair(
            "return",
            &return_path(params.get("return").map(String::as_str)),
        )
        .append_pair("state", &state)
        .append_pair("preview", if preview { "1" } else { "0" });
    let site = match preview || framed(headers) {
        true => Site::Framed,
        false => Site::Lax,
    };
    let mut response = redirect(StatusCode::FOUND, target.as_str(), frame);
    if let Some(cookie) = set_cookie(STATE, &state, STATE_SECONDS, site) {
        response.headers_mut().append(SET_COOKIE, cookie);
    }
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
        page(StatusCode::BAD_REQUEST, "Sign-in expired", message, frame)
    };
    let (Some(code), Some(state)) = (params.get("code"), params.get("state")) else {
        return expired();
    };
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
        pick: None,
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
    out.append(SET_COOKIE, cookie);
    for clear in [Site::Lax, Site::Framed] {
        if let Some(cookie) = set_cookie(STATE, "", 0, clear) {
            out.append(SET_COOKIE, cookie);
        }
    }
    response
}

/// Clears every session cookie of the host, partitioned or not.
pub fn logout(visit: &Visit) -> Response {
    let mut response = redirect(StatusCode::SEE_OTHER, "/", visit.frame());
    let out = response.headers_mut();
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
