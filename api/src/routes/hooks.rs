//! Takes webhook requests for plugin installs on any node: bounds them before the body is read,
//! relays them with their raw bytes to the install's receiver, and admits them there.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::{Arc, LazyLock};
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::plugin_runs::HookRequest;
use aruna_core::time::unix_timestamp_millis;
use aruna_operations::distribution::delegate::node_urls;
use aruna_operations::plugins::hooks::{HookError, admit_hook, checks_authorization};
use aruna_operations::plugins::install::read_config;
use aruna_operations::plugins::release::ReleaseKeys;
use axum::body::{Body, Bytes};
use axum::extract::{Path, RawQuery, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use tracing::warn;
use ulid::Ulid;
use utoipa::OpenApi;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::error::{ErrorResponse, ServerError};
use crate::routes::management_relay::{RELAY_HOP_HEADER, passed_hops};
use crate::routes::storage::relay::{capped_body, relay_url};
use crate::server::state::ServerState;

/// Bounds of a webhook request, checked before its body is read or relayed.
pub const MAX_WEBHOOK: usize = 256 * 1024;
pub const MAX_HOOK_HEADERS: usize = 64;
pub const MAX_HEADER_BYTES: usize = 16 * 1024;
pub const MAX_HOOK_QUERY: usize = 2 * 1024;
/// Seconds a provider waits before it sends a refused request again.
const HOOK_RETRY_SECS: u32 = 60;

/// Headers a relay never passes on: hop-by-hop ones and credentials no check reads.
const NOT_RELAYED: &[&str] = &[
    "connection",
    "content-length",
    "cookie",
    "host",
    "keep-alive",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

/// The relay client follows no redirect; without one the relay answers 503.
static HOOK_CLIENT: LazyLock<Option<reqwest::Client>> = LazyLock::new(|| {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .ok()
});

#[derive(OpenApi)]
pub struct HooksApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(HooksApiDoc::openapi()).routes(routes!(receive))
}

#[utoipa::path(
    post,
    path = "/plugin-hooks/{id}",
    tag = "plugins",
    summary = "Receive a plugin webhook",
    description = r#"Takes a webhook request for a plugin install and starts a run of its webhook trigger.

**Authentication**: none; the install's verify secret checks each request.

**Behavior**
- Any node takes the request and relays its raw body and headers to the install's receiver,
  without `cookie`, `proxy-authorization`, and `authorization` unless the secret checks it.
- The receiver checks the request, removes the checked and credential headers and queues a run.
- With the manifest's delivery id header, a repeated id answers 202 and starts nothing.

**Limits**
- At most 256 KiB of body, 64 headers of 16 KiB in total, a 2 KiB query and 60 requests per
  minute per install."#,
    params(("id" = String, Path, description = "The install id")),
    request_body(content = Vec<u8>, content_type = "application/octet-stream", description = "The provider's request body, kept byte for byte"),
    responses(
        (status = 202, description = "The run is queued, or the delivery id was admitted before"),
        (status = 401, description = "The request did not pass the install's check", body = ErrorResponse),
        (status = 404, description = "No install with a webhook trigger has this id", body = ErrorResponse),
        (status = 413, description = "The body is larger than 256 KiB", body = ErrorResponse),
        (status = 414, description = "The query is longer than 2 KiB", body = ErrorResponse),
        (status = 429, description = "The install is over its webhook rate", body = ErrorResponse,
            headers(("Retry-After" = u32, description = "Seconds before sending again"))),
        (status = 431, description = "Too many or too large headers", body = ErrorResponse),
        (status = 503, description = "The install is paused, blocked or full, or cannot check now", body = ErrorResponse,
            headers(("Retry-After" = u32, description = "Seconds before sending again")))
    ),
    security(())
)]
pub async fn receive(
    State(state): State<Arc<ServerState>>,
    Path(id): Path<String>,
    RawQuery(query): RawQuery,
    method: Method,
    headers: HeaderMap,
    body: Body,
) -> Response {
    if let Some(refused) = over_bounds(&headers, query.as_deref()) {
        return refused;
    }
    let Ok(body) = axum::body::to_bytes(body, MAX_WEBHOOK).await else {
        return refused(
            StatusCode::PAYLOAD_TOO_LARGE,
            "the body is larger than 256 KiB",
        );
    };
    let Ok(install_id) = Ulid::from_string(&id) else {
        return ServerError::NotFound.into_response();
    };
    let ctx = state.get_ctx();
    let keys = (ctx.metadata_handle.as_ref())
        .map(|handle| ReleaseKeys {
            seal: handle.realm_seal_key(),
            vault: handle.realm_vault_key(),
        })
        .unwrap_or_default();
    let request = HookRequest {
        delivery_id: None,
        method: method.to_string(),
        query: query.clone().unwrap_or_default(),
        headers: (headers.iter())
            .filter_map(|(name, value)| Some((name.to_string(), value.to_str().ok()?.to_string())))
            .collect(),
        body: body.to_vec(),
    };
    let place = (state.get_node_id(), &keys);
    let ids = (state.get_realm_id(), install_id);
    match admit_hook(&ctx, place, ids, request, unix_timestamp_millis()).await {
        Ok(_) => StatusCode::ACCEPTED.into_response(),
        Err(HookError::Elsewhere(receiver)) => {
            let path = match query {
                Some(query) => format!("/plugin-hooks/{install_id}?{query}"),
                None => format!("/plugin-hooks/{install_id}"),
            };
            relay(&state, (receiver, install_id), &path, &headers, body).await
        }
        Err(error) => hook_error(install_id, error),
    }
}

/// A refusal of a request over its bounds, before anything is read or relayed.
fn over_bounds(headers: &HeaderMap, query: Option<&str>) -> Option<Response> {
    let declared = (headers.get(header::CONTENT_LENGTH))
        .and_then(|value| value.to_str().ok()?.parse::<usize>().ok());
    if declared.is_some_and(|length| length > MAX_WEBHOOK) {
        return Some(refused(
            StatusCode::PAYLOAD_TOO_LARGE,
            "the body is larger than 256 KiB",
        ));
    }
    let bytes: usize = (headers.iter())
        .map(|(name, value)| name.as_str().len() + value.len())
        .sum();
    if headers.len() > MAX_HOOK_HEADERS || bytes > MAX_HEADER_BYTES {
        let why = "more than 64 headers or 16 KiB of headers";
        return Some(refused(StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE, why));
    }
    if query.is_some_and(|query| query.len() > MAX_HOOK_QUERY) {
        return Some(refused(
            StatusCode::URI_TOO_LONG,
            "the query is longer than 2 KiB",
        ));
    }
    None
}

fn refused(status: StatusCode, why: &str) -> Response {
    ServerError::Refused(status, "webhook_refused", why.to_string()).into_response()
}

fn retry_later(mut response: Response) -> Response {
    let wait = HeaderValue::from(HOOK_RETRY_SECS);
    response.headers_mut().insert(header::RETRY_AFTER, wait);
    response
}

/// The answer to an anonymous caller names no internal cause; the log keeps it.
fn hook_error(install_id: Ulid, error: HookError) -> Response {
    match error {
        HookError::NotFound => ServerError::NotFound.into_response(),
        HookError::Unverified => ServerError::Unauthorized.into_response(),
        HookError::Rate => retry_later(refused(StatusCode::TOO_MANY_REQUESTS, "over the rate")),
        HookError::Paused(_) => retry_later(ServerError::ServiceUnavailable.into_response()),
        HookError::Elsewhere(_) | HookError::Unavailable(_) | HookError::Run(_) => {
            warn!(%install_id, %error, "A plugin webhook was not admitted");
            retry_later(ServerError::ServiceUnavailable.into_response())
        }
    }
}

/// The headers a relay passes on: all but hop-by-hop ones, `cookie` and `proxy-authorization`,
/// and `authorization` unless the install's verify secret checks it.
fn relayed(headers: &HeaderMap, keep_authorization: bool) -> HeaderMap {
    let mut sent = headers.clone();
    for name in NOT_RELAYED {
        sent.remove(*name);
    }
    sent.remove(RELAY_HOP_HEADER);
    if !keep_authorization {
        sent.remove(header::AUTHORIZATION);
    }
    sent
}

/// Sends the request on to the receiver and answers what it answered.
async fn relay(
    state: &ServerState,
    (receiver, install_id): (NodeId, Ulid),
    path: &str,
    headers: &HeaderMap,
    body: Bytes,
) -> Response {
    let unavailable = || retry_later(ServerError::ServiceUnavailable.into_response());
    let (Some(client), Some(hops)) = (
        HOOK_CLIENT.as_ref(),
        passed_hops(state, headers, Some(receiver)),
    ) else {
        return unavailable();
    };
    let ctx = state.get_ctx();
    let Some(base) = node_urls(&ctx, receiver).await.and_then(|urls| urls.api) else {
        return unavailable();
    };
    let keep = match read_config(&ctx, state.get_realm_id()).await {
        Ok(config) => checks_authorization(&ctx, &config, install_id).await,
        Err(_) => false,
    };
    let mut sent = relayed(headers, keep);
    sent.insert(RELAY_HOP_HEADER, hops);
    let url = relay_url(&base, path);
    let answer = client.post(&url).headers(sent).body(body).send().await;
    let answer = match answer {
        Ok(answer) => answer,
        Err(error) => {
            warn!(%receiver, error = %error.without_url(), "A plugin webhook relay failed");
            return unavailable();
        }
    };
    let status = answer.status();
    let wait = answer.headers().get(header::RETRY_AFTER).cloned();
    let kind = answer.headers().get(header::CONTENT_TYPE).cloned();
    let Ok(body) = capped_body(answer).await else {
        return unavailable();
    };
    let mut response = (status, body).into_response();
    for (name, value) in [(header::RETRY_AFTER, wait), (header::CONTENT_TYPE, kind)] {
        if let Some(value) = value {
            response.headers_mut().insert(name, value);
        }
    }
    response
}

#[cfg(test)]
#[path = "hooks_tests.rs"]
mod tests;
