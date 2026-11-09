//! Serves the caller's own webhook subscription for share upload notifications.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::execution::webhook::{WEBHOOK_EVENT, WebhookSubscription};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_operations::notifications::webhook::{self, WebhookError, read_webhook, remove_webhook};
use axum::extract::State;
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::require_unrestricted_auth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::emails::{setting_error, settings_node};
use crate::server::state::ServerState;

#[derive(OpenApi)]
#[openapi()]
pub struct WebhooksApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(WebhooksApiDoc::openapi()).routes(routes!(
        get_webhook,
        set_webhook,
        delete_webhook
    ))
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct WebhookRequest {
    pub url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct WebhookResponse {
    pub subscription_id: String,
    pub url: String,
    /// The one delivered event, `share.upload`.
    pub event: String,
    pub created_at_ms: u64,
    /// The hex signing secret, returned only by the request that set it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret: Option<String>,
}

#[utoipa::path(
    get,
    path = "/system/notifications/webhook",
    tag = "system/notifications",
    summary = "Read the caller's webhook",
    description = r#"Returns the caller's webhook subscription, without its secret.

**Authentication**: realm bearer token without path restrictions; only the caller's own
subscription is read.

**Behavior**
- The subscription is a replicated realm setting. This node shows the newest confirmed setting
  it received so far; a node that has not received a recent change yet shows the previous one.
  Deliveries always decide from most management nodes.

**Errors**
- 409 on a user device, which keeps no notification settings.
- 503 when this node cannot read the realm configuration now."#,
    responses(
        (status = 200, description = "The subscription", body = WebhookResponse, example = json!({
            "subscription_id": "01JWEBHK0123456789ABCDEFGH",
            "url": "https://hooks.example.org/aruna",
            "event": "share.upload",
            "created_at_ms": 1762160400000u64
        })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "A path-restricted token", body = ErrorResponse),
        (status = 404, description = "The caller has no webhook", body = ErrorResponse),
        (status = 409, description = "This node is a user device", body = ErrorResponse),
        (status = 503, description = "The realm configuration cannot be read now; retry", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_webhook(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<Json<WebhookResponse>> {
    let auth = require_unrestricted_auth(&state, auth)?;
    settings_node(&state, &auth).await?;
    let subscription = read_webhook(&state.get_ctx(), auth.user_id)
        .await
        .map_err(map_error)?;
    let subscription = subscription.ok_or(ServerError::NotFound)?;
    Ok(Json(response(subscription, None)))
}

#[utoipa::path(
    put,
    path = "/system/notifications/webhook",
    tag = "system/notifications",
    summary = "Set the caller's webhook",
    description = r#"Subscribes a URL to the caller's share upload notifications, with a new signing secret.

**Authentication**: realm bearer token without path restrictions; the subscription belongs to
the caller.

**Behavior**
- Each upload through one of the caller's share links is sent once as a JSON `POST` with the
  event `share.upload`, the share, bucket, key and size, carrying only what the caller's
  notification shows.
- `x-aruna-delivery` holds the notification id, the same for every retry, so a receiver drops
  repeats. `x-aruna-signature` is `t=<unix seconds>,v1=<hex HMAC-SHA256 of "<t>.<body>">`
  under the secret.
- Each delivery is a job of the caller, followed and cancelled with the job routes. A failed
  attempt is retried a bounded number of times; a 4xx answer other than 408 and 429 ends it.
- The URL is checked against the node's egress rules at delivery, like other outbound requests.
- The subscription is a replicated realm setting, so it keeps working when the caller's inbox
  moves to another node. Setting a webhook replaces the previous one and its secret. Each
  delivery reads the setting from most management nodes when it runs, so queued deliveries
  use the newest subscription.
- The secret is returned only here. It is sealed to the realm key, which only management nodes
  hold.
- A 200 means most management nodes confirmed the subscription, and it decides every later
  delivery.
- A 503 is an unknown outcome: the change may still take effect. A retry or a GET shows the
  result.
- The order in which overlapping requests complete does not order them: a DELETE that overlaps
  a retried PUT can return 204 while that PUT's newer version wins. A DELETE started after the
  failed PUT completed always wins.

**Errors**
- 409 on a user device.
- 503 when the realm configuration cannot be read now, or too few management nodes answered."#,
    request_body(content = WebhookRequest, example = json!({"url": "https://hooks.example.org/aruna"})),
    responses(
        (status = 200, description = "The new subscription with its secret", body = WebhookResponse, example = json!({
            "subscription_id": "01JWEBHK0123456789ABCDEFGH",
            "url": "https://hooks.example.org/aruna",
            "event": "share.upload",
            "created_at_ms": 1762160400000u64,
            "secret": "<hex signing secret>"
        })),
        (status = 400, description = "Not an http or https URL, a URL with credentials, or longer than 2048 bytes", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "A path-restricted token", body = ErrorResponse),
        (status = 409, description = "This node is a user device", body = ErrorResponse),
        (status = 503, description = "Unknown outcome: the configuration cannot be read or too few management nodes answered; retry or read", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn set_webhook(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<WebhookRequest>,
) -> ServerResult<Json<WebhookResponse>> {
    let auth = require_unrestricted_auth(&state, auth)?;
    settings_node(&state, &auth).await?;
    let ctx = state.get_ctx();
    let outcome = webhook::set_webhook(&ctx, state.get_node_id(), auth.user_id, request.url)
        .await
        .map_err(map_error)?;
    Ok(Json(response(outcome.subscription, Some(outcome.secret))))
}

#[utoipa::path(
    delete,
    path = "/system/notifications/webhook",
    tag = "system/notifications",
    summary = "Remove the caller's webhook",
    description = r#"Turns the caller's webhook off.

**Authentication**: realm bearer token without path restrictions.

**Behavior**
- A 204 means most management nodes confirmed the change; queued deliveries send nothing
  afterwards. Removing a missing webhook succeeds too.
- A 503 is an unknown outcome: the change may still take effect. A retry or a GET shows the
  result.

**Errors**
- 409 on a user device.
- 503 when the realm configuration cannot be read now, or too few management nodes answered."#,
    responses(
        (status = 204, description = "Most management nodes confirmed that the webhook is off"),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "A path-restricted token", body = ErrorResponse),
        (status = 409, description = "This node is a user device", body = ErrorResponse),
        (status = 503, description = "Unknown outcome: the configuration cannot be read or too few management nodes answered; retry or read", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_webhook(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<StatusCode> {
    let auth = require_unrestricted_auth(&state, auth)?;
    settings_node(&state, &auth).await?;
    remove_webhook(&state.get_ctx(), state.get_node_id(), auth.user_id)
        .await
        .map_err(map_error)?;
    Ok(StatusCode::NO_CONTENT)
}

fn response(subscription: WebhookSubscription, secret: Option<String>) -> WebhookResponse {
    WebhookResponse {
        subscription_id: subscription.subscription_id.to_string(),
        url: subscription.url.unwrap_or_default(),
        event: WEBHOOK_EVENT.to_string(),
        created_at_ms: subscription.created_at_ms,
        secret,
    }
}

fn map_error(error: WebhookError) -> ServerError {
    match error {
        WebhookError::InvalidUrl => ServerError::BadRequestReason(error.to_string()),
        WebhookError::Setting(error) => setting_error(error),
        WebhookError::Conversion(_) => ServerError::InternalError(error.to_string()),
    }
}

#[cfg(test)]
#[path = "webhooks_tests.rs"]
mod tests;
