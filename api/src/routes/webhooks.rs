//! Serves the caller's own webhook subscription for share upload notifications.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::execution::webhook::{WEBHOOK_EVENT, WebhookSubscription};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_operations::driver::drive;
use aruna_operations::notifications::placement::holds_inbox;
use aruna_operations::notifications::webhook::{WebhookAction, WebhookError, WebhookOperation};
use axum::extract::State;
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::require_unrestricted_auth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
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
    description = r#"Returns the caller's webhook subscription on this node, without its secret.

**Authentication**: realm bearer token without path restrictions; only the caller's own
subscription is read."#,
    responses(
        (status = 200, description = "The subscription", body = WebhookResponse, example = json!({
            "subscription_id": "01JWEBHK0123456789ABCDEFGH",
            "url": "https://hooks.example.org/aruna",
            "event": "share.upload",
            "created_at_ms": 1762160400000u64
        })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "A path-restricted token", body = ErrorResponse),
        (status = 404, description = "The caller has no webhook on this node", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_webhook(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<Json<WebhookResponse>> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let outcome = drive(
        WebhookOperation::new(auth.user_id, WebhookAction::Read),
        &state.get_ctx(),
    )
    .await
    .map_err(map_error)?;
    let subscription = outcome.subscription.ok_or(ServerError::NotFound)?;
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
- Setting a webhook replaces the previous one and its secret; queued deliveries of the old
  subscription send nothing.
- The secret is returned only here and is kept in the node vault.

**Errors**
- 409 when this node does not hold the caller's inbox; set the webhook on the node that does."#,
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
        (status = 409, description = "This node does not hold the caller's inbox", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn set_webhook(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<WebhookRequest>,
) -> ServerResult<Json<WebhookResponse>> {
    let auth = require_unrestricted_auth(&state, auth)?;
    if !holds_inbox(&state.get_ctx(), auth.user_id).await {
        return Err(ServerError::Conflict(
            "this node does not hold the caller's inbox".to_string(),
        ));
    }
    let outcome = drive(
        WebhookOperation::new(auth.user_id, WebhookAction::Set { url: request.url }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_error)?;
    let subscription = outcome
        .subscription
        .ok_or_else(|| ServerError::InternalError("webhook was not stored".to_string()))?;
    Ok(Json(response(subscription, outcome.secret)))
}

#[utoipa::path(
    delete,
    path = "/system/notifications/webhook",
    tag = "system/notifications",
    summary = "Remove the caller's webhook",
    description = r#"Removes the caller's webhook subscription and its secret from this node.

**Authentication**: realm bearer token without path restrictions.

**Behavior**
- Queued deliveries send nothing afterwards. Removing a missing webhook succeeds too."#,
    responses(
        (status = 204, description = "No webhook is left on this node"),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "A path-restricted token", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_webhook(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<StatusCode> {
    let auth = require_unrestricted_auth(&state, auth)?;
    drive(
        WebhookOperation::new(auth.user_id, WebhookAction::Remove),
        &state.get_ctx(),
    )
    .await
    .map_err(map_error)?;
    Ok(StatusCode::NO_CONTENT)
}

fn response(subscription: WebhookSubscription, secret: Option<String>) -> WebhookResponse {
    WebhookResponse {
        subscription_id: subscription.subscription_id.to_string(),
        url: subscription.url,
        event: WEBHOOK_EVENT.to_string(),
        created_at_ms: subscription.created_at_ms,
        secret,
    }
}

fn map_error(error: WebhookError) -> ServerError {
    match error {
        WebhookError::InvalidUrl => ServerError::BadRequestReason(error.to_string()),
        WebhookError::Storage(_)
        | WebhookError::Conversion(_)
        | WebhookError::NotFinished
        | WebhookError::UnexpectedEvent { .. } => ServerError::InternalError(error.to_string()),
    }
}

#[cfg(test)]
#[path = "webhooks_tests.rs"]
mod tests;
