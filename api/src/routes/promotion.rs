//! Promotes a server node of this realm to a management node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::error::{ServerError, ServerResult};
use crate::routes::onboarding::authorize_onboarding_admin;
use crate::server::state::ServerState;
use aruna_core::NodeId;
use aruna_core::errors::StorageError;
use aruna_core::structs::identity::auth::{Actor, AuthContext, NodeCapabilities};
use aruna_operations::realm::ensure_config::EnsureConfigError;
use aruna_operations::realm::promote_node::{PromoteNodeConfig, PromoteNodeError, promote_node};
use axum::Extension;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use std::str::FromStr;
use std::sync::Arc;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new().routes(routes!(promote_server_node))
}

#[utoipa::path(
    post,
    path = "/system/realm/nodes/{node_id}/promote",
    tag = "system/realm",
    summary = "Promote a server node to a management node",
    description = r#"Makes an onboarded server node a management node, without wiping or re-onboarding it.

**Authentication**: realm bearer token with WRITE on the realm's onboarding administration path, the
same grant that can create a management onboarding secret. A management node serves it, and every
other node relays the call to one.

**Behavior**
- The serving node first hands the realm signing key to the node over the encrypted node-to-node
  connection. The key never passes through the caller and is never returned.
- Only after the node stored the key, the realm configuration lists it as a management node. This
  change replicates like any other configuration change.
- The node acts as a management node, for example signing tokens, after its next restart. Restart it
  right after this call.
- A node that is offline or refuses the key leaves the realm unchanged, and the call can be repeated.
  Calling it again for an already promoted node is accepted.
- A management node cannot be demoted here: a copied key cannot be taken back."#,
    params(("node_id" = String, Path, description = "Node id of the server node, as the realm configuration lists it")),
    responses(
        (status = 202, description = "The node holds the realm key and is listed as a management node; it acts as one after its next restart"),
        (status = 401, description = "Missing or invalid bearer token", body = crate::error::ErrorResponse),
        (status = 403, description = "The caller lacks WRITE on the realm's onboarding administration path", body = crate::error::ErrorResponse),
        (status = 404, description = "No realm node carries this node id", body = crate::error::ErrorResponse),
        (status = 409, description = "The node is a device or the serving node itself, a group deletion is pending, or another update of the realm configuration won the race; retry", body = crate::error::ErrorResponse),
        (status = 502, description = "A relayed call failed after the management node may already have applied it; code `relay_failed`", body = crate::error::ErrorResponse),
        (status = 503, description = "The node could not be reached or refused the key, or no management node was reachable to serve the relayed call; retry", body = crate::error::ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn promote_server_node(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(node_id): Path<String>,
) -> ServerResult<StatusCode> {
    let auth = authorize_onboarding_admin(&state, auth).await?;
    let node_id = NodeId::from_str(&node_id).map_err(|_| ServerError::NotFound)?;
    let NodeCapabilities::Management {
        realm_signing_key, ..
    } = state.node_capabilities()
    else {
        return Err(ServerError::Forbidden);
    };
    let config = PromoteNodeConfig {
        actor: Actor {
            node_id: state.get_node_id(),
            user_id: auth.user_id,
            realm_id: auth.realm_id,
        },
        node_id,
        realm_signing_key: realm_signing_key.clone(),
    };
    promote_node(&state.get_ctx(), config)
        .await
        .map_err(map_promote_error)?;
    Ok(StatusCode::ACCEPTED)
}

fn map_promote_error(error: PromoteNodeError) -> ServerError {
    match error {
        PromoteNodeError::ConfigMissing | PromoteNodeError::NodeNotFound { .. } => {
            ServerError::NotFound
        }
        PromoteNodeError::NotManagementNode => ServerError::Forbidden,
        PromoteNodeError::NotServer => {
            ServerError::Conflict("only another server node can be promoted".to_string())
        }
        PromoteNodeError::Unreachable(_) | PromoteNodeError::Refused(_) => {
            ServerError::ServiceUnavailableReason(
                "the node did not take the realm key; the realm is unchanged, retry".to_string(),
            )
        }
        PromoteNodeError::Ensure(EnsureConfigError::DeletionPending) => {
            ServerError::Conflict("group deletion is pending; retry the promotion".to_string())
        }
        PromoteNodeError::Ensure(EnsureConfigError::StorageError(
            StorageError::TransactionConflict,
        )) => ServerError::Conflict("concurrent realm configuration update; retry".to_string()),
        PromoteNodeError::Ensure(EnsureConfigError::StorageError(
            StorageError::CleanupCapacity,
        )) => ServerError::ServiceUnavailableReason(
            "storage cleanup capacity exhausted; retry".to_string(),
        ),
        other => ServerError::InternalError(other.to_string()),
    }
}
