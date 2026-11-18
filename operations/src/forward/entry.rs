//! The async entry points of forwarded token revokes and realm admin relays.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::revoke_token::RevokeTokenOperation;
use crate::auth::revoke_token::{RevokeTokenAdmission, RevokeTokenConfig, RevokeTokenError};
use crate::document_repository::GetConfigOperation;
use crate::driver::{DriverContext, drive};
use crate::forward::authorize::{authorize_forwarded_caller, authorize_write, forward_auth_error};
use crate::forward::realm::{ApplyRelayOperation, RelayAdminOperation};
use crate::forward::token::ForwardRevokeOperation;
use crate::forward::transport::{ForwardError, local_identity, reject};
use crate::metadata::protocol::{MetadataTransportMessage, WriteAuthError};
use aruna_core::NodeId;
use aruna_core::admin_documents::AdminDocumentEvent;
use aruna_core::auth::{bearer_token_hash, valid_revocation_expiry};
use aruna_core::document::DocumentTarget;
use aruna_core::metadata::AuthToken;
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::time::unix_timestamp_secs;
use std::sync::Arc;
use tracing::warn;
use ulid::Ulid;

/// Hands an origin-signed administrative envelope to a holder of its shard.
pub async fn relay_admin_event(
    context: &Arc<DriverContext>,
    holders: &[NodeId],
    target: DocumentTarget,
    event: Box<AdminDocumentEvent>,
    placement: PlacementRef,
    origin_signature: iroh::Signature,
) -> Result<(), ForwardError> {
    let local = local_identity(context).map(|(node_id, _)| node_id);
    let relay =
        RelayAdminOperation::new(local, holders, target, event, placement, origin_signature);
    drive(relay, context).await
}

/// Accepts a relayed administrative envelope; a failed write leaves it to another holder.
pub(crate) async fn apply_admin_relay(
    context: &Arc<DriverContext>,
    peer: NodeId,
    message: MetadataTransportMessage,
) -> MetadataTransportMessage {
    let apply = ApplyRelayOperation::new(local_identity(context), peer, message);
    drive(apply, context).await.unwrap_or_else(|error| {
        warn!(%error, "Failed to persist a relayed admin event");
        MetadataTransportMessage::ForwardedWriteUnavailable
    })
}

/// Asks ranked realm peers to revoke `token` for a node that cannot publish it.
pub async fn forward_token_revoke(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    auth_token: AuthToken,
    token: String,
) -> Result<(), ForwardError> {
    let local = local_identity(context).map(|(node_id, _)| node_id);
    let salt = Ulid::generate();
    let revoke = ForwardRevokeOperation::new(realm_id, local, auth_token, token, salt);
    drive(revoke, context).await
}

pub(crate) async fn apply_token_revoke(
    context: &Arc<DriverContext>,
    peer: NodeId,
    message: MetadataTransportMessage,
) -> MetadataTransportMessage {
    let Some(net_handle) = context.net_handle.as_ref() else {
        return MetadataTransportMessage::ForwardedWriteUnavailable;
    };
    let realm_id = *net_handle.realm_id();
    let Some(config) = drive(GetConfigOperation::new(realm_id), context).await.ok() else {
        return MetadataTransportMessage::ForwardedWriteUnavailable;
    };
    let local_node = config
        .nodes
        .iter()
        .find(|node| node.node_id == net_handle.node_id().to_string());
    if !local_node.is_some_and(|node| node.kind.is_sync_eligible()) {
        return MetadataTransportMessage::ForwardedWriteUnavailable;
    }
    let Some(metadata) = context.metadata_handle.as_ref() else {
        return MetadataTransportMessage::ForwardedWriteUnavailable;
    };
    let MetadataTransportMessage::ForwardTokenRevocation { auth_token, .. } = &message else {
        return reject("unexpected token revocation message");
    };
    let AuthToken::Bearer(caller_token) = auth_token else {
        return MetadataTransportMessage::ForwardedWriteDenied {
            error: WriteAuthError::Unauthorized,
        };
    };
    let caller_hash = bearer_token_hash(caller_token.as_str());
    let auth = match authorize_forwarded_caller(context, peer, realm_id, &message).await {
        Ok(auth) => auth,
        Err(error) => return forward_auth_error(error),
    };
    let MetadataTransportMessage::ForwardTokenRevocation { token, .. } = message else {
        return reject("unexpected token revocation message");
    };
    let claims = match metadata.claims_for_revocation(&token).await {
        Ok(claims) => claims,
        Err(error) => return reject(format!("invalid token revocation target: {error}")),
    };
    let expires_at = claims.exp;
    let now = unix_timestamp_secs();
    if !valid_revocation_expiry(expires_at, now) {
        return reject("token revocation expiry is outside the supported window");
    }
    let subject: AuthContext = match claims.try_into() {
        Ok(subject) => subject,
        Err(error) => return reject(format!("invalid token revocation subject: {error}")),
    };
    if subject.realm_id != realm_id {
        return MetadataTransportMessage::ForwardedWriteDenied {
            error: WriteAuthError::Forbidden,
        };
    }
    // A path-restricted caller may retire only the token it presented, never another one.
    let self_service = auth.user_id == subject.user_id
        && (auth.path_restrictions.is_none() || caller_hash == bearer_token_hash(&token));
    if !self_service
        && let Err(error) = authorize_write(
            context,
            auth.clone(),
            format!("/{realm_id}/admin/u/{}", subject.user_id),
        )
        .await
    {
        return forward_auth_error(error);
    }
    match drive(
        RevokeTokenOperation::new(RevokeTokenConfig {
            actor: Actor {
                node_id: net_handle.node_id(),
                user_id: auth.user_id,
                realm_id,
            },
            token_hash: bearer_token_hash(&token),
            expires_at,
            token_owner: subject.user_id,
            admission: if self_service {
                RevokeTokenAdmission::SelfService
            } else {
                RevokeTokenAdmission::Privileged
            },
            now,
        }),
        context.as_ref(),
    )
    .await
    {
        Ok(_) => MetadataTransportMessage::ForwardedTokenRevoked,
        Err(RevokeTokenError::CapacityReached) => MetadataTransportMessage::TokenRevocationCapacity,
        Err(error) => reject(format!("token revocation failed: {error}")),
    }
}
