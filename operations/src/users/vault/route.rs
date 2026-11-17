//! Routes vault changes to a holder and serves vault fetches between realm nodes.
//! A holder checks the forwarded bearer token itself and serves a vault only to its own user.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::effects::VaultQuery;
use aruna_core::metadata::AuthToken;
use aruna_core::structs::identity::user::vault::VaultRecords;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use super::read::LocalRecordsOperation;
use super::write::{
    AppendVaultConfig, AppendVaultError, AppendVaultOperation, VaultAppended, VaultRefusal,
};
use crate::document_repository::GetConfigOperation;
use crate::driver::{DriverContext, drive, drive_retry};
use crate::forward::routing::forward_to_holders;
use crate::forward::transport::MetadataWriteError;
use crate::metadata::handle::WritePeerError;
use crate::metadata::protocol::{MetadataReadError, MetadataTransportMessage, WriteAuthError};
use crate::metadata::transport_message_kind;
use crate::realm::peer_trust::{is_sync_eligible, peer_acts_for};
use aruna_core::placement::{holds_placement, target_placement_ref};

#[derive(Debug, Error)]
pub enum VaultRouteError {
    #[error(transparent)]
    Append(#[from] AppendVaultError),
    #[error(transparent)]
    Forward(#[from] MetadataWriteError),
    #[error("a vault holder refused the change: {0:?}")]
    Refused(VaultRefusal),
}

/// Appends on this node when it holds the user's vault, otherwise on one of the
/// holders; a change no holder accepts fails instead of waiting in an outbox (D10).
pub async fn append_vault_routed(
    context: &Arc<DriverContext>,
    config: AppendVaultConfig,
    auth_token: Option<AuthToken>,
) -> Result<VaultAppended, VaultRouteError> {
    let holders = match append_local(context, config.clone()).await {
        Err(AppendVaultError::NotHolder { holders }) => holders,
        result => return Ok(result?),
    };
    let message = MetadataTransportMessage::ForwardVaultChange {
        auth_token,
        record_id: config.record_id,
        change: Box::new(config.change),
        created_at_ms: config.now_ms,
    };
    match forward_to_holders(context, &holders, message, None, false).await? {
        MetadataTransportMessage::ForwardedVaultChange { result } => {
            result.map_err(VaultRouteError::Refused)
        }
        other => Err(MetadataWriteError::Undeliverable(format!(
            "holder answered a vault change with {}",
            transport_message_kind(&other)
        ))
        .into()),
    }
}

/// Retries a save that lost its transaction to a concurrent one on this holder.
async fn append_local(
    context: &Arc<DriverContext>,
    config: AppendVaultConfig,
) -> Result<VaultAppended, AppendVaultError> {
    let seed = config.record_id.to_bytes();
    let build = || AppendVaultOperation::new(config.clone());
    drive_retry(build, context.as_ref(), &seed).await
}

/// Runs one forwarded vault change for the user the forwarded token names.
pub(crate) async fn apply_forwarded_vault(
    context: &Arc<DriverContext>,
    peer: NodeId,
    message: MetadataTransportMessage,
) -> MetadataTransportMessage {
    let MetadataTransportMessage::ForwardVaultChange {
        auth_token,
        record_id,
        change,
        created_at_ms,
    } = message
    else {
        return MetadataTransportMessage::Reject("unexpected vault change".to_string());
    };
    let Some(net_handle) = context.net_handle.as_ref() else {
        return MetadataTransportMessage::ForwardedWriteUnavailable;
    };
    let user_id = match vault_caller(context, peer, auth_token).await {
        Ok(user_id) => user_id,
        Err(MetadataReadError::Unauthorized) => {
            return MetadataTransportMessage::ForwardedWriteDenied {
                error: WriteAuthError::Unauthorized,
            };
        }
        Err(MetadataReadError::Forbidden) => {
            return MetadataTransportMessage::ForwardedWriteDenied {
                error: WriteAuthError::Forbidden,
            };
        }
        Err(_) => return MetadataTransportMessage::ForwardedWriteUnavailable,
    };
    let config = AppendVaultConfig {
        node_id: net_handle.node_id(),
        realm_id: *net_handle.realm_id(),
        user_id,
        record_id,
        change: *change,
        now_ms: created_at_ms,
    };
    let result = match append_local(context, config).await {
        Ok(appended) => Ok(appended),
        Err(AppendVaultError::Record(_)) => Err(VaultRefusal::Invalid),
        Err(AppendVaultError::TooManyKeys) => Err(VaultRefusal::TooManyKeys),
        // Another holder may still accept it.
        Err(error) => {
            warn!(%error, "Forwarded vault change failed");
            return MetadataTransportMessage::ForwardedWriteUnavailable;
        }
    };
    MetadataTransportMessage::ForwardedVaultChange { result }
}

/// The user a forwarded bearer token names, checked again on this node. Internal
/// principals and path-restricted tokens never reach a vault.
async fn vault_caller(
    context: &Arc<DriverContext>,
    peer: NodeId,
    auth_token: Option<AuthToken>,
) -> Result<UserId, MetadataReadError> {
    let (Some(metadata), Some(net_handle)) = (
        context.metadata_handle.as_ref(),
        context.net_handle.as_ref(),
    ) else {
        return Err(MetadataReadError::Unavailable);
    };
    if !matches!(auth_token, Some(AuthToken::Bearer(_))) {
        return Err(MetadataReadError::Unauthorized);
    }
    let auth = metadata
        .authorize_write_peer(peer, auth_token)
        .await
        .map_err(|error| match error {
            WritePeerError::Unauthorized => MetadataReadError::Unauthorized,
            WritePeerError::Unavailable(_) => MetadataReadError::Unavailable,
        })?;
    let realm_id = *net_handle.realm_id();
    let config = drive(GetConfigOperation::new(realm_id), context)
        .await
        .ok()
        .ok_or(MetadataReadError::Unavailable)?;
    if auth.realm_id != realm_id
        || auth.path_restrictions.is_some()
        || !peer_acts_for(&config, peer, auth.user_id)
    {
        return Err(MetadataReadError::Forbidden);
    }
    Ok(auth.user_id)
}

/// Serves this holder's records of one user to a peer.
pub(crate) async fn serve_vault_fetch(
    context: &Arc<DriverContext>,
    peer: NodeId,
    user_id: UserId,
    query: VaultQuery,
) -> MetadataTransportMessage {
    MetadataTransportMessage::FetchedVaultRecords {
        result: local_records(context, peer, user_id, query).await,
    }
}

async fn local_records(
    context: &Arc<DriverContext>,
    peer: NodeId,
    user_id: UserId,
    query: VaultQuery,
) -> Result<VaultRecords, MetadataReadError> {
    let net_handle = context
        .net_handle
        .as_ref()
        .ok_or(MetadataReadError::Unavailable)?;
    let config = drive(GetConfigOperation::new(*net_handle.realm_id()), context)
        .await
        .ok()
        .ok_or(MetadataReadError::Unavailable)?;
    let (target, heads) = match query {
        VaultQuery::Heads { auth_token } => {
            if vault_caller(context, peer, auth_token).await? != user_id {
                return Err(MetadataReadError::Forbidden);
            }
            let target = aruna_core::document::DocumentTarget::VaultRevision {
                realm_id: *net_handle.realm_id(),
                user_id,
                revision_id: Ulid::nil(),
            };
            (target, true)
        }
        VaultQuery::Keys => {
            if !is_sync_eligible(&config, peer) {
                return Err(MetadataReadError::Forbidden);
            }
            let target = aruna_core::document::DocumentTarget::UserKey {
                realm_id: *net_handle.realm_id(),
                user_id,
                record_id: Ulid::nil(),
            };
            (target, false)
        }
    };
    let placement = target_placement_ref(&config, &target, Default::default());
    // A node that holds no replica has nothing to say, so the requester asks the next holder.
    if !holds_placement(&config, &placement, net_handle.node_id()) {
        return Err(MetadataReadError::Unavailable);
    }
    drive(LocalRecordsOperation::new(user_id, heads), context).await
}
