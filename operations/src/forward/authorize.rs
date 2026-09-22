//! Decides which peer may forward a request, for which user, and what it may write.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::request_authorization::AuthorizeError;
use crate::auth::request_authorization::authorize;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::driver::DriverContext;
use crate::metadata::handle::WritePeerError;
use crate::metadata::protocol::AuthToken;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::metadata::protocol::WriteAuthError;
use crate::placement::process_placements::load_realm_config;
use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::identity::realm::RealmNodeKind;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::telemetry::time_stage;
use aruna_core::types::GroupId;
use std::sync::Arc;
use tracing::warn;
use ulid::Ulid;

pub(crate) fn is_sync_eligible(config: &RealmConfigDocument, node_id: NodeId) -> bool {
    configured_kind(config, node_id).is_some_and(RealmNodeKind::is_sync_eligible)
}

/// A User peer is owner-bound: it may forward only for the owner its realm
/// config names, whatever token it managed to present. Other kinds are not
/// owner-bound, so any authenticated caller may travel through them.
pub(crate) fn peer_acts_for(config: &RealmConfigDocument, peer: NodeId, user_id: UserId) -> bool {
    match configured_kind(config, peer).and_then(RealmNodeKind::owner) {
        Some(owner) => owner == user_id,
        None => true,
    }
}

pub(super) fn configured_kind(
    config: &RealmConfigDocument,
    node_id: NodeId,
) -> Option<&RealmNodeKind> {
    let node_id = node_id.to_string();
    config
        .nodes
        .iter()
        .find(|node| node.node_id == node_id)
        .map(|node| &node.kind)
}

pub(crate) async fn authorize_forwarded_pid(
    context: &Arc<DriverContext>,
    peer: NodeId,
    realm_id: RealmId,
    auth_token: Option<AuthToken>,
) -> Result<AuthContext, ForwardAuthError> {
    let Some(metadata_handle) = context.metadata_handle.as_ref() else {
        return Err(ForwardAuthError::Unavailable(
            "forwarded persistent id transition needs a metadata handle".to_string(),
        ));
    };
    let auth = metadata_handle
        .authorize_write_peer(peer, auth_token)
        .await
        .map_err(|error| match error {
            WritePeerError::Unauthorized => ForwardAuthError::Unauthorized,
            WritePeerError::Unavailable(error) => ForwardAuthError::Unavailable(error.to_string()),
        })?;
    if auth.realm_id != realm_id {
        return Err(ForwardAuthError::Forbidden);
    }
    Ok(auth)
}

pub(crate) async fn authorize_forwarded_caller(
    context: &Arc<DriverContext>,
    peer: NodeId,
    realm_id: RealmId,
    message: &MetadataTransportMessage,
) -> Result<AuthContext, ForwardAuthError> {
    let Some(metadata_handle) = context.metadata_handle.as_ref() else {
        return Err(ForwardAuthError::Unavailable(
            "forwarded metadata write needs a metadata handle".to_string(),
        ));
    };
    let auth_token = match message {
        MetadataTransportMessage::ForwardCreateDocument { auth_token, .. }
        | MetadataTransportMessage::ForwardUpdateDocument { auth_token, .. }
        | MetadataTransportMessage::ForwardDeleteDocument { auth_token, .. } => auth_token.clone(),
        MetadataTransportMessage::ForwardTokenRevocation { auth_token, .. } => {
            Some(auth_token.clone())
        }
        MetadataTransportMessage::ForwardCreatePolicy { auth_token, .. } => auth_token.clone(),
        MetadataTransportMessage::ForwardGroupCreate { auth_token, .. } => auth_token.clone(),
        MetadataTransportMessage::GroupDeletion { auth_token, .. } => auth_token.clone(),
        MetadataTransportMessage::ForwardApplyBatch { auth_token, .. } => Some(auth_token.clone()),
        _ => None,
    };
    let auth = metadata_handle
        .authorize_write_peer(peer, auth_token)
        .await
        .map_err(|error| match error {
            WritePeerError::Unauthorized => ForwardAuthError::Unauthorized,
            WritePeerError::Unavailable(error) => ForwardAuthError::Unavailable(error.to_string()),
        })?;
    if auth.realm_id != realm_id {
        return Err(ForwardAuthError::Forbidden);
    }
    // A User peer is owner-bound: whatever token it presents, the write it
    // forwards must be its owner's own.
    let Some(config) = load_realm_config(context, realm_id).await else {
        return Err(ForwardAuthError::Unavailable(
            "forwarded metadata write needs the realm configuration".to_string(),
        ));
    };
    if !peer_acts_for(&config, peer, auth.user_id) {
        return Err(ForwardAuthError::Forbidden);
    }
    Ok(auth)
}

pub(crate) async fn authorize_write(
    context: &Arc<DriverContext>,
    auth_context: AuthContext,
    path: String,
) -> Result<(), ForwardAuthError> {
    match authorize(
        context.as_ref(),
        auth_context.realm_id,
        &auth_context,
        &path,
        &Permission::WRITE,
        PolicyRequestExtras::rest(),
    )
    .await
    {
        Ok(()) => Ok(()),
        Err(AuthorizeError::PermissionDenied | AuthorizeError::Policy(_)) => {
            Err(ForwardAuthError::Forbidden)
        }
        Err(AuthorizeError::CheckFailed(error)) => Err(ForwardAuthError::Unavailable(error)),
        Err(AuthorizeError::Storage(error)) => {
            Err(ForwardAuthError::Unavailable(error.to_string()))
        }
    }
}

pub(crate) enum ForwardAuthError {
    Unauthorized,
    Forbidden,
    Unavailable(String),
}

pub(crate) fn forward_auth_error(error: ForwardAuthError) -> MetadataTransportMessage {
    match error {
        ForwardAuthError::Unauthorized => MetadataTransportMessage::ForwardedWriteDenied {
            error: WriteAuthError::Unauthorized,
        },
        ForwardAuthError::Forbidden => MetadataTransportMessage::ForwardedWriteDenied {
            error: WriteAuthError::Forbidden,
        },
        ForwardAuthError::Unavailable(error) => {
            warn!(%error, "Forwarded metadata authorization is unavailable");
            MetadataTransportMessage::ForwardedWriteUnavailable
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn authorize_create(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    auth: &AuthContext,
    extras: PolicyRequestExtras,
    group_id: GroupId,
    path: &str,
    document_id: Ulid,
) -> Result<(), AuthorizeError> {
    time_stage(
        "permission",
        authorize(
            context.as_ref(),
            realm_id,
            auth,
            &format!("/{realm_id}/g/{group_id}/meta/**"),
            &Permission::WRITE,
            extras.clone(),
        ),
    )
    .await?;
    time_stage(
        "permission",
        authorize(
            context.as_ref(),
            realm_id,
            auth,
            &MetadataRegistryRecord::permission_path_for(
                &auth.realm_id,
                group_id,
                path,
                document_id,
            ),
            &Permission::WRITE,
            extras,
        ),
    )
    .await?;
    Ok(())
}
