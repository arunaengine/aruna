//! Gathers a group's audit trail across the realm and serves this node's audit page to peers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::request_authorization::AuthorizeError;
use crate::auth::request_authorization::authorize;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::driver::DriverContext;
use crate::driver::drive_until;
use crate::metadata::protocol::MetadataReadError;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::realm::load_config::load_realm_config;
use aruna_core::audit::AuditPageRequest;
use aruna_core::audit::AuditPageResponse;
use aruna_core::audit::validate_request;
use aruna_core::effects::AuditPageEffect;
use aruna_core::id::NodeId;
use aruna_core::metadata::AuthToken;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::identity::realm::RealmId;
use std::sync::Arc;

use crate::metadata::audit::{
    AUDIT_INBOUND_ADMISSION, AUDIT_OUTBOUND_ADMISSION, AUDIT_PAGE_LIMIT, AUDIT_PAGE_SIZE,
    AuditAggregate, ListAuditError, ListAuditOperation, ListAuditRequest, LocalPageOperation,
    decode_cursor,
};

/// Gathers the group's audit trail from every eligible realm node and merges the
/// pages. `local_node` reads its own keyspace directly; the remaining nodes are
/// queried over the metadata control transport.
pub async fn list_audit(
    context: &DriverContext,
    realm_id: RealmId,
    local_node: NodeId,
    auth_token: Option<AuthToken>,
    request: ListAuditRequest,
    deadline: tokio::time::Instant,
) -> Result<AuditAggregate, ListAuditError> {
    let _admission = AUDIT_OUTBOUND_ADMISSION
        .clone()
        .try_acquire_owned()
        .map_err(|_| ListAuditError::Unavailable)?;
    let limit = request
        .limit
        .unwrap_or(AUDIT_PAGE_SIZE)
        .clamp(1, AUDIT_PAGE_LIMIT);

    // Membership and its digest are eventual candidates; each peer revalidates them.
    let mut partial = false;
    let mut config_document = None;
    let mut config_digest = [0u8; 32];
    let mut digest_ready = false;
    let mut include_local = false;
    let mut user_origin = false;
    let config = tokio::time::timeout_at(deadline, load_realm_config(context, realm_id))
        .await
        .map_err(|_| ListAuditError::Unavailable)?;
    match config {
        None => partial = true,
        Some(config) => {
            match config.digest() {
                Ok(digest) => {
                    config_digest = digest;
                    digest_ready = true;
                }
                Err(_) => partial = true,
            }
            match config
                .nodes
                .iter()
                .find(|node| node.node_id == local_node.to_string())
            {
                Some(node) => {
                    let current_local_authorized = node.kind.is_sync_eligible();
                    user_origin = !current_local_authorized;
                    include_local = request.local_authorized && current_local_authorized;
                    if request.local_authorized != current_local_authorized {
                        partial = true;
                    }
                }
                None => partial = true,
            }
            config_document = Some(config);
        }
    }
    if request.cursor.is_some() && !digest_ready {
        return Err(ListAuditError::InvalidCursor);
    }
    if user_origin && auth_token.is_none() {
        return Err(ListAuditError::Unauthorized);
    }
    let start_after = request
        .cursor
        .as_deref()
        .map(|cursor| {
            decode_cursor(
                cursor,
                realm_id,
                config_digest,
                request.group_id,
                request.document_id,
            )
        })
        .transpose()?;

    let mut invalid_peer = false;
    let peer_nodes = config_document
        .as_ref()
        .into_iter()
        .flat_map(|config| config.nodes.iter())
        .filter(|node| node.kind.is_sync_eligible())
        .filter_map(|node| match node.node_id.parse::<NodeId>() {
            Ok(node) if node != local_node && digest_ready => Some(node),
            Ok(_) => None,
            Err(_) => {
                invalid_peer = true;
                None
            }
        });
    let operation = ListAuditOperation::new(
        realm_id,
        local_node,
        include_local,
        request.group_id,
        request.document_id,
        peer_nodes,
        start_after,
        limit,
        auth_token,
        config_digest,
    );
    partial |= invalid_peer;
    let mut aggregate = drive_until(operation, context, deadline).await?;
    aggregate.partial |= partial;
    if aggregate.partial {
        aggregate.next_cursor = None;
    }
    Ok(aggregate)
}

/// Serves a peer's request for this node's local audit page after re-checking the
/// caller's group-admin authority under the forwarded token.
pub(crate) async fn serve_local_audit(
    context: &Arc<DriverContext>,
    peer: NodeId,
    request: AuditPageRequest,
    deadline: tokio::time::Instant,
) -> MetadataTransportMessage {
    MetadataTransportMessage::ForwardedAuditPage {
        result: local_audit_result(context, peer, request, deadline).await,
    }
}

async fn local_audit_result(
    context: &Arc<DriverContext>,
    peer: NodeId,
    request: AuditPageRequest,
    deadline: tokio::time::Instant,
) -> Result<AuditPageResponse, MetadataReadError> {
    let _admission = AUDIT_INBOUND_ADMISSION
        .clone()
        .try_acquire_owned()
        .map_err(|_| MetadataReadError::Unavailable)?;
    let net_handle = context
        .net_handle
        .as_ref()
        .ok_or(MetadataReadError::Unavailable)?;
    let realm_id = *net_handle.realm_id();
    if request.realm_id != realm_id {
        return Err(MetadataReadError::Unavailable);
    }
    validate_request(&request).map_err(|_| MetadataReadError::Unavailable)?;
    let config = tokio::time::timeout_at(deadline, load_realm_config(context.as_ref(), realm_id))
        .await
        .map_err(|_| MetadataReadError::Unavailable)?
        .ok_or(MetadataReadError::Unavailable)?;
    if config.digest().ok() != Some(request.config_digest) {
        return Err(MetadataReadError::Unavailable);
    }
    let local_node = net_handle.node_id();
    if !audit_member(&config, realm_id, local_node) {
        return Err(MetadataReadError::Unavailable);
    }
    let metadata = context
        .metadata_handle
        .as_ref()
        .ok_or(MetadataReadError::Unavailable)?;
    let auth_result = tokio::time::timeout_at(
        deadline,
        metadata.authorize_read_peer(peer, request.auth_token.clone(), false),
    )
    .await
    .map_err(|_| MetadataReadError::Unavailable)?;
    let auth = auth_result?.ok_or(MetadataReadError::Unauthorized)?;
    let path = format!("/{realm_id}/g/{}/admin", request.group_id);
    let auth_result = tokio::time::timeout_at(deadline, authorize_admin(context, auth, path))
        .await
        .map_err(|_| MetadataReadError::Unavailable)?;
    auth_result?;

    let operation = LocalPageOperation::new(
        realm_id,
        request.group_id,
        request.document_id,
        request.start_after,
        request.limit.clamp(1, AUDIT_PAGE_LIMIT),
    );
    drive_until(operation, context.as_ref(), deadline)
        .await
        .map_err(|_| MetadataReadError::Unavailable)
}

fn audit_member(config: &RealmConfigDocument, realm_id: RealmId, node_id: NodeId) -> bool {
    config.realm_id == realm_id
        && config
            .nodes
            .iter()
            .any(|node| node.node_id == node_id.to_string() && node.kind.is_sync_eligible())
}

pub(super) async fn authorize_admin(
    context: &Arc<DriverContext>,
    auth_context: AuthContext,
    path: String,
) -> Result<(), MetadataReadError> {
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
            Err(MetadataReadError::Forbidden)
        }
        Err(AuthorizeError::CheckFailed(_) | AuthorizeError::Storage(_)) => {
            Err(MetadataReadError::Unavailable)
        }
    }
}

/// Adapter I/O for [`AuditPageEffect`]: requests one node's local page over the
/// metadata control transport. Kept out of the sans-I/O operation.
pub(crate) async fn send_audit_request(
    context: &DriverContext,
    node: NodeId,
    request: AuditPageRequest,
) -> Result<AuditPageResponse, MetadataReadError> {
    let metadata = context
        .metadata_handle
        .as_ref()
        .ok_or(MetadataReadError::Unavailable)?;
    match metadata
        .request_forwarded_write(node, MetadataTransportMessage::ForwardAuditPage { request })
        .await
    {
        Ok(MetadataTransportMessage::ForwardedAuditPage { result }) => result,
        Ok(_) | Err(_) => Err(MetadataReadError::Unavailable),
    }
}
