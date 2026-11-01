//! Routes persistent identifier requests to the one node that owns that document's PID state.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::driver::DriverContext;
use crate::metadata::api::MetadataApiError;
use crate::metadata::create_document::resolve_metadata_id;
use crate::metadata::get_document::load_document_record;
use crate::metadata::protocol::AuthToken;
use crate::metadata::protocol::MetadataReadError;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::metadata::protocol::PersistentIdOutcome;
use crate::metadata::protocol::PersistentIdRequest;
use crate::metadata::protocol::PersistentIdResolution;
use crate::placement::process_placements::load_realm_config;
use crate::placement::resolve_shard_holders;
use aruna_core::MetaResourceId;
use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::structs::MintPersistentSpec;
use aruna_core::structs::PersistentIdFailure;
use aruna_core::structs::PersistentIdMapping;
use aruna_core::structs::execution::job::JobId;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::{DocumentClass, PlacementScope};
use aruna_core::structs::secondary_id::SecondaryIdentifier;
use std::sync::Arc;
use tracing::warn;
use ulid::Ulid;

use crate::forward::authorize::ForwardAuthError;
use crate::forward::authorize::authorize_forwarded_pid;
use crate::forward::authorize::authorize_write;
use crate::forward::authorize::forward_auth_error;
use crate::forward::replay::document_deleted_here;
use crate::forward::replay::existing_record;
use crate::forward::transport::forward_to_holders;
use crate::forward::transport::read_error;
use crate::forward::transport::reject;
use crate::forward::transport::write_error;

/// The one node that owns a document's PID state: rank-0 holder of the placement
/// derived from the structured id, never the registry row (a delete removes it
/// while the mapping must survive for a permanent 410). All nodes derive the same.
pub(crate) fn pid_authority_node(
    config: &RealmConfigDocument,
    realm_id: RealmId,
    document_id: Ulid,
) -> Option<NodeId> {
    let placement = resolve_metadata_id(config, realm_id, None, document_id).ok()?;
    resolve_shard_holders(config, &placement).first().copied()
}

/// True only when the id provably names no metadata placement of this realm: it is no
/// structured id, or its binding has another class or realm. Unknown, conflicting or partial
/// bindings may still be replicating, so they stay an unreachable authority.
fn outside_realm(config: &RealmConfigDocument, realm_id: RealmId, document_id: Ulid) -> bool {
    let Ok(id) = MetaResourceId::from_bytes(document_id.to_bytes()) else {
        return true;
    };
    let bucket_count = |strategy_id: Ulid| {
        config
            .strategy(&strategy_id)
            .and_then(|strategy| u16::try_from(strategy.shard_count).ok())
    };
    match config.binding_directory().resolve_id(&id, bucket_count) {
        Ok(binding) => {
            binding.document_class != DocumentClass::Metadata
                || matches!(binding.scope, PlacementScope::Realm(owner) if owner != realm_id)
        }
        Err(_) => false,
    }
}

pub(super) async fn pid_authority(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    document_id: Ulid,
) -> Result<(RealmConfigDocument, NodeId), MetadataApiError> {
    let config = load_realm_config(context, realm_id)
        .await
        .ok_or(MetadataApiError::ServiceUnavailable)?;
    let authority = pid_authority_node(&config, realm_id, document_id)
        .ok_or(MetadataApiError::ServiceUnavailable)?;
    Ok((config, authority))
}

pub(super) fn is_local_node(context: &Arc<DriverContext>, node_id: NodeId) -> bool {
    context
        .net_handle
        .as_ref()
        .is_some_and(|net| net.node_id() == node_id)
}

/// Mint through the document's authority. Every other node forwards; none mints
/// into its own store, so one document has exactly one mapping row lineage.
pub async fn mint_pid_routed(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    document_id: Ulid,
    minted_by: UserId,
    minted_at_ms: u64,
    auth_token: Option<AuthToken>,
) -> Result<(PersistentIdMapping, bool), MetadataApiError> {
    if context.net_handle.is_none() {
        return crate::metadata::persistent_id::mint_persistent_id(
            context.as_ref(),
            realm_id,
            document_id,
            minted_by,
            minted_at_ms,
        )
        .await
        .map_err(pid_error);
    }
    let (config, authority) = pid_authority(context, realm_id, document_id).await?;
    if is_local_node(context, authority) {
        return crate::metadata::persistent_id::mint_persistent_id(
            context.as_ref(),
            realm_id,
            document_id,
            minted_by,
            minted_at_ms,
        )
        .await
        .map_err(pid_error);
    }
    let outcome = forward_pid(
        context,
        &config,
        authority,
        document_id,
        PersistentIdRequest::Mint {
            minted_by,
            minted_at_ms,
        },
        auth_token,
    )
    .await?;
    match outcome {
        PersistentIdOutcome::Mapping { mapping, changed } => Ok((*mapping, changed)),
        _ => Err(MetadataApiError::ServiceUnavailable),
    }
}

/// Queue the PID mint job on the document's authority. The job store is
/// node-local, so a document-scoped dedup row only deduplicates when one node
/// owns it; alternating ingress nodes would otherwise open a job each.
pub async fn submit_pid_routed(
    context: &Arc<DriverContext>,
    document_id: Ulid,
    minted_by: UserId,
    local_node_id: NodeId,
    retention_ms: u64,
    auth_token: Option<AuthToken>,
) -> Result<(JobId, bool), MetadataApiError> {
    let realm_id = context.serving_realm(minted_by.realm_id);
    if let Some(job_id) = read_pid_routed(context, realm_id, document_id)
        .await?
        .and_then(|mapping| mapping.job_id)
    {
        return Ok((job_id, false));
    }
    if context.net_handle.is_none() {
        return submit_pid_local(context, document_id, minted_by, local_node_id, retention_ms)
            .await;
    }
    let (config, authority) = pid_authority(context, realm_id, document_id).await?;
    if is_local_node(context, authority) {
        return submit_pid_local(context, document_id, minted_by, authority, retention_ms).await;
    }
    let outcome = forward_pid(
        context,
        &config,
        authority,
        document_id,
        PersistentIdRequest::SubmitMint {
            minted_by,
            retention_ms,
        },
        auth_token,
    )
    .await?;
    match outcome {
        PersistentIdOutcome::Submission { job_id, created } => Ok((job_id, created)),
        _ => Err(MetadataApiError::ServiceUnavailable),
    }
}

pub(super) async fn submit_pid_local(
    context: &Arc<DriverContext>,
    document_id: Ulid,
    minted_by: UserId,
    owner_node_id: NodeId,
    retention_ms: u64,
) -> Result<(JobId, bool), MetadataApiError> {
    crate::jobs::service::submit_mint_local(
        context.as_ref(),
        MintPersistentSpec {
            document_id,
            minted_by,
        },
        owner_node_id,
        retention_ms,
    )
    .await
    .map(|result| (result.job_id, result.created))
    .map_err(|error| MetadataApiError::Internal(error.to_string()))
}

/// Explicit withdrawal through the document's authority.
pub async fn withdraw_pid_routed(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    document_id: Ulid,
    withdrawn_by: UserId,
    reason: String,
    withdrawn_at_ms: u64,
    auth_token: Option<AuthToken>,
) -> Result<PersistentIdMapping, MetadataApiError> {
    if context.net_handle.is_none() {
        return crate::metadata::persistent_id::admin_withdraw_pid(
            context.as_ref(),
            realm_id,
            document_id,
            withdrawn_by,
            reason,
            withdrawn_at_ms,
        )
        .await
        .map(|(mapping, _)| mapping)
        .map_err(pid_error);
    }
    let (config, authority) = pid_authority(context, realm_id, document_id).await?;
    if is_local_node(context, authority) {
        return crate::metadata::persistent_id::admin_withdraw_pid(
            context.as_ref(),
            realm_id,
            document_id,
            withdrawn_by,
            reason,
            withdrawn_at_ms,
        )
        .await
        .map(|(mapping, _)| mapping)
        .map_err(pid_error);
    }
    let outcome = forward_pid(
        context,
        &config,
        authority,
        document_id,
        PersistentIdRequest::Withdraw {
            withdrawn_by,
            reason,
            withdrawn_at_ms,
        },
        auth_token,
    )
    .await?;
    match outcome {
        PersistentIdOutcome::Mapping { mapping, .. } => Ok(*mapping),
        _ => Err(MetadataApiError::ServiceUnavailable),
    }
}

/// Adds external identifiers through the document's authority, which owns the mapping lineage.
/// The caller needs WRITE on the document on every path, as a forwarded request re-checks.
pub async fn add_identifiers_routed(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    document_id: Ulid,
    identifiers: Vec<SecondaryIdentifier>,
    occurred_at_ms: u64,
    auth: AuthContext,
) -> Result<(PersistentIdMapping, bool), MetadataApiError> {
    let add = async || {
        let record = existing_record(context, document_id)
            .await
            .map_err(MetadataApiError::Internal)?
            .ok_or(MetadataApiError::NotFound)?;
        authorize_write(context, auth.clone(), record.permission_path)
            .await
            .map_err(|error| match error {
                ForwardAuthError::Unauthorized => MetadataApiError::Unauthorized,
                ForwardAuthError::Forbidden => MetadataApiError::Forbidden,
                ForwardAuthError::Unavailable(_) => MetadataApiError::ServiceUnavailable,
            })?;
        crate::metadata::persistent_id::add_secondary_ids(
            context.as_ref(),
            realm_id,
            document_id,
            identifiers.clone(),
            occurred_at_ms,
        )
        .await
        .map_err(pid_error)
    };
    if context.net_handle.is_none() {
        return add().await;
    }
    let (config, authority) = pid_authority(context, realm_id, document_id).await?;
    if is_local_node(context, authority) {
        return add().await;
    }
    let auth_token = Some(AuthToken::internal(auth.clone()));
    let outcome = forward_pid(
        context,
        &config,
        authority,
        document_id,
        PersistentIdRequest::AddIdentifiers {
            identifiers: identifiers.clone(),
            occurred_at_ms,
        },
        auth_token,
    )
    .await?;
    match outcome {
        PersistentIdOutcome::Mapping { mapping, changed } => Ok((*mapping, changed)),
        _ => Err(MetadataApiError::ServiceUnavailable),
    }
}

/// Read the typed intent from the document's PID authority. This reads only the
/// mapping; the HTTP layer applies its visibility/permission contract before
/// returning any status.
pub async fn read_pid_routed(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    document_id: Ulid,
) -> Result<Option<PersistentIdMapping>, MetadataApiError> {
    if context.net_handle.is_none() {
        return crate::metadata::persistent_id::read_mapping(context.as_ref(), document_id)
            .await
            .map_err(pid_error);
    }
    let (config, authority) = pid_authority(context, realm_id, document_id).await?;
    if is_local_node(context, authority) {
        return crate::metadata::persistent_id::read_mapping(context.as_ref(), document_id)
            .await
            .map_err(pid_error);
    }
    match forward_pid(
        context,
        &config,
        authority,
        document_id,
        PersistentIdRequest::Status,
        None,
    )
    .await?
    {
        PersistentIdOutcome::Status(mapping) => Ok(mapping.map(|mapping| *mapping)),
        _ => Err(MetadataApiError::ServiceUnavailable),
    }
}

/// Store a terminal provider failure on the authority. Only the internal mint
/// worker calls this; HTTP callers have no transition that can forge failures.
pub async fn fail_pid_routed(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    document_id: Ulid,
    failure: PersistentIdFailure,
    auth_token: AuthToken,
) -> Result<PersistentIdMapping, MetadataApiError> {
    if context.net_handle.is_none() {
        return crate::metadata::persistent_id::fail_persistent_id(
            context.as_ref(),
            realm_id,
            document_id,
            failure,
        )
        .await
        .map(|(mapping, _)| mapping)
        .map_err(pid_error);
    }
    let (config, authority) = pid_authority(context, realm_id, document_id).await?;
    if is_local_node(context, authority) {
        return crate::metadata::persistent_id::fail_persistent_id(
            context.as_ref(),
            realm_id,
            document_id,
            failure,
        )
        .await
        .map(|(mapping, _)| mapping)
        .map_err(pid_error);
    }
    match forward_pid(
        context,
        &config,
        authority,
        document_id,
        PersistentIdRequest::Fail { failure },
        Some(auth_token),
    )
    .await?
    {
        PersistentIdOutcome::Mapping { mapping, .. } => Ok(*mapping),
        _ => Err(MetadataApiError::ServiceUnavailable),
    }
}

/// Resolve a landing request on the document's authority, which is the only node
/// that answers: replica rows carry no comparable version, so folding answers
/// could promote a stale redirect or premature mapping. Others proxy or fail.
pub async fn resolve_pid_routed(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    document_id: Ulid,
    pid: String,
) -> Result<PersistentIdResolution, MetadataApiError> {
    if context.net_handle.is_none() {
        return local_pid_resolution(context, realm_id, document_id, &pid).await;
    }
    let config = load_realm_config(context, realm_id)
        .await
        .ok_or(MetadataApiError::ServiceUnavailable)?;
    if outside_realm(&config, realm_id, document_id) {
        return Ok(PersistentIdResolution::Missing);
    }
    let authority = pid_authority_node(&config, realm_id, document_id)
        .ok_or(MetadataApiError::ServiceUnavailable)?;
    if is_local_node(context, authority) {
        return local_pid_resolution(context, realm_id, document_id, &pid).await;
    }
    let outcome = forward_pid(
        context,
        &config,
        authority,
        document_id,
        PersistentIdRequest::Resolve { pid },
        None,
    )
    .await?;
    match outcome {
        PersistentIdOutcome::Resolution(resolution) => Ok(resolution),
        _ => Err(MetadataApiError::ServiceUnavailable),
    }
}

/// The authority's own answer: a withdrawn mapping of a public intent is a permanent 410,
/// an active one redirects only while the document is anonymously readable now,
/// and everything else is indistinguishable from unminted.
pub(super) async fn local_pid_resolution(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    document_id: Ulid,
    expected_pid: &str,
) -> Result<PersistentIdResolution, MetadataApiError> {
    let mapping = crate::metadata::persistent_id::read_mapping(context.as_ref(), document_id)
        .await
        .map_err(pid_error)?;
    let Some(mapping) = mapping else {
        return Ok(PersistentIdResolution::Missing);
    };
    if mapping.pid != expected_pid {
        return Ok(PersistentIdResolution::Missing);
    }
    if mapping.is_retired() {
        // The record may be gone, so a withdrawal keeps the visibility frozen at creation.
        return Ok(match mapping.public {
            Some(false) => PersistentIdResolution::Missing,
            _ => PersistentIdResolution::Gone { pid: mapping.pid },
        });
    }
    if !mapping.is_active() {
        return Ok(PersistentIdResolution::Missing);
    }
    let record = load_document_record(context.as_ref(), document_id)
        .await
        .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    // An active mapping without a registry row is a permanent 410 only once this node has
    // evidence the document was created here and is gone.
    let Some(record) = record else {
        return if document_deleted_here(context, document_id).await? {
            Ok(PersistentIdResolution::Gone { pid: mapping.pid })
        } else {
            Err(MetadataApiError::ServiceUnavailable)
        };
    };
    if crate::metadata::api::can_read_record(context.as_ref(), realm_id, None, &record).await? {
        Ok(PersistentIdResolution::Redirect)
    } else {
        Ok(PersistentIdResolution::Missing)
    }
}

pub(super) async fn forward_pid(
    context: &Arc<DriverContext>,
    config: &RealmConfigDocument,
    authority: NodeId,
    document_id: Ulid,
    request: PersistentIdRequest,
    auth_token: Option<AuthToken>,
) -> Result<PersistentIdOutcome, MetadataApiError> {
    let config_digest = config
        .digest()
        .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    let response = forward_to_holders(
        context,
        &[authority],
        MetadataTransportMessage::ForwardPersistentId {
            auth_token,
            config_digest,
            document_id,
            request,
        },
        None,
        false,
    )
    .await
    .map_err(write_error)?;
    match response {
        MetadataTransportMessage::ForwardedPersistentId {
            result: Ok(outcome),
        } => Ok(outcome),
        MetadataTransportMessage::ForwardedPersistentId { result: Err(error) } => {
            Err(match error {
                MetadataReadError::Unauthorized => MetadataApiError::Unauthorized,
                MetadataReadError::Forbidden => MetadataApiError::Forbidden,
                MetadataReadError::NotFound => MetadataApiError::NotFound,
                MetadataReadError::Unavailable => MetadataApiError::ServiceUnavailable,
            })
        }
        _ => Err(MetadataApiError::ServiceUnavailable),
    }
}

/// Applies a PID transition or landing resolution on the authority.
pub(crate) async fn apply_forwarded_pid(
    context: &Arc<DriverContext>,
    peer: NodeId,
    message: MetadataTransportMessage,
) -> MetadataTransportMessage {
    let MetadataTransportMessage::ForwardPersistentId {
        auth_token,
        config_digest,
        document_id,
        request,
    } = message
    else {
        return reject("unexpected forwarded persistent id message");
    };
    let Some(net_handle) = context.net_handle.as_ref() else {
        return MetadataTransportMessage::ForwardedWriteUnavailable;
    };
    let realm_id = *net_handle.realm_id();
    let Some(config) = load_realm_config(context, realm_id).await else {
        return MetadataTransportMessage::ForwardedWriteUnavailable;
    };
    if config.digest().ok() != Some(config_digest) {
        return MetadataTransportMessage::ForwardedWriteUnavailable;
    }
    if pid_authority_node(&config, realm_id, document_id) != Some(net_handle.node_id()) {
        return MetadataTransportMessage::ForwardedWriteUnavailable;
    }
    match &request {
        PersistentIdRequest::Resolve { pid } => {
            let result = local_pid_resolution(context, realm_id, document_id, pid)
                .await
                .map(PersistentIdOutcome::Resolution)
                .map_err(read_error);
            return MetadataTransportMessage::ForwardedPersistentId { result };
        }
        PersistentIdRequest::Status => {
            let result =
                crate::metadata::persistent_id::read_mapping(context.as_ref(), document_id)
                    .await
                    .map(|mapping| PersistentIdOutcome::Status(mapping.map(Box::new)))
                    .map_err(|_| MetadataReadError::Unavailable);
            return MetadataTransportMessage::ForwardedPersistentId { result };
        }
        _ => {}
    }

    // Transitions carry the caller's authority: forwarding is a routing hop, so
    // the holder re-runs the WRITE check the origin's handler ran.
    let internal = matches!(&auth_token, Some(AuthToken::Internal(_)));
    let auth = match authorize_forwarded_pid(context, peer, realm_id, auth_token).await {
        Ok(auth) => auth,
        Err(error) => return forward_auth_error(error),
    };
    // The minting subject is the token's own subject: a routing hop may not
    // attribute a mint to a user it merely relays for.
    let minting_subject = match &request {
        PersistentIdRequest::Mint { minted_by, .. }
        | PersistentIdRequest::SubmitMint { minted_by, .. } => Some(*minted_by),
        PersistentIdRequest::Withdraw { withdrawn_by, .. } => Some(*withdrawn_by),
        _ => None,
    };
    if minting_subject.is_some_and(|minted_by| minted_by != auth.user_id) {
        return forward_auth_error(ForwardAuthError::Forbidden);
    }
    let record = match existing_record(context, document_id).await {
        Ok(Some(record)) => Some(record),
        Ok(None) => None,
        Err(error) => return reject(error),
    };
    match (&request, record.as_ref()) {
        (PersistentIdRequest::Fail { .. }, _) if !internal => {
            return forward_auth_error(ForwardAuthError::Forbidden);
        }
        (PersistentIdRequest::Mint { .. } | PersistentIdRequest::Fail { .. }, _) if internal => {}
        (PersistentIdRequest::Withdraw { .. }, Some(_)) => {
            if let Err(error) = authorize_write(
                context,
                auth.clone(),
                format!("/{realm_id}/admin/pids/{document_id}"),
            )
            .await
            {
                return forward_auth_error(error);
            }
        }
        (_, Some(record)) => {
            if let Err(error) =
                authorize_write(context, auth.clone(), record.permission_path.clone()).await
            {
                return forward_auth_error(error);
            }
        }
        // Without a registry row there is no permission path that can authorize a transition.
        (_, None) => return MetadataTransportMessage::WriteNotFound,
    }

    let outcome = match request {
        PersistentIdRequest::Mint {
            minted_by,
            minted_at_ms,
        } => crate::metadata::persistent_id::mint_persistent_id(
            context.as_ref(),
            realm_id,
            document_id,
            minted_by,
            minted_at_ms,
        )
        .await
        .map(|(mapping, changed)| PersistentIdOutcome::Mapping {
            mapping: Box::new(mapping),
            changed,
        }),
        PersistentIdRequest::Withdraw {
            withdrawn_by,
            reason,
            withdrawn_at_ms,
        } => crate::metadata::persistent_id::admin_withdraw_pid(
            context.as_ref(),
            realm_id,
            document_id,
            withdrawn_by,
            reason,
            withdrawn_at_ms,
        )
        .await
        .map(|(mapping, changed)| PersistentIdOutcome::Mapping {
            mapping: Box::new(mapping),
            changed,
        }),
        PersistentIdRequest::SubmitMint {
            minted_by,
            retention_ms,
        } => {
            return match submit_pid_local(
                context,
                document_id,
                minted_by,
                net_handle.node_id(),
                retention_ms,
            )
            .await
            {
                Ok((job_id, created)) => MetadataTransportMessage::ForwardedPersistentId {
                    result: Ok(PersistentIdOutcome::Submission { job_id, created }),
                },
                Err(error) => {
                    warn!(%document_id, ?error, "Forwarded persistent id job submission failed");
                    MetadataTransportMessage::ForwardedWriteUnavailable
                }
            };
        }
        PersistentIdRequest::Fail { failure } => {
            crate::metadata::persistent_id::fail_persistent_id(
                context.as_ref(),
                realm_id,
                document_id,
                failure,
            )
            .await
            .map(|(mapping, changed)| PersistentIdOutcome::Mapping {
                mapping: Box::new(mapping),
                changed,
            })
        }
        PersistentIdRequest::AddIdentifiers {
            identifiers,
            occurred_at_ms,
        } => crate::metadata::persistent_id::add_secondary_ids(
            context.as_ref(),
            realm_id,
            document_id,
            identifiers,
            occurred_at_ms,
        )
        .await
        .map(|(mapping, changed)| PersistentIdOutcome::Mapping {
            mapping: Box::new(mapping),
            changed,
        }),
        PersistentIdRequest::Resolve { .. } | PersistentIdRequest::Status => {
            unreachable!("read-only request returned above")
        }
    };
    match outcome {
        Ok(outcome) => MetadataTransportMessage::ForwardedPersistentId {
            result: Ok(outcome),
        },
        Err(
            crate::metadata::persistent_id::PersistentIdError::DocumentMissing
            | crate::metadata::persistent_id::PersistentIdError::IntentMissing,
        ) => MetadataTransportMessage::WriteNotFound,
        Err(error) => {
            warn!(%document_id, ?error, "Forwarded persistent id transition failed");
            MetadataTransportMessage::ForwardedWriteUnavailable
        }
    }
}

pub(super) fn pid_error(
    error: crate::metadata::persistent_id::PersistentIdError,
) -> MetadataApiError {
    match error {
        crate::metadata::persistent_id::PersistentIdError::DocumentMissing
        | crate::metadata::persistent_id::PersistentIdError::IntentMissing => {
            MetadataApiError::NotFound
        }
        error => MetadataApiError::Internal(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::placement::record::{
        FIRST_GRANTABLE_HANDLE, HANDLE_RANGE_SIZE, HandleRange, METADATA_HANDLE, PlacementBinding,
    };
    use aruna_core::{BucketId, PlacementHandle, StructuredId};

    fn minted(handle: u32) -> Ulid {
        let handle = PlacementHandle::new(handle).unwrap();
        let bucket = BucketId::new(0).unwrap();
        MetaResourceId::from_parts(1, handle, bucket, 7)
            .unwrap()
            .as_ulid()
    }

    #[test]
    fn outside_only_provably() {
        let realm_id = RealmId::from_bytes([5; 32]);
        let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        config.seed_default_placement();
        let strategy_id = config.default_strategy_id.unwrap();
        let owner = iroh::SecretKey::from_bytes(&[8; 32]).public();
        let range_id = Ulid::from_bytes([19; 16]);
        config.placement_handle_ranges.push(HandleRange {
            range_id,
            owner,
            start: FIRST_GRANTABLE_HANDLE,
            end: FIRST_GRANTABLE_HANDLE + HANDLE_RANGE_SIZE,
        });
        let bind = |handle, scope, document_class| PlacementBinding {
            handle: PlacementHandle::new(handle).unwrap(),
            scope,
            document_class,
            strategy_id,
            allocator_range_id: Some(range_id),
            allocated_by: Some(owner),
            allocated_at_ms: Some(1),
        };
        let other = RealmId::from_bytes([6; 32]);
        config.placement_bindings.extend([
            bind(
                500,
                PlacementScope::Realm(realm_id),
                DocumentClass::JobControl,
            ),
            bind(501, PlacementScope::Realm(other), DocumentClass::Metadata),
            bind(
                502,
                PlacementScope::Group(Ulid::from_bytes([1; 16])),
                DocumentClass::Metadata,
            ),
            bind(
                504,
                PlacementScope::Realm(realm_id),
                DocumentClass::Metadata,
            ),
            bind(504, PlacementScope::Realm(other), DocumentClass::Metadata),
        ]);
        // No structured id, another class or another realm: never minted here, a 404.
        let reserved = Ulid::from_parts(1_700_000_000_000, 0x1234_5678);
        assert!(outside_realm(&config, realm_id, reserved));
        assert!(outside_realm(&config, realm_id, minted(500)));
        assert!(outside_realm(&config, realm_id, minted(501)));
        assert!(!outside_realm(&config, realm_id, minted(METADATA_HANDLE)));
        assert!(!outside_realm(&config, realm_id, minted(502)));
        // An unbound, conflicting or out of range placement, as a v2 id carries, may still be
        // replicating: unavailable, not 404.
        let legacy = Ulid::from_string("01H8Y3R7Z5X9C2V4B6N8M0Q1W3").unwrap();
        assert!(!outside_realm(&config, realm_id, legacy));
        assert!(!outside_realm(&config, realm_id, minted(503)));
        assert!(!outside_realm(&config, realm_id, minted(504)));
        let handle = PlacementHandle::new(METADATA_HANDLE).unwrap();
        let far = MetaResourceId::from_parts(1, handle, BucketId::new(4095).unwrap(), 7).unwrap();
        assert!(!outside_realm(&config, realm_id, far.as_ulid()));
        // A strategy unknown here is indeterminate too.
        config
            .strategies
            .retain(|strategy| strategy.strategy_id != strategy_id);
        assert!(!outside_realm(&config, realm_id, minted(METADATA_HANDLE)));
    }
}
