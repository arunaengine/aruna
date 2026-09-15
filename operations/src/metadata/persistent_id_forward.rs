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
use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::structs::JobId;
use aruna_core::structs::MintPersistentSpec;
use aruna_core::structs::PersistentIdFailure;
use aruna_core::structs::PersistentIdMapping;
use aruna_core::structs::RealmConfigDocument;
use aruna_core::structs::RealmId;
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
    let realm_id = minted_by.realm_id;
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
    let (config, authority) = pid_authority(context, realm_id, document_id).await?;
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

/// The authority's own answer: a withdrawn mapping is a permanent 410 whatever
/// the document's visibility, an active one redirects only while the document is
/// anonymously readable, and everything else is indistinguishable from unminted.
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
    if mapping.public == Some(false) {
        return Ok(PersistentIdResolution::Missing);
    }
    if mapping.is_retired() {
        return Ok(PersistentIdResolution::Gone { pid: mapping.pid });
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
        (_, None) => return MetadataTransportMessage::ForwardedWriteNotFound,
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
        ) => MetadataTransportMessage::ForwardedWriteNotFound,
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
