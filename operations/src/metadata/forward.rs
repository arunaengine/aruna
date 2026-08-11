use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::auth::{bearer_token_hash, valid_revocation_expiry};
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    METADATA_CREATE_ACCEPTANCE_KEYSPACE, METADATA_PENDING_PROJECTION_KEYSPACE,
};
use aruna_core::metadata::{MetadataCreateEventRecord, MetadataError, MetadataQueryResults};
use aruna_core::storage_entries::metadata_create_acceptance_key;
use aruna_core::structs::{
    Actor, AuthContext, JobId, MetadataRegistryRecord, MintPersistentIdSpec, Permission,
    PersistentIdMapping, PlacementRef, RealmConfigDocument, RealmId, RealmNodeKind,
};
use aruna_core::types::UserId;
use aruna_core::util::unix_timestamp_secs;
use aruna_core::{MetaResourceId, StructuredId};
use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use thiserror::Error;
use tokio::time::{Instant, timeout};
use tracing::{error, warn};
use ulid::Ulid;

use crate::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentError, CreateMetadataDocumentOperation,
    CreateMetadataDocumentResult, accepted_create_matches, create_metadata_document,
    mint_forward_document, resolve_metadata_id,
};
use crate::delete_metadata_document::{
    DeleteMetadataDocumentError, DeleteMetadataDocumentOperation, delete_metadata_document,
};
use crate::driver::{DriverContext, drive};
use crate::get_metadata_document::load_metadata_record_by_document;
use crate::metadata::api::{
    ExportMetadataRoCrateRequest, ExportMetadataRoCrateResult, GetVisibleMetadataDocumentRequest,
    MetadataApiError, export_metadata_rocrate, get_visible_metadata_document,
};
use crate::metadata::handle::{
    MetadataRequestDelivery, MetadataRequestError, MetadataWritePeerError,
};
use crate::metadata::protocol::{
    MetadataAuthToken, MetadataReadError, MetadataTransportMessage, MetadataWriteAuthError,
    PersistentIdOutcome, PersistentIdRequest, PersistentIdResolution,
};
use crate::placement::selector::{ROLE_NODE, neg_log2_q48, selector_hash};
use crate::placement::{
    MAX_READ_HOLDERS, holds_placement, resolve_holders_limit, resolve_shard_holders,
};
use crate::process_placements::load_realm_config;
use crate::request_authorization::{AuthorizeError, authorize};
use crate::request_policy::PolicyRequestExtras;
use crate::revoke_token::{
    RevokeTokenAdmission, RevokeTokenConfig, RevokeTokenError, RevokeTokenOperation,
};
use crate::update_metadata_document::{
    UpdateMetadataDocumentConfig, UpdateMetadataDocumentError, UpdateMetadataDocumentMutation,
    UpdateMetadataDocumentOperation, update_metadata_document,
};

/// Where a metadata write must be applied.
///
/// Topic membership is the bucket's holder set, so a non-holder can neither
/// publish the write nor join the topic to try: the mutation goes to a holder
/// instead. Membership is never widened to admit the origin — that would grow
/// every bucket toward every node and dissolve sharding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataWriteRoute {
    Local,
    /// Holders of the document's bucket in rank order (rank-0 first).
    Forward(Vec<NodeId>),
}

#[derive(Debug, Error)]
pub enum MetadataWriteError {
    #[error("metadata write requires authentication")]
    Unauthorized,
    #[error("metadata write is forbidden")]
    Forbidden,
    #[error("metadata document not found")]
    NotFound,
    #[error(transparent)]
    Create(#[from] CreateMetadataDocumentError),
    #[error(transparent)]
    Update(#[from] UpdateMetadataDocumentError),
    #[error(transparent)]
    Delete(#[from] DeleteMetadataDocumentError),
    /// The write reached a node that cannot publish it and no holder accepted
    /// the forward. Loud by construction: never accepted, never deferred into an
    /// outbox that can only drain to a topic this node may not join.
    #[error("metadata write is undeliverable: {0}")]
    Undeliverable(String),
}

const TOKEN_REVOKE_PEER_LIMIT: usize = 4;
const TOKEN_REVOKE_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(3);
const TOKEN_REVOKE_DEADLINE: Duration = Duration::from_secs(15);
const METADATA_READ_FANOUT_LIMIT: usize = 8;
const METADATA_READ_PEER_TIMEOUT: Duration = Duration::from_secs(2);
const METADATA_READ_DEADLINE: Duration = Duration::from_secs(12);

/// Route for a write against `placement`, from the local node's point of view.
///
/// [`PlacementRef::NIL`] has no governing strategy (early bootstrap), so the
/// local node stays the authority even without readable config. A non-NIL
/// placement needs config to establish authority; when config is unavailable,
/// an empty forward route fails closed as undeliverable.
pub fn write_route(
    config: Option<&RealmConfigDocument>,
    placement: &PlacementRef,
    local_node_id: NodeId,
) -> MetadataWriteRoute {
    let Some(config) = config else {
        return if *placement == PlacementRef::NIL {
            MetadataWriteRoute::Local
        } else {
            MetadataWriteRoute::Forward(Vec::new())
        };
    };
    if holds_placement(config, placement, local_node_id) {
        return MetadataWriteRoute::Local;
    }
    MetadataWriteRoute::Forward(resolve_shard_holders(config, placement))
}

/// User-kind nodes hold no metadata or authorization buckets, so their HTTP
/// write handlers must defer permission checks to the selected holder.
pub async fn is_user_origin(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    local_node_id: NodeId,
) -> Result<bool, MetadataApiError> {
    let config = drive(
        crate::get_realm_config::GetRealmConfigOperation::new(realm_id),
        context.as_ref(),
    )
    .await
    .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    let node = config
        .nodes
        .into_iter()
        .find(|node| node.node_id == local_node_id.to_string())
        .ok_or(MetadataApiError::ServiceUnavailable)?;
    Ok(node.kind == RealmNodeKind::User)
}

pub async fn forward_token_revoke(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    auth_token: MetadataAuthToken,
    token: String,
) -> Result<(), MetadataApiError> {
    let Some(config) = load_realm_config(context, realm_id).await else {
        return Err(MetadataApiError::ServiceUnavailable);
    };
    let Some(metadata) = context.metadata_handle.as_ref() else {
        return Err(MetadataApiError::ServiceUnavailable);
    };
    let local_node_id = context.net_handle.as_ref().map(|net| net.node_id());
    let mut subject = bearer_token_hash(&token).into_bytes();
    subject.extend_from_slice(&Ulid::generate().to_bytes());
    let peers = rank_revoke_peers(
        config
            .nodes
            .iter()
            .filter(|node| {
                matches!(
                    &node.kind,
                    RealmNodeKind::Management | RealmNodeKind::Server
                )
            })
            .filter_map(|node| NodeId::from_str(&node.node_id).ok())
            .filter(|peer| Some(*peer) != local_node_id),
        &subject,
    );
    if peers.is_empty() {
        return Err(MetadataApiError::ServiceUnavailable);
    }

    let message = MetadataTransportMessage::ForwardTokenRevocation { auth_token, token };
    run_revoke(
        &peers,
        message,
        Instant::now() + TOKEN_REVOKE_DEADLINE,
        |peer, message| metadata.request_forwarded_write(peer, message),
    )
    .await
}

async fn run_revoke<F, Fut>(
    peers: &[NodeId],
    message: MetadataTransportMessage,
    deadline: Instant,
    mut request: F,
) -> Result<(), MetadataApiError>
where
    F: FnMut(NodeId, MetadataTransportMessage) -> Fut,
    Fut: Future<Output = Result<MetadataTransportMessage, MetadataRequestError>>,
{
    let mut seen = Vec::with_capacity(TOKEN_REVOKE_PEER_LIMIT);
    for peer in peers.iter().copied() {
        if seen.len() >= TOKEN_REVOKE_PEER_LIMIT {
            break;
        }
        if seen.contains(&peer) {
            continue;
        }
        seen.push(peer);
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        let attempt = remaining.min(TOKEN_REVOKE_ATTEMPT_TIMEOUT);
        match timeout(attempt, request(peer, message.clone())).await {
            Err(_) => {
                warn!(%peer, "Token revocation forwarding attempt timed out");
                continue;
            }
            Ok(Ok(MetadataTransportMessage::ForwardedTokenRevoked)) => return Ok(()),
            Ok(Ok(MetadataTransportMessage::ForwardedWriteDenied {
                error: MetadataWriteAuthError::Unauthorized,
            })) => return Err(MetadataApiError::Unauthorized),
            Ok(Ok(MetadataTransportMessage::ForwardedWriteDenied {
                error: MetadataWriteAuthError::Forbidden,
            })) => return Err(MetadataApiError::Forbidden),
            Ok(Ok(MetadataTransportMessage::ForwardedWriteUnavailable))
            | Ok(Ok(MetadataTransportMessage::ForwardedTokenRevocationCapacity)) => continue,
            Ok(Ok(MetadataTransportMessage::Reject(error))) => {
                warn!(%peer, %error, "Peer rejected a forwarded token revocation");
                return Err(MetadataApiError::ServiceUnavailable);
            }
            Ok(Ok(response)) => {
                warn!(%peer, response = ?super::handle::transport_message_kind(&response), "Peer returned an unexpected token revocation response");
                return Err(MetadataApiError::ServiceUnavailable);
            }
            Ok(Err(error)) => {
                // Revocation is keyed by token hash, so an ambiguous write is safe to replay.
                warn!(%peer, %error, "Failed to forward a token revocation");
            }
        }
    }
    Err(MetadataApiError::ServiceUnavailable)
}

fn rank_revoke_peers(peers: impl IntoIterator<Item = NodeId>, subject: &[u8]) -> Vec<NodeId> {
    let mut ranked = Vec::with_capacity(TOKEN_REVOKE_PEER_LIMIT);
    for peer in peers {
        let score = neg_log2_q48(selector_hash(ROLE_NODE, subject, peer.as_bytes()));
        insert_revoke_peer(&mut ranked, peer, score);
    }
    ranked.into_iter().map(|(peer, _)| peer).collect()
}

fn insert_revoke_peer(ranked: &mut Vec<(NodeId, u64)>, peer: NodeId, score: u64) {
    if ranked.iter().any(|candidate| candidate.0 == peer) {
        return;
    }
    let position = ranked.iter().position(|candidate| {
        score < candidate.1 || (score == candidate.1 && peer.as_bytes() < candidate.0.as_bytes())
    });
    let Some(position) = position else {
        if ranked.len() < TOKEN_REVOKE_PEER_LIMIT {
            ranked.push((peer, score));
        }
        return;
    };
    ranked.insert(position, (peer, score));
    ranked.truncate(TOKEN_REVOKE_PEER_LIMIT);
}

/// Whether the origin currently holds a structured metadata document's bucket.
pub async fn origin_holds_document(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    local_node_id: NodeId,
    document_id: Ulid,
) -> Result<bool, MetadataApiError> {
    let config = drive(
        crate::get_realm_config::GetRealmConfigOperation::new(realm_id),
        context.as_ref(),
    )
    .await
    .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    if !config.has_node(local_node_id) {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    let placement = resolve_metadata_id(&config, realm_id, None, document_id)
        .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    Ok(holds_placement(&config, &placement, local_node_id))
}

async fn read_holders<T, F>(
    holders: Vec<NodeId>,
    request: F,
) -> (Vec<(NodeId, Result<T, MetadataReadError>)>, bool)
where
    T: Send + 'static,
    F: Fn(NodeId) -> BoxFuture<'static, Result<T, MetadataReadError>> + Send + Sync,
{
    let requests = futures_util::stream::iter(holders.into_iter().map(|holder| {
        let request = request(holder);
        async move {
            let result = timeout(METADATA_READ_PEER_TIMEOUT, request)
                .await
                .unwrap_or(Err(MetadataReadError::Unavailable));
            (holder, result)
        }
    }))
    .buffer_unordered(METADATA_READ_FANOUT_LIMIT);
    futures_util::pin_mut!(requests);

    let deadline = Instant::now() + METADATA_READ_DEADLINE;
    let mut results = Vec::new();
    loop {
        match tokio::time::timeout_at(deadline, requests.next()).await {
            Ok(Some(result)) => results.push(result),
            Ok(None) => return (results, false),
            Err(_) => return (results, true),
        }
    }
}

pub async fn get_metadata_routed(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    request: GetVisibleMetadataDocumentRequest,
    auth_token: Option<MetadataAuthToken>,
) -> Result<MetadataRegistryRecord, MetadataApiError> {
    if context.net_handle.is_none() {
        return get_visible_metadata_document(context.as_ref(), realm_id, request).await;
    }
    let config = load_realm_config(context, realm_id)
        .await
        .ok_or(MetadataApiError::ServiceUnavailable)?;
    let config_digest = config
        .digest()
        .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    let placement = resolve_metadata_id(&config, realm_id, None, request.document_id)
        .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    let holders = resolve_holders_limit(&config, &placement, MAX_READ_HOLDERS);
    let holder_count = holders.len();
    let local_node = context.net_handle.as_ref().map(|net| net.node_id());
    let context = Arc::clone(context);
    let config = Arc::new(config);
    let metadata = context.metadata_handle.clone();
    let request_template = request.clone();
    let (responses, timed_out) = read_holders(holders, move |holder| {
        let context = context.clone();
        let config = config.clone();
        let metadata = metadata.clone();
        let request = request_template.clone();
        let auth_token = auth_token.clone();
        Box::pin(async move {
            if Some(holder) == local_node {
                let record = get_visible_metadata_document(context.as_ref(), realm_id, request)
                    .await
                    .map_err(read_error)?;
                if routed_record_matches(&config, realm_id, record.document_id, &placement, &record)
                {
                    Ok(record)
                } else {
                    Err(MetadataReadError::Unavailable)
                }
            } else {
                let Some(metadata) = metadata else {
                    return Err(MetadataReadError::Unavailable);
                };
                match metadata
                    .request_forwarded_write(
                        holder,
                        MetadataTransportMessage::ForwardReadDocument {
                            auth_token,
                            config_digest,
                            document_id: request.document_id,
                        },
                    )
                    .await
                {
                    Ok(MetadataTransportMessage::ForwardedRead { result }) => {
                        let record = result?;
                        if routed_record_matches(
                            &config,
                            realm_id,
                            record.document_id,
                            &placement,
                            &record,
                        ) {
                            Ok(*record)
                        } else {
                            Err(MetadataReadError::Unavailable)
                        }
                    }
                    _ => Err(MetadataReadError::Unavailable),
                }
            }
        })
    })
    .await;
    let mut not_found = 0usize;
    let mut success = None;
    let mut auth_error = None;
    let mut unavailable = timed_out;
    for (_, response) in responses {
        match response {
            Ok(record) => {
                success.get_or_insert(record);
            }
            Err(MetadataReadError::Unauthorized) => {
                auth_error.get_or_insert(MetadataApiError::Unauthorized);
            }
            Err(MetadataReadError::Forbidden) => {
                auth_error.get_or_insert(MetadataApiError::Forbidden);
            }
            Err(MetadataReadError::NotFound) => not_found += 1,
            Err(MetadataReadError::Unavailable) => unavailable = true,
        };
    }
    if let Some(error) = auth_error {
        return Err(error);
    }
    if success.is_some() && not_found > 0 {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    if let Some(record) = success {
        return Ok(record);
    }
    if !unavailable && holder_count > 0 && not_found == holder_count {
        Err(MetadataApiError::NotFound)
    } else {
        Err(MetadataApiError::ServiceUnavailable)
    }
}

/// Exports locally on a holder or forwards with the caller's bearer or
/// peer-attested internal principal for another READ check.
pub async fn export_rocrate_routed(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    request: ExportMetadataRoCrateRequest,
    forward_token: Option<MetadataAuthToken>,
    metadata_bytes: u64,
) -> Result<ExportMetadataRoCrateResult, MetadataApiError> {
    if context.net_handle.is_none() {
        let export = export_metadata_rocrate(context.as_ref(), realm_id, request).await?;
        ensure_export_limit(&export, metadata_bytes)?;
        return Ok(export);
    }
    let config = load_realm_config(context, realm_id)
        .await
        .ok_or(MetadataApiError::ServiceUnavailable)?;
    let config_digest = config
        .digest()
        .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    let placement = resolve_metadata_id(&config, realm_id, None, request.document_id)
        .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    let holders = resolve_holders_limit(&config, &placement, MAX_READ_HOLDERS);
    let holder_count = holders.len();
    let local_node = context.net_handle.as_ref().map(|net| net.node_id());
    let context = Arc::clone(context);
    let metadata = context.metadata_handle.clone();
    let request_template = request.clone();
    let (responses, timed_out) = read_holders(holders, move |holder| {
        let context = context.clone();
        let metadata = metadata.clone();
        let request = request_template.clone();
        let forward_token = forward_token.clone();
        Box::pin(async move {
            if Some(holder) == local_node {
                let export = export_metadata_rocrate(context.as_ref(), realm_id, request).await;
                let export = export.map_err(read_error)?;
                ensure_export_limit(&export, metadata_bytes).map_err(read_error)?;
                Ok(export)
            } else {
                let Some(metadata) = metadata else {
                    return Err(MetadataReadError::Unavailable);
                };
                match metadata
                    .request_export(
                        holder,
                        MetadataTransportMessage::ForwardExportDocument {
                            auth_token: forward_token,
                            config_digest,
                            document_id: request.document_id,
                            view: request.view,
                            metadata_bytes,
                            limit: request.limit,
                            offset: request.offset,
                            after: request.after,
                        },
                    )
                    .await
                {
                    Ok(result) => result,
                    Err(_) => Err(MetadataReadError::Unavailable),
                }
            }
        })
    })
    .await;
    let mut not_found = 0usize;
    let mut success = None;
    let mut auth_error = None;
    let mut unavailable = timed_out;
    for (_, response) in responses {
        match response {
            Ok(export) => {
                success.get_or_insert(export);
            }
            Err(MetadataReadError::Unauthorized) => {
                auth_error.get_or_insert(MetadataApiError::Unauthorized);
            }
            Err(MetadataReadError::Forbidden) => {
                auth_error.get_or_insert(MetadataApiError::Forbidden);
            }
            Err(MetadataReadError::NotFound) => not_found += 1,
            Err(MetadataReadError::Unavailable) => unavailable = true,
        }
    }
    if let Some(error) = auth_error {
        return Err(error);
    }
    if success.is_some() && not_found > 0 {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    if let Some(export) = success {
        return Ok(export);
    }
    if !unavailable && holder_count > 0 && not_found == holder_count {
        Err(MetadataApiError::NotFound)
    } else {
        Err(MetadataApiError::ServiceUnavailable)
    }
}

fn ensure_export_limit(
    export: &ExportMetadataRoCrateResult,
    metadata_bytes: u64,
) -> Result<(), MetadataApiError> {
    let length = match export {
        ExportMetadataRoCrateResult::Full { jsonld, .. }
        | ExportMetadataRoCrateResult::Summary { jsonld, .. } => jsonld.len(),
        ExportMetadataRoCrateResult::Page { page, .. } => page.jsonld.len(),
        ExportMetadataRoCrateResult::Raw { raw, .. } => raw.revision.jsonld.len(),
    };
    if u64::try_from(length).unwrap_or(u64::MAX) > metadata_bytes {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    Ok(())
}

/// Creates locally when the origin holds the bucket, otherwise at a holder.
/// Definitely unsent requests may try another holder; ambiguous delivery is
/// terminal so the create is not replayed.
pub async fn create_metadata_document_routed(
    operation: CreateMetadataDocumentOperation,
    context: Arc<DriverContext>,
    auth_token: Option<MetadataAuthToken>,
) -> Result<CreateMetadataDocumentResult, MetadataWriteError> {
    let config = operation.config().clone();
    match create_metadata_document(operation, context.clone()).await {
        Err(CreateMetadataDocumentError::OriginHoldsNoBucket) => {}
        Ok(created) => return Ok(created),
        Err(error) => return Err(error.into()),
    }

    // Mint the forwarded id at the origin with the blind-hash bucket of the D8
    // subject, so every candidate holder stamps the same bucket (D3/D4).
    let realm_config = load_realm_config(&context, config.actor.realm_id)
        .await
        .ok_or_else(|| {
            MetadataWriteError::Undeliverable("realm placement config is unavailable".to_string())
        })?;
    let document_id = if config.document_id.is_nil() {
        mint_forward_document(
            &realm_config,
            &config.actor,
            config.group_id,
            &config.document_path,
        )?
    } else {
        MetaResourceId::from_bytes(config.document_id.to_bytes()).map_err(|error| {
            CreateMetadataDocumentError::PlacementBindingUnavailable(format!(
                "forwarded document id is not a structured id: {error}"
            ))
        })?
    };
    let (placement, holders) =
        create_forward_holders(&realm_config, &config, document_id.as_ulid()).ok_or_else(|| {
            MetadataWriteError::Undeliverable(
                "document id has no resolvable metadata placement".to_string(),
            )
        })?;
    let config_digest = realm_config
        .digest()
        .map_err(|error| MetadataWriteError::Undeliverable(error.to_string()))?;
    let response = forward_to_holders(
        &context,
        &holders,
        MetadataTransportMessage::ForwardCreateDocument {
            auth_token,
            config_digest,
            group_id: config.group_id,
            document_id: document_id.as_ulid(),
            document_path: config.document_path.clone(),
            public: config.public,
            payload: config.payload.clone(),
        },
        None,
        false,
    )
    .await?;
    match response {
        MetadataTransportMessage::ForwardedRecord { record }
            if create_record_matches(&config, document_id.as_ulid(), &placement, &record) =>
        {
            Ok(CreateMetadataDocumentResult {
                event_id: record.last_event_id,
                record: *record,
            })
        }
        MetadataTransportMessage::ForwardedRecord { .. } => Err(MetadataWriteError::Undeliverable(
            "holder returned a metadata create record for another document".to_string(),
        )),
        other => Err(unexpected_response(other)),
    }
}

pub async fn update_metadata_document_routed(
    context: &Arc<DriverContext>,
    actor: Actor,
    record: Option<&MetadataRegistryRecord>,
    document_id: Ulid,
    public: Option<bool>,
    mutation: UpdateMetadataDocumentMutation,
    auth_token: Option<MetadataAuthToken>,
) -> Result<MetadataRegistryRecord, MetadataWriteError> {
    let config = load_realm_config(context, actor.realm_id)
        .await
        .ok_or_else(|| {
            MetadataWriteError::Undeliverable("realm placement config is unavailable".to_string())
        })?;
    let placement = resolve_metadata_id(
        &config,
        actor.realm_id,
        record.map(|record| record.group_id),
        document_id,
    )
    .map_err(|error| MetadataWriteError::Undeliverable(error.to_string()))?;
    let config_digest = config
        .digest()
        .map_err(|error| MetadataWriteError::Undeliverable(error.to_string()))?;
    if record.is_some_and(|record| {
        !routed_record_matches(&config, actor.realm_id, document_id, &placement, record)
    }) {
        return Err(MetadataWriteError::Undeliverable(
            "local metadata registry record does not match the routed document".to_string(),
        ));
    }
    let local_node_id = actor.node_id;
    let current_holders = resolve_shard_holders(&config, &placement);
    let holders = record.map_or_else(
        || current_holders.clone(),
        |record| holder_intersection(&current_holders, &record.holder_node_ids),
    );
    if record.is_some() && holders.is_empty() {
        return Err(MetadataWriteError::Undeliverable(
            "metadata document has no active frozen holder with history capacity".to_string(),
        ));
    }
    let local_holds = holders.contains(&local_node_id);
    let mut local_capacity = false;
    if local_holds && let Some(record) = record {
        match update_metadata_document(
            UpdateMetadataDocumentOperation::new(UpdateMetadataDocumentConfig {
                actor: actor.clone(),
                group_id: record.group_id,
                document_id,
                public: public.unwrap_or(record.public),
                mutation: mutation.clone(),
            }),
            context.as_ref(),
        )
        .await
        {
            Ok(record) => return Ok(record),
            Err(UpdateMetadataDocumentError::RawLimit) => local_capacity = true,
            Err(error) => return Err(error.into()),
        }
    }
    let response = forward_to_holders(
        context,
        &holders,
        MetadataTransportMessage::ForwardUpdateDocument {
            auth_token,
            config_digest,
            document_id,
            public,
            mutation,
        },
        local_holds.then_some(local_node_id),
        local_capacity,
    )
    .await?;
    match response {
        MetadataTransportMessage::ForwardedRecord {
            record: response_record,
        } if routed_record_matches(
            &config,
            actor.realm_id,
            document_id,
            &placement,
            &response_record,
        ) && record.is_none_or(|record| update_record_matches(record, &response_record))
            && public.is_none_or(|public| response_record.public == public) =>
        {
            Ok(*response_record)
        }
        MetadataTransportMessage::ForwardedRecord { .. } => Err(MetadataWriteError::Undeliverable(
            "holder returned a metadata update record for another document".to_string(),
        )),
        MetadataTransportMessage::ForwardedUpdateInvalidInput { message } => Err(
            UpdateMetadataDocumentError::MetadataError(MetadataError::InvalidInput(message)).into(),
        ),
        other => Err(unexpected_response(other)),
    }
}

pub async fn delete_metadata_document_routed(
    context: &Arc<DriverContext>,
    actor: Actor,
    record: Option<&MetadataRegistryRecord>,
    document_id: Ulid,
    auth_token: Option<MetadataAuthToken>,
) -> Result<(), MetadataWriteError> {
    let config = load_realm_config(context, actor.realm_id)
        .await
        .ok_or_else(|| {
            MetadataWriteError::Undeliverable("realm placement config is unavailable".to_string())
        })?;
    let placement = resolve_metadata_id(
        &config,
        actor.realm_id,
        record.map(|record| record.group_id),
        document_id,
    )
    .map_err(|error| MetadataWriteError::Undeliverable(error.to_string()))?;
    let config_digest = config
        .digest()
        .map_err(|error| MetadataWriteError::Undeliverable(error.to_string()))?;
    if record.is_some_and(|record| {
        !routed_record_matches(&config, actor.realm_id, document_id, &placement, record)
    }) {
        return Err(MetadataWriteError::Undeliverable(
            "local metadata registry record does not match the routed document".to_string(),
        ));
    }
    let local_node_id = actor.node_id;
    let current_holders = resolve_shard_holders(&config, &placement);
    let holders = record.map_or_else(
        || current_holders.clone(),
        |record| holder_intersection(&current_holders, &record.holder_node_ids),
    );
    if record.is_some() && holders.is_empty() {
        return Err(MetadataWriteError::Undeliverable(
            "metadata document has no active frozen holder with history capacity".to_string(),
        ));
    }
    let local_holds = holders.contains(&local_node_id);
    if local_holds && let Some(record) = record {
        delete_metadata_document(
            DeleteMetadataDocumentOperation::new(actor, record.group_id, document_id),
            context.as_ref(),
            document_id,
        )
        .await
        .map_err(MetadataWriteError::from)?;
        return Ok(());
    }
    let response = forward_to_holders(
        context,
        &holders,
        MetadataTransportMessage::ForwardDeleteDocument {
            auth_token,
            config_digest,
            document_id,
        },
        local_holds.then_some(local_node_id),
        false,
    )
    .await?;
    match response {
        MetadataTransportMessage::ForwardedDelete => Ok(()),
        other => Err(unexpected_response(other)),
    }
}

pub(crate) async fn apply_forwarded_export(
    context: &Arc<DriverContext>,
    peer: NodeId,
    message: MetadataTransportMessage,
    local_limit: u64,
) -> Result<(ExportMetadataRoCrateResult, u64), MetadataReadError> {
    let MetadataTransportMessage::ForwardExportDocument {
        auth_token,
        config_digest,
        document_id,
        view,
        metadata_bytes,
        limit,
        offset,
        after,
    } = message
    else {
        return Err(MetadataReadError::Unavailable);
    };
    let net_handle = context
        .net_handle
        .as_ref()
        .ok_or(MetadataReadError::Unavailable)?;
    let realm_id = *net_handle.realm_id();
    let config = load_realm_config(context, realm_id)
        .await
        .ok_or(MetadataReadError::Unavailable)?;
    if config.digest().ok() != Some(config_digest) {
        return Err(MetadataReadError::Unavailable);
    }
    let metadata = context
        .metadata_handle
        .as_ref()
        .ok_or(MetadataReadError::Unavailable)?;
    let auth = metadata
        .authorize_read_peer(peer, auth_token, false)
        .await?;
    if !holds_metadata_id(&config, realm_id, net_handle.node_id(), document_id) {
        return Err(MetadataReadError::Unavailable);
    }
    let export = export_metadata_rocrate(
        context.as_ref(),
        realm_id,
        ExportMetadataRoCrateRequest {
            document_id,
            auth,
            view,
            limit,
            offset,
            after,
        },
    )
    .await
    .map_err(read_error)?;
    let metadata_bytes = metadata_bytes.min(local_limit);
    ensure_export_limit(&export, metadata_bytes).map_err(read_error)?;
    Ok((export, metadata_bytes))
}

pub(crate) async fn apply_document_query(
    context: &Arc<DriverContext>,
    peer: NodeId,
    message: MetadataTransportMessage,
) -> Result<MetadataQueryResults, MetadataReadError> {
    let MetadataTransportMessage::QueryDocument {
        auth_token,
        config_digest,
        document_id,
        sparql,
    } = message
    else {
        return Err(MetadataReadError::Unavailable);
    };
    let net_handle = context
        .net_handle
        .as_ref()
        .ok_or(MetadataReadError::Unavailable)?;
    let realm_id = *net_handle.realm_id();
    let config = load_realm_config(context, realm_id)
        .await
        .ok_or(MetadataReadError::Unavailable)?;
    if config.digest().ok() != Some(config_digest)
        || !holds_metadata_id(&config, realm_id, net_handle.node_id(), document_id)
    {
        return Err(MetadataReadError::Unavailable);
    }
    let metadata = context
        .metadata_handle
        .as_ref()
        .ok_or(MetadataReadError::Unavailable)?;
    let auth = metadata
        .authorize_read_peer(peer, auth_token, false)
        .await?;
    let record = get_visible_metadata_document(
        context.as_ref(),
        realm_id,
        GetVisibleMetadataDocumentRequest {
            document_id,
            auth: auth.clone(),
        },
    )
    .await
    .map_err(read_error)?;
    metadata
        .query_authorized_local(auth, Some(vec![record.graph_iri]), sparql)
        .await
        .map_err(|_| MetadataReadError::Unavailable)
}

/// Applies a write forwarded by a non-holder, under the caller's authority.
///
/// The forwarded bearer token is re-validated and the same permission checks the
/// origin's HTTP handler runs are re-run here: forwarding is a routing hop, not
/// an internal trust bypass.
///
/// The peer gate is realm membership (`authorize_remote_peer` confirms the peer
/// is a configured node of the token's realm), deliberately *not*
/// sync-eligibility. User-kind nodes are never sync-eligible and therefore hold
/// no bucket at all, which makes them precisely the nodes that must forward every
/// write; gating the forward on sync-eligibility would reject exactly the case it
/// exists to serve. This grants nothing: a forward can do nothing the peer could
/// not do by calling this node's HTTP API directly, under the same token and the
/// same permission check. Sync-eligibility keeps guarding who may *hold* and sync
/// documents — that is a separate question from who may ask a holder to write.
pub(crate) async fn apply_forwarded_write(
    context: &Arc<DriverContext>,
    peer: NodeId,
    message: MetadataTransportMessage,
) -> MetadataTransportMessage {
    let Some(net_handle) = context.net_handle.as_ref() else {
        return forwarded_unavailable(&message);
    };
    let realm_id = *net_handle.realm_id();
    let Some(config) = load_realm_config(context, realm_id).await else {
        return forwarded_unavailable(&message);
    };
    let expected_digest = match &message {
        MetadataTransportMessage::ForwardCreateDocument { config_digest, .. }
        | MetadataTransportMessage::ForwardUpdateDocument { config_digest, .. }
        | MetadataTransportMessage::ForwardDeleteDocument { config_digest, .. }
        | MetadataTransportMessage::ForwardReadDocument { config_digest, .. } => *config_digest,
        _ => return reject("unexpected forwarded metadata message"),
    };
    if config.digest().ok() != Some(expected_digest) {
        return forwarded_unavailable(&message);
    };

    if let MetadataTransportMessage::ForwardReadDocument {
        auth_token,
        document_id,
        ..
    } = &message
    {
        let Some(metadata) = context.metadata_handle.as_ref() else {
            return reject("forwarded metadata read needs a metadata handle");
        };
        let result = match metadata
            .authorize_read_peer(peer, auth_token.clone(), false)
            .await
        {
            Ok(auth)
                if holds_metadata_id(&config, realm_id, net_handle.node_id(), *document_id) =>
            {
                get_visible_metadata_document(
                    context.as_ref(),
                    realm_id,
                    GetVisibleMetadataDocumentRequest {
                        document_id: *document_id,
                        auth,
                    },
                )
                .await
                .map(Box::new)
                .map_err(read_error)
            }
            Ok(_) => Err(MetadataReadError::Unavailable),
            Err(error) => Err(error),
        };
        return MetadataTransportMessage::ForwardedRead { result };
    }

    let auth = match authorize_forwarded_caller(context, peer, realm_id, &message).await {
        Ok(auth) => auth,
        Err(error) => return forward_auth_error(error),
    };

    match message {
        MetadataTransportMessage::ForwardCreateDocument {
            group_id,
            document_id,
            document_path,
            public,
            payload,
            ..
        } => {
            let normalized_document_path =
                MetadataRegistryRecord::normalize_document_path(&document_path);
            if normalized_document_path.is_empty() {
                return reject("forwarded metadata create has an empty document path");
            }
            let path = MetadataRegistryRecord::permission_path_for(
                &realm_id,
                group_id,
                &normalized_document_path,
                document_id,
            );
            if let Err(error) = authorize_write(context, auth.clone(), path).await {
                return forward_auth_error(error);
            }
            let create_config = CreateMetadataDocumentConfig {
                actor: Actor {
                    node_id: net_handle.node_id(),
                    user_id: auth.user_id,
                    realm_id,
                },
                group_id,
                document_id,
                document_path,
                public,
                payload,
            };
            match forwarded_create_replay(context, &create_config).await {
                Ok(Some(response)) => return response,
                Ok(None) => {}
                Err(error) => return reject(error),
            }
            let operation = CreateMetadataDocumentOperation::new_forwarded(create_config.clone());
            match create_metadata_document(operation, context.clone()).await {
                Ok(created) => MetadataTransportMessage::ForwardedRecord {
                    record: Box::new(created.record),
                },
                // Lost the race against a concurrent delivery of the same
                // forward: the winner's record is the answer, not an error.
                Err(CreateMetadataDocumentError::DocumentAlreadyExists) => {
                    match forwarded_create_replay(context, &create_config).await {
                        Ok(Some(response)) => response,
                        Ok(None) => reject(format!(
                            "forwarded metadata create for `{document_id}` raced a delete"
                        )),
                        Err(error) => reject(error),
                    }
                }
                Err(error) => reject(format!("forwarded metadata create failed: {error}")),
            }
        }
        MetadataTransportMessage::ForwardUpdateDocument {
            document_id,
            public,
            mutation,
            ..
        } => {
            let record =
                match held_record(context, &config, net_handle.node_id(), document_id).await {
                    Ok(record) => record,
                    Err(HeldRecordError::NotFound) => {
                        return MetadataTransportMessage::ForwardedWriteNotFound;
                    }
                    Err(HeldRecordError::Unavailable(error)) => {
                        warn!(%document_id, %error, "Forwarded metadata update is unavailable");
                        return MetadataTransportMessage::ForwardedWriteUnavailable;
                    }
                };
            if let Err(error) =
                authorize_write(context, auth.clone(), record.permission_path.clone()).await
            {
                return forward_auth_error(error);
            }
            let operation = UpdateMetadataDocumentOperation::new(UpdateMetadataDocumentConfig {
                actor: Actor {
                    node_id: net_handle.node_id(),
                    user_id: auth.user_id,
                    realm_id,
                },
                group_id: record.group_id,
                document_id,
                public: public.unwrap_or(record.public),
                mutation,
            });
            match update_metadata_document(operation, context.as_ref()).await {
                Ok(record) => MetadataTransportMessage::ForwardedRecord {
                    record: Box::new(record),
                },
                Err(UpdateMetadataDocumentError::RawLimit) => {
                    MetadataTransportMessage::ForwardedMetadataHistoryCapacity
                }
                Err(UpdateMetadataDocumentError::MetadataError(MetadataError::InvalidInput(
                    message,
                ))) => MetadataTransportMessage::ForwardedUpdateInvalidInput { message },
                Err(error) => reject(format!("forwarded metadata update failed: {error}")),
            }
        }
        MetadataTransportMessage::ForwardDeleteDocument { document_id, .. } => {
            let record =
                match held_record(context, &config, net_handle.node_id(), document_id).await {
                    Ok(record) => record,
                    Err(HeldRecordError::NotFound) => {
                        return MetadataTransportMessage::ForwardedWriteNotFound;
                    }
                    Err(HeldRecordError::Unavailable(error)) => {
                        warn!(%document_id, %error, "Forwarded metadata delete is unavailable");
                        return MetadataTransportMessage::ForwardedWriteUnavailable;
                    }
                };
            if let Err(error) =
                authorize_write(context, auth.clone(), record.permission_path.clone()).await
            {
                return forward_auth_error(error);
            }
            let operation = DeleteMetadataDocumentOperation::new(
                Actor {
                    node_id: net_handle.node_id(),
                    user_id: auth.user_id,
                    realm_id,
                },
                record.group_id,
                document_id,
            );
            match delete_metadata_document(operation, context.as_ref(), document_id).await {
                Ok(()) => MetadataTransportMessage::ForwardedDelete,
                Err(error) => reject(format!("forwarded metadata delete failed: {error}")),
            }
        }
        other => reject(format!(
            "unexpected forwarded metadata message: {}",
            super::handle::transport_message_kind(&other)
        )),
    }
}

/// The one node that owns a document's PID state: the rank-0 current holder of
/// the placement derived from the structured id, never from the registry row,
/// which a delete removes while the mapping must survive to serve a permanent
/// 410. Every node derives the same node from the same replicated config, so
/// transitions and landing answers have a single source and cannot disagree.
pub(crate) fn pid_authority_node(
    config: &RealmConfigDocument,
    realm_id: RealmId,
    document_id: Ulid,
) -> Option<NodeId> {
    let placement = resolve_metadata_id(config, realm_id, None, document_id).ok()?;
    resolve_shard_holders(config, &placement).first().copied()
}

async fn pid_authority(
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

fn is_local_node(context: &Arc<DriverContext>, node_id: NodeId) -> bool {
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
    auth_token: Option<MetadataAuthToken>,
) -> Result<(PersistentIdMapping, bool), MetadataApiError> {
    if context.net_handle.is_none() {
        return crate::persistent_id::mint_persistent_id(
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
        return crate::persistent_id::mint_persistent_id(
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
    auth_token: Option<MetadataAuthToken>,
) -> Result<(JobId, bool), MetadataApiError> {
    let realm_id = minted_by.realm_id;
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

async fn submit_pid_local(
    context: &Arc<DriverContext>,
    document_id: Ulid,
    minted_by: UserId,
    owner_node_id: NodeId,
    retention_ms: u64,
) -> Result<(JobId, bool), MetadataApiError> {
    crate::jobs::service::submit_mint_local(
        context.as_ref(),
        MintPersistentIdSpec {
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
    withdrawn_at_ms: u64,
    auth_token: Option<MetadataAuthToken>,
) -> Result<PersistentIdMapping, MetadataApiError> {
    if context.net_handle.is_none() {
        return crate::persistent_id::withdraw_persistent_id(
            context.as_ref(),
            realm_id,
            document_id,
            withdrawn_at_ms,
        )
        .await
        .map(|(mapping, _)| mapping)
        .map_err(pid_error);
    }
    let (config, authority) = pid_authority(context, realm_id, document_id).await?;
    if is_local_node(context, authority) {
        return crate::persistent_id::withdraw_persistent_id(
            context.as_ref(),
            realm_id,
            document_id,
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
        PersistentIdRequest::Withdraw { withdrawn_at_ms },
        auth_token,
    )
    .await?;
    match outcome {
        PersistentIdOutcome::Mapping { mapping, .. } => Ok(*mapping),
        _ => Err(MetadataApiError::ServiceUnavailable),
    }
}

/// Resolve a landing request on the document's authority. Only that node answers:
/// a replica's mapping row carries no version an outsider can compare, so folding
/// several answers can promote a stale redirect over an up-to-date denial or a
/// premature mapping over a live document. Every other node returns the
/// authority's answer or service unavailable, never a local one.
pub async fn resolve_pid_routed(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    document_id: Ulid,
) -> Result<PersistentIdResolution, MetadataApiError> {
    if context.net_handle.is_none() {
        return local_pid_resolution(context, realm_id, document_id).await;
    }
    let (config, authority) = pid_authority(context, realm_id, document_id).await?;
    if is_local_node(context, authority) {
        return local_pid_resolution(context, realm_id, document_id).await;
    }
    let outcome = forward_pid(
        context,
        &config,
        authority,
        document_id,
        PersistentIdRequest::Resolve,
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
async fn local_pid_resolution(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    document_id: Ulid,
) -> Result<PersistentIdResolution, MetadataApiError> {
    let mapping = crate::persistent_id::read_mapping(context.as_ref(), document_id)
        .await
        .map_err(pid_error)?;
    let Some(mapping) = mapping else {
        return Ok(PersistentIdResolution::Missing);
    };
    if !mapping.is_active() {
        return Ok(PersistentIdResolution::Gone { pid: mapping.pid });
    }
    let record = load_metadata_record_by_document(context.as_ref(), document_id)
        .await
        .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    // An active mapping without a registry row is a permanent 410 only once this
    // node has evidence the document was created here and is gone. An unprojected
    // create looks identical, and answering Gone for it would kill a live PID.
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

async fn forward_pid(
    context: &Arc<DriverContext>,
    config: &RealmConfigDocument,
    authority: NodeId,
    document_id: Ulid,
    request: PersistentIdRequest,
    auth_token: Option<MetadataAuthToken>,
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
    if let PersistentIdRequest::Resolve = request {
        let result = local_pid_resolution(context, realm_id, document_id)
            .await
            .map(PersistentIdOutcome::Resolution)
            .map_err(read_error);
        return MetadataTransportMessage::ForwardedPersistentId { result };
    }

    // Transitions carry the caller's authority: forwarding is a routing hop, so
    // the holder re-runs the WRITE check the origin's handler ran.
    let auth = match authorize_forwarded_pid(context, peer, realm_id, auth_token).await {
        Ok(auth) => auth,
        Err(error) => return forward_auth_error(error),
    };
    // The minting subject is the token's own subject: a routing hop may not
    // attribute a mint to a user it merely relays for.
    let minting_subject = match &request {
        PersistentIdRequest::Mint { minted_by, .. }
        | PersistentIdRequest::SubmitMint { minted_by, .. } => Some(*minted_by),
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
        (_, Some(record)) => {
            if let Err(error) =
                authorize_write(context, auth.clone(), record.permission_path.clone()).await
            {
                return forward_auth_error(error);
            }
        }
        // Without a registry row there is no permission path to check, so no
        // transition is authorized: bare realm membership must never tombstone an
        // arbitrary document id, and a withdrawal that outlived its own document
        // was already written by the delete that removed it.
        (_, None) => return MetadataTransportMessage::ForwardedWriteNotFound,
    }

    let outcome = match request {
        PersistentIdRequest::Mint {
            minted_by,
            minted_at_ms,
        } => crate::persistent_id::mint_persistent_id(
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
        PersistentIdRequest::Withdraw { withdrawn_at_ms } => {
            crate::persistent_id::withdraw_persistent_id(
                context.as_ref(),
                realm_id,
                document_id,
                withdrawn_at_ms,
            )
            .await
            .map(|(mapping, changed)| PersistentIdOutcome::Mapping {
                mapping: Box::new(mapping),
                changed,
            })
        }
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
        PersistentIdRequest::Resolve => unreachable!("resolve returned above"),
    };
    match outcome {
        Ok(outcome) => MetadataTransportMessage::ForwardedPersistentId {
            result: Ok(outcome),
        },
        Err(crate::persistent_id::PersistentIdError::DocumentMissing) => {
            MetadataTransportMessage::ForwardedWriteNotFound
        }
        Err(error) => {
            warn!(%document_id, ?error, "Forwarded persistent id transition failed");
            MetadataTransportMessage::ForwardedWriteUnavailable
        }
    }
}

async fn authorize_forwarded_pid(
    context: &Arc<DriverContext>,
    peer: NodeId,
    realm_id: RealmId,
    auth_token: Option<MetadataAuthToken>,
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
            MetadataWritePeerError::Unauthorized => ForwardAuthError::Unauthorized,
            MetadataWritePeerError::Unavailable(error) => {
                ForwardAuthError::Unavailable(error.to_string())
            }
        })?;
    if auth.realm_id != realm_id {
        return Err(ForwardAuthError::Forbidden);
    }
    Ok(auth)
}

fn pid_error(error: crate::persistent_id::PersistentIdError) -> MetadataApiError {
    match error {
        crate::persistent_id::PersistentIdError::DocumentMissing => MetadataApiError::NotFound,
        error => MetadataApiError::Internal(error.to_string()),
    }
}

fn write_error(error: MetadataWriteError) -> MetadataApiError {
    match error {
        MetadataWriteError::Unauthorized => MetadataApiError::Unauthorized,
        MetadataWriteError::Forbidden => MetadataApiError::Forbidden,
        MetadataWriteError::NotFound => MetadataApiError::NotFound,
        _ => MetadataApiError::ServiceUnavailable,
    }
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
    let Some(config) = load_realm_config(context, realm_id).await else {
        return MetadataTransportMessage::ForwardedWriteUnavailable;
    };
    let local_node = config
        .nodes
        .iter()
        .find(|node| node.node_id == net_handle.node_id().to_string());
    if !local_node.is_some_and(|node| {
        matches!(
            &node.kind,
            RealmNodeKind::Management | RealmNodeKind::Server
        )
    }) {
        return MetadataTransportMessage::ForwardedWriteUnavailable;
    }
    let Some(metadata) = context.metadata_handle.as_ref() else {
        return MetadataTransportMessage::ForwardedWriteUnavailable;
    };
    let MetadataTransportMessage::ForwardTokenRevocation { auth_token, .. } = &message else {
        return reject("unexpected token revocation message");
    };
    if !matches!(auth_token, MetadataAuthToken::Bearer(_)) {
        return MetadataTransportMessage::ForwardedWriteDenied {
            error: MetadataWriteAuthError::Unauthorized,
        };
    }
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
            error: MetadataWriteAuthError::Forbidden,
        };
    }
    if auth.user_id != subject.user_id
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
            admission: if auth.user_id == subject.user_id {
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
        Err(RevokeTokenError::CapacityReached) => {
            MetadataTransportMessage::ForwardedTokenRevocationCapacity
        }
        Err(error) => reject(format!("token revocation failed: {error}")),
    }
}

fn routed_record_matches(
    config: &RealmConfigDocument,
    realm_id: RealmId,
    document_id: Ulid,
    placement: &PlacementRef,
    record: &MetadataRegistryRecord,
) -> bool {
    record.realm_id == realm_id
        && record.document_id == document_id
        && record.placement == *placement
        && record.graph_iri == MetadataRegistryRecord::graph_iri_for(document_id)
        && record.permission_path
            == MetadataRegistryRecord::permission_path_for(
                &realm_id,
                record.group_id,
                &record.document_path,
                document_id,
            )
        && resolve_metadata_id(config, realm_id, Some(record.group_id), record.document_id)
            .is_ok_and(|resolved| resolved == *placement)
}

fn create_record_matches(
    config: &CreateMetadataDocumentConfig,
    document_id: Ulid,
    placement: &PlacementRef,
    record: &MetadataRegistryRecord,
) -> bool {
    let normalized_path = MetadataRegistryRecord::normalize_document_path(&config.document_path);
    record.realm_id == config.actor.realm_id
        && record.group_id == config.group_id
        && record.document_id == document_id
        && record.document_path == normalized_path
        && record.graph_iri == MetadataRegistryRecord::graph_iri_for(document_id)
        && record.permission_path
            == MetadataRegistryRecord::permission_path_for(
                &config.actor.realm_id,
                config.group_id,
                &normalized_path,
                document_id,
            )
        && record.placement == *placement
        && record.public == config.public
}

fn update_record_matches(
    expected: &MetadataRegistryRecord,
    actual: &MetadataRegistryRecord,
) -> bool {
    expected.realm_id == actual.realm_id
        && expected.group_id == actual.group_id
        && expected.document_id == actual.document_id
        && expected.document_path == actual.document_path
        && expected.graph_iri == actual.graph_iri
        && expected.permission_path == actual.permission_path
        && expected.placement == actual.placement
        && expected.created_at_ms == actual.created_at_ms
        && expected.establishing_event_id == actual.establishing_event_id
}

fn holds_metadata_id(
    config: &RealmConfigDocument,
    realm_id: RealmId,
    local_node_id: NodeId,
    document_id: Ulid,
) -> bool {
    resolve_metadata_id(config, realm_id, None, document_id)
        .is_ok_and(|placement| holds_placement(config, &placement, local_node_id))
}

fn read_error(error: MetadataApiError) -> MetadataReadError {
    match error {
        MetadataApiError::Unauthorized => MetadataReadError::Unauthorized,
        MetadataApiError::Forbidden => MetadataReadError::Forbidden,
        MetadataApiError::NotFound => MetadataReadError::NotFound,
        MetadataApiError::BadRequest
        | MetadataApiError::ServiceUnavailable
        | MetadataApiError::InvalidCursor(_)
        | MetadataApiError::Internal(_) => MetadataReadError::Unavailable,
    }
}

async fn forwarded_create_replay(
    context: &Arc<DriverContext>,
    config: &CreateMetadataDocumentConfig,
) -> Result<Option<MetadataTransportMessage>, String> {
    let Some(record) = existing_record(context, config.document_id).await? else {
        return Ok(None);
    };
    let accepted = accepted_create(context, config.document_id)
        .await?
        .ok_or_else(|| "existing metadata document has no create acceptance".to_string())?;
    if !accepted_create_matches(config, &accepted)
        || record.realm_id != config.actor.realm_id
        || record.group_id != config.group_id
        || record.document_path
            != MetadataRegistryRecord::normalize_document_path(&config.document_path)
    {
        return Err("forwarded metadata create collides with an existing document".to_string());
    }
    Ok(Some(MetadataTransportMessage::ForwardedRecord {
        record: Box::new(record),
    }))
}

async fn accepted_create(
    context: &Arc<DriverContext>,
    document_id: Ulid,
) -> Result<Option<MetadataCreateEventRecord>, String> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: METADATA_CREATE_ACCEPTANCE_KEYSPACE.to_string(),
            key: metadata_create_acceptance_key(document_id),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => postcard::from_bytes(&bytes)
            .map(Some)
            .map_err(|error| format!("metadata create acceptance decode failed: {error}")),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(None),
        Event::Storage(StorageEvent::Error { error }) => {
            Err(format!("metadata create acceptance read failed: {error}"))
        }
        other => Err(format!(
            "unexpected metadata create acceptance read result: {other:?}"
        )),
    }
}

/// Whether this node has evidence that the document was created here and is now
/// gone, rather than a create whose registry projection has not landed yet. The
/// create acceptance survives the delete; a queued projection means the row is
/// still on its way and no terminal answer may be derived from its absence.
async fn document_deleted_here(
    context: &Arc<DriverContext>,
    document_id: Ulid,
) -> Result<bool, MetadataApiError> {
    if projection_queued(context, document_id).await? {
        return Ok(false);
    }
    accepted_create(context, document_id)
        .await
        .map(|accepted| accepted.is_some())
        .map_err(|_| MetadataApiError::ServiceUnavailable)
}

/// Whether a committed metadata event for this document is still waiting to be
/// projected into this node's registry.
pub(crate) async fn projection_queued(
    context: &Arc<DriverContext>,
    document_id: Ulid,
) -> Result<bool, MetadataApiError> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Iter {
            key_space: METADATA_PENDING_PROJECTION_KEYSPACE.to_string(),
            prefix: Some(byteview::ByteView::from(document_id.to_bytes().to_vec())),
            start: None,
            limit: 1,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => Ok(!values.is_empty()),
        _ => Err(MetadataApiError::ServiceUnavailable),
    }
}

/// The document's registry record, whatever this node's holdership of it.
async fn existing_record(
    context: &Arc<DriverContext>,
    document_id: Ulid,
) -> Result<Option<MetadataRegistryRecord>, String> {
    load_metadata_record_by_document(context.as_ref(), document_id)
        .await
        .map_err(|error| format!("metadata registry read failed: {error:?}"))
}

enum HeldRecordError {
    NotFound,
    Unavailable(String),
}

/// Loads a document only when this node holds its current structured placement.
async fn held_record(
    context: &Arc<DriverContext>,
    config: &RealmConfigDocument,
    local_node_id: NodeId,
    document_id: Ulid,
) -> Result<MetadataRegistryRecord, HeldRecordError> {
    let placement = resolve_metadata_id(config, config.realm_id, None, document_id)
        .map_err(|error| HeldRecordError::Unavailable(error.to_string()))?;
    if !holds_placement(config, &placement, local_node_id) {
        return Err(HeldRecordError::Unavailable(format!(
            "node does not hold bucket {}/{} of metadata document `{document_id}`",
            placement.strategy_id, placement.shard
        )));
    }
    let record = match existing_record(context, document_id)
        .await
        .map_err(HeldRecordError::Unavailable)?
    {
        Some(record) => record,
        // An empty registry read is not absence while this node still owes the
        // projection of a committed create: reporting not-found would let a caller
        // that polls every holder conclude the document never existed.
        None => {
            return Err(match projection_queued(context, document_id).await {
                Ok(true) => HeldRecordError::Unavailable(format!(
                    "metadata document `{document_id}` has a queued registry projection"
                )),
                Ok(false) => HeldRecordError::NotFound,
                Err(_) => HeldRecordError::Unavailable(
                    "pending metadata projection scan is unavailable".to_string(),
                ),
            });
        }
    };
    if !routed_record_matches(config, config.realm_id, document_id, &placement, &record) {
        return Err(HeldRecordError::Unavailable(
            "metadata registry record does not match its structured placement".to_string(),
        ));
    }
    if !record.holder_node_ids.contains(&local_node_id) {
        return Err(HeldRecordError::Unavailable(
            "node is not a frozen holder for this metadata document".to_string(),
        ));
    }
    Ok(record)
}

async fn authorize_forwarded_caller(
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
        _ => None,
    };
    let auth = metadata_handle
        .authorize_write_peer(peer, auth_token)
        .await
        .map_err(|error| match error {
            MetadataWritePeerError::Unauthorized => ForwardAuthError::Unauthorized,
            MetadataWritePeerError::Unavailable(error) => {
                ForwardAuthError::Unavailable(error.to_string())
            }
        })?;
    if auth.realm_id != realm_id {
        return Err(ForwardAuthError::Forbidden);
    }
    Ok(auth)
}

async fn authorize_write(
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
    }
}

/// Holders of the document's blind-hashed bucket: the candidates for a create the
/// origin cannot place. Every candidate holds that one bucket, and a forwarded
/// create stamps exactly it (see `CreateMetadataDocumentOperation::new_forwarded`),
/// so which candidate answers cannot change where the document lands.
fn create_forward_holders(
    realm_config: &RealmConfigDocument,
    config: &CreateMetadataDocumentConfig,
    document_id: Ulid,
) -> Option<(PlacementRef, Vec<NodeId>)> {
    let placement = resolve_metadata_id(
        realm_config,
        config.actor.realm_id,
        Some(config.group_id),
        document_id,
    )
    .ok()?;
    let holders = resolve_shard_holders(realm_config, &placement);
    Some((placement, holders))
}

async fn forward_to_holders(
    context: &Arc<DriverContext>,
    holders: &[NodeId],
    message: MetadataTransportMessage,
    local_miss: Option<NodeId>,
    local_capacity: bool,
) -> Result<MetadataTransportMessage, MetadataWriteError> {
    let Some(metadata_handle) = context.metadata_handle.as_ref() else {
        return Err(MetadataWriteError::Undeliverable(
            "no metadata handle to forward with".to_string(),
        ));
    };
    let holders = distinct_holders(holders);
    let local_node_id = local_miss.or_else(|| context.net_handle.as_ref().map(|net| net.node_id()));
    let tracks_not_found = matches!(
        &message,
        MetadataTransportMessage::ForwardUpdateDocument { .. }
            | MetadataTransportMessage::ForwardDeleteDocument { .. }
            | MetadataTransportMessage::ForwardPersistentId { .. }
    );

    let mut failures: Vec<String> = Vec::new();
    let mut not_found = usize::from(local_miss.is_some_and(|local| holders.contains(&local)));
    let mut capacity =
        usize::from(local_capacity && local_miss.is_some_and(|local| holders.contains(&local)));
    for holder in holders
        .iter()
        .filter(|holder| Some(**holder) != local_node_id)
    {
        match metadata_handle
            .request_forwarded_write(*holder, message.clone())
            .await
        {
            Ok(MetadataTransportMessage::ForwardedWriteDenied {
                error: MetadataWriteAuthError::Unauthorized,
            }) => return Err(MetadataWriteError::Unauthorized),
            Ok(MetadataTransportMessage::ForwardedWriteDenied {
                error: MetadataWriteAuthError::Forbidden,
            }) => return Err(MetadataWriteError::Forbidden),
            Ok(MetadataTransportMessage::ForwardedWriteNotFound) if tracks_not_found => {
                not_found += 1;
            }
            Ok(MetadataTransportMessage::ForwardedWriteNotFound) => {
                failures.push(format!(
                    "{holder}: holder returned not found for a forwarded create"
                ));
            }
            Ok(MetadataTransportMessage::ForwardedWriteUnavailable) => {
                failures.push(format!("{holder}: holder placement view is unavailable"));
            }
            Ok(MetadataTransportMessage::ForwardedMetadataHistoryCapacity)
                if matches!(
                    &message,
                    MetadataTransportMessage::ForwardUpdateDocument { .. }
                ) =>
            {
                capacity += 1;
            }
            Ok(MetadataTransportMessage::ForwardedMetadataHistoryCapacity) => {
                failures.push(format!(
                    "{holder}: holder returned metadata history capacity for a non-update"
                ));
            }
            Ok(MetadataTransportMessage::Reject(error)) => {
                warn!(holder = %holder, error = %error, "Holder rejected a forwarded metadata write");
                return Err(MetadataWriteError::Undeliverable(format!(
                    "holder `{holder}` rejected the forwarded metadata write; refusing to replay it: {error}"
                )));
            }
            Ok(response) => return Ok(response),
            Err(error) => {
                warn!(holder = %holder, error = %error, "Failed to forward a metadata write to holder");
                if retry_disposition(error.delivery()) == RetryDisposition::Stop {
                    return Err(MetadataWriteError::Undeliverable(format!(
                        "forward to holder `{holder}` may have applied the metadata write before failing; refusing to replay it: {error}"
                    )));
                }
                failures.push(format!("{holder}: {error}"));
            }
        }
    }

    if !holders.is_empty() && capacity == holders.len() {
        return Err(MetadataWriteError::Undeliverable(
            "metadata history capacity reached on every holder".to_string(),
        ));
    }

    if tracks_not_found && !holders.is_empty() && not_found == holders.len() {
        return Err(MetadataWriteError::NotFound);
    }

    let detail = if failures.is_empty() {
        "the document's bucket has no reachable holder".to_string()
    } else {
        failures.join("; ")
    };
    error!(
        holders = holders.len(),
        detail = %detail,
        "Metadata write reached a non-holder and no holder accepted the forward"
    );
    Err(MetadataWriteError::Undeliverable(detail))
}

fn distinct_holders(holders: &[NodeId]) -> Vec<NodeId> {
    let mut distinct = Vec::with_capacity(holders.len());
    for holder in holders.iter().copied() {
        if !distinct.contains(&holder) {
            distinct.push(holder);
        }
    }
    distinct
}

fn holder_intersection(current: &[NodeId], frozen: &[NodeId]) -> Vec<NodeId> {
    let holders = current
        .iter()
        .copied()
        .filter(|holder| frozen.contains(holder))
        .collect::<Vec<_>>();
    distinct_holders(&holders)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RetryDisposition {
    TryNext,
    Stop,
}

fn retry_disposition(delivery: MetadataRequestDelivery) -> RetryDisposition {
    match delivery {
        MetadataRequestDelivery::DefinitelyNotSent => RetryDisposition::TryNext,
        MetadataRequestDelivery::PossiblySent => RetryDisposition::Stop,
    }
}

fn unexpected_response(response: MetadataTransportMessage) -> MetadataWriteError {
    MetadataWriteError::Undeliverable(format!(
        "unexpected forwarded metadata response: {}",
        super::handle::transport_message_kind(&response)
    ))
}

fn reject(error: impl Into<String>) -> MetadataTransportMessage {
    MetadataTransportMessage::Reject(error.into())
}

fn forwarded_unavailable(message: &MetadataTransportMessage) -> MetadataTransportMessage {
    match message {
        MetadataTransportMessage::ForwardReadDocument { .. } => {
            MetadataTransportMessage::ForwardedRead {
                result: Err(MetadataReadError::Unavailable),
            }
        }
        MetadataTransportMessage::ForwardCreateDocument { .. }
        | MetadataTransportMessage::ForwardUpdateDocument { .. }
        | MetadataTransportMessage::ForwardDeleteDocument { .. }
        | MetadataTransportMessage::ForwardTokenRevocation { .. } => {
            MetadataTransportMessage::ForwardedWriteUnavailable
        }
        _ => reject("unexpected forwarded metadata message"),
    }
}

enum ForwardAuthError {
    Unauthorized,
    Forbidden,
    Unavailable(String),
}

fn forward_auth_error(error: ForwardAuthError) -> MetadataTransportMessage {
    match error {
        ForwardAuthError::Unauthorized => MetadataTransportMessage::ForwardedWriteDenied {
            error: MetadataWriteAuthError::Unauthorized,
        },
        ForwardAuthError::Forbidden => MetadataTransportMessage::ForwardedWriteDenied {
            error: MetadataWriteAuthError::Forbidden,
        },
        ForwardAuthError::Unavailable(error) => {
            warn!(%error, "Forwarded metadata authorization is unavailable");
            MetadataTransportMessage::ForwardedWriteUnavailable
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::{METADATA_HANDLE, PlacementStrategy, RealmNodeKind};
    use aruna_core::structured_id::{BucketId, PlacementHandle};
    use aruna_core::{MetaResourceId, StructuredId};

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn config_and_placement() -> (RealmConfigDocument, PlacementRef) {
        let mut config = RealmConfigDocument::new(RealmId::from_bytes([7u8; 32]), Vec::new(), 3);
        let strategy = PlacementStrategy {
            strategy_id: Ulid::from_bytes([4u8; 16]),
            name: "default".to_string(),
            replica_count: Some(2),
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count: 64,
        };
        config.default_strategy_id = Some(strategy.strategy_id);
        config.strategies = vec![strategy.clone()];
        for seed in 1..=4u8 {
            config.ensure_node(node(seed), RealmNodeKind::Server);
        }
        (
            config,
            PlacementRef {
                strategy_id: strategy.strategy_id,
                epoch: 0,
                shard: 9,
            },
        )
    }

    #[test]
    fn holder_writes_stay_local() {
        let (config, placement) = config_and_placement();
        let holders = resolve_shard_holders(&config, &placement);

        assert_eq!(
            write_route(Some(&config), &placement, holders[0]),
            MetadataWriteRoute::Local
        );
    }

    #[test]
    fn non_holder_writes_forward() {
        // Rank order is the holder set's own: rank-0 is tried first, the rest on
        // failure. Replica 2 of 4 servers guarantees a non-holder exists.
        let (config, placement) = config_and_placement();
        let holders = resolve_shard_holders(&config, &placement);
        let outsider = (1..=4u8)
            .map(node)
            .find(|candidate| !holders.contains(candidate))
            .expect("a replica-capped bucket leaves a non-holder");

        assert_eq!(
            write_route(Some(&config), &placement, outsider),
            MetadataWriteRoute::Forward(holders)
        );
    }

    #[test]
    fn user_node_writes_forward() {
        // A User-kind node is never sync-eligible, so it holds no bucket at all:
        // locality is unattainable for it and every write must be forwarded. The
        // receiving half — a holder accepting that forward from a User peer, and
        // applying it under the caller's token — is
        // `metadata_forwarding::user_node_forwards_create`, which needs a real
        // node and a real token and so cannot live here.
        let (mut config, placement) = config_and_placement();
        config.ensure_node(node(9), RealmNodeKind::User);

        assert!(matches!(
            write_route(Some(&config), &placement, node(9)),
            MetadataWriteRoute::Forward(_)
        ));
    }

    #[test]
    fn missing_config_forwards() {
        let (_, placement) = config_and_placement();

        assert_eq!(
            write_route(None, &placement, node(1)),
            MetadataWriteRoute::Forward(Vec::new())
        );
    }

    #[test]
    fn unplaced_writes_stay_local() {
        // No strategy governs a NIL ref (early bootstrap): nowhere to forward to,
        // and no sharding to respect.
        let (config, _) = config_and_placement();

        assert_eq!(
            write_route(Some(&config), &PlacementRef::NIL, node(1)),
            MetadataWriteRoute::Local
        );
        assert_eq!(
            write_route(None, &PlacementRef::NIL, node(1)),
            MetadataWriteRoute::Local
        );
    }

    #[test]
    fn ambiguous_delivery_stops() {
        assert_eq!(
            retry_disposition(MetadataRequestDelivery::PossiblySent),
            RetryDisposition::Stop
        );
        assert_eq!(
            retry_disposition(MetadataRequestDelivery::DefinitelyNotSent),
            RetryDisposition::TryNext
        );
    }

    #[test]
    fn nonholder_read_rejected() {
        let realm_id = RealmId::from_bytes([8u8; 32]);
        let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        config.seed_default_placement();
        for seed in 1..=4u8 {
            config.ensure_node(node(seed), RealmNodeKind::Server);
        }
        let document_id = MetaResourceId::from_parts(
            1,
            PlacementHandle::new(METADATA_HANDLE).unwrap(),
            BucketId::new(9).unwrap(),
            1,
        )
        .unwrap()
        .as_ulid();
        let placement = resolve_metadata_id(&config, realm_id, None, document_id).unwrap();
        let holders = resolve_shard_holders(&config, &placement);
        let outsider = (1..=4u8)
            .map(node)
            .find(|candidate| !holders.contains(candidate))
            .unwrap();

        assert!(!holds_metadata_id(&config, realm_id, outsider, document_id));
    }

    #[test]
    fn response_records_checked() {
        let realm_id = RealmId::from_bytes([8u8; 32]);
        let group_id = Ulid::from_bytes([3u8; 16]);
        let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        config.seed_default_placement();
        let document_id = MetaResourceId::from_parts(
            1,
            PlacementHandle::new(METADATA_HANDLE).unwrap(),
            BucketId::new(9).unwrap(),
            1,
        )
        .unwrap()
        .as_ulid();
        let placement =
            resolve_metadata_id(&config, realm_id, Some(group_id), document_id).unwrap();
        let record = MetadataRegistryRecord {
            realm_id,
            group_id,
            document_id,
            document_path: "docs/one".to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: true,
            permission_path: MetadataRegistryRecord::permission_path_for(
                &realm_id,
                group_id,
                "docs/one",
                document_id,
            ),
            placement,
            holder_node_ids: Vec::new(),
            created_at_ms: 1,
            updated_at_ms: 1,
            establishing_event_id: Ulid::from_bytes([4u8; 16]),
            last_event_id: Ulid::from_bytes([4u8; 16]),
        };

        assert!(routed_record_matches(
            &config,
            realm_id,
            document_id,
            &placement,
            &record,
        ));
        let mut substituted = record.clone();
        substituted.document_id = Ulid::from_bytes([5u8; 16]);
        assert!(!routed_record_matches(
            &config,
            realm_id,
            document_id,
            &placement,
            &substituted,
        ));

        let create = CreateMetadataDocumentConfig {
            actor: Actor {
                node_id: node(1),
                user_id: aruna_core::UserId::local(Ulid::from_bytes([6u8; 16]), realm_id),
                realm_id,
            },
            group_id,
            document_id: Ulid::nil(),
            document_path: "/docs/one/".to_string(),
            public: true,
            payload: crate::create_metadata_document::CreateMetadataDocumentPayload::Scaffold {
                name: "one".to_string(),
                description: String::new(),
                date_published: "2026-01-01".to_string(),
                license: None,
            },
        };
        assert!(create_record_matches(
            &create,
            document_id,
            &placement,
            &record
        ));
        let mut moved = record.clone();
        moved.placement.shard += 1;
        assert!(!create_record_matches(
            &create,
            document_id,
            &placement,
            &moved
        ));

        let mut changed = record.clone();
        changed.document_path = "docs/two".to_string();
        assert!(!update_record_matches(&record, &changed));
    }

    fn revoke_message() -> MetadataTransportMessage {
        MetadataTransportMessage::ForwardTokenRevocation {
            auth_token: MetadataAuthToken::bearer("caller-token").unwrap(),
            token: "target-token".to_string(),
        }
    }

    #[tokio::test]
    async fn capacity_then_success() {
        let peers = [node(1), node(2)];
        let order = rank_revoke_peers(
            peers.iter().copied(),
            bearer_token_hash("target-token").as_bytes(),
        );
        let mut calls = Vec::new();
        let result = run_revoke(
            &order,
            revoke_message(),
            Instant::now() + TOKEN_REVOKE_DEADLINE,
            |peer, _| {
                calls.push(peer);
                std::future::ready(Ok(if peer == order[0] {
                    MetadataTransportMessage::ForwardedTokenRevocationCapacity
                } else {
                    MetadataTransportMessage::ForwardedTokenRevoked
                }))
            },
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(calls, order);
    }

    #[tokio::test]
    async fn retries_possible_send() {
        let peers = [node(1), node(2)];
        let order = rank_revoke_peers(
            peers.iter().copied(),
            bearer_token_hash("target-token").as_bytes(),
        );
        let mut calls = Vec::new();
        let result = run_revoke(
            &order,
            revoke_message(),
            Instant::now() + TOKEN_REVOKE_DEADLINE,
            |peer, _| {
                calls.push(peer);
                if peer == order[0] {
                    std::future::ready(Err(MetadataRequestError::possibly_sent(
                        MetadataError::HandleMissing,
                    )))
                } else {
                    std::future::ready(Ok(MetadataTransportMessage::ForwardedTokenRevoked))
                }
            },
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(calls, order);
    }

    #[tokio::test]
    async fn all_capacity_unavailable() {
        let peers = [node(1), node(2)];
        let order = rank_revoke_peers(
            peers.iter().copied(),
            bearer_token_hash("target-token").as_bytes(),
        );
        let mut calls = Vec::new();
        let result = run_revoke(
            &order,
            revoke_message(),
            Instant::now() + TOKEN_REVOKE_DEADLINE,
            |peer, _| {
                calls.push(peer);
                std::future::ready(Ok(
                    MetadataTransportMessage::ForwardedTokenRevocationCapacity,
                ))
            },
        )
        .await;

        assert!(matches!(result, Err(MetadataApiError::ServiceUnavailable)));
        assert_eq!(calls, order);
    }

    #[tokio::test]
    async fn reject_stops_retry() {
        let peers = [node(1), node(2)];
        let order = rank_revoke_peers(
            peers.iter().copied(),
            bearer_token_hash("target-token").as_bytes(),
        );
        let mut calls = Vec::new();
        let result = run_revoke(
            &order,
            revoke_message(),
            Instant::now() + TOKEN_REVOKE_DEADLINE,
            |peer, _| {
                calls.push(peer);
                std::future::ready(Ok(MetadataTransportMessage::Reject(
                    "invalid token".to_string(),
                )))
            },
        )
        .await;

        assert!(matches!(result, Err(MetadataApiError::ServiceUnavailable)));
        assert_eq!(calls, vec![order[0]]);
    }

    #[tokio::test]
    async fn no_retry_loop() {
        let peer = node(1);
        let peers = vec![peer, peer];
        let mut calls = Vec::new();
        let result = run_revoke(
            &peers,
            revoke_message(),
            Instant::now() + TOKEN_REVOKE_DEADLINE,
            |peer, _| {
                calls.push(peer);
                std::future::ready(Ok(
                    MetadataTransportMessage::ForwardedTokenRevocationCapacity,
                ))
            },
        )
        .await;

        assert!(matches!(result, Err(MetadataApiError::ServiceUnavailable)));
        assert_eq!(calls, vec![peer]);
    }

    #[test]
    fn bounded_peer_order() {
        let peers = (1..=16).map(node).collect::<Vec<_>>();
        let reversed = peers.iter().copied().rev().collect::<Vec<_>>();
        let subject = bearer_token_hash("target-token");
        let first = rank_revoke_peers(peers.iter().copied(), subject.as_bytes());
        let second = rank_revoke_peers(reversed.iter().copied(), subject.as_bytes());

        assert_eq!(first, second);
        assert_eq!(first.len(), TOKEN_REVOKE_PEER_LIMIT);
        assert!(first.iter().all(|peer| peers.contains(peer)));
    }

    #[test]
    fn holders_deduplicate() {
        let first = node(1);
        let second = node(2);

        assert_eq!(
            distinct_holders(&[first, second, first, second]),
            vec![first, second]
        );
    }

    #[test]
    fn frozen_holders_intersect() {
        let current = [node(1), node(2)];
        let frozen = [node(2), node(3)];

        assert_eq!(holder_intersection(&current, &frozen), vec![node(2)]);
        assert!(holder_intersection(&[node(1)], &[node(2)]).is_empty());
    }

    #[tokio::test]
    async fn deadline_stops_calls() {
        let peers = vec![node(1), node(2)];
        let mut calls = Vec::new();
        let result = run_revoke(&peers, revoke_message(), Instant::now(), |peer, _| {
            calls.push(peer);
            std::future::ready(Ok(MetadataTransportMessage::ForwardedTokenRevoked))
        })
        .await;

        assert!(matches!(result, Err(MetadataApiError::ServiceUnavailable)));
        assert!(calls.is_empty());
    }
}
