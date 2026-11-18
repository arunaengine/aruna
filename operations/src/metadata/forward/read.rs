//! Routes metadata reads such as get, profile status and RO-Crate export to holder nodes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::device::replica::ReplicaRecord;
use crate::device::replica_store::read_replica;
use crate::device::selection::keep_read;
use crate::driver::{DriverContext, drive};
use crate::metadata::api::MetadataApiError;
use crate::metadata::api::export::ExportOperation;
use crate::metadata::api::export::VisibleRecordOperation;
use crate::metadata::api::export_types::ExportMetadataRequest;
use crate::metadata::api::export_types::ExportMetadataResult;
use crate::metadata::api::export_types::GetVisibleRequest;
use crate::metadata::api::export_types::RoCrateExportView;
use crate::metadata::api::export_types::{ENTRY_ROOM, PAGE_DEFAULT, PAGE_MAX};
use crate::metadata::api::read::ensure_record_readable;
use crate::metadata::api::read::load_live_record;
use crate::metadata::create_document::resolve_metadata_id;
use crate::metadata::profile::rules::PROFILE_DOCUMENT_PREFIX;
use crate::metadata::profile::validation::{RevalidateOperation, ValidationStatusOperation};
use crate::metadata::protocol::MetadataReadError;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::metadata::revision_read::MetadataRawView;
use crate::metadata::revision_read::RawDisplayOperation;
use crate::placement::process_placements::load_realm_config;
use crate::realm::mutate_placement::node_kind;
use crate::realm::peer_trust::PeerTrust;
use crate::realm::peer_trust::ensure_peer_trust;
use aruna_core::NodeId;
use aruna_core::metadata::AuthToken;
use aruna_core::metadata::MaterializationState;
use aruna_core::metadata::MetadataMergedRevision;
use aruna_core::metadata::MetadataQueryResults;
use aruna_core::metadata::MetadataRawRevision;
use aruna_core::metadata::ProfileValidationStatus;
use aruna_core::metadata::raw_context_digest;
use aruna_core::placement::read_holder_sets;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use futures_util::future::BoxFuture;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tokio::time::timeout;
use ulid::Ulid;

use super::holder_reads::{
    AuthFailure, READ_FANOUT_LIMIT, READ_PEER_TIMEOUT, ReadDecision, collect_profile_export,
    device_raw_revision, reduce_holder_reads,
};
use super::profile_export::ProfileLocalOperation;
use super::routed_export::{ExportWithinOperation, ensure_export_limit, owner_reads};
use crate::forward::replay::routed_record_matches;
use crate::forward::routing::holds_metadata_id;
use crate::forward::transport::read_error;
use crate::realm::peer_trust::is_sync_eligible;
use futures_util::StreamExt;

pub(super) const METADATA_READ_DEADLINE: Duration = Duration::from_secs(12);

pub(super) async fn read_holders<T, F>(
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
            let result = timeout(READ_PEER_TIMEOUT, request)
                .await
                .unwrap_or(Err(MetadataReadError::Unavailable));
            (holder, result)
        }
    }))
    .buffer_unordered(READ_FANOUT_LIMIT);
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

/// The replica this device keeps of one document, when it can answer on its
/// own. A node that holds buckets is never a device and always reads its own
/// registry instead.
pub(super) async fn device_replica(
    context: &Arc<DriverContext>,
    config: &RealmConfigDocument,
    local_node: Option<NodeId>,
    document_id: Ulid,
) -> Option<ReplicaRecord> {
    if local_node.is_none_or(|node| is_sync_eligible(config, node)) {
        return None;
    }
    read_replica(context, document_id)
        .await
        .filter(ReplicaRecord::serves_reads)
}

pub async fn get_metadata_routed(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    request: GetVisibleRequest,
    auth_token: Option<AuthToken>,
) -> Result<MetadataRegistryRecord, MetadataApiError> {
    if context.net_handle.is_none() {
        return drive(
            VisibleRecordOperation::new(realm_id, request),
            context.as_ref(),
        )
        .await;
    }
    let config = load_realm_config(context, realm_id)
        .await
        .ok_or(MetadataApiError::ServiceUnavailable)?;
    let local_node = context.net_handle.as_ref().map(|net| net.node_id());
    // A device answers from its pinned replica, so a selected document stays readable while the
    // realm is out of reach; an unselected one is a cache the realm's answer refreshes.
    let cached = device_replica(context, &config, local_node, request.document_id).await;
    if let Some(replica) = cached
        .clone()
        .filter(|replica| replica.selected || replica.pending_edits > 0)
    {
        return replica_record(context, realm_id, &request, replica).await;
    }
    let keeps = owner_reads(&config, local_node, request.auth.as_ref());
    let routed = read_routed(
        context,
        realm_id,
        config,
        local_node,
        request.clone(),
        auth_token,
    )
    .await;
    match (routed, cached) {
        (Ok(record), _) => {
            if keeps {
                keep_read(context, &record).await;
            }
            Ok(record)
        }
        (Err(MetadataApiError::ServiceUnavailable), Some(replica)) => {
            replica_record(context, realm_id, &request, replica).await
        }
        (result, _) => result,
    }
}

/// The registry record a device replica holds, after the caller's own READ check.
async fn replica_record(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    request: &GetVisibleRequest,
    replica: ReplicaRecord,
) -> Result<MetadataRegistryRecord, MetadataApiError> {
    let record = replica
        .record
        .map(|record| *record)
        .ok_or(MetadataApiError::ServiceUnavailable)?;
    ensure_record_readable(
        context.as_ref(),
        realm_id,
        request.auth.as_ref(),
        &record,
        None,
    )
    .await?;
    Ok(record)
}

/// Reads one registry record from the document's holders.
async fn read_routed(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    config: RealmConfigDocument,
    local_node: Option<NodeId>,
    request: GetVisibleRequest,
    auth_token: Option<AuthToken>,
) -> Result<MetadataRegistryRecord, MetadataApiError> {
    let config_digest = config
        .digest()
        .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    let placement = resolve_metadata_id(&config, realm_id, None, request.document_id)
        .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    let holders =
        read_holder_sets(&config, &placement).map_err(MetadataApiError::PlacementUnavailable)?;
    let holder_count = holders.len();
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
                let record = drive(
                    VisibleRecordOperation::new(realm_id, request),
                    context.as_ref(),
                )
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
            Err(MetadataReadError::Unavailable | MetadataReadError::TooLarge) => unavailable = true,
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

/// Reads or recomputes Profile status on the document's holders. The holder
/// performs the same per-document READ check as the ordinary metadata route.
pub async fn route_profile_status(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    request: GetVisibleRequest,
    auth_token: Option<AuthToken>,
    revalidate: bool,
) -> Result<ProfileValidationStatus, MetadataApiError> {
    let registry = load_live_record(context.as_ref(), request.document_id).await?;
    if context.net_handle.is_none() {
        ensure_record_readable(
            context.as_ref(),
            realm_id,
            request.auth.as_ref(),
            &registry,
            None,
        )
        .await?;
        return if revalidate {
            drive(
                RevalidateOperation::new(
                    context
                        .as_ref()
                        .net_handle
                        .as_ref()
                        .map(|net| net.node_id()),
                    registry.clone(),
                ),
                context.as_ref(),
            )
            .await
        } else {
            drive(
                ValidationStatusOperation::new(registry.clone()),
                context.as_ref(),
            )
            .await
        }
        .map_err(|_| MetadataApiError::ServiceUnavailable);
    }
    let config = load_realm_config(context, realm_id)
        .await
        .ok_or(MetadataApiError::ServiceUnavailable)?;
    let config_digest = config
        .digest()
        .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    let placement = resolve_metadata_id(&config, realm_id, None, request.document_id)
        .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    let holders =
        read_holder_sets(&config, &placement).map_err(MetadataApiError::PlacementUnavailable)?;
    let holder_count = holders.len();
    let local_node = context.net_handle.as_ref().map(|net| net.node_id());
    let context = Arc::clone(context);
    let metadata = context.metadata_handle.clone();
    let request_template = request.clone();
    let (responses, timed_out) = read_holders(holders, move |holder| {
        let context = context.clone();
        let metadata = metadata.clone();
        let request = request_template.clone();
        let auth_token = auth_token.clone();
        Box::pin(async move {
            if Some(holder) == local_node {
                let record = load_live_record(context.as_ref(), request.document_id)
                    .await
                    .map_err(read_error)?;
                ensure_record_readable(
                    context.as_ref(),
                    realm_id,
                    request.auth.as_ref(),
                    &record,
                    None,
                )
                .await
                .map_err(read_error)?;
                if revalidate {
                    drive(
                        RevalidateOperation::new(
                            context
                                .as_ref()
                                .net_handle
                                .as_ref()
                                .map(|net| net.node_id()),
                            record.clone(),
                        ),
                        context.as_ref(),
                    )
                    .await
                } else {
                    drive(
                        ValidationStatusOperation::new(record.clone()),
                        context.as_ref(),
                    )
                    .await
                }
                .map_err(|_| MetadataReadError::Unavailable)
            } else {
                let Some(metadata) = metadata else {
                    return Err(MetadataReadError::Unavailable);
                };
                match metadata
                    .request_forwarded_write(
                        holder,
                        MetadataTransportMessage::ForwardValidationStatus {
                            auth_token,
                            config_digest,
                            document_id: request.document_id,
                            revalidate,
                        },
                    )
                    .await
                {
                    Ok(MetadataTransportMessage::ForwardedValidationStatus { result }) => {
                        result.map(|status| *status)
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
            Ok(status) => {
                keep_status(&mut success, status, registry.last_event_id);
            }
            Err(MetadataReadError::Unauthorized) => {
                auth_error.get_or_insert(MetadataApiError::Unauthorized);
            }
            Err(MetadataReadError::Forbidden) => {
                auth_error.get_or_insert(MetadataApiError::Forbidden);
            }
            Err(MetadataReadError::NotFound) => not_found += 1,
            Err(MetadataReadError::Unavailable | MetadataReadError::TooLarge) => unavailable = true,
        }
    }
    if let Some(error) = auth_error {
        return Err(error);
    }
    if success.is_some() && not_found > 0 {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    if let Some(status) = success {
        return Ok(status);
    }
    if !unavailable && holder_count > 0 && not_found == holder_count {
        Err(MetadataApiError::NotFound)
    } else {
        Err(MetadataApiError::ServiceUnavailable)
    }
}

pub(super) fn keep_status(
    current: &mut Option<ProfileValidationStatus>,
    incoming: ProfileValidationStatus,
    expected_revision: Ulid,
) {
    let incoming_exact = incoming.dataset_revision == expected_revision;
    let current_exact = current
        .as_ref()
        .is_some_and(|status| status.dataset_revision == expected_revision);
    if current.is_none() || incoming_exact && !current_exact {
        *current = Some(incoming);
    }
}

/// Whether a peer may ask the validation channel at all. The fetch vouches for
/// no user, so an owner-bound device is refused and only infrastructure peers
/// of this realm are admitted.
pub fn admits_profile_peer(config: &RealmConfigDocument, peer: NodeId, realm_id: RealmId) -> bool {
    ensure_peer_trust(config, peer, realm_id, PeerTrust::Vouched(None)).is_ok()
}

/// Answers a peer's Profile fetch. Every gate fails closed and none of them
/// reads a caller identity, because the channel never carries one.
pub(crate) async fn apply_forwarded_profile(
    context: &Arc<DriverContext>,
    peer: NodeId,
    message: MetadataTransportMessage,
    local_limit: u64,
) -> Result<(ExportMetadataResult, u64), MetadataReadError> {
    let MetadataTransportMessage::ForwardExportProfile {
        config_digest,
        profile_id,
        expected_revision,
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
    if !admits_profile_peer(&config, peer, realm_id) {
        return Err(MetadataReadError::Forbidden);
    }
    if !holds_metadata_id(&config, realm_id, net_handle.node_id(), profile_id) {
        return Err(MetadataReadError::Unavailable);
    }
    let local = ProfileLocalOperation::new(realm_id, profile_id, expected_revision);
    let export = drive(local, context.as_ref()).await?;
    ensure_export_limit(&export, local_limit).map_err(read_error)?;
    Ok((export, local_limit))
}

pub(crate) async fn apply_forwarded_export(
    context: &Arc<DriverContext>,
    peer: NodeId,
    message: MetadataTransportMessage,
    local_limit: u64,
) -> Result<(ExportMetadataResult, u64), MetadataReadError> {
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
    let metadata_bytes = metadata_bytes.min(local_limit);
    let request = ExportMetadataRequest {
        document_id,
        auth,
        view,
        limit,
        offset,
        after,
    };
    let export = ExportWithinOperation::new(realm_id, request, metadata_bytes);
    let export = drive(export, context.as_ref()).await.map_err(read_error)?;
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
    let record = drive(
        VisibleRecordOperation::new(
            realm_id,
            GetVisibleRequest {
                document_id,
                auth: auth.clone(),
            },
        ),
        context.as_ref(),
    )
    .await
    .map_err(read_error)?;
    crate::metadata::query_authorized_local(context, auth, Some(vec![record.graph_iri]), sparql)
        .await
        .map_err(|_| MetadataReadError::Unavailable)
}
