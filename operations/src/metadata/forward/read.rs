//! Routes metadata reads such as get, profile status and RO-Crate export to holder nodes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::device::replica::ReplicaRecord;
use crate::device::replica::read_replica;
use crate::driver::DriverContext;
use crate::metadata::api::ExportMetadataRequest;
use crate::metadata::api::ExportMetadataResult;
use crate::metadata::api::GetVisibleRequest;
use crate::metadata::api::MetadataApiError;
use crate::metadata::api::RoCrateExportView;
use crate::metadata::api::ensure_record_readable;
use crate::metadata::api::export_metadata_rocrate;
use crate::metadata::api::get_visible_document;
use crate::metadata::api::load_live_record;
use crate::metadata::create_document::resolve_metadata_id;
use crate::metadata::profile::validation::current_validation_status;
use crate::metadata::profile::validation::revalidate_current;
use crate::metadata::protocol::AuthToken;
use crate::metadata::protocol::MetadataReadError;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::metadata::raw_revision::MetadataRawView;
use crate::metadata::raw_revision::load_raw_view;
use crate::placement::process_placements::load_realm_config;
use crate::placement::read_holder_sets;
use crate::realm::peer_trust::PeerTrust;
use crate::realm::peer_trust::ensure_peer_trust;
use aruna_core::NodeId;
use aruna_core::metadata::MaterializationState;
use aruna_core::metadata::MetadataMergedRevision;
use aruna_core::metadata::MetadataQueryResults;
use aruna_core::metadata::MetadataRawRevision;
use aruna_core::metadata::ProfileValidationStatus;
use aruna_core::metadata::raw_context_digest;
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

use crate::forward::authorize::is_sync_eligible;
use crate::forward::replay::routed_record_matches;
use crate::forward::routing::holds_metadata_id;
use crate::forward::transport::read_error;
use futures_util::StreamExt;

pub(super) const READ_FANOUT_LIMIT: usize = 8;

pub(super) const READ_PEER_TIMEOUT: Duration = Duration::from_secs(2);

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

/// Exports one document from this device's replica. The displayed render is
/// what the device holds, and the graph views come from the local craqle graph
/// the replica installed.
pub(super) async fn device_export(
    context: &Arc<DriverContext>,
    replica: ReplicaRecord,
    request: &ExportMetadataRequest,
) -> Result<ExportMetadataResult, MetadataApiError> {
    let record = replica
        .record
        .map(|record| *record)
        .ok_or(MetadataApiError::ServiceUnavailable)?;
    ensure_record_readable(
        context.as_ref(),
        record.realm_id,
        request.auth.as_ref(),
        &record,
        None,
    )
    .await?;
    let handle = context
        .metadata_handle
        .clone()
        .ok_or(MetadataApiError::ServiceUnavailable)?;
    match request.view {
        RoCrateExportView::Full => Ok(ExportMetadataResult::Full {
            jsonld: replica.displayed_jsonld,
            record,
        }),
        RoCrateExportView::Raw => {
            let merged = if replica.findings > 0 {
                Some(
                    handle
                        .export_rocrate_jsonld(record.graph_iri.clone())
                        .await
                        .map_err(|_| MetadataApiError::ServiceUnavailable)?,
                )
            } else {
                None
            };
            Ok(ExportMetadataResult::Raw {
                raw: MetadataRawView {
                    revision: device_raw_revision(
                        &replica.displayed_jsonld,
                        replica.dataset_digest,
                        replica.findings,
                        record.last_event_id,
                        merged,
                    ),
                    projection_state: MaterializationState::Materialized,
                    projected_event_id: Some(record.last_event_id),
                },
                dataset_digest: replica.dataset_digest,
                record,
            })
        }
        RoCrateExportView::Summary => Ok(ExportMetadataResult::Summary {
            jsonld: handle
                .export_summary_jsonld(record.graph_iri.clone())
                .await
                .map_err(|_| MetadataApiError::ServiceUnavailable)?,
            record,
        }),
        RoCrateExportView::Page => Ok(ExportMetadataResult::Page {
            page: handle
                .export_rocrate_page(
                    record.graph_iri.clone(),
                    request.limit.unwrap_or(100).clamp(1, 1_000),
                    request.offset,
                    request.after.clone(),
                )
                .await
                .map_err(|_| MetadataApiError::ServiceUnavailable)?,
            record,
        }),
    }
}

pub(super) fn device_raw_revision(
    displayed_jsonld: &str,
    dataset_digest: Option<[u8; 32]>,
    findings: u32,
    winning_event_id: Ulid,
    merged_jsonld: Option<String>,
) -> MetadataRawRevision {
    MetadataRawRevision {
        context_digest: raw_context_digest(displayed_jsonld).unwrap_or_default(),
        jsonld: displayed_jsonld.to_string(),
        winning_event_id,
        dataset_digest,
        merged: merged_jsonld.map(|jsonld| MetadataMergedRevision { jsonld, findings }),
    }
}

pub async fn get_metadata_routed(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    request: GetVisibleRequest,
    auth_token: Option<AuthToken>,
) -> Result<MetadataRegistryRecord, MetadataApiError> {
    if context.net_handle.is_none() {
        return get_visible_document(context.as_ref(), realm_id, request).await;
    }
    let config = load_realm_config(context, realm_id)
        .await
        .ok_or(MetadataApiError::ServiceUnavailable)?;
    let local_node = context.net_handle.as_ref().map(|net| net.node_id());
    // A device answers from its own replica, so a selected document stays
    // readable while the realm is out of reach.
    if let Some(replica) = device_replica(context, &config, local_node, request.document_id).await {
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
        return Ok(record);
    }
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
                let record = get_visible_document(context.as_ref(), realm_id, request)
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
            revalidate_current(context.as_ref(), &registry).await
        } else {
            current_validation_status(context.as_ref(), &registry).await
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
                    revalidate_current(context.as_ref(), &record).await
                } else {
                    current_validation_status(context.as_ref(), &record).await
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
            Err(MetadataReadError::Unavailable) => unavailable = true,
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

/// Exports locally on a holder or forwards with the caller's bearer or
/// peer-attested internal principal for another READ check.
pub async fn export_rocrate_routed(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    request: ExportMetadataRequest,
    forward_token: Option<AuthToken>,
    metadata_bytes: u64,
) -> Result<ExportMetadataResult, MetadataApiError> {
    if context.net_handle.is_none() {
        let export = export_metadata_rocrate(context.as_ref(), realm_id, request).await?;
        ensure_export_limit(&export, metadata_bytes)?;
        return Ok(export);
    }
    let config = load_realm_config(context, realm_id)
        .await
        .ok_or(MetadataApiError::ServiceUnavailable)?;
    let local_node = context.net_handle.as_ref().map(|net| net.node_id());
    if let Some(replica) = device_replica(context, &config, local_node, request.document_id).await {
        let export = device_export(context, replica, &request).await?;
        ensure_export_limit(&export, metadata_bytes)?;
        return Ok(export);
    }
    let config_digest = config
        .digest()
        .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    let placement = resolve_metadata_id(&config, realm_id, None, request.document_id)
        .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    let holders =
        read_holder_sets(&config, &placement).map_err(MetadataApiError::PlacementUnavailable)?;
    let holder_count = holders.len();
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
            Err(error @ (MetadataReadError::Unauthorized | MetadataReadError::Forbidden)) => {
                auth_error.get_or_insert(error);
            }
            Err(MetadataReadError::NotFound) => not_found += 1,
            Err(MetadataReadError::Unavailable) => unavailable = true,
        }
    }
    let conflict = success.is_some() && not_found > 0;
    let all_not_found = holder_count > 0 && not_found == holder_count;
    match reduce_holder_reads(
        success,
        auth_error,
        all_not_found,
        conflict,
        unavailable,
        AuthFailure::Fatal,
    ) {
        ReadDecision::Success(export) => Ok(export),
        ReadDecision::NotFound => Err(MetadataApiError::NotFound),
        ReadDecision::Auth(MetadataReadError::Unauthorized) => Err(MetadataApiError::Unauthorized),
        ReadDecision::Auth(MetadataReadError::Forbidden) => Err(MetadataApiError::Forbidden),
        ReadDecision::Auth(_) | ReadDecision::Unavailable => {
            Err(MetadataApiError::ServiceUnavailable)
        }
    }
}

pub(super) fn ensure_export_limit(
    export: &ExportMetadataResult,
    metadata_bytes: u64,
) -> Result<(), MetadataApiError> {
    let length = match export {
        ExportMetadataResult::Full { jsonld, .. }
        | ExportMetadataResult::Summary { jsonld, .. } => jsonld.len(),
        ExportMetadataResult::Page { page, .. } => page.jsonld.len(),
        ExportMetadataResult::Raw { raw, .. } => raw.revision.jsonld.len(),
    };
    if u64::try_from(length).unwrap_or(u64::MAX) > metadata_bytes {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    Ok(())
}

/// The only document path the Profile validation channel serves. Serving
/// nothing else is the whole authorization of a fetch that asserts no user.
pub(super) const PROFILE_DOCUMENT_PREFIX: &str = "profiles/";

/// Reads one registered Profile at an exact revision so a holder can validate a
/// Dataset without a caller. Infrastructure nodes use the user-less purpose-bound
/// channel; devices read a replica or ask under their owner's authority only.
pub(crate) async fn export_profile_routed(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    profile_id: Ulid,
    expected_revision: Ulid,
) -> Result<ExportMetadataResult, MetadataReadError> {
    let Some(net_handle) = context.net_handle.as_ref() else {
        return export_profile_local(context.as_ref(), realm_id, profile_id, expected_revision)
            .await;
    };
    let local_node = net_handle.node_id();
    let config = load_realm_config(context, realm_id)
        .await
        .ok_or(MetadataReadError::Unavailable)?;
    if !is_sync_eligible(&config, local_node) {
        return export_as_owner(context, &config, realm_id, local_node, profile_id).await;
    }
    let config_digest = config
        .digest()
        .map_err(|_| MetadataReadError::Unavailable)?;
    let placement = resolve_metadata_id(&config, realm_id, None, profile_id)
        .map_err(|_| MetadataReadError::Unavailable)?;
    let holders =
        read_holder_sets(&config, &placement).map_err(|_| MetadataReadError::Unavailable)?;
    let holder_count = holders.len();
    let context = Arc::clone(context);
    let metadata = context.metadata_handle.clone();
    let (responses, timed_out) = read_holders(holders, move |holder| {
        let context = context.clone();
        let metadata = metadata.clone();
        Box::pin(async move {
            if holder == local_node {
                return export_profile_local(
                    context.as_ref(),
                    realm_id,
                    profile_id,
                    expected_revision,
                )
                .await;
            }
            let Some(metadata) = metadata else {
                return Err(MetadataReadError::Unavailable);
            };
            metadata
                .request_export(
                    holder,
                    MetadataTransportMessage::ForwardExportProfile {
                        config_digest,
                        profile_id,
                        expected_revision,
                    },
                )
                .await
                .unwrap_or(Err(MetadataReadError::Unavailable))
        })
    })
    .await;
    collect_profile_export(responses, holder_count, timed_out)
}

/// Any answer is the exact revision that was asked for, so a lagging holder's
/// not-found never outranks a holder that served that revision.
pub(super) fn collect_profile_export(
    responses: Vec<(NodeId, Result<ExportMetadataResult, MetadataReadError>)>,
    holder_count: usize,
    timed_out: bool,
) -> Result<ExportMetadataResult, MetadataReadError> {
    let mut not_found = 0usize;
    let mut success = None;
    let mut auth_error = None;
    let mut unavailable = timed_out;
    for (_, response) in responses {
        match response {
            Ok(export) => {
                success.get_or_insert(export);
            }
            Err(error @ (MetadataReadError::Unauthorized | MetadataReadError::Forbidden)) => {
                auth_error.get_or_insert(error);
            }
            Err(MetadataReadError::NotFound) => not_found += 1,
            Err(MetadataReadError::Unavailable) => unavailable = true,
        }
    }
    let all_not_found = holder_count > 0 && not_found == holder_count;
    match reduce_holder_reads(
        success,
        auth_error,
        all_not_found,
        false,
        unavailable,
        AuthFailure::Unavailable,
    ) {
        ReadDecision::Success(export) => Ok(export),
        ReadDecision::NotFound => Err(MetadataReadError::NotFound),
        ReadDecision::Auth(_) | ReadDecision::Unavailable => Err(MetadataReadError::Unavailable),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AuthFailure {
    Fatal,
    Unavailable,
}

pub(crate) enum ReadDecision<T> {
    Success(T),
    NotFound,
    Auth(MetadataReadError),
    Unavailable,
}

/// Reduces holder answers; `conflict` outranks success and `all_not_found`
/// is only `NotFound` while no holder was unavailable.
pub(crate) fn reduce_holder_reads<T>(
    success: Option<T>,
    auth_error: Option<MetadataReadError>,
    all_not_found: bool,
    conflict: bool,
    mut unavailable: bool,
    auth_failure: AuthFailure,
) -> ReadDecision<T> {
    if let Some(error) = auth_error {
        match auth_failure {
            AuthFailure::Fatal => return ReadDecision::Auth(error),
            AuthFailure::Unavailable => unavailable = true,
        }
    }
    if conflict {
        return ReadDecision::Unavailable;
    }
    if let Some(value) = success {
        return ReadDecision::Success(value);
    }
    if all_not_found && !unavailable {
        ReadDecision::NotFound
    } else {
        ReadDecision::Unavailable
    }
}

/// A device never joins the validation channel: it reads its own replica first
/// and otherwise asks a holder as its owner, so it learns nothing its owner
/// may not read.
pub(super) async fn export_as_owner(
    context: &Arc<DriverContext>,
    config: &RealmConfigDocument,
    realm_id: RealmId,
    local_node: NodeId,
    profile_id: Ulid,
) -> Result<ExportMetadataResult, MetadataReadError> {
    let owner = crate::realm::mutate_placement::node_kind(config, local_node)
        .and_then(|kind| kind.owner())
        .ok_or(MetadataReadError::Unavailable)?;
    let auth = AuthContext {
        user_id: owner,
        realm_id,
        path_restrictions: None,
        session: None,
    };
    export_rocrate_routed(
        context,
        realm_id,
        ExportMetadataRequest {
            document_id: profile_id,
            auth: Some(auth.clone()),
            view: RoCrateExportView::Raw,
            limit: None,
            offset: None,
            after: None,
        },
        Some(AuthToken::internal(auth)),
        u64::MAX,
    )
    .await
    .map_err(read_error)
}

/// Serves one registered Profile with no caller at all. The `profiles/` path,
/// the graph IRI and the revision fence are the whole authorization; every
/// other document is reported as not found.
pub async fn export_profile_local(
    context: &DriverContext,
    realm_id: RealmId,
    profile_id: Ulid,
    expected_revision: Ulid,
) -> Result<ExportMetadataResult, MetadataReadError> {
    let record = load_live_record(context, profile_id)
        .await
        .map_err(read_error)?;
    if record.realm_id != realm_id
        || record.document_id != profile_id
        || !record.document_path.starts_with(PROFILE_DOCUMENT_PREFIX)
        || record.graph_iri != MetadataRegistryRecord::graph_iri_for(profile_id)
    {
        return Err(MetadataReadError::NotFound);
    }
    if record.last_event_id != expected_revision {
        return Err(MetadataReadError::Unavailable);
    }
    let raw = load_raw_view(context, profile_id, None)
        .await
        .map_err(|_| MetadataReadError::Unavailable)?
        .ok_or(MetadataReadError::NotFound)?;
    if raw.revision.winning_event_id != expected_revision {
        return Err(MetadataReadError::Unavailable);
    }
    let dataset_digest = raw.revision.dataset_digest;
    Ok(ExportMetadataResult::Raw {
        record,
        raw,
        dataset_digest,
    })
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
    let export =
        export_profile_local(context.as_ref(), realm_id, profile_id, expected_revision).await?;
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
    let export = export_metadata_rocrate(
        context.as_ref(),
        realm_id,
        ExportMetadataRequest {
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
    let record = get_visible_document(
        context.as_ref(),
        realm_id,
        GetVisibleRequest {
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
