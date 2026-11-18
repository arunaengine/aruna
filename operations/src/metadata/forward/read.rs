//! Routes metadata reads such as get, profile status and RO-Crate export to holder nodes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::device::replica::ReplicaRecord;
use crate::device::replica_store::read_replica;
use crate::document_repository::GetConfigOperation;
use crate::driver::{DriverContext, drive};
use crate::metadata::api::MetadataApiError;
use crate::metadata::api::export::VisibleRecordOperation;
use crate::metadata::api::export_types::ExportMetadataRequest;
use crate::metadata::api::export_types::ExportMetadataResult;
use crate::metadata::api::export_types::GetVisibleRequest;
use crate::metadata::api::export_types::RoCrateExportView;
use crate::metadata::protocol::MetadataReadError;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::realm::peer_trust::PeerTrust;
use crate::realm::peer_trust::ensure_peer_trust;
use aruna_core::NodeId;
use aruna_core::metadata::MetadataQueryResults;
use aruna_core::metadata::ProfileValidationStatus;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::dataset_description::{DatasetSummaryRequest, DescriptionError};
use std::sync::Arc;
use std::time::Duration;
use ulid::Ulid;

use super::profile_export::ProfileLocalOperation;
use super::routed_export::{ExportWithinOperation, RoutedExportOperation, ensure_export_limit};
use crate::forward::routing::holds_metadata_id;
use crate::forward::transport::read_error;
use crate::realm::load_config::config_unavailable;
use crate::realm::peer_trust::is_sync_eligible;

pub(super) const METADATA_READ_DEADLINE: Duration = Duration::from_secs(12);

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
    drive(read_replica(document_id), context)
        .await
        .ok()
        .flatten()
        .filter(ReplicaRecord::serves_reads)
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

/// Bytes the summary of a dataset may have; only its root entity is read.
const SUMMARY_BYTES: u64 = 16 * 1024 * 1024;

/// The JSON-LD summary of a dataset, read from its holders with the caller's own access.
pub(crate) async fn dataset_summary(
    context: &DriverContext,
    request: DatasetSummaryRequest,
) -> Result<String, DescriptionError> {
    let export = ExportMetadataRequest {
        document_id: request.dataset,
        auth: request.auth,
        view: RoCrateExportView::Summary,
        limit: None,
        offset: None,
        after: None,
    };
    let local_node = context.net_handle.as_ref().map(|net| net.node_id());
    let (realm_id, token) = (request.realm_id, request.token);
    let routed = RoutedExportOperation::new(local_node, realm_id, export, token, SUMMARY_BYTES);
    match drive(routed, context).await {
        Ok(ExportMetadataResult::Summary { jsonld, .. }) => Ok(jsonld),
        Err(
            MetadataApiError::NotFound
            | MetadataApiError::Forbidden
            | MetadataApiError::Unauthorized
            | MetadataApiError::BadRequest,
        ) => Err(DescriptionError::Unreadable),
        Ok(_) | Err(_) => Err(DescriptionError::Unavailable),
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
    let config = drive(GetConfigOperation::new(realm_id), context)
        .await
        .inspect_err(config_unavailable)
        .ok()
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
    let config = drive(GetConfigOperation::new(realm_id), context)
        .await
        .inspect_err(config_unavailable)
        .ok()
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
    let config = drive(GetConfigOperation::new(realm_id), context)
        .await
        .inspect_err(config_unavailable)
        .ok()
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
