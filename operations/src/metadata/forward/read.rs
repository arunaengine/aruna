//! Shared parts of routed metadata reads: status choice, summary export and Profile peer check.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::metadata::api::MetadataApiError;
use crate::metadata::api::export_types::ExportMetadataRequest;
use crate::metadata::api::export_types::ExportMetadataResult;
use crate::metadata::api::export_types::RoCrateExportView;
use crate::realm::peer_trust::PeerTrust;
use crate::realm::peer_trust::ensure_peer_trust;
use aruna_core::NodeId;
use aruna_core::metadata::ProfileValidationStatus;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::dataset_description::{DatasetSummaryRequest, DescriptionError};
use std::time::Duration;
use ulid::Ulid;

use super::routed::export::RoutedExportOperation;

pub(super) const METADATA_READ_DEADLINE: Duration = Duration::from_secs(12);

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

/// The routed export that reads the JSON-LD summary of a dataset with the caller's own access.
pub(crate) fn summary_export(
    local_node: Option<NodeId>,
    request: DatasetSummaryRequest,
) -> RoutedExportOperation {
    let export = ExportMetadataRequest {
        document_id: request.dataset,
        auth: request.auth,
        view: RoCrateExportView::Summary,
        limit: None,
        offset: None,
        after: None,
    };
    let (realm_id, token) = (request.realm_id, request.token);
    RoutedExportOperation::new(local_node, realm_id, export, token, SUMMARY_BYTES)
}

/// The summary a [`summary_export`] answered, or why the dataset has none for this caller.
pub(crate) fn summary_answer(
    export: Result<ExportMetadataResult, MetadataApiError>,
) -> Result<String, DescriptionError> {
    match export {
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
