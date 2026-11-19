//! Shared parts of routed metadata reads: status choice, summary exports, the dataset summary of
//! a bucket and the Profile peer check.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::metadata::api::MetadataApiError;
use crate::metadata::api::export_types::ExportMetadataRequest;
use crate::realm::peer_trust::PeerTrust;
use crate::realm::peer_trust::ensure_peer_trust;
use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, NetEvent};
use aruna_core::metadata::api::{ExportMetadataResult, RoCrateExportView};
use aruna_core::metadata::{AuthToken, ProfileValidationStatus};
use aruna_core::operation::Operation;
use aruna_core::rows::event;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::dataset_description::{
    DatasetDescription, DatasetSummaryRequest, DescriptionError, describe,
};
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

/// What the dataset says about a bucket, read from its holders with the caller's own access,
/// so a reader never sees a description it may not read.
#[derive(Debug, PartialEq)]
pub struct DatasetSummaryOperation {
    request: Option<DatasetSummaryRequest>,
}

impl DatasetSummaryOperation {
    pub fn new(
        realm_id: RealmId,
        dataset: Ulid,
        (auth, token): (Option<&AuthContext>, Option<AuthToken>),
    ) -> Self {
        let auth = auth.cloned();
        let request = DatasetSummaryRequest {
            realm_id,
            dataset,
            auth,
            token,
        };
        Self {
            request: Some(request),
        }
    }
}

impl Operation for DatasetSummaryOperation {
    type Output = DatasetDescription;
    type Error = DescriptionError;

    fn first(&mut self) -> Result<Next<DatasetDescription>, DescriptionError> {
        let local = Effect::Net(NetEffect::LocalNode);
        Ok(Next::Call(IoCall::Effect(Box::new(local))))
    }

    // The summary is read from the dataset's holders with the caller's own access.
    fn next(&mut self, result: IoResult) -> Result<Next<DatasetDescription>, DescriptionError> {
        if let Some(request) = self.request.take() {
            let Ok(Event::Net(NetEvent::LocalNode(local_node))) = event(result) else {
                return Err(DescriptionError::Unavailable);
            };
            let export = summary_export(local_node, request);
            return Ok(Next::Call(IoCall::child(export)));
        }
        let export = (result.child::<ExportMetadataResult, MetadataApiError>())
            .map_err(|_| DescriptionError::Unavailable)?;
        let summary = summary_answer(export)?;
        let described = describe(&summary).map_err(DescriptionError::Invalid)?;
        Ok(Next::Done(described))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn summary() -> DatasetSummaryOperation {
        DatasetSummaryOperation::new(
            RealmId::from_bytes([1; 32]),
            Ulid::from_parts(1, 1),
            (None, None),
        )
    }

    // A summary walked past its local node lookup to the export it reads.
    fn exporting() -> DatasetSummaryOperation {
        let mut operation = summary();
        let local = Effect::Net(NetEffect::LocalNode);
        assert_eq!(
            operation.first(),
            Ok(Next::Call(IoCall::Effect(Box::new(local))))
        );
        let node = IoResult::Event(Box::new(Event::Net(NetEvent::LocalNode(None))));
        assert!(matches!(
            operation.next(node),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        operation
    }

    fn exported(export: Result<ExportMetadataResult, MetadataApiError>) -> IoResult {
        IoResult::Child(aruna_core::calls::ChildOutcome::new(export))
    }

    // The summary is read once with the caller's access, and its root must be a Dataset.
    #[test]
    fn summary_needs_dataset() {
        let summary = ExportMetadataResult::Summary {
            record: crate::metadata::visible_scope::call_tests::record(1),
            jsonld: "{}".to_string(),
        };
        let refused = exporting().next(exported(Ok(summary)));
        assert!(matches!(refused, Err(DescriptionError::Invalid(_))));
    }

    // A refused export leaves the dataset unreadable; any other failure leaves it unavailable.
    #[test]
    fn summary_keeps_refusal() {
        let refused = exporting().next(exported(Err(MetadataApiError::Forbidden)));
        assert_eq!(refused, Err(DescriptionError::Unreadable));
        let failed = exporting().next(exported(Err(MetadataApiError::ServiceUnavailable)));
        assert_eq!(failed, Err(DescriptionError::Unavailable));
        assert_eq!(
            exporting().next(IoResult::Done),
            Err(DescriptionError::Unavailable)
        );
    }
}
