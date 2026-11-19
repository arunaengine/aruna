//! Reads the crate a dataset exports and records it as the export's snapshot.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::repository::ExportIdentity;
use aruna_core::structs::PersistentIdMapping;
use aruna_core::structs::execution::job::{ExportRoCrateSpec, ReasonCode};
use aruna_core::structs::identity::auth::AuthContext;
use serde_json::Value as JsonValue;
use ulid::Ulid;

use super::archive::{keep_references, map_crate_error, recognize_entities, web_entity};
use super::{ExportCheckpoint, ExportEntity, ExportFailure, ExportPhase};
use crate::jobs::repository::TransferError;
use crate::metadata::api::MetadataApiError;
use crate::metadata::api::export_types::{
    ExportMetadataRequest, ExportMetadataResult, RoCrateExportView,
};
use crate::metadata::forward::routed::export::RoutedExportOperation;
use crate::metadata::pid_read::ReadPidOperation;

fn unexpected_view() -> ExportFailure {
    ExportFailure::Permanent("metadata export returned an unexpected view".to_string())
}

fn snapshot_read_failure(error: MetadataApiError) -> ExportFailure {
    match error {
        MetadataApiError::NotFound => {
            ExportFailure::Permanent("metadata document has no raw RO-Crate revision".to_string())
        }
        MetadataApiError::Unauthorized | MetadataApiError::Forbidden => {
            ExportFailure::Permanent("metadata document READ permission denied".to_string())
        }
        MetadataApiError::BadRequest => {
            ExportFailure::Permanent("invalid metadata export request".to_string())
        }
        MetadataApiError::InvalidQuery(message) => ExportFailure::Permanent(message),
        MetadataApiError::ServiceUnavailable | MetadataApiError::TooLarge => {
            ExportFailure::Retryable("metadata document is unavailable".to_string())
        }
        MetadataApiError::PlacementUnavailable(error) => {
            ExportFailure::Retryable(error.to_string())
        }
        MetadataApiError::InvalidCursor(message)
        | MetadataApiError::SearchIncomplete(message)
        | MetadataApiError::Internal(message) => ExportFailure::Retryable(message),
    }
}

/// A crate with its event and context digest.
pub(super) type CrateRead = (String, Ulid, [u8; 32]);

/// Reads the crate a dataset exports: the raw revision, else a scaffold's rendered graph, the
/// crate the dataset view shows. A document holder answers, and it re-checks READ for this peer,
/// since a job on a job-control bucket rarely holds the document's bucket.
#[derive(Debug, PartialEq)]
pub(super) struct ReadCrateOperation {
    local_node: Option<NodeId>,
    auth: AuthContext,
    document_id: Ulid,
    metadata_bytes: u64,
    full: bool,
}

impl ReadCrateOperation {
    pub(super) fn new(
        local_node: Option<NodeId>,
        auth: AuthContext,
        document_id: Ulid,
        metadata_bytes: u64,
    ) -> Self {
        Self {
            local_node,
            auth,
            document_id,
            metadata_bytes,
            full: false,
        }
    }

    fn read(&self, view: RoCrateExportView) -> IoCall {
        IoCall::child(RoutedExportOperation::new(
            self.local_node,
            self.auth.realm_id,
            ExportMetadataRequest {
                document_id: self.document_id,
                auth: Some(self.auth.clone()),
                view,
                limit: None,
                offset: None,
                after: None,
            },
            Some(AuthToken::internal(self.auth.clone())),
            self.metadata_bytes,
        ))
    }
}

impl Operation for ReadCrateOperation {
    type Output = CrateRead;
    type Error = ExportFailure;

    fn first(&mut self) -> Result<Next<CrateRead>, ExportFailure> {
        Ok(Next::Call(self.read(RoCrateExportView::Raw)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<CrateRead>, ExportFailure> {
        let read = result.child::<ExportMetadataResult, MetadataApiError>()?;
        Ok(Next::Done(match (self.full, read) {
            (false, Ok(ExportMetadataResult::Raw { raw, .. })) => (
                raw.revision.jsonld,
                raw.revision.winning_event_id,
                raw.revision.context_digest,
            ),
            (false, Err(MetadataApiError::NotFound)) => {
                self.full = true;
                return Ok(Next::Call(self.read(RoCrateExportView::Full)));
            }
            (true, Ok(ExportMetadataResult::Full { record, jsonld })) => {
                let digest = aruna_core::metadata::raw_context_digest(&jsonld)
                    .map_err(|error| ExportFailure::Permanent(error.to_string()))?;
                (jsonld, record.last_event_id, digest)
            }
            (_, Ok(_)) => return Err(unexpected_view()),
            (_, Err(error)) => return Err(snapshot_read_failure(error)),
        }))
    }
}

/// The crate JSON-LD a repository export of the dataset would start from, with its revision.
#[derive(Debug, PartialEq)]
pub struct CrateJsonOperation {
    read: Option<ReadCrateOperation>,
}

impl CrateJsonOperation {
    /// `local_node` is this node on the network, if it has one.
    pub fn new(
        local_node: Option<NodeId>,
        auth: AuthContext,
        document_id: Ulid,
        metadata_bytes: u64,
    ) -> Self {
        let read = ReadCrateOperation::new(local_node, auth, document_id, metadata_bytes);
        Self { read: Some(read) }
    }
}

impl Operation for CrateJsonOperation {
    type Output = (String, Ulid);
    type Error = TransferError;

    fn first(&mut self) -> Result<Next<(String, Ulid)>, TransferError> {
        let read = self
            .read
            .take()
            .ok_or_else(|| TransferError::Retryable("the crate was already read".to_string()))?;
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<(String, Ulid)>, TransferError> {
        match result.child::<CrateRead, ExportFailure>()? {
            Ok((jsonld, event_id, _)) => Ok(Next::Done((jsonld, event_id))),
            Err(ExportFailure::Permanent(message)) => Err(TransferError::Permanent(message)),
            Err(ExportFailure::Validation(_)) => Err(TransferError::Permanent(
                "the dataset crate is invalid".to_string(),
            )),
            Err(error) => Err(TransferError::Retryable(format!("{error:?}"))),
        }
    }
}

/// The crate as read and validated, while its identifiers are fetched.
#[derive(Debug, PartialEq)]
struct ReadSnapshot {
    jsonld: String,
    event_id: Ulid,
    context_digest: [u8; 32],
    dataset_digest: [u8; 32],
    document: JsonValue,
    entities: Vec<ExportEntity>,
}

/// Takes the snapshot an export works from: the validated crate, its File entities and the
/// dataset's identifiers.
#[derive(Debug, PartialEq)]
pub(super) struct SnapshotOperation {
    spec: Arc<ExportRoCrateSpec>,
    local_node: Option<NodeId>,
    checkpoint: ExportCheckpoint,
    read: Option<ReadSnapshot>,
}

/// The dataset's own PID and live identifiers; a retired mapping has none.
fn identity_of(mapping: Option<PersistentIdMapping>) -> ExportIdentity {
    let Some(mapping) = mapping.filter(|mapping| !mapping.is_retired()) else {
        return ExportIdentity::default();
    };
    ExportIdentity {
        own: (mapping.is_active())
            .then(|| mapping.pid.clone())
            .into_iter()
            .collect(),
        identifiers: mapping.secondary_identifiers.into_iter().collect(),
        references: Vec::new(),
    }
}

impl SnapshotOperation {
    pub(super) fn new(
        spec: Arc<ExportRoCrateSpec>,
        local_node: Option<NodeId>,
        checkpoint: ExportCheckpoint,
    ) -> Self {
        Self {
            spec,
            local_node,
            checkpoint,
            read: None,
        }
    }

    fn validated(&self, read: CrateRead) -> Result<ReadSnapshot, ExportFailure> {
        let spec = &self.spec;
        let (jsonld, event_id, context_digest) = read;
        if jsonld.len() as u64 > spec.limits.metadata_bytes {
            return Err(ExportFailure::Permanent(format!(
                "RO-Crate metadata exceeds the {} byte limit",
                spec.limits.metadata_bytes
            )));
        }
        let canonical = craqle::validate_rocrate_jsonld(&jsonld).map_err(map_crate_error)?;
        let document: JsonValue = serde_json::from_str(&jsonld)
            .map_err(|error| ExportFailure::Permanent(error.to_string()))?;
        let realm_id = spec.auth_context.realm_id;
        let mut entities = recognize_entities(&document, &canonical.nquads, realm_id)?;
        if let Some(selection) = &spec.selection {
            keep_references(&mut entities, &selection.files);
        }
        if entities.len() as u64 > spec.limits.max_entries {
            return Err(ExportFailure::Permanent(format!(
                "RO-Crate has more than {} File entities",
                spec.limits.max_entries
            )));
        }
        Ok(ReadSnapshot {
            jsonld,
            event_id,
            context_digest,
            dataset_digest: canonical.digest,
            document,
            entities,
        })
    }

    fn finish(&mut self, read: ReadSnapshot, mut identity: ExportIdentity) -> ExportCheckpoint {
        let jsonld = if identity.identifiers.is_empty() {
            read.jsonld
        } else {
            let mut document = read.document;
            aruna_core::repository::fields::add_root_identifiers(&mut document, &identity);
            document.to_string()
        };
        identity.references = (read.entities.iter())
            .filter(|entity| entity.omission == Some(ReasonCode::External))
            .map(|entity| entity.entity_id.clone())
            .filter(|id| web_entity(id))
            .collect();
        let mut checkpoint = std::mem::take(&mut self.checkpoint);
        checkpoint.winning_event_id = Some(read.event_id);
        checkpoint.context_digest = Some(read.context_digest);
        checkpoint.dataset_digest = Some(read.dataset_digest);
        checkpoint.identity = identity;
        checkpoint.raw_jsonld = Some(jsonld);
        checkpoint.entities = read.entities;
        checkpoint.phase = ExportPhase::Resolve;
        checkpoint
    }
}

impl Operation for SnapshotOperation {
    type Output = ExportCheckpoint;
    type Error = ExportFailure;

    fn first(&mut self) -> Result<Next<ExportCheckpoint>, ExportFailure> {
        let spec = &self.spec;
        let read = ReadCrateOperation::new(
            self.local_node,
            spec.auth_context.clone(),
            spec.document_id,
            spec.limits.metadata_bytes,
        );
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ExportCheckpoint>, ExportFailure> {
        let Some(read) = self.read.take() else {
            let read = result.child::<CrateRead, ExportFailure>()??;
            self.read = Some(self.validated(read)?);
            let spec = &self.spec;
            let realm_id = spec.auth_context.realm_id;
            let pid = ReadPidOperation::new(self.local_node, realm_id, spec.document_id);
            return Ok(Next::Call(IoCall::child(pid)));
        };
        let mapping = (result.child::<Option<PersistentIdMapping>, MetadataApiError>()?).map_err(
            |error| ExportFailure::Retryable(format!("reading identifiers failed: {error}")),
        )?;
        Ok(Next::Done(self.finish(read, identity_of(mapping))))
    }
}

#[cfg(test)]
#[path = "snapshot_tests.rs"]
mod tests;
