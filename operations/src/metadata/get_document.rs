//! Reads a metadata document view from its registry row, lifecycle and materialization state.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::keyspaces::{
    GRAPH_LIFECYCLE_KEYSPACE, MATERIALIZATION_STATUS_KEYSPACE, METADATA_INDEX_KEYSPACE,
};
use aruna_core::metadata::{
    GraphLifecycleRecord, MaterializationState, MaterializationStatusRecord, MetadataDocumentView,
    MetadataEffect, MetadataError, MetadataEvent,
};
use aruna_core::operation::Operation;
use aruna_core::rows::{decode, decoded, read_row, values};
use aruna_core::storage_entries::{
    graph_lifecycle_key, materialization_status_key, metadata_registry_key,
};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::GroupId;
use thiserror::Error;
use ulid::Ulid;

use crate::metadata::call_results::effect_event;

/// Reads the registry row, then its graph lifecycle and materialization status in one batch,
/// then exports the graph. A deleted graph or a graph not yet materialized for the row's last
/// event is not found.
#[derive(Debug, PartialEq)]
pub struct GetDocumentOperation {
    group_id: GroupId,
    document_id: Ulid,
    record: Option<MetadataRegistryRecord>,
    step: Step,
}

#[derive(Debug, PartialEq)]
enum Step {
    Record,
    State,
    Export,
}

#[derive(Debug, Error, PartialEq)]
pub enum GetDocumentError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error(transparent)]
    MetadataError(#[from] MetadataError),
    #[error("document not found")]
    DocumentNotFound,
}

type GetTurn = Result<Next<MetadataDocumentView>, GetDocumentError>;

impl GetDocumentOperation {
    pub fn new(group_id: GroupId, document_id: Ulid) -> Self {
        Self {
            group_id,
            document_id,
            record: None,
            step: Step::Record,
        }
    }

    fn record_read(&mut self, result: IoResult) -> GetTurn {
        let record: MetadataRegistryRecord =
            decoded(result)?.ok_or(GetDocumentError::DocumentNotFound)?;
        let reads = vec![
            (
                GRAPH_LIFECYCLE_KEYSPACE.to_string(),
                graph_lifecycle_key(&record.graph_iri),
            ),
            (
                MATERIALIZATION_STATUS_KEYSPACE.to_string(),
                materialization_status_key(record.document_id),
            ),
        ];
        self.record = Some(record);
        self.step = Step::State;
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn state_read(&mut self, result: IoResult) -> GetTurn {
        let [lifecycle, status] = values::<2>(result)?;
        let lifecycle: Option<GraphLifecycleRecord> =
            lifecycle.map(|value| decode(&value)).transpose()?;
        if lifecycle.is_some_and(|lifecycle| lifecycle.is_deleted()) {
            return Err(GetDocumentError::DocumentNotFound);
        }
        let record = self
            .record
            .as_ref()
            .ok_or(GetDocumentError::DocumentNotFound)?;
        let status: Option<MaterializationStatusRecord> =
            status.map(|value| decode(&value)).transpose()?;
        if status.is_some_and(|status| {
            status.event_id == record.last_event_id
                && !matches!(status.state, MaterializationState::Materialized)
        }) {
            return Err(MetadataError::GraphNotFound.into());
        }
        let graph_iri = record.graph_iri.clone();
        self.step = Step::Export;
        let export = Effect::Metadata(MetadataEffect::ExportRoCrate { graph_iri });
        Ok(Next::Call(IoCall::Effect(Box::new(export))))
    }

    fn exported(&mut self, result: IoResult) -> GetTurn {
        match effect_event(result)? {
            Event::Metadata(MetadataEvent::RoCrateExportResult { jsonld, .. }) => {
                let record = self
                    .record
                    .take()
                    .ok_or(GetDocumentError::DocumentNotFound)?;
                Ok(Next::Done(MetadataDocumentView { record, jsonld }))
            }
            Event::Metadata(MetadataEvent::Error { error, .. }) => Err(error.into()),
            other => Err(IoError::Unexpected {
                expected: "MetadataEvent::RoCrateExportResult",
                got: other.label(),
            }
            .into()),
        }
    }
}

impl Operation for GetDocumentOperation {
    type Output = MetadataDocumentView;
    type Error = GetDocumentError;

    fn first(&mut self) -> GetTurn {
        let key = metadata_registry_key(self.group_id, self.document_id);
        Ok(Next::Call(read_row(METADATA_INDEX_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> GetTurn {
        match self.step {
            Step::Record => self.record_read(result),
            Step::State => self.state_read(result),
            Step::Export => self.exported(result),
        }
    }

    // A read of a document that does not exist yet, or whose graph has not
    // materialized, is a 404 for the caller, not a node failure.
    fn expected_error(error: &Self::Error) -> bool {
        matches!(
            error,
            GetDocumentError::DocumentNotFound
                | GetDocumentError::MetadataError(MetadataError::GraphNotFound)
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::driver::DriverContext;
    use crate::metadata::record_reads::RecordMaterializedOperation;
    use aruna_core::effects::StorageEffect;
    use aruna_core::handle::Handle;
    use aruna_core::metadata::{MaterializationState, MaterializationStatusRecord};
    use aruna_core::storage_entries::materialization_status_entry;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::record::PlacementRef;
    use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
    use aruna_storage::storage::FjallStorage;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn make_record(document_id: Ulid, last_event_id: Ulid) -> MetadataRegistryRecord {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let group_id = Ulid::generate();
        let path = format!("datasets/{document_id}");
        MetadataRegistryRecord {
            realm_id,
            group_id,
            document_id,
            document_path: path.clone(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: false,
            permission_path: MetadataRegistryRecord::permission_path_for(
                &realm_id,
                group_id,
                &path,
                document_id,
            ),
            placement: PlacementRef::NIL,
            holder_node_ids: Vec::new(),
            created_at_ms: 1,
            updated_at_ms: 1,
            establishing_event_id: last_event_id,
            last_event_id,
        }
    }

    fn make_status(document_id: Ulid, event_id: Ulid) -> MaterializationStatusRecord {
        MaterializationStatusRecord {
            document_id,
            event_id,
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            context_digest: None,
            dataset_digest: None,
            state: MaterializationState::Materialized,
            attempts: 1,
            failures: 0,
            last_error: None,
            updated_at_ms: 1,
        }
    }

    #[tokio::test]
    async fn withholds_stale_status() {
        // A registry row replicated ahead of its document event must not export
        // the graph materialized for the previous event.
        let temp_dir = tempdir().unwrap();
        let storage_handle = FjallStorage::open(temp_dir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };

        let document_id = Ulid::generate();
        let materialized_event = Ulid::generate();
        let status = make_status(document_id, materialized_event);
        let (key_space, key, value) = materialization_status_entry(&status).unwrap();
        storage_handle
            .send_effect(aruna_core::effects::Effect::Storage(StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: None,
            }))
            .await;

        let stale = make_record(document_id, Ulid::generate());
        assert!(
            !drive(RecordMaterializedOperation::new(&stale), &context)
                .await
                .unwrap()
        );

        let current = make_record(document_id, materialized_event);
        assert!(
            drive(RecordMaterializedOperation::new(&current), &context)
                .await
                .unwrap()
        );
    }

    mod calls {
        use super::super::*;
        use super::{make_record, make_status};
        use aruna_core::rows::encode;
        use aruna_core::types::Value;

        fn record() -> MetadataRegistryRecord {
            make_record(Ulid::from_parts(2, 2), Ulid::from_parts(3, 3))
        }

        fn stored<T: serde::Serialize>(value: &T) -> Option<Value> {
            Some(encode(value).unwrap())
        }

        fn status(state: MaterializationState) -> MaterializationStatusRecord {
            let record = record();
            let mut status = make_status(record.document_id, record.last_event_id);
            status.state = state;
            status
        }

        fn export_call() -> GetTurn {
            let graph_iri = record().graph_iri;
            let export = Effect::Metadata(MetadataEffect::ExportRoCrate { graph_iri });
            Ok(Next::Call(IoCall::Effect(Box::new(export))))
        }

        fn exported(event: MetadataEvent) -> IoResult {
            IoResult::Event(Box::new(Event::Metadata(event)))
        }

        /// The operation after the registry row was read, with the state read answered.
        fn after_state(lifecycle: Option<Value>, status: Option<Value>) -> GetTurn {
            let record = record();
            let mut operation = GetDocumentOperation::new(record.group_id, record.document_id);
            operation.first().unwrap();
            operation
                .next(IoResult::Values(vec![stored(&record)]))
                .unwrap();
            operation.next(IoResult::Values(vec![lifecycle, status]))
        }

        #[test]
        fn exports_document() {
            let record = record();
            let mut operation = GetDocumentOperation::new(record.group_id, record.document_id);
            let key = metadata_registry_key(record.group_id, record.document_id);
            assert_eq!(
                operation.first(),
                Ok(Next::Call(read_row(METADATA_INDEX_KEYSPACE, key)))
            );
            let reads = vec![
                (
                    GRAPH_LIFECYCLE_KEYSPACE.to_string(),
                    graph_lifecycle_key(&record.graph_iri),
                ),
                (
                    MATERIALIZATION_STATUS_KEYSPACE.to_string(),
                    materialization_status_key(record.document_id),
                ),
            ];
            assert_eq!(
                operation.next(IoResult::Values(vec![stored(&record)])),
                Ok(Next::Call(IoCall::Read(reads)))
            );
            let materialized = stored(&status(MaterializationState::Materialized));
            assert_eq!(
                operation.next(IoResult::Values(vec![None, materialized])),
                export_call()
            );
            let result = MetadataEvent::RoCrateExportResult {
                graph_iri: record.graph_iri.clone(),
                jsonld: "{}".to_string(),
            };
            assert_eq!(
                operation.next(exported(result)),
                Ok(Next::Done(MetadataDocumentView {
                    record,
                    jsonld: "{}".to_string(),
                }))
            );
        }

        #[test]
        fn deleted_not_found() {
            let record = record();
            let mut missing = GetDocumentOperation::new(record.group_id, record.document_id);
            missing.first().unwrap();
            assert_eq!(
                missing.next(IoResult::Values(vec![None])),
                Err(GetDocumentError::DocumentNotFound)
            );
            let deleted = GraphLifecycleRecord::deleted(
                record.graph_iri.clone(),
                record.realm_id,
                record.group_id,
                record.document_id,
                1,
            );
            assert_eq!(
                after_state(stored(&deleted), None),
                Err(GetDocumentError::DocumentNotFound)
            );
            assert!(GetDocumentOperation::expected_error(
                &GetDocumentError::DocumentNotFound
            ));
        }

        #[test]
        fn pending_graph_withheld() {
            // A status for the row's last event that is not materialized hides the graph;
            // no status at all exports it.
            let pending = stored(&status(MaterializationState::Pending));
            assert_eq!(
                after_state(None, pending),
                Err(GetDocumentError::MetadataError(
                    MetadataError::GraphNotFound
                ))
            );
            assert_eq!(after_state(None, None), export_call());
        }

        #[test]
        fn export_errors_fail() {
            let error = MetadataEvent::Error {
                graph_iri: None,
                error: MetadataError::GraphNotFound,
            };
            let record = record();
            let mut operation = GetDocumentOperation::new(record.group_id, record.document_id);
            operation.first().unwrap();
            operation
                .next(IoResult::Values(vec![stored(&record)]))
                .unwrap();
            operation.next(IoResult::Values(vec![None, None])).unwrap();
            assert_eq!(
                operation.next(exported(error)),
                Err(GetDocumentError::MetadataError(
                    MetadataError::GraphNotFound
                ))
            );
            assert!(matches!(
                after_state(Some(vec![0xff].into()), None),
                Err(GetDocumentError::Io(IoError::Codec(_)))
            ));
        }
    }
}
