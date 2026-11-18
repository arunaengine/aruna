//! Reads a document's registry record and whether its graph matches that record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoResult, Next};
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::{DOCUMENT_INDEX_KEYSPACE, MATERIALIZATION_STATUS_KEYSPACE};
use aruna_core::metadata::{MaterializationState, MaterializationStatusRecord};
use aruna_core::operation::Operation;
use aruna_core::rows::{read_row, value};
use aruna_core::storage_entries::{materialization_status_key, metadata_document_key};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use serde::de::DeserializeOwned;
use ulid::Ulid;

use crate::storage_read::StorageReadError;

fn decoded<T: DeserializeOwned>(result: IoResult) -> Result<Option<T>, StorageReadError> {
    value(result)?
        .map(|bytes| postcard::from_bytes(&bytes).map_err(ConversionError::from))
        .transpose()
        .map_err(StorageReadError::Conversion)
}

/// Reads the registry record of one document by its id; a missing record is `None`.
#[derive(Debug, PartialEq)]
pub struct DocumentRecordOperation {
    document_id: Ulid,
}

impl DocumentRecordOperation {
    pub fn new(document_id: Ulid) -> Self {
        Self { document_id }
    }
}

impl Operation for DocumentRecordOperation {
    type Output = Option<MetadataRegistryRecord>;
    type Error = StorageReadError;

    fn first(&mut self) -> Result<Next<Self::Output>, StorageReadError> {
        let key = metadata_document_key(self.document_id);
        Ok(Next::Call(read_row(DOCUMENT_INDEX_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, StorageReadError> {
        Ok(Next::Done(decoded(result)?))
    }
}

/// Whether the local graph matches a registry record. Registry rows can replicate ahead of the
/// document event, so only a status recorded for exactly the record's cursor proves it.
#[derive(Debug, PartialEq)]
pub struct RecordMaterializedOperation {
    document_id: Ulid,
    last_event_id: Ulid,
}

impl RecordMaterializedOperation {
    pub fn new(record: &MetadataRegistryRecord) -> Self {
        Self {
            document_id: record.document_id,
            last_event_id: record.last_event_id,
        }
    }
}

impl Operation for RecordMaterializedOperation {
    type Output = bool;
    type Error = StorageReadError;

    fn first(&mut self) -> Result<Next<bool>, StorageReadError> {
        let key = materialization_status_key(self.document_id);
        Ok(Next::Call(read_row(MATERIALIZATION_STATUS_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, StorageReadError> {
        let Some(status) = decoded::<MaterializationStatusRecord>(result)? else {
            return Ok(Next::Done(true));
        };
        Ok(Next::Done(
            status.event_id == self.last_event_id
                && matches!(status.state, MaterializationState::Materialized),
        ))
    }
}

#[cfg(test)]
mod call_tests {
    use super::*;
    use aruna_core::calls::{IoCall, IoError};
    use aruna_core::structs::identity::realm::RealmId;

    fn record() -> MetadataRegistryRecord {
        let document_id = Ulid::from_parts(3, 3);
        MetadataRegistryRecord {
            realm_id: RealmId::from_bytes([1; 32]),
            group_id: Ulid::from_parts(2, 2),
            document_id,
            document_path: "datasets/read".to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: false,
            permission_path: String::new(),
            placement: Default::default(),
            holder_node_ids: Vec::new(),
            created_at_ms: 1,
            updated_at_ms: 1,
            establishing_event_id: Ulid::from_parts(4, 4),
            last_event_id: Ulid::from_parts(5, 5),
        }
    }

    fn status(event_id: Ulid, state: MaterializationState) -> Option<aruna_core::types::Value> {
        let status = MaterializationStatusRecord {
            document_id: record().document_id,
            event_id,
            graph_iri: record().graph_iri,
            context_digest: None,
            dataset_digest: None,
            state,
            attempts: 0,
            failures: 0,
            last_error: None,
            updated_at_ms: 1,
        };
        Some(postcard::to_allocvec(&status).unwrap().into())
    }

    #[test]
    fn record_read_decodes() {
        let stored = record();
        let mut read = DocumentRecordOperation::new(stored.document_id);
        assert_eq!(
            read.first(),
            Ok(Next::Call(IoCall::Read(vec![(
                DOCUMENT_INDEX_KEYSPACE.to_string(),
                metadata_document_key(stored.document_id),
            )])))
        );
        let row = Some(postcard::to_allocvec(&stored).unwrap().into());
        assert_eq!(
            read.next(IoResult::Values(vec![row])),
            Ok(Next::Done(Some(stored)))
        );
        assert_eq!(
            read.next(IoResult::Values(vec![None])),
            Ok(Next::Done(None))
        );
        assert!(matches!(
            read.next(IoResult::Values(vec![Some(vec![0xff].into())])),
            Err(StorageReadError::Conversion(_))
        ));
        assert!(matches!(
            read.next(IoResult::Done),
            Err(StorageReadError::Storage(_))
        ));
        assert_eq!(
            StorageReadError::from(IoError::Aborted),
            StorageReadError::Storage(IoError::Aborted.into())
        );
    }

    #[test]
    fn exact_status_materialized() {
        let current = record();
        let mut read = RecordMaterializedOperation::new(&current);
        assert_eq!(
            read.first(),
            Ok(Next::Call(IoCall::Read(vec![(
                MATERIALIZATION_STATUS_KEYSPACE.to_string(),
                materialization_status_key(current.document_id),
            )])))
        );
        let answer = |row| read.next(IoResult::Values(vec![row]));
        assert_eq!(answer(None), Ok(Next::Done(true)));
        assert_eq!(
            answer(status(
                current.last_event_id,
                MaterializationState::Materialized
            )),
            Ok(Next::Done(true))
        );
        assert_eq!(
            answer(status(
                Ulid::from_parts(4, 4),
                MaterializationState::Materialized
            )),
            Ok(Next::Done(false))
        );
        assert_eq!(
            answer(status(current.last_event_id, MaterializationState::Pending)),
            Ok(Next::Done(false))
        );
    }
}
