use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::METADATA_GRAPH_LIFECYCLE_KEYSPACE;
use aruna_core::metadata::MetadataGraphLifecycleRecord;
use aruna_core::operation::Operation;
use aruna_core::storage_entries::metadata_graph_lifecycle_key;
use aruna_core::structs::MetadataRegistryRecord;
use aruna_core::types::{Effects, GroupId, Key};
use smallvec::smallvec;
use thiserror::Error;

use crate::metadata::repository::{StorageReadError, iter_registry_effect, parse_registry_iter};

#[derive(Debug, PartialEq)]
pub struct ListMetadataDocumentsOperation {
    group_id: GroupId,
    documents: Vec<MetadataRegistryRecord>,
    pending_documents: Vec<MetadataRegistryRecord>,
    pending_document: Option<MetadataRegistryRecord>,
    next_start_after: Option<Key>,
    state: ListMetadataDocumentsState,
    output: Option<Result<Vec<MetadataRegistryRecord>, ListMetadataDocumentsError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum ListMetadataDocumentsState {
    Init,
    ListDocuments,
    CheckLifecycle,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ListMetadataDocumentsError {
    #[error(transparent)]
    StorageError(#[from] aruna_core::errors::StorageError),
    #[error(transparent)]
    ConversionError(#[from] aruna_core::errors::ConversionError),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl ListMetadataDocumentsOperation {
    pub fn new(group_id: GroupId) -> Self {
        Self {
            group_id,
            documents: Vec::new(),
            pending_documents: Vec::new(),
            pending_document: None,
            next_start_after: None,
            state: ListMetadataDocumentsState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: ListMetadataDocumentsError) -> Effects {
        self.state = ListMetadataDocumentsState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(ListMetadataDocumentsError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }

    fn iter_effect(&self, start_after: Option<Key>) -> Effect {
        iter_registry_effect(self.group_id, start_after, None)
    }

    fn next_lifecycle_check(&mut self) -> Effects {
        if let Some(record) = self.pending_documents.pop() {
            self.state = ListMetadataDocumentsState::CheckLifecycle;
            let graph_iri = record.graph_iri.clone();
            self.pending_document = Some(record);
            return smallvec![Effect::Storage(StorageEffect::Read {
                key_space: METADATA_GRAPH_LIFECYCLE_KEYSPACE.to_string(),
                key: metadata_graph_lifecycle_key(&graph_iri),
                txn_id: None,
            })];
        }

        if let Some(cursor) = self.next_start_after.take() {
            self.state = ListMetadataDocumentsState::ListDocuments;
            return smallvec![self.iter_effect(Some(cursor))];
        }

        self.state = ListMetadataDocumentsState::Finish;
        self.output = Some(Ok(std::mem::take(&mut self.documents)));
        smallvec![]
    }
}

impl Operation for ListMetadataDocumentsOperation {
    type Output = Vec<MetadataRegistryRecord>;
    type Error = ListMetadataDocumentsError;

    fn start(&mut self) -> Effects {
        self.state = ListMetadataDocumentsState::ListDocuments;
        smallvec![self.iter_effect(None)]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ListMetadataDocumentsState::ListDocuments => match parse_registry_iter(event) {
                Ok((mut page, next_start_after)) => {
                    page.reverse();
                    self.pending_documents = page;
                    self.next_start_after = next_start_after;
                    self.next_lifecycle_check()
                }
                Err(StorageReadError::Storage(error)) => self.fail(error.into()),
                Err(StorageReadError::Conversion(error)) => self.fail(error.into()),
            },
            ListMetadataDocumentsState::CheckLifecycle => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    let Some(record) = self.pending_document.take() else {
                        return self.unexpected_event(
                            "metadata graph lifecycle read result",
                            "missing pending document".to_string(),
                        );
                    };
                    let deleted = match value {
                        Some(value) => {
                            match postcard::from_bytes::<MetadataGraphLifecycleRecord>(&value) {
                                Ok(lifecycle) => lifecycle.is_deleted(),
                                Err(error) => {
                                    return self.fail(
                                        aruna_core::errors::ConversionError::from(error).into(),
                                    );
                                }
                            }
                        }
                        None => false,
                    };
                    if !deleted {
                        self.documents.push(record);
                    }
                    self.next_lifecycle_check()
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self
                    .unexpected_event("metadata graph lifecycle read result", format!("{other:?}")),
            },
            ListMetadataDocumentsState::Finish
            | ListMetadataDocumentsState::Error
            | ListMetadataDocumentsState::Init => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ListMetadataDocumentsState::Finish | ListMetadataDocumentsState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Ok(Vec::new()))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use aruna_core::handle::Handle;
    use aruna_core::metadata::MetadataGraphLifecycleRecord;
    use aruna_core::structs::{MetadataRegistryRecord, RealmId};
    use aruna_storage::FjallStorage;
    use tempfile::tempdir;
    use ulid::Ulid;

    use crate::driver::{DriverContext, drive};
    use crate::metadata::repository::{write_graph_lifecycle_effect, write_registry_effect};

    #[tokio::test]
    async fn lists_documents_across_multiple_pages() {
        let temp = tempdir().unwrap();
        let storage_handle = FjallStorage::open(temp.path().to_str().unwrap()).unwrap();
        let group_id = Ulid::new();

        for idx in 0..(crate::metadata::repository::LIST_METADATA_PAGE_SIZE + 5) {
            let document_id = Ulid::new();
            let now = idx as u64;
            let record = MetadataRegistryRecord {
                realm_id: RealmId([4u8; 32]),
                group_id,
                document_id,
                document_path: format!("docs/{idx}"),
                graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
                public: idx % 2 == 0,
                permission_path: MetadataRegistryRecord::permission_path_for(
                    &RealmId([4u8; 32]),
                    group_id,
                    &format!("docs/{idx}"),
                    document_id,
                ),
                holder_node_ids: Vec::new(),
                created_at_ms: now,
                updated_at_ms: now,
            };
            let event = storage_handle
                .send_effect(write_registry_effect(&record, None).unwrap())
                .await;
            assert!(matches!(
                event,
                aruna_core::events::Event::Storage(
                    aruna_core::events::StorageEvent::WriteResult { .. }
                )
            ));
        }

        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        let result = drive(ListMetadataDocumentsOperation::new(group_id), &context)
            .await
            .unwrap();
        assert_eq!(
            result.len(),
            crate::metadata::repository::LIST_METADATA_PAGE_SIZE + 5
        );
    }

    #[tokio::test]
    async fn omits_deleted_lifecycle_records() {
        let temp = tempdir().unwrap();
        let storage_handle = FjallStorage::open(temp.path().to_str().unwrap()).unwrap();
        let realm_id = RealmId([5u8; 32]);
        let group_id = Ulid::new();
        let active_id = Ulid::new();
        let deleted_id = Ulid::new();

        let active = metadata_record(realm_id, group_id, active_id, "docs/active");
        let deleted = metadata_record(realm_id, group_id, deleted_id, "docs/deleted");
        for record in [&active, &deleted] {
            let event = storage_handle
                .send_effect(write_registry_effect(record, None).unwrap())
                .await;
            assert!(matches!(
                event,
                aruna_core::events::Event::Storage(
                    aruna_core::events::StorageEvent::WriteResult { .. }
                )
            ));
        }

        let lifecycle = MetadataGraphLifecycleRecord::deleted(
            deleted.graph_iri.clone(),
            realm_id,
            group_id,
            deleted_id,
            1,
        );
        let event = storage_handle
            .send_effect(write_graph_lifecycle_effect(&lifecycle, None).unwrap())
            .await;
        assert!(matches!(
            event,
            aruna_core::events::Event::Storage(
                aruna_core::events::StorageEvent::WriteResult { .. }
            )
        ));

        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        let result = drive(ListMetadataDocumentsOperation::new(group_id), &context)
            .await
            .unwrap();
        assert_eq!(result, vec![active]);
    }

    fn metadata_record(
        realm_id: RealmId,
        group_id: Ulid,
        document_id: Ulid,
        path: &str,
    ) -> MetadataRegistryRecord {
        MetadataRegistryRecord {
            realm_id,
            group_id,
            document_id,
            document_path: path.to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: true,
            permission_path: MetadataRegistryRecord::permission_path_for(
                &realm_id,
                group_id,
                path,
                document_id,
            ),
            holder_node_ids: Vec::new(),
            created_at_ms: 0,
            updated_at_ms: 0,
        }
    }
}
