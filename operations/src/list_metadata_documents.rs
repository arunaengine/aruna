use aruna_core::events::Event;
use aruna_core::operation::Operation;
use aruna_core::structs::MetadataRegistryRecord;
use aruna_core::types::{Effects, GroupId, Key};
use smallvec::smallvec;
use thiserror::Error;

use crate::metadata::repository::{
    StorageReadError, iter_registry_effect, parse_registry_iter,
};

#[derive(Debug, PartialEq)]
pub struct ListMetadataDocumentsOperation {
    group_id: GroupId,
    documents: Vec<MetadataRegistryRecord>,
    state: ListMetadataDocumentsState,
    output: Option<Result<Vec<MetadataRegistryRecord>, ListMetadataDocumentsError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum ListMetadataDocumentsState {
    Init,
    ListDocuments,
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
            state: ListMetadataDocumentsState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: ListMetadataDocumentsError) -> Effects {
        self.state = ListMetadataDocumentsState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn iter_effect(&self, start_after: Option<Key>) -> aruna_core::effects::Effect {
        iter_registry_effect(self.group_id, start_after, None)
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
                    self.documents.append(&mut page);
                    if let Some(cursor) = next_start_after {
                        smallvec![self.iter_effect(Some(cursor))]
                    } else {
                        self.state = ListMetadataDocumentsState::Finish;
                        self.output = Some(Ok(std::mem::take(&mut self.documents)));
                        smallvec![]
                    }
                }
                Err(StorageReadError::Storage(error)) => self.fail(error.into()),
                Err(StorageReadError::Conversion(error)) => self.fail(error.into()),
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
    use aruna_core::structs::{MetadataRegistryRecord, RealmId};
    use aruna_storage::FjallStorage;
    use tempfile::tempdir;
    use ulid::Ulid;

    use crate::driver::{DriverContext, drive};
    use crate::metadata::repository::write_registry_effect;

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
                graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
                public: idx % 2 == 0,
                permission_path: MetadataRegistryRecord::permission_path_for(
                    &RealmId([4u8; 32]),
                    group_id,
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
                aruna_core::events::Event::Storage(aruna_core::events::StorageEvent::WriteResult {
                    ..
                })
            ));
        }

        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            automerge_handle: None,
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
}
