use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::MetadataDocument;
use aruna_core::types::{GroupId, Key};
use smallvec::smallvec;
use thiserror::Error;

use crate::automerge::repository::iter_metadata_effect;
use aruna_core::effects::Effect;
use aruna_core::types::Effects;

const LIST_METADATA_PAGE_SIZE: usize = 128;

#[derive(Debug, PartialEq)]
pub struct ListMetadataDocumentsOperation {
    group_id: GroupId,
    documents: Vec<MetadataDocument>,
    state: ListMetadataDocumentsState,
    output: Option<Result<Vec<MetadataDocument>, ListMetadataDocumentsError>>,
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
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
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

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(ListMetadataDocumentsError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for ListMetadataDocumentsOperation {
    type Output = Vec<MetadataDocument>;
    type Error = ListMetadataDocumentsError;

    fn start(&mut self) -> Effects {
        self.state = ListMetadataDocumentsState::ListDocuments;
        smallvec![self.iter_effect(None)]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ListMetadataDocumentsState::ListDocuments => match event {
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) => {
                    let page = values
                        .into_iter()
                        .map(|(_, value)| {
                            MetadataDocument::from_bytes(&value)
                                .map_err(ListMetadataDocumentsError::from)
                        })
                        .collect::<Result<Vec<_>, _>>();

                    match page {
                        Ok(mut page) => {
                            self.documents.append(&mut page);
                            if let Some(cursor) = next_start_after {
                                smallvec![self.iter_effect(Some(cursor))]
                            } else {
                                self.state = ListMetadataDocumentsState::Finish;
                                self.output = Some(Ok(std::mem::take(&mut self.documents)));
                                smallvec![]
                            }
                        }
                        Err(error) => self.fail(error),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage iteration result", format!("{other:?}")),
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

impl ListMetadataDocumentsOperation {
    fn iter_effect(&self, start_after: Option<Key>) -> Effect {
        iter_metadata_effect(self.group_id, None, start_after, LIST_METADATA_PAGE_SIZE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::handle::Handle;
    use aruna_core::structs::{Actor, MetadataDocument, RealmId};
    use aruna_storage::FjallStorage;
    use tempfile::tempdir;
    use ulid::Ulid;

    use crate::automerge::repository::write_effect;
    use crate::driver::{DriverContext, drive};

    #[tokio::test]
    async fn lists_documents_across_multiple_pages() {
        let temp = tempdir().unwrap();
        let storage_handle = FjallStorage::open(temp.path().to_str().unwrap()).unwrap();
        let group_id = Ulid::new();
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[5u8; 32]).public(),
            user_id: Ulid::new(),
            realm_id: RealmId([4u8; 32]),
        };

        for idx in 0..(LIST_METADATA_PAGE_SIZE + 5) {
            let mut document = MetadataDocument::new(group_id, Ulid::new(), String::new());
            document.triples.insert(format!(
                "<http://example.org/{idx}> <http://schema.org/name> \"doc-{idx}\""
            ));
            let bytes = document.to_bytes(&actor).unwrap();
            let variant = aruna_core::automerge::AutomergeDocumentVariant::Metadata {
                group_id,
                document_id: document.document_id,
            };
            let event = storage_handle
                .send_effect(write_effect(&variant, bytes, None))
                .await;
            assert!(matches!(
                event,
                aruna_core::events::Event::Storage(StorageEvent::WriteResult { .. })
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
        assert_eq!(result.len(), LIST_METADATA_PAGE_SIZE + 5);
    }

    #[tokio::test]
    async fn storage_iter_zero_limit_returns_empty_page() {
        let temp = tempdir().unwrap();
        let storage_handle = FjallStorage::open(temp.path().to_str().unwrap()).unwrap();
        let event = storage_handle
            .send_effect(Effect::Storage(StorageEffect::Iter {
                key_space: "test".to_string(),
                prefix: None,
                start_after: None,
                limit: 0,
                txn_id: None,
            }))
            .await;

        assert_eq!(
            event,
            Event::Storage(StorageEvent::IterResult {
                values: Vec::new(),
                next_start_after: None,
            })
        );
    }
}
