use std::collections::HashSet;

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::METADATA_GRAPH_LIFECYCLE_KEYSPACE;
use aruna_core::metadata::MetadataGraphLifecycleRecord;
use aruna_core::operation::Operation;
use aruna_core::structs::MetadataRegistryRecord;
use aruna_core::types::{Effects, GroupId, Key};
use smallvec::smallvec;
use thiserror::Error;

use crate::metadata::repository::{
    LIST_METADATA_PAGE_SIZE, StorageReadError, iter_registry_effect, parse_registry_iter,
};

#[derive(Debug, PartialEq)]
pub struct ListMetadataDocumentsOperation {
    group_id: GroupId,
    documents: Vec<MetadataRegistryRecord>,
    deleted_graph_iris: HashSet<String>,
    state: ListMetadataDocumentsState,
    output: Option<Result<Vec<MetadataRegistryRecord>, ListMetadataDocumentsError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum ListMetadataDocumentsState {
    Init,
    ListDeleted,
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
            deleted_graph_iris: HashSet::new(),
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

    fn lifecycle_iter_effect(start_after: Option<Key>) -> Effect {
        Effect::Storage(StorageEffect::Iter {
            key_space: METADATA_GRAPH_LIFECYCLE_KEYSPACE.to_string(),
            prefix: None,
            start: start_after.map(IterStart::After),
            limit: LIST_METADATA_PAGE_SIZE,
            txn_id: None,
        })
    }
}

impl Operation for ListMetadataDocumentsOperation {
    type Output = Vec<MetadataRegistryRecord>;
    type Error = ListMetadataDocumentsError;

    fn start(&mut self) -> Effects {
        self.state = ListMetadataDocumentsState::ListDeleted;
        smallvec![Self::lifecycle_iter_effect(None)]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ListMetadataDocumentsState::ListDeleted => match event {
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) => {
                    for (_, value) in values {
                        match postcard::from_bytes::<MetadataGraphLifecycleRecord>(&value) {
                            Ok(lifecycle) => {
                                if lifecycle.is_deleted() {
                                    self.deleted_graph_iris.insert(lifecycle.graph_iri);
                                }
                            }
                            Err(error) => {
                                return self
                                    .fail(aruna_core::errors::ConversionError::from(error).into());
                            }
                        }
                    }
                    if next_start_after.is_some() {
                        return smallvec![Self::lifecycle_iter_effect(next_start_after)];
                    }
                    self.state = ListMetadataDocumentsState::ListDocuments;
                    smallvec![self.iter_effect(None)]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self
                    .unexpected_event("metadata graph lifecycle iter result", format!("{other:?}")),
            },
            ListMetadataDocumentsState::ListDocuments => match parse_registry_iter(event) {
                Ok((page, next_start_after)) => {
                    self.documents.extend(
                        page.into_iter()
                            .filter(|record| !self.deleted_graph_iris.contains(&record.graph_iri)),
                    );
                    if next_start_after.is_some() {
                        return smallvec![self.iter_effect(next_start_after)];
                    }
                    self.state = ListMetadataDocumentsState::Finish;
                    self.output = Some(Ok(std::mem::take(&mut self.documents)));
                    smallvec![]
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
    use aruna_core::keyspaces::METADATA_INDEX_KEYSPACE;
    use aruna_core::metadata::MetadataGraphLifecycleRecord;
    use aruna_core::structs::{MetadataRegistryRecord, PlacementRef, RealmId};
    use aruna_storage::FjallStorage;
    use byteview::ByteView;
    use tempfile::tempdir;
    use ulid::Ulid;

    use crate::driver::{DriverContext, drive};
    use crate::metadata::repository::{write_graph_lifecycle_effect, write_registry_effect};

    #[tokio::test]
    async fn lists_documents_across_multiple_pages() {
        let temp = tempdir().unwrap();
        let storage_handle = FjallStorage::open(temp.path().to_str().unwrap()).unwrap();
        let group_id = Ulid::r#gen();

        for idx in 0..(crate::metadata::repository::LIST_METADATA_PAGE_SIZE + 5) {
            let document_id = Ulid::r#gen();
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
                placement: PlacementRef::NIL,
                holder_node_ids: Vec::new(),
                created_at_ms: now,
                updated_at_ms: now,
                last_event_id: Ulid::nil(),
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
        let group_id = Ulid::r#gen();
        let active_id = Ulid::r#gen();
        let deleted_id = Ulid::r#gen();

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

    #[test]
    fn filters_deleted_documents_without_per_record_reads() {
        let realm_id = RealmId([6u8; 32]);
        let group_id = Ulid::r#gen();
        let active = metadata_record(realm_id, group_id, Ulid::r#gen(), "docs/active");
        let deleted = metadata_record(realm_id, group_id, Ulid::r#gen(), "docs/deleted");
        let lifecycle = MetadataGraphLifecycleRecord::deleted(
            deleted.graph_iri.clone(),
            realm_id,
            group_id,
            deleted.document_id,
            1,
        );

        let mut operation = ListMetadataDocumentsOperation::new(group_id);
        let effects = operation.start();
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Iter {
                key_space,
                prefix: None,
                start: None,
                ..
            })] if key_space == METADATA_GRAPH_LIFECYCLE_KEYSPACE
        ));

        let effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![(
                ByteView::from(vec![1u8]),
                ByteView::from(postcard::to_allocvec(&lifecycle).unwrap()),
            )],
            next_start_after: None,
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Iter { key_space, .. })]
                if key_space == METADATA_INDEX_KEYSPACE
        ));

        let effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![
                (
                    ByteView::from(vec![2u8]),
                    ByteView::from(postcard::to_allocvec(&active).unwrap()),
                ),
                (
                    ByteView::from(vec![3u8]),
                    ByteView::from(postcard::to_allocvec(&deleted).unwrap()),
                ),
            ],
            next_start_after: None,
        }));
        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert_eq!(operation.finalize().unwrap(), vec![active]);
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
            placement: PlacementRef::NIL,
            holder_node_ids: Vec::new(),
            created_at_ms: 0,
            updated_at_ms: 0,
            last_event_id: Ulid::nil(),
        }
    }
}
