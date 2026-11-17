//! Lists a group's metadata documents and leaves out graphs that already carry a tombstone.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashSet;

use aruna_core::calls::{IoError, IoResult, Next};
use aruna_core::keyspaces::{GRAPH_LIFECYCLE_KEYSPACE, METADATA_INDEX_KEYSPACE};
use aruna_core::metadata::GraphLifecycleRecord;
use aruna_core::operation::Operation;
use aruna_core::rows::{PrefixPages, decode};
use aruna_core::storage_entries::metadata_registry_prefix;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::{GroupId, Key};

use crate::metadata::repository::LIST_METADATA_SIZE;

type ListTurn = Result<Next<Vec<MetadataRegistryRecord>>, IoError>;

/// Pages through every graph lifecycle to collect the deleted graphs, then through the group's
/// registry rows, keeping the documents whose graph is not deleted.
#[derive(Debug, PartialEq)]
pub struct ListDocumentsOperation {
    group_id: GroupId,
    lifecycles: PrefixPages,
    /// The registry pages once every lifecycle was read.
    registry: Option<PrefixPages>,
    documents: Vec<MetadataRegistryRecord>,
    deleted_graph_iris: HashSet<String>,
}

impl ListDocumentsOperation {
    pub fn new(group_id: GroupId) -> Self {
        Self {
            group_id,
            lifecycles: PrefixPages::new(
                GRAPH_LIFECYCLE_KEYSPACE,
                Key::from(Vec::new()),
                LIST_METADATA_SIZE,
            ),
            registry: None,
            documents: Vec::new(),
            deleted_graph_iris: HashSet::new(),
        }
    }

    fn lifecycle_page(&mut self, result: IoResult) -> ListTurn {
        for (_, value) in self.lifecycles.take(result)? {
            let lifecycle: GraphLifecycleRecord = decode(&value)?;
            if lifecycle.is_deleted() {
                self.deleted_graph_iris.insert(lifecycle.graph_iri);
            }
        }
        if !self.lifecycles.done() {
            return Ok(Next::Call(self.lifecycles.call()));
        }
        let prefix = metadata_registry_prefix(self.group_id);
        let registry = PrefixPages::new(METADATA_INDEX_KEYSPACE, prefix, LIST_METADATA_SIZE);
        let call = registry.call();
        self.registry = Some(registry);
        Ok(Next::Call(call))
    }
}

impl Operation for ListDocumentsOperation {
    type Output = Vec<MetadataRegistryRecord>;
    type Error = IoError;

    fn first(&mut self) -> ListTurn {
        Ok(Next::Call(self.lifecycles.call()))
    }

    fn next(&mut self, result: IoResult) -> ListTurn {
        let Some(registry) = self.registry.as_mut() else {
            return self.lifecycle_page(result);
        };
        for (_, value) in registry.take(result)? {
            let record: MetadataRegistryRecord = decode(&value)?;
            if !self.deleted_graph_iris.contains(&record.graph_iri) {
                self.documents.push(record);
            }
        }
        if !registry.done() {
            return Ok(Next::Call(registry.call()));
        }
        Ok(Next::Done(std::mem::take(&mut self.documents)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use aruna_core::calls::IoCall;
    use aruna_core::handle::Handle;
    use aruna_core::rows::encode;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::record::PlacementRef;
    use aruna_core::types::Value;
    use aruna_storage::FjallStorage;
    use tempfile::tempdir;
    use ulid::Ulid;

    use crate::driver::{DriverContext, drive};
    use crate::metadata::repository::{write_graph_lifecycle, write_registry_effect};

    #[tokio::test]
    async fn lists_across_multiple() {
        let temp = tempdir().unwrap();
        let storage_handle = FjallStorage::open(temp.path().to_str().unwrap()).unwrap();
        let group_id = Ulid::generate();

        for idx in 0..(crate::metadata::repository::LIST_METADATA_SIZE + 5) {
            let document_id = Ulid::generate();
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
                establishing_event_id: Ulid::nil(),
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
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };

        let result = drive(ListDocumentsOperation::new(group_id), &context)
            .await
            .unwrap();
        assert_eq!(
            result.len(),
            crate::metadata::repository::LIST_METADATA_SIZE + 5
        );
    }

    #[tokio::test]
    async fn omits_deleted_lifecycle() {
        let temp = tempdir().unwrap();
        let storage_handle = FjallStorage::open(temp.path().to_str().unwrap()).unwrap();
        let realm_id = RealmId([5u8; 32]);
        let group_id = Ulid::generate();
        let active_id = Ulid::generate();
        let deleted_id = Ulid::generate();

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

        let lifecycle = GraphLifecycleRecord::deleted(
            deleted.graph_iri.clone(),
            realm_id,
            group_id,
            deleted_id,
            1,
        );
        let event = storage_handle
            .send_effect(write_graph_lifecycle(&lifecycle, None).unwrap())
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
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };

        let result = drive(ListDocumentsOperation::new(group_id), &context)
            .await
            .unwrap();
        assert_eq!(result, vec![active]);
    }

    fn page(rows: Vec<(Key, Value)>) -> IoResult {
        IoResult::Page { rows, next: None }
    }

    fn row<T: serde::Serialize>(seed: u8, value: &T) -> (Key, Value) {
        (Key::from(vec![seed]), encode(value).unwrap())
    }

    #[test]
    fn filters_deleted_records() {
        let realm_id = RealmId([6u8; 32]);
        let group_id = Ulid::generate();
        let active = metadata_record(realm_id, group_id, Ulid::generate(), "docs/active");
        let deleted = metadata_record(realm_id, group_id, Ulid::generate(), "docs/deleted");
        let lifecycle = GraphLifecycleRecord::deleted(
            deleted.graph_iri.clone(),
            realm_id,
            group_id,
            deleted.document_id,
            1,
        );

        let mut operation = ListDocumentsOperation::new(group_id);
        let lifecycles = PrefixPages::new(
            GRAPH_LIFECYCLE_KEYSPACE,
            Key::from(Vec::new()),
            LIST_METADATA_SIZE,
        );
        assert_eq!(operation.first(), Ok(Next::Call(lifecycles.call())));
        let registry = PrefixPages::new(
            METADATA_INDEX_KEYSPACE,
            metadata_registry_prefix(group_id),
            LIST_METADATA_SIZE,
        );
        assert_eq!(
            operation.next(page(vec![row(1, &lifecycle)])),
            Ok(Next::Call(registry.call()))
        );
        assert_eq!(
            operation.next(page(vec![row(2, &active), row(3, &deleted)])),
            Ok(Next::Done(vec![active]))
        );
    }

    #[test]
    fn follows_next_pages() {
        let group_id = Ulid::from_parts(1, 1);
        let mut operation = ListDocumentsOperation::new(group_id);
        operation.first().unwrap();
        let more = IoResult::Page {
            rows: Vec::new(),
            next: Some(Key::from(vec![9])),
        };
        let Ok(Next::Call(IoCall::Page { space, after, .. })) = operation.next(more) else {
            panic!("expected the next lifecycle page");
        };
        assert_eq!(space, GRAPH_LIFECYCLE_KEYSPACE);
        assert_eq!(after, Some(Key::from(vec![9])));
        assert!(matches!(
            operation.next(page(vec![(Key::from(vec![1]), vec![0xff].into())])),
            Err(IoError::Codec(_))
        ));
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
            establishing_event_id: Ulid::nil(),
            last_event_id: Ulid::nil(),
        }
    }
}
