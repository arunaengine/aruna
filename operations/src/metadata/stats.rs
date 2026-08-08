use aruna_core::structs::RealmId;

use super::api::MetadataApiError;
use crate::driver::DriverContext;

/// Realm-wide number of live metadata documents, not filtered by what any
/// caller may read.
///
/// The count comes from the cached registry snapshot plus one bounded lifecycle
/// batch, and excludes lifecycle-deleted documents. Returns `None` when the node
/// runs without a metadata subsystem, so an absent count stays distinguishable
/// from zero documents.
///
/// An exact per-caller count would need per-document glob evaluation, because
/// read visibility is glob-granular: a `DENY` can subtract a single document
/// from a group-wide grant. The realm total discloses only document volume,
/// which callers already reach realm auth to see.
pub async fn count_realm_documents(
    context: &DriverContext,
    realm_id: RealmId,
) -> Result<Option<u64>, MetadataApiError> {
    let Some(metadata_handle) = context.metadata_handle.as_ref() else {
        return Ok(None);
    };
    let records = metadata_handle
        .list_cached_registry_records()
        .await
        .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
    let records =
        super::api::filter_live_records(&context.storage_handle, records.as_ref()).await?;
    Ok(Some(
        records
            .iter()
            .filter(|record| record.realm_id == realm_id)
            .count() as u64,
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::metadata::MetadataGraphLifecycleRecord;
    use aruna_core::storage_entries::{
        metadata_graph_lifecycle_write_entry, metadata_registry_write_entries,
    };
    use aruna_core::structs::{MetadataRegistryRecord, PlacementRef};
    use aruna_core::types::GroupId;
    use aruna_storage::{FjallStorage, StorageHandle};
    use byteview::ByteView;
    use tempfile::{TempDir, tempdir};
    use ulid::Ulid;

    use super::*;
    use crate::metadata::MetadataHandle;

    const REALM_SEED: [u8; 32] = [7u8; 32];

    struct Fixture {
        _storage_dir: TempDir,
        _metadata_dir: TempDir,
        storage: StorageHandle,
        context: Arc<DriverContext>,
        realm_id: RealmId,
    }

    fn setup_fixture() -> Fixture {
        let storage_dir = tempdir().expect("temp dir");
        let metadata_dir = tempdir().expect("metadata dir");
        let storage = FjallStorage::open(storage_dir.path().to_str().expect("utf-8 path"))
            .expect("storage opens");
        let node_id = iroh::SecretKey::from_bytes(&[8u8; 32]).public();
        let metadata_handle = MetadataHandle::new(
            metadata_dir.path(),
            node_id,
            storage.clone(),
            None,
            None,
            None,
        )
        .expect("metadata handle opens");
        let context = Arc::new(DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: Some(metadata_handle),
            task_handle: None,
            compute_handle: None,
        });
        Fixture {
            _storage_dir: storage_dir,
            _metadata_dir: metadata_dir,
            storage,
            context,
            realm_id: RealmId::from_bytes(REALM_SEED),
        }
    }

    fn registry_record(
        realm_id: RealmId,
        group_id: GroupId,
        public: bool,
    ) -> MetadataRegistryRecord {
        let document_id = Ulid::generate();
        let path = format!("datasets/{document_id}");
        MetadataRegistryRecord {
            realm_id,
            group_id,
            document_id,
            document_path: path.clone(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public,
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
            establishing_event_id: Ulid::nil(),
            last_event_id: Ulid::nil(),
        }
    }

    async fn write_entries(storage: &StorageHandle, writes: Vec<(String, ByteView, ByteView)>) {
        match storage
            .send_storage_effect(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    async fn write_record(fixture: &Fixture, record: &MetadataRegistryRecord) {
        write_entries(
            &fixture.storage,
            metadata_registry_write_entries(record).expect("registry entries"),
        )
        .await;
    }

    #[tokio::test]
    async fn counts_realm_total() {
        // Private documents and groups the caller holds no role in still count.
        let fixture = setup_fixture();
        let first_group = Ulid::generate();
        let second_group = Ulid::generate();
        for record in [
            registry_record(fixture.realm_id, first_group, false),
            registry_record(fixture.realm_id, first_group, true),
            registry_record(fixture.realm_id, second_group, false),
        ] {
            write_record(&fixture, &record).await;
        }

        assert_eq!(
            count_realm_documents(&fixture.context, fixture.realm_id)
                .await
                .expect("count succeeds"),
            Some(3)
        );
    }

    #[tokio::test]
    async fn skips_foreign_realm() {
        let fixture = setup_fixture();
        let group_id = Ulid::generate();
        write_record(&fixture, &registry_record(fixture.realm_id, group_id, true)).await;
        write_record(
            &fixture,
            &registry_record(RealmId::from_bytes([9u8; 32]), group_id, true),
        )
        .await;

        assert_eq!(
            count_realm_documents(&fixture.context, fixture.realm_id)
                .await
                .expect("count succeeds"),
            Some(1)
        );
    }

    #[tokio::test]
    async fn deleted_not_counted() {
        // A graph lifecycle tombstone hides its document from the count even
        // while the registry row is still present.
        let fixture = setup_fixture();
        let group_id = Ulid::generate();
        let kept = registry_record(fixture.realm_id, group_id, true);
        let deleted = registry_record(fixture.realm_id, group_id, true);
        write_record(&fixture, &kept).await;
        write_record(&fixture, &deleted).await;
        let tombstone = MetadataGraphLifecycleRecord::deleted(
            deleted.graph_iri.clone(),
            fixture.realm_id,
            group_id,
            deleted.document_id,
            2,
        );
        write_entries(
            &fixture.storage,
            vec![metadata_graph_lifecycle_write_entry(&tombstone).expect("lifecycle entry")],
        )
        .await;

        assert_eq!(
            count_realm_documents(&fixture.context, fixture.realm_id)
                .await
                .expect("count succeeds"),
            Some(1)
        );
    }

    #[tokio::test]
    async fn unconfigured_reports_none() {
        // An absent count must stay distinguishable from zero documents.
        let storage_dir = tempdir().expect("temp dir");
        let context = DriverContext {
            storage_handle: FjallStorage::open(storage_dir.path().to_str().expect("utf-8 path"))
                .expect("storage opens"),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        assert_eq!(
            count_realm_documents(&context, RealmId::from_bytes(REALM_SEED))
                .await
                .expect("count succeeds"),
            None
        );
    }
}
