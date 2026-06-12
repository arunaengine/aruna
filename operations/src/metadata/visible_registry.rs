use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use aruna_core::effects::StorageEffect;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::METADATA_GRAPH_LIFECYCLE_KEYSPACE;
use aruna_core::metadata::MetadataGraphLifecycleRecord;
use aruna_core::structs::MetadataRegistryRecord;
use aruna_core::types::{GroupId, Key};
use thiserror::Error;
use ulid::Ulid;

use crate::driver::DriverContext;
use crate::metadata::repository::{
    REGISTRY_FILL_PAGE_SIZE, StorageReadError, iter_all_registry_effect, parse_registry_iter,
};

const VISIBLE_REGISTRY_TTL: Duration = Duration::from_secs(30);

static VISIBLE_REGISTRY_SLOTS: OnceLock<Mutex<HashMap<[u8; 32], Arc<CacheSlot>>>> = OnceLock::new();

#[derive(Debug, Error)]
pub enum VisibleRegistryError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("unexpected event while filling visible registry cache: {0}")]
    UnexpectedEvent(String),
}

#[derive(Default)]
struct CacheSlot {
    state: Mutex<Option<CacheState>>,
    fill: Arc<tokio::sync::Mutex<()>>,
}

struct CacheState {
    records: BTreeMap<(GroupId, Ulid), MetadataRegistryRecord>,
    snapshot: Option<Arc<Vec<MetadataRegistryRecord>>>,
    group_snapshots: HashMap<GroupId, Arc<Vec<MetadataRegistryRecord>>>,
    expires_at: Instant,
}

struct CachedView {
    records: Arc<Vec<MetadataRegistryRecord>>,
    stale: bool,
}

impl CacheSlot {
    // Expired entries are served stale; the caller triggers a background
    // refill instead of blocking the request on a full registry sweep.
    fn snapshot(&self) -> Option<CachedView> {
        let mut state = self.state.lock().unwrap_or_else(|lock| lock.into_inner());
        let entry = state.as_mut()?;
        let records = entry
            .snapshot
            .get_or_insert_with(|| Arc::new(entry.records.values().cloned().collect()))
            .clone();
        Some(CachedView {
            records,
            stale: entry.expires_at <= Instant::now(),
        })
    }

    fn group_snapshot(&self, group_id: GroupId) -> Option<CachedView> {
        let mut state = self.state.lock().unwrap_or_else(|lock| lock.into_inner());
        let entry = state.as_mut()?;
        let records = match entry.group_snapshots.get(&group_id) {
            Some(records) => records.clone(),
            None => {
                let records = Arc::new(
                    entry
                        .records
                        .range((group_id, Ulid::nil())..)
                        .take_while(|((group, _), _)| *group == group_id)
                        .map(|(_, record)| record.clone())
                        .collect::<Vec<_>>(),
                );
                entry.group_snapshots.insert(group_id, records.clone());
                records
            }
        };
        Some(CachedView {
            records,
            stale: entry.expires_at <= Instant::now(),
        })
    }

    fn is_fresh(&self) -> bool {
        let state = self.state.lock().unwrap_or_else(|lock| lock.into_inner());
        state
            .as_ref()
            .is_some_and(|entry| entry.expires_at > Instant::now())
    }

    fn store(&self, records: Vec<MetadataRegistryRecord>) {
        let records: BTreeMap<_, _> = records
            .into_iter()
            .map(|record| ((record.group_id, record.document_id), record))
            .collect();
        let mut state = self.state.lock().unwrap_or_else(|lock| lock.into_inner());
        *state = Some(CacheState {
            records,
            snapshot: None,
            group_snapshots: HashMap::new(),
            expires_at: Instant::now() + VISIBLE_REGISTRY_TTL,
        });
    }

    fn upsert(&self, updates: &[MetadataRegistryRecord]) {
        if updates.is_empty() {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(|lock| lock.into_inner());
        let Some(entry) = state.as_mut() else {
            return;
        };
        for update in updates {
            entry
                .records
                .insert((update.group_id, update.document_id), update.clone());
            entry.group_snapshots.remove(&update.group_id);
        }
        entry.snapshot = None;
    }

    fn remove(&self, group_id: GroupId, document_id: Ulid) {
        let mut state = self.state.lock().unwrap_or_else(|lock| lock.into_inner());
        let Some(entry) = state.as_mut() else {
            return;
        };
        if entry.records.remove(&(group_id, document_id)).is_some() {
            entry.group_snapshots.remove(&group_id);
            entry.snapshot = None;
        }
    }

    fn invalidate(&self) {
        let mut state = self.state.lock().unwrap_or_else(|lock| lock.into_inner());
        *state = None;
    }
}

fn cache_key(context: &DriverContext) -> [u8; 32] {
    match context.net_handle.as_ref() {
        Some(net) => *net.node_id().as_bytes(),
        None => {
            let mut key = [0u8; 32];
            key[..8]
                .copy_from_slice(&(context as *const DriverContext as usize as u64).to_be_bytes());
            key
        }
    }
}

fn slot(context: &DriverContext) -> Arc<CacheSlot> {
    let slots = VISIBLE_REGISTRY_SLOTS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut slots = slots.lock().unwrap_or_else(|lock| lock.into_inner());
    slots.entry(cache_key(context)).or_default().clone()
}

pub async fn list_visible_registry_records(
    context: &DriverContext,
) -> Result<Arc<Vec<MetadataRegistryRecord>>, VisibleRegistryError> {
    cached_records(context, |slot| slot.snapshot()).await
}

pub async fn list_visible_registry_records_for_group(
    context: &DriverContext,
    group_id: GroupId,
) -> Result<Arc<Vec<MetadataRegistryRecord>>, VisibleRegistryError> {
    cached_records(context, move |slot| slot.group_snapshot(group_id)).await
}

async fn cached_records(
    context: &DriverContext,
    view: impl Fn(&CacheSlot) -> Option<CachedView>,
) -> Result<Arc<Vec<MetadataRegistryRecord>>, VisibleRegistryError> {
    let slot = slot(context);
    if let Some(cached) = view(&slot) {
        if cached.stale {
            spawn_background_refill(context.clone(), slot.clone());
        }
        return Ok(cached.records);
    }
    // Cold cache (boot before warmup): the request pays the fill inline.
    let _fill = slot.fill.lock().await;
    if let Some(cached) = view(&slot) {
        return Ok(cached.records);
    }
    let records = fill_from_storage(context).await?;
    slot.store(records);
    Ok(view(&slot)
        .map(|cached| cached.records)
        .unwrap_or_else(|| Arc::new(Vec::new())))
}

fn spawn_background_refill(context: DriverContext, slot: Arc<CacheSlot>) {
    tokio::spawn(async move {
        let Ok(_fill) = slot.fill.clone().try_lock_owned() else {
            return;
        };
        if slot.is_fresh() {
            return;
        }
        match fill_from_storage(&context).await {
            Ok(records) => slot.store(records),
            Err(error) => {
                tracing::warn!(error = %error, "visible registry background refill failed");
            }
        }
    });
}

pub fn upsert_visible_registry_records(
    context: &DriverContext,
    records: &[MetadataRegistryRecord],
) {
    slot(context).upsert(records);
}

pub fn remove_visible_registry_record(
    context: &DriverContext,
    group_id: GroupId,
    document_id: Ulid,
) {
    slot(context).remove(group_id, document_id);
}

pub fn invalidate_visible_registry(context: &DriverContext) {
    slot(context).invalidate();
}

async fn fill_from_storage(
    context: &DriverContext,
) -> Result<Vec<MetadataRegistryRecord>, VisibleRegistryError> {
    let deleted_graph_iris = read_deleted_graph_iris(context).await?;
    let mut records = Vec::new();
    let mut start_after: Option<Key> = None;
    loop {
        let event = context
            .storage_handle
            .send_effect(iter_all_registry_effect(start_after.take(), None))
            .await;
        let (page, next_start_after) = parse_registry_iter(event).map_err(|error| match error {
            StorageReadError::Storage(error) => VisibleRegistryError::Storage(error),
            StorageReadError::Conversion(error) => VisibleRegistryError::Conversion(error),
        })?;
        records.extend(
            page.into_iter()
                .filter(|record| !deleted_graph_iris.contains(&record.graph_iri)),
        );
        match next_start_after {
            Some(next) => start_after = Some(next),
            None => return Ok(records),
        }
    }
}

async fn read_deleted_graph_iris(
    context: &DriverContext,
) -> Result<HashSet<String>, VisibleRegistryError> {
    let mut deleted = HashSet::new();
    let mut start_after: Option<Key> = None;
    loop {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: METADATA_GRAPH_LIFECYCLE_KEYSPACE.to_string(),
                prefix: None,
                start_after: start_after.take(),
                limit: REGISTRY_FILL_PAGE_SIZE,
                txn_id: None,
            })
            .await;
        match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => {
                for (_, value) in values {
                    let lifecycle: MetadataGraphLifecycleRecord =
                        postcard::from_bytes(&value).map_err(ConversionError::from)?;
                    if lifecycle.is_deleted() {
                        deleted.insert(lifecycle.graph_iri);
                    }
                }
                match next_start_after {
                    Some(next) => start_after = Some(next),
                    None => return Ok(deleted),
                }
            }
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            other => {
                return Err(VisibleRegistryError::UnexpectedEvent(format!("{other:?}")));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use aruna_core::structs::RealmId;
    use aruna_storage::FjallStorage;
    use tempfile::tempdir;

    use crate::metadata::repository::{write_graph_lifecycle_effect, write_registry_effect};

    fn record(group_id: GroupId, path: &str) -> MetadataRegistryRecord {
        let realm_id = RealmId([7u8; 32]);
        let document_id = Ulid::new();
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
            created_at_ms: 1,
            updated_at_ms: 1,
            last_event_id: Ulid::nil(),
        }
    }

    async fn write_record(context: &DriverContext, record: &MetadataRegistryRecord) {
        let event = context
            .storage_handle
            .send_effect(write_registry_effect(record, None).unwrap())
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    fn test_context() -> (DriverContext, tempfile::TempDir) {
        let temp = tempdir().unwrap();
        let storage_handle = FjallStorage::open(temp.path().to_str().unwrap()).unwrap();
        (
            DriverContext {
                storage_handle,
                net_handle: None,
                blob_handle: None,
                metadata_handle: None,
                task_handle: None,
            },
            temp,
        )
    }

    #[tokio::test]
    async fn fill_filters_deleted_graphs_and_reuses_snapshot() {
        let (context, _temp) = test_context();
        invalidate_visible_registry(&context);

        let active = record(Ulid::new(), "docs/active");
        let deleted = record(Ulid::new(), "docs/deleted");
        write_record(&context, &active).await;
        write_record(&context, &deleted).await;
        let lifecycle = MetadataGraphLifecycleRecord::deleted(
            deleted.graph_iri.clone(),
            deleted.realm_id,
            deleted.group_id,
            deleted.document_id,
            1,
        );
        let event = context
            .storage_handle
            .send_effect(write_graph_lifecycle_effect(&lifecycle, None).unwrap())
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));

        let first = list_visible_registry_records(&context).await.unwrap();
        assert_eq!(first.as_ref(), &vec![active.clone()]);

        write_record(&context, &record(Ulid::new(), "docs/uncached")).await;
        let second = list_visible_registry_records(&context).await.unwrap();
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[tokio::test]
    async fn warm_cache_applies_upserts_and_removals() {
        let (context, _temp) = test_context();
        invalidate_visible_registry(&context);

        let existing = record(Ulid::new(), "docs/existing");
        write_record(&context, &existing).await;
        let records = list_visible_registry_records(&context).await.unwrap();
        assert_eq!(records.as_ref(), &vec![existing.clone()]);

        let added = record(Ulid::new(), "docs/added");
        let mut updated = existing.clone();
        updated.public = false;
        upsert_visible_registry_records(&context, &[added.clone(), updated.clone()]);

        let records = list_visible_registry_records(&context).await.unwrap();
        assert_eq!(records.len(), 2);
        assert!(records.contains(&added));
        assert!(records.contains(&updated));

        remove_visible_registry_record(&context, added.group_id, added.document_id);
        let records = list_visible_registry_records(&context).await.unwrap();
        assert_eq!(records.as_ref(), &vec![updated]);
    }

    fn force_expire(context: &DriverContext) {
        let slot = slot(context);
        let mut state = slot.state.lock().unwrap_or_else(|lock| lock.into_inner());
        if let Some(entry) = state.as_mut() {
            entry.expires_at = Instant::now() - Duration::from_secs(1);
        }
    }

    #[tokio::test]
    async fn group_listing_is_scoped_and_snapshot_survives_other_group_churn() {
        let (context, _temp) = test_context();
        invalidate_visible_registry(&context);

        let group_a = Ulid::new();
        let group_b = Ulid::new();
        let record_a = record(group_a, "docs/a");
        let record_b = record(group_b, "docs/b");
        write_record(&context, &record_a).await;
        write_record(&context, &record_b).await;

        let listed_a = list_visible_registry_records_for_group(&context, group_a)
            .await
            .unwrap();
        assert_eq!(listed_a.as_ref(), &vec![record_a.clone()]);

        // Churn in group B must not invalidate group A's snapshot.
        upsert_visible_registry_records(&context, &[record(group_b, "docs/b2")]);
        let listed_a_again = list_visible_registry_records_for_group(&context, group_a)
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&listed_a, &listed_a_again));

        let listed_b = list_visible_registry_records_for_group(&context, group_b)
            .await
            .unwrap();
        assert_eq!(listed_b.len(), 2);

        upsert_visible_registry_records(&context, &[record(group_a, "docs/a2")]);
        let listed_a_after = list_visible_registry_records_for_group(&context, group_a)
            .await
            .unwrap();
        assert_eq!(listed_a_after.len(), 2);
    }

    #[tokio::test]
    async fn expired_cache_serves_stale_and_refreshes_in_background() {
        let (context, _temp) = test_context();
        invalidate_visible_registry(&context);

        let group_id = Ulid::new();
        let first = record(group_id, "docs/first");
        write_record(&context, &first).await;
        let listed = list_visible_registry_records_for_group(&context, group_id)
            .await
            .unwrap();
        assert_eq!(listed.as_ref(), &vec![first.clone()]);

        // A record written behind the cache's back becomes visible only via
        // refill; an expired read must serve the stale view without blocking.
        let second = record(group_id, "docs/second");
        write_record(&context, &second).await;
        force_expire(&context);
        let stale = list_visible_registry_records_for_group(&context, group_id)
            .await
            .unwrap();
        assert_eq!(stale.as_ref(), &vec![first.clone()]);

        let mut refreshed = stale;
        for _ in 0..100 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            refreshed = list_visible_registry_records_for_group(&context, group_id)
                .await
                .unwrap();
            if refreshed.len() == 2 {
                break;
            }
        }
        assert_eq!(refreshed.len(), 2);
    }

    #[tokio::test]
    async fn cold_cache_ignores_upserts_until_filled() {
        let (context, _temp) = test_context();
        invalidate_visible_registry(&context);

        let stored = record(Ulid::new(), "docs/stored");
        write_record(&context, &stored).await;
        upsert_visible_registry_records(&context, &[record(Ulid::new(), "docs/never-stored")]);

        let records = list_visible_registry_records(&context).await.unwrap();
        assert_eq!(records.as_ref(), &vec![stored]);
    }
}
