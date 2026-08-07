use std::time::Duration;

use crate::replication::util::dht_registration_effect;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{BlobEvent, DhtEvent, Event, NetEvent, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    BLOB_CLEANUP_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, GROUP_STORAGE_BACKEND_KEYSPACE,
    S3_MULTIPART_UPLOAD_PART_KEYSPACE,
};
use aruna_core::structs::{
    BackendLocation, BackendRef, BlobCleanupWork, BlobLocationKey, GroupStorageBackend,
    MultipartUploadPart, MultipartUploadPartKey, RealmId, RoCrateLimits, WriteOwner,
};
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::Key;
use tracing::{error, warn};
use ulid::Ulid;

use crate::driver::DriverContext;
use crate::group_backends::{backend_key, parse_read};
use crate::jobs::store::iter_prefix_page;

pub const BLOB_CLEANUP_AFTER: Duration = Duration::from_secs(300);
pub const BLOB_CLEANUP_RETRY: Duration = Duration::from_secs(30);
const CLEANUP_PAGE_SIZE: usize = 128;

/// A cleanup row an operation has emitted but storage has not yet accepted.
/// The row is the only durable record that the written object exists, so the
/// work is held until the write succeeds.
#[derive(Debug, Default, PartialEq)]
pub struct PendingCleanup {
    work: Option<BlobCleanupWork>,
    key: Option<Key>,
    channel_closed: bool,
}

impl PendingCleanup {
    /// The write effect for one row. `None` means the row will never encode,
    /// leaving the caller with the failure it already carries.
    pub fn queue(&mut self, work: BlobCleanupWork) -> Option<Effect> {
        let key = cleanup_row_key(&work);
        let effect = cleanup_row_write(&work, &key)?;
        self.work = Some(work);
        self.key = Some(key);
        self.channel_closed = false;
        Some(effect)
    }

    pub fn accepted(&mut self) {
        self.work = None;
        self.key = None;
    }

    /// Re-emit one write per temporary rejection; the caller remains queued.
    pub fn retry(&mut self, error: &StorageError) -> Option<Effect> {
        if self.channel_closed {
            return None;
        }
        if matches!(error, StorageError::ChannelClosed) {
            self.channel_closed = true;
            return None;
        }
        cleanup_row_write(self.work.as_ref()?, self.key.as_ref()?)
    }
}

fn cleanup_row_key(work: &BlobCleanupWork) -> Key {
    match work {
        BlobCleanupWork::ReconcileWrite { location, .. } => {
            location.ulid.to_bytes().to_vec().into()
        }
        BlobCleanupWork::DeleteBlob { .. }
        | BlobCleanupWork::RegisterDht { .. }
        | BlobCleanupWork::ReconcileReservation { .. } => {
            Ulid::generate().to_bytes().to_vec().into()
        }
    }
}

fn cleanup_row_write(work: &BlobCleanupWork, key: &Key) -> Option<Effect> {
    match work.to_bytes() {
        Ok(bytes) => Some(Effect::Storage(StorageEffect::Write {
            key_space: BLOB_CLEANUP_KEYSPACE.to_string(),
            key: key.clone(),
            value: bytes.into(),
            txn_id: None,
        })),
        Err(error) => {
            warn!(error = %error, "Blob cleanup row could not be encoded");
            None
        }
    }
}

pub fn schedule_blob_cleanup_effect() -> Effect {
    Effect::Task(TaskEffect::ShortenTimer {
        key: TaskKey::DrainBlobCleanupQueue,
        after: Duration::ZERO,
    })
}

#[derive(Debug, Default)]
pub struct BlobCleanupOutcome {
    pub processed: usize,
    pub failed: usize,
    pub dropped: usize,
}

// Pages through the whole queue so persistently failing rows cannot starve the
// rows behind them, deleting each page's completed rows before the next fetch.
pub async fn process_cleanup_batch(context: &DriverContext) -> Result<BlobCleanupOutcome, String> {
    let mut outcome = BlobCleanupOutcome::default();
    let mut start_after = None;

    loop {
        let (values, next) = iter_prefix_page(
            &context.storage_handle,
            BLOB_CLEANUP_KEYSPACE,
            None,
            start_after,
            CLEANUP_PAGE_SIZE,
            None,
        )
        .await?;

        let mut done: Vec<(String, Key)> = Vec::new();
        for (key, value) in values {
            // A row nobody can decode would wedge the drain forever.
            let work = match BlobCleanupWork::from_bytes(&value) {
                Ok(work) => work,
                Err(error) => {
                    warn!(error = %error, "Dropping undecodable blob cleanup row");
                    done.push((BLOB_CLEANUP_KEYSPACE.to_string(), key));
                    outcome.dropped = outcome.dropped.saturating_add(1);
                    continue;
                }
            };
            // A tenant backend that is gone can never resolve credentials, so
            // retrying this row forever would wedge the drain.
            if let Some(backend) = work_backend(&work)
                && is_removed_backend(context, backend).await
            {
                error!(backend = %backend, "Dropping blob cleanup for a removed backend");
                done.push((BLOB_CLEANUP_KEYSPACE.to_string(), key));
                outcome.dropped = outcome.dropped.saturating_add(1);
                continue;
            }
            if run_cleanup_work(context, work).await {
                done.push((BLOB_CLEANUP_KEYSPACE.to_string(), key));
                outcome.processed = outcome.processed.saturating_add(1);
            } else {
                outcome.failed = outcome.failed.saturating_add(1);
            }
        }

        delete_cleanup_rows(context, done).await?;

        let Some(next) = next else {
            return Ok(outcome);
        };
        start_after = Some(next);
    }
}

/// The backend a row needs credentials for, if it needs any.
fn work_backend(work: &BlobCleanupWork) -> Option<&BackendRef> {
    work.location().map(|location| &location.backend)
}

/// Only a tenant backend whose record is provably absent counts as removed; an
/// unreadable record leaves the row queued.
async fn is_removed_backend(context: &DriverContext, backend: &BackendRef) -> bool {
    let BackendRef::Group(backend_id) = backend else {
        return false;
    };
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: GROUP_STORAGE_BACKEND_KEYSPACE.to_string(),
            key: backend_key(*backend_id),
            txn_id: None,
        })
        .await;
    matches!(parse_read(event, GroupStorageBackend::from_bytes), Ok(None))
}

async fn delete_cleanup_rows(
    context: &DriverContext,
    deletes: Vec<(String, Key)>,
) -> Result<(), String> {
    if deletes.is_empty() {
        return Ok(());
    }
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchDelete {
            deletes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected cleanup delete event: {other:?}")),
    }
}

// Best-effort execution: a failed entry stays queued and retries next drain.
async fn run_cleanup_work(context: &DriverContext, work: BlobCleanupWork) -> bool {
    match work {
        BlobCleanupWork::DeleteBlob { location } => delete_blob(context, location).await,
        // The committed metadata decides. An unreadable owner is not a proof of
        // either outcome, so the row waits for a drain that can read it.
        BlobCleanupWork::ReconcileWrite { location, owner } => {
            match owns_write(context, &owner, &location).await {
                None => false,
                Some(true) => {
                    if let Some(blob_handle) = context.blob_handle.as_ref() {
                        blob_handle.clear_reservation(location.ulid);
                    }
                    match owner {
                        WriteOwner::Blob {
                            blake3,
                            realm_id,
                            ttl_ms,
                        } => register_dht(context, blake3, realm_id, ttl_ms).await,
                        WriteOwner::UploadPart { .. } => true,
                    }
                }
                Some(false) => delete_blob(context, location).await,
            }
        }
        BlobCleanupWork::ReconcileReservation { location } => {
            reconcile_reservation(context, location).await
        }
        BlobCleanupWork::RegisterDht {
            blake3,
            realm_id,
            ttl_ms,
        } => register_dht(context, blake3, realm_id, ttl_ms).await,
    }
}

async fn register_dht(
    context: &DriverContext,
    blake3: [u8; 32],
    realm_id: RealmId,
    ttl_ms: u64,
) -> bool {
    let Some(net_handle) = context.net_handle.as_ref() else {
        return false;
    };
    let limits = RoCrateLimits {
        holder_ttl_ms: ttl_ms,
        ..RoCrateLimits::default()
    };
    let Ok(effect) = dht_registration_effect(&blake3, realm_id, net_handle.node_id(), &limits)
    else {
        return false;
    };
    match net_handle.send_effect(effect).await {
        Event::Net(NetEvent::Dht(DhtEvent::PutComplete { .. })) => true,
        event => {
            warn!(?event, "Deferred blob DHT registration failed");
            false
        }
    }
}

async fn delete_blob(context: &DriverContext, location: BackendLocation) -> bool {
    let Some(blob_handle) = context.blob_handle.as_ref() else {
        return false;
    };
    match blob_handle
        .send_blob_effect(BlobEffect::Delete { location })
        .await
    {
        Event::Blob(BlobEvent::DeleteFinished) => true,
        event => {
            warn!(?event, "Deferred blob delete failed");
            false
        }
    }
}

async fn reconcile_reservation(context: &DriverContext, location: BackendLocation) -> bool {
    let Some(blob_handle) = context.blob_handle.as_ref() else {
        return false;
    };
    match blob_handle.reconcile_reservation(location).await {
        Ok(done) => done,
        Err(error) => {
            warn!(%error, "Deferred bucket reservation reconciliation failed");
            false
        }
    }
}

/// Whether committed metadata still names this exact physical copy. `None`
/// means the record could not be read or decoded, so nothing is proven.
async fn owns_write(
    context: &DriverContext,
    owner: &WriteOwner,
    location: &BackendLocation,
) -> Option<bool> {
    let (key_space, key): (&str, Key) = match owner {
        WriteOwner::Blob { blake3, .. } => (
            BLOB_LOCATIONS_KEYSPACE,
            BlobLocationKey::new(*blake3, location.backend.clone())
                .to_bytes()
                .into(),
        ),
        WriteOwner::UploadPart {
            upload_id,
            part_number,
        } => (
            S3_MULTIPART_UPLOAD_PART_KEYSPACE,
            MultipartUploadPartKey::new(*upload_id, *part_number)
                .to_bytes()
                .ok()?
                .into(),
        ),
    };
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: key_space.to_string(),
            key,
            txn_id: None,
        })
        .await;
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
        warn!(?event, "Ambiguous write owner could not be read");
        return None;
    };
    let Some(value) = value else {
        return Some(false);
    };
    let owned = match owner {
        WriteOwner::Blob { .. } => BackendLocation::from_bytes(&value).ok()?,
        WriteOwner::UploadPart { .. } => MultipartUploadPart::from_bytes(&value).ok()?.location,
    };
    Some(owned.same_object(location))
}

#[cfg(test)]
mod tests {
    use super::{CLEANUP_PAGE_SIZE, PendingCleanup, process_cleanup_batch};
    use crate::driver::DriverContext;
    use crate::jobs::store::iter_prefix_page;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{BLOB_CLEANUP_KEYSPACE, BLOB_LOCATIONS_KEYSPACE};
    use aruna_core::structs::{
        BackendLocation, BackendRef, BlobCleanupWork, BlobLocationKey, RoCrateLimits, WriteOwner,
    };
    use aruna_core::types::UserId;
    use aruna_storage::storage::{FjallStorage, StorageHandle};
    use std::collections::HashMap;
    use std::time::SystemTime;
    use tempfile::{TempDir, tempdir};
    use ulid::Ulid;

    fn setup_context() -> (TempDir, StorageHandle, DriverContext) {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        (dir, storage, context)
    }

    fn delete_work() -> Vec<u8> {
        let realm_id = aruna_core::structs::RealmId::from_bytes([3u8; 32]);
        BlobCleanupWork::DeleteBlob {
            location: BackendLocation {
                backend: BackendRef::node_default(),
                storage_class: None,
                root: "root".to_string(),
                storage_bucket: "bucket".to_string(),
                backend_path: "bucket/object".to_string(),
                ulid: Ulid::generate(),
                compressed: false,
                encrypted: false,
                created_by: UserId::local(Ulid::generate(), realm_id),
                created_at: SystemTime::now(),
                staging: false,
                partial: false,
                blob_size: 0,
                hashes: HashMap::new(),
            },
        }
        .to_bytes()
        .unwrap()
    }

    #[test]
    fn retains_refused_row() {
        // A temporary storage rejection must not discard the only location.
        let work = BlobCleanupWork::from_bytes(&delete_work()).unwrap();
        let error = aruna_core::errors::StorageError::QueueFull;
        let mut pending = PendingCleanup::default();

        let first = pending.queue(work).unwrap();
        assert_eq!(pending.retry(&error), Some(first));
        let first = pending.retry(&error).unwrap();
        assert_eq!(pending.retry(&error), Some(first));

        let mut pending = PendingCleanup::default();
        assert!(
            pending
                .queue(BlobCleanupWork::from_bytes(&delete_work()).unwrap())
                .is_some()
        );
        pending.accepted();
        assert!(pending.retry(&error).is_none());
    }

    #[test]
    fn closed_retry_stops() {
        let work = BlobCleanupWork::from_bytes(&delete_work()).unwrap();
        let mut pending = PendingCleanup::default();
        assert!(pending.queue(work).is_some());

        assert_eq!(
            pending.retry(&aruna_core::errors::StorageError::ChannelClosed),
            None
        );
        assert_eq!(
            pending.retry(&aruna_core::errors::StorageError::Timeout),
            None
        );
    }

    fn group_delete_work() -> Vec<u8> {
        let mut work = BlobCleanupWork::from_bytes(&delete_work()).unwrap();
        if let BlobCleanupWork::DeleteBlob { location } = &mut work {
            location.backend = BackendRef::Group(Ulid::generate());
        }
        work.to_bytes().unwrap()
    }

    async fn write_rows(storage: &StorageHandle, rows: Vec<Vec<u8>>) {
        let writes = rows
            .into_iter()
            .map(|value| {
                (
                    BLOB_CLEANUP_KEYSPACE.to_string(),
                    Ulid::generate().to_bytes().to_vec().into(),
                    value.into(),
                )
            })
            .collect();
        let event = storage
            .send_storage_effect(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::BatchWriteResult { .. })
        ));
    }

    async fn remaining_rows(storage: &StorageHandle) -> usize {
        let (values, _) = iter_prefix_page(storage, BLOB_CLEANUP_KEYSPACE, None, None, 4096, None)
            .await
            .unwrap();
        values.len()
    }

    #[tokio::test]
    async fn drops_poison_rows() {
        // An undecodable row must be deleted, and the rows after it still run.
        let (_dir, storage, context) = setup_context();
        write_rows(
            &storage,
            vec![b"not-postcard-at-all".to_vec(), delete_work()],
        )
        .await;

        let outcome = process_cleanup_batch(&context).await.unwrap();

        assert_eq!(outcome.dropped, 1);
        // The delete row fails because this context has no blob handle.
        assert_eq!(outcome.failed, 1);
        assert_eq!(remaining_rows(&storage).await, 1);
    }

    #[tokio::test]
    async fn drops_removed_backend() {
        // A tenant backend whose record is gone can never resolve credentials,
        // so its delete must be dropped instead of retried forever.
        let (_dir, storage, context) = setup_context();
        write_rows(&storage, vec![group_delete_work()]).await;

        let outcome = process_cleanup_batch(&context).await.unwrap();

        assert_eq!(outcome.dropped, 1);
        assert_eq!(outcome.failed, 0);
        assert_eq!(remaining_rows(&storage).await, 0);
    }

    fn reconcile_work(location: &BackendLocation) -> Vec<u8> {
        BlobCleanupWork::ReconcileWrite {
            location: location.clone(),
            owner: WriteOwner::Blob {
                blake3: [7u8; 32],
                realm_id: location.created_by.realm_id,
                ttl_ms: RoCrateLimits::default().holder_ttl_ms,
            },
        }
        .to_bytes()
        .unwrap()
    }

    #[tokio::test]
    async fn owned_write_waits() {
        // The committed copy stays queued until its DHT holder is registered.
        let (_dir, storage, context) = setup_context();
        let BlobCleanupWork::DeleteBlob { location } =
            BlobCleanupWork::from_bytes(&delete_work()).unwrap()
        else {
            panic!("expected a delete row")
        };
        let event = storage
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
                key: BlobLocationKey::new([7u8; 32], location.backend.clone())
                    .to_bytes()
                    .into(),
                value: location.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
        write_rows(&storage, vec![reconcile_work(&location)]).await;

        let outcome = process_cleanup_batch(&context).await.unwrap();

        assert_eq!(outcome.processed, 0);
        assert_eq!(outcome.failed, 1);
        assert_eq!(remaining_rows(&storage).await, 1);
    }

    #[tokio::test]
    async fn unowned_write_deletes() {
        // Without a location row naming this copy the commit never landed, so
        // the bytes have to go; this context has no blob handle, so the delete
        // fails and the row stays for the next drain.
        let (_dir, storage, context) = setup_context();
        let BlobCleanupWork::DeleteBlob { location } =
            BlobCleanupWork::from_bytes(&delete_work()).unwrap()
        else {
            panic!("expected a delete row")
        };
        write_rows(&storage, vec![reconcile_work(&location)]).await;

        let outcome = process_cleanup_batch(&context).await.unwrap();

        assert_eq!(outcome.processed, 0);
        assert_eq!(outcome.failed, 1);
        assert_eq!(remaining_rows(&storage).await, 1);
    }

    #[tokio::test]
    async fn pages_past_failures() {
        // Rows behind a full page of poison must still be reached in one drain.
        let (_dir, storage, context) = setup_context();
        let rows = std::iter::repeat_n(b"broken".to_vec(), CLEANUP_PAGE_SIZE + 5).collect();
        write_rows(&storage, rows).await;

        let outcome = process_cleanup_batch(&context).await.unwrap();

        assert_eq!(outcome.dropped, CLEANUP_PAGE_SIZE + 5);
        assert_eq!(remaining_rows(&storage).await, 0);
    }
}
