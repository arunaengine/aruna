//! Asks each folder's realm node to pull the local versions that changed.
//! The device never pushes: one request names the exact local version, which the
//! node reads back and commits as the owner.

use std::sync::Arc;
use std::time::Duration;

use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{SYNC_BASE_KEYSPACE, SYNC_UPLOAD_OUTBOX_KEYSPACE};
use aruna_core::metadata::MetadataAuthToken;
use aruna_core::structs::{
    AuthContext, EntrySide, EntryState, FolderState, RealmId, SyncBase, SyncPullAck, SyncRefusal,
    SyncedBytes, SyncedFolder, VersionedObjectArn,
};
use aruna_core::task::TaskKey;
use aruna_core::types::{Key, TxnId};
use aruna_core::time::unix_timestamp_millis;
use aruna_storage::storage::StorageHandle;
use aruna_tasks::TaskHandle;
use tracing::{info, warn};

use crate::device::backlog::{
    ForwardOutcome, QueueDrain, arm_timer, drain_queue, exhausted, retry_due_ms,
};
use crate::device::drain::DrainOutcome;
use crate::driver::DriverContext;
use crate::metadata::protocol::MetadataTransportMessage;

use super::repository::{
    MAX_UPLOAD_ATTEMPTS, SYNC_PAGE_SIZE, SyncUpload, UploadState, abort_txn, base_entry, base_key,
    commit_txn, folder_key, read_value, start_txn, upload_entry, write_rows,
};

/// Delay before a deferred pass looks for the realm again.
pub const UPLOAD_DEFER_RETRY_AFTER: Duration = Duration::from_secs(15);

/// Delay between passes while uploads are still due.
pub const UPLOAD_CONTINUE_AFTER: Duration = Duration::from_millis(250);

/// Forwards every due upload, oldest folder first.
pub async fn drain_sync_outbox(context: &Arc<DriverContext>) -> DrainOutcome {
    let Some(net_handle) = context.net_handle.as_ref() else {
        return DrainOutcome::Deferred;
    };
    let realm_id = *net_handle.realm_id();
    let node_id = net_handle.node_id();
    let now = unix_timestamp_millis();
    drain_queue(
        now,
        UploadDrain {
            context,
            realm_id,
            node_id,
        },
    )
    .await
}

/// One upload drain queue handed to the shared page loop.
struct UploadDrain<'a> {
    context: &'a Arc<DriverContext>,
    realm_id: RealmId,
    node_id: aruna_core::NodeId,
}

impl QueueDrain for UploadDrain<'_> {
    type Entry = SyncUpload;

    async fn read(&mut self, cursor: Option<Key>) -> Option<(Vec<SyncUpload>, Option<Key>)> {
        read_page(self.context, cursor).await
    }

    async fn forward(&mut self, upload: SyncUpload) -> ForwardOutcome {
        let Some(folder) = load_folder(self.context, upload.folder_id).await else {
            // The folder is unbound: this entry is nobody's work any more, and
            // leaving it would keep the drain awake for a folder that is gone.
            drop_upload(self.context, &upload).await;
            return ForwardOutcome::Settled;
        };
        // An unbinding folder publishes nothing more; its entries are going.
        if folder.state == FolderState::Deleting {
            return ForwardOutcome::Settled;
        }
        let attempts = upload.attempts().saturating_add(1);
        if !claim_upload(self.context, &upload, attempts).await {
            return ForwardOutcome::Settled;
        }
        // Only a claimed entry keeps the drain awake.
        let source = match VersionedObjectArn::new(
            self.realm_id,
            self.node_id,
            folder.local_bucket.clone(),
            upload.relative.clone(),
            upload.local_version.unwrap_or_default(),
        ) {
            Ok(source) => source,
            Err(error) => {
                park(self.context, &upload, error.to_string(), false).await;
                return ForwardOutcome::Recheck;
            }
        };
        forward_upload(self.context, &folder, &upload, source, attempts).await;
        ForwardOutcome::Recheck
    }
}

async fn forward_upload(
    context: &Arc<DriverContext>,
    folder: &SyncedFolder,
    upload: &SyncUpload,
    source: VersionedObjectArn,
    attempts: u32,
) {
    let Some(metadata) = context.metadata_handle.as_ref() else {
        return;
    };
    let auth = AuthContext {
        user_id: folder.created_by,
        realm_id: source.realm_id,
        path_restrictions: None,
        session: None,
    };
    let message = MetadataTransportMessage::ForwardSyncPull {
        auth_token: MetadataAuthToken::internal(auth),
        source: Box::new(source),
        blake3: upload.blake3,
        size: upload.size,
        target_bucket: folder.remote.bucket.clone(),
        target_key: folder.remote.remote_key(&upload.relative),
        deleted: upload.deleted,
    };
    match metadata
        .request_forwarded_write(folder.remote.node_id, message)
        .await
    {
        Ok(MetadataTransportMessage::ForwardedSyncPull { result: Ok(ack) }) => {
            settle_upload(context, upload, &ack).await;
        }
        Ok(MetadataTransportMessage::ForwardedSyncPull {
            result: Err(refusal),
        }) => match permanent(&refusal) {
            true => park(context, upload, format!("{refusal:?}"), false).await,
            false => retry(context, upload, format!("{refusal:?}"), attempts).await,
        },
        Ok(other) => {
            retry(
                context,
                upload,
                crate::metadata::transport_message_kind(&other).to_string(),
                attempts,
            )
            .await;
        }
        Err(error) => retry(context, upload, error.to_string(), attempts).await,
    }
}

/// Removes one upload row whose folder no longer exists.
async fn drop_upload(context: &Arc<DriverContext>, upload: &SyncUpload) {
    if !super::repository::delete_rows(
        context,
        vec![(
            SYNC_UPLOAD_OUTBOX_KEYSPACE.to_string(),
            base_key(upload.folder_id, &upload.relative),
        )],
        None,
    )
    .await
    {
        warn!(relative = %upload.relative, "Failed to drop an orphaned upload row");
    }
}

/// Authorization and target verdicts do not improve by waiting.
fn permanent(refusal: &SyncRefusal) -> bool {
    // Invalid covers a device serve refused for its bytes: retrying cannot help.
    matches!(
        refusal,
        SyncRefusal::Unauthorized
            | SyncRefusal::Forbidden
            | SyncRefusal::NotFound
            | SyncRefusal::Invalid(_)
    )
}

/// Records the realm version as the entry's new base and drops the outbox row in
/// one transaction, so an acknowledged pull is never forwarded twice; a row the
/// owner must still answer keeps its reported state.
async fn settle_upload(context: &Arc<DriverContext>, upload: &SyncUpload, ack: &SyncPullAck) {
    let current = read_value(
        context,
        SYNC_BASE_KEYSPACE,
        base_key(upload.folder_id, &upload.relative),
        None,
    )
    .await
    .and_then(|bytes| SyncBase::from_bytes(&bytes).ok());
    let base = settled_base(upload, ack, current);
    let Ok(row) = base_entry(upload.folder_id, &upload.relative, &base) else {
        warn!(relative = %upload.relative, "Failed to encode a synced base");
        return;
    };
    let Some(txn_id) = start_txn(context).await else {
        return;
    };
    let settled = write_rows(context, vec![row], Some(txn_id)).await
        && super::repository::delete_rows(
            context,
            vec![(
                SYNC_UPLOAD_OUTBOX_KEYSPACE.to_string(),
                base_key(upload.folder_id, &upload.relative),
            )],
            Some(txn_id),
        )
        .await;
    if !settled {
        abort_txn(context, txn_id).await;
        return;
    }
    if commit_txn(context, txn_id).await {
        info!(relative = %upload.relative, version = %ack.version_id, "Published a synced file");
    }
}

/// The base one acknowledged pull leaves behind. A pending entry keeps the
/// state and the observation the owner was shown; only the synced bytes move.
fn settled_base(upload: &SyncUpload, ack: &SyncPullAck, current: Option<SyncBase>) -> SyncBase {
    let pending = current
        .as_ref()
        .is_some_and(|base| base.pending_at.is_some());
    let local = EntrySide {
        size: upload.size,
        modified_at_ms: None,
        fingerprint: Some(upload.fingerprint.clone()),
        blake3: upload.blake3,
        version_id: upload.local_version,
    };
    let remote = EntrySide {
        size: upload.size,
        modified_at_ms: None,
        fingerprint: None,
        blake3: upload.blake3,
        version_id: Some(ack.version_id),
    };
    // Only bytes this device knows the strong hash of become a synced base; a
    // delete leaves none at all.
    let synced = (!upload.deleted)
        .then_some(upload.blake3)
        .flatten()
        .map(|blake3| SyncedBytes {
            fingerprint: upload.fingerprint.clone(),
            blake3,
            size: upload.size,
            remote_version_id: ack.version_id,
        });
    match (pending, current) {
        (true, Some(base)) => SyncBase {
            synced,
            local_version_id: upload.local_version,
            synced_at_ms: unix_timestamp_millis(),
            remote: Some(remote),
            ..base
        },
        (_, _) => SyncBase {
            synced,
            local_version_id: upload.local_version,
            synced_at_ms: unix_timestamp_millis(),
            entry: match upload.deleted {
                true => EntryState::LocalDeleted,
                false => EntryState::InSync,
            },
            pending_at: None,
            local: (!upload.deleted).then_some(local),
            remote: Some(remote),
        },
    }
}

/// Stores the next state only while the row still carries the state the scan
/// read, so a concurrent unbind or a newer version wins instead of coming back.
async fn claim_upload(context: &Arc<DriverContext>, upload: &SyncUpload, attempts: u32) -> bool {
    let next = UploadState::Pending {
        due_at_ms: retry_due_ms(attempts),
        attempts,
        last_error: None,
    };
    store_state(context, upload, next, true).await
}

async fn retry(context: &Arc<DriverContext>, upload: &SyncUpload, reason: String, attempts: u32) {
    let next = match exhausted(attempts, MAX_UPLOAD_ATTEMPTS) {
        true => UploadState::Failed {
            reason,
            retryable: true,
        },
        false => UploadState::Pending {
            due_at_ms: retry_due_ms(attempts),
            attempts,
            last_error: Some(reason),
        },
    };
    store_state(context, upload, next, false).await;
}

async fn park(context: &Arc<DriverContext>, upload: &SyncUpload, reason: String, retryable: bool) {
    store_state(
        context,
        upload,
        UploadState::Failed { reason, retryable },
        false,
    )
    .await;
}

/// Writes one upload row under a new state. `guarded` refuses the write when
/// the stored row no longer carries the state this pass read.
async fn store_state(
    context: &Arc<DriverContext>,
    upload: &SyncUpload,
    next: UploadState,
    guarded: bool,
) -> bool {
    let Some(txn_id) = start_txn(context).await else {
        return false;
    };
    let updated = SyncUpload {
        state: next,
        ..upload.clone()
    };
    let stored = match upload_entry(&updated) {
        Ok(row) => {
            (!guarded || holds_state(context, upload, txn_id).await)
                && write_rows(context, vec![row], Some(txn_id)).await
        }
        Err(_) => false,
    };
    if !stored {
        abort_txn(context, txn_id).await;
        return false;
    }
    commit_txn(context, txn_id).await
}

async fn holds_state(context: &Arc<DriverContext>, upload: &SyncUpload, txn_id: TxnId) -> bool {
    read_value(
        context,
        SYNC_UPLOAD_OUTBOX_KEYSPACE,
        base_key(upload.folder_id, &upload.relative),
        Some(txn_id),
    )
    .await
    .and_then(|bytes| SyncUpload::from_bytes(&bytes).ok())
    .is_some_and(|stored| stored.state == upload.state)
}

async fn read_page(
    context: &Arc<DriverContext>,
    cursor: Option<Key>,
) -> Option<(Vec<SyncUpload>, Option<Key>)> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Iter {
            key_space: SYNC_UPLOAD_OUTBOX_KEYSPACE.to_string(),
            prefix: None,
            start: cursor.map(IterStart::After),
            limit: SYNC_PAGE_SIZE,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => Some((
            values
                .into_iter()
                .filter_map(|(_, bytes)| SyncUpload::from_bytes(&bytes).ok())
                .collect(),
            next_start_after,
        )),
        other => {
            warn!(event = ?other, "Failed to scan the synced-folder upload outbox");
            None
        }
    }
}

pub(crate) async fn load_folder(
    context: &Arc<DriverContext>,
    folder_id: ulid::Ulid,
) -> Option<SyncedFolder> {
    read_value(
        context,
        aruna_core::keyspaces::SYNCED_FOLDER_KEYSPACE,
        folder_key(folder_id),
        None,
    )
    .await
    .and_then(|bytes| SyncedFolder::from_bytes(&bytes).ok())
}

/// Re-arms the drain when the outbox still holds rows.
pub async fn restore_upload_timer(storage: &StorageHandle, task_handle: &TaskHandle) {
    if !has_rows(storage, SYNC_UPLOAD_OUTBOX_KEYSPACE).await {
        return;
    }
    arm_timer(
        task_handle,
        TaskKey::DrainSyncUploadOutbox,
        "Failed to restore the synced-folder upload timer",
    )
    .await;
}

/// Whether a keyspace holds at least one row.
pub(crate) async fn has_rows(storage: &StorageHandle, key_space: &str) -> bool {
    matches!(
        storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: key_space.to_string(),
                prefix: None,
                start: None,
                limit: 1,
                txn_id: None,
            })
            .await,
        Event::Storage(StorageEvent::IterResult { values, .. }) if !values.is_empty()
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use aruna_core::structs::{FolderMode, RemoteBinding};
    use aruna_core::types::UserId;
    use ulid::Ulid;

    use super::{
        ForwardOutcome, QueueDrain, SyncUpload, UploadDrain, UploadState, drain_sync_outbox,
    };
    use crate::device::drain::DrainOutcome;
    use crate::device::sync::folders::store_folder;
    use crate::device::sync::repository::{base_key, read_value, upload_entry, write_rows};
    use crate::device::tests::fixtures::context;
    use crate::driver::DriverContext;
    use aruna_core::keyspaces::SYNC_UPLOAD_OUTBOX_KEYSPACE;
    use aruna_core::structs::{FolderState, RealmId, SyncedFolder};

    fn node_id() -> aruna_core::NodeId {
        iroh::SecretKey::from_bytes(&[1u8; 32]).public()
    }

    fn folder(folder_id: Ulid) -> SyncedFolder {
        SyncedFolder {
            folder_id,
            root: "/home/ada/lab".to_string(),
            local_bucket: "folder-1".to_string(),
            group_id: Ulid::from_bytes([1u8; 16]),
            remote: RemoteBinding {
                node_id: node_id(),
                bucket: "lab".to_string(),
                prefix: "ada/".to_string(),
            },
            mode: FolderMode::TwoWay,
            propagate_deletes: false,
            state: FolderState::Active,
            created_by: UserId::local(Ulid::generate(), RealmId::from_bytes([1u8; 32])),
            created_at_ms: 1,
            last_reconcile_ms: None,
            last_error: None,
            last_error_at_ms: None,
            observed_files: 0,
            list_cursor: None,
        }
    }

    fn upload(folder_id: Ulid) -> SyncUpload {
        SyncUpload {
            folder_id,
            relative: "notes.txt".to_string(),
            deleted: false,
            fingerprint: "1-1-1-1".to_string(),
            blake3: Some([5u8; 32]),
            size: 1,
            local_version: Some(Ulid::generate()),
            queued_at_ms: 0,
            state: UploadState::Pending {
                due_at_ms: 0,
                attempts: 0,
                last_error: None,
            },
        }
    }

    fn drain(context: &Arc<DriverContext>) -> UploadDrain<'_> {
        UploadDrain {
            context,
            realm_id: RealmId::from_bytes([1u8; 32]),
            node_id: node_id(),
        }
    }

    async fn store(context: &Arc<DriverContext>, upload: &SyncUpload) {
        assert!(write_rows(context, vec![upload_entry(upload).unwrap()], None).await);
    }

    async fn stored(context: &Arc<DriverContext>, upload: &SyncUpload) -> SyncUpload {
        let bytes = read_value(
            context,
            SYNC_UPLOAD_OUTBOX_KEYSPACE,
            base_key(upload.folder_id, &upload.relative),
            None,
        )
        .await
        .expect("the upload row is stored");
        SyncUpload::from_bytes(&bytes).unwrap()
    }

    #[tokio::test]
    async fn defers_without_realm() {
        let (_tempdir, context) = context().await;
        let context = Arc::new(context);
        assert_eq!(drain_sync_outbox(&context).await, DrainOutcome::Deferred);
    }

    #[tokio::test]
    async fn drops_unbound_upload() {
        // An unbound folder owns nothing: the entry is dropped instead of
        // keeping the drain awake for a folder that is gone.
        let (_tempdir, context) = context().await;
        let context = Arc::new(context);
        let entry = upload(Ulid::generate());
        store(&context, &entry).await;

        assert_eq!(
            drain(&context).forward(entry.clone()).await,
            ForwardOutcome::Settled
        );
        assert!(
            read_value(
                &context,
                SYNC_UPLOAD_OUTBOX_KEYSPACE,
                base_key(entry.folder_id, &entry.relative),
                None,
            )
            .await
            .is_none()
        );
    }

    #[tokio::test]
    async fn claims_before_forward() {
        // The claim is stored before the forward, so a crash mid-forward
        // resumes from the claimed state rather than the old one.
        let (_tempdir, context) = context().await;
        let context = Arc::new(context);
        let folder = folder(Ulid::generate());
        store_folder(&context, &folder).await.unwrap();
        let entry = upload(folder.folder_id);
        store(&context, &entry).await;

        assert_eq!(
            drain(&context).forward(entry.clone()).await,
            ForwardOutcome::Recheck
        );
        assert!(matches!(
            stored(&context, &entry).await.state,
            UploadState::Pending { attempts: 1, .. }
        ));
    }

    #[tokio::test]
    async fn keeps_newer_state() {
        // The entry advanced while the page was in flight: the failed claim
        // must let the newer state win instead of overwriting it.
        let (_tempdir, context) = context().await;
        let context = Arc::new(context);
        let folder = folder(Ulid::generate());
        store_folder(&context, &folder).await.unwrap();
        let entry = upload(folder.folder_id);
        store(&context, &entry).await;
        let newer = SyncUpload {
            state: UploadState::Failed {
                reason: "gone".to_string(),
                retryable: false,
            },
            ..entry.clone()
        };
        store(&context, &newer).await;

        assert_eq!(
            drain(&context).forward(entry).await,
            ForwardOutcome::Settled
        );
        assert_eq!(stored(&context, &newer).await.state, newer.state);
    }
}
