//! Two-way synced folders on the owner's own machine.
//!
//! A folder is bound to one realm bucket prefix. The device observes its files
//! as a read-only local bucket, asks the realm node to pull what changed, and
//! writes what the realm changed back to disk through guarded local writes.
//! Local data always wins locally: see `aruna_core::structs::decide`.

pub mod actions;
pub mod folders;
pub mod materialize;
pub mod outbox;
pub mod reconcile;
pub mod repository;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use aruna_core::metadata::MetadataAuthToken;
use aruna_core::structs::{
    AuthContext, FolderState, Observed, RemoteHead, SyncListCursor, SyncPageLimit, SyncedFolder,
};
use aruna_core::task::{TaskEvent, TaskKey};
use aruna_core::util::unix_timestamp_millis;
use aruna_storage::storage::StorageHandle;
use aruna_tasks::TaskHandle;
use tracing::{debug, warn};

use crate::device::drain::DrainOutcome;
use crate::driver::DriverContext;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::staging::offered_directory::OfferDirectoryInput;

use folders::{list_folders, store_folder};
use materialize::apply_downloads;
use reconcile::{ReconcileFolderOperation, ReconcileInput, ReconcilePlan};
use repository::SYNC_PAGE_SIZE;

pub use outbox::{
    UPLOAD_CONTINUE_AFTER, UPLOAD_DEFER_RETRY_AFTER, drain_sync_outbox, restore_upload_timer,
};

/// Delay before the next pass when every folder is settled.
pub const RECONCILE_IDLE_AFTER: Duration = Duration::from_secs(30);

/// Delay before a deferred pass looks for the realm again.
pub const RECONCILE_RETRY_AFTER: Duration = Duration::from_secs(15);

/// Delay between passes while a folder still has work.
pub const RECONCILE_CONTINUE_AFTER: Duration = Duration::from_millis(250);

/// Remote heads one pass holds in memory. A folder beyond this converges over
/// several passes instead of building one unbounded listing.
const MAX_REMOTE_HEADS: usize = 16_384;

/// Reconciles every active folder once.
pub async fn reconcile_folders(context: &Arc<DriverContext>) -> DrainOutcome {
    if context.net_handle.is_none() {
        return DrainOutcome::Deferred;
    }
    let Ok(folders) = list_folders(context).await else {
        return DrainOutcome::Deferred;
    };
    let mut work = false;
    for folder in folders {
        if folder.state != FolderState::Active {
            continue;
        }
        match reconcile_folder(context, &folder).await {
            Some(plan) => work |= plan.uploads > 0 || !plan.downloads.is_empty(),
            None => debug!(folder = %folder.folder_id, "Could not reconcile the folder"),
        }
    }
    match work {
        true => DrainOutcome::More,
        false => DrainOutcome::Idle,
    }
}

/// Reconciles one folder: observe the directory, list the realm heads, decide
/// every entry, then write what the decisions allow.
pub async fn reconcile_folder(
    context: &Arc<DriverContext>,
    folder: &SyncedFolder,
) -> Option<ReconcilePlan> {
    let net_handle = context.net_handle.as_ref()?;
    let sweep = crate::staging::offered_directory::offer_directory(
        context,
        OfferDirectoryInput {
            bucket: folder.local_bucket.clone(),
            root: folder.root.clone(),
            group_id: folder.group_id,
            realm_id: *net_handle.realm_id(),
            node_id: net_handle.node_id(),
            user_id: folder.created_by,
        },
    )
    .await
    .inspect_err(|error| warn!(folder = %folder.folder_id, error = %error, "Folder sweep failed"))
    .ok()?;

    let local: BTreeMap<String, Observed> = sweep
        .observed
        .into_iter()
        .map(|file| {
            (
                file.relative,
                Observed {
                    fingerprint: file.fingerprint,
                    size: file.size,
                    blake3: None,
                    modified_at_ms: file.modified_at_ms,
                    version_id: Some(file.version_id),
                },
            )
        })
        .collect();
    let remote = match folder.mode {
        aruna_core::structs::FolderMode::UploadOnly => BTreeMap::new(),
        aruna_core::structs::FolderMode::TwoWay => fetch_heads(context, folder).await?,
    };

    let mut keys: Vec<String> = local.keys().chain(remote.keys()).cloned().collect();
    keys.sort();
    keys.dedup();
    let mut plan = ReconcilePlan::default();
    for chunk in keys.chunks(SYNC_PAGE_SIZE) {
        let page = ReconcileInput {
            folder: folder.clone(),
            local: subset(&local, chunk),
            remote: subset(&remote, chunk),
            now_ms: unix_timestamp_millis(),
        };
        match crate::driver::drive(ReconcileFolderOperation::new(page), context).await {
            Ok(page) => plan.absorb(page),
            Err(error) => {
                warn!(folder = %folder.folder_id, error = %error, "Could not decide a folder page");
                return None;
            }
        }
    }
    apply_downloads(context, folder, std::mem::take(&mut plan.downloads)).await;
    let _ = store_folder(
        context,
        &SyncedFolder {
            last_reconcile_ms: Some(unix_timestamp_millis()),
            ..folder.clone()
        },
    )
    .await;
    if plan.uploads > 0 {
        arm_timer(context, TaskKey::DrainSyncUploadOutbox).await;
    }
    Some(plan)
}

fn subset<T: Clone>(source: &BTreeMap<String, T>, keys: &[String]) -> BTreeMap<String, T> {
    keys.iter()
        .filter_map(|key| source.get_key_value(key))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

/// Every current head under the folder's bound prefix, as bounded pages served
/// by the folder's realm node with the owner's authority.
async fn fetch_heads(
    context: &Arc<DriverContext>,
    folder: &SyncedFolder,
) -> Option<BTreeMap<String, RemoteHead>> {
    let metadata = context.metadata_handle.as_ref()?;
    let realm_id = *context.net_handle.as_ref()?.realm_id();
    let auth = AuthContext {
        user_id: folder.created_by,
        realm_id,
        path_restrictions: None,
    };
    let mut heads = BTreeMap::new();
    let mut cursor: Option<SyncListCursor> = None;
    loop {
        let message = MetadataTransportMessage::ForwardListVersions {
            auth_token: MetadataAuthToken::internal(auth.clone()),
            bucket: folder.remote.bucket.clone(),
            prefix: folder.remote.prefix.clone(),
            cursor,
            limit: SyncPageLimit::default(),
        };
        let reply = metadata
            .request_forwarded_write(folder.remote.node_id, message)
            .await
            .ok()?;
        let MetadataTransportMessage::ForwardedVersions { result: Ok(page) } = reply else {
            debug!(folder = %folder.folder_id, "The realm node refused the folder listing");
            return None;
        };
        let (page_heads, next) = page.into_parts();
        for head in page_heads {
            heads.insert(head.relative.clone(), head);
        }
        if heads.len() >= MAX_REMOTE_HEADS {
            return Some(heads);
        }
        match next {
            Some(next) => cursor = Some(next),
            None => return Some(heads),
        }
    }
}

async fn arm_timer(context: &Arc<DriverContext>, key: TaskKey) {
    let Some(task_handle) = context.task_handle.as_ref() else {
        return;
    };
    if let TaskEvent::Error { message, .. } = task_handle
        .schedule_timer_if_idle(key, Duration::ZERO)
        .await
    {
        warn!(message = %message, "Failed to arm a synced-folder timer");
    }
}

/// Re-arms both folder timers when this device binds any folder. A node that
/// binds none is not a device that syncs, so nothing is scheduled there.
pub async fn restore_sync_timers(storage: &StorageHandle, task_handle: &TaskHandle) {
    if outbox::has_rows(storage, aruna_core::keyspaces::SYNCED_FOLDER_KEYSPACE).await
        && let TaskEvent::Error { message, .. } = task_handle
            .schedule_timer_if_idle(TaskKey::ReconcileSyncedFolders, Duration::ZERO)
            .await
    {
        warn!(message = %message, "Failed to restore the synced-folder timer");
    }
    restore_upload_timer(storage, task_handle).await;
}
