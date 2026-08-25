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
            Some(plan) => work |= plan.uploads > 0 || plan.truncated,
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
    let view = match folder.mode {
        aruna_core::structs::FolderMode::UploadOnly => RemoteView::default(),
        aruna_core::structs::FolderMode::TwoWay => fetch_heads(context, folder).await?,
    };

    let mut keys: Vec<String> = local
        .keys()
        .chain(view.heads.keys())
        .filter(|key| view.covers(key))
        .cloned()
        .collect();
    keys.sort();
    keys.dedup();
    let mut plan = ReconcilePlan {
        truncated: view.next_cursor.is_some(),
        ..ReconcilePlan::default()
    };
    for chunk in keys.chunks(SYNC_PAGE_SIZE) {
        let page = ReconcileInput {
            folder: folder.clone(),
            local: subset(&local, chunk),
            remote: subset(&view.heads, chunk),
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
            list_cursor: view.next_cursor,
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

/// What one pass listed of the realm side, and the window it may decide about.
/// A pass never judges a key it did not list: that is how a folder larger than
/// one listing converges without inventing deletions.
#[derive(Default)]
struct RemoteView {
    heads: BTreeMap<String, RemoteHead>,
    /// Exclusive lower bound this pass resumed from.
    resume_after: Option<String>,
    /// Inclusive upper bound the listing reached, when it was cut short.
    boundary: Option<String>,
    /// Where the next pass resumes. `None` restarts from the beginning.
    next_cursor: Option<SyncListCursor>,
}

impl RemoteView {
    /// Whether this pass listed far enough to decide about `key`.
    fn covers(&self, key: &str) -> bool {
        self.resume_after.as_deref().is_none_or(|after| key > after)
            && self.boundary.as_deref().is_none_or(|bound| key <= bound)
    }
}

/// Closes one listing window. An exhausted listing covers every remaining key,
/// however the pass ended: the boundary is cleared so local-only keys after the
/// last head are decided too, and the next pass restarts instead of repeating
/// exactly this window.
fn close_window(view: &mut RemoteView, next: Option<SyncListCursor>) {
    if next.is_none() {
        view.boundary = None;
    }
    view.next_cursor = next;
}

/// Every current head under the folder's bound prefix, as bounded pages served
/// by the folder's realm node with the owner's authority.
async fn fetch_heads(context: &Arc<DriverContext>, folder: &SyncedFolder) -> Option<RemoteView> {
    let metadata = context.metadata_handle.as_ref()?;
    let realm_id = *context.net_handle.as_ref()?.realm_id();
    let auth = AuthContext {
        user_id: folder.created_by,
        realm_id,
        path_restrictions: None,
    };
    let mut view = RemoteView {
        resume_after: folder
            .list_cursor
            .as_ref()
            .and_then(|cursor| folder.remote.relative_path(&cursor.key)),
        ..RemoteView::default()
    };
    let mut cursor = folder.list_cursor.clone();
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
            view.boundary = Some(head.relative.clone());
            view.heads.insert(head.relative.clone(), head);
        }
        // A folder past the in-memory bound stops here and resumes next pass;
        // the boundary keeps this pass from judging the keys it never saw.
        if view.heads.len() >= MAX_REMOTE_HEADS {
            close_window(&mut view, next);
            return Some(view);
        }
        match next {
            Some(next) => cursor = Some(next),
            None => {
                close_window(&mut view, None);
                return Some(view);
            }
        }
    }
}

/// Wakes the upload drain after an explicit owner action queued a row.
pub(crate) async fn arm_upload_timer(context: &Arc<DriverContext>) {
    arm_timer(context, TaskKey::DrainSyncUploadOutbox).await;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn view(boundary: &str) -> RemoteView {
        RemoteView {
            boundary: Some(boundary.to_string()),
            ..RemoteView::default()
        }
    }

    // The in-memory bound and the end of the listing can coincide. The window
    // must then cover the keys after the last head, and the next pass must start
    // over instead of listing exactly this window again forever.
    #[test]
    fn clears_final_boundary() {
        let mut exhausted = view("m.txt");
        close_window(&mut exhausted, None);
        assert!(exhausted.next_cursor.is_none());
        assert!(exhausted.covers("z.txt"));

        let mut truncated = view("m.txt");
        close_window(
            &mut truncated,
            Some(SyncListCursor {
                key: "m.txt".to_string(),
                version_id: None,
            }),
        );
        assert!(truncated.next_cursor.is_some());
        assert!(!truncated.covers("z.txt"));
        assert!(truncated.covers("a.txt"));
    }

    // A resumed pass never re-decides the keys the previous one already passed.
    #[test]
    fn window_skips_resumed() {
        let resumed = RemoteView {
            resume_after: Some("m.txt".to_string()),
            ..RemoteView::default()
        };
        assert!(!resumed.covers("a.txt"));
        assert!(resumed.covers("z.txt"));
    }
}
