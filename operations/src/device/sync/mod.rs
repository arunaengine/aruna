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
    AuthContext, FolderState, Observed, RemoteBinding, RemoteHead, SyncListCursor, SyncPageLimit,
    SyncRefusal, SyncVersionPage, SyncedFolder,
};
use aruna_core::task::{TaskEvent, TaskKey};
use aruna_core::types::NodeId;
use aruna_core::util::unix_timestamp_millis;
use aruna_tasks::TaskHandle;
use thiserror::Error;
use tracing::warn;

use crate::device::drain::DrainOutcome;
use crate::driver::DriverContext;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::staging::offered_directory::{OfferDirectoryInput, OfferedDirectoryError};

use folders::{list_folders, store_folder};
use materialize::apply_downloads;
use reconcile::{ReconcileError, ReconcileFolderOperation, ReconcileInput, ReconcilePlan};
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

#[derive(Debug, Error, PartialEq)]
pub enum ReconcileFolderError {
    #[error(transparent)]
    Sweep(#[from] OfferedDirectoryError),
    #[error("the realm node refused the folder sync: {0:?}")]
    Refused(SyncRefusal),
    #[error("the realm node is unreachable: {0}")]
    Unreachable(String),
    #[error("could not decide the folder: {0}")]
    Decide(#[from] ReconcileError),
    #[error("the folder sync is unavailable")]
    Unavailable,
}

impl ReconcileFolderError {
    /// The owner-facing reason, naming the remote the folder is bound to.
    pub fn describe(&self, remote: &RemoteBinding) -> String {
        match self {
            Self::Refused(SyncRefusal::NotFound) => format!(
                "the bucket \"{}\" does not exist on node {}",
                remote.bucket, remote.node_id
            ),
            Self::Refused(SyncRefusal::Unauthorized | SyncRefusal::Forbidden) => {
                format!("access to bucket \"{}\" is forbidden", remote.bucket)
            }
            Self::Unreachable(reason) => {
                format!("node {} is unreachable: {reason}", remote.node_id)
            }
            other => other.to_string(),
        }
    }
}

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
            Ok(plan) => work |= plan.uploads > 0 || plan.truncated,
            Err(error) => {
                warn!(folder = %folder.folder_id, reason = %error, "Could not reconcile the folder")
            }
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
) -> Result<ReconcilePlan, ReconcileFolderError> {
    let mut stored = folder.clone();
    let result: Result<_, ReconcileFolderError> = async {
        let net_handle = context
            .net_handle
            .as_ref()
            .ok_or(ReconcileFolderError::Unavailable)?;
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
        .await?;
        stored.observed_files = sweep.files as u64;

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
                        stat: file.stat,
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
            let page = crate::driver::drive(ReconcileFolderOperation::new(page), context).await?;
            plan.absorb(page);
        }
        apply_downloads(context, folder, std::mem::take(&mut plan.downloads)).await;
        Ok((plan, view.next_cursor))
    }
    .await;

    let now_ms = unix_timestamp_millis();
    match result {
        Ok((plan, list_cursor)) => {
            stored.last_reconcile_ms = Some(now_ms);
            stored.last_error = None;
            stored.last_error_at_ms = None;
            stored.list_cursor = list_cursor;
            store_folder(context, &stored)
                .await
                .map_err(|_| ReconcileFolderError::Unavailable)?;
            if plan.uploads > 0 {
                arm_timer(context, TaskKey::DrainSyncUploadOutbox).await;
            }
            Ok(plan)
        }
        Err(error) => {
            stored.last_error = Some(error.describe(&folder.remote));
            stored.last_error_at_ms = Some(now_ms);
            if let Err(store_error) = store_folder(context, &stored).await {
                warn!(folder = %folder.folder_id, reason = %store_error, "Could not store the folder error");
            }
            Err(error)
        }
    }
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
/// so its boundary clears and the next pass starts over instead of repeating
/// exactly this window.
fn close_window(view: &mut RemoteView, next: Option<SyncListCursor>) {
    if next.is_none() {
        view.boundary = None;
    }
    view.next_cursor = next;
}

/// Every current head under the folder's bound prefix, as bounded pages served
/// by the folder's realm node with the owner's authority.
async fn fetch_heads(
    context: &Arc<DriverContext>,
    folder: &SyncedFolder,
) -> Result<RemoteView, ReconcileFolderError> {
    let realm_id = *context
        .net_handle
        .as_ref()
        .ok_or(ReconcileFolderError::Unavailable)?
        .realm_id();
    let auth = AuthContext {
        user_id: folder.created_by,
        realm_id,
        path_restrictions: None,
        session: None,
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
        let page = request_versions(
            context,
            folder.remote.node_id,
            auth.clone(),
            folder.remote.bucket.clone(),
            folder.remote.prefix.clone(),
            cursor,
            SyncPageLimit::default(),
        )
        .await?;
        let (page_heads, next) = page.into_parts();
        for head in page_heads {
            view.boundary = Some(head.relative.clone());
            view.heads.insert(head.relative.clone(), head);
        }
        // A folder past the in-memory bound stops here and resumes next pass;
        // the boundary keeps this pass from judging the keys it never saw.
        if view.heads.len() >= MAX_REMOTE_HEADS {
            close_window(&mut view, next);
            return Ok(view);
        }
        match next {
            Some(next) => cursor = Some(next),
            None => {
                close_window(&mut view, None);
                return Ok(view);
            }
        }
    }
}

pub(super) async fn request_versions(
    context: &Arc<DriverContext>,
    node_id: NodeId,
    auth: AuthContext,
    bucket: String,
    prefix: String,
    cursor: Option<SyncListCursor>,
    limit: SyncPageLimit,
) -> Result<SyncVersionPage, ReconcileFolderError> {
    let metadata = context
        .metadata_handle
        .as_ref()
        .ok_or(ReconcileFolderError::Unavailable)?;
    let reply = metadata
        .request_forwarded_write(
            node_id,
            MetadataTransportMessage::ForwardListVersions {
                auth_token: MetadataAuthToken::internal(auth),
                bucket,
                prefix,
                cursor,
                limit,
            },
        )
        .await
        .map_err(|error| ReconcileFolderError::Unreachable(error.to_string()))?;
    match reply {
        MetadataTransportMessage::ForwardedVersions { result: Ok(page) } => Ok(page),
        MetadataTransportMessage::ForwardedVersions {
            result: Err(refusal),
        } => Err(ReconcileFolderError::Refused(refusal)),
        other => Err(ReconcileFolderError::Unreachable(format!(
            "unexpected metadata response: {}",
            crate::metadata::transport_message_kind(&other)
        ))),
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

/// Re-arms the device timers. The folder pass is armed for a bound folder and
/// on every device, because it is also the beat a device fetches the realm-wide
/// documents on; a realm node with no folders schedules nothing.
pub async fn restore_sync_timers(context: &Arc<DriverContext>, task_handle: &TaskHandle) {
    let storage = &context.storage_handle;
    let due = outbox::has_rows(storage, aruna_core::keyspaces::SYNCED_FOLDER_KEYSPACE).await
        || local_is_device(context).await;
    if due
        && let TaskEvent::Error { message, .. } = task_handle
            .schedule_timer_if_idle(TaskKey::ReconcileSyncedFolders, Duration::ZERO)
            .await
    {
        warn!(message = %message, "Failed to restore the synced-folder timer");
    }
    restore_upload_timer(storage, task_handle).await;
}

/// Whether this node is a user device, which decides who keeps the folder beat.
async fn local_is_device(context: &Arc<DriverContext>) -> bool {
    let Some(net_handle) = context.net_handle.as_ref() else {
        return false;
    };
    crate::replication::bao_read::local_is_user(context, *net_handle.realm_id()).await
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
