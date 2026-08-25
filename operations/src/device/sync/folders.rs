//! Binding, listing and unbinding the folders this device syncs.

use std::sync::Arc;

use aruna_core::keyspaces::{
    SYNC_ACTION_LOG_KEYSPACE, SYNC_BASE_KEYSPACE, SYNC_UPLOAD_OUTBOX_KEYSPACE,
    SYNCED_FOLDER_KEYSPACE,
};
use aruna_core::structs::{
    EntryState, FolderMode, FolderState, RealmId, RemoteBinding, SyncActionRecord, SyncBase,
    SyncedFolder,
};
use aruna_core::types::{GroupId, Key, NodeId, UserId};
use aruna_core::util::unix_timestamp_millis;
use thiserror::Error;
use ulid::Ulid;

use crate::driver::DriverContext;
use crate::staging::offered_directory::{
    OfferDirectoryInput, OfferedDirectoryError, WithdrawOfferInput, offer_directory, withdraw_offer,
};

use super::repository::{
    MAX_SYNCED_FOLDERS, SyncUpload, abort_txn, commit_txn, delete_rows, folder_entry, folder_key,
    key_path, read_value, scan_folder, scan_folders, scan_page, start_txn, write_rows,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BindFolderInput {
    pub root: String,
    pub local_bucket: String,
    pub group_id: GroupId,
    pub remote: RemoteBinding,
    pub mode: FolderMode,
    pub propagate_deletes: bool,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub user_id: UserId,
}

#[derive(Debug, Error, PartialEq)]
pub enum FolderError {
    #[error("this device already binds the maximum of {0} folders")]
    TooManyFolders(usize),
    #[error("`{0}` overlaps a folder this device already binds")]
    RootOverlaps(String),
    #[error("bucket `{0}` is already bound to another folder")]
    BucketBound(String),
    #[error("no such synced folder")]
    NotFound,
    #[error("the device store is unavailable")]
    Unavailable,
    #[error(transparent)]
    Offer(#[from] OfferedDirectoryError),
}

/// How much of a folder still needs attention. Every counter is derived from
/// the base rows, so it always describes what the owner would see.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FolderCounters {
    pub in_sync: usize,
    pub uploading: usize,
    pub conflicts: usize,
    pub pending_replacements: usize,
    pub remote_deleted: usize,
    pub errors: usize,
}

/// Binds one directory to a realm bucket prefix and takes the first
/// observation of it. The bucket becomes this device's read-only view of the
/// directory, exactly as an offered directory is.
pub async fn bind_folder(
    context: &Arc<DriverContext>,
    input: BindFolderInput,
) -> Result<SyncedFolder, FolderError> {
    let bound = list_folders(context).await?;
    if bound.len() >= MAX_SYNCED_FOLDERS {
        return Err(FolderError::TooManyFolders(MAX_SYNCED_FOLDERS));
    }
    if let Some(existing) = bound
        .iter()
        .find(|folder| overlaps(&folder.root, &input.root))
    {
        return Err(FolderError::RootOverlaps(existing.root.clone()));
    }
    if bound
        .iter()
        .any(|folder| folder.local_bucket == input.local_bucket)
    {
        return Err(FolderError::BucketBound(input.local_bucket));
    }
    offer_directory(
        context,
        OfferDirectoryInput {
            bucket: input.local_bucket.clone(),
            root: input.root.clone(),
            group_id: input.group_id,
            realm_id: input.realm_id,
            node_id: input.node_id,
            user_id: input.user_id,
        },
    )
    .await?;

    let folder = SyncedFolder {
        folder_id: Ulid::generate(),
        root: input.root,
        local_bucket: input.local_bucket,
        group_id: input.group_id,
        remote: input.remote,
        mode: input.mode,
        propagate_deletes: input.propagate_deletes,
        state: FolderState::Active,
        created_by: input.user_id,
        created_at_ms: unix_timestamp_millis(),
        last_reconcile_ms: None,
    };
    store_folder(context, &folder).await?;
    Ok(folder)
}

/// Whether two roots are the same directory or one contains the other. The
/// comparison is textual: a folder is bound by the path the owner gave.
fn overlaps(left: &str, right: &str) -> bool {
    let left = left.trim_end_matches('/');
    let right = right.trim_end_matches('/');
    left == right
        || right.starts_with(&format!("{left}/"))
        || left.starts_with(&format!("{right}/"))
}

pub async fn list_folders(context: &Arc<DriverContext>) -> Result<Vec<SyncedFolder>, FolderError> {
    let mut folders = Vec::new();
    let mut cursor: Option<Key> = None;
    loop {
        let (values, next) = scan_page(context, scan_folders(cursor, None))
            .await
            .ok_or(FolderError::Unavailable)?;
        folders.extend(
            values
                .into_iter()
                .filter_map(|(_, bytes)| SyncedFolder::from_bytes(&bytes).ok()),
        );
        match next {
            Some(next) => cursor = Some(next),
            None => return Ok(folders),
        }
    }
}

pub async fn read_bound(
    context: &Arc<DriverContext>,
    folder_id: Ulid,
) -> Result<SyncedFolder, FolderError> {
    read_value(context, SYNCED_FOLDER_KEYSPACE, folder_key(folder_id), None)
        .await
        .and_then(|bytes| SyncedFolder::from_bytes(&bytes).ok())
        .ok_or(FolderError::NotFound)
}

/// Stops syncing one folder. Nothing on the owner's filesystem is touched: the
/// device only withdraws what it published about the directory.
pub async fn unbind_folder(
    context: &Arc<DriverContext>,
    folder_id: Ulid,
    realm_id: RealmId,
    node_id: NodeId,
    user_id: UserId,
) -> Result<usize, FolderError> {
    let folder = read_bound(context, folder_id).await?;
    let Some(txn_id) = start_txn(context).await else {
        return Err(FolderError::Unavailable);
    };
    let dropped = delete_rows(
        context,
        vec![(SYNCED_FOLDER_KEYSPACE.to_string(), folder_key(folder_id))],
        Some(txn_id),
    )
    .await
        && commit_txn(context, txn_id).await;
    if !dropped {
        abort_txn(context, txn_id).await;
        return Err(FolderError::Unavailable);
    }
    for key_space in [
        SYNC_BASE_KEYSPACE,
        SYNC_UPLOAD_OUTBOX_KEYSPACE,
        SYNC_ACTION_LOG_KEYSPACE,
    ] {
        clear_rows(context, key_space, folder_id).await?;
    }
    let removed = withdraw_offer(
        context,
        WithdrawOfferInput {
            bucket: folder.local_bucket,
            realm_id,
            node_id,
            user_id,
        },
    )
    .await?;
    Ok(removed)
}

/// Deletes every row one folder owns in a keyspace, page by page.
async fn clear_rows(
    context: &Arc<DriverContext>,
    key_space: &str,
    folder_id: Ulid,
) -> Result<(), FolderError> {
    loop {
        let (values, _) = scan_page(context, scan_folder(key_space, folder_id, None, None))
            .await
            .ok_or(FolderError::Unavailable)?;
        if values.is_empty() {
            return Ok(());
        }
        let deletes = values
            .into_iter()
            .map(|(key, _)| (key_space.to_string(), key))
            .collect();
        if !delete_rows(context, deletes, None).await {
            return Err(FolderError::Unavailable);
        }
    }
}

pub async fn set_folder_state(
    context: &Arc<DriverContext>,
    folder_id: Ulid,
    state: FolderState,
) -> Result<SyncedFolder, FolderError> {
    let folder = SyncedFolder {
        state,
        ..read_bound(context, folder_id).await?
    };
    store_folder(context, &folder).await?;
    Ok(folder)
}

pub(crate) async fn store_folder(
    context: &Arc<DriverContext>,
    folder: &SyncedFolder,
) -> Result<(), FolderError> {
    let row = folder_entry(folder).map_err(|_| FolderError::Unavailable)?;
    match write_rows(context, vec![row], None).await {
        true => Ok(()),
        false => Err(FolderError::Unavailable),
    }
}

/// One page of a folder's entries, filtered by state name when asked.
pub async fn list_entries(
    context: &Arc<DriverContext>,
    folder_id: Ulid,
    state: Option<&str>,
    cursor: Option<Key>,
) -> Result<(Vec<(String, SyncBase)>, Option<Key>), FolderError> {
    let (values, next) = scan_page(
        context,
        scan_folder(SYNC_BASE_KEYSPACE, folder_id, cursor, None),
    )
    .await
    .ok_or(FolderError::Unavailable)?;
    let entries = values
        .into_iter()
        .filter_map(|(key, bytes)| {
            let base = SyncBase::from_bytes(&bytes).ok()?;
            Some((key_path(key.as_ref())?, base))
        })
        .filter(|(_, base)| state.is_none_or(|state| base.entry.name() == state))
        .collect();
    Ok((entries, next))
}

/// One page of a folder's audit log, oldest action first.
pub async fn list_actions(
    context: &Arc<DriverContext>,
    folder_id: Ulid,
    cursor: Option<Key>,
) -> Result<(Vec<SyncActionRecord>, Option<Key>), FolderError> {
    let (values, next) = scan_page(
        context,
        scan_folder(SYNC_ACTION_LOG_KEYSPACE, folder_id, cursor, None),
    )
    .await
    .ok_or(FolderError::Unavailable)?;
    Ok((
        values
            .into_iter()
            .filter_map(|(_, bytes)| SyncActionRecord::from_bytes(&bytes).ok())
            .collect(),
        next,
    ))
}

/// Counts one folder's entries by state, and the uploads still queued for it.
pub async fn folder_counters(
    context: &Arc<DriverContext>,
    folder_id: Ulid,
) -> Result<FolderCounters, FolderError> {
    let mut counters = FolderCounters::default();
    let mut cursor: Option<Key> = None;
    loop {
        let (values, next) = scan_page(
            context,
            scan_folder(SYNC_BASE_KEYSPACE, folder_id, cursor, None),
        )
        .await
        .ok_or(FolderError::Unavailable)?;
        for (_, bytes) in values {
            let Ok(base) = SyncBase::from_bytes(&bytes) else {
                continue;
            };
            match base.entry {
                EntryState::InSync => counters.in_sync += 1,
                EntryState::Conflict { .. } => counters.conflicts += 1,
                EntryState::PendingReplace { .. } => counters.pending_replacements += 1,
                EntryState::RemoteDeleted { .. } => counters.remote_deleted += 1,
                EntryState::Error { .. } => counters.errors += 1,
                _ => {}
            }
        }
        match next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    counters.uploading = count_uploads(context, folder_id).await?;
    Ok(counters)
}

async fn count_uploads(
    context: &Arc<DriverContext>,
    folder_id: Ulid,
) -> Result<usize, FolderError> {
    let mut cursor: Option<Key> = None;
    let mut queued = 0usize;
    loop {
        let (values, next) = scan_page(
            context,
            scan_folder(SYNC_UPLOAD_OUTBOX_KEYSPACE, folder_id, cursor, None),
        )
        .await
        .ok_or(FolderError::Unavailable)?;
        queued += values.len();
        match next {
            Some(next) => cursor = Some(next),
            None => return Ok(queued),
        }
    }
}

/// Every queued upload of this device, newest folder last.
pub async fn list_transfers(context: &Arc<DriverContext>) -> Result<Vec<SyncUpload>, FolderError> {
    let mut uploads = Vec::new();
    for folder in list_folders(context).await? {
        let mut cursor: Option<Key> = None;
        loop {
            let (values, next) = scan_page(
                context,
                scan_folder(SYNC_UPLOAD_OUTBOX_KEYSPACE, folder.folder_id, cursor, None),
            )
            .await
            .ok_or(FolderError::Unavailable)?;
            uploads.extend(
                values
                    .into_iter()
                    .filter_map(|(_, bytes)| SyncUpload::from_bytes(&bytes).ok()),
            );
            match next {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
    }
    Ok(uploads)
}

#[cfg(test)]
mod tests {
    use super::overlaps;

    #[test]
    fn refuses_nested_roots() {
        // A folder inside another would observe the same file twice.
        assert!(overlaps("/home/ada/data", "/home/ada/data"));
        assert!(overlaps("/home/ada/data", "/home/ada/data/sub"));
        assert!(overlaps("/home/ada/data/sub", "/home/ada/data"));
        assert!(!overlaps("/home/ada/data", "/home/ada/database"));
        assert!(!overlaps("/home/ada/data", "/home/ada/other"));
    }
}
