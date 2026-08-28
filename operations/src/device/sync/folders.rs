//! Binding, listing and unbinding the folders this device syncs.

use std::sync::Arc;

use aruna_core::keyspaces::{
    SYNC_ACTION_LOG_KEYSPACE, SYNC_BASE_KEYSPACE, SYNC_UPLOAD_OUTBOX_KEYSPACE,
    SYNCED_FOLDER_KEYSPACE,
};
use aruna_core::structs::{
    AuthContext, EntryState, FolderMode, FolderState, RealmId, RemoteBinding, SyncActionRecord,
    SyncBase, SyncPageLimit, SyncRefusal, SyncedFolder,
};
use aruna_core::types::{GroupId, Key, NodeId, UserId};
use aruna_core::util::unix_timestamp_millis;
use thiserror::Error;
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::get_realm_config::GetRealmConfigOperation;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::staging::offered_directory::{
    OfferDirectoryInput, OfferedDirectoryError, WithdrawOfferInput, offer_directory, withdraw_offer,
};

use super::repository::{
    MAX_SYNCED_FOLDERS, SyncUpload, abort_txn, commit_txn, delete_rows, folder_entry, folder_key,
    key_path, read_value, scan_folder, scan_folders, scan_page, start_txn, write_rows,
};
use super::{ReconcileFolderError, request_versions};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BindFolderInput {
    /// Minted by the caller so it can check the derived bucket first.
    pub folder_id: Ulid,
    pub root: String,
    pub group_id: GroupId,
    pub remote: RemoteBinding,
    pub create_bucket: bool,
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
    #[error("node {0} is not a realm server")]
    NotRealmNode(NodeId),
    #[error("the bucket \"{bucket}\" does not exist on node {node}")]
    RemoteBucketMissing { node: NodeId, bucket: String },
    #[error("access to bucket \"{bucket}\" is forbidden")]
    RemoteForbidden { bucket: String },
    #[error("cannot bind bucket \"{bucket}\": {reason}")]
    RemoteBucketConflict { bucket: String, reason: String },
    #[error("node {node} is unreachable: {reason}")]
    RemoteUnreachable { node: NodeId, reason: String },
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
    let local_bucket = folder_bucket(input.folder_id);
    if bound
        .iter()
        .any(|folder| folder.local_bucket == local_bucket)
    {
        return Err(FolderError::BucketBound(local_bucket));
    }
    let config = drive(GetRealmConfigOperation::new(input.realm_id), context)
        .await
        .map_err(|_| FolderError::Unavailable)?;
    let eligible = config
        .sync_eligible_node_ids()
        .map_err(|_| FolderError::Unavailable)?;
    if !eligible.contains(&input.remote.node_id) {
        return Err(FolderError::NotRealmNode(input.remote.node_id));
    }
    let auth = AuthContext {
        user_id: input.user_id,
        realm_id: input.realm_id,
        path_restrictions: None,
    };
    if input.create_bucket {
        let metadata = context
            .metadata_handle
            .as_ref()
            .ok_or(FolderError::Unavailable)?;
        let creation = metadata
            .request_forwarded_write(
                input.remote.node_id,
                MetadataTransportMessage::ForwardCreateBucket {
                    auth_token: aruna_core::metadata::MetadataAuthToken::internal(auth.clone()),
                    bucket: input.remote.bucket.clone(),
                    group_id: input.group_id,
                },
            )
            .await
            .map_err(|error| ReconcileFolderError::Unreachable(error.to_string()));
        let creation = match creation {
            Ok(MetadataTransportMessage::ForwardedBucketCreated { result }) => {
                result.map_err(ReconcileFolderError::Refused)
            }
            Ok(other) => Err(ReconcileFolderError::Unreachable(format!(
                "unexpected metadata response: {}",
                crate::metadata::transport_message_kind(&other)
            ))),
            Err(error) => Err(error),
        };
        creation.map_err(|error| map_remote_error(error, &input.remote))?;
    }
    request_versions(
        context,
        input.remote.node_id,
        auth,
        input.remote.bucket.clone(),
        input.remote.prefix.clone(),
        None,
        SyncPageLimit::new(1),
    )
    .await
    .map_err(|error| map_remote_error(error, &input.remote))?;
    let sweep = offer_directory(
        context,
        OfferDirectoryInput {
            bucket: local_bucket.clone(),
            root: input.root.clone(),
            group_id: input.group_id,
            realm_id: input.realm_id,
            node_id: input.node_id,
            user_id: input.user_id,
        },
    )
    .await?;

    let folder = SyncedFolder {
        folder_id: input.folder_id,
        root: input.root,
        local_bucket,
        group_id: input.group_id,
        remote: input.remote,
        mode: input.mode,
        propagate_deletes: input.propagate_deletes,
        state: FolderState::Active,
        created_by: input.user_id,
        created_at_ms: unix_timestamp_millis(),
        last_reconcile_ms: None,
        last_error: None,
        last_error_at_ms: None,
        observed_files: sweep.files as u64,
        list_cursor: None,
    };
    store_folder(context, &folder).await?;
    Ok(folder)
}

fn map_remote_error(error: ReconcileFolderError, remote: &RemoteBinding) -> FolderError {
    match error {
        ReconcileFolderError::Refused(SyncRefusal::NotFound) => FolderError::RemoteBucketMissing {
            node: remote.node_id,
            bucket: remote.bucket.clone(),
        },
        ReconcileFolderError::Refused(SyncRefusal::Unauthorized | SyncRefusal::Forbidden) => {
            FolderError::RemoteForbidden {
                bucket: remote.bucket.clone(),
            }
        }
        ReconcileFolderError::Refused(SyncRefusal::Invalid(reason)) => {
            FolderError::RemoteBucketConflict {
                bucket: remote.bucket.clone(),
                reason,
            }
        }
        ReconcileFolderError::Unreachable(reason) => FolderError::RemoteUnreachable {
            node: remote.node_id,
            reason,
        },
        ReconcileFolderError::Refused(refusal) => FolderError::RemoteUnreachable {
            node: remote.node_id,
            reason: format!("{refusal:?}"),
        },
        ReconcileFolderError::Unavailable
        | ReconcileFolderError::Sweep(_)
        | ReconcileFolderError::Decide(_) => FolderError::Unavailable,
    }
}

/// The device-local bucket a folder is observed as. The owner never names it:
/// a derived name cannot collide with a bucket they already use.
pub fn folder_bucket(folder_id: Ulid) -> String {
    format!("folder-{}", folder_id.to_string().to_lowercase())
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
///
/// The binding is marked `Deleting` first and removed last, so a crash or a
/// failure in the middle leaves a folder that the next call resumes instead of
/// an orphaned offer, outbox row or audit log with nothing pointing at it.
pub async fn unbind_folder(
    context: &Arc<DriverContext>,
    folder_id: Ulid,
    realm_id: RealmId,
    node_id: NodeId,
    user_id: UserId,
) -> Result<usize, FolderError> {
    let folder = read_bound(context, folder_id).await?;
    if folder.state != FolderState::Deleting {
        store_folder(
            context,
            &SyncedFolder {
                state: FolderState::Deleting,
                ..folder.clone()
            },
        )
        .await?;
    }
    for key_space in [
        SYNC_BASE_KEYSPACE,
        SYNC_UPLOAD_OUTBOX_KEYSPACE,
        SYNC_ACTION_LOG_KEYSPACE,
    ] {
        clear_rows(context, key_space, folder_id).await?;
    }
    let removed = match withdraw_offer(
        context,
        WithdrawOfferInput {
            bucket: folder.local_bucket,
            realm_id,
            node_id,
            user_id,
        },
    )
    .await
    {
        Ok(removed) => removed,
        // A resumed unbind finds the registration already withdrawn and
        // finishes the rest instead of failing on it.
        Err(OfferedDirectoryError::NotOffered(_)) => 0,
        Err(error) => return Err(error.into()),
    };
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
    let current = read_bound(context, folder_id).await?;
    // A folder whose cleanup is running is on its way out, not resumable.
    if current.state == FolderState::Deleting {
        return Err(FolderError::NotFound);
    }
    let folder = SyncedFolder { state, ..current };
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

/// One entry of a folder, by its path.
pub async fn read_entry(
    context: &Arc<DriverContext>,
    folder_id: Ulid,
    relative: &str,
) -> Result<SyncBase, FolderError> {
    read_value(
        context,
        SYNC_BASE_KEYSPACE,
        super::repository::base_key(folder_id, relative),
        None,
    )
    .await
    .and_then(|bytes| SyncBase::from_bytes(&bytes).ok())
    .ok_or(FolderError::NotFound)
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
    use super::*;
    use aruna_core::structs::{EntrySide, SyncedBytes};
    use aruna_storage::FjallStorage;

    #[test]
    fn refuses_nested_roots() {
        // A folder inside another would observe the same file twice.
        assert!(overlaps("/home/ada/data", "/home/ada/data"));
        assert!(overlaps("/home/ada/data", "/home/ada/data/sub"));
        assert!(overlaps("/home/ada/data/sub", "/home/ada/data"));
        assert!(!overlaps("/home/ada/data", "/home/ada/database"));
        assert!(!overlaps("/home/ada/data", "/home/ada/other"));
    }

    fn realm() -> RealmId {
        RealmId::from_bytes([7u8; 32])
    }

    fn node() -> NodeId {
        iroh::SecretKey::from_bytes(&[3; 32]).public()
    }

    fn bound_folder(state: FolderState) -> SyncedFolder {
        SyncedFolder {
            folder_id: Ulid::from_bytes([1u8; 16]),
            root: "/home/ada/data".to_string(),
            local_bucket: "folder-x".to_string(),
            group_id: Ulid::from_bytes([2u8; 16]),
            remote: RemoteBinding {
                node_id: node(),
                bucket: "lab".to_string(),
                prefix: String::new(),
            },
            mode: FolderMode::TwoWay,
            propagate_deletes: true,
            state,
            created_by: UserId::new(Ulid::from_bytes([4u8; 16]), realm()),
            created_at_ms: 1,
            last_reconcile_ms: None,
            last_error: None,
            last_error_at_ms: None,
            observed_files: 0,
            list_cursor: None,
        }
    }

    fn base_row() -> SyncBase {
        SyncBase {
            synced: Some(SyncedBytes {
                fingerprint: "4-1-1-1".to_string(),
                blake3: [9u8; 32],
                size: 4,
                remote_version_id: Ulid::from_bytes([5u8; 16]),
            }),
            local_version_id: None,
            synced_at_ms: 1,
            entry: EntryState::InSync,
            pending_at: None,
            local: None::<EntrySide>,
            remote: None,
        }
    }

    async fn context() -> (tempfile::TempDir, Arc<DriverContext>) {
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        (
            dir,
            Arc::new(DriverContext {
                storage_handle: storage,
                net_handle: None,
                blob_handle: None,
                metadata_handle: None,
                task_handle: None,
                compute_handle: None,
            }),
        )
    }

    // An unbind interrupted after its state was persisted must finish on the
    // next call: the binding is the durable handle on the cleanup, so it may
    // never be missing while rows it owns are still there.
    #[tokio::test]
    async fn resumes_interrupted_unbind() {
        let (_dir, context) = context().await;
        let folder = bound_folder(FolderState::Deleting);
        store_folder(&context, &folder).await.expect("row stored");
        let row = super::super::repository::base_entry(folder.folder_id, "note.txt", &base_row())
            .expect("base encodes");
        assert!(super::super::repository::write_rows(&context, vec![row], None).await);

        let removed = unbind_folder(
            &context,
            folder.folder_id,
            realm(),
            node(),
            folder.created_by,
        )
        .await
        .expect("a resumed unbind finishes");

        assert_eq!(removed, 0);
        assert_eq!(
            read_bound(&context, folder.folder_id).await,
            Err(FolderError::NotFound)
        );
        let (entries, _) = list_entries(&context, folder.folder_id, None, None)
            .await
            .expect("the entries read");
        assert!(entries.is_empty());
    }

    // The binding survives until the cleanup is done, so a crash in the middle
    // leaves something to resume from rather than an orphaned offer.
    #[tokio::test]
    async fn marks_folder_deleting() {
        let (_dir, context) = context().await;
        let folder = bound_folder(FolderState::Active);
        store_folder(&context, &folder).await.expect("row stored");

        assert_eq!(
            set_folder_state(&context, folder.folder_id, FolderState::Paused)
                .await
                .expect("a bound folder pauses")
                .state,
            FolderState::Paused
        );
        store_folder(&context, &bound_folder(FolderState::Deleting))
            .await
            .expect("row stored");
        assert_eq!(
            set_folder_state(&context, folder.folder_id, FolderState::Active).await,
            Err(FolderError::NotFound)
        );
    }
}
