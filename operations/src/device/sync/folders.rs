//! Binds, lists and unbinds the realm folders this device keeps in sync, and creates realm buckets.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::UserId;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    SYNC_BASE_KEYSPACE, SYNC_LOG_KEYSPACE, SYNC_UPLOAD_KEYSPACE, SYNCED_FOLDER_KEYSPACE,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::{
    EntryState, FolderMode, FolderState, RemoteBinding, SyncActionRecord, SyncBase, SyncPageLimit,
    SyncRefusal, SyncedFolder,
};
use aruna_core::task::TaskKey;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{GroupId, Key, TxnId};
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::metadata::protocol::MetadataTransportMessage;
use crate::realm::get_config::GetConfigOperation;
use crate::staging::offered_directory::{
    OfferedDirectoryError, WithdrawOfferInput, check_root, withdraw_offer,
};

use super::repository::{
    MAX_SYNCED_FOLDERS, SyncUpload, abort_txn, commit_txn, delete_rows, folder_entry, folder_key,
    key_path, read_value, scan_folder, scan_folders, scan_page, start_txn, sync_all, write_rows,
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
    pub user_id: UserId,
}

/// A bucket the owner creates on a realm node without binding a folder to it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RealmBucketInput {
    pub bucket: String,
    pub group_id: GroupId,
    pub node_id: NodeId,
    pub realm_id: RealmId,
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
    #[error("the bucket \"{bucket}\" already exists on node {node}")]
    BucketExists { node: NodeId, bucket: String },
    #[error("the folder is still being bound")]
    StillBinding,
    #[error(transparent)]
    Offer(#[from] OfferedDirectoryError),
    #[error("a download-only folder never propagates deletions")]
    DownloadOnly,
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

/// Binds one directory to a realm bucket prefix and answers before the directory is walked:
/// the first reconcile pass, woken here, observes and publishes it in the background.
pub async fn bind_folder(
    context: &Arc<DriverContext>,
    input: BindFolderInput,
) -> Result<SyncedFolder, FolderError> {
    check_root(context, &input.root).await?;
    ensure_realm_node(context, input.realm_id, input.remote.node_id).await?;
    let folder = SyncedFolder {
        folder_id: input.folder_id,
        root: input.root,
        local_bucket: folder_bucket(input.folder_id),
        group_id: input.group_id,
        remote: input.remote,
        mode: input.mode,
        propagate_deletes: input.propagate_deletes,
        state: FolderState::Binding {
            create_bucket: input.create_bucket,
        },
        created_by: input.user_id,
        created_at_ms: unix_timestamp_millis(),
        last_reconcile_ms: None,
        last_error: None,
        last_error_ms: None,
        observed_files: 0,
        list_cursor: None,
    };
    reserve_binding(context, &folder).await?;
    // Detached, so a dropped request still records the outcome; a crash leaves it to the next pass.
    let context = context.clone();
    tokio::spawn(async move { finish_binding(&context, folder).await })
        .await
        .map_err(|_| FolderError::Unavailable)?
}

/// Admits `folder` and records it in one transaction. Its scan conflicts with any concurrent
/// reservation, so a retried request cannot bind the same root twice.
async fn reserve_binding(
    context: &Arc<DriverContext>,
    folder: &SyncedFolder,
) -> Result<(), FolderError> {
    let txn_id = start_txn(context).await.ok_or(FolderError::Unavailable)?;
    let admitted = match scan_bound(context, Some(txn_id)).await {
        Ok(bound) => admit(&bound, folder),
        Err(error) => Err(error),
    };
    let written = match admitted {
        Ok(()) => match folder_entry(folder) {
            Ok(row) => match write_rows(context, vec![row], Some(txn_id)).await {
                true => Ok(()),
                false => Err(FolderError::Unavailable),
            },
            Err(_) => Err(FolderError::Unavailable),
        },
        Err(error) => Err(error),
    };
    if let Err(error) = written {
        abort_txn(context, txn_id).await;
        return Err(error);
    }
    match commit_txn(context, txn_id).await {
        true => Ok(()),
        false => Err(FolderError::Unavailable),
    }
}

/// The count, overlap and bucket checks a new folder must pass against the bound ones.
fn admit(bound: &[SyncedFolder], folder: &SyncedFolder) -> Result<(), FolderError> {
    if bound.len() >= MAX_SYNCED_FOLDERS {
        return Err(FolderError::TooManyFolders(MAX_SYNCED_FOLDERS));
    }
    if let Some(existing) = bound
        .iter()
        .find(|bound| overlaps(&bound.root, &folder.root))
    {
        return Err(FolderError::RootOverlaps(existing.root.clone()));
    }
    if bound
        .iter()
        .any(|bound| bound.local_bucket == folder.local_bucket)
    {
        return Err(FolderError::BucketBound(folder.local_bucket.clone()));
    }
    Ok(())
}

/// Confirms the realm bucket of a binding in progress and records the outcome: an accepted
/// remote activates the folder, a refusal drops the binding, and a failure without an answer
/// keeps it for the next folder pass. Creating again is safe: the node accepts its own bucket.
pub(crate) async fn finish_binding(
    context: &Arc<DriverContext>,
    folder: SyncedFolder,
) -> Result<SyncedFolder, FolderError> {
    let _pass = super::folder_lock(folder.folder_id).await;
    // The caller's copy may be stale: a binding unbound or finished since is left alone.
    let folder = read_bound(context, folder.folder_id).await?;
    let FolderState::Binding { create_bucket } = folder.state else {
        return Ok(folder);
    };
    let auth = AuthContext {
        user_id: folder.created_by,
        realm_id: folder.created_by.realm_id,
        path_restrictions: None,
        session: None,
    };
    let remote = match create_bucket {
        // The binding must be on disk before the realm node may create a bucket for it.
        true if !sync_all(context).await => Err(FolderError::Unavailable),
        true => forward_create(context, auth, &folder.remote, folder.group_id).await,
        false => request_versions(
            context,
            folder.remote.node_id,
            auth,
            folder.remote.bucket.clone(),
            folder.remote.prefix.clone(),
            None,
            SyncPageLimit::new(1),
        )
        .await
        .map(|_| ())
        .map_err(|error| map_remote_error(error, &folder.remote)),
    };
    let binding = |current: &SyncedFolder| matches!(current.state, FolderState::Binding { .. });
    match remote {
        Ok(()) => {
            let active = update_folder(context, folder.folder_id, |current| {
                match binding(&current) {
                    true => Ok(SyncedFolder {
                        state: FolderState::Active,
                        last_error: None,
                        last_error_ms: None,
                        ..current
                    }),
                    false => Ok(current),
                }
            })
            .await?;
            crate::device::sync_status::arm(context, TaskKey::ReconcileSyncedFolders).await;
            Ok(active)
        }
        Err(
            error @ (FolderError::RemoteBucketMissing { .. }
            | FolderError::RemoteForbidden { .. }
            | FolderError::RemoteBucketConflict { .. }),
        ) => {
            drop_binding(context, folder.folder_id).await;
            Err(error)
        }
        Err(error) => {
            let reason = error.to_string();
            let noted = update_folder(context, folder.folder_id, |current| {
                match binding(&current) {
                    true => Ok(SyncedFolder {
                        last_error: Some(reason),
                        last_error_ms: Some(unix_timestamp_millis()),
                        ..current
                    }),
                    false => Ok(current),
                }
            })
            .await;
            if let Err(note_error) = noted {
                warn!(folder = %folder.folder_id, reason = %note_error, "Could not note the binding error");
            }
            Err(error)
        }
    }
}

/// Removes a binding the realm refused, unless it changed meanwhile.
async fn drop_binding(context: &Arc<DriverContext>, folder_id: Ulid) {
    let Some(txn_id) = start_txn(context).await else {
        return;
    };
    let key = folder_key(folder_id);
    let binding = read_value(context, SYNCED_FOLDER_KEYSPACE, key.clone(), Some(txn_id))
        .await
        .and_then(|bytes| SyncedFolder::from_bytes(&bytes).ok())
        .is_some_and(|folder| matches!(folder.state, FolderState::Binding { .. }));
    let row = vec![(SYNCED_FOLDER_KEYSPACE.to_string(), key)];
    match binding && delete_rows(context, row, Some(txn_id)).await {
        true if commit_txn(context, txn_id).await => {}
        true => warn!(folder = %folder_id, "Could not drop the refused binding"),
        false => abort_txn(context, txn_id).await,
    }
}

/// Refuses a node that cannot hold realm buckets, such as this computer.
async fn ensure_realm_node(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    node_id: NodeId,
) -> Result<(), FolderError> {
    let config = drive(GetConfigOperation::new(realm_id), context)
        .await
        .map_err(|_| FolderError::Unavailable)?;
    let eligible = config
        .sync_eligible_nodes()
        .map_err(|_| FolderError::Unavailable)?;
    match eligible.contains(&node_id) {
        true => Ok(()),
        false => Err(FolderError::NotRealmNode(node_id)),
    }
}

/// Creates the remote bucket through the node plane. The realm node accepts a bucket of the
/// same group that already exists.
async fn forward_create(
    context: &Arc<DriverContext>,
    auth: AuthContext,
    remote: &RemoteBinding,
    group_id: GroupId,
) -> Result<(), FolderError> {
    let metadata = context
        .metadata_handle
        .as_ref()
        .ok_or(FolderError::Unavailable)?;
    let creation = metadata
        .request_forwarded_write(
            remote.node_id,
            MetadataTransportMessage::ForwardCreateBucket {
                auth_token: aruna_core::metadata::AuthToken::internal(auth),
                bucket: remote.bucket.clone(),
                group_id,
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
    creation.map_err(|error| map_remote_error(error, remote))
}

/// Creates a bucket on a realm node for the owner without binding a folder to it. A bucket of
/// that name the node already holds, in any group, is refused, never adopted.
pub async fn create_realm_bucket(
    context: &Arc<DriverContext>,
    input: RealmBucketInput,
) -> Result<(), FolderError> {
    ensure_realm_node(context, input.realm_id, input.node_id).await?;
    let auth = AuthContext {
        user_id: input.user_id,
        realm_id: input.realm_id,
        path_restrictions: None,
        session: None,
    };
    let remote = RemoteBinding {
        node_id: input.node_id,
        bucket: input.bucket,
        prefix: String::new(),
    };
    let metadata = context
        .metadata_handle
        .as_ref()
        .ok_or(FolderError::Unavailable)?;
    // The node decides existence inside its create transaction, so concurrent creates cannot
    // both succeed.
    let reply = metadata
        .request_forwarded_write(
            remote.node_id,
            MetadataTransportMessage::ForwardCreateNewBucket {
                auth_token: aruna_core::metadata::AuthToken::internal(auth),
                bucket: remote.bucket.clone(),
                group_id: input.group_id,
            },
        )
        .await
        .map_err(|error| ReconcileFolderError::Unreachable(error.to_string()));
    let created = match reply {
        Ok(MetadataTransportMessage::ForwardedNewBucket { result }) => {
            result.map_err(ReconcileFolderError::Refused)
        }
        Ok(other) => Err(ReconcileFolderError::Unreachable(format!(
            "unexpected metadata response: {}",
            crate::metadata::transport_message_kind(&other)
        ))),
        Err(error) => Err(error),
    };
    match created.map_err(|error| map_remote_error(error, &remote))? {
        true => Ok(()),
        false => Err(FolderError::BucketExists {
            node: remote.node_id,
            bucket: remote.bucket,
        }),
    }
}

/// Wakes the background pass for a folder whose root is still a usable directory.
pub async fn request_sync(
    context: &Arc<DriverContext>,
    folder: &SyncedFolder,
) -> Result<(), FolderError> {
    check_root(context, &folder.root).await?;
    crate::device::sync_status::arm(context, TaskKey::ReconcileSyncedFolders).await;
    Ok(())
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
    scan_bound(context, None).await
}

async fn scan_bound(
    context: &Arc<DriverContext>,
    txn_id: Option<TxnId>,
) -> Result<Vec<SyncedFolder>, FolderError> {
    let mut folders = Vec::new();
    let mut cursor: Option<Key> = None;
    loop {
        let (values, next) = scan_page(context, scan_folders(cursor, txn_id))
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

/// Stops syncing one folder; the owner's filesystem is untouched. The folder is marked
/// `Deleting` and the folder pass, woken here, removes it, also after a cancelled call or a restart.
pub async fn unbind_folder(
    context: &Arc<DriverContext>,
    folder_id: Ulid,
) -> Result<(), FolderError> {
    let folder = read_bound(context, folder_id).await?;
    if folder.state != FolderState::Deleting {
        store_folder(
            context,
            &SyncedFolder {
                state: FolderState::Deleting,
                ..folder
            },
        )
        .await?;
    }
    crate::device::sync_status::arm(context, TaskKey::ReconcileSyncedFolders).await;
    Ok(())
}

/// Removes a `Deleting` folder under its lock, after any pass in flight. The row goes last, so an
/// interrupted cleanup resumes in the next pass. Answers the observations it tombstoned.
pub(crate) async fn finish_unbind(
    context: &Arc<DriverContext>,
    folder_id: Ulid,
    node_id: NodeId,
) -> Result<usize, FolderError> {
    let _pass = super::folder_lock(folder_id).await;
    let folder = read_bound(context, folder_id).await?;
    if folder.state != FolderState::Deleting {
        return Ok(0);
    }
    for key_space in [SYNC_BASE_KEYSPACE, SYNC_UPLOAD_KEYSPACE, SYNC_LOG_KEYSPACE] {
        clear_rows(context, key_space, folder_id).await?;
    }
    let removed = match withdraw_offer(
        context,
        WithdrawOfferInput {
            bucket: folder.local_bucket,
            realm_id: folder.created_by.realm_id,
            node_id,
            user_id: folder.created_by,
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
    // Pausing or resuming must not skip the realm bucket a binding still has to confirm.
    if matches!(current.state, FolderState::Binding { .. }) {
        return Err(FolderError::StillBinding);
    }
    let folder = SyncedFolder { state, ..current };
    store_folder(context, &folder).await?;
    Ok(folder)
}

/// Turns the propagation of local deletions on or off for one folder.
pub async fn set_propagate_deletes(
    context: &Arc<DriverContext>,
    folder_id: Ulid,
    propagate_deletes: bool,
) -> Result<SyncedFolder, FolderError> {
    update_folder(context, folder_id, |current| match current.state {
        FolderState::Deleting => Err(FolderError::NotFound),
        _ if propagate_deletes && current.mode == FolderMode::DownloadOnly => {
            Err(FolderError::DownloadOnly)
        }
        _ => Ok(SyncedFolder {
            propagate_deletes,
            ..current
        }),
    })
    .await
}

/// Stores what one reconcile pass learned onto the current row, so an owner
/// change made during the pass is kept and an unbound folder is not written back.
pub(crate) async fn store_progress(
    context: &Arc<DriverContext>,
    progress: &SyncedFolder,
) -> Result<SyncedFolder, FolderError> {
    update_folder(context, progress.folder_id, |current| {
        Ok(SyncedFolder {
            last_reconcile_ms: progress.last_reconcile_ms,
            last_error: progress.last_error.clone(),
            last_error_ms: progress.last_error_ms,
            observed_files: progress.observed_files,
            list_cursor: progress.list_cursor.clone(),
            ..current
        })
    })
    .await
}

/// Rewrites one folder row from its stored value in one transaction.
async fn update_folder(
    context: &Arc<DriverContext>,
    folder_id: Ulid,
    change: impl FnOnce(SyncedFolder) -> Result<SyncedFolder, FolderError>,
) -> Result<SyncedFolder, FolderError> {
    let txn_id = start_txn(context).await.ok_or(FolderError::Unavailable)?;
    let current = read_value(
        context,
        SYNCED_FOLDER_KEYSPACE,
        folder_key(folder_id),
        Some(txn_id),
    )
    .await
    .and_then(|bytes| SyncedFolder::from_bytes(&bytes).ok());
    let changed = current.ok_or(FolderError::NotFound).and_then(change);
    let written = match &changed {
        Ok(folder) => match folder_entry(folder) {
            Ok(row) => write_rows(context, vec![row], Some(txn_id)).await,
            Err(_) => false,
        },
        Err(_) => false,
    };
    if !written {
        abort_txn(context, txn_id).await;
        return changed.and(Err(FolderError::Unavailable));
    }
    match commit_txn(context, txn_id).await {
        true => changed,
        false => Err(FolderError::Unavailable),
    }
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
        scan_folder(SYNC_LOG_KEYSPACE, folder_id, cursor, None),
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
            scan_folder(SYNC_UPLOAD_KEYSPACE, folder_id, cursor, None),
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
                scan_folder(SYNC_UPLOAD_KEYSPACE, folder.folder_id, cursor, None),
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
    use crate::tests::device::context;
    use aruna_core::structs::{EntrySide, SyncedBytes};

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
            last_error_ms: None,
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

    // An unbind interrupted after its state was persisted must finish on the next
    // call: the binding is the durable handle on the cleanup while its rows remain.
    #[tokio::test]
    async fn resumes_interrupted_unbind() {
        let (_dir, context) = context().await;
        let context = Arc::new(context);
        let folder = bound_folder(FolderState::Deleting);
        store_folder(&context, &folder).await.expect("row stored");
        let row = super::super::repository::base_entry(folder.folder_id, "note.txt", &base_row())
            .expect("base encodes");
        assert!(super::super::repository::write_rows(&context, vec![row], None).await);

        let removed = finish_unbind(&context, folder.folder_id, node())
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

    // A reconcile pass that started before the owner's change keeps it, and a
    // folder unbound meanwhile is not written back.
    #[tokio::test]
    async fn progress_keeps_settings() {
        let (_dir, context) = context().await;
        let context = Arc::new(context);
        let mut pass = bound_folder(FolderState::Active);
        store_folder(&context, &pass).await.expect("row stored");

        set_propagate_deletes(&context, pass.folder_id, false)
            .await
            .expect("the toggle applies");
        pass.last_reconcile_ms = Some(5);
        let stored = store_progress(&context, &pass)
            .await
            .expect("progress stored");
        assert!(!stored.propagate_deletes);
        assert_eq!(stored.last_reconcile_ms, Some(5));

        assert!(
            delete_rows(
                &context,
                vec![(
                    SYNCED_FOLDER_KEYSPACE.to_string(),
                    folder_key(pass.folder_id)
                )],
                None
            )
            .await
        );
        assert_eq!(
            store_progress(&context, &pass).await,
            Err(FolderError::NotFound)
        );
        assert_eq!(
            read_bound(&context, pass.folder_id).await,
            Err(FolderError::NotFound)
        );
    }

    #[tokio::test]
    async fn download_refuses_deletes() {
        let (_dir, context) = context().await;
        let context = Arc::new(context);
        let mut folder = bound_folder(FolderState::Active);
        folder.mode = FolderMode::DownloadOnly;
        folder.propagate_deletes = false;
        store_folder(&context, &folder).await.expect("row stored");
        assert_eq!(
            set_propagate_deletes(&context, folder.folder_id, true).await,
            Err(FolderError::DownloadOnly)
        );
        assert!(
            !read_bound(&context, folder.folder_id)
                .await
                .expect("still bound")
                .propagate_deletes
        );
    }

    // The binding survives until the cleanup is done, so a crash in the middle
    // leaves something to resume from rather than an orphaned offer.
    #[tokio::test]
    async fn marks_folder_deleting() {
        let (_dir, context) = context().await;
        let context = Arc::new(context);
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

    // The removal of an unbound folder waits for the pass in flight on it, so rows that pass still
    // writes are cleared too instead of outliving the folder.
    #[tokio::test]
    async fn unbind_awaits_pass() {
        let (_dir, context) = context().await;
        let context = Arc::new(context);
        let folder = bound_folder(FolderState::Active);
        store_folder(&context, &folder).await.expect("row stored");

        unbind_folder(&context, folder.folder_id)
            .await
            .expect("the folder is marked");
        let pass = super::super::folder_lock(folder.folder_id).await;
        let unbind = tokio::spawn({
            let context = context.clone();
            let folder_id = folder.folder_id;
            async move { finish_unbind(&context, folder_id, node()).await }
        });
        let row = super::super::repository::base_entry(folder.folder_id, "note.txt", &base_row())
            .expect("base encodes");
        assert!(super::super::repository::write_rows(&context, vec![row], None).await);
        drop(pass);

        assert_eq!(unbind.await.expect("the unbind task ends"), Ok(0));
        let (entries, _) = list_entries(&context, folder.folder_id, None, None)
            .await
            .expect("the entries read");
        assert!(entries.is_empty());
        assert_eq!(
            read_bound(&context, folder.folder_id).await,
            Err(FolderError::NotFound)
        );
    }

    // A pass that listed a binding before its unbind must not ask the realm node for a bucket once
    // the folder is deleting or gone. This context reaches no realm node, so a request would fail.
    #[tokio::test]
    async fn skips_unbound_binding() {
        let (_dir, context) = context().await;
        let context = Arc::new(context);
        let listed = bound_folder(FolderState::Binding {
            create_bucket: true,
        });
        let deleting = SyncedFolder {
            state: FolderState::Deleting,
            ..listed.clone()
        };
        store_folder(&context, &deleting).await.expect("row stored");
        assert_eq!(finish_binding(&context, listed.clone()).await, Ok(deleting));

        let row = vec![(
            SYNCED_FOLDER_KEYSPACE.to_string(),
            folder_key(listed.folder_id),
        )];
        assert!(delete_rows(&context, row, None).await);
        assert_eq!(
            finish_binding(&context, listed).await,
            Err(FolderError::NotFound)
        );
    }

    // Two reservations of one root race in storage; the transaction lets exactly one through,
    // and a binding cannot be paused past its realm bucket.
    #[tokio::test]
    async fn reserves_one_root() {
        let (_dir, context) = context().await;
        let context = Arc::new(context);
        let binding = FolderState::Binding {
            create_bucket: true,
        };
        let first = bound_folder(binding.clone());
        let second = SyncedFolder {
            folder_id: Ulid::from_bytes([6u8; 16]),
            local_bucket: "folder-y".to_string(),
            ..bound_folder(binding)
        };

        let (left, right) = tokio::join!(
            reserve_binding(&context, &first),
            reserve_binding(&context, &second)
        );
        assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
        let bound = list_folders(&context).await.expect("the folders list");
        assert_eq!(bound.len(), 1);
        assert_eq!(
            reserve_binding(&context, &first).await,
            Err(FolderError::RootOverlaps(first.root.clone()))
        );
        assert_eq!(
            set_folder_state(&context, bound[0].folder_id, FolderState::Paused).await,
            Err(FolderError::StillBinding)
        );
    }
}
