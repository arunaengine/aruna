//! Binds, lists and unbinds the realm folders this device keeps in sync, and creates realm buckets.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod bind;
pub mod reads;
pub mod unbind;

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::SYNCED_FOLDER_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{done, read_row, value};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::{FolderMode, FolderState, RemoteBinding, SyncedFolder};
use aruna_core::types::GroupId;
use thiserror::Error;
use ulid::Ulid;

use crate::s3::bucket::delete::DeleteBucketError;
use crate::s3::multipart::abort::AbortUploadError;
use crate::s3::multipart::uploads::ListUploadsError;
use crate::s3::object::delete::DeleteObjectError;
use crate::s3::object::versions::ListVersionsError;
use crate::staging::offered_directory::OfferedDirectoryError;

use super::repository::{folder_entry, folder_key};
pub use bind::{
    BindFolderOperation, CreateRealmOperation, FinishBindingOperation, RequestSyncOperation,
    bind_folder, create_realm_bucket, finish_binding, request_sync,
};
pub use reads::{
    folder_counters, list_actions, list_entries, list_folders, list_transfers, read_bound,
    read_entry,
};
pub use unbind::{FinishUnbindOperation, UnbindFolderOperation, finish_unbind, unbind_folder};

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
    #[error(transparent)]
    ListVersions(#[from] ListVersionsError),
    #[error(transparent)]
    DeleteObject(#[from] DeleteObjectError),
    #[error(transparent)]
    DeleteBucket(#[from] DeleteBucketError),
    #[error(transparent)]
    ListUploads(#[from] ListUploadsError),
    #[error(transparent)]
    AbortUpload(#[from] AbortUploadError),
}

impl From<IoError> for FolderError {
    fn from(_: IoError) -> Self {
        Self::Unavailable
    }
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

/// One change to a folder row, decided on the row as stored.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum FolderChange {
    /// A binding the realm accepted becomes active.
    Activate,
    /// A binding keeps the reason its remote could not be confirmed.
    NoteError {
        reason: String,
        at_ms: u64,
    },
    Deleting,
    State(FolderState),
    Propagate(bool),
    /// What one reconcile pass learned; an owner change made during the pass is kept.
    Progress(Box<SyncedFolder>),
}

impl FolderChange {
    fn apply(self, current: SyncedFolder) -> Result<SyncedFolder, FolderError> {
        let binding = matches!(current.state, FolderState::Binding { .. });
        Ok(match self {
            Self::Activate if binding => SyncedFolder {
                state: FolderState::Active,
                last_error: None,
                last_error_ms: None,
                ..current
            },
            Self::NoteError { reason, at_ms } if binding => SyncedFolder {
                last_error: Some(reason),
                last_error_ms: Some(at_ms),
                ..current
            },
            Self::Activate | Self::NoteError { .. } => current,
            Self::Deleting => SyncedFolder {
                state: FolderState::Deleting,
                ..current
            },
            Self::State(state) => match current.state {
                // A folder whose cleanup is running is on its way out, not resumable.
                FolderState::Deleting => return Err(FolderError::NotFound),
                // Pausing or resuming must not skip the bucket a binding still has to confirm.
                FolderState::Binding { .. } => return Err(FolderError::StillBinding),
                _ => SyncedFolder { state, ..current },
            },
            Self::Propagate(propagate_deletes) => match current.state {
                FolderState::Deleting => return Err(FolderError::NotFound),
                _ if propagate_deletes && current.mode == FolderMode::DownloadOnly => {
                    return Err(FolderError::DownloadOnly);
                }
                _ => SyncedFolder {
                    propagate_deletes,
                    ..current
                },
            },
            Self::Progress(progress) => SyncedFolder {
                last_reconcile_ms: progress.last_reconcile_ms,
                last_error: progress.last_error,
                last_error_ms: progress.last_error_ms,
                observed_files: progress.observed_files,
                list_cursor: progress.list_cursor,
                ..current
            },
        })
    }
}

/// Rewrites one folder row from its stored value in one transaction.
#[derive(Debug, PartialEq)]
pub struct UpdateFolderOperation {
    folder_id: Ulid,
    change: Option<FolderChange>,
    changed: Option<SyncedFolder>,
}

impl UpdateFolderOperation {
    pub(super) fn new(folder_id: Ulid, change: FolderChange) -> Self {
        Self {
            folder_id,
            change: Some(change),
            changed: None,
        }
    }
}

impl Operation for UpdateFolderOperation {
    type Output = SyncedFolder;
    type Error = FolderError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<SyncedFolder>, FolderError> {
        let key = folder_key(self.folder_id);
        Ok(Next::Call(read_row(SYNCED_FOLDER_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<SyncedFolder>, FolderError> {
        if let Some(changed) = self.changed.take() {
            done(result)?;
            return Ok(Next::Done(changed));
        }
        let row = value(result)?;
        let current = row.and_then(|row| SyncedFolder::from_bytes(&row).ok());
        let current = current.ok_or(FolderError::NotFound)?;
        let change = self.change.take().ok_or(IoError::Unfinished)?;
        let changed = change.apply(current)?;
        let row = folder_entry(&changed).map_err(|_| FolderError::Unavailable)?;
        self.changed = Some(changed);
        Ok(Next::Call(IoCall::Write(vec![row])))
    }
}

/// Pauses or resumes one folder.
pub fn set_folder_state(folder_id: Ulid, state: FolderState) -> UpdateFolderOperation {
    UpdateFolderOperation::new(folder_id, FolderChange::State(state))
}

/// Turns the propagation of local deletions on or off for one folder.
pub fn set_propagate_deletes(folder_id: Ulid, propagate: bool) -> UpdateFolderOperation {
    UpdateFolderOperation::new(folder_id, FolderChange::Propagate(propagate))
}

/// Stores what one reconcile pass learned onto the current row, so an owner change made during
/// the pass is kept and an unbound folder is not written back.
pub(super) fn store_progress(progress: &SyncedFolder) -> UpdateFolderOperation {
    let change = FolderChange::Progress(Box::new(progress.clone()));
    UpdateFolderOperation::new(progress.folder_id, change)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::super::folder_gate;
    use super::super::repository::folder_bucket;
    use super::bind::ReserveBindingOperation;
    use super::unbind::{ClearRowsOperation, DeleteLocalOperation};
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::s3::multipart::uploads::{ListUploadsInput, ListUploadsOperation};
    use crate::s3::object::versions::{ListVersionsInput, ListVersionsOperation};
    use crate::staging::offered_directory::{WithdrawOfferInput, withdraw_offer};
    use crate::tests::device::{context, delete_rows, store_folder, write_rows};
    use aruna_core::keyspaces::{SYNC_BASE_KEYSPACE, UPLOAD_KEYSPACE};
    use aruna_core::structs::storage::multipart::{MultipartUpload, MultipartUploadStatus};
    use aruna_core::structs::{EntrySide, EntryState, SyncBase, SyncedBytes};

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
        assert!(write_rows(&context, vec![row], None).await);

        drive(
            finish_unbind(context.node_and_realm(), folder.folder_id, node()),
            &context,
        )
        .await
        .expect("a resumed unbind finishes");

        assert_eq!(
            drive(read_bound(folder.folder_id), &context).await,
            Err(FolderError::NotFound)
        );
        let (entries, _) = drive(list_entries(folder.folder_id, None, None), &context)
            .await
            .expect("the entries read");
        assert!(entries.is_empty());
    }

    // Rows that keep arriving cannot hold the folder lock: a pass stops at its limit, and the
    // next pass deletes the rest.
    #[tokio::test]
    async fn limits_row_cleanup() {
        use super::super::repository::base_entry;

        let (_dir, context) = context().await;
        let context = Arc::new(context);
        let folder_id = Ulid::generate();
        let rows = ["a.txt", "b.txt"]
            .map(|relative| base_entry(folder_id, relative, &base_row()).expect("base encodes"));
        assert!(write_rows(&context, rows.to_vec(), None).await);

        assert_eq!(
            drive(
                ClearRowsOperation::new(SYNC_BASE_KEYSPACE, folder_id, 1),
                &context
            )
            .await,
            Err(FolderError::Unavailable)
        );
        let (entries, _) = drive(list_entries(folder_id, None, None), &context)
            .await
            .expect("the entries read");
        assert_eq!(entries.len(), 1);
        assert_eq!(
            drive(
                ClearRowsOperation::new(SYNC_BASE_KEYSPACE, folder_id, 1),
                &context
            )
            .await,
            Ok(())
        );
        let (entries, _) = drive(list_entries(folder_id, None, None), &context)
            .await
            .expect("the entries read");
        assert!(entries.is_empty());
    }

    // An owner action on a folder whose cleanup is running writes no audit row for it to remove.
    #[tokio::test]
    async fn actions_refuse_deleting() {
        use super::super::actions::{ActionError, ApplyActionInput, apply_action};
        use aruna_core::structs::{ActionKind, ActionScope};

        let (_dir, context) = context().await;
        let context = Arc::new(context);
        let folder = SyncedFolder {
            folder_id: Ulid::generate(),
            ..bound_folder(FolderState::Deleting)
        };
        store_folder(&context, &folder).await.expect("row stored");
        let input = ApplyActionInput {
            folder_id: folder.folder_id,
            kind: ActionKind::KeepLocal,
            scope: ActionScope::AllPending,
            expected: None,
            actor: folder.created_by,
        };

        assert!(matches!(
            drive(apply_action(context.node_and_realm(), input), &context).await,
            Err(ActionError::Folder(FolderError::NotFound))
        ));
        let (actions, _) = drive(list_actions(folder.folder_id, None), &context)
            .await
            .expect("the log reads");
        assert!(actions.is_empty());
    }

    /// A deleting folder whose bucket offers one file, and an upload opened in that bucket.
    async fn offered_folder(
        context: &Arc<DriverContext>,
    ) -> (tempfile::TempDir, SyncedFolder, MultipartUpload) {
        use crate::s3::multipart::create::{CreateMultipartInput, CreateMultipartOperation};
        use crate::staging::offered_directory::{OfferDirectoryInput, offer_directory};
        use aruna_core::structs::storage::routing::RoutingSnapshot;

        let root = tempfile::tempdir().expect("root created");
        std::fs::write(root.path().join("note.txt"), b"note").expect("file written");
        let folder_id = Ulid::generate();
        let folder = SyncedFolder {
            folder_id,
            root: root.path().to_string_lossy().to_string(),
            local_bucket: folder_bucket(folder_id),
            ..bound_folder(FolderState::Deleting)
        };
        let offer = OfferDirectoryInput {
            bucket: folder.local_bucket.clone(),
            root: folder.root.clone(),
            group_id: folder.group_id,
            realm_id: realm(),
            node_id: node(),
            user_id: folder.created_by,
        };
        drive(offer_directory(offer), context)
            .await
            .expect("offered");
        let upload = CreateMultipartOperation::new(CreateMultipartInput {
            bucket: folder.local_bucket.clone(),
            key: "late.bin".to_string(),
            group_id: folder.group_id,
            created_by: folder.created_by,
            checksum_hint: None,
            routing: RoutingSnapshot::single(folder.group_id),
        });
        let upload = drive(upload, context).await.expect("upload opened").record;
        store_folder(context, &folder).await.expect("row stored");
        (root, folder, upload)
    }

    async fn store_upload(context: &Arc<DriverContext>, upload: &MultipartUpload) {
        let key = upload.upload_id.to_bytes().to_vec().into();
        let value = upload.to_bytes().expect("upload encodes").into();
        let row = (UPLOAD_KEYSPACE.to_string(), key, value);
        assert!(write_rows(context, vec![row], None).await);
    }

    async fn bucket_gone(context: &Arc<DriverContext>, bucket: &str) -> bool {
        use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
        let bucket = GetBucketOperation::new(bucket.to_string());
        matches!(drive(bucket, context).await, Err(GetBucketError::NotFound))
    }

    // A bucket left without its offer, with an upload in it, is still deleted. A cleanup cut short
    // after the bucket delete finds it gone and finishes.
    #[tokio::test]
    async fn deletes_local_bucket() {
        let fixture = crate::tests::staging::setup_driver_context().await;
        let context = Arc::new(fixture.driver_context);
        let (_root, folder, _) = offered_folder(&context).await;
        let withdrawal = WithdrawOfferInput {
            bucket: folder.local_bucket.clone(),
            realm_id: realm(),
            node_id: node(),
            user_id: folder.created_by,
        };
        assert_eq!(drive(withdraw_offer(withdrawal), &context).await, Ok(1));

        for attempt in ["withdrawn offer", "deleted bucket"] {
            store_folder(&context, &folder).await.expect("row stored");
            assert_eq!(
                drive(
                    finish_unbind(context.node_and_realm(), folder.folder_id, node()),
                    &context
                )
                .await,
                Ok(()),
                "{attempt}"
            );
            assert_eq!(
                drive(read_bound(folder.folder_id), &context).await,
                Err(FolderError::NotFound)
            );
            assert!(
                bucket_gone(&context, &folder.local_bucket).await,
                "{attempt}"
            );
        }
    }

    // A pass stops at its limit, so writes that keep arriving cannot hold the folder lock. The
    // leftover upload and version fail the bucket delete, and the next pass deletes the bucket.
    #[tokio::test]
    async fn limits_cleanup_pass() {
        use crate::s3::multipart::create::{CreateMultipartInput, CreateMultipartOperation};
        use crate::staging::offered_directory::{OfferDirectoryInput, offer_directory};
        use aruna_core::structs::storage::routing::RoutingSnapshot;

        let fixture = crate::tests::staging::setup_driver_context().await;
        let context = Arc::new(fixture.driver_context);
        let (_root, folder, _) = offered_folder(&context).await;
        let second = CreateMultipartOperation::new(CreateMultipartInput {
            bucket: folder.local_bucket.clone(),
            key: "later.bin".to_string(),
            group_id: folder.group_id,
            created_by: folder.created_by,
            checksum_hint: None,
            routing: RoutingSnapshot::single(folder.group_id),
        });
        drive(second, &context).await.expect("second upload opened");
        std::fs::write(std::path::Path::new(&folder.root).join("more.txt"), b"more")
            .expect("file written");
        let offer = OfferDirectoryInput {
            bucket: folder.local_bucket.clone(),
            root: folder.root.clone(),
            group_id: folder.group_id,
            realm_id: realm(),
            node_id: node(),
            user_id: folder.created_by,
        };
        drive(offer_directory(offer), &context)
            .await
            .expect("offer refreshed");

        assert!(matches!(
            drive(
                DeleteLocalOperation::new(&folder, (node(), context.node_and_realm()), 1),
                &context
            )
            .await,
            Err(FolderError::DeleteBucket(DeleteBucketError::NotEmpty))
        ));
        assert!(!bucket_gone(&context, &folder.local_bucket).await);
        let uploads = ListUploadsOperation::new(ListUploadsInput {
            bucket: folder.local_bucket.clone(),
            prefix: None,
            delimiter: None,
            key_marker: None,
            upload_id_marker: None,
            max_uploads: ListUploadsOperation::DEFAULT_MAX_UPLOADS,
        })
        .including_in_progress();
        let uploads = drive(uploads, &context).await.expect("uploads listed");
        assert_eq!(uploads.uploads.len(), 1);
        let versions = ListVersionsOperation::new(ListVersionsInput {
            bucket: folder.local_bucket.clone(),
            prefix: None,
            delimiter: None,
            key_marker: None,
            version_id_marker: None,
            max_keys: None,
        });
        let versions = drive(versions, &context).await.expect("versions listed");
        assert_eq!(versions.items.len(), 1);
        assert_eq!(
            drive(
                DeleteLocalOperation::new(&folder, (node(), context.node_and_realm()), 1),
                &context
            )
            .await,
            Ok(())
        );
        assert!(bucket_gone(&context, &folder.local_bucket).await);
    }

    // A deleting folder's bucket without an offer, as after a crash before the offer was stored,
    // is read-only for clients, and the cleanup still deletes it.
    #[tokio::test]
    async fn guards_unoffered_bucket() {
        use crate::staging::bucket_guard::BucketGuardOperation;
        use crate::staging::offered_directory::OfferedDirectoryError;

        let fixture = crate::tests::staging::setup_driver_context().await;
        let context = Arc::new(fixture.driver_context);
        let (_root, folder, _) = offered_folder(&context).await;
        let bucket = folder.local_bucket.clone();
        let withdrawal = WithdrawOfferInput {
            bucket: bucket.clone(),
            realm_id: realm(),
            node_id: node(),
            user_id: folder.created_by,
        };
        assert_eq!(drive(withdraw_offer(withdrawal), &context).await, Ok(1));

        assert_eq!(
            drive(BucketGuardOperation::new(&bucket), &context)
                .await
                .map_err(OfferedDirectoryError::from),
            Err(OfferedDirectoryError::ReadOnly(bucket.clone()))
        );
        assert_eq!(
            drive(
                finish_unbind(context.node_and_realm(), folder.folder_id, node()),
                &context
            )
            .await,
            Ok(())
        );
        assert!(bucket_gone(&context, &bucket).await);
        assert_eq!(
            drive(BucketGuardOperation::new(&bucket), &context)
                .await
                .map_err(OfferedDirectoryError::from),
            Ok(())
        );
    }

    // A cleanup cut short midway, here by an upload being completed, leaves the bucket read-only
    // for clients, so no upload can be opened before the next pass deletes it.
    #[tokio::test]
    async fn guards_interrupted_cleanup() {
        use crate::staging::bucket_guard::BucketGuardOperation;
        use crate::staging::offered_directory::OfferedDirectoryError;

        let fixture = crate::tests::staging::setup_driver_context().await;
        let context = Arc::new(fixture.driver_context);
        let (_root, folder, mut upload) = offered_folder(&context).await;
        upload.status = MultipartUploadStatus::Completing;
        upload.completing_since_ms = Some(aruna_core::time::unix_timestamp_millis());
        store_upload(&context, &upload).await;

        assert_eq!(
            drive(
                finish_unbind(context.node_and_realm(), folder.folder_id, node()),
                &context
            )
            .await,
            Err(FolderError::AbortUpload(
                AbortUploadError::CompletionInProgress
            ))
        );
        let bucket = folder.local_bucket.clone();
        assert_eq!(
            drive(BucketGuardOperation::new(&bucket), &context)
                .await
                .map_err(OfferedDirectoryError::from),
            Err(OfferedDirectoryError::ReadOnly(bucket.clone()))
        );

        upload.status = MultipartUploadStatus::Open;
        upload.completing_since_ms = None;
        store_upload(&context, &upload).await;
        assert_eq!(
            drive(
                finish_unbind(context.node_and_realm(), folder.folder_id, node()),
                &context
            )
            .await,
            Ok(())
        );
        assert!(bucket_gone(&context, &bucket).await);
        assert_eq!(
            drive(BucketGuardOperation::new(&bucket), &context)
                .await
                .map_err(OfferedDirectoryError::from),
            Ok(())
        );
    }

    // A reconcile pass that started before the owner's change keeps it, and a
    // folder unbound meanwhile is not written back.
    #[tokio::test]
    async fn progress_keeps_settings() {
        let (_dir, context) = context().await;
        let context = Arc::new(context);
        let mut pass = bound_folder(FolderState::Active);
        store_folder(&context, &pass).await.expect("row stored");

        drive(set_propagate_deletes(pass.folder_id, false), &context)
            .await
            .expect("the toggle applies");
        pass.last_reconcile_ms = Some(5);
        let stored = drive(store_progress(&pass), &context)
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
            drive(store_progress(&pass), &context).await,
            Err(FolderError::NotFound)
        );
        assert_eq!(
            drive(read_bound(pass.folder_id), &context).await,
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
            drive(set_propagate_deletes(folder.folder_id, true), &context).await,
            Err(FolderError::DownloadOnly)
        );
        assert!(
            !drive(read_bound(folder.folder_id), &context)
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
            drive(
                set_folder_state(folder.folder_id, FolderState::Paused),
                &context
            )
            .await
            .expect("a bound folder pauses")
            .state,
            FolderState::Paused
        );
        store_folder(&context, &bound_folder(FolderState::Deleting))
            .await
            .expect("row stored");
        assert_eq!(
            drive(
                set_folder_state(folder.folder_id, FolderState::Active),
                &context
            )
            .await,
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

        drive(unbind_folder(folder.folder_id), &context)
            .await
            .expect("the folder is marked");
        let pass = folder_gate(folder.folder_id).lock().await;
        let unbind = tokio::spawn({
            let context = context.clone();
            let folder_id = folder.folder_id;
            async move {
                let _pass = folder_gate(folder_id).lock().await;
                drive(
                    finish_unbind(context.node_and_realm(), folder_id, node()),
                    &context,
                )
                .await
            }
        });
        let row = super::super::repository::base_entry(folder.folder_id, "note.txt", &base_row())
            .expect("base encodes");
        assert!(write_rows(&context, vec![row], None).await);
        drop(pass);

        assert_eq!(unbind.await.expect("the unbind task ends"), Ok(()));
        let (entries, _) = drive(list_entries(folder.folder_id, None, None), &context)
            .await
            .expect("the entries read");
        assert!(entries.is_empty());
        assert_eq!(
            drive(read_bound(folder.folder_id), &context).await,
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
        assert_eq!(
            drive(finish_binding(listed.folder_id), &context).await,
            Ok(deleting)
        );

        let row = vec![(
            SYNCED_FOLDER_KEYSPACE.to_string(),
            folder_key(listed.folder_id),
        )];
        assert!(delete_rows(&context, row, None).await);
        assert_eq!(
            drive(finish_binding(listed.folder_id), &context).await,
            Err(FolderError::NotFound)
        );
    }

    /// Hands every storage effect to `storage`. The first write of a folder row in `held` waits
    /// for `release` on its own thread, so the effects behind it still run. `committed` gets the
    /// error the commit of that write's transaction answered with.
    fn hold_write(
        storage: aruna_storage::StorageHandle,
        receivers: aruna_storage::storage::StorageReceivers,
        held: FolderState,
        reached: tokio::sync::oneshot::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
        committed: tokio::sync::oneshot::Sender<Option<aruna_core::errors::StorageError>>,
    ) {
        fn forward(
            storage: &aruna_storage::StorageHandle,
            effect: aruna_core::effects::StorageEffect,
            response: aruna_storage::storage::ResponseSender,
        ) -> Option<aruna_core::errors::StorageError> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .expect("a proxy runtime");
            let aruna_core::events::Event::Storage(event) =
                runtime.block_on(storage.send_storage_effect(effect))
            else {
                return None;
            };
            let error = match &event {
                aruna_core::events::StorageEvent::Error { error } => Some(error.clone()),
                _ => None,
            };
            response.send(event);
            error
        }
        let writes_held = move |effect: &aruna_core::effects::StorageEffect| match effect {
            aruna_core::effects::StorageEffect::BatchWrite { writes, .. } => {
                writes.iter().any(|(key_space, _, value)| {
                    key_space == SYNCED_FOLDER_KEYSPACE
                        && SyncedFolder::from_bytes(value).is_ok_and(|row| row.state == held)
                })
            }
            _ => false,
        };
        std::thread::spawn(move || {
            let _bulk = receivers.bulk;
            let mut gate = Some((reached, release));
            let mut committed = Some(committed);
            let mut held_txn = None;
            while let Ok((effect, response, ..)) = receivers.foreground.recv() {
                if writes_held(&effect)
                    && let Some((reached, release)) = gate.take()
                {
                    if let aruna_core::effects::StorageEffect::BatchWrite { txn_id, .. } = &effect {
                        held_txn = *txn_id;
                    }
                    let storage = storage.clone();
                    std::thread::spawn(move || {
                        let _ = reached.send(());
                        let _ = release.recv();
                        forward(&storage, effect, response);
                    });
                    continue;
                }
                let commit = matches!(
                    &effect,
                    aruna_core::effects::StorageEffect::CommitTransaction { txn_id }
                        if Some(*txn_id) == held_txn
                );
                let error = forward(&storage, effect, response);
                if commit && let Some(committed) = committed.take() {
                    let _ = committed.send(error);
                }
            }
        });
    }

    // A pause or resume that read the folder before an acknowledged unbind must not write over
    // `Deleting`. The test plays the storage worker and holds the setter's write until then.
    #[tokio::test]
    async fn unbind_outlasts_pause() {
        for (start, target) in [
            (FolderState::Active, FolderState::Paused),
            (FolderState::Paused, FolderState::Active),
        ] {
            let (_dir, context) = context().await;
            let context = Arc::new(context);
            let folder = bound_folder(start);
            let folder_id = folder.folder_id;
            store_folder(&context, &folder).await.expect("row stored");
            let (storage, receivers) = aruna_storage::StorageHandle::new();
            let (reached, held) = tokio::sync::oneshot::channel();
            let (release, gate) = std::sync::mpsc::channel();
            let (committed, commit) = tokio::sync::oneshot::channel();
            hold_write(
                context.storage_handle.clone(),
                receivers,
                target.clone(),
                reached,
                gate,
                committed,
            );
            let proxied = Arc::new(DriverContext {
                storage_handle: storage,
                ..(*context).clone()
            });

            let setter = tokio::spawn({
                let proxied = proxied.clone();
                let target = target.clone();
                async move { drive(set_folder_state(folder_id, target), &proxied).await }
            });
            held.await.expect("the setter reaches its write");
            drive(unbind_folder(folder_id), &proxied)
                .await
                .expect("the unbind is acknowledged");
            release.send(()).expect("the held write waits");

            let set = setter.await.expect("the setter task ends");
            assert_eq!(set, Err(FolderError::Unavailable), "{target:?}");
            // Dropping the last proxied handle ends the proxy, so a commit that never came fails.
            drop(proxied);
            assert_eq!(
                commit.await.expect("the setter commits its held write"),
                Some(aruna_core::errors::StorageError::TransactionConflict),
                "{target:?}"
            );
            assert_eq!(
                drive(read_bound(folder_id), &context)
                    .await
                    .map(|row| row.state),
                Ok(FolderState::Deleting)
            );
        }
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
            drive(ReserveBindingOperation::new(first.clone()), &context),
            drive(ReserveBindingOperation::new(second.clone()), &context)
        );
        assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
        let bound = drive(list_folders(), &context)
            .await
            .expect("the folders list");
        assert_eq!(bound.len(), 1);
        assert_eq!(
            drive(ReserveBindingOperation::new(first.clone()), &context).await,
            Err(FolderError::RootOverlaps(first.root.clone()))
        );
        assert_eq!(
            drive(
                set_folder_state(bound[0].folder_id, FolderState::Paused),
                &context
            )
            .await,
            Err(FolderError::StillBinding)
        );
    }
}
