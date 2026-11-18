//! Removes an unbound folder: its rows, its local bucket and its offer, with the folder row
//! last, so an interrupted cleanup resumes in the next pass.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    S3_BUCKET_KEYSPACE, SYNC_BASE_KEYSPACE, SYNC_LOG_KEYSPACE, SYNC_UPLOAD_KEYSPACE,
    SYNCED_FOLDER_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::rows::{done, read_row, value};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::multipart::{MultipartUpload, MultipartUploadStatus};
use aruna_core::structs::{FolderState, SyncedFolder};
use aruna_core::task::TaskKey;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::Key;
use ulid::Ulid;

use crate::device::sync_status::ArmTimerOperation;
use crate::s3::bucket::delete::{DeleteBucketError, DeleteBucketOperation};
use crate::s3::multipart::abort::{AbortUploadError, AbortUploadInput, AbortUploadOperation};
use crate::s3::multipart::uploads::{
    ListUploadsError, ListUploadsInput, ListUploadsOperation, ListUploadsResult,
};
use crate::s3::object::delete::DeleteObjectError;
use crate::s3::object::delete::bulk::{
    BulkDeleteEntry, BulkDeleteInput, BulkDeleteOperation, BulkDeleteOutcome,
};
use crate::s3::object::versions::{
    ListVersionsError, ListVersionsInput, ListVersionsItem, ListVersionsOperation,
    ListVersionsResult,
};
use crate::staging::offered_directory::{
    OfferedDirectoryError, WithdrawOfferInput, withdraw_offer,
};

use super::super::repository::{SYNC_PAGE_SIZE, folder_key, folder_prefix};
use super::reads::read_bound;
use super::{FolderChange, FolderError, UpdateFolderOperation};

/// Rows, uploads and versions one cleanup pass removes at most, so writes that keep arriving
/// cannot hold the folder lock. Anything left fails this pass, and the next pass goes on.
const CLEANUP_LIMIT: usize = 16_000;

/// Keyspaces holding rows of one folder, cleared in this order.
const FOLDER_ROWS: [&str; 3] = [SYNC_BASE_KEYSPACE, SYNC_UPLOAD_KEYSPACE, SYNC_LOG_KEYSPACE];

/// Stops syncing one folder; the owner's filesystem is untouched. The folder is marked
/// `Deleting` and the folder pass, woken here, removes it, also after a restart.
pub fn unbind_folder(folder_id: Ulid) -> UnbindFolderOperation {
    UnbindFolderOperation {
        folder_id,
        arming: false,
    }
}

#[derive(Debug, PartialEq)]
pub struct UnbindFolderOperation {
    folder_id: Ulid,
    arming: bool,
}

impl Operation for UnbindFolderOperation {
    type Output = ();
    type Error = FolderError;

    // One transaction, so a pause or resume that read the folder earlier cannot write over it.
    fn first(&mut self) -> Result<Next<()>, FolderError> {
        let mark = UpdateFolderOperation::new(self.folder_id, FolderChange::Deleting);
        Ok(Next::Call(IoCall::child(mark)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, FolderError> {
        if self.arming {
            result.child::<(), IoError>()?.ok();
            return Ok(Next::Done(()));
        }
        result.child::<SyncedFolder, FolderError>()??;
        self.arming = true;
        let arm = ArmTimerOperation::new(TaskKey::ReconcileSyncedFolders);
        Ok(Next::Call(IoCall::child(arm)))
    }
}

/// Removes a `Deleting` folder. Its caller holds the folder's gate, so a pass in flight ends
/// first and the rows it wrote are cleared too.
pub fn finish_unbind(
    origin: Option<(NodeId, RealmId)>,
    folder_id: Ulid,
    node_id: NodeId,
) -> FinishUnbindOperation {
    FinishUnbindOperation {
        folder_id,
        node_id,
        origin,
        step: UnbindStep::Read,
        cleared: 0,
        folder: None,
    }
}

/// What the pending call of a folder cleanup answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UnbindStep {
    Read,
    Rows,
    Bucket,
    Withdraw,
    Drop,
}

#[derive(Debug, PartialEq)]
pub struct FinishUnbindOperation {
    folder_id: Ulid,
    node_id: NodeId,
    origin: Option<(NodeId, RealmId)>,
    step: UnbindStep,
    cleared: usize,
    folder: Option<SyncedFolder>,
}

type UnbindTurn = Result<Next<()>, FolderError>;

impl FinishUnbindOperation {
    fn folder(&self) -> Result<&SyncedFolder, IoError> {
        self.folder.as_ref().ok_or(IoError::Unfinished)
    }

    fn clear_next(&mut self) -> UnbindTurn {
        let Some(space) = FOLDER_ROWS.get(self.cleared) else {
            // The offer keeps the bucket read-only for clients until the bucket is gone.
            let ids = (self.node_id, self.origin);
            let bucket = DeleteLocalOperation::new(self.folder()?, ids, CLEANUP_LIMIT);
            self.step = UnbindStep::Bucket;
            return Ok(Next::Call(IoCall::child(bucket)));
        };
        self.cleared += 1;
        self.step = UnbindStep::Rows;
        let clear = ClearRowsOperation::new(space, self.folder_id, CLEANUP_LIMIT);
        Ok(Next::Call(IoCall::child(clear)))
    }

    fn withdraw(&mut self) -> UnbindTurn {
        let folder = self.folder()?;
        let withdraw = withdraw_offer(WithdrawOfferInput {
            bucket: folder.local_bucket.clone(),
            realm_id: folder.created_by.realm_id,
            node_id: self.node_id,
            user_id: folder.created_by,
        });
        self.step = UnbindStep::Withdraw;
        Ok(Next::Call(IoCall::child(withdraw)))
    }
}

impl Operation for FinishUnbindOperation {
    type Output = ();
    type Error = FolderError;

    fn first(&mut self) -> UnbindTurn {
        Ok(Next::Call(IoCall::child(read_bound(self.folder_id))))
    }

    fn next(&mut self, result: IoResult) -> UnbindTurn {
        match self.step {
            UnbindStep::Read => {
                let folder = result.child::<SyncedFolder, FolderError>()??;
                if folder.state != FolderState::Deleting {
                    return Ok(Next::Done(()));
                }
                self.folder = Some(folder);
                self.clear_next()
            }
            UnbindStep::Rows => {
                result.child::<(), FolderError>()??;
                self.clear_next()
            }
            UnbindStep::Bucket => {
                result.child::<(), FolderError>()??;
                self.withdraw()
            }
            UnbindStep::Withdraw => {
                match result.child::<usize, OfferedDirectoryError>()? {
                    // A resumed unbind finds the registration already withdrawn.
                    Ok(_) | Err(OfferedDirectoryError::NotOffered(_)) => {}
                    Err(error) => return Err(error.into()),
                }
                self.step = UnbindStep::Drop;
                let key = folder_key(self.folder_id);
                let row = (SYNCED_FOLDER_KEYSPACE.to_string(), key);
                Ok(Next::Call(IoCall::Delete(vec![row])))
            }
            UnbindStep::Drop => {
                done(result)?;
                Ok(Next::Done(()))
            }
        }
    }
}

/// Deletes up to `limit` rows one folder owns in a keyspace, page by page. Rows left over fail
/// this pass, so the folder row and its offer stay for the next pass.
#[derive(Debug, PartialEq)]
pub(super) struct ClearRowsOperation {
    space: String,
    prefix: Key,
    left: usize,
    deleting: bool,
}

impl ClearRowsOperation {
    pub(super) fn new(space: &str, folder_id: Ulid, limit: usize) -> Self {
        Self {
            space: space.to_string(),
            prefix: folder_prefix(folder_id),
            left: limit,
            deleting: false,
        }
    }

    fn page(&mut self) -> Next<()> {
        self.deleting = false;
        Next::Call(IoCall::Page {
            space: self.space.clone(),
            prefix: self.prefix.clone(),
            after: None,
            limit: SYNC_PAGE_SIZE,
        })
    }
}

impl Operation for ClearRowsOperation {
    type Output = ();
    type Error = FolderError;

    fn first(&mut self) -> Result<Next<()>, FolderError> {
        Ok(self.page())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, FolderError> {
        if self.deleting {
            done(result)?;
            return Ok(self.page());
        }
        let (rows, _) = result.page()?;
        if rows.is_empty() {
            return Ok(Next::Done(()));
        }
        if self.left == 0 {
            return Err(FolderError::Unavailable);
        }
        let deletes: Vec<_> = (rows.into_iter().take(self.left))
            .map(|(key, _)| (self.space.clone(), key))
            .collect();
        self.left -= deletes.len();
        self.deleting = true;
        Ok(Next::Call(IoCall::Delete(deletes)))
    }
}

/// What the pending call of deleting the local bucket answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BucketStep {
    Row,
    Uploads,
    List,
    Bulk,
    Delete,
}

/// Deletes the folder's device-local bucket and every version left in it, removing at most
/// `limit` uploads and `limit` versions. A bucket already gone counts as deleted.
#[derive(Debug, PartialEq)]
pub(super) struct DeleteLocalOperation {
    folder: SyncedFolder,
    node_id: NodeId,
    origin: Option<(NodeId, RealmId)>,
    limit: usize,
    left: usize,
    step: BucketStep,
}

type BucketTurn = Result<Next<()>, FolderError>;

impl DeleteLocalOperation {
    pub(super) fn new(
        folder: &SyncedFolder,
        (node_id, origin): (NodeId, Option<(NodeId, RealmId)>),
        limit: usize,
    ) -> Self {
        Self {
            folder: folder.clone(),
            node_id,
            origin,
            limit,
            left: limit,
            step: BucketStep::Row,
        }
    }

    fn list(&mut self) -> BucketTurn {
        if self.left == 0 {
            return self.delete();
        }
        self.step = BucketStep::List;
        let list = ListVersionsOperation::new(ListVersionsInput {
            bucket: self.folder.local_bucket.clone(),
            prefix: None,
            delimiter: None,
            key_marker: None,
            version_id_marker: None,
            max_keys: Some(self.left.min(ListVersionsOperation::DEFAULT_MAX_KEYS)),
        });
        Ok(Next::Call(IoCall::child(list)))
    }

    fn listed(&mut self, page: ListVersionsResult) -> BucketTurn {
        if page.items.is_empty() {
            return self.delete();
        }
        self.left = self.left.saturating_sub(page.items.len());
        let entries = (page.items.into_iter())
            .map(|item| match item {
                ListVersionsItem::Version {
                    key, version_id, ..
                }
                | ListVersionsItem::DeleteMarker {
                    key, version_id, ..
                } => BulkDeleteEntry {
                    key,
                    version_id: Some(version_id),
                },
            })
            .collect();
        let folder = &self.folder;
        let bulk = BulkDeleteOperation::new(BulkDeleteInput {
            bucket: folder.local_bucket.clone(),
            entries,
            group_id: folder.group_id,
            realm_id: folder.created_by.realm_id,
            node_id: self.node_id,
            deleted_by: folder.created_by,
            restrictions: None,
            guards: Vec::new(),
        });
        self.step = BucketStep::Bulk;
        Ok(Next::Call(IoCall::child(bulk)))
    }

    fn delete(&mut self) -> BucketTurn {
        self.step = BucketStep::Delete;
        let delete =
            DeleteBucketOperation::new(self.folder.local_bucket.clone()).purge_notices(self.origin);
        Ok(Next::Call(IoCall::child(delete)))
    }
}

impl Operation for DeleteLocalOperation {
    type Output = ();
    type Error = FolderError;

    // Checked first, so the routine missing bucket is not logged as a failed delete.
    fn first(&mut self) -> BucketTurn {
        let key = self.folder.local_bucket.as_bytes().to_vec();
        Ok(Next::Call(read_row(S3_BUCKET_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> BucketTurn {
        match self.step {
            BucketStep::Row => {
                let Some(row) = value(result)? else {
                    return Ok(Next::Done(()));
                };
                BucketInfo::from_bytes(&row).map_err(OfferedDirectoryError::Conversion)?;
                self.step = BucketStep::Uploads;
                let bucket = self.folder.local_bucket.clone();
                let abort = AbortUploadsOperation::new(bucket, self.limit);
                Ok(Next::Call(IoCall::child(abort)))
            }
            BucketStep::Uploads => {
                result.child::<(), FolderError>()??;
                self.list()
            }
            BucketStep::List => {
                let page = result.child::<ListVersionsResult, ListVersionsError>()??;
                self.listed(page)
            }
            BucketStep::Bulk => {
                let outcomes = result.child::<Vec<BulkDeleteOutcome>, DeleteObjectError>()??;
                for outcome in outcomes {
                    match outcome.result {
                        Ok(_) | Err(DeleteObjectError::NoSuchVersion) => {}
                        Err(error) => return Err(error.into()),
                    }
                }
                self.list()
            }
            BucketStep::Delete => match result.child::<(), DeleteBucketError>()? {
                Ok(()) | Err(DeleteBucketError::NotFound) => Ok(Next::Done(())),
                Err(error) => Err(error.into()),
            },
        }
    }
}

/// Aborts up to `limit` multipart uploads left in a bucket, page by page. An upload still being
/// completed fails this pass, so the next pass retries once its lease is stale.
#[derive(Debug, PartialEq)]
struct AbortUploadsOperation {
    bucket: String,
    left: usize,
    queue: VecDeque<MultipartUpload>,
    aborting: bool,
}

type AbortTurn = Result<Next<()>, FolderError>;

impl AbortUploadsOperation {
    fn new(bucket: String, limit: usize) -> Self {
        Self {
            bucket,
            left: limit,
            queue: VecDeque::new(),
            aborting: false,
        }
    }

    fn list(&mut self) -> AbortTurn {
        if self.left == 0 {
            return Ok(Next::Done(()));
        }
        self.aborting = false;
        let list = ListUploadsOperation::new(ListUploadsInput {
            bucket: self.bucket.clone(),
            prefix: None,
            delimiter: None,
            key_marker: None,
            upload_id_marker: None,
            max_uploads: self.left.min(ListUploadsOperation::DEFAULT_MAX_UPLOADS),
        })
        .including_in_progress()
        .complete_scan();
        Ok(Next::Call(IoCall::child(list)))
    }

    fn abort_next(&mut self) -> AbortTurn {
        let Some(upload) = self.queue.pop_front() else {
            return self.list();
        };
        self.aborting = true;
        let resume = upload.status == MultipartUploadStatus::Aborting;
        let mut abort = AbortUploadOperation::new(AbortUploadInput {
            bucket: upload.bucket,
            key: upload.key,
            upload_id: upload.upload_id,
            now_ms: unix_timestamp_millis(),
        });
        if resume {
            abort = abort.resuming_abort();
        }
        Ok(Next::Call(IoCall::child(abort)))
    }
}

impl Operation for AbortUploadsOperation {
    type Output = ();
    type Error = FolderError;

    fn first(&mut self) -> AbortTurn {
        self.list()
    }

    fn next(&mut self, result: IoResult) -> AbortTurn {
        if self.aborting {
            match result.child::<(), AbortUploadError>()? {
                Ok(()) | Err(AbortUploadError::NoSuchUpload) => {}
                Err(error) => return Err(error.into()),
            }
            return self.abort_next();
        }
        let page = result.child::<ListUploadsResult, ListUploadsError>()??;
        if page.uploads.is_empty() {
            return Ok(Next::Done(()));
        }
        self.left = self.left.saturating_sub(page.uploads.len());
        self.queue = page.uploads.into();
        self.abort_next()
    }
}

#[cfg(test)]
#[path = "unbind_tests.rs"]
mod tests;
