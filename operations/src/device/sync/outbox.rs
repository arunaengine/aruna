//! Asks each folder's realm node to pull the local versions that changed.
//! The device never pushes bytes; a request names the exact local version to read back.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::rows::{outside_answer, outside_call};
use std::collections::VecDeque;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::keyspaces::{
    SYNC_BASE_KEYSPACE, SYNC_ORIGIN_KEYSPACE, SYNC_UPLOAD_KEYSPACE, SYNCED_FOLDER_KEYSPACE,
};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{read_row, value};
use aruna_core::structs::execution::offered_directory::modified_ms;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_core::structs::{
    EntrySide, EntryState, FolderState, LocalOrigin, SyncBase, SyncPull, SyncPullAck, SyncRefusal,
    SyncedBytes, SyncedFolder,
};
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{Key, Value};
use tracing::{info, warn};
use ulid::Ulid;

use crate::device::backlog::{
    DrainOperation, DrainQueue, ForwardOutcome, forward_member, outcome_rechecks,
};
use crate::device::drain::DrainOutcome;
use crate::device::retry::{exhausted, retry_due_ms};
use crate::device::scan_rows::{ScanRowsError, ScanRowsOperation};
use crate::document_repository::{config_of, realm_config};
use crate::driver::DriverContext;
use aruna_core::structs::identity::realm::NodeRealm;

use super::folders::FolderError;
use super::pull::{PullStep, PullWalk};
use super::repository::{
    MAX_UPLOAD_ATTEMPTS, SYNC_PAGE_SIZE, StoreUploadError, StoreUploadOperation, SyncUpload,
    UploadState, base_entry, base_key, folder_key, requeued, same_version,
};

/// Delay before a deferred pass looks for the realm again.
pub const UPLOAD_DEFER_AFTER: Duration = Duration::from_secs(15);

/// Delay between passes while uploads are still due.
pub const UPLOAD_CONTINUE_AFTER: Duration = Duration::from_millis(250);

/// Uploads forwarded at once. Each one holds a single realm pull and its polls.
const UPLOADS_IN_FLIGHT: usize = 8;

/// The upload outbox as the page drain runs it.
#[derive(Debug, PartialEq)]
struct UploadQueue {
    realm_id: RealmId,
    node_id: NodeId,
}

impl DrainQueue for UploadQueue {
    type Entry = SyncUpload;

    fn page(&self) -> (&'static str, usize) {
        (SYNC_UPLOAD_KEYSPACE, SYNC_PAGE_SIZE)
    }

    fn entry(&self, value: &Value) -> Option<SyncUpload> {
        SyncUpload::from_bytes(value).ok()
    }

    fn forward(&self, upload: SyncUpload) -> Effect {
        forward_member(ForwardUploadOperation::new(
            self.realm_id,
            self.node_id,
            upload,
        ))
    }

    fn rechecks(&self, outcome: ChildOutcome) -> bool {
        outcome_rechecks::<IoError>(outcome)
    }
}

/// Forwards every due upload, oldest folder first. A realm that limits a device's concurrent
/// requests gets no more uploads at once.
#[derive(Debug, PartialEq)]
pub struct DrainOutboxOperation {
    ids: NodeRealm,
    draining: bool,
}

/// One pass over the upload outbox; see [`DrainOutboxOperation`].
pub fn drain_sync_outbox(context: &DriverContext) -> DrainOutboxOperation {
    DrainOutboxOperation {
        ids: context.node_and_realm(),
        draining: false,
    }
}

impl DrainOutboxOperation {
    fn limit(config: Option<RealmConfigDocument>) -> usize {
        config
            .and_then(|config| config.quota.device_concurrent_pulls)
            .map_or(UPLOADS_IN_FLIGHT, |limit| {
                UPLOADS_IN_FLIGHT.min(limit as usize)
            })
    }
}

impl Operation for DrainOutboxOperation {
    type Output = DrainOutcome;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<DrainOutcome>, IoError> {
        match self.ids {
            Some((_, realm_id)) => Ok(Next::Call(realm_config(realm_id))),
            None => Ok(Next::Done(DrainOutcome::Deferred)),
        }
    }

    fn next(&mut self, result: IoResult) -> Result<Next<DrainOutcome>, IoError> {
        if self.draining {
            return Ok(Next::Done(result.child::<DrainOutcome, IoError>()??));
        }
        let (node_id, realm_id) = self.ids.ok_or(IoError::Unfinished)?;
        let limit = Self::limit(config_of(result)?);
        let queue = UploadQueue { realm_id, node_id };
        let drain = DrainOperation::new(queue, unix_timestamp_millis(), limit);
        self.draining = true;
        Ok(Next::Call(IoCall::child(drain)))
    }
}

/// The pull that asks the folder's realm node for this upload's exact local version.
fn pull_of(
    (realm_id, node_id): (RealmId, NodeId),
    folder: &SyncedFolder,
    upload: &SyncUpload,
) -> Result<SyncPull, String> {
    let source = VersionedObjectArn::new(
        realm_id,
        node_id,
        folder.local_bucket.clone(),
        upload.relative.clone(),
        upload.local_version.unwrap_or_default(),
    )
    .map_err(|error| error.to_string())?;
    Ok(SyncPull {
        source,
        blake3: upload.blake3,
        size: upload.size,
        target_bucket: folder.remote.bucket.clone(),
        target_key: folder.remote.remote_key(&upload.relative),
        deleted: upload.deleted,
    })
}

fn owner_token(realm_id: RealmId, folder: &SyncedFolder) -> AuthToken {
    AuthToken::internal(AuthContext {
        user_id: folder.created_by,
        realm_id,
        path_restrictions: None,
        session: None,
    })
}

fn upload_row(upload: &SyncUpload) -> (String, Key) {
    (
        SYNC_UPLOAD_KEYSPACE.to_string(),
        base_key(upload.folder_id, &upload.relative),
    )
}

fn store_call(upload: &SyncUpload, next: UploadState) -> IoCall {
    IoCall::child(StoreUploadOperation::new(upload.clone(), next, true))
}

fn stored(result: IoResult) -> Result<bool, IoError> {
    Ok(result.child::<bool, StoreUploadError>()?.unwrap_or(false))
}

/// What the pending call of one forwarded upload answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UploadStep {
    Folder,
    Drop,
    Claim,
    Answer,
    Progress,
    Delay,
    Settle,
    Parked,
}

/// Forwards one due upload: claims it, runs the pull, and stores how the pull ended. The row
/// follows the bytes the realm node has, so the transfer list shows them.
#[derive(Debug, PartialEq)]
struct ForwardUploadOperation {
    realm_id: RealmId,
    node_id: NodeId,
    upload: SyncUpload,
    step: UploadStep,
    walk: Option<PullWalk>,
    moved: Option<UploadState>,
}

type Forwarding = Result<Next<ForwardOutcome>, IoError>;

impl ForwardUploadOperation {
    fn new(realm_id: RealmId, node_id: NodeId, upload: SyncUpload) -> Self {
        Self {
            realm_id,
            node_id,
            upload,
            step: UploadStep::Folder,
            walk: None,
            moved: None,
        }
    }

    /// Removes the row of a folder that is gone or of a delete that stays local.
    fn drop_row(&mut self) -> Forwarding {
        self.step = UploadStep::Drop;
        let delete = IoCall::Delete(vec![upload_row(&self.upload)]);
        Ok(Next::Call(outside_call(delete)))
    }

    fn park(&mut self, reason: String) -> Forwarding {
        let next = UploadState::Failed {
            reason,
            retryable: false,
        };
        self.step = UploadStep::Parked;
        Ok(Next::Call(store_call(&self.upload, next)))
    }

    /// Spends one attempt and backs off, parking the row once the attempts are spent.
    fn retry(&mut self, reason: String) -> Forwarding {
        let attempts = self.upload.attempts().saturating_add(1);
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
        self.step = UploadStep::Parked;
        Ok(Next::Call(store_call(&self.upload, next)))
    }

    fn foldered(&mut self, folder: Option<SyncedFolder>) -> Forwarding {
        // The folder is unbound: this entry is nobody's work any more, and leaving it would
        // keep the drain awake for a folder that is gone.
        let Some(folder) = folder else {
            return self.drop_row();
        };
        // An unbinding folder publishes nothing more; its entries are going.
        if folder.state == FolderState::Deleting {
            return Ok(Next::Done(ForwardOutcome::Settled));
        }
        // A deletion the owner stopped propagating stays local; the next pass reports it.
        if self.upload.deleted && !folder.propagate_deletes {
            return self.drop_row();
        }
        let ids = (self.realm_id, self.node_id);
        let pull = match pull_of(ids, &folder, &self.upload) {
            Ok(pull) => pull,
            Err(reason) => return self.park(reason),
        };
        let token = owner_token(self.realm_id, &folder);
        self.walk = Some(PullWalk::new(folder.remote.node_id, token, pull));
        // The running state goes in only while the row still carries what the scan read, so a
        // concurrent unbind or a newer version wins instead of coming back.
        let claimed = UploadState::Transferring {
            attempts: self.upload.attempts(),
            bytes_done: 0,
        };
        self.moved = Some(claimed.clone());
        self.step = UploadStep::Claim;
        Ok(Next::Call(store_call(&self.upload, claimed)))
    }

    fn walk(&mut self) -> Result<&mut PullWalk, IoError> {
        self.walk.as_mut().ok_or(IoError::Unfinished)
    }

    /// Keeps a state the row now holds as the one later writes are guarded by.
    fn state_kept(&mut self, kept: bool) {
        if let (true, Some(state)) = (kept, self.moved.take()) {
            self.upload.state = state;
        }
    }

    fn follow(&mut self, step: PullStep) -> Forwarding {
        match step {
            PullStep::Ask(call) => {
                self.step = UploadStep::Answer;
                Ok(Next::Call(call))
            }
            PullStep::Moved(bytes_done) => {
                let next = UploadState::Transferring {
                    attempts: self.upload.attempts(),
                    bytes_done,
                };
                self.moved = Some(next.clone());
                self.step = UploadStep::Progress;
                Ok(Next::Call(store_call(&self.upload, next)))
            }
            PullStep::Wait(delay) => {
                self.step = UploadStep::Delay;
                Ok(Next::Call(IoCall::Delay(delay)))
            }
            PullStep::Ended(Ok(ack)) => {
                self.step = UploadStep::Settle;
                let settle = SettleUploadOperation::new(self.upload.clone(), ack);
                Ok(Next::Call(IoCall::child(settle)))
            }
            PullStep::Ended(Err(refusal)) if permanent(&refusal) => {
                self.park(format!("{refusal:?}"))
            }
            PullStep::Ended(Err(refusal)) => self.retry(format!("{refusal:?}")),
        }
    }
}

impl Operation for ForwardUploadOperation {
    type Output = ForwardOutcome;
    type Error = IoError;

    fn first(&mut self) -> Forwarding {
        let key = folder_key(self.upload.folder_id).to_vec();
        let read = read_row(SYNCED_FOLDER_KEYSPACE, key);
        Ok(Next::Call(outside_call(read)))
    }

    fn next(&mut self, result: IoResult) -> Forwarding {
        let recheck = Ok(Next::Done(ForwardOutcome::Recheck));
        match self.step {
            UploadStep::Folder => {
                let stored = outside_answer(result).and_then(|read| read.value());
                let folder =
                    (stored.ok().flatten()).and_then(|bytes| SyncedFolder::from_bytes(&bytes).ok());
                self.foldered(folder)
            }
            UploadStep::Drop => {
                if outside_answer(result).is_err() {
                    let relative = &self.upload.relative;
                    warn!(%relative, "Failed to drop an orphaned upload row");
                }
                Ok(Next::Done(ForwardOutcome::Settled))
            }
            UploadStep::Claim => {
                let claimed = stored(result)?;
                if !claimed {
                    return Ok(Next::Done(ForwardOutcome::Settled));
                }
                self.state_kept(true);
                let step = self.walk()?.request();
                self.follow(step)
            }
            UploadStep::Answer => {
                let step = self.walk()?.answered(result)?;
                self.follow(step)
            }
            UploadStep::Progress => {
                let kept = stored(result)?;
                self.state_kept(kept);
                let step = self.walk()?.wait();
                self.follow(step)
            }
            UploadStep::Delay => {
                let step = self.walk()?.poll();
                self.follow(step)
            }
            UploadStep::Settle => {
                result.child::<(), IoError>()?.ok();
                recheck
            }
            UploadStep::Parked => {
                stored(result)?;
                recheck
            }
        }
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

/// Records the realm version as the entry's new base and drops the outbox row in one
/// transaction, so an acknowledged pull is never forwarded twice; a row the owner must still
/// answer keeps its reported state.
#[derive(Debug, PartialEq)]
struct SettleUploadOperation {
    upload: SyncUpload,
    ack: SyncPullAck,
    step: SettleStep,
    holds: bool,
}

/// What the pending call of a settle answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SettleStep {
    Rows,
    Write,
    Delete,
}

impl SettleUploadOperation {
    fn new(upload: SyncUpload, ack: SyncPullAck) -> Self {
        Self {
            upload,
            ack,
            step: SettleStep::Rows,
            holds: false,
        }
    }

    fn base_key(&self) -> Key {
        base_key(self.upload.folder_id, &self.upload.relative)
    }

    fn committed(&self) -> Next<()> {
        let (relative, version) = (&self.upload.relative, self.ack.version_id);
        info!(%relative, %version, "Published a synced file");
        Next::Done(())
    }
}

impl Operation for SettleUploadOperation {
    type Output = ();
    type Error = IoError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<()>, IoError> {
        Ok(Next::Call(IoCall::Read(vec![
            (SYNC_BASE_KEYSPACE.to_string(), self.base_key()),
            upload_row(&self.upload),
        ])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        match self.step {
            SettleStep::Rows => {
                let mut rows = result.values()?.into_iter();
                let current =
                    (rows.next().flatten()).and_then(|bytes| SyncBase::from_bytes(&bytes).ok());
                let held =
                    (rows.next().flatten()).and_then(|bytes| SyncUpload::from_bytes(&bytes).ok());
                let base = settled_base(&self.upload, &self.ack, current);
                let (folder_id, relative) = (self.upload.folder_id, &self.upload.relative);
                let row = base_entry(folder_id, relative, &base).map_err(|_| {
                    warn!(%relative, "Failed to encode a synced base");
                    IoError::Codec("the synced base does not encode".to_string())
                })?;
                let mut writes: Vec<_> = origin_row(&self.upload, &self.ack).into_iter().collect();
                // Without a row this answer repeats a settled one, and a newer base may
                // already stand.
                if held.is_some() {
                    writes.push(row);
                }
                // A pass may have queued a newer version of the file; that row is still owed.
                self.holds = held.is_some_and(|held| same_version(&held, &self.upload));
                self.step = SettleStep::Write;
                Ok(Next::Call(IoCall::Write(writes)))
            }
            SettleStep::Write => {
                result.done()?;
                if !self.holds {
                    return Ok(self.committed());
                }
                self.step = SettleStep::Delete;
                Ok(Next::Call(IoCall::Delete(vec![upload_row(&self.upload)])))
            }
            SettleStep::Delete => {
                result.done()?;
                Ok(self.committed())
            }
        }
    }
}

/// Which local file the acknowledged version came from, kept on this device only. A delete
/// leaves no file to name.
fn origin_row(upload: &SyncUpload, ack: &SyncPullAck) -> Option<(String, Key, Value)> {
    let origin = LocalOrigin {
        folder_id: upload.folder_id,
        relative: upload.relative.clone(),
        modified_at_ms: modified_ms(&upload.fingerprint),
        fingerprint: upload.fingerprint.clone(),
    };
    let bytes = (!upload.deleted)
        .then(|| origin.to_bytes().ok())
        .flatten()?;
    let key = ack.version_id.to_bytes().to_vec().into();
    Some((SYNC_ORIGIN_KEYSPACE.to_string(), key, bytes.into()))
}

/// The local file this device published as a realm version, if it did.
#[derive(Debug, PartialEq)]
pub struct LocalOriginOperation {
    version_id: Ulid,
}

/// The read of a version's local origin; see [`LocalOriginOperation`].
pub fn local_origin(version_id: Ulid) -> LocalOriginOperation {
    LocalOriginOperation { version_id }
}

impl Operation for LocalOriginOperation {
    type Output = Option<LocalOrigin>;
    type Error = FolderError;

    fn first(&mut self) -> Result<Next<Option<LocalOrigin>>, FolderError> {
        let key = self.version_id.to_bytes().to_vec();
        Ok(Next::Call(read_row(SYNC_ORIGIN_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<LocalOrigin>>, FolderError> {
        let origin = value(result)?.map(|bytes| LocalOrigin::from_bytes(&bytes));
        let origin = origin.transpose().map_err(|_| FolderError::Unavailable)?;
        Ok(Next::Done(origin))
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

/// Makes every backed-off upload due now and re-arms retryable failures, for an explicit
/// owner-requested sync. A running pull keeps its polls.
#[derive(Debug, PartialEq)]
pub(crate) struct RequeueUploadsOperation {
    now_ms: u64,
    scanned: bool,
    uploads: VecDeque<SyncUpload>,
}

impl RequeueUploadsOperation {
    pub(crate) fn new(now_ms: u64) -> Self {
        Self {
            now_ms,
            scanned: false,
            uploads: VecDeque::new(),
        }
    }

    fn next_upload(&mut self) -> Result<Next<()>, IoError> {
        while let Some(upload) = self.uploads.pop_front() {
            if let Some(state) = requeued(&upload.state, self.now_ms) {
                return Ok(Next::Call(store_call(&upload, state)));
            }
        }
        Ok(Next::Done(()))
    }
}

impl Operation for RequeueUploadsOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        let scan = ScanRowsOperation::new(SYNC_UPLOAD_KEYSPACE, None, SYNC_PAGE_SIZE);
        Ok(Next::Call(IoCall::child(scan)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        if self.scanned {
            stored(result)?;
            return self.next_upload();
        }
        self.scanned = true;
        match result.child::<Vec<Value>, ScanRowsError>()? {
            Ok(rows) => {
                self.uploads = (rows.iter())
                    .filter_map(|bytes| SyncUpload::from_bytes(bytes).ok())
                    .collect();
                self.next_upload()
            }
            Err(error) => {
                warn!(%error, "Failed to scan the folder uploads");
                Ok(Next::Done(()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use aruna_core::UserId;
    use aruna_core::structs::{FolderMode, RemoteBinding};
    use ulid::Ulid;

    use super::{
        ForwardOutcome, ForwardUploadOperation, SettleUploadOperation, StoreUploadOperation,
        SyncPullAck, SyncUpload, UploadState, drain_sync_outbox,
    };
    use crate::device::drain::DrainOutcome;
    use crate::device::sync::repository::requeued;
    use crate::device::sync::repository::{base_key, upload_entry};
    use crate::driver::{DriverContext, drive};
    use crate::tests::device::{context, read_value, store_folder, write_rows};
    use aruna_core::keyspaces::{SYNC_BASE_KEYSPACE, SYNC_UPLOAD_KEYSPACE};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::{FolderState, SyncBase, SyncedFolder};

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
            last_error_ms: None,
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

    async fn forward(context: &Arc<DriverContext>, entry: SyncUpload) -> ForwardOutcome {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let forward = ForwardUploadOperation::new(realm_id, node_id(), entry);
        drive(forward, context).await.unwrap()
    }

    async fn claim_upload(context: &Arc<DriverContext>, upload: &SyncUpload) -> Option<SyncUpload> {
        let state = UploadState::Transferring {
            attempts: upload.attempts(),
            bytes_done: 0,
        };
        let claim = StoreUploadOperation::new(upload.clone(), state.clone(), true);
        let claimed = SyncUpload {
            state,
            ..upload.clone()
        };
        drive(claim, context).await.unwrap().then_some(claimed)
    }

    async fn settle_upload(context: &Arc<DriverContext>, upload: &SyncUpload, ack: &SyncPullAck) {
        let settle = SettleUploadOperation::new(upload.clone(), ack.clone());
        drive(settle, context).await.unwrap();
    }

    async fn store(context: &Arc<DriverContext>, upload: &SyncUpload) {
        assert!(write_rows(context, vec![upload_entry(upload).unwrap()], None).await);
    }

    async fn stored(context: &Arc<DriverContext>, upload: &SyncUpload) -> SyncUpload {
        let bytes = read_value(
            context,
            SYNC_UPLOAD_KEYSPACE,
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
        assert_eq!(
            drive(drain_sync_outbox(&context), &context).await,
            Ok(DrainOutcome::Deferred)
        );
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
            forward(&context, entry.clone()).await,
            ForwardOutcome::Settled
        );
        assert!(
            read_value(
                &context,
                SYNC_UPLOAD_KEYSPACE,
                base_key(entry.folder_id, &entry.relative),
                None,
            )
            .await
            .is_none()
        );
    }

    #[tokio::test]
    async fn claims_before_forward() {
        // The claim is stored before the forward; a forward that reaches no
        // realm node spends one attempt and backs off.
        let (_tempdir, context) = context().await;
        let context = Arc::new(context);
        let folder = folder(Ulid::generate());
        store_folder(&context, &folder).await.unwrap();
        let entry = upload(folder.folder_id);
        store(&context, &entry).await;

        assert_eq!(
            forward(&context, entry.clone()).await,
            ForwardOutcome::Recheck
        );
        assert!(matches!(
            stored(&context, &entry).await.state,
            UploadState::Pending { attempts: 1, .. }
        ));
    }

    #[tokio::test]
    async fn drops_suppressed_delete() {
        // Propagation was turned off after the deletion queued: it is never forwarded.
        let (_tempdir, context) = context().await;
        let context = Arc::new(context);
        let folder = folder(Ulid::generate());
        store_folder(&context, &folder).await.unwrap();
        let entry = SyncUpload {
            deleted: true,
            ..upload(folder.folder_id)
        };
        store(&context, &entry).await;

        assert_eq!(
            forward(&context, entry.clone()).await,
            ForwardOutcome::Settled
        );
        assert!(
            read_value(
                &context,
                SYNC_UPLOAD_KEYSPACE,
                base_key(entry.folder_id, &entry.relative),
                None,
            )
            .await
            .is_none()
        );
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

        assert_eq!(forward(&context, entry).await, ForwardOutcome::Settled);
        assert_eq!(stored(&context, &newer).await.state, newer.state);
    }

    #[tokio::test]
    async fn keeps_newer_version() {
        // A late answer for an older version must leave the row a later pass queued untouched.
        let (_tempdir, context) = context().await;
        let context = Arc::new(context);
        let older = upload(Ulid::generate());
        let newer = SyncUpload {
            local_version: Some(Ulid::generate()),
            ..older.clone()
        };
        store(&context, &newer).await;

        assert_eq!(claim_upload(&context, &older).await, None);
        let ack = SyncPullAck {
            version_id: Ulid::generate(),
            already_applied: true,
        };
        settle_upload(&context, &older, &ack).await;
        assert_eq!(stored(&context, &newer).await, newer);
    }

    #[tokio::test]
    async fn keeps_settled_base() {
        // A repeated answer for an older version must not replace the base a newer one left.
        let (_tempdir, context) = context().await;
        let context = Arc::new(context);
        let older = upload(Ulid::generate());
        let newer = SyncUpload {
            local_version: Some(Ulid::generate()),
            ..older.clone()
        };
        store(&context, &newer).await;
        let answer = |version_id| SyncPullAck {
            version_id,
            already_applied: true,
        };
        let newer_version = Ulid::generate();
        settle_upload(&context, &newer, &answer(newer_version)).await;
        settle_upload(&context, &older, &answer(Ulid::generate())).await;

        let bytes = read_value(
            &context,
            SYNC_BASE_KEYSPACE,
            base_key(newer.folder_id, &newer.relative),
            None,
        )
        .await
        .expect("the base row is stored");
        let base = SyncBase::from_bytes(&bytes).unwrap();
        assert_eq!(base.local_version_id, newer.local_version);
        assert_eq!(
            base.synced.map(|synced| synced.remote_version_id),
            Some(newer_version)
        );
    }

    #[test]
    fn requeues_on_request() {
        // An explicit sync runs backed-off and retryable uploads now, nothing else.
        let backed_off = UploadState::Pending {
            due_at_ms: 50,
            attempts: 3,
            last_error: Some("unreachable".to_string()),
        };
        assert_eq!(
            requeued(&backed_off, 10),
            Some(UploadState::Pending {
                due_at_ms: 10,
                attempts: 3,
                last_error: Some("unreachable".to_string()),
            })
        );
        let retryable = UploadState::Failed {
            reason: "unreachable".to_string(),
            retryable: true,
        };
        assert!(matches!(
            requeued(&retryable, 10),
            Some(UploadState::Pending {
                due_at_ms: 10,
                attempts: 0,
                ..
            })
        ));
        let refused = UploadState::Failed {
            reason: "Forbidden".to_string(),
            retryable: false,
        };
        assert_eq!(requeued(&refused, 10), None);
        let running = UploadState::Transferring {
            attempts: 1,
            bytes_done: 5,
        };
        assert_eq!(requeued(&running, 10), None);
    }
}
