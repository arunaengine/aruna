//! Changes of local bucket names: new directories, publication of dirty bytes at flush and
//! last close, rename with a prepared destination write, and delete-pending disposition.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use aruna_core::id::NodeId;
use aruna_core::stream::{BackendStream, ByteStream};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::vfs::{
    BucketScope, DirectoryWitness, FileIdentity, HandleIdentity, HandleKind, NfsStageRecord,
    StageBase,
};
use aruna_core::user::id::UserId;
use aruna_core::vfs_effects::{InstanceSummary, ReadLockCheck, VfsFailure, VfsProtocol};
use aruna_ops_data::replication::live_queue::{CompletePutInput, CompletePutOperation};
use aruna_ops_data::s3::object::head::{HeadObjectError, HeadObjectInput, HeadObjectOperation};
use aruna_ops_data::s3::object::put::{PutObjectConfig, PutObjectInput};
use aruna_ops_data::vfs::io::{DispositionInput, ReadInput};
use aruna_ops_data::vfs::mutate::{MkdirInput, RenameInput};
use aruna_ops_data::vfs::path::{NameUse, check_component, child_key, marker_key, split_parent};
use aruna_ops_data::vfs::policy::{VfsAccess, VfsRequest};
use aruna_ops_data::vfs::publish::{CloseInput, CloseOutcome, PublishInput, Published};
use aruna_ops_data::vfs::recovery::{ReceiptInput, ReceiptOutcome};
use aruna_ops_data::vfs::{
    AuthorizeVfsOperation, InstanceTarget, LiveStageOperation, LookupInstanceOperation,
    VfsAccessContext, VfsCloseOperation, VfsDispositionOperation, VfsError, VfsMkdirOperation,
    VfsPublishOperation, VfsReadOperation, VfsReceiptOperation, VfsRenameOperation,
};
use aruna_ops_identity::document_repository::GetConfigOperation;
use aruna_ops_identity::groups::routing::{BucketRules, SnapshotOperation};
use aruna_runner::driver::{DriverContext, drive, node_routing};
use bytes::Bytes;
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

use super::bodies::chunked_body;
use super::files::current_open;
use super::names::Resolution;
use super::{
    ArunaSmbBackend, FileOpen, PendingDelete, PendingMap, PublishRetry, file_info, object_kind,
    share_context, vfs_failure,
};
use crate::backend::{BackendError, FileInfo};

/// Tries of one FLUSH before a writer that keeps changing the file wins.
const FLUSH_TRIES: usize = 4;
/// First and longest pause between background retries of a transiently failed close.
const RETRY_PAUSE_MIN: Duration = Duration::from_millis(50);
const RETRY_PAUSE_MAX: Duration = Duration::from_millis(1000);

/// A pending close by bucket scope and open.
type PendingKey = (BucketScope, Ulid);

/// What one close hands to the close operation besides the open itself.
#[derive(Default)]
struct CloseParts {
    size: u64,
    stage: Option<NfsStageRecord>,
    publish: Option<(PublishInput, PutObjectConfig)>,
    marker: Option<PutObjectConfig>,
}

/// A prepared write: the destination, its lazily read body and what it replaces.
struct Prepared {
    key: String,
    body: ByteStream,
    length: u64,
    exists: bool,
    version_id: Option<Ulid>,
}

/// What a close needs of the backend, owned, so a background retry outlives a borrow of it.
pub(super) struct CloseRunner {
    driver: Arc<DriverContext>,
    realm_id: RealmId,
    node_id: NodeId,
}

impl CloseRunner {
    /// The durable checkpoint row an NFS writer keeps for the instance, as it reads now.
    async fn live_stage(
        &self,
        open: &FileOpen,
        summary: &InstanceSummary,
    ) -> Result<Option<NfsStageRecord>, VfsError> {
        let (Some(handle_id), Some(_)) = (summary.handle_id, summary.stage_id) else {
            return Ok(None);
        };
        let scope = BucketScope::new(open.bucket.clone(), open.incarnation);
        drive(LiveStageOperation::new(scope, handle_id), &self.driver).await
    }

    /// Closes `open` with its prepared parts; a retry whose open is gone finds the instance.
    async fn run_close(
        &self,
        open: &FileOpen,
        parts: CloseParts,
    ) -> Result<CloseOutcome, VfsError> {
        let node = (self.realm_id, self.node_id);
        let input = CloseInput {
            context: share_context(node, &open.access_key, &open.bucket, open.incarnation),
            scope: BucketScope::new(open.bucket.clone(), open.incarnation),
            open_id: open.open_id,
            publish: parts.publish,
            stage: parts.stage,
        };
        let mut close = VfsCloseOperation::new(input).with_instance(open.instance_id);
        if let Some(put) = parts.marker {
            close = close.with_parent_marker(put);
        }
        drive(close, &self.driver).await
    }

    /// The routed, quota-bounded write of `prepared` for `user` into the open's bucket.
    async fn put_config(
        &self,
        user: UserId,
        bucket: (&str, &BucketInfo),
        prepared: Prepared,
    ) -> Result<PutObjectConfig, BackendError> {
        let (name, info) = bucket;
        let internal = |error: &dyn std::fmt::Display| BackendError::Internal(error.to_string());
        let realm = drive(GetConfigOperation::new(self.realm_id), &self.driver)
            .await
            .map_err(|error| internal(&error))?;
        let rules = BucketRules::of(info);
        let snapshot = SnapshotOperation::new(&node_routing(&self.driver), info.group_id, rules);
        let routing = drive(snapshot, &self.driver)
            .await
            .map_err(|error| internal(&error))?;
        Ok(PutObjectConfig {
            user_id: user,
            group_id: info.group_id,
            realm_id: self.realm_id,
            node_id: self.node_id,
            request: PutObjectInput {
                bucket: name.to_string(),
                key: prepared.key,
                content_length: Some(prepared.length),
                body: Some(prepared.body),
            },
            expected_checksums: Vec::new(),
            checksum_type: None,
            exists: prepared.exists,
            version_source: None,
            preassigned_version_id: prepared.version_id,
            quota_ceiling: realm.quota.effective_group_ceiling(&info.group_id),
            routing,
        })
    }

    /// The zero-length marker write a final delete hands to its removal, which names the
    /// parent and writes as the disposition owner from the live instance itself.
    async fn marker_template(&self, open: &FileOpen) -> Result<PutObjectConfig, BackendError> {
        let prepared = Prepared {
            key: String::new(),
            body: BackendStream::empty(),
            length: 0,
            exists: false,
            version_id: None,
        };
        let bucket = (open.bucket.as_str(), &open.info);
        self.put_config(open.owner.user, bucket, prepared).await
    }

    /// Who asks through the share of `open`, at this moment.
    fn open_context(&self, open: &FileOpen) -> VfsAccessContext {
        let node = (self.realm_id, self.node_id);
        share_context(node, &open.access_key, &open.bucket, open.incarnation)
    }

    /// A read of the open's effective image over `version_id` as its own writer, which
    /// byte-range locks never refuse.
    fn image_read(&self, open: &FileOpen, version_id: Option<Ulid>) -> ReadInput {
        ReadInput {
            context: self.open_context(open),
            key: open.key.clone(),
            instance_id: Some(open.instance_id),
            version_id,
            offset: 0,
            length: 0,
            lock_check: ReadLockCheck::Bypass,
        }
    }

    /// The `size` bytes `input` reads, each chunk read only when the write consumes it.
    fn image(&self, input: ReadInput, size: u64) -> ByteStream {
        let driver = self.driver.clone();
        chunked_body(size, move |offset, length| {
            let (driver, mut input) = (driver.clone(), input.clone());
            input.offset = offset;
            input.length = length;
            async move {
                let read = drive(VfsReadOperation::new(input), &driver).await;
                read.map(|read| Bytes::from(read.data))
            }
        })
    }

    /// The publication of an open's dirty stage and its live checkpoint row, prepared outside
    /// the namespace permit. It fences the edit generation it read against later writes.
    async fn prepare(
        &self,
        open: &FileOpen,
        summary: &InstanceSummary,
        stage: Option<NfsStageRecord>,
    ) -> Result<(PublishInput, PutObjectConfig), BackendError> {
        let version_id = summary.base_version.or(open.version_id);
        let base = match &summary.identity {
            HandleIdentity::File(FileIdentity::Committed { life_id }) => StageBase::Committed {
                version_id: version_id.ok_or(BackendError::Busy)?,
                life_id: *life_id,
            },
            HandleIdentity::File(FileIdentity::Unpublished { token, predecessor }) => {
                StageBase::Unpublished {
                    token: *token,
                    predecessor: *predecessor,
                }
            }
            HandleIdentity::Directory(_) => return Err(BackendError::IsDirectory),
        };
        let size = summary.logical_size;
        let prepared = Prepared {
            key: summary.key.clone(),
            body: self.image(self.image_read(open, version_id), size),
            length: size,
            exists: matches!(base, StageBase::Committed { .. }),
            version_id: None,
        };
        let bucket = (open.bucket.as_str(), &open.info);
        let put = self.put_config(open.owner.user, bucket, prepared).await?;
        let input = PublishInput {
            context: self.open_context(open),
            instance_id: open.instance_id,
            key: summary.key.clone(),
            handle_id: summary.handle_id,
            base,
            replaces: false,
            stage,
            logical_size: size,
            attempt_id: Ulid::generate(),
            updates: Vec::new(),
            expected_edit: Some(summary.edit_generation),
            require_writerless: false,
        };
        Ok((input, put))
    }

    /// The ordinary post-PUT completion of the version `prior` committed for `open`.
    pub(super) fn completion_input(
        &self,
        open: &FileOpen,
        prior: &PublishRetry,
        version_id: Ulid,
    ) -> CompletePutInput {
        CompletePutInput {
            realm_id: self.realm_id,
            node_id: self.node_id,
            auth: open.auth.clone(),
            group_id: open.info.group_id,
            bucket: open.bucket.clone(),
            key: prior.key.clone(),
            version_id,
            size_bytes: prior.logical_size,
        }
    }

    /// Settles the publication attempt an earlier try of `entry` left, so a write whose
    /// outcome stayed unknown releases its attempt before another one starts. A committed
    /// attempt gets the post-PUT completion its lost close never ran.
    async fn settle_publish(&self, entry: &PendingDelete) -> Result<(), VfsError> {
        let prior = entry.publish.lock().clone();
        let Some(prior) = prior else {
            return Ok(());
        };
        let open = &entry.open;
        let receipt = ReceiptInput {
            scope: BucketScope::new(open.bucket.clone(), open.incarnation),
            attempt_id: prior.attempt_id,
            instance_id: Some(open.instance_id),
            updates: Vec::new(),
            logical_size: Some(prior.logical_size),
            expected_edit: prior.expected_edit,
        };
        let outcome = drive(VfsReceiptOperation::new(receipt), &self.driver).await?;
        if let ReceiptOutcome::Applied {
            version_id: Some(version_id),
            ..
        } = outcome
        {
            let complete =
                CompletePutOperation::new(self.completion_input(open, &prior, version_id));
            // Best effort, matching VfsPublishOperation and REST stage publish.
            let _ = drive(complete, &self.driver).await;
        }
        *entry.publish.lock() = None;
        Ok(())
    }

    /// Closes the open of `entry` again with the instance's live checkpoint row, the rebuilt
    /// publication of its dirty last writer and a fresh copy of its marker write; a pending
    /// delete is never published. Also tells whether this open still owns publication.
    async fn retry_close(&self, entry: &PendingDelete) -> Result<(CloseOutcome, bool), VfsError> {
        self.settle_publish(entry).await?;
        let open = &entry.open;
        let scope = BucketScope::new(open.bucket.clone(), open.incarnation);
        let lookup =
            LookupInstanceOperation::new(scope, InstanceTarget::Instance(open.instance_id));
        let found = drive(lookup, &self.driver).await?;
        let mut parts = CloseParts::default();
        let mut owner = false;
        if let Some(summary) = found.filter(|summary| summary.instance_id == open.instance_id) {
            parts.stage = self.live_stage(open, &summary).await?;
            owner = open.write && summary.detached.is_none();
            if owns_publish(open, &summary) {
                let live = current_open(open, Some(&summary));
                let stage = parts.stage.clone();
                let publish = self.prepare(&live, &summary, stage).await;
                parts.publish = Some(publish.map_err(retry_prepare_error)?);
            }
        }
        let copy = |put: &PutObjectConfig| marker_copy(put, Some(BackendStream::empty()));
        let cached = entry.marker.lock().as_ref().map(copy);
        parts.marker = match cached {
            Some(put) => Some(put),
            None => {
                let put = self
                    .marker_template(open)
                    .await
                    .map_err(|error| VfsError::Unavailable(error.to_string()))?;
                *entry.marker.lock() = Some(marker_copy(&put, None));
                Some(put)
            }
        };
        // A write whose outcome stays unknown after a failure is settled by the next retry.
        *entry.publish.lock() = parts.publish.as_ref().map(retry_of);
        let outcome = self.run_close(open, parts).await?;
        *entry.publish.lock() = None;
        Ok((outcome, owner))
    }
}

/// The attempt of a prepared publication that a later retry may have to settle.
fn retry_of(publish: &(PublishInput, PutObjectConfig)) -> PublishRetry {
    let (input, _) = publish;
    PublishRetry {
        attempt_id: input.attempt_id,
        key: input.key.clone(),
        logical_size: input.logical_size,
        expected_edit: input.expected_edit,
    }
}

/// True when the close of `open` publishes the dirty bytes itself: it is the last writer,
/// with no pending delete and a name left. A full quota fails that close.
pub(super) fn owns_publish(open: &FileOpen, summary: &InstanceSummary) -> bool {
    let deleting = summary.delete_pending || open.delete_on_close;
    let last = summary.writer_count <= 1 && !deleting && summary.detached.is_none();
    open.write && summary.dirty && last
}

/// A close failure a later retry may still get past; any other one ended the close for good.
fn is_transient_close(error: &VfsError) -> bool {
    matches!(error, VfsError::Unavailable(_) | VfsError::Storage(_))
        || matches!(error, VfsError::Vfs(failure) if *failure != VfsFailure::NotFound)
}

/// A failed rebuild of a publication as a close failure; a busy or internal one may pass on
/// a later retry.
pub(super) fn retry_prepare_error(error: BackendError) -> VfsError {
    match error {
        BackendError::Internal(message) => VfsError::Unavailable(message),
        BackendError::Busy | BackendError::Sharing => {
            VfsError::Unavailable("publish preparation busy".into())
        }
        BackendError::NotFound => VfsError::NotFound,
        BackendError::IsDirectory => VfsError::IsDirectory,
        _ => VfsError::Denied,
    }
}

/// True once a retry ended the close. Dirty bytes whose own writer still owns publication
/// keep it, as does a failure a later retry may still get past.
pub(super) fn retry_done(result: Result<(CloseOutcome, bool), VfsError>) -> bool {
    match result {
        Ok((CloseOutcome::Pending, true)) => false,
        Ok(_) => true,
        Err(error) => !is_transient_close(&error),
    }
}

/// The marker write `put` with `body`, since a body stream is read only once.
fn marker_copy(put: &PutObjectConfig, body: Option<ByteStream>) -> PutObjectConfig {
    let request = &put.request;
    PutObjectConfig {
        user_id: put.user_id,
        group_id: put.group_id,
        realm_id: put.realm_id,
        node_id: put.node_id,
        request: PutObjectInput {
            bucket: request.bucket.clone(),
            key: request.key.clone(),
            content_length: request.content_length,
            body,
        },
        expected_checksums: put.expected_checksums.clone(),
        checksum_type: put.checksum_type.clone(),
        exists: put.exists,
        version_source: put.version_source.clone(),
        preassigned_version_id: put.preassigned_version_id,
        quota_ceiling: put.quota_ceiling,
        routing: put.routing.clone(),
    }
}

/// One retry of `entry`, one at a time; true once nothing is left to retry.
async fn try_pending(
    runner: &CloseRunner,
    pending: &PendingMap,
    key: &PendingKey,
    entry: &PendingDelete,
) -> bool {
    let _gate = entry.gate.lock().await;
    if !pending.lock().contains_key(key) {
        return true;
    }
    // A gone instance and a definitive refusal, which already cleared the disposition or
    // retained the checkpoint, end the retries as a finished close does.
    let done = retry_done(runner.retry_close(entry).await);
    if done {
        pending.lock().remove(key);
    }
    done
}

/// Retries one pending close with a growing pause until it ends or the backend is dropped.
async fn finish_pending(
    runner: CloseRunner,
    pending: Arc<PendingMap>,
    stop: CancellationToken,
    key: PendingKey,
    entry: Arc<PendingDelete>,
) {
    let mut pause = RETRY_PAUSE_MIN;
    loop {
        tokio::select! {
            () = stop.cancelled() => return,
            () = tokio::time::sleep(pause) => {}
        }
        if try_pending(&runner, &pending, &key, &entry).await {
            return;
        }
        pause = (pause * 2).min(RETRY_PAUSE_MAX);
    }
}

impl ArunaSmbBackend {
    pub(super) fn close_runner(&self) -> CloseRunner {
        CloseRunner {
            driver: Arc::clone(&self.driver),
            realm_id: self.realm_id,
            node_id: self.node_id,
        }
    }

    /// Hands a transiently failed close to one background retry, so a pending delete it left
    /// never wedges the name once the SMB handle is gone. `publish` is the attempt it settles.
    pub(super) fn queue_pending(
        &self,
        open: FileOpen,
        marker: Option<PutObjectConfig>,
        publish: Option<PublishRetry>,
    ) {
        let key = (
            BucketScope::new(open.bucket.clone(), open.incarnation),
            open.open_id,
        );
        let entry = Arc::new(PendingDelete {
            open,
            marker: Mutex::new(marker),
            gate: tokio::sync::Mutex::new(()),
            publish: Mutex::new(publish),
        });
        {
            let mut pending = self.pending.lock();
            if pending.contains_key(&key) {
                return;
            }
            pending.insert(key.clone(), Arc::clone(&entry));
        }
        let (runner, pending) = (self.close_runner(), Arc::clone(&self.pending));
        let stop = self.stop.clone();
        tokio::spawn(finish_pending(runner, pending, stop, key, entry));
    }

    /// Retries every pending close in `scope` now; true when one ended, so a refused open
    /// may try again.
    pub(super) async fn drain_pending(&self, scope: &BucketScope) -> bool {
        let entries: Vec<_> = (self.pending.lock().iter())
            .filter(|(key, _)| key.0 == *scope)
            .map(|(key, entry)| (key.clone(), Arc::clone(entry)))
            .collect();
        let runner = self.close_runner();
        let mut ended = false;
        for (key, entry) in entries {
            ended |= try_pending(&runner, &self.pending, &key, &entry).await;
        }
        ended
    }

    /// The routed, quota-bounded write of `prepared` for `user` into the open's bucket.
    async fn put_config(
        &self,
        user: UserId,
        bucket: (&str, &BucketInfo),
        prepared: Prepared,
    ) -> Result<PutObjectConfig, BackendError> {
        self.close_runner().put_config(user, bucket, prepared).await
    }

    /// The durable checkpoint row an NFS writer keeps for the instance, as it reads now.
    async fn live_stage(
        &self,
        open: &FileOpen,
        summary: &InstanceSummary,
    ) -> Result<Option<NfsStageRecord>, BackendError> {
        let runner = self.close_runner();
        runner.live_stage(open, summary).await.map_err(vfs_failure)
    }

    /// The exact committed version `version_id` of `key`, else its head, with its length.
    async fn committed_base(
        &self,
        open: &FileOpen,
        version_id: Option<Ulid>,
    ) -> Result<(Ulid, u64), BackendError> {
        let input = HeadObjectInput {
            bucket: open.bucket.clone(),
            key: open.key.clone(),
            version_id,
        };
        let head = match drive(HeadObjectOperation::new(input), &self.driver).await {
            Ok(head) => head,
            Err(HeadObjectError::NoSuchKey | HeadObjectError::NoSuchVersion) => {
                return Err(BackendError::NotFound);
            }
            Err(error) => return Err(BackendError::Internal(error.to_string())),
        };
        let size = head
            .location
            .as_ref()
            .map_or(0, |location| location.blob_size);
        let version_id = version_id
            .or(head.version_id)
            .ok_or(BackendError::NotFound)?;
        Ok((version_id, size))
    }

    /// Creates the marker `d/` of a new directory as the caller's own write.
    pub(super) async fn make_directory(
        &self,
        context: &VfsAccessContext,
        key: &str,
    ) -> Result<(), BackendError> {
        let (bucket, _) = context.bucket_scope().ok_or(BackendError::Denied)?;
        let marker = marker_key(key);
        let mut request = VfsRequest::new(VfsAccess::Put, bucket, Some(&marker));
        request.length = Some(0);
        let check = AuthorizeVfsOperation::new(context.clone(), request);
        let grant = drive(check, &self.driver).await.map_err(vfs_failure)?;
        let prepared = Prepared {
            key: marker,
            body: BackendStream::empty(),
            length: 0,
            exists: false,
            version_id: None,
        };
        let put = self
            .put_config(grant.owner, (bucket, &grant.bucket), prepared)
            .await?;
        let input = MkdirInput {
            context: context.clone(),
            key: key.to_string(),
            put,
        };
        drive(VfsMkdirOperation::new(input), &self.driver)
            .await
            .map_err(vfs_failure)?;
        Ok(())
    }

    /// The publication of an open's dirty stage and its live checkpoint row.
    async fn prepare(
        &self,
        open: &FileOpen,
        summary: &InstanceSummary,
        stage: Option<NfsStageRecord>,
    ) -> Result<(PublishInput, PutObjectConfig), BackendError> {
        self.close_runner().prepare(open, summary, stage).await
    }

    /// Every open of the instance continues from the published version.
    fn published(&self, open: &FileOpen, published: &Published) {
        let (version_id, life_id) = (published.version_id, published.life_id);
        self.update_opens(&open.bucket, open.instance_id, |other| {
            other.version_id = Some(version_id);
            if let Some(life_id) = life_id {
                other.identity = HandleIdentity::File(FileIdentity::Committed { life_id });
            }
        });
    }

    /// Publishes dirty bytes as one new version; a clean stage creates none.
    pub(super) async fn flush_file(&self, open: &FileOpen) -> Result<(), BackendError> {
        if open.kind == HandleKind::Directory || !open.write {
            return Ok(());
        }
        for _ in 0..FLUSH_TRIES {
            let summary = self.summary(open).await?.ok_or(BackendError::NotFound)?;
            // A detached instance has no name left to publish to.
            if !summary.dirty || summary.detached.is_some() {
                return Ok(());
            }
            let live = current_open(open, Some(&summary));
            let stage = self.live_stage(&live, &summary).await?;
            let (input, put) = self.prepare(&live, &summary, stage).await?;
            let publish = VfsPublishOperation::new(input, put);
            match drive(publish, &self.driver).await {
                Ok(published) => {
                    self.published(open, &published);
                    return Ok(());
                }
                // Another write moved the file on while this one was prepared; read it again.
                Err(VfsError::Conflict) => {}
                Err(error) => return Err(vfs_failure(error)),
            }
        }
        Err(BackendError::Sharing)
    }

    /// The zero-length marker write that keeps the parent of `key` when a final delete removes
    /// its last child, written as `user`; `None` at the share root.
    async fn parent_marker(
        &self,
        open: &FileOpen,
        key: &str,
        user: UserId,
    ) -> Result<Option<PutObjectConfig>, BackendError> {
        let (parent, _) = split_parent(key).ok_or(BackendError::Invalid)?;
        if parent.is_empty() {
            return Ok(None);
        }
        let prepared = Prepared {
            key: marker_key(parent),
            body: BackendStream::empty(),
            length: 0,
            exists: false,
            version_id: None,
        };
        let bucket = (open.bucket.as_str(), &open.info);
        Ok(Some(self.put_config(user, bucket, prepared).await?))
    }

    /// What one close brings: the shared durable stage and the publication of the last dirty
    /// writer, each read from the live instance. The close itself turns FILE_DELETE_ON_CLOSE
    /// into delete-pending owned by this open.
    async fn close_parts(
        &self,
        open: &FileOpen,
        summary: &InstanceSummary,
        parts: &mut CloseParts,
    ) -> Result<(), BackendError> {
        parts.stage = self.live_stage(open, summary).await?;
        if owns_publish(open, summary) {
            let stage = parts.stage.clone();
            parts.publish = Some(self.prepare(open, summary, stage).await?);
        }
        Ok(())
    }

    /// Closes one open. The last dirty writer brings its prepared publication; a pending
    /// delete is completed inside the close as its disposition owner.
    pub(super) async fn close_file(&self, open: FileOpen) -> Result<FileInfo, BackendError> {
        let mut parts = CloseParts::default();
        let mut failure = None;
        let mut time = open.version_id;
        let mut detached = false;
        match self.summary(&open).await {
            Ok(Some(summary)) => {
                parts.size = summary.logical_size;
                time = summary.base_version.or(time);
                detached = summary.detached.is_some();
                let live = current_open(&open, Some(&summary));
                failure = self.close_parts(&live, &summary, &mut parts).await.err();
            }
            Ok(None) => {}
            Err(error) => failure = Some(error),
        }
        let publish = parts.publish.as_ref().map(retry_of);
        // Retry in the background if the marker template fails so a final delete never lacks it.
        let put = match self.close_runner().marker_template(&open).await {
            Ok(put) => put,
            Err(error) => {
                self.queue_pending(open, None, None);
                return Err(error);
            }
        };
        let marker = Some(marker_copy(&put, None));
        parts.marker = Some(put);
        let size = parts.size;
        let outcome = match self.close_runner().run_close(&open, parts).await {
            Ok(outcome) => outcome,
            Err(error) => {
                // The SMB handle is gone already; the backend finishes the close itself.
                if is_transient_close(&error) {
                    self.queue_pending(open, marker, publish);
                }
                return Err(vfs_failure(error));
            }
        };
        match outcome {
            CloseOutcome::Published(published) => {
                self.published(&open, &published);
                time = Some(published.version_id);
            }
            CloseOutcome::Closed | CloseOutcome::Deleted | CloseOutcome::Discarded => {}
            // A detached instance has no name left to publish to; a read-only open leaves
            // publication (and any scheduled quota retry) to its writer.
            CloseOutcome::Pending if detached || !open.write => {}
            CloseOutcome::QuotaBlocked => return Err(BackendError::DiskFull),
            CloseOutcome::Retained(_) => return Err(BackendError::Denied),
            // The backend keeps the close and publishes the dirty bytes on a later retry.
            CloseOutcome::Pending => {
                self.queue_pending(open, marker, None);
                let unpublished = BackendError::Internal("dirty bytes stay unpublished".into());
                return Err(failure.unwrap_or(unpublished));
            }
        }
        let time = time.map_or(open.opened_ms, |id| id.timestamp_ms());
        Ok(file_info(object_kind(open.kind), size, time, false))
    }

    /// Renames the open object, keeping the open. A same-parent case change keeps the new
    /// spelling; a committed file, an unpublished file replacing a live name, or a marker
    /// moves with a prepared destination write.
    pub(super) async fn rename_file(
        &self,
        open: &FileOpen,
        target: &str,
        replace: bool,
    ) -> Result<(), BackendError> {
        // Pin the source as it lives now; a changed life refuses the whole move.
        let (open, summary) = self.current(open).await?;
        let summary = summary.ok_or(BackendError::NotFound)?;
        // A detached object has no name left to move.
        if summary.detached.is_some() {
            return Err(BackendError::NotFound);
        }
        let open = &open;
        let context = self.open_context(open);
        let (parent, name) = split_parent(target).ok_or(BackendError::Invalid)?;
        check_component(name, VfsProtocol::Smb, NameUse::Create, parent.is_empty())
            .map_err(vfs_failure)?;
        let (source_parent, _) = split_parent(&open.key).ok_or(BackendError::Invalid)?;
        let (to, exists) = match self.resolve(&context, target).await? {
            Resolution::Found(found) if found.key == open.key => {
                (child_key(source_parent, name), false)
            }
            Resolution::Found(found) => (found.key, true),
            Resolution::Missing(key) => (key, false),
        };
        if to == open.key {
            let delete = VfsRequest::new(VfsAccess::Delete, &open.bucket, Some(&open.key));
            return match self.allows(&context, delete).await? {
                true => Ok(()),
                false => Err(BackendError::Denied),
            };
        }
        let attempt_id = Ulid::generate();
        let committed = matches!(
            summary.identity,
            HandleIdentity::File(FileIdentity::Committed { .. })
        );
        // An unpublished file over a live name publishes its image at the destination.
        let unpublished = matches!(
            summary.identity,
            HandleIdentity::File(FileIdentity::Unpublished { .. })
        );
        let replaces = unpublished && exists && replace;
        // A committed file moves as its exact base version, which also fences the source
        // delete; its unflushed overlay stays staged on the instance and is not copied.
        let copied = match committed {
            true => {
                let version_id = summary.base_version.or(open.version_id);
                Some(self.committed_base(open, version_id).await?)
            }
            false => None,
        };
        let input = RenameInput {
            context,
            from: open.key.clone(),
            to: to.clone(),
            kind: open.kind,
            identity: summary.identity.clone(),
            handle_id: summary.handle_id,
            instance_id: Some(open.instance_id),
            replace,
            attempt_id,
            stage: self.live_stage(open, &summary).await?,
            source_version: copied.map(|(version_id, _)| version_id),
        };
        let runner = self.close_runner();
        let moved = match (&summary.identity, copied) {
            (HandleIdentity::File(_), Some((version_id, size))) => {
                let mut read = runner.image_read(open, Some(version_id));
                read.instance_id = None;
                Some(Prepared {
                    key: to.clone(),
                    body: runner.image(read, size),
                    length: size,
                    exists,
                    version_id: Some(attempt_id),
                })
            }
            (HandleIdentity::File(_), None) if replaces => Some(Prepared {
                key: to.clone(),
                body: runner.image(runner.image_read(open, None), summary.logical_size),
                length: summary.logical_size,
                exists,
                version_id: Some(attempt_id),
            }),
            (HandleIdentity::Directory(DirectoryWitness::Marker { .. }), _) => Some(Prepared {
                key: marker_key(&to),
                body: BackendStream::empty(),
                length: 0,
                exists: false,
                version_id: Some(attempt_id),
            }),
            _ => None,
        };
        let bucket = (open.bucket.as_str(), &open.info);
        let mut rename = match moved {
            Some(prepared) => {
                let put = self.put_config(open.owner.user, bucket, prepared).await?;
                VfsRenameOperation::new(input).with_destination_put(put)
            }
            None => VfsRenameOperation::new(input),
        };
        // A move out of an implicit directory keeps it with its own marker, if still needed.
        let (target_parent, _) = split_parent(&to).ok_or(BackendError::Invalid)?;
        if target_parent != source_parent
            && let Some(put) = self.parent_marker(open, &open.key, open.owner.user).await?
        {
            rename = rename.with_parent_marker(put);
        }
        drive(rename, &self.driver).await.map_err(vfs_failure)?;
        // The file instance at the destination tells its live identity and base version.
        let found = match open.kind {
            HandleKind::File => self.summary(open).await.ok().flatten(),
            HandleKind::Directory => None,
        };
        let identity = found.as_ref().map(|found| found.identity.clone());
        let published = committed
            || matches!(
                identity,
                Some(HandleIdentity::File(FileIdentity::Committed { .. }))
            );
        let base = found.and_then(|found| found.base_version);
        let version_id = base.or(published.then_some(attempt_id));
        self.update_opens(&open.bucket, open.instance_id, |other| {
            other.key = to.clone();
            if version_id.is_some() {
                other.version_id = version_id;
            }
            if let Some(identity) = &identity {
                other.identity = identity.clone();
            }
        });
        Ok(())
    }

    /// Sets or clears delete-pending on the shared link, owned by this open's credential.
    pub(super) async fn dispose_file(
        &self,
        open: &FileOpen,
        delete: bool,
    ) -> Result<(), BackendError> {
        let (open, _) = self.current(open).await?;
        let input = DispositionInput {
            context: self.open_context(&open),
            key: open.key.clone(),
            kind: open.kind,
            instance_id: open.instance_id,
            open_id: open.open_id,
            pending: delete,
            owner: Some(open.owner.clone()),
        };
        drive(VfsDispositionOperation::new(input), &self.driver)
            .await
            .map_err(vfs_failure)
    }
}
