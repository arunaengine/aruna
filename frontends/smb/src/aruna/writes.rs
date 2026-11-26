//! Changes of local bucket names: new directories, publication of dirty bytes at flush and
//! last close, rename with a prepared destination write, and delete-pending disposition.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::stream::{BackendStream, ByteStream};
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::vfs::{
    BucketScope, DirectoryWitness, FileIdentity, HandleIdentity, HandleKind, NfsStageRecord,
    StageBase,
};
use aruna_core::user::id::UserId;
use aruna_core::vfs_effects::{InstanceSummary, ReadLockCheck, VfsProtocol};
use aruna_ops_data::s3::object::head::{HeadObjectError, HeadObjectInput, HeadObjectOperation};
use aruna_ops_data::s3::object::put::{PutObjectConfig, PutObjectInput};
use aruna_ops_data::vfs::io::{DispositionInput, ReadInput};
use aruna_ops_data::vfs::mutate::{MkdirInput, RenameInput};
use aruna_ops_data::vfs::path::{NameUse, check_component, child_key, marker_key, split_parent};
use aruna_ops_data::vfs::policy::{VfsAccess, VfsRequest};
use aruna_ops_data::vfs::publish::{CloseInput, CloseOutcome, PublishInput, Published};
use aruna_ops_data::vfs::{
    AuthorizeVfsOperation, LiveStageOperation, VfsAccessContext, VfsCloseOperation,
    VfsDispositionOperation, VfsError, VfsMkdirOperation, VfsPublishOperation, VfsReadOperation,
    VfsRenameOperation,
};
use aruna_ops_identity::document_repository::GetConfigOperation;
use aruna_ops_identity::groups::routing::{BucketRules, SnapshotOperation};
use aruna_runner::driver::{drive, node_routing};
use bytes::Bytes;
use ulid::Ulid;

use super::bodies::chunked_body;
use super::files::current_open;
use super::names::Resolution;
use super::{ArunaSmbBackend, FileOpen, file_info, object_kind, vfs_failure};
use crate::backend::{BackendError, FileInfo};

/// Tries of one FLUSH before a writer that keeps changing the file wins.
const FLUSH_TRIES: usize = 4;

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

impl ArunaSmbBackend {
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

    /// The durable checkpoint row an NFS writer keeps for the instance, as it reads now.
    async fn live_stage(
        &self,
        open: &FileOpen,
        summary: &InstanceSummary,
    ) -> Result<Option<NfsStageRecord>, BackendError> {
        let (Some(handle_id), Some(_)) = (summary.handle_id, summary.stage_id) else {
            return Ok(None);
        };
        let scope = BucketScope::new(open.bucket.clone(), open.incarnation);
        drive(LiveStageOperation::new(scope, handle_id), &self.driver)
            .await
            .map_err(vfs_failure)
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

    /// What one close brings: the shared durable stage, the publication of the last dirty
    /// writer and the parent marker of a final delete, each read from the live instance. The
    /// close itself turns FILE_DELETE_ON_CLOSE into delete-pending owned by this open.
    async fn close_parts(
        &self,
        open: &FileOpen,
        summary: &InstanceSummary,
        parts: &mut CloseParts,
    ) -> Result<(), BackendError> {
        parts.stage = self.live_stage(open, summary).await?;
        let deleting = summary.delete_pending || open.delete_on_close;
        if deleting {
            let owner = summary.disposition_owner.as_ref().unwrap_or(&open.owner);
            parts.marker = self.parent_marker(open, &summary.key, owner.user).await?;
        }
        let detached = summary.detached.is_some();
        let last = summary.writer_count <= 1 && !deleting && !detached;
        if open.write && summary.dirty && last {
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
        let input = CloseInput {
            context: self.open_context(&open),
            scope: BucketScope::new(open.bucket.clone(), open.incarnation),
            open_id: open.open_id,
            publish: parts.publish,
            stage: parts.stage,
        };
        let mut close = VfsCloseOperation::new(input).with_instance(open.instance_id);
        if let Some(put) = parts.marker {
            close = close.with_parent_marker(put);
        }
        let outcome = drive(close, &self.driver).await.map_err(vfs_failure)?;
        match outcome {
            CloseOutcome::Published(published) => {
                self.published(&open, &published);
                time = Some(published.version_id);
            }
            CloseOutcome::Closed | CloseOutcome::Deleted | CloseOutcome::Discarded => {}
            // A detached instance has no name left to publish to.
            CloseOutcome::Pending if detached => {}
            CloseOutcome::QuotaBlocked => return Err(BackendError::DiskFull),
            CloseOutcome::Retained(_) => return Err(BackendError::Denied),
            CloseOutcome::Pending => {
                let unpublished = BackendError::Internal("dirty bytes stay unpublished".into());
                return Err(failure.unwrap_or(unpublished));
            }
        }
        let time = time.map_or(open.opened_ms, |id| id.timestamp_ms());
        Ok(file_info(object_kind(open.kind), parts.size, time, false))
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
        let moved = match (&summary.identity, copied) {
            (HandleIdentity::File(_), Some((version_id, size))) => {
                let mut read = self.image_read(open, Some(version_id));
                read.instance_id = None;
                Some(Prepared {
                    key: to.clone(),
                    body: self.image(read, size),
                    length: size,
                    exists,
                    version_id: Some(attempt_id),
                })
            }
            (HandleIdentity::File(_), None) if replaces => Some(Prepared {
                key: to.clone(),
                body: self.image(self.image_read(open, None), summary.logical_size),
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
