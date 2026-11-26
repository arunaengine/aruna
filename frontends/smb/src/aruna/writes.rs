//! Changes of local bucket names: new directories, publication of dirty bytes at flush and
//! last close, rename with a prepared destination write, and delete-pending disposition.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::stream::{BackendStream, ByteStream};
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::vfs::{
    BucketScope, DirectoryWitness, FileIdentity, HandleIdentity, HandleKind, StageBase,
};
use aruna_core::user::id::UserId;
use aruna_core::vfs_effects::{InstanceSummary, VfsProtocol};
use aruna_ops_data::s3::object::put::{PutObjectConfig, PutObjectInput};
use aruna_ops_data::vfs::io::{DispositionInput, ReadInput};
use aruna_ops_data::vfs::mutate::{MkdirInput, RenameInput};
use aruna_ops_data::vfs::path::{NameUse, check_component, child_key, marker_key, split_parent};
use aruna_ops_data::vfs::policy::{VfsAccess, VfsRequest};
use aruna_ops_data::vfs::publish::{CloseInput, CloseOutcome, PublishInput, Published};
use aruna_ops_data::vfs::{
    AuthorizeVfsOperation, VfsAccessContext, VfsCloseOperation, VfsDispositionOperation,
    VfsMkdirOperation, VfsPublishOperation, VfsReadOperation, VfsRenameOperation,
};
use aruna_ops_identity::document_repository::GetConfigOperation;
use aruna_ops_identity::groups::routing::{BucketRules, SnapshotOperation};
use aruna_runner::driver::{drive, node_routing};
use bytes::Bytes;
use ulid::Ulid;

use super::bodies::chunked_body;
use super::names::Resolution;
use super::{ArunaSmbBackend, FileOpen, file_info, object_kind, vfs_failure};
use crate::backend::{BackendError, FileInfo};

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

    /// The effective image of an open file, each chunk read only when the write consumes it.
    fn image(&self, open: &FileOpen, size: u64) -> ByteStream {
        let driver = self.driver.clone();
        let input = ReadInput {
            context: self.open_context(open),
            key: open.key.clone(),
            instance_id: Some(open.instance_id),
            version_id: open.version_id,
            offset: 0,
            length: 0,
        };
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

    /// The publication of an open's dirty stage, prepared outside the namespace permit.
    async fn prepare(
        &self,
        open: &FileOpen,
        summary: &InstanceSummary,
    ) -> Result<(PublishInput, PutObjectConfig), BackendError> {
        let base = match &summary.identity {
            HandleIdentity::File(FileIdentity::Committed { life_id }) => StageBase::Committed {
                version_id: open.version_id.ok_or(BackendError::Busy)?,
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
            key: open.key.clone(),
            body: self.image(open, size),
            length: size,
            exists: matches!(base, StageBase::Committed { .. }),
            version_id: None,
        };
        let bucket = (open.bucket.as_str(), &open.info);
        let put = self.put_config(open.owner.user, bucket, prepared).await?;
        let input = PublishInput {
            context: self.open_context(open),
            instance_id: open.instance_id,
            key: open.key.clone(),
            handle_id: summary.handle_id,
            base,
            replaces: false,
            stage: None,
            logical_size: size,
            attempt_id: Ulid::generate(),
            updates: Vec::new(),
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
        let Some(summary) = self.summary(open).await? else {
            return Err(BackendError::NotFound);
        };
        if !summary.dirty {
            return Ok(());
        }
        let (input, put) = self.prepare(open, &summary).await?;
        let publish = VfsPublishOperation::new(input, put);
        let published = drive(publish, &self.driver).await.map_err(vfs_failure)?;
        self.published(open, &published);
        Ok(())
    }

    /// Closes one open. The last dirty writer brings its prepared publication; a pending
    /// delete is completed inside the close as its disposition owner.
    pub(super) async fn close_file(&self, open: FileOpen) -> Result<FileInfo, BackendError> {
        let mut size = 0;
        let mut publish = None;
        let mut failure = None;
        if open.write {
            match self.summary(&open).await {
                Ok(Some(summary)) => {
                    size = summary.logical_size;
                    let last = summary.writer_count <= 1 && !summary.delete_pending;
                    if summary.dirty && last {
                        match self.prepare(&open, &summary).await {
                            Ok(prepared) => publish = Some(prepared),
                            Err(error) => failure = Some(error),
                        }
                    }
                }
                Ok(None) => {}
                Err(error) => failure = Some(error),
            }
        }
        let input = CloseInput {
            context: self.open_context(&open),
            scope: BucketScope::new(open.bucket.clone(), open.incarnation),
            open_id: open.open_id,
            publish,
            stage: None,
        };
        let outcome = drive(VfsCloseOperation::new(input), &self.driver)
            .await
            .map_err(vfs_failure)?;
        match outcome {
            CloseOutcome::Published(published) => self.published(&open, &published),
            CloseOutcome::Closed | CloseOutcome::Deleted | CloseOutcome::Discarded => {}
            CloseOutcome::QuotaBlocked => return Err(BackendError::DiskFull),
            CloseOutcome::Retained(_) => return Err(BackendError::Denied),
            CloseOutcome::Pending => {
                let unpublished = BackendError::Internal("dirty bytes stay unpublished".into());
                return Err(failure.unwrap_or(unpublished));
            }
        }
        let time = open
            .version_id
            .map_or(open.opened_ms, |id| id.timestamp_ms());
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
        // Pin the source as it lives now; a changed life refuses the whole move.
        let summary = self.summary(open).await?.ok_or(BackendError::NotFound)?;
        if to == open.key {
            let delete = VfsRequest::new(VfsAccess::Delete, &open.bucket, Some(&open.key));
            return match self.allows(&context, delete).await? {
                true => Ok(()),
                false => Err(BackendError::Denied),
            };
        }
        let attempt_id = Ulid::generate();
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
            stage: None,
        };
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
        let moved = match &summary.identity {
            HandleIdentity::File(_) if committed || replaces => Some(Prepared {
                key: to.clone(),
                body: self.image(open, summary.logical_size),
                length: summary.logical_size,
                exists,
                version_id: Some(attempt_id),
            }),
            HandleIdentity::Directory(DirectoryWitness::Marker { .. }) => Some(Prepared {
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
        if !source_parent.is_empty() && target_parent != source_parent {
            let prepared = Prepared {
                key: marker_key(source_parent),
                body: BackendStream::empty(),
                length: 0,
                exists: false,
                version_id: None,
            };
            let put = self.put_config(open.owner.user, bucket, prepared).await?;
            rename = rename.with_parent_marker(put);
        }
        drive(rename, &self.driver).await.map_err(vfs_failure)?;
        // The instance at the destination tells whether the stage was published or rebound.
        let identity = match replaces {
            true => {
                let moved = FileOpen {
                    key: to.clone(),
                    ..open.clone()
                };
                let found = self.summary(&moved).await.ok().flatten();
                found.map(|found| found.identity)
            }
            false => None,
        };
        let published = committed
            || matches!(
                identity,
                Some(HandleIdentity::File(FileIdentity::Committed { .. }))
            );
        self.update_opens(&open.bucket, open.instance_id, |other| {
            other.key = to.clone();
            if published {
                other.version_id = Some(attempt_id);
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
        let input = DispositionInput {
            context: self.open_context(open),
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
