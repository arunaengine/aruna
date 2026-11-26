//! Local bucket files and directories over the shared file server operations: lookup with live
//! rights, opens, exact-version reads, staged writes, resizes, listings and byte-range locks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::structs::storage::vfs::{BucketScope, HandleIdentity, HandleKind, StageOwner};
use aruna_core::vfs_effects::{
    ByteLock, InstanceSummary, ReadLockCheck, ShareMode, VfsFailure, VfsProtocol,
};
use aruna_ops_data::vfs::io::{
    LockInput, LockOutcome, LockRequest, OpenInput, ReadInput, StageChange, WriteInput,
    WriteResult, WriteStable,
};
use aruna_ops_data::vfs::path::marker_key;
use aruna_ops_data::vfs::policy::{VfsAccess, VfsRequest};
use aruna_ops_data::vfs::{
    InstanceTarget, LookupInstanceOperation, VfsAccessContext, VfsError, VfsLockOperation,
    VfsOpenOperation, VfsReadOperation, VfsWriteOperation, stage_scope,
};
use aruna_runner::driver::drive;
use bytes::Bytes;
use ulid::Ulid;

use super::names::{Resolution, cursor_of};
use super::{ArunaSmbBackend, Entry, FileOpen, RootOpen, file_info, object_kind, vfs_failure};
use crate::access::{
    DELETE, FILE_APPEND_DATA, FILE_EXECUTE, FILE_READ_DATA, FILE_WRITE_DATA, Rights, WriteMode,
    append_allowed,
};
use crate::backend::{
    BackendError, BasicChange, CreateAction, DirectoryEntry, Disposition, FileInfo, ListPage,
    LockRange, Lookup, ObjectKind, OpenGrant, OpenRequest, Scope,
};

const SHARE_READ: u32 = 0x1;
const SHARE_WRITE: u32 = 0x2;
const SHARE_DELETE: u32 = 0x4;
/// First and longest pause between tries of a waiting byte-range lock.
const LOCK_PAUSE_MIN: Duration = Duration::from_millis(10);
const LOCK_PAUSE_MAX: Duration = Duration::from_millis(500);

/// The lock owner of one SMB open; it matches the owner the file server releases on close.
pub(crate) fn lock_owner(open_id: Ulid) -> u64 {
    let value = u128::from(open_id);
    (value as u64) ^ ((value >> 64) as u64)
}

fn share_mode(granted: u32, share_access: u32) -> ShareMode {
    ShareMode {
        read_access: granted & (FILE_READ_DATA | FILE_EXECUTE) != 0,
        write_access: granted & (FILE_WRITE_DATA | FILE_APPEND_DATA) != 0,
        delete_access: granted & DELETE != 0,
        share_read: share_access & SHARE_READ != 0,
        share_write: share_access & SHARE_WRITE != 0,
        share_delete: share_access & SHARE_DELETE != 0,
    }
}

/// `open` with the name, identity and base version its shared instance has now; another
/// protocol may have published or renamed it since the open.
pub(super) fn current_open(open: &FileOpen, summary: Option<&InstanceSummary>) -> FileOpen {
    let mut open = open.clone();
    if let Some(summary) = summary {
        open.key = summary.key.clone();
        open.identity = summary.identity.clone();
        open.version_id = summary.base_version.or(open.version_id);
    }
    open
}

/// The time an object shows: its version's, else when it was opened.
fn shown_time(version_id: Option<Ulid>, fallback: u64) -> u64 {
    version_id.map_or(fallback, |version| version.timestamp_ms())
}

impl ArunaSmbBackend {
    /// Each right on its own, from live admission of the exact resolved path.
    async fn rights_of(
        &self,
        context: &VfsAccessContext,
        bucket: &str,
        key: &str,
        kind: HandleKind,
    ) -> Result<Rights, BackendError> {
        if key.is_empty() {
            let mut list = VfsRequest::new(VfsAccess::List, bucket, None);
            list.prefix = Some(String::new());
            let put = VfsRequest::new(VfsAccess::Put, bucket, None);
            return Ok(Rights {
                read: self.allows(context, list).await?,
                write: self.allows(context, put).await?,
                delete: false,
            });
        }
        let marker = marker_key(key);
        let (read, target) = match kind {
            HandleKind::File => (VfsRequest::new(VfsAccess::Get, bucket, Some(key)), key),
            HandleKind::Directory => {
                let mut list = VfsRequest::new(VfsAccess::List, bucket, None);
                list.prefix = Some(marker.clone());
                (list, marker.as_str())
            }
        };
        let put = VfsRequest::new(VfsAccess::Put, bucket, Some(target));
        let delete = VfsRequest::new(VfsAccess::Delete, bucket, Some(target));
        Ok(Rights {
            read: self.allows(context, read).await?,
            write: self.allows(context, put).await?,
            delete: self.allows(context, delete).await?,
        })
    }

    pub(super) async fn lookup_file(
        &self,
        scope: &Scope,
        bucket: &str,
        path: &str,
    ) -> Result<Lookup, BackendError> {
        let access_key = &scope.credential.access_key;
        let incarnation = self.incarnation(access_key, bucket).await?;
        let context = self.context(access_key, bucket, incarnation);
        match self.resolve(&context, path).await? {
            Resolution::Found(found) => {
                let kind = found.entry.kind;
                let rights = self.rights_of(&context, bucket, &found.key, kind).await?;
                Ok(Lookup {
                    kind: Some(object_kind(kind)),
                    rights,
                })
            }
            Resolution::Missing(key) => {
                let rights = self
                    .rights_of(&context, bucket, &key, HandleKind::File)
                    .await?;
                Ok(Lookup { kind: None, rights })
            }
        }
    }

    /// Opens the share root directory without a directory witness, so an empty bucket works.
    async fn open_root(
        &self,
        context: &VfsAccessContext,
        access_key: String,
        bucket: &str,
        incarnation: u128,
        request: &OpenRequest,
    ) -> Result<OpenGrant, BackendError> {
        if request.disposition == Disposition::Create {
            return Err(BackendError::Exists);
        }
        if request.delete_on_close {
            return Err(BackendError::Denied);
        }
        let mut list = VfsRequest::new(VfsAccess::List, bucket, None);
        list.prefix = Some(String::new());
        if !self.allows(context, list).await? {
            return Err(BackendError::Denied);
        }
        let opened_ms = aruna_clock::unix_timestamp_millis();
        let root = RootOpen {
            access_key,
            bucket: bucket.to_string(),
            incarnation,
            opened_ms,
        };
        let info = file_info(ObjectKind::Directory, 0, opened_ms, false);
        let handle = self.store(Entry::Root(root));
        Ok(OpenGrant {
            handle,
            action: CreateAction::Opened,
            info,
        })
    }

    pub(super) async fn open_file(
        &self,
        scope: &Scope,
        bucket: &str,
        request: OpenRequest,
    ) -> Result<OpenGrant, BackendError> {
        let access_key = scope.credential.access_key.clone();
        let incarnation = self.incarnation(&access_key, bucket).await?;
        let context = self.context(&access_key, bucket, incarnation);
        let (key, kind, version_id, existed) = match self.resolve(&context, &request.path).await? {
            Resolution::Found(found) => (found.key, found.entry.kind, found.entry.version_id, true),
            Resolution::Missing(key) => {
                let kind = match request.kind {
                    Some(ObjectKind::Directory) => HandleKind::Directory,
                    _ => HandleKind::File,
                };
                (key, kind, None, false)
            }
        };
        if key.is_empty() {
            return self
                .open_root(&context, access_key, bucket, incarnation, &request)
                .await;
        }
        match (existed, request.disposition) {
            (false, disposition) if !disposition.creates() => return Err(BackendError::NotFound),
            (true, Disposition::Create) => return Err(BackendError::Exists),
            (false, disposition) if kind == HandleKind::Directory => {
                match self.make_directory(&context, &key).await {
                    Ok(()) => {}
                    Err(BackendError::Exists) if disposition != Disposition::Create => {}
                    Err(error) => return Err(error),
                }
            }
            _ => {}
        }
        let file = kind == HandleKind::File;
        let truncate = existed && file && request.disposition.truncates();
        let writes = request.granted & (FILE_WRITE_DATA | FILE_APPEND_DATA) != 0;
        let write = file && (writes || truncate || !existed);
        let open_id = Ulid::generate();
        let input = OpenInput {
            context: context.clone(),
            key: key.clone(),
            kind,
            create: file && !existed,
            exclusive: file && request.disposition == Disposition::Create,
            write,
            share: share_mode(request.granted, request.share_access),
            delete_on_close: request.delete_on_close,
            open_id,
            stable: false,
            mode: 0,
            mount_id: Ulid::nil(),
            stage_scope: stage_scope(&access_key),
        };
        let opened = drive(VfsOpenOperation::new(input), &self.driver)
            .await
            .map_err(vfs_failure)?;
        let opened_ms = aruna_clock::unix_timestamp_millis();
        let open = FileOpen {
            access_key: access_key.clone(),
            bucket: bucket.to_string(),
            incarnation: opened.grant.incarnation,
            key,
            kind,
            open_id,
            instance_id: opened.instance_id,
            identity: opened.identity,
            version_id,
            write,
            delete_on_close: request.delete_on_close,
            owner: StageOwner {
                user: opened.grant.owner,
                access_key: access_key.clone(),
                mount_id: Ulid::nil(),
                scope: stage_scope(&access_key),
            },
            info: opened.grant.bucket,
            opened_ms,
        };
        let mut size = opened.size;
        let mut action = match existed {
            true => CreateAction::Opened,
            false => CreateAction::Created,
        };
        if truncate {
            // An open that cannot replace the bytes it promised is closed again.
            if let Err(error) = self
                .change_stage(&open, StageChange::Truncate { size: 0 })
                .await
            {
                let _ = self.close_file(open).await;
                return Err(error);
            }
            size = 0;
            action = match request.disposition {
                Disposition::Supersede => CreateAction::Superseded,
                _ => CreateAction::Overwritten,
            };
        }
        let time = shown_time(open.version_id, opened_ms);
        let info = file_info(object_kind(kind), size, time, false);
        let handle = self.store(Entry::Bucket(Box::new(open)));
        Ok(OpenGrant {
            handle,
            action,
            info,
        })
    }

    /// The live or detached shared instance the open joined, found by its stable id.
    pub(super) async fn summary(
        &self,
        open: &FileOpen,
    ) -> Result<Option<InstanceSummary>, BackendError> {
        let scope = BucketScope::new(open.bucket.clone(), open.incarnation);
        let lookup =
            LookupInstanceOperation::new(scope, InstanceTarget::Instance(open.instance_id));
        let found = drive(lookup, &self.driver).await.map_err(vfs_failure)?;
        Ok(found.filter(|summary| summary.instance_id == open.instance_id))
    }

    /// The open as its shared instance has it now, together with that instance.
    pub(super) async fn current(
        &self,
        open: &FileOpen,
    ) -> Result<(FileOpen, Option<InstanceSummary>), BackendError> {
        let summary = self.summary(open).await?;
        Ok((current_open(open, summary.as_ref()), summary))
    }

    pub(super) async fn read_file(
        &self,
        open: &FileOpen,
        offset: u64,
        length: u32,
    ) -> Result<Bytes, BackendError> {
        if open.kind == HandleKind::Directory {
            return Err(BackendError::IsDirectory);
        }
        // The shared instance names the base the other protocol last published or renamed to;
        // a detached instance still serves its existing opens.
        let (open, _) = self.current(open).await?;
        let input = ReadInput {
            context: self.open_context(&open),
            key: open.key.clone(),
            instance_id: Some(open.instance_id),
            version_id: open.version_id,
            offset,
            length,
            lock_check: ReadLockCheck::Owner(lock_owner(open.open_id)),
        };
        let read = drive(VfsReadOperation::new(input), &self.driver)
            .await
            .map_err(vfs_failure)?;
        Ok(Bytes::from(read.data))
    }

    /// One staged change of an open writer under the instance's file guard.
    async fn change_stage(
        &self,
        open: &FileOpen,
        change: StageChange,
    ) -> Result<WriteResult, BackendError> {
        if !open.write || open.kind == HandleKind::Directory {
            return Err(BackendError::Denied);
        }
        let (open, _) = self.current(open).await?;
        let input = WriteInput {
            context: self.open_context(&open),
            key: open.key.clone(),
            instance_id: open.instance_id,
            owner_id: lock_owner(open.open_id),
            change,
            stable: WriteStable::Unstable,
            anonymous: false,
            checkpoint: None,
        };
        drive(VfsWriteOperation::new(input), &self.driver)
            .await
            .map_err(vfs_failure)
    }

    /// A write at the shared end of file lands at the end under the file guard; an append-only
    /// open may not name an offset below it.
    pub(super) async fn write_file(
        &self,
        open: &FileOpen,
        request: crate::backend::WriteRequest,
    ) -> Result<u32, BackendError> {
        let (offset, append) = match (request.offset, request.mode) {
            (None, _) => (0, true),
            (Some(offset), WriteMode::Append) => {
                let summary = self.summary(open).await?;
                let size = summary.map_or(0, |summary| summary.logical_size);
                if !append_allowed(offset, size) {
                    return Err(BackendError::Denied);
                }
                (offset, true)
            }
            (Some(offset), WriteMode::Overwrite) => (offset, false),
        };
        let data = request.data.to_vec();
        let change = StageChange::Write {
            offset,
            data,
            append,
        };
        let written = self.change_stage(open, change).await?;
        if request.through || written.publish {
            self.flush_file(open).await?;
        }
        Ok(written.written)
    }

    pub(super) async fn resize_file(&self, open: &FileOpen, size: u64) -> Result<(), BackendError> {
        let resized = self
            .change_stage(open, StageChange::Truncate { size })
            .await?;
        match resized.publish {
            true => self.flush_file(open).await,
            false => Ok(()),
        }
    }

    /// Times and attributes are not stored; the change is admitted and then has no effect.
    pub(super) async fn touch_file(
        &self,
        open: &FileOpen,
        _change: BasicChange,
    ) -> Result<(), BackendError> {
        let (open, _) = self.current(open).await?;
        let head = VfsRequest::new(VfsAccess::Head, &open.bucket, Some(&open.key));
        match self.allows(&self.open_context(&open), head).await? {
            true => Ok(()),
            false => Err(BackendError::Denied),
        }
    }

    pub(super) async fn file_status(&self, open: &FileOpen) -> Result<FileInfo, BackendError> {
        let (open, summary) = self.current(open).await?;
        let head = VfsRequest::new(VfsAccess::Head, &open.bucket, Some(&open.key));
        if !self.allows(&self.open_context(&open), head).await? {
            return Err(BackendError::Denied);
        }
        let summary = summary.ok_or(BackendError::NotFound)?;
        let time = shown_time(open.version_id, open.opened_ms);
        let size = match open.kind {
            HandleKind::File => summary.logical_size,
            HandleKind::Directory => 0,
        };
        let kind = object_kind(open.kind);
        Ok(file_info(kind, size, time, summary.delete_pending))
    }

    pub(super) async fn root_status(&self, open: &RootOpen) -> Result<FileInfo, BackendError> {
        let context = self.context(&open.access_key, &open.bucket, open.incarnation);
        let mut list = VfsRequest::new(VfsAccess::List, &open.bucket, None);
        list.prefix = Some(String::new());
        if !self.allows(&context, list).await? {
            return Err(BackendError::Denied);
        }
        Ok(file_info(ObjectKind::Directory, 0, open.opened_ms, false))
    }

    fn list_page(listing: aruna_ops_data::vfs::handles::VfsListing, opened_ms: u64) -> ListPage {
        let entries = listing.entries.into_iter().map(|entry| {
            let time = shown_time(entry.version_id, opened_ms);
            DirectoryEntry {
                info: file_info(object_kind(entry.kind), entry.size, time, false),
                name: entry.name,
            }
        });
        ListPage {
            entries: entries.collect(),
            next: cursor_of(&listing.next),
        }
    }

    pub(super) async fn list_root(
        &self,
        open: &RootOpen,
        cursor: Option<String>,
    ) -> Result<ListPage, BackendError> {
        let context = self.context(&open.access_key, &open.bucket, open.incarnation);
        let listing = self
            .list_slice(&context, "", None, cursor.as_deref())
            .await?;
        Ok(Self::list_page(listing, open.opened_ms))
    }

    pub(super) async fn list_files(
        &self,
        open: &FileOpen,
        cursor: Option<String>,
    ) -> Result<ListPage, BackendError> {
        let (open, _) = self.current(open).await?;
        let HandleIdentity::Directory(witness) = &open.identity else {
            return Err(BackendError::NotDirectory);
        };
        let context = self.open_context(&open);
        let listing = self
            .list_slice(
                &context,
                &open.key,
                Some(witness.clone()),
                cursor.as_deref(),
            )
            .await?;
        Ok(Self::list_page(listing, open.opened_ms))
    }

    fn byte_lock(open: &FileOpen, range: &LockRange) -> ByteLock {
        ByteLock {
            owner: lock_owner(open.open_id),
            offset: range.offset,
            length: range.length,
            exclusive: range.exclusive,
            protocol: VfsProtocol::Smb,
        }
    }

    async fn lock_request(
        &self,
        open: &FileOpen,
        request: LockRequest,
    ) -> Result<LockOutcome, VfsError> {
        let input = LockInput {
            context: self.open_context(open),
            key: open.key.clone(),
            instance_id: open.instance_id,
            detached: None,
            request,
        };
        drive(VfsLockOperation::new(input), &self.driver).await
    }

    /// Takes all ranges or none; a waiting request tries again until granted or dropped.
    pub(super) async fn lock_file(
        &self,
        open: &FileOpen,
        ranges: Vec<LockRange>,
        wait: bool,
    ) -> Result<(), BackendError> {
        let locks: Vec<ByteLock> = ranges
            .iter()
            .map(|range| Self::byte_lock(open, range))
            .collect();
        let mut pause = LOCK_PAUSE_MIN;
        loop {
            let request = LockRequest::Lock {
                locks: locks.clone(),
                atomic: true,
            };
            // Each try admits the name the instance has now; another protocol may rename it.
            let (live, _) = self.current(open).await?;
            match self.lock_request(&live, request).await {
                Ok(LockOutcome::Done | LockOutcome::Free) => return Ok(()),
                Ok(LockOutcome::Conflict(_)) | Err(VfsError::LockConflict) if wait => {
                    tokio::time::sleep(pause).await;
                    pause = (pause * 2).min(LOCK_PAUSE_MAX);
                }
                Ok(LockOutcome::Conflict(_)) | Err(VfsError::LockConflict) => {
                    return Err(BackendError::LockRefused);
                }
                Err(error) => return Err(vfs_failure(error)),
            }
        }
    }

    /// SMB unlocks name the range only, so a range is released whichever mode it was taken in.
    pub(super) async fn unlock_file(
        &self,
        open: &FileOpen,
        ranges: Vec<LockRange>,
    ) -> Result<(), BackendError> {
        let (open, _) = self.current(open).await?;
        let open = &open;
        for range in &ranges {
            let lock = Self::byte_lock(open, range);
            let other = ByteLock {
                exclusive: !lock.exclusive,
                ..lock
            };
            let mut released = false;
            for candidate in [lock, other] {
                let request = LockRequest::Unlock {
                    locks: vec![candidate],
                };
                match self.lock_request(open, request).await {
                    Ok(_) => {
                        released = true;
                        break;
                    }
                    Err(VfsError::Vfs(VfsFailure::NotFound)) => {}
                    Err(error) => return Err(vfs_failure(error)),
                }
            }
            if !released {
                return Err(BackendError::NotLocked);
            }
        }
        Ok(())
    }
}
