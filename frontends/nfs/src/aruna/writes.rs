//! Writers of live exports: shared opens and their share bits, staged writes and resizes
//! with durable checkpoints, publication at the last close, namespace changes and locks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::vfs::{
    DetachReason, DirectoryWitness, FileIdentity, HandleIdentity, HandleKind, NfsMountBinding,
    NfsStageRecord, StageBase, StageDisposition, StageOwner, StageScope,
};
use aruna_core::vfs_effects::{ByteLock, InstanceSummary, ReadLockCheck, VfsFailure, VfsProtocol};
use aruna_ops_data::s3::object::put::PutObjectConfig;
use aruna_ops_data::vfs::handles::HandleClaim;
use aruna_ops_data::vfs::io::{
    CommitInput, LockInput, LockOutcome, LockRequest, OpenInput, OpenResult, ReadInput,
    StageChange, WriteInput, WriteResult, WriteStable,
};
use aruna_ops_data::vfs::mutate::{MkdirInput, RemoveInput, RenameInput};
use aruna_ops_data::vfs::path::{NameUse, check_component, child_key, split_parent};
use aruna_ops_data::vfs::publish::{CloseInput, CloseOutcome, PublishInput};
use aruna_ops_data::vfs::{
    AllocateHandleOperation, InstanceTarget, LiveStageOperation, LookupInstanceOperation,
    QuotaStageOperation, UpdateShareOperation, VfsCloseOperation, VfsCommitOperation, VfsError,
    VfsLockOperation, VfsMkdirOperation, VfsOpenOperation, VfsReadOperation, VfsRemoveOperation,
    VfsRenameOperation, VfsWriteOperation,
};
use aruna_runner::driver::drive;
use bytes::Bytes;
use tracing::debug;
use ulid::Ulid;

use super::bodies::image_body;
use super::exports::{
    Located, bucket_scope, display_mode, holds, lookup_status, share_mode, status_of,
};
use super::{ArunaNfsBackend, OpenSlot};
use crate::backend::{
    AttributeChange, Caller, ChangeInfo, CreateMode, CreateReply, IoState, LockDenial, LockError,
    LockKind, LockRange, ObjectRef, OwnerKey, ReadReply, ReadSource, SetTime, ShareRequest,
    WriteReply,
};
use crate::types::{IO_LIMIT, NfsStat4, StableHow, share};

/// The byte-range lock and stage writer number of an NFS owner, scoped to its client.
pub(super) fn owner_number(owner: &OwnerKey) -> u64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&owner.client_id.to_be_bytes());
    hasher.update(&owner.owner);
    let mut number = [0; 8];
    number.copy_from_slice(&hasher.finalize().as_bytes()[..8]);
    u64::from_be_bytes(number)
}

/// The writer number of an open the backend holds for itself.
fn open_number(open_id: Ulid) -> u64 {
    open_id.0 as u64
}

/// The byte I/O owner of a stateid: its lock owner, else its open owner.
fn io_owner(owner: &OwnerKey, lock_owner: Option<&OwnerKey>) -> u64 {
    owner_number(lock_owner.unwrap_or(owner))
}

/// How a client READ under `state` meets exclusive byte-range locks: the bypass stateid
/// ignores them, the anonymous one conflicts with any, an open with any other owner's.
pub(super) fn lock_check(state: &IoState) -> ReadLockCheck {
    match state {
        IoState::Anonymous => ReadLockCheck::Anonymous,
        IoState::Bypass => ReadLockCheck::Bypass,
        IoState::Open { owner, lock_owner } => {
            ReadLockCheck::Owner(io_owner(owner, lock_owner.as_ref()))
        }
    }
}

/// `slot` with the name, identity, base version and size its shared instance has now.
fn current_slot(slot: &OpenSlot, summary: Option<&InstanceSummary>) -> OpenSlot {
    let mut slot = slot.clone();
    if let Some(summary) = summary {
        slot.key = summary.key.clone();
        slot.identity = summary.identity.clone();
        slot.version_id = summary.base_version.or(slot.version_id);
        slot.size = summary.logical_size;
    }
    slot
}

/// The answer to a close whose open ended; a publication that did not happen is an error,
/// except for a detached instance, which has no name left to publish to.
fn close_status(outcome: CloseOutcome, detached: bool) -> Result<(), NfsStat4> {
    match outcome {
        CloseOutcome::Published(_)
        | CloseOutcome::Closed
        | CloseOutcome::Deleted
        | CloseOutcome::Discarded => Ok(()),
        CloseOutcome::Pending if detached => Ok(()),
        CloseOutcome::Pending => Err(NfsStat4::Delay),
        CloseOutcome::QuotaBlocked => Err(NfsStat4::QuotaExceeded),
        // The acknowledged bytes stay retained for recovery, but the name did not get them.
        CloseOutcome::Retained(_) => Err(NfsStat4::Io),
    }
}

fn stable_level(stable: StableHow) -> WriteStable {
    match stable {
        StableHow::Unstable => WriteStable::Unstable,
        StableHow::DataSync => WriteStable::DataSync,
        StableHow::FileSync => WriteStable::FileSync,
    }
}

/// What an open's stage continues; a committed base needs its exact version.
fn stage_base(slot: &OpenSlot) -> Option<StageBase> {
    match &slot.identity {
        HandleIdentity::File(FileIdentity::Committed { life_id }) => {
            let life_id = *life_id;
            (slot.version_id).map(|version_id| StageBase::Committed {
                version_id,
                life_id,
            })
        }
        HandleIdentity::File(FileIdentity::Unpublished { token, predecessor }) => {
            Some(StageBase::Unpublished {
                token: *token,
                predecessor: *predecessor,
            })
        }
        HandleIdentity::Directory(_) => None,
    }
}

fn lock_denial(holder: ByteLock) -> LockDenial {
    LockDenial {
        range: LockRange {
            offset: holder.offset,
            length: holder.length,
        },
        kind: match holder.exclusive {
            true => LockKind::Write,
            false => LockKind::Read,
        },
        owner: OwnerKey {
            client_id: 0,
            owner: Bytes::copy_from_slice(&holder.owner.to_be_bytes()),
        },
    }
}

impl ArunaNfsBackend {
    /// The open slot of a fresh VFS open, with the base version its stage continues.
    async fn slot_of(
        &self,
        binding: &NfsMountBinding,
        key: &str,
        open_id: Ulid,
        opened: &OpenResult,
        (access, deny): (u32, u32),
    ) -> Result<OpenSlot, NfsStat4> {
        let version_id = match &opened.identity {
            HandleIdentity::File(FileIdentity::Committed { .. }) => {
                self.head_info(binding, key).await?.0
            }
            _ => None,
        };
        Ok(OpenSlot {
            open_id,
            instance_id: opened.instance_id,
            key: key.to_string(),
            write: access & share::WRITE != 0,
            identity: opened.identity.clone(),
            version_id,
            size: opened.size,
            access,
            deny,
        })
    }

    /// A new open of the file `located` names with these share bits; a name that now holds
    /// another identity than the handle is stale.
    async fn open_slot(
        &self,
        binding: &NfsMountBinding,
        located: &Located,
        (access, deny): (u32, u32),
    ) -> Result<OpenSlot, NfsStat4> {
        let open_id = Ulid::generate();
        let input = OpenInput {
            context: self.access_context(binding),
            key: located.key.clone(),
            kind: HandleKind::File,
            create: false,
            exclusive: false,
            write: access & share::WRITE != 0,
            share: share_mode(access, deny),
            delete_on_close: false,
            open_id,
            stable: true,
            mode: display_mode(binding, HandleKind::File),
            mount_id: binding.mount_id,
            stage_scope: StageScope::User,
        };
        let opened = drive(VfsOpenOperation::new(input), &self.context).await;
        let opened = opened.map_err(status_of)?;
        if located
            .identity
            .as_ref()
            .is_some_and(|identity| *identity != opened.identity)
        {
            self.discard_open(binding, open_id).await;
            return Err(NfsStat4::Stale);
        }
        let slot = self
            .slot_of(binding, &located.key, open_id, &opened, (access, deny))
            .await;
        if slot.is_err() {
            self.discard_open(binding, open_id).await;
        }
        slot
    }

    pub(super) async fn live_open(
        &self,
        binding: &NfsMountBinding,
        object: ObjectRef,
        share: &ShareRequest,
    ) -> Result<(), NfsStat4> {
        let key = (object, share.owner.clone());
        // The creating owner's OPEN takes over the open that already holds its modes.
        let leftover = {
            let mut tables = self.tables.lock();
            match tables.created.remove(&key) {
                Some(mut slot) if !tables.opens.contains_key(&key) => {
                    (slot.access, slot.deny) = (share.access, share.deny);
                    tables.closed_errors.remove(&key);
                    tables.opens.insert(key, slot);
                    return Ok(());
                }
                leftover => leftover,
            }
        };
        if let Some(slot) = leftover {
            self.release_quietly(binding, object, slot).await;
        }
        let located = self.locate(binding, object).await?;
        if located.kind == HandleKind::Directory {
            return Err(NfsStat4::IsDirectory);
        }
        // A detached file serves its existing opens only.
        if located.detached.is_some() {
            return Err(NfsStat4::Stale);
        }
        if share.access & share::WRITE != 0 && binding.read_only {
            return Err(NfsStat4::ReadOnly);
        }
        let existing = self.tables.lock().opens.get(&key).cloned();
        if existing
            .as_ref()
            .is_some_and(|slot| slot.access == share.access && slot.deny == share.deny)
        {
            return Ok(());
        }
        let modes = (share.access, share.deny);
        let slot = match self.open_slot(binding, &located, modes).await {
            // The owner's own earlier modes conflict with the new ones; they end first.
            Err(NfsStat4::ShareDenied) if existing.is_some() => {
                let old = self.tables.lock().opens.remove(&key);
                if let Some(old) = old {
                    self.release(binding, object, old).await?;
                }
                self.open_slot(binding, &located, modes).await?
            }
            other => other?,
        };
        let replaced = {
            let mut tables = self.tables.lock();
            tables.closed_errors.remove(&key);
            tables.opens.insert(key, slot)
        };
        if let Some(old) = replaced {
            self.release_quietly(binding, object, old).await;
        }
        Ok(())
    }

    pub(super) async fn live_close(
        &self,
        caller: &Caller,
        binding: &NfsMountBinding,
        object: ObjectRef,
        owner: &OwnerKey,
    ) -> Result<(), NfsStat4> {
        let key = (object, owner.clone());
        // The slot stays until its open has ended, so a CLOSE that failed first can be retried.
        let (slot, ended) = {
            let tables = self.tables.lock();
            let ended = tables.closed_errors.get(&key).copied();
            (tables.opens.get(&key).cloned(), ended)
        };
        let Some(slot) = slot else {
            // A retried CLOSE whose open already ended answers as the close that ended it.
            return ended.map_or(Ok(()), Err);
        };
        if !self.release_admitted(caller, binding).await? {
            self.revoked_close(binding, object, &slot).await?;
            self.tables.lock().opens.remove(&key);
            return Ok(());
        }
        let (input, detached) = self.close_input(binding, object, &slot).await?;
        let status = match drive(VfsCloseOperation::new(input), &self.context).await {
            Ok(outcome) => close_status(outcome, detached),
            // An earlier attempt ended the open, but its outcome never reached the client.
            Err(VfsError::Vfs(VfsFailure::NotFound)) => Err(NfsStat4::Io),
            Err(error) => return Err(status_of(error)),
        };
        let mut tables = self.tables.lock();
        tables.opens.remove(&key);
        if let Err(status) = status {
            tables.closed_errors.insert(key, status);
        }
        status
    }

    /// Ends an open whose binding or host was revoked without publishing through it: the open
    /// and its share end, and an acknowledged checkpoint the last writer leaves is retained.
    async fn revoked_close(
        &self,
        binding: &NfsMountBinding,
        object: ObjectRef,
        slot: &OpenSlot,
    ) -> Result<(), NfsStat4> {
        let scope = bucket_scope(binding).map_err(status_of)?;
        let read = LiveStageOperation::new(scope.clone(), object.object_id);
        let stage = drive(read, &self.context).await.map_err(status_of)?;
        let input = CloseInput {
            context: self.access_context(binding),
            scope: scope.clone(),
            open_id: slot.open_id,
            publish: None,
            stage: stage.clone(),
        };
        let stage = match drive(VfsCloseOperation::new(input), &self.context).await {
            Ok(CloseOutcome::Pending) => stage,
            // An earlier attempt ended the open; a live checkpoint of this binding stays.
            Err(VfsError::Vfs(VfsFailure::NotFound)) => {
                stage.filter(|stage| stage.owner.mount_id == binding.mount_id)
            }
            Ok(_) => None,
            Err(error) => return Err(status_of(error)),
        };
        let Some(stage) = stage else {
            return Ok(());
        };
        let retain = QuotaStageOperation::retain(scope, stage, DetachReason::LostLife)
            .with_instance(slot.instance_id);
        drive(retain, &self.context).await.map_err(status_of)
    }

    /// Ends one VFS open; the last dirty writer publishes the effective image.
    async fn release(
        &self,
        binding: &NfsMountBinding,
        object: ObjectRef,
        slot: OpenSlot,
    ) -> Result<(), NfsStat4> {
        let (input, detached) = self.close_input(binding, object, &slot).await?;
        let closed = drive(VfsCloseOperation::new(input), &self.context).await;
        close_status(closed.map_err(status_of)?, detached)
    }

    /// The shared instance of an open of this server now, by handle or else by name; another
    /// protocol may have published, renamed or detached it since the open.
    async fn shared_of(
        &self,
        binding: &NfsMountBinding,
        object: ObjectRef,
        slot: &OpenSlot,
    ) -> Result<Option<InstanceSummary>, NfsStat4> {
        let scope = bucket_scope(binding).map_err(status_of)?;
        let targets = [
            InstanceTarget::Handle(object.object_id),
            InstanceTarget::Key(slot.key.clone()),
        ];
        for target in targets {
            let lookup = LookupInstanceOperation::new(scope.clone(), target);
            let summary = drive(lookup, &self.context).await.map_err(status_of)?;
            if let Some(summary) = summary.filter(|found| found.instance_id == slot.instance_id) {
                return Ok(Some(summary));
            }
        }
        Ok(None)
    }

    /// The close of one open, with the publication a last close of a dirty instance runs, and
    /// whether the instance is detached.
    async fn close_input(
        &self,
        binding: &NfsMountBinding,
        object: ObjectRef,
        slot: &OpenSlot,
    ) -> Result<(CloseInput, bool), NfsStat4> {
        let scope = bucket_scope(binding).map_err(status_of)?;
        let summary = self.shared_of(binding, object, slot).await?;
        let detached = summary
            .as_ref()
            .is_some_and(|found| found.detached.is_some());
        let dirty = summary.as_ref().is_some_and(|found| found.dirty);
        let (publish, stage) = match slot.write || dirty {
            true => {
                let read = LiveStageOperation::new(scope.clone(), object.object_id);
                let stage = drive(read, &self.context).await.map_err(status_of)?;
                let publish = self
                    .publication(binding, object, slot, (stage.clone(), summary))
                    .await?;
                (publish, stage)
            }
            false => (None, None),
        };
        let input = CloseInput {
            context: self.access_context(binding),
            scope,
            open_id: slot.open_id,
            publish,
            stage,
        };
        Ok((input, detached))
    }

    async fn release_quietly(&self, binding: &NfsMountBinding, object: ObjectRef, slot: OpenSlot) {
        if let Err(status) = self.release(binding, object, slot).await {
            debug!(status = ?status, "NFS replaced open did not close cleanly");
        }
    }

    /// The publication a last close of a dirty instance runs: the stage image as the body of an
    /// ordinary write, read in bounded chunks only while the write consumes it. A detached
    /// instance has no name left, so its close prepares none.
    async fn publication(
        &self,
        binding: &NfsMountBinding,
        object: ObjectRef,
        slot: &OpenSlot,
        (stage, summary): (Option<NfsStageRecord>, Option<InstanceSummary>),
    ) -> Result<Option<(PublishInput, PutObjectConfig)>, NfsStat4> {
        if summary
            .as_ref()
            .is_some_and(|found| found.detached.is_some())
        {
            return Ok(None);
        }
        let slot = current_slot(slot, summary.as_ref());
        let Some(base) = stage_base(&slot) else {
            return Ok(None);
        };
        let size = slot.size;
        let read = ReadInput {
            context: self.access_context(binding),
            key: slot.key.clone(),
            instance_id: Some(slot.instance_id),
            version_id: slot.version_id,
            offset: 0,
            length: 0,
            lock_check: ReadLockCheck::Bypass,
        };
        let body = image_body(self.context.clone(), read, size);
        let exists = matches!(base, StageBase::Committed { .. });
        let put = self
            .put_config(binding, &slot.key, body, (size, exists))
            .await?;
        let input = PublishInput {
            context: self.access_context(binding),
            instance_id: slot.instance_id,
            key: slot.key.clone(),
            handle_id: Some(object.object_id),
            base,
            replaces: false,
            stage,
            logical_size: size,
            attempt_id: Ulid::generate(),
            updates: Vec::new(),
            expected_edit: summary.map(|found| found.edit_generation),
            require_writerless: false,
        };
        Ok(Some((input, put)))
    }

    /// The next durable checkpoint generation of an open's stage, after the live row.
    async fn checkpoint(
        &self,
        binding: &NfsMountBinding,
        object: ObjectRef,
        slot: &OpenSlot,
    ) -> Result<Option<CommitInput>, NfsStat4> {
        let Some(base) = stage_base(slot) else {
            return Ok(None);
        };
        let scope = bucket_scope(binding).map_err(status_of)?;
        let read = LiveStageOperation::new(scope, object.object_id);
        let previous = drive(read, &self.context).await.map_err(status_of)?;
        let summary = self.shared_of(binding, object, slot).await?;
        let now_ms = aruna_clock::unix_timestamp_millis();
        let owner = StageOwner {
            user: binding.owner,
            access_key: binding.access_key.clone(),
            mount_id: binding.mount_id,
            scope: StageScope::User,
        };
        let known = previous.as_ref().map_or(0, |record| record.generation);
        let running = summary.as_ref().map_or(0, |summary| summary.generation);
        let generation = known.max(running) + 1;
        let record = match previous.clone() {
            Some(mut record) => {
                record.generation = generation;
                record.owner = owner;
                record.logical_size = slot.size;
                record.updated_at_ms = now_ms;
                record
            }
            None => NfsStageRecord {
                stage_id: summary
                    .and_then(|summary| summary.stage_id)
                    .unwrap_or_else(Ulid::generate),
                handle_id: object.object_id,
                owner,
                key: slot.key.clone(),
                generation,
                checkpoint: String::new(),
                logical_size: slot.size,
                base,
                source_copies: Vec::new(),
                source_policies: Vec::new(),
                destination_copies: Vec::new(),
                destination_policies: Vec::new(),
                disposition: StageDisposition::Live,
                quota_retry: None,
                foreign_life: None,
                updated_at_ms: now_ms,
            },
        };
        Ok(Some(CommitInput {
            context: self.access_context(binding),
            instance_id: slot.instance_id,
            record,
            previous,
        }))
    }

    /// The writer open a WRITE or size change runs under: the owner's own, or for the special
    /// stateids a temporary one the call closes again, which publishes before it answers.
    async fn writer(
        &self,
        binding: &NfsMountBinding,
        object: ObjectRef,
        located: &Located,
        state: &IoState,
    ) -> Result<(OpenSlot, u64, bool), NfsStat4> {
        if binding.read_only {
            return Err(NfsStat4::ReadOnly);
        }
        match state {
            IoState::Open { owner, lock_owner } => {
                let slot = self
                    .tables
                    .lock()
                    .opens
                    .get(&(object, owner.clone()))
                    .cloned();
                let slot = slot.filter(|slot| slot.write).ok_or(NfsStat4::OpenMode)?;
                let summary = self.shared_of(binding, object, &slot).await?;
                let slot = current_slot(&slot, summary.as_ref());
                Ok((slot, io_owner(owner, lock_owner.as_ref()), false))
            }
            IoState::Anonymous | IoState::Bypass => {
                let slot = self.open_slot(binding, located, (share::WRITE, 0)).await?;
                let number = open_number(slot.open_id);
                Ok((slot, number, true))
            }
        }
    }

    /// One staged change under its writer; a stable change or any resize is checkpointed.
    async fn apply_change(
        &self,
        binding: &NfsMountBinding,
        object: ObjectRef,
        (slot, owner_id): (&OpenSlot, u64),
        change: StageChange,
        stable: WriteStable,
    ) -> Result<WriteResult, NfsStat4> {
        let resize = matches!(change, StageChange::Truncate { .. });
        let checkpoint = match stable != WriteStable::Unstable || resize {
            true => self.checkpoint(binding, object, slot).await?,
            false => None,
        };
        let input = WriteInput {
            context: self.access_context(binding),
            key: slot.key.clone(),
            instance_id: slot.instance_id,
            owner_id,
            change,
            stable,
            anonymous: false,
            checkpoint,
        };
        let written = drive(VfsWriteOperation::new(input), &self.context).await;
        let written = written.map_err(status_of)?;
        let mut guard = self.tables.lock();
        let tables = &mut *guard;
        tables.write_verifier = Some(written.verifier);
        let opens = tables.opens.values_mut().chain(tables.created.values_mut());
        for open in opens.filter(|open| open.instance_id == slot.instance_id) {
            open.size = written.new_size;
        }
        Ok(written)
    }

    /// Closes a temporary writer after its change; its close publishes the change.
    async fn finish_writer<T>(
        &self,
        binding: &NfsMountBinding,
        object: ObjectRef,
        (slot, temporary): (OpenSlot, bool),
        outcome: Result<T, NfsStat4>,
    ) -> Result<T, NfsStat4> {
        if !temporary {
            return outcome;
        }
        let released = self.release(binding, object, slot).await;
        let value = outcome?;
        released.map(|()| value)
    }

    pub(super) async fn live_read(
        &self,
        binding: &NfsMountBinding,
        object: ObjectRef,
        state: &IoState,
        offset: u64,
        count: u32,
    ) -> Result<ReadReply, NfsStat4> {
        let located = self.locate(binding, object).await?;
        if located.kind == HandleKind::Directory {
            return Err(NfsStat4::IsDirectory);
        }
        let own = match state {
            IoState::Open { owner, .. } => {
                let tables = self.tables.lock();
                tables.opens.get(&(object, owner.clone())).cloned()
            }
            IoState::Anonymous | IoState::Bypass => None,
        };
        let mut key = located.key;
        let (instance_id, version_id) = match (own, &located.detached) {
            (Some(slot), _) => {
                let summary = self.shared_of(binding, object, &slot).await?;
                let slot = current_slot(&slot, summary.as_ref());
                key = slot.key;
                (Some(slot.instance_id), slot.version_id)
            }
            (None, Some(_)) => return Err(NfsStat4::Stale),
            (None, None) => {
                let identity = located.identity.as_ref();
                let summary = self.instance_at(binding, &key, identity).await?;
                let committed = matches!(
                    identity,
                    Some(HandleIdentity::File(FileIdentity::Committed { .. }))
                );
                let base = summary.as_ref().and_then(|found| found.base_version);
                let version_id = match (committed, base) {
                    (true, Some(base)) => Some(base),
                    (true, None) => self.head_info(binding, &key).await?.0,
                    (false, _) => None,
                };
                (summary.map(|summary| summary.instance_id), version_id)
            }
        };
        let input = ReadInput {
            context: self.access_context(binding),
            key: key.clone(),
            instance_id,
            version_id,
            offset,
            length: count.min(IO_LIMIT),
            lock_check: lock_check(state),
        };
        let read = drive(VfsReadOperation::new(input), &self.context).await;
        let read = read.map_err(status_of)?;
        Ok(ReadReply {
            data: read.data.into(),
            eof: read.eof,
            source: Some(ReadSource {
                key,
                version_id: read.version_id,
                instance_id,
            }),
        })
    }

    /// Reads the exact bytes a cached READ served again, admitted as a fresh read of that
    /// version or instance; a withdrawn version or a gone instance refuses the replay.
    pub(super) async fn replay_read(
        &self,
        binding: &NfsMountBinding,
        source: &ReadSource,
        (offset, length): (u64, u32),
    ) -> Result<(), NfsStat4> {
        let pinned = source.version_id.is_some() || source.instance_id.is_some();
        if !pinned || !holds(binding, &source.key) {
            return Err(NfsStat4::Access);
        }
        let input = ReadInput {
            context: self.access_context(binding),
            key: source.key.clone(),
            instance_id: source.instance_id,
            version_id: source.version_id,
            offset,
            length: length.min(IO_LIMIT),
            lock_check: ReadLockCheck::Bypass,
        };
        let read = drive(VfsReadOperation::new(input), &self.context).await;
        read.map(|_| ()).map_err(status_of)
    }

    pub(super) async fn live_write(
        &self,
        binding: &NfsMountBinding,
        object: ObjectRef,
        state: &IoState,
        (offset, data, stable): (u64, Bytes, StableHow),
    ) -> Result<WriteReply, NfsStat4> {
        let located = self.locate(binding, object).await?;
        if located.kind == HandleKind::Directory {
            return Err(NfsStat4::IsDirectory);
        }
        let (mut slot, owner_id, temporary) = self.writer(binding, object, &located, state).await?;
        let end = offset.saturating_add(data.len() as u64);
        slot.size = slot.size.max(end);
        let change = StageChange::Write {
            offset,
            data: data.to_vec(),
            append: false,
        };
        // A temporary writer publishes on close, so its change is checkpointed first.
        let level = match temporary {
            true => WriteStable::FileSync,
            false => stable_level(stable),
        };
        let outcome = self
            .apply_change(binding, object, (&slot, owner_id), change, level)
            .await;
        let written = self
            .finish_writer(binding, object, (slot, temporary), outcome)
            .await?;
        let committed = match written.committed || temporary {
            true => stable,
            false => StableHow::Unstable,
        };
        Ok(WriteReply {
            count: written.written,
            committed,
            verifier: written.verifier,
        })
    }

    pub(super) async fn live_commit(
        &self,
        binding: &NfsMountBinding,
        object: ObjectRef,
    ) -> Result<[u8; 8], NfsStat4> {
        self.locate(binding, object).await?;
        let found = self.writer_slot(object);
        let Some(slot) = found else {
            // Nothing is staged by a writer of this server; earlier writes were published.
            let tables = self.tables.lock();
            return Ok(tables.write_verifier.unwrap_or(self.verifier));
        };
        let summary = self.shared_of(binding, object, &slot).await?;
        let slot = current_slot(&slot, summary.as_ref());
        let checkpoint = self.checkpoint(binding, object, &slot).await?;
        let checkpoint = checkpoint.ok_or(NfsStat4::Delay)?;
        let committed = drive(VfsCommitOperation::new(checkpoint), &self.context).await;
        let verifier = committed.map_err(status_of)?;
        self.tables.lock().write_verifier = Some(verifier);
        Ok(verifier)
    }

    /// Any writer open of this server on `object`.
    fn writer_slot(&self, object: ObjectRef) -> Option<OpenSlot> {
        let tables = self.tables.lock();
        let mut slots = tables.opens.iter().chain(tables.created.iter());
        let found = slots.find(|((open, _), slot)| *open == object && slot.write);
        found.map(|(_, slot)| slot.clone())
    }

    pub(super) async fn live_setattr(
        &self,
        binding: &NfsMountBinding,
        object: ObjectRef,
        state: &IoState,
        change: &AttributeChange,
    ) -> Result<(), NfsStat4> {
        let located = self.locate(binding, object).await?;
        // Only changes this server keeps are reported as applied.
        let shown = display_mode(binding, located.kind);
        if change.mode.is_some_and(|mode| mode & 0o7777 != shown) {
            return Err(NfsStat4::AttributeUnsupported);
        }
        let client_time = |time: Option<SetTime>| matches!(time, Some(SetTime::Client(_)));
        if client_time(change.accessed) || client_time(change.modified) {
            return Err(NfsStat4::AttributeUnsupported);
        }
        let Some(size) = change.size else {
            return Ok(());
        };
        if located.kind == HandleKind::Directory {
            return Err(NfsStat4::IsDirectory);
        }
        let (mut slot, owner_id, temporary) = self.writer(binding, object, &located, state).await?;
        slot.size = size;
        let resize = StageChange::Truncate { size };
        let outcome = self
            .apply_change(
                binding,
                object,
                (&slot, owner_id),
                resize,
                WriteStable::FileSync,
            )
            .await;
        self.finish_writer(binding, object, (slot, temporary), outcome)
            .await
            .map(|_| ())
    }

    pub(super) async fn live_create(
        &self,
        binding: &NfsMountBinding,
        directory: ObjectRef,
        (name, mode, change): (&str, CreateMode, &AttributeChange),
        share: &ShareRequest,
    ) -> Result<CreateReply, NfsStat4> {
        if binding.read_only {
            return Err(NfsStat4::ReadOnly);
        }
        let parent = self.directory_of(binding, directory).await?;
        let at_root = parent.key.is_empty();
        check_component(name, VfsProtocol::Nfs, NameUse::Create, at_root).map_err(status_of)?;
        let key = child_key(&parent.key, name);
        if !holds(binding, &key) {
            return Err(NfsStat4::Access);
        }
        let before = self.change_of(binding, &parent.key).await?;
        let verifier = match mode {
            CreateMode::Exclusive(verifier) => Some(verifier),
            CreateMode::Unchecked | CreateMode::Guarded => None,
        };
        let remembered = (binding.mount_id, key.clone());
        let replay = verifier.is_some()
            && self.tables.lock().exclusive.get(&remembered) == verifier.as_ref();
        if replay {
            let claimed = self.claim(binding, &key, Some(HandleKind::File)).await;
            let claimed = claimed.map_err(status_of)?;
            return Ok(CreateReply {
                object: self.object_ref(binding, claimed.handle_id),
                created: false,
                change: ChangeInfo {
                    atomic: false,
                    before,
                    after: before,
                },
            });
        }
        let open_id = Ulid::generate();
        // The creating open writes the new file and holds the deny bits the OPEN asked for.
        let modes = (share.access | share::WRITE, share.deny);
        let input = OpenInput {
            context: self.access_context(binding),
            key: key.clone(),
            kind: HandleKind::File,
            create: true,
            exclusive: !matches!(mode, CreateMode::Unchecked),
            write: true,
            share: share_mode(modes.0, modes.1),
            delete_on_close: false,
            open_id,
            stable: true,
            mode: display_mode(binding, HandleKind::File),
            mount_id: binding.mount_id,
            stage_scope: StageScope::User,
        };
        let opened = drive(VfsOpenOperation::new(input), &self.context).await;
        let opened = opened.map_err(status_of)?;
        let settled = self
            .settle_create(binding, (&key, open_id, share), &opened, change)
            .await;
        let (object, slot) = match settled {
            Ok(settled) => settled,
            Err(status) => {
                self.discard_open(binding, open_id).await;
                return Err(status);
            }
        };
        if opened.created
            && let Some(verifier) = verifier
        {
            self.tables.lock().exclusive.insert(remembered, verifier);
        }
        let created = (object, share.owner.clone());
        let replaced = self.tables.lock().created.insert(created, slot);
        if let Some(old) = replaced {
            self.release_quietly(binding, object, old).await;
        }
        let after = self.change_of(binding, &parent.key).await.unwrap_or(before);
        Ok(CreateReply {
            object,
            created: opened.created,
            change: ChangeInfo {
                atomic: false,
                before,
                after,
            },
        })
    }

    /// Gives a new or opened file its handle, makes a new file durable in a checkpoint,
    /// applies a requested size and drops the write share a read-only OPEN did not ask for,
    /// before the creating writer waits for its OPEN.
    async fn settle_create(
        &self,
        binding: &NfsMountBinding,
        (key, open_id, share): (&str, Ulid, &ShareRequest),
        opened: &OpenResult,
        change: &AttributeChange,
    ) -> Result<(ObjectRef, OpenSlot), NfsStat4> {
        let modes = (share.access | share::WRITE, share.deny);
        let handle_id = match opened.handle {
            Some(handle) => handle.handle_id,
            None => {
                let claim = HandleClaim {
                    scope: bucket_scope(binding).map_err(status_of)?,
                    key: key.to_string(),
                    identity: opened.identity.clone(),
                    mode: display_mode(binding, HandleKind::File),
                };
                let allocated = drive(AllocateHandleOperation::new(claim), &self.context).await;
                allocated.map_err(status_of)?.handle_id
            }
        };
        let object = self.object_ref(binding, handle_id);
        let mut slot = self.slot_of(binding, key, open_id, opened, modes).await?;
        if opened.created {
            let checkpoint = self.checkpoint(binding, object, &slot).await?;
            let checkpoint = checkpoint.ok_or(NfsStat4::Delay)?;
            let committed = drive(VfsCommitOperation::new(checkpoint), &self.context).await;
            committed.map_err(status_of)?;
        }
        if let Some(size) = change.size {
            slot.size = size;
            let resize = StageChange::Truncate { size };
            let writer = (&slot, open_number(open_id));
            let resized = self
                .apply_change(binding, object, writer, resize, WriteStable::FileSync)
                .await?;
            slot.size = resized.new_size;
        }
        if share.access & share::WRITE == 0 {
            let scope = bucket_scope(binding).map_err(status_of)?;
            let reduced = share_mode(share.access, share.deny);
            let update = UpdateShareOperation::new(scope, open_id, reduced);
            drive(update, &self.context).await.map_err(status_of)?;
            (slot.write, slot.access, slot.deny) = (false, share.access, share.deny);
        }
        Ok((object, slot))
    }

    pub(super) async fn live_mkdir(
        &self,
        binding: &NfsMountBinding,
        directory: ObjectRef,
        name: &str,
    ) -> Result<CreateReply, NfsStat4> {
        if binding.read_only {
            return Err(NfsStat4::ReadOnly);
        }
        let parent = self.directory_of(binding, directory).await?;
        let at_root = parent.key.is_empty();
        check_component(name, VfsProtocol::Nfs, NameUse::Create, at_root).map_err(status_of)?;
        let key = child_key(&parent.key, name);
        if !holds(binding, &key) {
            return Err(NfsStat4::Access);
        }
        let before = self.change_of(binding, &parent.key).await?;
        let put = self.marker_put(binding, &key).await?;
        let input = MkdirInput {
            context: self.access_context(binding),
            key: key.clone(),
            put,
        };
        let made = drive(VfsMkdirOperation::new(input), &self.context).await;
        made.map_err(status_of)?;
        let claimed = self.claim(binding, &key, Some(HandleKind::Directory)).await;
        let claimed = claimed.map_err(status_of)?;
        let after = self.change_of(binding, &parent.key).await.unwrap_or(before);
        Ok(CreateReply {
            object: self.object_ref(binding, claimed.handle_id),
            created: true,
            change: ChangeInfo {
                atomic: false,
                before,
                after,
            },
        })
    }

    pub(super) async fn live_remove(
        &self,
        binding: &NfsMountBinding,
        directory: ObjectRef,
        name: &str,
    ) -> Result<ChangeInfo, NfsStat4> {
        if binding.read_only {
            return Err(NfsStat4::ReadOnly);
        }
        let parent = self.directory_of(binding, directory).await?;
        let at_root = parent.key.is_empty();
        check_component(name, VfsProtocol::Nfs, NameUse::Lookup, at_root)
            .map_err(|error| lookup_status(&error))?;
        let key = child_key(&parent.key, name);
        if !holds(binding, &key) {
            return Err(NfsStat4::NoEntry);
        }
        let before = self.change_of(binding, &parent.key).await?;
        let claimed = self.claim(binding, &key, None).await;
        let claimed = claimed.map_err(|error| lookup_status(&error))?;
        let identity = Some(&claimed.probe.identity);
        let instance = self.instance_at(binding, &key, identity).await?;
        let input = RemoveInput {
            context: self.access_context(binding),
            key: key.clone(),
            kind: claimed.probe.kind,
            handle_id: Some(claimed.handle_id),
            identity: claimed.probe.identity,
            instance_id: instance.map(|summary| summary.instance_id),
            attempt_id: Ulid::generate(),
        };
        let mut remove = VfsRemoveOperation::new(input);
        // The source parent keeps existing through a marker once its last child goes.
        if let Some((source, _)) = split_parent(&key)
            && !source.is_empty()
        {
            remove = remove.with_parent_marker(self.marker_put(binding, source).await?);
        }
        drive(remove, &self.context).await.map_err(status_of)?;
        let after = self.change_of(binding, &parent.key).await.unwrap_or(before);
        Ok(ChangeInfo {
            atomic: false,
            before,
            after,
        })
    }

    pub(super) async fn live_rename(
        &self,
        binding: &NfsMountBinding,
        (source, old, target, new): (ObjectRef, &str, ObjectRef, &str),
    ) -> Result<(ChangeInfo, ChangeInfo), NfsStat4> {
        if binding.read_only {
            return Err(NfsStat4::ReadOnly);
        }
        let from_parent = self.directory_of(binding, source).await?;
        let to_parent = self.directory_of(binding, target).await?;
        let from_root = from_parent.key.is_empty();
        check_component(old, VfsProtocol::Nfs, NameUse::Lookup, from_root)
            .map_err(|error| lookup_status(&error))?;
        let to_root = to_parent.key.is_empty();
        check_component(new, VfsProtocol::Nfs, NameUse::Create, to_root).map_err(status_of)?;
        let from = child_key(&from_parent.key, old);
        let to = child_key(&to_parent.key, new);
        if !holds(binding, &from) {
            return Err(NfsStat4::NoEntry);
        }
        if !holds(binding, &to) {
            return Err(NfsStat4::Xdev);
        }
        let source_before = self.change_of(binding, &from_parent.key).await?;
        let target_before = self.change_of(binding, &to_parent.key).await?;
        let claimed = self.claim(binding, &from, None).await;
        let claimed = claimed.map_err(|error| lookup_status(&error))?;
        let identity = claimed.probe.identity.clone();
        let instance = self.instance_at(binding, &from, Some(&identity)).await?;
        let instance_id = instance.as_ref().map(|summary| summary.instance_id);
        let stage = match claimed.probe.kind {
            HandleKind::File => {
                let scope = bucket_scope(binding).map_err(status_of)?;
                let read = LiveStageOperation::new(scope, claimed.handle_id);
                drive(read, &self.context).await.map_err(status_of)?
            }
            HandleKind::Directory => None,
        };
        let stage_size = stage.as_ref().map(|record| record.logical_size);
        let target_exists = self.head_info(binding, &to).await?.0.is_some();
        let (head_version, head_size) = match &identity {
            HandleIdentity::File(FileIdentity::Committed { .. }) => {
                self.head_info(binding, &from).await?
            }
            _ => (None, 0),
        };
        // A committed file moves as its committed base, and the source fence expects exactly it.
        let base_version = instance.as_ref().and_then(|summary| summary.base_version);
        let source_version = base_version.or(head_version);
        let input = RenameInput {
            context: self.access_context(binding),
            from: from.clone(),
            to: to.clone(),
            kind: claimed.probe.kind,
            identity: identity.clone(),
            handle_id: Some(claimed.handle_id),
            instance_id,
            replace: true,
            attempt_id: Ulid::generate(),
            stage,
            source_version,
        };
        let mut rename = VfsRenameOperation::new(input);
        match &identity {
            // The destination gets the committed bytes only; an unflushed overlay stays staged.
            HandleIdentity::File(FileIdentity::Committed { .. }) => {
                let size = match source_version == head_version {
                    true => head_size,
                    false => instance.map_or(head_size, |summary| summary.base_limit),
                };
                let read = ReadInput {
                    context: self.access_context(binding),
                    key: from.clone(),
                    instance_id: None,
                    version_id: source_version,
                    offset: 0,
                    length: 0,
                    lock_check: ReadLockCheck::Bypass,
                };
                let body = image_body(self.context.clone(), read, size);
                let put = self
                    .put_config(binding, &to, body, (size, target_exists))
                    .await?;
                rename = rename.with_destination_put(put);
            }
            // An unpublished file over a live destination replaces it with its staged image.
            HandleIdentity::File(FileIdentity::Unpublished { .. }) if target_exists => {
                let size = instance.as_ref().map(|summary| summary.logical_size);
                let size = size.or(stage_size).unwrap_or(0);
                let read = ReadInput {
                    context: self.access_context(binding),
                    key: from.clone(),
                    instance_id,
                    version_id: None,
                    offset: 0,
                    length: 0,
                    lock_check: ReadLockCheck::Bypass,
                };
                let body = image_body(self.context.clone(), read, size);
                let put = self.put_config(binding, &to, body, (size, true)).await?;
                rename = rename.with_destination_put(put);
            }
            HandleIdentity::Directory(DirectoryWitness::Marker { .. }) => {
                rename = rename.with_destination_put(self.marker_put(binding, &to).await?);
            }
            _ => {}
        }
        // The source parent keeps existing through a marker once its last child moves away.
        if !from_parent.key.is_empty() && from_parent.key != to_parent.key {
            let marker = self.marker_put(binding, &from_parent.key).await?;
            rename = rename.with_parent_marker(marker);
        }
        drive(rename, &self.context).await.map_err(status_of)?;
        let source_after = self.change_of(binding, &from_parent.key).await;
        let target_after = self.change_of(binding, &to_parent.key).await;
        Ok((
            ChangeInfo {
                atomic: false,
                before: source_before,
                after: source_after.unwrap_or(source_before),
            },
            ChangeInfo {
                atomic: false,
                before: target_before,
                after: target_after.unwrap_or(target_before),
            },
        ))
    }

    /// One byte-range lock request on the shared instance of a file; a detached instance
    /// serves only the holders of its existing opens.
    async fn lock_request(
        &self,
        binding: &NfsMountBinding,
        object: ObjectRef,
        request: LockRequest,
    ) -> Result<LockOutcome, NfsStat4> {
        let located = self.locate(binding, object).await?;
        if located.kind == HandleKind::Directory {
            return Err(NfsStat4::IsDirectory);
        }
        let (instance_id, detached) = match &located.detached {
            Some(summary) => (summary.instance_id, Some(object.object_id)),
            None => {
                let held = {
                    let tables = self.tables.lock();
                    let opens = tables.opens.iter().filter(|((open, _), _)| *open == object);
                    opens.map(|(_, slot)| slot.instance_id).next()
                };
                let instance_id = match held {
                    Some(instance_id) => instance_id,
                    None => {
                        let identity = located.identity.as_ref();
                        let summary = self.instance_at(binding, &located.key, identity).await?;
                        summary.ok_or(NfsStat4::OpenMode)?.instance_id
                    }
                };
                (instance_id, None)
            }
        };
        let input = LockInput {
            context: self.access_context(binding),
            key: located.key,
            instance_id,
            detached,
            request,
        };
        let outcome = drive(VfsLockOperation::new(input), &self.context).await;
        outcome.map_err(status_of)
    }

    pub(super) async fn live_lock(
        &self,
        binding: &NfsMountBinding,
        object: ObjectRef,
        owner: &OwnerKey,
        (kind, range): (LockKind, LockRange),
    ) -> Result<(), LockError> {
        let lock = byte_lock(owner, kind, range);
        let request = LockRequest::Lock {
            locks: vec![lock],
            atomic: true,
        };
        match self.lock_request(binding, object, request).await? {
            LockOutcome::Conflict(holder) => Err(LockError::Denied(lock_denial(holder))),
            LockOutcome::Done | LockOutcome::Free => {
                self.remember_lock(object, owner, range);
                Ok(())
            }
        }
    }

    pub(super) async fn live_test(
        &self,
        binding: &NfsMountBinding,
        object: ObjectRef,
        owner: &OwnerKey,
        (kind, range): (LockKind, LockRange),
    ) -> Result<(), LockError> {
        let lock = byte_lock(owner, kind, range);
        match self
            .lock_request(binding, object, LockRequest::Test { lock })
            .await?
        {
            LockOutcome::Conflict(holder) => Err(LockError::Denied(lock_denial(holder))),
            LockOutcome::Done | LockOutcome::Free => Ok(()),
        }
    }

    pub(super) async fn live_unlock(
        &self,
        binding: &NfsMountBinding,
        object: ObjectRef,
        owner: &OwnerKey,
        range: LockRange,
    ) -> Result<bool, NfsStat4> {
        let lock = byte_lock(owner, LockKind::Read, range);
        let request = LockRequest::Unlock { locks: vec![lock] };
        self.lock_request(binding, object, request).await?;
        Ok(self.forget_lock(object, owner, range))
    }
}

fn byte_lock(owner: &OwnerKey, kind: LockKind, range: LockRange) -> ByteLock {
    ByteLock {
        owner: owner_number(owner),
        offset: range.offset,
        length: range.length,
        exclusive: kind == LockKind::Write,
        protocol: VfsProtocol::Nfs,
    }
}
