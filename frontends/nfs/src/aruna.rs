//! The Aruna side of the NFS server: enrolled hosts, mount bindings, live bucket and folder
//! exports through the shared file server operations, and immutable job input views.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod admit;
mod bodies;
mod exports;
mod views;
mod writes;

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;

use aruna_core::id::NodeId;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::vfs::{HandleIdentity, NfsMountBinding, NfsMountTarget};
use aruna_ops_data::vfs::VfsError;
use aruna_ops_work::jobs::workflow::input_view::ViewError;
use aruna_runner::driver::DriverContext;
use bytes::Bytes;
use parking_lot::Mutex;
use tokio::sync::broadcast;
use ulid::Ulid;

use crate::backend::{
    AttributeChange, Caller, ChangeInfo, CreateMode, CreateReply, DirectoryPage, HandleStatus,
    HostIdentity, IoState, LockError, LockKind, LockRange, MountExport, NfsBackend,
    ObjectAttributes, ObjectRef, OwnerKey, ReadReply, ReplayGrant, Revocation, ShareRequest,
    WriteReply,
};
use crate::types::{NfsStat4, StableHow};

pub use admit::network_allows;

/// The object id of a live export's root directory; durable handle ids start above it.
pub const LIVE_ROOT: u64 = 0;
/// Revocations a slow server task may lag behind before it resubscribes.
const REVOCATION_BACKLOG: usize = 256;

/// What the node supplies to the NFS backend.
#[derive(Clone)]
pub struct ArunaNfsConfig {
    pub context: Arc<DriverContext>,
    pub realm_id: RealmId,
    pub node_id: NodeId,
}

/// One NFS open of an owner on a shared file instance.
#[derive(Clone, Debug, PartialEq)]
struct OpenSlot {
    open_id: Ulid,
    instance_id: u64,
    key: String,
    write: bool,
    identity: HandleIdentity,
    /// The committed version the instance's untouched bytes come from.
    version_id: Option<Ulid>,
    size: u64,
    access: u32,
    deny: u32,
}

/// Protocol bookkeeping only; every request admits its caller again from live state.
#[derive(Default)]
struct Tables {
    /// The binding each export of the current compounds was admitted with.
    mounts: HashMap<Ulid, NfsMountBinding>,
    opens: HashMap<(ObjectRef, OwnerKey), OpenSlot>,
    /// The writer open of a new file, until the OPEN that created it takes it over.
    created: HashMap<ObjectRef, OpenSlot>,
    /// The name each issued directory cookie was emitted for, by mount and cookie.
    cookies: HashMap<(Ulid, u64), String>,
    /// The verifier of an exclusive create, by mount and key, so a retry finds its file.
    exclusive: HashMap<(Ulid, String), [u8; 8]>,
    locks: HashMap<(ObjectRef, OwnerKey), Vec<LockRange>>,
    write_verifier: Option<[u8; 8]>,
}

/// The NFS backend over Aruna local buckets, scoped folders and job input views. Every call
/// drives the shared operations through the runner; AUTH_SYS ids never select a principal.
pub struct ArunaNfsBackend {
    context: Arc<DriverContext>,
    realm_id: RealmId,
    node_id: NodeId,
    events: broadcast::Sender<Revocation>,
    tables: Mutex<Tables>,
    /// The directory cookie verifier of this server instance.
    verifier: [u8; 8],
}

impl ArunaNfsBackend {
    pub fn new(config: ArunaNfsConfig) -> Self {
        let (events, _) = broadcast::channel(REVOCATION_BACKLOG);
        let boot = Ulid::generate().to_bytes();
        let mut verifier = [0; 8];
        verifier.copy_from_slice(&boot[8..]);
        Self {
            context: config.context,
            realm_id: config.realm_id,
            node_id: config.node_id,
            events,
            tables: Mutex::new(Tables::default()),
            verifier,
        }
    }

    /// Forwards one revocation from the node's file server adapter to every NFS server task,
    /// and drops the bookkeeping of a deleted binding.
    pub fn publish_revocation(&self, event: Revocation) {
        if let Revocation::Mount(mount_id) = &event {
            let mut tables = self.tables.lock();
            tables.mounts.remove(mount_id);
            tables.cookies.retain(|(mount, _), _| mount != mount_id);
            tables.exclusive.retain(|(mount, _), _| mount != mount_id);
        }
        // No subscriber means no server runs yet; nothing holds state to revoke.
        let _ = self.events.send(event);
    }

    /// The binding `caller`'s export was admitted with in this compound.
    fn binding(&self, caller: &Caller) -> Result<NfsMountBinding, NfsStat4> {
        let tables = self.tables.lock();
        let binding = tables.mounts.get(&caller.export.mount_id).cloned();
        binding.ok_or(NfsStat4::Stale)
    }
}

/// The NFS status of a file server failure.
pub fn vfs_status(error: &VfsError) -> NfsStat4 {
    match error {
        VfsError::NotFound => NfsStat4::NoEntry,
        VfsError::Denied
        | VfsError::Distributed
        | VfsError::VaultLocked
        | VfsError::KeyRequired
        | VfsError::Quarantined
        | VfsError::DeletePending => NfsStat4::Access,
        VfsError::InvalidName => NfsStat4::Invalid,
        VfsError::IsDirectory => NfsStat4::IsDirectory,
        VfsError::NotDirectory => NfsStat4::NotDirectory,
        VfsError::NotEmpty => NfsStat4::NotEmpty,
        VfsError::Exists => NfsStat4::Exist,
        VfsError::CrossExport => NfsStat4::Xdev,
        VfsError::ReadOnly => NfsStat4::ReadOnly,
        VfsError::QuotaExceeded => NfsStat4::QuotaExceeded,
        VfsError::NoSpace => NfsStat4::NoSpace,
        VfsError::ShareViolation => NfsStat4::ShareDenied,
        VfsError::LockConflict => NfsStat4::Denied,
        VfsError::StaleHandle => NfsStat4::Stale,
        VfsError::ExpiredHandle => NfsStat4::FilehandleExpired,
        VfsError::BadCookie => NfsStat4::BadCookie,
        VfsError::Conflict | VfsError::Unavailable(_) => NfsStat4::Delay,
        VfsError::CorruptStage | VfsError::Storage(_) | VfsError::Vfs(_) => NfsStat4::Io,
    }
}

/// The NFS status of a job input view failure; a view never substitutes another version.
pub fn view_status(error: &ViewError) -> NfsStat4 {
    match error {
        ViewError::NotFound => NfsStat4::NoEntry,
        ViewError::Denied | ViewError::Locked | ViewError::KeyRequired => NfsStat4::Access,
        ViewError::MissingVersion => NfsStat4::Stale,
        ViewError::Unavailable(_) => NfsStat4::Io,
    }
}

/// Whether a binding exports an immutable job input view.
fn job_view(binding: &NfsMountBinding) -> bool {
    matches!(binding.target, NfsMountTarget::JobInputView { .. })
}

impl NfsBackend for ArunaNfsBackend {
    async fn enrolled_host(&self, fingerprint: [u8; 32], peer: IpAddr) -> Option<HostIdentity> {
        self.admit_host(fingerprint, peer).await
    }

    async fn loopback_host(&self, peer: IpAddr) -> Option<HostIdentity> {
        self.admit_loopback(peer).await
    }

    async fn mount_export(
        &self,
        host: &HostIdentity,
        peer: IpAddr,
        mount_id: Ulid,
    ) -> Result<MountExport, NfsStat4> {
        self.admit_mount(host, peer, mount_id).await
    }

    async fn recheck_replay(&self, grant: &ReplayGrant) -> Result<(), NfsStat4> {
        self.recheck_grant(grant).await
    }

    async fn resolve(&self, caller: &Caller, object: ObjectRef) -> HandleStatus {
        let Ok(binding) = self.binding(caller) else {
            return HandleStatus::Stale;
        };
        match job_view(&binding) {
            true => self.view_resolve(&binding, object).await,
            false => self.live_resolve(&binding, object).await,
        }
    }

    async fn lookup(
        &self,
        caller: &Caller,
        directory: ObjectRef,
        name: &str,
    ) -> Result<ObjectRef, NfsStat4> {
        let binding = self.binding(caller)?;
        match job_view(&binding) {
            true => self.view_lookup(&binding, directory, name).await,
            false => self.live_lookup(&binding, directory, name).await,
        }
    }

    async fn parent(&self, caller: &Caller, object: ObjectRef) -> Result<ObjectRef, NfsStat4> {
        let binding = self.binding(caller)?;
        match job_view(&binding) {
            true => self.view_parent(&binding, object).await,
            false => self.live_parent(&binding, object).await,
        }
    }

    async fn attributes(
        &self,
        caller: &Caller,
        object: ObjectRef,
    ) -> Result<ObjectAttributes, NfsStat4> {
        let binding = self.binding(caller)?;
        match job_view(&binding) {
            true => self.view_attributes(&binding, object).await,
            false => self.live_attributes(&binding, object).await,
        }
    }

    async fn access(
        &self,
        caller: &Caller,
        object: ObjectRef,
        requested: u32,
    ) -> Result<u32, NfsStat4> {
        let binding = self.binding(caller)?;
        match job_view(&binding) {
            true => self.view_access(&binding, object, requested).await,
            false => self.live_access(&binding, object, requested).await,
        }
    }

    async fn set_attributes(
        &self,
        caller: &Caller,
        object: ObjectRef,
        state: &IoState,
        change: &AttributeChange,
    ) -> Result<(), NfsStat4> {
        let binding = self.binding(caller)?;
        match job_view(&binding) {
            true => Err(NfsStat4::ReadOnly),
            false => self.live_setattr(&binding, object, state, change).await,
        }
    }

    async fn read_directory(
        &self,
        caller: &Caller,
        directory: ObjectRef,
        cookie: u64,
        verifier: [u8; 8],
        limit: usize,
    ) -> Result<DirectoryPage, NfsStat4> {
        let binding = self.binding(caller)?;
        let position = (cookie, verifier, limit);
        match job_view(&binding) {
            true => self.view_directory(&binding, directory, position).await,
            false => self.live_directory(&binding, directory, position).await,
        }
    }

    async fn open(
        &self,
        caller: &Caller,
        object: ObjectRef,
        share: &ShareRequest,
    ) -> Result<(), NfsStat4> {
        let binding = self.binding(caller)?;
        match job_view(&binding) {
            true => self.view_open(&binding, object, share).await,
            false => self.live_open(&binding, object, share).await,
        }
    }

    async fn close(
        &self,
        caller: &Caller,
        object: ObjectRef,
        owner: &OwnerKey,
    ) -> Result<(), NfsStat4> {
        let binding = self.binding(caller)?;
        match job_view(&binding) {
            true => Ok(()),
            false => self.live_close(&binding, object, owner).await,
        }
    }

    async fn read(
        &self,
        caller: &Caller,
        object: ObjectRef,
        state: &IoState,
        offset: u64,
        count: u32,
    ) -> Result<ReadReply, NfsStat4> {
        let binding = self.binding(caller)?;
        match job_view(&binding) {
            true => self.view_read(&binding, object, offset, count).await,
            false => self.live_read(&binding, object, state, offset, count).await,
        }
    }

    async fn write(
        &self,
        caller: &Caller,
        object: ObjectRef,
        state: &IoState,
        offset: u64,
        data: Bytes,
        stable: StableHow,
    ) -> Result<WriteReply, NfsStat4> {
        let binding = self.binding(caller)?;
        match job_view(&binding) {
            true => Err(NfsStat4::ReadOnly),
            false => {
                let write = (offset, data, stable);
                self.live_write(&binding, object, state, write).await
            }
        }
    }

    async fn commit(
        &self,
        caller: &Caller,
        object: ObjectRef,
        _offset: u64,
        _count: u32,
    ) -> Result<[u8; 8], NfsStat4> {
        let binding = self.binding(caller)?;
        match job_view(&binding) {
            true => Err(NfsStat4::ReadOnly),
            false => self.live_commit(&binding, object).await,
        }
    }

    async fn create_file(
        &self,
        caller: &Caller,
        directory: ObjectRef,
        name: &str,
        mode: CreateMode,
        change: &AttributeChange,
    ) -> Result<CreateReply, NfsStat4> {
        let binding = self.binding(caller)?;
        match job_view(&binding) {
            true => Err(NfsStat4::ReadOnly),
            false => {
                let request = (name, mode, change);
                self.live_create(&binding, directory, request).await
            }
        }
    }

    async fn create_directory(
        &self,
        caller: &Caller,
        directory: ObjectRef,
        name: &str,
        _change: &AttributeChange,
    ) -> Result<CreateReply, NfsStat4> {
        let binding = self.binding(caller)?;
        match job_view(&binding) {
            true => Err(NfsStat4::ReadOnly),
            false => self.live_mkdir(&binding, directory, name).await,
        }
    }

    async fn remove(
        &self,
        caller: &Caller,
        directory: ObjectRef,
        name: &str,
    ) -> Result<ChangeInfo, NfsStat4> {
        let binding = self.binding(caller)?;
        match job_view(&binding) {
            true => Err(NfsStat4::ReadOnly),
            false => self.live_remove(&binding, directory, name).await,
        }
    }

    async fn rename(
        &self,
        caller: &Caller,
        source: ObjectRef,
        old: &str,
        target: ObjectRef,
        new: &str,
    ) -> Result<(ChangeInfo, ChangeInfo), NfsStat4> {
        let binding = self.binding(caller)?;
        match job_view(&binding) {
            true => Err(NfsStat4::ReadOnly),
            false => {
                let names = (source, old, target, new);
                self.live_rename(&binding, names).await
            }
        }
    }

    async fn lock(
        &self,
        caller: &Caller,
        object: ObjectRef,
        owner: &OwnerKey,
        kind: LockKind,
        range: LockRange,
    ) -> Result<(), LockError> {
        let binding = self.binding(caller)?;
        match job_view(&binding) {
            true => self.view_lock(object, owner, kind, range),
            false => self.live_lock(&binding, object, owner, (kind, range)).await,
        }
    }

    async fn test_lock(
        &self,
        caller: &Caller,
        object: ObjectRef,
        owner: &OwnerKey,
        kind: LockKind,
        range: LockRange,
    ) -> Result<(), LockError> {
        let binding = self.binding(caller)?;
        match job_view(&binding) {
            // Only read locks exist on a read-only view, and they never conflict.
            true => Ok(()),
            false => self.live_test(&binding, object, owner, (kind, range)).await,
        }
    }

    async fn unlock(
        &self,
        caller: &Caller,
        object: ObjectRef,
        owner: &OwnerKey,
        range: LockRange,
    ) -> Result<bool, NfsStat4> {
        let binding = self.binding(caller)?;
        match job_view(&binding) {
            true => Ok(self.forget_lock(object, owner, range)),
            false => self.live_unlock(&binding, object, owner, range).await,
        }
    }

    fn revocations(&self) -> broadcast::Receiver<Revocation> {
        self.events.subscribe()
    }
}

impl ArunaNfsBackend {
    /// Records a granted lock range of `owner`.
    fn remember_lock(&self, object: ObjectRef, owner: &OwnerKey, range: LockRange) {
        let mut tables = self.tables.lock();
        let held = tables.locks.entry((object, owner.clone())).or_default();
        held.push(range);
    }

    /// Drops the ranges of `owner` the unlock covers; answers whether any lock remains.
    fn forget_lock(&self, object: ObjectRef, owner: &OwnerKey, range: LockRange) -> bool {
        let mut tables = self.tables.lock();
        let key = (object, owner.clone());
        let Some(held) = tables.locks.get_mut(&key) else {
            return false;
        };
        let end = range.offset.saturating_add(range.length);
        held.retain(|lock| {
            let lock_end = lock.offset.saturating_add(lock.length);
            !(lock.offset >= range.offset && lock_end <= end)
        });
        let remaining = !held.is_empty();
        if !remaining {
            tables.locks.remove(&key);
        }
        remaining
    }
}

#[cfg(test)]
#[path = "aruna_tests.rs"]
mod aruna_tests;
