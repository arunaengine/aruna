//! The seam between the NFS protocol engine and Aruna: host and binding admission, replay
//! re-authorization, file operations, share and lock arbitration, and revocation events.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::future::Future;
use std::net::IpAddr;
use std::sync::Arc;

use aruna_core::user::id::UserId;
use aruna_core::vfs_effects::ByteRange;
use bytes::Bytes;
use tokio::sync::broadcast;
use ulid::Ulid;

use crate::rpc::RpcPrincipal;
use crate::types::{NfsStat4, NfsTime, NfsType4, StableHow};

/// An enrolled host that proved possession of one of its pinned keys from an allowed network.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct HostIdentity {
    pub host_id: Ulid,
    pub owner: UserId,
    /// The SPKI digest a TLS peer proved; `None` for a cleartext loopback connection.
    pub fingerprint: Option<[u8; 32]>,
}

/// A live mount binding as one request sees it, after the host, network and grant checks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MountExport {
    pub mount_id: Ulid,
    pub host_id: Ulid,
    /// The Aruna principal the binding selects; RPC credentials never change it.
    pub principal: UserId,
    /// The `UserAccess` key whose live grants admit each request; never its secret.
    pub access_key: String,
    /// The bucket incarnation every handle of this export carries.
    pub incarnation: u128,
    /// The object id of the export root.
    pub root: u64,
    pub read_only: bool,
    /// An immutable job input view with persistent handles.
    pub persistent: bool,
}

impl MountExport {
    /// A distinct `fsid` per binding, taken from its mount id.
    pub fn fsid(&self) -> (u64, u64) {
        let value = self.mount_id.0;
        ((value >> 64) as u64, value as u64)
    }
}

/// Who asks for one backend action: the peer host and the binding it acts through.
#[derive(Clone, Debug)]
pub struct Caller {
    pub host: HostIdentity,
    pub peer: IpAddr,
    pub export: Arc<MountExport>,
}

/// An object inside one export, by its durable handle id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ObjectRef {
    pub mount_id: Ulid,
    pub object_id: u64,
}

/// What a handle id names now: a live object, an expired volatile identity, or a known
/// deleted object or incarnation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HandleStatus {
    Live,
    Expired,
    Stale,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectAttributes {
    pub kind: NfsType4,
    pub size: u64,
    pub change: u64,
    pub fileid: u64,
    pub mode: u32,
    pub links: u32,
    pub uid: u32,
    pub gid: u32,
    pub space_used: u64,
    pub accessed: NfsTime,
    pub modified: NfsTime,
    pub changed: NfsTime,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetTime {
    Server,
    Client(NfsTime),
}

/// Requested attribute changes of SETATTR, CREATE and OPEN.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AttributeChange {
    pub size: Option<u64>,
    pub mode: Option<u32>,
    pub accessed: Option<SetTime>,
    pub modified: Option<SetTime>,
}

/// `change_info4`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ChangeInfo {
    pub atomic: bool,
    pub before: u64,
    pub after: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirectoryEntry {
    pub name: String,
    /// The position cookie; never 0, 1 or 2.
    pub cookie: u64,
    pub object: ObjectRef,
    pub attributes: ObjectAttributes,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirectoryPage {
    pub entries: Vec<DirectoryEntry>,
    pub verifier: [u8; 8],
    pub eof: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadReply {
    pub data: Bytes,
    pub eof: bool,
    /// The exact bytes a READ served, so a replay re-admits them and not the current head.
    pub source: Option<ReadSource>,
}

/// The name, committed version and shared instance whose bytes one READ served.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadSource {
    pub key: String,
    pub version_id: Option<Ulid>,
    pub instance_id: Option<u64>,
    /// The ranges served from the committed version, read again past any later overlay.
    pub base_ranges: Vec<ByteRange>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WriteReply {
    pub count: u32,
    pub committed: StableHow,
    pub verifier: [u8; 8],
}

/// How OPEN creates a file. The backend keeps an exclusive verifier with the file, so a
/// retried exclusive create finds its own file again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CreateMode {
    Unchecked,
    Guarded,
    Exclusive([u8; 8]),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateReply {
    pub object: ObjectRef,
    pub created: bool,
    pub change: ChangeInfo,
}

/// An open or lock owner, scoped to its client.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct OwnerKey {
    pub client_id: u64,
    pub owner: Bytes,
}

/// A share reservation of one open owner; it replaces any earlier one of that owner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShareRequest {
    pub owner: OwnerKey,
    pub access: u32,
    pub deny: u32,
}

/// The state a READ, WRITE or size change runs under.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IoState {
    /// The anonymous stateid: no open, but other owners' deny reservations apply.
    Anonymous,
    /// The READ bypass stateid: deny reservations do not apply.
    Bypass,
    /// An open, and the lock owner when the stateid was a lock stateid; byte I/O runs as the
    /// lock owner, share checks as the open owner.
    Open {
        owner: OwnerKey,
        lock_owner: Option<OwnerKey>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum LockKind {
    Read,
    Write,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct LockRange {
    pub offset: u64,
    /// `u64::MAX` reaches the end of the file.
    pub length: u64,
}

impl LockRange {
    /// The full range a revoked or expired owner releases.
    pub const ALL: Self = Self {
        offset: 0,
        length: u64::MAX,
    };
}

/// A conflicting lock, reported by LOCK and LOCKT.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LockDenial {
    pub range: LockRange,
    pub kind: LockKind,
    pub owner: OwnerKey,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LockError {
    Denied(LockDenial),
    Status(NfsStat4),
}

impl From<NfsStat4> for LockError {
    fn from(status: NfsStat4) -> Self {
        Self::Status(status)
    }
}

/// An administrative change that ends access while connections or state may still hold it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Revocation {
    /// The host is revoked: drop its connections, sessions and state.
    Host(Ulid),
    /// The host key rotated: drop its connections so they reconnect with the new key.
    HostRotated(Ulid),
    /// The binding is deleted: revoke only its state.
    Mount(Ulid),
    /// The access key is revoked: revoke the state of every binding using it.
    AccessKey(String),
}

/// How one operation of a cached reply used an object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplayAccess {
    Head,
    List,
    /// A READ of the exact bytes `source` names; `None` when the backend did not say.
    Read {
        offset: u64,
        length: u32,
        source: Option<ReadSource>,
    },
    Write,
}

/// One object use of a cached reply, admitted again from live state before a replay.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplayCheck {
    pub object: ObjectRef,
    pub access: ReplayAccess,
}

/// What a cached reply was produced under; checked again before its bytes are released.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplayGrant {
    pub host: HostIdentity,
    pub peer: IpAddr,
    pub principal: RpcPrincipal,
    /// Every binding the original compound used.
    pub mounts: Vec<Ulid>,
    /// Every object use whose names, metadata or bytes the reply carries.
    pub checks: Vec<ReplayCheck>,
}

/// Every Aruna interaction of the NFS engine. Each call checks the live binding, grants,
/// policy, quarantine and vault state itself; the engine adds only protocol state.
pub trait NfsBackend: Send + Sync + 'static {
    /// The enrolled, unrevoked host pinning this SPKI SHA-256 digest whose networks hold `peer`.
    fn enrolled_host(
        &self,
        fingerprint: [u8; 32],
        peer: IpAddr,
    ) -> impl Future<Output = Option<HostIdentity>> + Send;

    /// The host a cleartext loopback connection acts as, when the operator allows one.
    fn loopback_host(&self, peer: IpAddr) -> impl Future<Output = Option<HostIdentity>> + Send {
        let _ = peer;
        async { None }
    }

    /// The live binding `mount_id` of `host`, checked against the peer network.
    fn mount_export(
        &self,
        host: &HostIdentity,
        peer: IpAddr,
        mount_id: Ulid,
    ) -> impl Future<Output = Result<MountExport, NfsStat4>> + Send;

    /// Whether a cached reply produced under `grant` may still be released.
    fn recheck_replay(
        &self,
        grant: &ReplayGrant,
    ) -> impl Future<Output = Result<(), NfsStat4>> + Send;

    fn resolve(
        &self,
        caller: &Caller,
        object: ObjectRef,
    ) -> impl Future<Output = HandleStatus> + Send;

    fn lookup(
        &self,
        caller: &Caller,
        directory: ObjectRef,
        name: &str,
    ) -> impl Future<Output = Result<ObjectRef, NfsStat4>> + Send;

    /// The parent directory inside the export; never called for the export root.
    fn parent(
        &self,
        caller: &Caller,
        object: ObjectRef,
    ) -> impl Future<Output = Result<ObjectRef, NfsStat4>> + Send;

    fn attributes(
        &self,
        caller: &Caller,
        object: ObjectRef,
    ) -> impl Future<Output = Result<ObjectAttributes, NfsStat4>> + Send;

    /// The subset of the `ACCESS4_*` bits in `requested` the caller holds.
    fn access(
        &self,
        caller: &Caller,
        object: ObjectRef,
        requested: u32,
    ) -> impl Future<Output = Result<u32, NfsStat4>> + Send;

    fn set_attributes(
        &self,
        caller: &Caller,
        object: ObjectRef,
        state: &IoState,
        change: &AttributeChange,
    ) -> impl Future<Output = Result<(), NfsStat4>> + Send;

    /// One page after `cookie`; a nonzero cookie whose verifier no longer matches is NOT_SAME
    /// or BAD_COOKIE.
    fn read_directory(
        &self,
        caller: &Caller,
        directory: ObjectRef,
        cookie: u64,
        verifier: [u8; 8],
        limit: usize,
    ) -> impl Future<Output = Result<DirectoryPage, NfsStat4>> + Send;

    /// Sets the share reservation of `share.owner` on `object`, or SHARE_DENIED on conflict.
    fn open(
        &self,
        caller: &Caller,
        object: ObjectRef,
        share: &ShareRequest,
    ) -> impl Future<Output = Result<(), NfsStat4>> + Send;

    /// Ends the open of `owner`, publishing what its last writer close requires.
    fn close(
        &self,
        caller: &Caller,
        object: ObjectRef,
        owner: &OwnerKey,
    ) -> impl Future<Output = Result<(), NfsStat4>> + Send;

    /// Ends the open of a revoked, expired or purged stateid. An error means the backend still
    /// holds the open, and the engine retries the release later.
    fn release_open(
        &self,
        caller: &Caller,
        object: ObjectRef,
        owner: &OwnerKey,
    ) -> impl Future<Output = Result<(), NfsStat4>> + Send {
        self.close(caller, object, owner)
    }

    fn read(
        &self,
        caller: &Caller,
        object: ObjectRef,
        state: &IoState,
        offset: u64,
        count: u32,
    ) -> impl Future<Output = Result<ReadReply, NfsStat4>> + Send;

    fn write(
        &self,
        caller: &Caller,
        object: ObjectRef,
        state: &IoState,
        offset: u64,
        data: Bytes,
        stable: StableHow,
    ) -> impl Future<Output = Result<WriteReply, NfsStat4>> + Send;

    /// Makes acknowledged writes in the range durable and returns the write verifier.
    fn commit(
        &self,
        caller: &Caller,
        object: ObjectRef,
        offset: u64,
        count: u32,
    ) -> impl Future<Output = Result<[u8; 8], NfsStat4>> + Send;

    /// Creates or opens `name` and holds `share` for its owner until the OPEN takes it over.
    fn create_file(
        &self,
        caller: &Caller,
        directory: ObjectRef,
        name: &str,
        mode: CreateMode,
        share: &ShareRequest,
        change: &AttributeChange,
    ) -> impl Future<Output = Result<CreateReply, NfsStat4>> + Send;

    fn create_directory(
        &self,
        caller: &Caller,
        directory: ObjectRef,
        name: &str,
        change: &AttributeChange,
    ) -> impl Future<Output = Result<CreateReply, NfsStat4>> + Send;

    fn remove(
        &self,
        caller: &Caller,
        directory: ObjectRef,
        name: &str,
    ) -> impl Future<Output = Result<ChangeInfo, NfsStat4>> + Send;

    /// Renames inside one export; the engine refuses a cross-export rename with XDEV first.
    fn rename(
        &self,
        caller: &Caller,
        source: ObjectRef,
        old: &str,
        target: ObjectRef,
        new: &str,
    ) -> impl Future<Output = Result<(ChangeInfo, ChangeInfo), NfsStat4>> + Send;

    fn lock(
        &self,
        caller: &Caller,
        object: ObjectRef,
        owner: &OwnerKey,
        kind: LockKind,
        range: LockRange,
    ) -> impl Future<Output = Result<(), LockError>> + Send;

    fn test_lock(
        &self,
        caller: &Caller,
        object: ObjectRef,
        owner: &OwnerKey,
        kind: LockKind,
        range: LockRange,
    ) -> impl Future<Output = Result<(), LockError>> + Send;

    /// Releases the range and reports whether `owner` still holds any lock on `object`.
    fn unlock(
        &self,
        caller: &Caller,
        object: ObjectRef,
        owner: &OwnerKey,
        range: LockRange,
    ) -> impl Future<Output = Result<bool, NfsStat4>> + Send;

    fn revocations(&self) -> broadcast::Receiver<Revocation>;
}
