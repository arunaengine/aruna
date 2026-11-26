//! The `SmbBackend` trait: every Aruna domain interaction the SMB engine needs, from
//! credential lookup and live admission to opens, data, metadata, listings and locks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::future::Future;

use aruna_core::user::id::UserId;
use bytes::Bytes;
use tokio::sync::broadcast;
use ulid::Ulid;
use zeroize::Zeroizing;

pub use crate::access::{Rights, WriteMode};
use crate::status;

/// Share names starting with this prefix are reserved for job input views.
pub const JOB_PREFIX: &str = "in-";

/// The live credential behind one authenticated session, named by its exact access key.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Credential {
    pub access_key: String,
    pub principal: UserId,
}

/// The issuer-decrypted secret of a live local `UserAccess`, wrapped at once so it is
/// zeroed when the NTLM proof check drops it.
pub struct CredentialSecret {
    pub principal: UserId,
    pub secret: Zeroizing<String>,
}

impl std::fmt::Debug for CredentialSecret {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CredentialSecret")
            .field("principal", &self.principal)
            .finish_non_exhaustive()
    }
}

/// What a TREE_CONNECT share name routes to.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ShareTarget {
    /// An ordinary local bucket, named exactly as the client spelled it.
    Bucket(String),
    /// The reserved read-only input view of one job, `in-<job_id>`.
    JobView(Ulid),
}

impl ShareTarget {
    /// Routes a share name. A reserved `in-` name that is not a job identifier routes
    /// nowhere instead of falling back to a bucket.
    pub fn route(name: &str) -> Option<Self> {
        if name.is_empty() || name.eq_ignore_ascii_case("IPC$") || name.ends_with('$') {
            return None;
        }
        match name.strip_prefix(JOB_PREFIX) {
            Some(job) => Ulid::from_string(job).ok().map(Self::JobView),
            None => Some(Self::Bucket(name.to_owned())),
        }
    }
}

/// Who acts and in which share; every backend call re-checks it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Scope {
    pub credential: Credential,
    pub share: ShareTarget,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TreeGrant {
    pub read_only: bool,
    pub capacity: u64,
    pub available: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObjectKind {
    File,
    Directory,
}

/// Sizes and FILETIME timestamps of one object.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileInfo {
    pub kind: ObjectKind,
    pub size: u64,
    pub allocation: u64,
    pub created: u64,
    pub accessed: u64,
    pub written: u64,
    pub changed: u64,
    pub delete_pending: bool,
}

/// A resolved path: its visible kind (`None` when missing or hidden) and live rights on it.
/// The backend must not distinguish a missing child from a protected one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Lookup {
    pub kind: Option<ObjectKind>,
    pub rights: Rights,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Disposition {
    Supersede,
    Open,
    Create,
    OpenIf,
    Overwrite,
    OverwriteIf,
}

impl Disposition {
    pub fn from_code(code: u32) -> Option<Self> {
        Some(match code {
            0 => Self::Supersede,
            1 => Self::Open,
            2 => Self::Create,
            3 => Self::OpenIf,
            4 => Self::Overwrite,
            5 => Self::OverwriteIf,
            _ => return None,
        })
    }

    /// Whether the disposition creates the object when it is missing.
    pub fn creates(self) -> bool {
        matches!(
            self,
            Self::Supersede | Self::Create | Self::OpenIf | Self::OverwriteIf
        )
    }

    /// Whether an existing object is replaced or truncated.
    pub fn truncates(self) -> bool {
        matches!(self, Self::Supersede | Self::Overwrite | Self::OverwriteIf)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CreateAction {
    Superseded,
    Opened,
    Created,
    Overwritten,
}

impl CreateAction {
    pub fn code(self) -> u32 {
        match self {
            Self::Superseded => 0,
            Self::Opened => 1,
            Self::Created => 2,
            Self::Overwritten => 3,
        }
    }
}

/// A CREATE after the engine computed the granted access. `kind` is the kind the client
/// demanded, if any; share access uses the FILE_SHARE_* bits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenRequest {
    pub path: String,
    pub disposition: Disposition,
    pub kind: Option<ObjectKind>,
    pub granted: u32,
    pub share_access: u32,
    pub delete_on_close: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpenGrant {
    pub handle: u64,
    pub action: CreateAction,
    pub info: FileInfo,
}

/// One WRITE: `offset` `None` writes at the shared end of file. In `Append` mode the
/// backend refuses an offset below the end of file under the same serialization as the
/// write (see [`crate::access::append_allowed`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteRequest {
    pub offset: Option<u64>,
    pub data: Bytes,
    pub mode: WriteMode,
    pub through: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BasicChange {
    pub created: Option<u64>,
    pub accessed: Option<u64>,
    pub written: Option<u64>,
    pub changed: Option<u64>,
    pub attributes: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InfoChange {
    Basic(BasicChange),
    EndOfFile(u64),
    Allocation(u64),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirectoryEntry {
    pub name: String,
    pub info: FileInfo,
}

/// One slice of a directory listing in exact order; `next` resumes after it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ListPage {
    pub entries: Vec<DirectoryEntry>,
    pub next: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LockRange {
    pub offset: u64,
    pub length: u64,
    pub exclusive: bool,
}

/// A credential whose sessions must end now.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Revocation {
    pub access_key: String,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum BackendError {
    #[error("not found")]
    NotFound,
    #[error("parent path not found")]
    PathNotFound,
    #[error("already exists")]
    Exists,
    #[error("access denied")]
    Denied,
    #[error("directory not empty")]
    NotEmpty,
    #[error("is a directory")]
    IsDirectory,
    #[error("not a directory")]
    NotDirectory,
    #[error("sharing violation")]
    Sharing,
    #[error("lock conflict")]
    LockConflict,
    #[error("lock not granted")]
    LockRefused,
    #[error("range not locked")]
    NotLocked,
    #[error("no space left")]
    DiskFull,
    #[error("quota exceeded")]
    QuotaExceeded,
    #[error("read-only")]
    ReadOnly,
    #[error("credential revoked or no longer admitted")]
    Revoked,
    #[error("unsupported")]
    Unsupported,
    #[error("invalid request")]
    Invalid,
    #[error("crosses shares or exports")]
    CrossDevice,
    #[error("delete pending")]
    DeletePending,
    #[error("busy")]
    Busy,
    #[error("cancelled")]
    Cancelled,
    #[error("internal error: {0}")]
    Internal(String),
}

impl BackendError {
    pub fn status(&self) -> u32 {
        match self {
            Self::NotFound => status::NAME_NOT_FOUND,
            Self::PathNotFound => status::PATH_NOT_FOUND,
            Self::Exists => status::NAME_COLLISION,
            Self::Denied => status::ACCESS_DENIED,
            Self::NotEmpty => status::DIRECTORY_NOT_EMPTY,
            Self::IsDirectory => status::IS_DIRECTORY,
            Self::NotDirectory => status::NOT_DIRECTORY,
            Self::Sharing => status::SHARING_VIOLATION,
            Self::LockConflict => status::LOCK_CONFLICT,
            Self::LockRefused => status::LOCK_NOT_GRANTED,
            Self::NotLocked => status::RANGE_NOT_LOCKED,
            Self::DiskFull => status::DISK_FULL,
            Self::QuotaExceeded => status::QUOTA_EXCEEDED,
            Self::ReadOnly => status::MEDIA_WRITE_PROTECTED,
            Self::Revoked => status::NETWORK_SESSION_EXPIRED,
            Self::Unsupported => status::NOT_SUPPORTED,
            Self::Invalid => status::INVALID_PARAMETER,
            Self::CrossDevice => status::NOT_SAME_DEVICE,
            Self::DeletePending => status::DELETE_PENDING,
            Self::Busy => status::INSUFFICIENT_RESOURCES,
            Self::Cancelled => status::CANCELLED,
            Self::Internal(_) => status::INTERNAL_ERROR,
        }
    }
}

/// Aruna domain interactions of the SMB engine. Implementations run live RBAC and CEL on
/// every call, apply the core reserved-name predicates and keep their futures cancel-safe:
/// the engine drops a future when a client cancels or the connection ends.
pub trait SmbBackend: Send + Sync + 'static {
    /// The secret of the live local credential with exactly this access key, if any.
    fn lookup_secret(
        &self,
        access_key: &str,
    ) -> impl Future<Output = Result<Option<CredentialSecret>, BackendError>> + Send;

    /// Live admission of the credential: issuer, realm, owner, cutoff, expiry, revocation.
    fn admit(
        &self,
        credential: &Credential,
    ) -> impl Future<Output = Result<(), BackendError>> + Send;

    /// Admits a share. A job view requires the credential to be that job's attempt credential.
    fn connect_tree(
        &self,
        credential: &Credential,
        share: &ShareTarget,
    ) -> impl Future<Output = Result<TreeGrant, BackendError>> + Send;

    /// Resolves a share-relative slash path and the caller's live rights on it.
    fn lookup(
        &self,
        scope: &Scope,
        path: &str,
    ) -> impl Future<Output = Result<Lookup, BackendError>> + Send;

    fn open(
        &self,
        scope: &Scope,
        request: OpenRequest,
    ) -> impl Future<Output = Result<OpenGrant, BackendError>> + Send;

    /// Closes one open, publishing dirty bytes or completing a pending delete as needed.
    fn close(
        &self,
        scope: &Scope,
        handle: u64,
    ) -> impl Future<Output = Result<FileInfo, BackendError>> + Send;

    fn read(
        &self,
        scope: &Scope,
        handle: u64,
        offset: u64,
        length: u32,
    ) -> impl Future<Output = Result<Bytes, BackendError>> + Send;

    /// Writes and returns the byte count written.
    fn write(
        &self,
        scope: &Scope,
        handle: u64,
        request: WriteRequest,
    ) -> impl Future<Output = Result<u32, BackendError>> + Send;

    fn flush(
        &self,
        scope: &Scope,
        handle: u64,
    ) -> impl Future<Output = Result<(), BackendError>> + Send;

    fn query_info(
        &self,
        scope: &Scope,
        handle: u64,
    ) -> impl Future<Output = Result<FileInfo, BackendError>> + Send;

    fn set_info(
        &self,
        scope: &Scope,
        handle: u64,
        change: InfoChange,
    ) -> impl Future<Output = Result<(), BackendError>> + Send;

    /// Renames the open object to a share-relative slash path, keeping the open.
    fn rename(
        &self,
        scope: &Scope,
        handle: u64,
        target: &str,
        replace: bool,
    ) -> impl Future<Output = Result<(), BackendError>> + Send;

    /// Sets or clears delete-pending on the open's link.
    fn set_disposition(
        &self,
        scope: &Scope,
        handle: u64,
        delete: bool,
    ) -> impl Future<Output = Result<(), BackendError>> + Send;

    /// Lists the directory behind an open from `cursor`, or from its start for `None`.
    fn list(
        &self,
        scope: &Scope,
        handle: u64,
        cursor: Option<String>,
    ) -> impl Future<Output = Result<ListPage, BackendError>> + Send;

    /// Takes all ranges or none. With `wait` the future resolves once granted.
    fn lock(
        &self,
        scope: &Scope,
        handle: u64,
        ranges: Vec<LockRange>,
        wait: bool,
    ) -> impl Future<Output = Result<(), BackendError>> + Send;

    fn unlock(
        &self,
        scope: &Scope,
        handle: u64,
        ranges: Vec<LockRange>,
    ) -> impl Future<Output = Result<(), BackendError>> + Send;

    /// Told after a client cancelled a waiting operation on `handle`.
    fn cancel(&self, scope: &Scope, handle: u64) -> impl Future<Output = ()> + Send;

    /// Credential revocations; the engine ends matching sessions when one arrives.
    fn revocations(&self) -> broadcast::Receiver<Revocation>;
}
