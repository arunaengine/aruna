//! The Aruna domain backend of the SMB server: live credential admission, share routing, local
//! bucket files through the shared file server operations and read-only job input views.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod admission;
mod bodies;
mod files;
mod jobs;
mod names;
mod writes;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use aruna_core::credential_encryption::CredentialEncryptionKey;
use aruna_core::id::NodeId;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::vfs::{HandleIdentity, HandleKind, NfsMountTarget, StageOwner};
use aruna_core::vfs_effects::{VfsFailure, VfsProtocol};
use aruna_ops_data::vfs::{VfsAccessContext, VfsError};
use aruna_ops_work::jobs::workflow::input_view::ViewError;
use aruna_runner::driver::DriverContext;
use bytes::Bytes;
use parking_lot::Mutex;
use tokio::sync::broadcast;
use ulid::Ulid;

use crate::backend::{
    BackendError, BasicChange, Credential, CredentialSecret, FileInfo, InfoChange, ListPage,
    LockRange, Lookup, ObjectKind, OpenGrant, OpenRequest, Revocation, Scope, ShareTarget,
    SmbBackend, TreeGrant, WriteRequest,
};

/// Revocations one slow receiver may fall behind before it sees a lag.
const REVOCATION_QUEUE: usize = 256;
/// Milliseconds between the Unix and the FILETIME epochs.
const FILETIME_OFFSET_MS: u64 = 11_644_473_600_000;

/// The node identity the backend serves as and the key that opens issued secrets.
#[derive(Clone, Debug)]
pub struct ArunaSmbConfig {
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub credential_key: CredentialEncryptionKey,
}

/// One open of a local bucket file or directory.
#[derive(Clone, Debug)]
struct FileOpen {
    access_key: String,
    bucket: String,
    incarnation: u128,
    key: String,
    kind: HandleKind,
    open_id: Ulid,
    instance_id: u64,
    identity: HandleIdentity,
    /// The committed version the base bytes come from; `None` for an unpublished name.
    version_id: Option<Ulid>,
    write: bool,
    /// Opened with FILE_DELETE_ON_CLOSE, so its close may be the final delete.
    delete_on_close: bool,
    owner: StageOwner,
    info: BucketInfo,
    opened_ms: u64,
}

/// One open of the share root directory, which exists even when the bucket is empty.
#[derive(Clone, Debug)]
struct RootOpen {
    access_key: String,
    bucket: String,
    incarnation: u128,
    opened_ms: u64,
}

/// One open inside a job input view, by its exact tree path.
#[derive(Clone, Debug)]
struct ViewOpen {
    access_key: String,
    job: Ulid,
    path: String,
    kind: ObjectKind,
}

#[derive(Clone, Debug)]
enum Entry {
    Bucket(Box<FileOpen>),
    Root(RootOpen),
    Job(ViewOpen),
}

/// Serves SMB through Aruna operations only. Every call admits the credential again; an open
/// handle, a share connection or a cached name never carries authority.
pub struct ArunaSmbBackend {
    driver: Arc<DriverContext>,
    realm_id: RealmId,
    node_id: NodeId,
    credential_key: CredentialEncryptionKey,
    revoked: broadcast::Sender<Revocation>,
    /// The bucket incarnation each credential connected a share to.
    shares: Mutex<HashMap<(String, String), u128>>,
    opens: Mutex<HashMap<u64, Entry>>,
    next_handle: AtomicU64,
}

impl ArunaSmbBackend {
    pub fn new(driver: Arc<DriverContext>, config: ArunaSmbConfig) -> Self {
        let (revoked, _) = broadcast::channel(REVOCATION_QUEUE);
        Self {
            driver,
            realm_id: config.realm_id,
            node_id: config.node_id,
            credential_key: config.credential_key,
            revoked,
            shares: Mutex::new(HashMap::new()),
            opens: Mutex::new(HashMap::new()),
            next_handle: AtomicU64::new(1),
        }
    }

    /// Ends every session of `access_key`; the node bridges file server revocations here.
    pub fn publish_revocation(&self, access_key: impl Into<String>) {
        let revocation = Revocation {
            access_key: access_key.into(),
        };
        // Without a running server nobody listens, and nothing is left to end.
        let _ = self.revoked.send(revocation);
    }

    /// A sender for a bridge task that outlives a borrow of the backend.
    pub fn revocation_sender(&self) -> broadcast::Sender<Revocation> {
        self.revoked.clone()
    }

    fn store(&self, entry: Entry) -> u64 {
        let handle = self.next_handle.fetch_add(1, Ordering::Relaxed);
        self.opens.lock().insert(handle, entry);
        handle
    }

    /// The open behind `handle`, only for the credential and share that made it.
    fn entry(&self, scope: &Scope, handle: u64) -> Result<Entry, BackendError> {
        let entry = self.opens.lock().get(&handle).cloned();
        let entry = entry.ok_or(BackendError::Invalid)?;
        let owned = match (&entry, &scope.share) {
            (Entry::Bucket(open), ShareTarget::Bucket(bucket)) => {
                open.access_key == scope.credential.access_key && open.bucket == *bucket
            }
            (Entry::Root(open), ShareTarget::Bucket(bucket)) => {
                open.access_key == scope.credential.access_key && open.bucket == *bucket
            }
            (Entry::Job(open), ShareTarget::JobView(job)) => {
                open.access_key == scope.credential.access_key && open.job == *job
            }
            _ => false,
        };
        match owned {
            true => Ok(entry),
            false => Err(BackendError::Invalid),
        }
    }

    /// Changes every open of one shared instance, such as after a publish or rename.
    fn update_opens(&self, bucket: &str, instance_id: u64, change: impl Fn(&mut FileOpen)) {
        for entry in self.opens.lock().values_mut() {
            if let Entry::Bucket(open) = entry
                && open.bucket == bucket
                && open.instance_id == instance_id
            {
                change(open);
            }
        }
    }

    /// Who asks through which share, at this moment.
    fn context(&self, access_key: &str, bucket: &str, incarnation: u128) -> VfsAccessContext {
        VfsAccessContext {
            realm_id: self.realm_id,
            node_id: self.node_id,
            access_key: access_key.to_string(),
            protocol: VfsProtocol::Smb,
            target: NfsMountTarget::Bucket {
                bucket: bucket.to_string(),
                bucket_incarnation: incarnation,
            },
            read_only: false,
            now_ms: aruna_clock::unix_timestamp_millis(),
        }
    }

    fn open_context(&self, open: &FileOpen) -> VfsAccessContext {
        self.context(&open.access_key, &open.bucket, open.incarnation)
    }
}

/// A file server failure as the backend error the SMB engine maps to NTSTATUS.
pub(crate) fn vfs_failure(error: VfsError) -> BackendError {
    match error {
        VfsError::NotFound => BackendError::NotFound,
        VfsError::Denied
        | VfsError::VaultLocked
        | VfsError::KeyRequired
        | VfsError::Quarantined
        | VfsError::Distributed => BackendError::Denied,
        VfsError::InvalidName | VfsError::BadCookie => BackendError::Invalid,
        VfsError::IsDirectory => BackendError::IsDirectory,
        VfsError::NotDirectory => BackendError::NotDirectory,
        VfsError::NotEmpty => BackendError::NotEmpty,
        VfsError::Exists => BackendError::Exists,
        VfsError::CrossExport => BackendError::CrossDevice,
        VfsError::ReadOnly => BackendError::ReadOnly,
        VfsError::QuotaExceeded | VfsError::NoSpace => BackendError::DiskFull,
        VfsError::ShareViolation | VfsError::Conflict => BackendError::Sharing,
        VfsError::LockConflict => BackendError::LockConflict,
        VfsError::DeletePending => BackendError::DeletePending,
        // The handle's object is gone; the engine has no closed-file error to report it with.
        VfsError::StaleHandle | VfsError::ExpiredHandle => BackendError::NotFound,
        VfsError::Vfs(VfsFailure::NotFound) => BackendError::NotFound,
        VfsError::Vfs(VfsFailure::Overflow) => BackendError::Invalid,
        error @ (VfsError::CorruptStage
        | VfsError::Unavailable(_)
        | VfsError::Storage(_)
        | VfsError::Vfs(_)) => BackendError::Internal(error.to_string()),
    }
}

/// A job input view failure; a captured input is never replaced by another version.
pub(crate) fn view_failure(error: ViewError) -> BackendError {
    match error {
        ViewError::NotFound | ViewError::MissingVersion => BackendError::NotFound,
        ViewError::Denied | ViewError::Locked | ViewError::KeyRequired => BackendError::Denied,
        ViewError::Unavailable(reason) => BackendError::Internal(reason),
    }
}

/// A Unix millisecond time as an SMB FILETIME.
fn filetime(unix_ms: u64) -> u64 {
    unix_ms
        .saturating_add(FILETIME_OFFSET_MS)
        .saturating_mul(10_000)
}

/// The sizes and times of one object; every time is the version's or the open's own.
fn file_info(kind: ObjectKind, size: u64, unix_ms: u64, pending: bool) -> FileInfo {
    let time = filetime(unix_ms);
    FileInfo {
        kind,
        size,
        allocation: size,
        created: time,
        accessed: time,
        written: time,
        changed: time,
        delete_pending: pending,
    }
}

fn object_kind(kind: HandleKind) -> ObjectKind {
    match kind {
        HandleKind::File => ObjectKind::File,
        HandleKind::Directory => ObjectKind::Directory,
    }
}

impl SmbBackend for ArunaSmbBackend {
    async fn lookup_secret(
        &self,
        access_key: &str,
    ) -> Result<Option<CredentialSecret>, BackendError> {
        self.secret_of(access_key).await
    }

    async fn admit(&self, credential: &Credential) -> Result<(), BackendError> {
        self.admit_live(credential).await
    }

    async fn connect_tree(
        &self,
        credential: &Credential,
        share: &ShareTarget,
    ) -> Result<TreeGrant, BackendError> {
        match share {
            ShareTarget::Bucket(bucket) => self.connect_bucket(credential, bucket).await,
            ShareTarget::JobView(job) => self.connect_view(credential, *job).await,
        }
    }

    async fn lookup(&self, scope: &Scope, path: &str) -> Result<Lookup, BackendError> {
        match &scope.share {
            ShareTarget::Bucket(bucket) => self.lookup_file(scope, bucket, path).await,
            ShareTarget::JobView(job) => self.lookup_view(scope, *job, path).await,
        }
    }

    async fn open(&self, scope: &Scope, request: OpenRequest) -> Result<OpenGrant, BackendError> {
        match &scope.share {
            ShareTarget::Bucket(bucket) => self.open_file(scope, bucket, request).await,
            ShareTarget::JobView(job) => self.open_view(scope, *job, request).await,
        }
    }

    async fn close(&self, scope: &Scope, handle: u64) -> Result<FileInfo, BackendError> {
        let entry = self.entry(scope, handle)?;
        self.opens.lock().remove(&handle);
        match entry {
            Entry::Bucket(open) => self.close_file(*open).await,
            Entry::Root(open) => Ok(file_info(ObjectKind::Directory, 0, open.opened_ms, false)),
            Entry::Job(open) => self.view_info(scope, &open).await,
        }
    }

    async fn read(
        &self,
        scope: &Scope,
        handle: u64,
        offset: u64,
        length: u32,
    ) -> Result<Bytes, BackendError> {
        match self.entry(scope, handle)? {
            Entry::Bucket(open) => self.read_file(&open, offset, length).await,
            Entry::Root(_) => Err(BackendError::IsDirectory),
            Entry::Job(open) => self.read_view(scope, &open, offset, length).await,
        }
    }

    async fn write(
        &self,
        scope: &Scope,
        handle: u64,
        request: WriteRequest,
    ) -> Result<u32, BackendError> {
        match self.entry(scope, handle)? {
            Entry::Bucket(open) => self.write_file(&open, request).await,
            Entry::Root(_) => Err(BackendError::IsDirectory),
            Entry::Job(_) => Err(BackendError::ReadOnly),
        }
    }

    async fn flush(&self, scope: &Scope, handle: u64) -> Result<(), BackendError> {
        match self.entry(scope, handle)? {
            Entry::Bucket(open) => self.flush_file(&open).await,
            Entry::Root(_) => Ok(()),
            Entry::Job(_) => Err(BackendError::ReadOnly),
        }
    }

    async fn query_info(&self, scope: &Scope, handle: u64) -> Result<FileInfo, BackendError> {
        match self.entry(scope, handle)? {
            Entry::Bucket(open) => self.file_status(&open).await,
            Entry::Root(open) => self.root_status(&open).await,
            Entry::Job(open) => self.view_info(scope, &open).await,
        }
    }

    async fn set_info(
        &self,
        scope: &Scope,
        handle: u64,
        change: InfoChange,
    ) -> Result<(), BackendError> {
        match self.entry(scope, handle)? {
            Entry::Bucket(open) => match change {
                InfoChange::EndOfFile(size) => self.resize_file(&open, size).await,
                InfoChange::Basic(change) => self.touch_file(&open, change).await,
                // The allocation size is a hint; the logical size alone decides the bytes.
                InfoChange::Allocation(_) => self.touch_file(&open, BasicChange::default()).await,
            },
            Entry::Root(open) => match change {
                InfoChange::EndOfFile(_) => Err(BackendError::IsDirectory),
                InfoChange::Basic(_) | InfoChange::Allocation(_) => {
                    self.root_status(&open).await.map(|_| ())
                }
            },
            Entry::Job(_) => Err(BackendError::ReadOnly),
        }
    }

    async fn rename(
        &self,
        scope: &Scope,
        handle: u64,
        target: &str,
        replace: bool,
    ) -> Result<(), BackendError> {
        match self.entry(scope, handle)? {
            Entry::Bucket(open) => self.rename_file(&open, target, replace).await,
            Entry::Root(_) => Err(BackendError::Denied),
            Entry::Job(_) => Err(BackendError::ReadOnly),
        }
    }

    async fn set_disposition(
        &self,
        scope: &Scope,
        handle: u64,
        delete: bool,
    ) -> Result<(), BackendError> {
        match self.entry(scope, handle)? {
            Entry::Bucket(open) => self.dispose_file(&open, delete).await,
            Entry::Root(_) => Err(BackendError::Denied),
            Entry::Job(_) => Err(BackendError::ReadOnly),
        }
    }

    async fn list(
        &self,
        scope: &Scope,
        handle: u64,
        cursor: Option<String>,
    ) -> Result<ListPage, BackendError> {
        match self.entry(scope, handle)? {
            Entry::Bucket(open) => self.list_files(&open, cursor).await,
            Entry::Root(open) => self.list_root(&open, cursor).await,
            Entry::Job(open) => self.list_view(scope, &open, cursor).await,
        }
    }

    async fn lock(
        &self,
        scope: &Scope,
        handle: u64,
        ranges: Vec<LockRange>,
        wait: bool,
    ) -> Result<(), BackendError> {
        match self.entry(scope, handle)? {
            Entry::Bucket(open) => self.lock_file(&open, ranges, wait).await,
            Entry::Root(_) => Err(BackendError::IsDirectory),
            Entry::Job(_) => Err(BackendError::Unsupported),
        }
    }

    async fn unlock(
        &self,
        scope: &Scope,
        handle: u64,
        ranges: Vec<LockRange>,
    ) -> Result<(), BackendError> {
        match self.entry(scope, handle)? {
            Entry::Bucket(open) => self.unlock_file(&open, ranges).await,
            Entry::Root(_) => Err(BackendError::IsDirectory),
            Entry::Job(_) => Err(BackendError::Unsupported),
        }
    }

    /// A waiting lock ends when the engine drops its future; nothing else waits here.
    async fn cancel(&self, _scope: &Scope, _handle: u64) {}

    fn revocations(&self) -> broadcast::Receiver<Revocation> {
        self.revoked.subscribe()
    }
}

#[cfg(test)]
#[path = "aruna_tests.rs"]
mod tests;
