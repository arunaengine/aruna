//! Shared sans-I/O operations of the embedded SMB and NFS file servers: admission, path
//! projection, handles, staged bytes, publication, namespace changes, recovery and admin rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod admin;
pub mod handles;
pub mod io;
pub mod mutate;
pub mod path;
pub mod policy;
pub mod publish;
pub mod recovery;

pub use admin::{
    BucketScopeOperation, CheckpointChunk, CreateMountOperation, DeleteMountOperation,
    DeleteStageOperation, EnrollHostOperation, FindHostOperation, GetHostOperation,
    GetMountOperation, GetStageOperation, ListHostsOperation, ListMountsOperation,
    ListStagesOperation, LiveStageOperation, LookupHostOperation, LookupMountOperation,
    LoopbackHostOperation, PublishStageOperation, ReadCheckpointOperation, RevokeHostOperation,
    RevokeMountsOperation, RotateHostOperation,
};
pub use handles::{
    AllocateHandleOperation, ResolveHandleOperation, RetireHandleOperation, VfsListOperation,
};
pub use io::{
    ChangeSnapshot, DetachInstanceOperation, DispositionInput, InstanceTarget,
    LookupInstanceOperation, ReadChangeOperation, UpdateShareOperation, VfsCommitOperation,
    VfsDispositionOperation, VfsLockOperation, VfsOpenOperation, VfsReadOperation,
    VfsTruncateOperation, VfsWriteOperation,
};
pub use mutate::{VfsMkdirOperation, VfsRemoveOperation, VfsRenameOperation};
pub use path::{JobEntry, JobNodeKind, JobTree, JobTreeItem};
pub use policy::{AuthorizeVfsOperation, VfsAccessContext, stage_scope, vfs_policy_extras};
pub use publish::{QuotaStageOperation, VfsCloseOperation, VfsPublishOperation};
pub use recovery::{ColdStartOperation, SweepTombstonesOperation, VfsReceiptOperation};

use aruna_core::calls::{IoCall, IoError, IoResult};
use aruna_core::effects::Effect;
use aruna_core::errors::ConversionError;
use aruna_core::events::Event;
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, NFS_STATE_KEYSPACE};
use aruna_core::structs::storage::blob::{BlobHeadKey, CurrentVersionPointer};
use aruna_core::structs::storage::vfs::{
    VfsKey, VfsRecord, VfsRecordError, VfsWatchKey, decode_token, encode_token,
};
use aruna_core::types::{Key, KeySpace, Value};
use aruna_core::vfs_effects::{VfsEffect, VfsEvent, VfsFailure};
use thiserror::Error;
use ulid::Ulid;

/// How long a retired handle and a displaced cookie position stay readable as tombstones.
pub const TOMBSTONE_RETENTION_MS: u64 = 24 * 60 * 60 * 1000;
/// How long a retained checkpoint stays recoverable by its stage id before the sweep removes it.
pub const STAGE_RETENTION_MS: u64 = 7 * 24 * 60 * 60 * 1000;
/// Rows one sweep or recovery page reads at most.
pub const STATE_PAGE: usize = 256;
/// Bytes one base-object read moves into a checkpoint or a read reply at most.
pub const CHUNK_BYTES: u64 = 1024 * 1024;
/// Rows one namespace transition writes and deletes together.
pub(crate) type StateChanges = (Vec<(String, Key, Value)>, Vec<(String, Key)>);

/// A typed file server failure, mapped to a legal wire error by each frontend.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum VfsError {
    #[error("no such file or directory")]
    NotFound,
    #[error("permission denied")]
    Denied,
    #[error("the name is not valid here")]
    InvalidName,
    #[error("the object is a directory")]
    IsDirectory,
    #[error("the object is not a directory")]
    NotDirectory,
    #[error("the directory is not empty")]
    NotEmpty,
    #[error("the name already exists")]
    Exists,
    #[error("the change crosses an export")]
    CrossExport,
    #[error("the export or bucket is read-only")]
    ReadOnly,
    #[error("distributed buckets are not served by the file servers")]
    Distributed,
    #[error("the vault_locked bucket is locked or read-only")]
    VaultLocked,
    #[error("the object needs key material")]
    KeyRequired,
    #[error("the object is quarantined")]
    Quarantined,
    #[error("the group quota is exceeded")]
    QuotaExceeded,
    #[error("the scratch space is full")]
    NoSpace,
    #[error("the open conflicts with another open")]
    ShareViolation,
    #[error("a byte-range lock conflicts")]
    LockConflict,
    #[error("the name is pending deletion")]
    DeletePending,
    #[error("the handle names a deleted object")]
    StaleHandle,
    #[error("the handle's identity is no longer known")]
    ExpiredHandle,
    #[error("the directory cookie is not valid")]
    BadCookie,
    #[error("a concurrent change conflicted")]
    Conflict,
    #[error("a stage checkpoint is missing or corrupt")]
    CorruptStage,
    #[error("the file servers are unavailable: {0}")]
    Unavailable(String),
    #[error("storage failed: {0}")]
    Storage(String),
    #[error("the file server adapter failed: {0:?}")]
    Vfs(VfsFailure),
}

impl From<IoError> for VfsError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::Conflict,
            IoError::Storage(error) => Self::Storage(error.to_string()),
            other => Self::Unavailable(other.to_string()),
        }
    }
}

impl From<VfsFailure> for VfsError {
    fn from(failure: VfsFailure) -> Self {
        match failure {
            VfsFailure::NoSpace => Self::NoSpace,
            VfsFailure::ShareViolation => Self::ShareViolation,
            VfsFailure::LockConflict => Self::LockConflict,
            VfsFailure::DeletePending => Self::DeletePending,
            VfsFailure::VaultFenced(_) => Self::VaultLocked,
            VfsFailure::CorruptStage(_) => Self::CorruptStage,
            VfsFailure::Conflict => Self::Conflict,
            other => Self::Vfs(other),
        }
    }
}

impl From<VfsRecordError> for VfsError {
    fn from(error: VfsRecordError) -> Self {
        Self::Storage(error.to_string())
    }
}

impl From<ConversionError> for VfsError {
    fn from(error: ConversionError) -> Self {
        Self::Storage(error.to_string())
    }
}

type VfsResult<T> = Result<T, VfsError>;

/// One call to this node's file server adapter, never inside a storage transaction.
pub(crate) fn vfs_call(effect: VfsEffect) -> IoCall {
    IoCall::Effect(Box::new(Effect::Vfs(Box::new(effect))))
}

/// The adapter's answer; a failure becomes its typed error.
pub(crate) fn vfs_event(result: IoResult) -> VfsResult<VfsEvent> {
    match result.event()? {
        Event::Vfs(event) => match *event {
            VfsEvent::Failed(failure) => Err(failure.into()),
            event => Ok(event),
        },
        other => Err(VfsError::Unavailable(other.label().to_string())),
    }
}

/// The adapter answered with an event the step did not ask for.
pub(crate) fn unexpected(event: &VfsEvent) -> VfsError {
    VfsError::Unavailable(format!("unexpected file server answer {event:?}"))
}

pub(crate) fn state_key(key: &VfsKey) -> VfsResult<Key> {
    Ok(key.to_bytes()?.into())
}

/// Reads `keys` of `nfs_state` in this order.
pub(crate) fn state_read(keys: &[VfsKey]) -> VfsResult<IoCall> {
    let space = || NFS_STATE_KEYSPACE.to_string();
    let reads = keys.iter().map(|key| Ok((space(), state_key(key)?)));
    Ok(IoCall::Read(reads.collect::<VfsResult<_>>()?))
}

pub(crate) fn state_row<T: VfsRecord>(
    key: &VfsKey,
    record: &T,
) -> VfsResult<(KeySpace, Key, Value)> {
    let value = record.to_bytes()?;
    Ok((
        NFS_STATE_KEYSPACE.to_string(),
        state_key(key)?,
        value.into(),
    ))
}

pub(crate) fn state_delete(key: &VfsKey) -> VfsResult<(KeySpace, Key)> {
    Ok((NFS_STATE_KEYSPACE.to_string(), state_key(key)?))
}

pub(crate) fn record_of<T: VfsRecord>(value: Option<&Value>) -> VfsResult<Option<T>> {
    let record = value.map(|value| T::from_bytes(value.as_ref()));
    Ok(record.transpose()?)
}

/// A page of `nfs_state` rows below `prefix`, after `after` when given.
pub(crate) fn state_page(prefix: Vec<u8>, after: Option<Key>, limit: usize) -> IoCall {
    IoCall::Page {
        space: NFS_STATE_KEYSPACE.to_string(),
        prefix: prefix.into(),
        after,
        limit,
    }
}

pub(crate) fn head_key(bucket: &str, key: &str) -> VfsResult<Key> {
    Ok(BlobHeadKey::new(bucket, key).to_bytes()?.into())
}

pub(crate) fn watch_key(bucket: &str, key: &str) -> VfsResult<Key> {
    Ok(VfsWatchKey::new(bucket, key).to_bytes()?.into())
}

/// The head and watch key rows of one exact name, in this order.
pub(crate) fn name_reads(bucket: &str, key: &str) -> VfsResult<Vec<(KeySpace, Key)>> {
    let space = || BLOB_HEAD_KEYSPACE.to_string();
    Ok(vec![
        (space(), head_key(bucket, key)?),
        (space(), watch_key(bucket, key)?),
    ])
}

pub(crate) fn head_of(value: Option<&Value>) -> VfsResult<Option<CurrentVersionPointer>> {
    let head = value.map(|value| CurrentVersionPointer::from_bytes(value.as_ref()));
    Ok(head.transpose()?)
}

pub(crate) fn token_of(value: Option<&Value>) -> VfsResult<Option<Ulid>> {
    let token = value.map(|value| decode_token(value.as_ref()));
    Ok(token.transpose()?)
}

/// The watch key row that binds an unpublished name to `token`.
pub(crate) fn watch_row(bucket: &str, key: &str, token: Ulid) -> VfsResult<(KeySpace, Key, Value)> {
    let value = Value::from(encode_token(token).to_vec());
    Ok((
        BLOB_HEAD_KEYSPACE.to_string(),
        watch_key(bucket, key)?,
        value,
    ))
}

/// The live life of a head, `None` for an absent head or a delete marker.
pub(crate) fn live_life(head: Option<&CurrentVersionPointer>) -> Option<Ulid> {
    head.and_then(|head| head.life_id)
}

#[cfg(test)]
#[path = "path_tests.rs"]
mod path_tests;

#[cfg(test)]
#[path = "handles_tests.rs"]
mod handles_tests;

#[cfg(test)]
#[path = "change_tests.rs"]
mod change_tests;

#[cfg(test)]
#[path = "recovery_tests.rs"]
mod recovery_tests;

#[cfg(test)]
#[path = "admin_tests.rs"]
mod admin_tests;
