//! Node-local records of the embedded file servers in `nfs_state`, and the watch keys of
//! unpublished names in the head keyspace.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::errors::ConversionError;
use crate::structs::execution::job::identity::JobId;
use crate::structs::placement::policy::PlacementPolicyRef;
use crate::structs::storage::blob::{BackendRef, BlobHeadKey};
use crate::structs::storage::dataset_location::{DatasetLocation, LocationError};
use crate::types::GroupId;
use crate::user::id::UserId;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

/// Boot generation and id high-water marks: `00`.
pub const BOOT_TAG: u8 = 0x00;
/// Enrolled host by host id: `01 || host_id`.
pub const HOST_TAG: u8 = 0x01;
/// Mount binding by mount id: `02 || mount_id`.
pub const MOUNT_TAG: u8 = 0x02;
/// Handle row: `03 || bucket || 00 || incarnation || handle_id`.
pub const HANDLE_TAG: u8 = 0x03;
/// Current handle of an exact key: `04 || bucket || 00 || incarnation || key`.
pub const NAME_TAG: u8 = 0x04;
/// Old position of a displaced cookie: `05 || bucket || 00 || incarnation || cookie_id`.
pub const COOKIE_TAG: u8 = 0x05;
/// Stage rows: `06 || bucket || 00 || incarnation || 00 || handle_id` or `.. || 01 || stage_id`.
pub const STAGE_TAG: u8 = 0x06;
/// Transaction receipt by attempt id: `07 || attempt_id`.
pub const RECEIPT_TAG: u8 = 0x07;
/// Stage subprefix of a live checkpoint, keyed by its handle.
pub const LIVE_STAGE: u8 = 0x00;
/// Stage subprefix of a retained checkpoint, keyed by its stage id.
pub const RETAINED_STAGE: u8 = 0x01;
/// The first handle id a fresh node hands out.
pub const FIRST_HANDLE: u64 = 1;
/// NFS reserves cookie 0 for a restart and 1 and 2 for the dot entries.
pub const FIRST_COOKIE: u64 = 3;
/// First byte of an unpublished-name watch key; an ordinary head key starts with its bucket.
pub const WATCH_TAG: u8 = 0x00;

#[derive(Debug, Error)]
pub enum VfsRecordError {
    #[error(transparent)]
    Codec(#[from] ConversionError),
    #[error(transparent)]
    Location(#[from] LocationError),
    #[error("a folder mount needs a nonempty prefix")]
    EmptyFolder,
    #[error("a bucket scope needs a nonempty name without NUL")]
    Bucket,
    #[error("malformed nfs_state key")]
    Malformed,
    #[error("the {0} counter is exhausted")]
    Exhausted(&'static str),
}

/// One `nfs_state` value, stored as postcard.
pub trait VfsRecord: Serialize + DeserializeOwned {
    fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// A bucket's rows: its name and its 16-byte creation incarnation, so a recreated bucket of the
/// same name never sees the rows of the old one.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BucketScope {
    pub bucket: String,
    pub incarnation: u128,
}

impl BucketScope {
    pub fn new(bucket: impl Into<String>, incarnation: u128) -> Self {
        Self {
            bucket: bucket.into(),
            incarnation,
        }
    }

    /// Appends `bucket || 00 || incarnation`; a bucket name holds no NUL, so scopes never nest.
    fn push_to(&self, out: &mut Vec<u8>) -> Result<(), VfsRecordError> {
        if self.bucket.is_empty() || self.bucket.as_bytes().contains(&0) {
            return Err(VfsRecordError::Bucket);
        }
        out.extend_from_slice(self.bucket.as_bytes());
        out.push(0);
        out.extend_from_slice(&self.incarnation.to_be_bytes());
        Ok(())
    }

    fn split(raw: &[u8]) -> Result<(Self, &[u8]), VfsRecordError> {
        let end = raw.iter().position(|byte| *byte == 0);
        let end = end
            .filter(|end| *end > 0)
            .ok_or(VfsRecordError::Malformed)?;
        let bucket = utf8(&raw[..end])?;
        let (incarnation, rest) = raw[end + 1..]
            .split_first_chunk::<16>()
            .ok_or(VfsRecordError::Malformed)?;
        Ok((Self::new(bucket, u128::from_be_bytes(*incarnation)), rest))
    }
}

/// The row key of one `nfs_state` record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VfsKey {
    Boot,
    Host(Ulid),
    Mount(Ulid),
    Handle(BucketScope, u64),
    Name(BucketScope, String),
    Cookie(BucketScope, u64),
    LiveStage(BucketScope, u64),
    RetainedStage(BucketScope, Ulid),
    Receipt(Ulid),
}

impl VfsKey {
    pub fn to_bytes(&self) -> Result<Vec<u8>, VfsRecordError> {
        let scoped = |tag, scope, tail: &[u8]| {
            Self::scope_prefix(tag, scope).map(|prefix| [prefix, tail.to_vec()].concat())
        };
        match self {
            Self::Boot => Ok(vec![BOOT_TAG]),
            Self::Host(id) => Ok(tagged_id(HOST_TAG, *id)),
            Self::Mount(id) => Ok(tagged_id(MOUNT_TAG, *id)),
            Self::Receipt(id) => Ok(tagged_id(RECEIPT_TAG, *id)),
            Self::Handle(scope, id) => scoped(HANDLE_TAG, scope, &id.to_be_bytes()),
            Self::Name(scope, key) => scoped(NAME_TAG, scope, key.as_bytes()),
            Self::Cookie(scope, id) => scoped(COOKIE_TAG, scope, &id.to_be_bytes()),
            Self::LiveStage(scope, id) => {
                let prefix = Self::stage_prefix(scope, LIVE_STAGE)?;
                Ok([prefix, id.to_be_bytes().to_vec()].concat())
            }
            Self::RetainedStage(scope, id) => {
                let prefix = Self::stage_prefix(scope, RETAINED_STAGE)?;
                Ok([prefix, id.to_bytes().to_vec()].concat())
            }
        }
    }

    pub fn from_bytes(raw: &[u8]) -> Result<Self, VfsRecordError> {
        let (tag, rest) = raw.split_first().ok_or(VfsRecordError::Malformed)?;
        match *tag {
            BOOT_TAG if rest.is_empty() => Ok(Self::Boot),
            HOST_TAG => Ok(Self::Host(exact_id(rest)?)),
            MOUNT_TAG => Ok(Self::Mount(exact_id(rest)?)),
            RECEIPT_TAG => Ok(Self::Receipt(exact_id(rest)?)),
            HANDLE_TAG => {
                let (scope, rest) = BucketScope::split(rest)?;
                Ok(Self::Handle(scope, exact_number(rest)?))
            }
            NAME_TAG => {
                let (scope, rest) = BucketScope::split(rest)?;
                Ok(Self::Name(scope, utf8(rest)?.to_string()))
            }
            COOKIE_TAG => {
                let (scope, rest) = BucketScope::split(rest)?;
                Ok(Self::Cookie(scope, exact_number(rest)?))
            }
            STAGE_TAG => {
                let (scope, rest) = BucketScope::split(rest)?;
                match rest.split_first() {
                    Some((&LIVE_STAGE, id)) => Ok(Self::LiveStage(scope, exact_number(id)?)),
                    Some((&RETAINED_STAGE, id)) => Ok(Self::RetainedStage(scope, exact_id(id)?)),
                    _ => Err(VfsRecordError::Malformed),
                }
            }
            _ => Err(VfsRecordError::Malformed),
        }
    }

    /// `tag || bucket || 00 || incarnation`: every row of one record kind in one bucket.
    pub fn scope_prefix(tag: u8, scope: &BucketScope) -> Result<Vec<u8>, VfsRecordError> {
        let mut out = vec![tag];
        scope.push_to(&mut out)?;
        Ok(out)
    }

    /// The live or retained stage rows of one bucket, which never share a prefix.
    pub fn stage_prefix(scope: &BucketScope, kind: u8) -> Result<Vec<u8>, VfsRecordError> {
        let mut out = Self::scope_prefix(STAGE_TAG, scope)?;
        out.push(kind);
        Ok(out)
    }
}

fn tagged_id(tag: u8, id: Ulid) -> Vec<u8> {
    [&[tag][..], &id.to_bytes()].concat()
}

fn utf8(raw: &[u8]) -> Result<&str, VfsRecordError> {
    std::str::from_utf8(raw).map_err(|_| VfsRecordError::Malformed)
}

fn exact_id(raw: &[u8]) -> Result<Ulid, VfsRecordError> {
    let bytes = <[u8; 16]>::try_from(raw).map_err(|_| VfsRecordError::Malformed)?;
    Ok(Ulid::from_bytes(bytes))
}

fn exact_number(raw: &[u8]) -> Result<u64, VfsRecordError> {
    let bytes = <[u8; 8]>::try_from(raw).map_err(|_| VfsRecordError::Malformed)?;
    Ok(u64::from_be_bytes(bytes))
}

/// The value of a name row (`04`): the handle id the exact key maps to.
pub fn encode_handle_id(handle_id: u64) -> [u8; 8] {
    handle_id.to_be_bytes()
}

pub fn decode_handle_id(raw: &[u8]) -> Result<u64, VfsRecordError> {
    exact_number(raw)
}

/// The boot generation and the reserved id high-water marks (`00`). An id is reserved durably
/// before it is exposed and never handed out again, even after its tombstone is collected.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VfsBootState {
    pub boot_generation: u64,
    pub next_handle_id: u64,
    pub next_cookie_id: u64,
}

impl VfsRecord for VfsBootState {}

impl Default for VfsBootState {
    fn default() -> Self {
        Self {
            boot_generation: 0,
            next_handle_id: FIRST_HANDLE,
            next_cookie_id: FIRST_COOKIE,
        }
    }
}

impl VfsBootState {
    /// The state of the next boot, which the node syncs before it serves.
    pub fn next_boot(self) -> Result<Self, VfsRecordError> {
        let boot_generation = self.boot_generation.checked_add(1);
        Ok(Self {
            boot_generation: boot_generation.ok_or(VfsRecordError::Exhausted("boot"))?,
            ..self
        })
    }

    /// Reserves `count` handle ids: the first of them and the state to store before using any.
    pub fn reserve_handles(self, count: u64) -> Result<(u64, Self), VfsRecordError> {
        let next = self.next_handle_id.checked_add(count);
        let next_handle_id = next.ok_or(VfsRecordError::Exhausted("handle"))?;
        Ok((
            self.next_handle_id,
            Self {
                next_handle_id,
                ..self
            },
        ))
    }

    /// Reserves `count` cookie ids: the first of them and the state to store before using any.
    pub fn reserve_cookies(self, count: u64) -> Result<(u64, Self), VfsRecordError> {
        let next = self.next_cookie_id.checked_add(count);
        let next_cookie_id = next.ok_or(VfsRecordError::Exhausted("cookie"))?;
        Ok((
            self.next_cookie_id,
            Self {
                next_cookie_id,
                ..self
            },
        ))
    }
}

/// An enrolled NFS client host (`01`). Holds identity references and peer restrictions only,
/// never a secret or unlock material.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnrolledHostRecord {
    pub host_id: Ulid,
    /// The verified owner who enrolled the host; only this owner rotates or revokes it.
    pub owner: UserId,
    /// SHA-256 digests of the accepted client certificate public keys.
    pub fingerprints: Vec<[u8; 32]>,
    /// Source networks in CIDR notation; each connection is checked against them.
    pub allowed_networks: Vec<String>,
    /// Set only for a trusted worker pool with verified pod isolation.
    pub worker_pool: bool,
    pub created_at_ms: u64,
    pub revoked_at_ms: Option<u64>,
}

impl VfsRecord for EnrolledHostRecord {}

/// What a mount exports: a local bucket, a folder in one, or an immutable job input view.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NfsMountTarget {
    Bucket {
        bucket: String,
        bucket_incarnation: u128,
    },
    /// `prefix` is canonical: nonempty, relative, ending in exactly one `/`.
    Folder {
        bucket: String,
        bucket_incarnation: u128,
        prefix: String,
    },
    JobInputView {
        job_id: JobId,
    },
}

impl NfsMountTarget {
    pub fn for_bucket(bucket: &str, bucket_incarnation: u128) -> Result<Self, VfsRecordError> {
        let location = DatasetLocation::new(bucket, "")?;
        Ok(Self::Bucket {
            bucket: location.bucket,
            bucket_incarnation,
        })
    }

    /// A folder root canonicalized like a dataset location; `team` and `team/` are one root.
    pub fn for_folder(
        bucket: &str,
        bucket_incarnation: u128,
        prefix: &str,
    ) -> Result<Self, VfsRecordError> {
        let location = DatasetLocation::new(bucket, prefix)?;
        if location.prefix.is_empty() {
            return Err(VfsRecordError::EmptyFolder);
        }
        Ok(Self::Folder {
            bucket: location.bucket,
            bucket_incarnation,
            prefix: location.prefix,
        })
    }

    /// Whether `key` of `bucket` lies inside the export; a job view holds no bucket key.
    pub fn contains(&self, bucket: &str, key: &str) -> bool {
        match self {
            Self::Bucket { bucket: own, .. } => own == bucket,
            Self::Folder {
                bucket: own,
                prefix,
                ..
            } => own == bucket && folder_contains(prefix, key),
            Self::JobInputView { .. } => false,
        }
    }
}

/// Component containment: `team/` holds `team/a` but never `team-old/a`. A prefix that is not
/// canonical contains nothing.
pub fn folder_contains(prefix: &str, key: &str) -> bool {
    let canonical = prefix.ends_with('/') && !prefix.ends_with("//") && !prefix.starts_with('/');
    canonical && key.starts_with(prefix)
}

/// A mount binding (`02`). Peer identity and this immutable binding select the Aruna principal;
/// the displayed ids and mode only describe files to the client.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NfsMountBinding {
    pub mount_id: Ulid,
    pub host_id: Ulid,
    pub owner: UserId,
    /// The `UserAccess` whose live grants admit each request; never its secret.
    pub access_key: String,
    pub group_id: GroupId,
    pub target: NfsMountTarget,
    pub read_only: bool,
    pub allowed_networks: Vec<String>,
    pub display_uid: u32,
    pub display_gid: u32,
    pub mode: u32,
    pub created_at_ms: u64,
    /// Set when the binding is deleted or revoked so a fixed mount id is never recreated.
    pub revoked_at_ms: Option<u64>,
}

impl VfsRecord for NfsMountBinding {}

/// Why an open instance left its name while it stays readable to its holders.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DetachReason {
    /// A proven unlink through the file servers.
    Unlinked,
    /// A rename onto this name replaced it.
    Displaced,
    /// An external change ended the life the instance was bound to.
    LostLife,
}

/// The identity a file handle is bound to.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileIdentity {
    /// A committed object: its node-local life at the head.
    Committed { life_id: Ulid },
    /// An unpublished name: its watch token and the absent or marker version it was created over.
    Unpublished {
        token: Ulid,
        predecessor: Option<Ulid>,
    },
}

/// The one continuous proof that a directory still exists, by preference order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DirectoryWitness {
    /// The live `d/` marker and its life.
    Marker { life_id: Ulid },
    /// A live descendant by exact key and life.
    Descendant { key: String, life_id: Ulid },
    /// An unpublished descendant by exact key and watch token.
    Unpublished { key: String, token: Ulid },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HandleKind {
    File,
    Directory,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HandleIdentity {
    File(FileIdentity),
    Directory(DirectoryWitness),
}

impl HandleIdentity {
    pub fn kind(&self) -> HandleKind {
        match self {
            Self::File(_) => HandleKind::File,
            Self::Directory(_) => HandleKind::Directory,
        }
    }
}

/// A handle bound to a name.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveHandle {
    pub key: String,
    pub identity: HandleIdentity,
    pub mode: u32,
    /// The change counter value of the last attribute change.
    pub attribute_change: u64,
    /// The position cookie of the name in its directory.
    pub cookie_id: u64,
    /// Set once the instance left its name; it serves only its existing opens.
    pub detached: Option<DetachReason>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetireReason {
    /// The object or bucket incarnation is known to be gone.
    Stale,
    /// A directory may exist, but its continuity is unknown.
    Expired,
}

/// A handle row (`03`): a live binding, or a dated tombstone kept for its retention time.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum VfsHandleRow {
    Live(LiveHandle),
    Tombstone {
        reason: RetireReason,
        retired_at_ms: u64,
    },
}

impl VfsRecord for VfsHandleRow {}

/// The old position of a cookie a rename or deletion displaced (`05`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CookiePosition {
    pub directory_key: String,
    pub entry_key: String,
    pub retired_at_ms: u64,
}

impl VfsRecord for CookiePosition {}

/// Whose stage this is; a grant to another attempt never takes it over.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StageScope {
    User,
    Attempt { job_id: JobId },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageOwner {
    pub user: UserId,
    pub access_key: String,
    pub mount_id: Ulid,
    pub scope: StageScope,
}

/// What the stage continues, checked before every use, restart and publish.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StageBase {
    /// A committed version and the life it belongs to.
    Committed { version_id: Ulid, life_id: Ulid },
    /// An unpublished name: its watch token and original absent or marker predecessor.
    Unpublished {
        token: Ulid,
        predecessor: Option<Ulid>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StageDisposition {
    /// Mapped at its name for its handle.
    Live,
    /// Kept outside the namespace by stage id for recovery.
    Retained { reason: DetachReason },
    /// Removed at the final close without publishing.
    DeletePending,
}

/// A writerless checkpoint waiting for quota; it stays live and is retried with backoff.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuotaRetry {
    pub attempts: u32,
    pub next_at_ms: u64,
}

/// A stable NFS checkpoint (`06`): live by handle or retained by stage id.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NfsStageRecord {
    pub stage_id: Ulid,
    pub handle_id: u64,
    pub owner: StageOwner,
    pub key: String,
    /// The generation of the immutable checkpoint file below; a new one never reuses it.
    pub generation: u64,
    pub checkpoint: String,
    pub logical_size: u64,
    pub base: StageBase,
    pub source_copies: Vec<BackendRef>,
    pub source_policies: Vec<PlacementPolicyRef>,
    pub destination_copies: Vec<BackendRef>,
    pub destination_policies: Vec<PlacementPolicyRef>,
    pub disposition: StageDisposition,
    pub quota_retry: Option<QuotaRetry>,
    /// The life a detached stage belonged to after another life took its name.
    pub foreign_life: Option<Ulid>,
    pub updated_at_ms: u64,
}

impl VfsRecord for NfsStageRecord {}

/// The namespace change one committed attempt still has to finish.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReceiptChange {
    FilePublish {
        key: String,
        version_id: Ulid,
        life_id: Ulid,
        handle_id: Option<u64>,
        stage_id: Option<Ulid>,
    },
    UnpublishedRename {
        from: String,
        to: String,
        token: Ulid,
        handle_id: Option<u64>,
        displaced: Option<u64>,
    },
    DirectoryRename {
        from: String,
        to: String,
        handle_id: Option<u64>,
    },
    Remove {
        key: String,
        marker_version: Option<Ulid>,
        handle_id: Option<u64>,
    },
}

/// A short-lived receipt (`07`) written in the mutation's own transaction and shared by both
/// file servers; it fixes the outcome of that exact attempt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VfsReceipt {
    pub attempt_id: Ulid,
    pub scope: BucketScope,
    pub created_at_ms: u64,
    pub change: ReceiptChange,
}

impl VfsRecord for VfsReceipt {}

/// The watch key of an unpublished name in `BLOB_HEAD_KEYSPACE`: `00 || bucket || 00 || key`.
/// Its value is the binding token; every write of the real head removes it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VfsWatchKey {
    pub bucket: String,
    pub key: String,
}

impl VfsWatchKey {
    pub fn new(bucket: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            key: key.into(),
        }
    }

    /// The watch key of the exact head `bucket/key`.
    pub fn of_head(head: &BlobHeadKey) -> Self {
        Self::new(&head.bucket, &head.key)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, VfsRecordError> {
        let mut out = Self::bucket_prefix(&self.bucket)?;
        out.extend_from_slice(self.key.as_bytes());
        Ok(out)
    }

    pub fn from_bytes(raw: &[u8]) -> Result<Self, VfsRecordError> {
        let rest = raw
            .strip_prefix(&[WATCH_TAG])
            .ok_or(VfsRecordError::Malformed)?;
        let end = rest.iter().position(|byte| *byte == 0);
        let end = end
            .filter(|end| *end > 0)
            .ok_or(VfsRecordError::Malformed)?;
        Ok(Self::new(utf8(&rest[..end])?, utf8(&rest[end + 1..])?))
    }

    /// `00 || bucket || 00`: every watch key of one bucket.
    pub fn bucket_prefix(bucket: &str) -> Result<Vec<u8>, VfsRecordError> {
        if bucket.is_empty() || bucket.as_bytes().contains(&0) {
            return Err(VfsRecordError::Bucket);
        }
        let mut out = Vec::with_capacity(bucket.len() + 2);
        out.push(WATCH_TAG);
        out.extend_from_slice(bucket.as_bytes());
        out.push(0);
        Ok(out)
    }

    /// Whether a raw head keyspace key is a watch key rather than an ordinary head.
    pub fn is_watch_key(raw: &[u8]) -> bool {
        raw.first() == Some(&WATCH_TAG)
    }
}

/// The value of a watch key: its binding token.
pub fn encode_token(token: Ulid) -> [u8; 16] {
    token.to_bytes()
}

pub fn decode_token(raw: &[u8]) -> Result<Ulid, ConversionError> {
    Ok(Ulid::from_bytes(<[u8; 16]>::try_from(raw)?))
}

/// Told after each committed change of a real head, so storage reports S3 writes to the file
/// servers without depending on them. A call must not block the storage worker.
pub trait HeadChangeObserver: Send + Sync {
    fn on_head_changed(&self, bucket: &str, key: &str);

    /// The heads of one grouped commit.
    fn on_heads_changed(&self, heads: &[BlobHeadKey]) {
        for head in heads {
            self.on_head_changed(&head.bucket, &head.key);
        }
    }
}

#[cfg(test)]
#[path = "vfs_tests.rs"]
mod tests;
