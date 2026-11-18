//! Declares the effects operations hand to adapters for blob, storage, network and task work.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::structs::placement::record::PlacementRef;
use crate::structs::placement::transition::ProofClaim;
use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use crate::UserId;
use crate::alpn::Alpn;
use crate::audit::AuditPageRequest;
use crate::compute::SharedSecret;
use crate::document::DocumentEffect;
use crate::id::{DhtKeyId, NodeId};
use crate::jobs::JobRequest;
use crate::metadata::AuthToken;
use crate::metadata::MetadataEffect;
use crate::node_vault::VaultEntry;
use crate::operation::SubOperation;
use crate::stream::{BackendStream, StreamError};
use crate::structs::WriteGuard;
use crate::structs::execution::job::{JobRecordEnvelope, JobRecordKind};
use crate::structs::execution::source_access::{ListRequest, ResolvedSourceAccess, SourceMetadata};
use crate::structs::identity::realm::RealmId;
use crate::structs::placement::policy::PlacementPolicyRef;
use crate::structs::placement::policy::document::PolicyPublicationClaim;
use crate::structs::storage::backend_import::PlainPath;
use crate::structs::storage::blob::{
    ArchiveKey, BackendLocation, BackendRef, HiddenBlobKey, ResolvedBackend,
};
use crate::structs::storage::distribution::accounting::RangeCounters;
use crate::structs::storage::encryption::{BucketKeyRef, CopyTarget, KeyTicket, ReadLease};
use crate::structs::storage::format::Compression;
use crate::structs::storage::group_backend::{GroupStorage, GroupStorageSecret};
use crate::structs::storage::multipart::{BackendUpload, MultipartPart, WorkingShare};
use crate::structs::storage::usage::{BucketDelta, UsageDelta};
use crate::task::TaskEffect;
use crate::types::{Key, KeySpace, TxnId, Value};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::ops::Range;
use thiserror::Error;
use ulid::Ulid;

#[derive(Debug, PartialEq)]
pub enum Effect {
    Blob(BlobEffect),
    StagingSource(StagingSourceEffect),
    LocalFile(LocalFileEffect),
    Storage(StorageEffect),
    Net(NetEffect),
    Metadata(MetadataEffect),
    SubOperation(Box<dyn SubOperation>),
    /// Starts a child beside the caller while a background slot is free; answered at once by
    /// `TaskEvent::BackgroundStarted`. The child's result goes nowhere.
    Background(Box<dyn SubOperation>),
    Task(TaskEffect),
    Search(),
    Stream(),
    Index(Box<crate::key_index::IndexEffect>),
    /// Independent effects the runner runs at the same time; never persisted.
    Group(EffectGroup),
    Session(Box<crate::compute::session::SessionEffect>),
    Outbound(crate::outbound::OutboundEffect),
    /// Wakes the operation with `Event::Delayed` after this long on the runner's clock; never
    /// persisted. A run deadline that passes first cuts it off like any other effect.
    Delay(Duration),
    /// One call to an executor backend of this node; never persisted.
    Compute(Box<crate::compute::effect::ComputeEffect>),
    /// A request to the host of a running job; never persisted.
    Job(crate::jobs::JobEffect),
}

/// Independent effects or sub operations the runner runs at the same time, at most `limit` at
/// once (0 counts as 1). Storage transaction effects and nested groups are refused as members.
#[derive(Debug, PartialEq)]
pub struct EffectGroup {
    pub members: Vec<Effect>,
    pub limit: usize,
    pub form: GroupForm,
}

/// How the results of an [`EffectGroup`] come back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GroupForm {
    /// One `GroupEvent::Finished` with every result in emission order. A member that has not
    /// finished `deadline` after the group started reports `GroupResult::Unfinished`.
    All { deadline: Option<Duration> },
    /// Each result comes back as its own `GroupEvent::Member` as soon as it finishes. Effects
    /// the operation returns while the window is open join it as new members, and
    /// `GroupEvent::Closed` follows once no member runs or waits.
    Window,
}

#[derive(Debug, PartialEq)]
pub enum BlobEffect {
    //GetOperator { bucket: Option<String>, },
    // ----- Blob read & write -----
    Write {
        bucket: String,
        key: String,
        /// Backend chosen by the operation. The adapter executes, never routes.
        resolved: ResolvedBackend,
        created_by: UserId,
        blob: BackendStream<Result<Bytes, StreamError>>,
        /// Declared size of the original bytes, when known; it sizes provider chunks.
        size: Option<u64>,
    },
    WritePart {
        upload_id: Ulid,
        part_number: u16,
        /// Pinned at CreateMultipartUpload and carried by every part.
        resolved: ResolvedBackend,
        created_by: UserId,
        /// Streams the part into this provider upload instead of a blob of its own.
        backend_upload: Option<Box<BackendUpload>>,
        /// Declared part size; a provider upload needs it before the first byte.
        size: Option<u64>,
        blob: BackendStream<Result<Bytes, StreamError>>,
    },
    /// Opens a provider multipart upload where the backend has one; others answer `None`.
    OpenUpload {
        /// The upload record that will own the provider upload, written after it opens.
        record_id: Ulid,
        bucket: String,
        key: String,
        resolved: ResolvedBackend,
        created_by: UserId,
        /// The tenant path and version id an upload under an import rule ends at.
        plain: Option<PlainPath>,
    },
    /// Assembles the listed in-place parts and hashes the finished object.
    CompleteUpload {
        backend_upload: Box<BackendUpload>,
        parts: Vec<MultipartPart>,
        /// Numbers of every streamed provider part, listed or not; completion never replaces one.
        streamed: Vec<u16>,
        /// The bucket's setting at completion, which a copy composed instead applies.
        compression: Compression,
        /// Fails with `PathTaken` when a file is already at the target path.
        if_not_exists: bool,
    },
    AbortUpload {
        backend_upload: Box<BackendUpload>,
    },
    Compose {
        bucket: String,
        key: String,
        resolved: ResolvedBackend,
        created_by: UserId,
        parts: Vec<BackendLocation>,
        /// Composes raw to this path of the tenant's folders and answers `PlainWritten`.
        plain: Option<PlainPath>,
    },
    Read {
        location: BackendLocation,
    },
    ReadRange {
        location: BackendLocation,
        range: Range<u64>,
    },
    Delete {
        location: BackendLocation,
    },
    ReleaseReservation {
        id: Ulid,
    },
    /// Settles the reservation a write left behind; answered by `ReservationReconciled`.
    ReconcileReservation {
        location: BackendLocation,
    },
    /// Removes an in-place target no upload record or version owns any more; answered by
    /// `DeleteFinished`.
    DiscardTarget {
        location: BackendLocation,
    },
    /// Aborts the provider uploads of an in-place target and keeps its object; answered by
    /// `UploadAborted`.
    AbortTarget {
        location: BackendLocation,
    },
    SpoolHidden {
        namespace: Ulid,
        name: String,
        created_by: UserId,
        max_bytes: Option<u64>,
        deadline: Option<Instant>,
        blob: BackendStream<Result<Bytes, StreamError>>,
    },
    /// Spools `blob` to an unnamed local temporary file and answers its hashes with a stream
    /// of `range` (all of it by default); the file is gone once that stream ends or drops.
    SpoolLocal {
        /// The exact length: a longer or shorter stream fails the spool.
        size: u64,
        deadline: Option<Instant>,
        range: Option<Range<u64>>,
        blob: BackendStream<Result<Bytes, StreamError>>,
    },
    ReadHiddenRange {
        location: BackendLocation,
        range: Range<u64>,
    },
    DeleteHidden {
        key: HiddenBlobKey,
    },
    ListHidden {
        namespace: Option<Ulid>,
        cursor: Option<Vec<u8>>,
    },
    // ----- Replication -----
    OpenConnection {
        node_id: NodeId,
    },
    SendMessage {
        stream_id: Ulid,
        payload: Vec<u8>,
    },
    ReadMessage {
        stream_id: Ulid,
    },
    CloseConnection {
        stream_id: Ulid,
    },
    Replicate {
        replication_id: Ulid,
        stream_id: Ulid,
        location: BackendLocation,
        keep_alive: bool,
    },
    HandleReplication {
        replication_id: Option<Ulid>,
        stream_id: Ulid,
        /// Each node routes its own replica; the sender's backend is ignored.
        resolved: ResolvedBackend,
        keep_alive: bool,
        /// A plaintext replica is also sealed to this object key.
        object: Option<[u8; 32]>,
    },
    ServeRead {
        stream_id: Ulid,
        location: BackendLocation,
        expected_blake3: [u8; 32],
        /// Serves only the chunks of this byte range, each verifiable against the root.
        range: Option<Range<u64>>,
    },
    /// Serves one version this node never copied into its blob store: a device
    /// streams the owner's own file. The file is refused unless it still
    /// carries `fingerprint` and hashes to `expected_blake3`.
    ServeSourceRead {
        stream_id: Ulid,
        access: ResolvedSourceAccess,
        size: u64,
        expected_blake3: [u8; 32],
        fingerprint: String,
    },
    /// Receives a verified bao read of a `size` byte object; with `range`, only those bytes,
    /// each chunk checked against `expected_blake3`.
    ReceiveRead {
        stream_id: Ulid,
        size: u64,
        expected_blake3: [u8; 32],
        range: Option<Range<u64>>,
    },
    /// Create-time reachability proof for a tenant backend: build the guarded
    /// operator and round-trip a sentinel object.
    CheckGroupBackend {
        record: GroupStorage,
        secret: GroupStorageSecret,
    },
    /// A fresh X25519 keypair for a bucket key generation.
    GenerateBucketKey,
    /// Seals the private key of `key` to each user key; the plain key is not returned.
    SealHolderCopies {
        key: BucketKeyRef,
        public_key: [u8; 32],
        private_key: SharedSecret,
        realm_id: RealmId,
        node_id: NodeId,
        holders: Vec<CopyTarget>,
    },
    /// Seals copies with the unlocked key of `key`, which never leaves the adapter. A locked
    /// generation answers `Locked`, so the caller records the holders as pending.
    SealUnlocked {
        key: BucketKeyRef,
        realm_id: RealmId,
        node_id: NodeId,
        holders: Vec<CopyTarget>,
    },
    /// Holds a key checked against `public_key`; no read may use it before activation.
    /// `max` bounds the session from its start, including every extension.
    PrepareKey {
        key: BucketKeyRef,
        public_key: [u8; 32],
        private_key: SharedSecret,
        duration: Option<Duration>,
        max: Option<Duration>,
    },
    ActivateKey {
        ticket: KeyTicket,
    },
    DiscardKey {
        ticket: KeyTicket,
    },
    ReadKeyStatus {
        bucket_id: Ulid,
    },
    /// Moves the timed lock of one session to `duration` from now.
    ExtendKey {
        key: BucketKeyRef,
        session_id: Ulid,
        duration: Option<Duration>,
    },
    /// Locks every generation of a bucket, or only the named session for a timed lock.
    LockKey {
        bucket_id: Ulid,
        session: Option<KeyTicket>,
    },
    /// Admits a plaintext read of `archive` while its key generation is unlocked.
    AdmitRead {
        key: BucketKeyRef,
        archive: ArchiveKey,
    },
    /// Rewrites `source` into the format of `target` inside the adapter, so its plaintext never
    /// reaches an operation. A sealed source needs `lease`; `grants_only` keeps its blocks.
    RewriteCopy {
        bucket: String,
        key: String,
        source: BackendLocation,
        lease: Option<Box<ReadLease>>,
        target: Box<ResolvedBackend>,
        grants_only: bool,
        /// A new object public key, a second recipient of the sealed copy.
        object: Option<Box<[u8; 32]>>,
    },
    /// Seals one part of an encrypted upload as a Pithos piece with the plan in `resolved`.
    /// The piece records a content tree at `content_offset` when one is given.
    WritePiece {
        upload_id: Ulid,
        part_number: u16,
        resolved: ResolvedBackend,
        created_by: UserId,
        content_offset: Option<u64>,
        /// The upload's object public key, a second recipient of the piece.
        object: Option<[u8; 32]>,
        blob: BackendStream<Result<Bytes, StreamError>>,
    },
    /// Composes the stored pieces of `parts`, in order, into one archive without a key, within
    /// the working set `share` reserved before the parts were loaded.
    ComposePieces {
        bucket: String,
        key: String,
        resolved: ResolvedBackend,
        created_by: UserId,
        parts: Vec<MultipartPart>,
        share: WorkingShare,
    },
    /// Hands out the unlocked key of `key`, for its node vault copy when a bucket leaves
    /// `vault_locked`. A locked generation answers `Locked`.
    ReadUnlockedKey {
        key: BucketKeyRef,
    },
    /// Reads the sealed copy at `location` through `lease` and answers the raw hashes of its
    /// verified plaintext; the plaintext never leaves the adapter.
    HashArchive {
        location: BackendLocation,
        lease: Box<ReadLease>,
    },
    /// Streams `range`, or the whole object, of the sealed copy at `location` under `lease`.
    /// The stream keeps the lease until it ends.
    ReadSealed {
        location: BackendLocation,
        range: Option<Range<u64>>,
        lease: Box<ReadLease>,
    },
    /// Serves the plaintext of `location`, a copy of an encrypting bucket, to an authorized
    /// remote reader under `lease`. Replication sends through `ReplicateLeased`.
    ServeSealedRead {
        stream_id: Ulid,
        location: BackendLocation,
        expected_blake3: [u8; 32],
        lease: Box<ReadLease>,
        range: Option<Range<u64>>,
    },
    /// Reserves the working set of composing at most `content` bytes, before the piece records
    /// are loaded. Answers `ComposeReserved`.
    ReserveCompose {
        content: u64,
    },
    /// Sends the copy at `location` of an encrypting bucket under `lease`, which covers the
    /// transfer. A sealed copy with `regrant` is granted to that key and `object`, and sent as
    /// stored bytes; otherwise the plaintext is sent. Answers `ReplicationFinished`.
    ReplicateLeased {
        replication_id: Ulid,
        stream_id: Ulid,
        location: BackendLocation,
        lease: Box<ReadLease>,
        regrant: Option<Box<crate::structs::storage::encryption::SealPlan>>,
        object: Option<[u8; 32]>,
    },
    Abe(Box<crate::structs::storage::abe::AbeEffect>),
    /// Lists at most `limit` objects under `prefix` of `backend`, in key order after
    /// `start_after`, leaving out the empty reservations of writes. Answers `ExternalListed`.
    ListExternal {
        backend: BackendRef,
        prefix: String,
        start_after: Option<String>,
        limit: usize,
    },
    /// Hashes the object at `path`, which Aruna did not write, and answers its location on
    /// `resolved` as `ExternalHashed`. The bytes are read once and never moved.
    HashExternal {
        resolved: ResolvedBackend,
        path: String,
        created_by: UserId,
    },
    /// Streams `blob` raw to `plain.path` on `resolved`, a path in the tenant's folders, and
    /// answers `PlainWritten`. A failed write never deletes or restores what the path holds.
    WritePlain {
        resolved: ResolvedBackend,
        plain: PlainPath,
        created_by: UserId,
        blob: BackendStream<Result<Bytes, StreamError>>,
    },
    /// Copies `source`, at its recorded backend version, raw to `plain.path` on `resolved` and
    /// answers `PlainWritten` with its hashes. S3 copies inside the provider, unversioned only
    /// while it keeps its recorded `etag` or one part proves its MD5; others stream and hash.
    CopyPlain {
        resolved: ResolvedBackend,
        plain: PlainPath,
        source: Box<BackendLocation>,
        etag: Option<String>,
    },
    /// Aborts the provider upload a copy `attempt` recorded, or exactly `upload` when given,
    /// and forgets the record only while it still names that upload; answers `UploadAborted`.
    /// Uploads of other attempts at the path stay.
    SettleCopy {
        attempt: Ulid,
        upload: Option<(Box<BackendLocation>, String)>,
    },
    /// Answers `CopyStat`: whether the backend still has the object of `location`.
    StatCopy {
        location: BackendLocation,
    },
    /// Answers `PlainStat` for `path` on `backend`.
    StatPlain {
        backend: BackendRef,
        path: String,
    },
    /// Answers `ExternalStat` with the size, ETag and backend version the provider reports for
    /// the file at `path` on `resolved`, reading none of its bytes; a write's empty reservation
    /// counts as no file.
    StatExternal {
        resolved: ResolvedBackend,
        path: String,
        created_by: UserId,
    },
    /// Removes the empty object that holds `path` on `backend` for the write of `version_id`,
    /// never another object. Answers `DeleteFinished`, also when the path holds no such object.
    DropReservation {
        backend: BackendRef,
        path: String,
        version_id: Ulid,
    },
    /// Deletes exactly backend `version` of a replaced tenant file at `location`; an empty one
    /// deletes it only while its size and full ETag equal `location` and `etag`. Answers
    /// `DeleteFinished`, also for a missing file, else `CleanupUnsupported` and keeps it.
    DeletePlain {
        location: BackendLocation,
        version: String,
        etag: Option<String>,
    },
    /// Finds or creates `bucket` on a group storage without a bucket and proves that Aruna
    /// may write and delete there; answers `BucketReady`.
    EnsureBucket {
        backend: BackendRef,
        bucket: String,
    },
    /// Answers `BucketExists` for `bucket` on a group storage without a bucket; an existing
    /// bucket gets a test write and delete. Nothing is created.
    HeadBucket {
        backend: BackendRef,
        bucket: String,
    },
    /// Deletes `bucket` on a group storage without a bucket while it holds no object;
    /// answers `BucketDeleted`.
    DeleteEmptyBucket {
        backend: BackendRef,
        bucket: String,
    },
    /// One repository, provider or stored artifact call of a job or link; answers
    /// `BlobEvent::Remote`.
    Remote(Box<crate::repository::remote::RemoteRequest>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StagingSourceEffect {
    Check {
        access: ResolvedSourceAccess,
    },
    Head {
        access: ResolvedSourceAccess,
    },
    List {
        access: ResolvedSourceAccess,
        request: ListRequest,
    },
    Read {
        access: ResolvedSourceAccess,
        range: Option<Range<u64>>,
        /// The registered observation the read must stay bound to: its ETag is sent as
        /// `If-Match` without a fresh HEAD, and a failed condition is `ConditionFailed`.
        expected: Option<SourceMetadata>,
    },
    /// This node's measurements of these sources, in their order, and a random draw.
    Health {
        accesses: Vec<ResolvedSourceAccess>,
    },
}

/// Writes into a folder the owner bound on their own machine. Every variant is non-destructive by
/// construction: a write only lands through a guard the adapter re-verifies at rename time, a
/// conflicted copy never replaces anything, and a removal moves the file aside instead of unlinking it.
#[derive(Debug)]
pub enum LocalFileEffect {
    Write {
        root: String,
        relative: String,
        guard: WriteGuard,
        blob: BackendStream<Result<Bytes, StreamError>>,
    },
    /// Adds the incoming bytes beside the file under a free conflicted-copy
    /// name. The file the owner has is never touched.
    WriteConflicted {
        root: String,
        relative: String,
        /// Timestamp the copy is named after, so the name is reproducible.
        at_ms: u64,
        blob: BackendStream<Result<Bytes, StreamError>>,
    },
    /// Moves one file into the folder's trash directory, but only while it
    /// still carries the bytes the owner decided about.
    MoveAside {
        root: String,
        relative: String,
        guard: WriteGuard,
    },
    /// Weak fingerprint and blake3 of one file, read as one stable observation.
    Hash { root: String, relative: String },
}

impl PartialEq for LocalFileEffect {
    /// Streams carry no identity, so two write effects compare by their target
    /// and their guard alone.
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                LocalFileEffect::Write {
                    root,
                    relative,
                    guard,
                    ..
                },
                LocalFileEffect::Write {
                    root: other_root,
                    relative: other_relative,
                    guard: other_guard,
                    ..
                },
            ) => root == other_root && relative == other_relative && guard == other_guard,
            (
                LocalFileEffect::WriteConflicted {
                    root,
                    relative,
                    at_ms,
                    ..
                },
                LocalFileEffect::WriteConflicted {
                    root: other_root,
                    relative: other_relative,
                    at_ms: other_at,
                    ..
                },
            ) => root == other_root && relative == other_relative && at_ms == other_at,
            (
                LocalFileEffect::MoveAside {
                    root,
                    relative,
                    guard,
                },
                LocalFileEffect::MoveAside {
                    root: other_root,
                    relative: other_relative,
                    guard: other_guard,
                },
            ) => root == other_root && relative == other_relative && guard == other_guard,
            (
                LocalFileEffect::Hash { root, relative },
                LocalFileEffect::Hash {
                    root: other_root,
                    relative: other_relative,
                },
            ) => root == other_root && relative == other_relative,
            _ => false,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum StorageEffect {
    StartTransaction {
        read: bool,
    },
    CommitTransaction {
        txn_id: TxnId,
    },
    Read {
        key_space: KeySpace,
        key: Key,
        txn_id: Option<TxnId>,
    },
    BatchRead {
        reads: Vec<(KeySpace, Key)>,
        txn_id: Option<TxnId>,
    },
    Write {
        key_space: KeySpace,
        key: Key,
        value: Value,
        txn_id: Option<TxnId>,
    },
    BatchWrite {
        writes: Vec<(KeySpace, Key, Value)>,
        txn_id: Option<TxnId>,
    },
    /// Adds usage deltas to the latest stored counters when the write transaction commits.
    /// The rows never join its read set, so parallel writers do not conflict on them.
    /// Answers `BatchWriteResult`.
    AddUsage {
        key_space: KeySpace,
        deltas: Vec<(Key, UsageDelta)>,
        /// Changes of `BucketCounters` rows, applied the same way.
        buckets: Vec<(Key, BucketDelta)>,
        txn_id: TxnId,
    },
    /// Adds range counter deltas to the latest stored rows when the write transaction commits,
    /// like `AddUsage`; the rows never join its read set. Answers `BatchWriteResult`.
    AddCounters {
        key_space: KeySpace,
        deltas: Vec<(Key, RangeCounters)>,
        txn_id: TxnId,
    },
    Delete {
        key_space: KeySpace,
        key: Key,
        txn_id: Option<TxnId>,
    },
    BatchDelete {
        deletes: Vec<(KeySpace, Key)>,
        txn_id: Option<TxnId>,
    },
    AbortTransaction {
        txn_id: TxnId,
    },
    /// Persist all pending storage data with `SyncAll` durability.
    SyncAll,
    /// Iterates keys lexicographically with optional prefix and lower-bound start.
    /// `limit` caps the returned entries.
    Iter {
        key_space: KeySpace,
        prefix: Option<Key>,
        start: Option<IterStart>,
        limit: usize,
        txn_id: Option<TxnId>,
    },
    /// Reads rows from `start` to `end` in key order: at most `limit` rows and, after the first,
    /// at most `max_bytes` of keys and values. Only this interval joins a transaction's read set.
    /// Answers `IterResult`; `next_start_after` names the last returned key while rows remain.
    IterRange {
        key_space: KeySpace,
        start: IterStart,
        end: IterEnd,
        limit: usize,
        max_bytes: usize,
        txn_id: Option<TxnId>,
    },
    /// Read the lexicographically last entry in a keyspace, optionally by prefix.
    Last {
        key_space: KeySpace,
        prefix: Option<Key>,
        txn_id: Option<TxnId>,
    },
    /// Seals `secret` into the node vault under `entry`. Answers `WriteResult`.
    VaultWrite {
        entry: VaultEntry,
        secret: SharedSecret,
        txn_id: Option<TxnId>,
    },
    /// Opens the node vault record of `entry`. Answers `VaultResult`.
    VaultRead {
        entry: VaultEntry,
        txn_id: Option<TxnId>,
    },
    /// Removes the node vault record of `entry`. Answers `DeleteResult`.
    VaultDelete {
        entry: VaultEntry,
        txn_id: Option<TxnId>,
    },
    /// Between two commits: clears old rebuild rows, writes the rebuild marker and from now on
    /// adds each committed usage delta to its rebuild row too. Answers `TransactionStarted`
    /// with a read snapshot taken at that point.
    BeginUsageRebuild {
        key_space: KeySpace,
    },
    /// In one commit: every live counter row becomes its rebuild row (or is deleted without
    /// one), rebuild rows and the marker go. Answers `BatchWriteResult`; fails unless this
    /// running storage began the rebuild.
    FinishUsageRebuild {
        key_space: KeySpace,
    },
    /// Opens a secret sealed to the realm keys. Only a node that holds them answers with it.
    RealmOpen(Box<crate::realm_vault::RealmSealed>),
}

impl StorageEffect {
    /// The transaction id of an effect that can run in a caller's transaction.
    pub fn txn_slot(&mut self) -> Option<&mut Option<TxnId>> {
        match self {
            Self::Read { txn_id, .. }
            | Self::BatchRead { txn_id, .. }
            | Self::Write { txn_id, .. }
            | Self::BatchWrite { txn_id, .. }
            | Self::Delete { txn_id, .. }
            | Self::BatchDelete { txn_id, .. }
            | Self::Iter { txn_id, .. }
            | Self::IterRange { txn_id, .. }
            | Self::Last { txn_id, .. }
            | Self::VaultWrite { txn_id, .. }
            | Self::VaultRead { txn_id, .. }
            | Self::VaultDelete { txn_id, .. } => Some(txn_id),
            _ => None,
        }
    }
}

/// Dispatch lane for a storage effect. Foreground is always served before Bulk,
/// so sync traffic is never starved by background materialization work.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum StoragePriority {
    #[default]
    Foreground,
    Bulk,
}

/// Lower bound for a [`StorageEffect::Iter`] scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IterStart {
    /// Exclusive cursor: iteration begins at the first key greater than this.
    After(Key),
    /// Inclusive seek: iteration begins at this key if it exists.
    At(Key),
}

impl IterStart {
    pub fn key(&self) -> &Key {
        match self {
            IterStart::After(key) | IterStart::At(key) => key,
        }
    }
}

/// Upper bound of a [`StorageEffect::IterRange`] read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IterEnd {
    /// Exclusive: the read stops before this key.
    Before(Key),
    /// Inclusive: the read ends with this key if it exists.
    Through(Key),
}

impl IterEnd {
    pub fn key(&self) -> &Key {
        match self {
            IterEnd::Before(key) | IterEnd::Through(key) => key,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum NetEffect {
    Dht(DhtEffect),
    DocumentSync(DocumentEffect),
    Stream(StreamEffect),
    JobControl(Box<JobControlEffect>),
    AuditPage(Box<AuditPageEffect>),
    /// Signs one policy publication claim with this node's key, after the
    /// operation checked the authorizing user's realm-admin permission.
    PolicySign(Box<PolicyPublicationClaim>),
    /// Signs vault or key records with this node's key; each must name this node.
    VaultSign(Box<crate::structs::identity::user::vault::VaultRecords>),
    /// Signs a distribution snapshot; only a snapshot naming this node as publisher.
    SnapshotSign(Box<crate::structs::storage::distribution::snapshot::SnapshotClaim>),
    /// Signs a member statement; only a claim naming this node as signer.
    StatementSign(Box<crate::structs::storage::distribution::StatementClaim>),
    /// Signs one job record of an execution; only a claim naming this node as publisher.
    ExecutionSign(Box<crate::structs::execution::job::ExecutionClaim>),
    /// Reads a release crate version or asks a range holder about releases.
    Release(Box<crate::structs::storage::release::ReleaseEffect>),
    /// Sends one plugin artifact chunk to a management node; answers `NetEvent::ArtifactStored`.
    ArtifactStore(Box<crate::plugins::ArtifactStore>),
    /// Sends one notification request frame to a holder and reads its answer frame.
    Notification(Box<crate::structs::execution::notification::NotificationRequest>),
    /// Wakes the live inbox streams of these local recipients.
    WakeInbox(Vec<crate::UserId>),
    /// Records, sends or keeps a heartbeat; the runner carries it out with the net handle.
    Heartbeat(Box<crate::heartbeat::HeartbeatEffect>),
    /// Sends one encoded metadata protocol request to one peer and reads its answer.
    MetadataRequest(Box<MetadataRequestEffect>),
    /// Applies a realm configuration to this node's peer set; answers
    /// `NetEvent::DocumentPeers`.
    RefreshDocumentPeers(Box<crate::structs::identity::realm::RealmConfigDocument>),
    /// Reads one version from another node under a read grant; answers
    /// `NetEvent::GrantedRead`.
    GrantedRead(Box<crate::native_reference::GrantedReadEffect>),
    /// Reads one version from another node under a delegation; answers
    /// `NetEvent::DelegatedRead`.
    DelegatedRead(Box<crate::native_reference::DelegatedReadEffect>),
    /// Signs a read grant; only a grant naming this node as issuer. Answers
    /// `NetEvent::GrantSign`.
    GrantSign(Box<crate::structs::storage::read_grant::ReadGrant>),
    /// Checks a forwarded caller's token for a write from `peer`; answers
    /// `NetEvent::WritePeerAuth`.
    WritePeerAuth(Box<WritePeerRequest>),
    /// Reads a dataset's summary from its holders; answers `NetEvent::DatasetSummary`.
    DatasetSummary(Box<crate::structs::storage::dataset_description::DatasetSummaryRequest>),
    /// Asks how long each node has been silent; answers `NetEvent::HeartbeatSilence`.
    HeartbeatSilence(Vec<NodeId>),
    /// Works on this node's shard topics; answers `NetEvent::Shard`.
    Shard(Box<ShardNetEffect>),
    /// Creates shared sync topics with these peers; answers `NetEvent::TopicPeers`.
    EnsureTopics {
        topics: Vec<irokle::TopicId>,
        peers: Vec<NodeId>,
    },
    /// Lets these peers into existing sync topics; answers `NetEvent::TopicPeers`.
    AllowTopicPeers {
        topics: Vec<irokle::TopicId>,
        peers: Vec<NodeId>,
    },
    /// Signs this node's statement that it is ready for a group deletion plan; answers
    /// `NetEvent::DeletePlanSigned`.
    SignDeletePlan(Box<crate::structs::identity::group_delete::GroupDeletePlan>),
    /// Checks a forwarded caller's credential and that the sending peer is a realm member;
    /// answers `NetEvent::WritePeerChecked`.
    AuthorizeWritePeer {
        peer: NodeId,
        auth_token: Option<Box<crate::metadata::AuthToken>>,
    },
    /// Verifies a token that may already be revoked and reads its claims; answers
    /// `NetEvent::RevocationClaims`.
    RevocationClaims(Box<crate::metadata::AuthToken>),
    /// Routes one harvest metadata write to the document's holders; answers
    /// `NetEvent::HarvestWrite`.
    HarvestWrite(Box<crate::repository::remote::HarvestWrite>),
    /// Tells this node's dashboard streams that job state changed; answers `NetEvent::Recorded`.
    DashboardChanged,
    /// Records one suppressed watch delivery in this node's watch metrics.
    RecordSuppression(crate::metrics::WatchMetricReason),
    /// Asks for this node's id; answers `NetEvent::LocalNode`.
    LocalNode,
    /// Asks which holders this node's watch interest matches; answers `NetEvent::WatchHolders`.
    WatchHolders {
        realm_id: crate::structs::identity::realm::RealmId,
        path: String,
        kind: crate::structs::execution::notification_watch::WatchEventKind,
    },
    /// Notes a watch a local user just created on `holder`, so events reach the holder before
    /// its interest digest arrives.
    RegisterInterest {
        watch_id: ulid::Ulid,
        realm_id: crate::structs::identity::realm::RealmId,
        holder: NodeId,
        entry: crate::structs::execution::notification_watch::WatchInterestEntry,
    },
    /// Removes a watch a local user just deleted from this node's watch interest.
    RetractInterest(ulid::Ulid),
    /// Replaces this node's whole watch interest cache with a rebuilt table.
    ReplaceInterest(Box<crate::structs::execution::notification_watch::WatchInterestTable>),
    /// Replaces one realm's node map in this node's watch interest cache.
    UpdateRealmInterest {
        realm_id: crate::structs::identity::realm::RealmId,
        nodes: std::collections::HashMap<
            NodeId,
            Vec<crate::structs::execution::notification_watch::WatchInterestEntry>,
        >,
    },
}

/// One request of the metadata peer protocol, encoded by the operation that owns it. The adapter
/// sends it to `peer`, waits at most `timeout`, and never routes or retries.
/// The exact membership a set of shard topics should have.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardMembership {
    pub topics: Vec<irokle::TopicId>,
    pub members: Vec<NodeId>,
    pub publishers: Vec<NodeId>,
    pub retained: BTreeSet<NodeId>,
    pub verified: BTreeSet<irokle::TopicId>,
}

/// Shard topic work the network side of this node carries out.
#[derive(Debug, Clone, PartialEq)]
pub enum ShardNetEffect {
    /// Which of these topics have a local genesis.
    Existing(Vec<irokle::TopicId>),
    /// Reopens topics this node holds again.
    Reopen(Vec<irokle::TopicId>),
    /// Closes a topic this node left; says whether a sync still runs on it.
    Close(irokle::TopicId),
    Membership(ShardMembership),
    /// Asks peers which of these topics they already hold.
    Probe {
        topics: Vec<irokle::TopicId>,
        peers: Vec<NodeId>,
    },
    /// Pulls these topics from peers and applies what arrived.
    Pull {
        topics: Vec<irokle::TopicId>,
        peers: Vec<NodeId>,
    },
    /// Creates topics held by this node alone.
    EnsureLocal(Vec<irokle::TopicId>),
    /// Creates topics shared with these peers.
    EnsureShared {
        topics: Vec<irokle::TopicId>,
        peers: Vec<NodeId>,
    },
    /// Whether an eviction hand-off for this bucket is still journalled.
    EvictionPending(PlacementRef),
    /// The topic's fingerprint and clock.
    TopicFacts(irokle::TopicId),
    /// The hash binding a topic's genesis and the operations at a frontier.
    FrontierRoot {
        topic: irokle::TopicId,
        frontier: irokle::ActorClock,
    },
    SignProof(ProofClaim),
    FetchManifest {
        peer: NodeId,
        realm_id: RealmId,
        placement: PlacementRef,
    },
}

#[derive(Clone, PartialEq, Eq)]
pub struct MetadataRequestEffect {
    pub peer: NodeId,
    pub frame: Vec<u8>,
    pub timeout: Duration,
}

/// A forwarded caller's credential and the peer that forwarded it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WritePeerRequest {
    pub peer: NodeId,
    pub token: crate::metadata::AuthToken,
}

/// Shows the frame size only: a frame can carry credentials or the realm key.
impl std::fmt::Debug for MetadataRequestEffect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataRequestEffect")
            .field("peer", &self.peer)
            .field("frame_bytes", &self.frame.len())
            .field("timeout", &self.timeout)
            .finish()
    }
}

/// Holders one policy fetch may consult. The operation resolves them from its
/// local placement view; a longer list would turn a cache miss into a fan-out.
pub const MAX_FETCH_HOLDERS: usize = 8;

/// Holders one job-record publish or fetch may consult.
pub const MAX_RECORD_HOLDERS: usize = 8;

/// Job-family records one fetch may return, so a family with many executions is
/// always read as bounded pages.
pub const MAX_RECORD_PAGE: usize = 64;

/// Bytes of one opaque page cursor: it carries a holder's record key only.
pub const MAX_CURSOR_BYTES: usize = 128;

/// Encoded bytes of one immutable job-family record.
pub const MAX_RECORD_BYTES: usize = 1024 * 1024;

/// Encoded bytes of one fetched record page.
pub const JOB_PAGE_BYTES: usize = 4 * 1024 * 1024;

/// Why a bounded policy-fetch or job-record frame was refused. Every bound holds
/// at construction and again at decode, because a peer supplies bytes.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FrameBoundsError {
    #[error("holder list must name 1..={max} nodes")]
    HolderCount { max: usize },
    #[error("cursor must be at most {MAX_CURSOR_BYTES} bytes")]
    CursorBytes,
    #[error("page limit must be 1..={MAX_RECORD_PAGE}")]
    PageLimit,
    #[error("page must carry at most {MAX_RECORD_PAGE} records")]
    RecordCount,
    #[error("record must encode to at most {MAX_RECORD_BYTES} bytes")]
    RecordBytes,
    #[error("frame must carry the one record kind it is defined for")]
    RecordKind,
    #[error("page must encode to at most {JOB_PAGE_BYTES} bytes")]
    PageBytes,
    #[error(transparent)]
    Encoding(#[from] postcard::Error),
}

/// Postcard size of a value without materializing its encoding.
pub(crate) fn encoded_len<T>(value: &T) -> Result<usize, FrameBoundsError>
where
    T: Serialize + ?Sized,
{
    Ok(postcard::experimental::serialized_size(value)?)
}

/// Holders resolved from the local placement view, in preference order. The
/// bound is part of the type, so no caller can widen one request into a fan-out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HolderList<const MAX: usize>(Vec<NodeId>);

impl<const MAX: usize> HolderList<MAX> {
    pub fn new(holders: Vec<NodeId>) -> Result<Self, FrameBoundsError> {
        if holders.is_empty() || holders.len() > MAX {
            return Err(FrameBoundsError::HolderCount { max: MAX });
        }
        Ok(Self(holders))
    }

    pub fn as_slice(&self) -> &[NodeId] {
        &self.0
    }

    pub fn into_inner(self) -> Vec<NodeId> {
        self.0
    }
}

/// Opaque page cursor a holder minted. Decoding rejects an oversized cursor, so
/// a peer cannot return a marker the next request would have to carry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Vec<u8>")]
pub struct FetchCursor(Vec<u8>);

impl FetchCursor {
    pub fn new(cursor: Vec<u8>) -> Result<Self, FrameBoundsError> {
        if cursor.is_empty() || cursor.len() > MAX_CURSOR_BYTES {
            return Err(FrameBoundsError::CursorBytes);
        }
        Ok(Self(cursor))
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl TryFrom<Vec<u8>> for FetchCursor {
    type Error = FrameBoundsError;

    fn try_from(cursor: Vec<u8>) -> Result<Self, Self::Error> {
        Self::new(cursor)
    }
}

/// Requested page size. A requester-supplied value is clamped to the documented
/// maximum; a decoded value outside the range is a malformed frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "usize")]
pub struct PageLimit(usize);

impl PageLimit {
    pub fn new(limit: usize) -> Self {
        Self(limit.clamp(1, MAX_RECORD_PAGE))
    }

    pub fn get(self) -> usize {
        self.0
    }
}

impl Default for PageLimit {
    fn default() -> Self {
        Self(MAX_RECORD_PAGE)
    }
}

impl TryFrom<usize> for PageLimit {
    type Error = FrameBoundsError;

    fn try_from(limit: usize) -> Result<Self, Self::Error> {
        if limit == 0 || limit > MAX_RECORD_PAGE {
            return Err(FrameBoundsError::PageLimit);
        }
        Ok(Self(limit))
    }
}

/// One authenticated job-family record bounded by its encoded size, so neither a
/// publisher nor a fetched page can carry an unbounded record. The envelope is
/// carried verbatim: bounding a frame never rewrites what its publisher signed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "JobRecordEnvelope")]
pub struct JobRecordFrame(JobRecordEnvelope);

impl JobRecordFrame {
    pub fn new(envelope: JobRecordEnvelope) -> Result<Self, FrameBoundsError> {
        if encoded_len(&envelope)? > MAX_RECORD_BYTES {
            return Err(FrameBoundsError::RecordBytes);
        }
        Ok(Self(envelope))
    }

    pub fn envelope(&self) -> &JobRecordEnvelope {
        &self.0
    }

    pub fn into_inner(self) -> JobRecordEnvelope {
        self.0
    }
}

impl TryFrom<JobRecordEnvelope> for JobRecordFrame {
    type Error = FrameBoundsError;

    fn try_from(envelope: JobRecordEnvelope) -> Result<Self, Self::Error> {
        Self::new(envelope)
    }
}

/// One bounded launch offer. Decoding rejects an oversized frame and any record
/// that is not a launch, so a peer's offer is refused before the target does
/// any admission work on it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "JobRecordEnvelope")]
pub struct LaunchFrame(JobRecordEnvelope);

impl LaunchFrame {
    pub fn new(envelope: JobRecordEnvelope) -> Result<Self, FrameBoundsError> {
        if envelope.kind() != JobRecordKind::Launch {
            return Err(FrameBoundsError::RecordKind);
        }
        if encoded_len(&envelope)? > MAX_RECORD_BYTES {
            return Err(FrameBoundsError::RecordBytes);
        }
        Ok(Self(envelope))
    }

    pub fn envelope(&self) -> &JobRecordEnvelope {
        &self.0
    }

    pub fn into_inner(self) -> JobRecordEnvelope {
        self.0
    }
}

impl TryFrom<JobRecordEnvelope> for LaunchFrame {
    type Error = FrameBoundsError;

    fn try_from(envelope: JobRecordEnvelope) -> Result<Self, Self::Error> {
        Self::new(envelope)
    }
}

/// One bounded execution receipt. A target's acceptance travels in its own
/// signed envelope, so the reply to a launch offer is bounded and kind-checked
/// before the scheduler does any work on it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "JobRecordEnvelope")]
pub struct ReceiptFrame(JobRecordEnvelope);

impl ReceiptFrame {
    pub fn new(envelope: JobRecordEnvelope) -> Result<Self, FrameBoundsError> {
        if envelope.kind() != JobRecordKind::Receipt {
            return Err(FrameBoundsError::RecordKind);
        }
        if encoded_len(&envelope)? > MAX_RECORD_BYTES {
            return Err(FrameBoundsError::RecordBytes);
        }
        Ok(Self(envelope))
    }

    pub fn envelope(&self) -> &JobRecordEnvelope {
        &self.0
    }

    pub fn into_inner(self) -> JobRecordEnvelope {
        self.0
    }
}

impl TryFrom<JobRecordEnvelope> for ReceiptFrame {
    type Error = FrameBoundsError;

    fn try_from(envelope: JobRecordEnvelope) -> Result<Self, Self::Error> {
        Self::new(envelope)
    }
}

/// Fetch of one immutable placement-policy document from the holders the
/// operation resolved. The fetch tries them in order and never routes; the
/// operation verifies id, realm, and digest before it caches anything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyFetchEffect {
    pub realm_id: RealmId,
    pub holders: HolderList<MAX_FETCH_HOLDERS>,
    pub policy_ref: PlacementPolicyRef,
    pub deadline: Duration,
}

/// What a vault fetch asks the holders of one user's vault for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum VaultQuery {
    /// The caller's own heads; each holder checks the forwarded token again.
    Heads { auth_token: Option<AuthToken> },
    /// Public key records, served to realm infrastructure nodes.
    Keys,
}

/// Fetch of one user's vault heads or key records from the holders the
/// operation resolved. The fetch tries them in order and never routes.
#[derive(Debug, Clone, PartialEq)]
pub struct VaultFetchEffect {
    pub holders: HolderList<MAX_FETCH_HOLDERS>,
    pub user_id: UserId,
    pub query: VaultQuery,
    pub deadline: Duration,
}

/// Discrete job-control request to a job's immutable owner. The adapter performs
/// the frame round-trip; the routing operation only emits this.
#[derive(Debug, Clone, PartialEq)]
pub struct JobControlEffect {
    pub owner: NodeId,
    pub request: JobRequest,
}

/// The audit fan-out: one page request asked of every listed node. The adapter
/// performs the frame round-trips concurrently and answers with one aggregated
/// event, so an unreachable node cannot delay the others.
#[derive(Debug, Clone, PartialEq)]
pub struct AuditPageEffect {
    pub nodes: Vec<NodeId>,
    pub request: AuditPageRequest,
}

/// Default wall-clock budget for a DHT read whose caller has no tighter
/// contract of its own.
pub const DHT_GET_DEADLINE: Duration = Duration::from_secs(10);

/// When a DHT read is allowed to stop early.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DhtCompletion {
    /// Runs the lookup frontier to exhaustion, keeping multi-publisher reads
    /// whole. This is the only policy valid for realm presence and blob-holder
    /// discovery.
    Exhaustive,
    /// Stops at the first signature-valid, unexpired entry published by
    /// `publisher` in `realm_id`. Only a caller that knows both may use it.
    FirstUsable {
        realm_id: RealmId,
        publisher: NodeId,
    },
}

/// Deadline and completion policy the DHT driver enforces for one read. The
/// driver owns the deadline, so a timed-out or cancelled read releases its
/// operation and pending RPC state instead of running on detached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DhtGetOptions {
    pub deadline: Duration,
    pub completion: DhtCompletion,
    /// Realm whose bounded-stale presence snapshot may answer this read. Every
    /// other read always reaches the DHT.
    pub presence: Option<RealmId>,
}

impl DhtGetOptions {
    pub fn exhaustive(deadline: Duration) -> Self {
        Self {
            deadline,
            completion: DhtCompletion::Exhaustive,
            presence: None,
        }
    }

    pub fn first_usable(deadline: Duration, realm_id: RealmId, publisher: NodeId) -> Self {
        Self {
            deadline,
            completion: DhtCompletion::FirstUsable {
                realm_id,
                publisher,
            },
            presence: None,
        }
    }

    pub fn presence(deadline: Duration, realm_id: RealmId) -> Self {
        Self {
            presence: Some(realm_id),
            ..Self::exhaustive(deadline)
        }
    }
}

impl Default for DhtGetOptions {
    fn default() -> Self {
        Self::exhaustive(DHT_GET_DEADLINE)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum DhtEffect {
    Put {
        key: DhtKeyId,
        realm_id: RealmId,
        value: Vec<u8>,
        ttl: Duration,
    },
    Get {
        key: DhtKeyId,
        realm_filter: Option<RealmId>,
        options: DhtGetOptions,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum StreamEffect {
    Open { node_id: NodeId, alpn: Alpn },
    Close { stream_id: u64 },
}

/// The abort of an open storage transaction, which `take` leaves closed; none without one.
pub fn abort_effects(txn_id: &mut Option<crate::types::TxnId>) -> crate::types::Effects {
    txn_id
        .take()
        .map_or_else(smallvec::SmallVec::new, |txn_id| {
            smallvec::smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
        })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    use crate::tests::effects::*;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    #[test]
    fn bounds_receipt_reply() {
        // A launch reply is bounded and kind-checked before the scheduler acts.
        assert_eq!(
            ReceiptFrame::new(sized_receipt(MAX_RECORD_BYTES)),
            Err(FrameBoundsError::RecordBytes)
        );
        assert_eq!(
            ReceiptFrame::new(sized_launch(8)),
            Err(FrameBoundsError::RecordKind)
        );
        let accepted = sized_receipt(8);
        let frame = ReceiptFrame::new(accepted.clone()).expect("bounded receipt");
        let bytes = postcard::to_allocvec(&frame).expect("frame encodes");
        let relayed: ReceiptFrame = postcard::from_bytes(&bytes).expect("frame decodes");
        assert_eq!(relayed.envelope(), &accepted);
        assert!(relayed.envelope().verify_signature().is_ok());

        let launch = postcard::to_allocvec(&sized_launch(8)).expect("record encodes");
        assert!(postcard::from_bytes::<ReceiptFrame>(&launch).is_err());
    }

    #[test]
    fn rejects_wide_holders() {
        let holders: Vec<NodeId> = (0..=MAX_RECORD_HOLDERS as u8).map(node).collect();
        assert_eq!(
            HolderList::<MAX_RECORD_HOLDERS>::new(holders),
            Err(FrameBoundsError::HolderCount {
                max: MAX_RECORD_HOLDERS
            })
        );
        assert_eq!(
            HolderList::<MAX_FETCH_HOLDERS>::new(Vec::new()),
            Err(FrameBoundsError::HolderCount {
                max: MAX_FETCH_HOLDERS
            })
        );
        let holders = vec![node(1), node(2)];
        assert_eq!(
            HolderList::<MAX_FETCH_HOLDERS>::new(holders.clone())
                .expect("bounded holders")
                .as_slice(),
            holders.as_slice()
        );
    }

    #[test]
    fn rejects_long_cursor() {
        let cursor = vec![9u8; MAX_CURSOR_BYTES + 1];
        assert_eq!(
            FetchCursor::new(cursor.clone()),
            Err(FrameBoundsError::CursorBytes)
        );
        let encoded = postcard::to_allocvec(&cursor).expect("cursor bytes encode");
        assert!(postcard::from_bytes::<FetchCursor>(&encoded).is_err());
    }

    #[test]
    fn cursor_round_trips() {
        let cursor = FetchCursor::new(vec![3u8; 32]).expect("bounded cursor");
        let encoded = postcard::to_allocvec(&cursor).expect("cursor encodes");
        assert_eq!(postcard::from_bytes::<FetchCursor>(&encoded), Ok(cursor));
    }

    #[test]
    fn clamps_page_limit() {
        assert_eq!(PageLimit::new(usize::MAX).get(), MAX_RECORD_PAGE);
        assert_eq!(PageLimit::new(0).get(), 1);
        assert_eq!(PageLimit::new(8).get(), 8);
        assert_eq!(PageLimit::default().get(), MAX_RECORD_PAGE);
    }

    #[test]
    fn rejects_decoded_limit() {
        // A requester clamps its own limit; a peer's frame is refused instead.
        let over = postcard::to_allocvec(&(MAX_RECORD_PAGE + 1)).expect("limit encodes");
        assert!(postcard::from_bytes::<PageLimit>(&over).is_err());
        let zero = postcard::to_allocvec(&0usize).expect("limit encodes");
        assert!(postcard::from_bytes::<PageLimit>(&zero).is_err());
        let valid = postcard::to_allocvec(&MAX_RECORD_PAGE).expect("limit encodes");
        assert_eq!(
            postcard::from_bytes::<PageLimit>(&valid),
            Ok(PageLimit::new(MAX_RECORD_PAGE))
        );
    }

    #[test]
    fn rejects_large_record() {
        let envelope = sized_envelope(1024, 1200);
        assert_eq!(
            JobRecordFrame::new(envelope.clone()),
            Err(FrameBoundsError::RecordBytes)
        );
        let encoded = postcard::to_allocvec(&envelope).expect("record encodes");
        assert!(postcard::from_bytes::<JobRecordFrame>(&encoded).is_err());
        assert!(JobRecordFrame::new(sized_envelope(2, 8)).is_ok());
    }

    #[test]
    fn bounds_launch_offer() {
        // An offer is refused for size or kind before any admission work runs.
        assert_eq!(
            LaunchFrame::new(sized_launch(MAX_RECORD_BYTES)),
            Err(FrameBoundsError::RecordBytes)
        );
        assert_eq!(
            LaunchFrame::new(sized_envelope(1, 8)),
            Err(FrameBoundsError::RecordKind)
        );
        let offered = sized_launch(8);
        let frame = LaunchFrame::new(offered.clone()).expect("bounded launch");
        assert_eq!(frame.envelope(), &offered);

        let encoded = postcard::to_allocvec(&sized_envelope(1, 8)).expect("record encodes");
        assert!(postcard::from_bytes::<LaunchFrame>(&encoded).is_err());
        let oversized =
            postcard::to_allocvec(&sized_launch(MAX_RECORD_BYTES)).expect("record encodes");
        assert!(postcard::from_bytes::<LaunchFrame>(&oversized).is_err());
    }

    #[test]
    fn launch_keeps_publisher() {
        // Bounding an offer never rewrites what its scheduler signed.
        let offered = sized_launch(8);
        let frame = LaunchFrame::new(offered.clone()).expect("bounded launch");
        let bytes = postcard::to_allocvec(&frame).expect("frame encodes");
        let relayed: LaunchFrame = postcard::from_bytes(&bytes).expect("frame decodes");
        assert_eq!(relayed.envelope(), &offered);
        assert!(relayed.envelope().verify_signature().is_ok());
    }

    #[test]
    fn frame_keeps_publisher() {
        // A frame is a size bound, never a rewrite of what the publisher signed.
        let envelope = sized_envelope(2, 8);
        let frame = JobRecordFrame::new(envelope.clone()).expect("bounded record");
        let bytes = postcard::to_allocvec(&frame).expect("frame encodes");
        let relayed: JobRecordFrame = postcard::from_bytes(&bytes).expect("frame decodes");
        assert_eq!(relayed.envelope(), &envelope);
        assert!(relayed.envelope().verify_signature().is_ok());
    }
}
