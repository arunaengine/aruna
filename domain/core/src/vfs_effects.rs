//! Declares the calls operations make to the file server adapter, which owns open instances,
//! permits, reservations, locks, sparse stages and checkpoints, and the answers it gives.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::structs::storage::vfs::{
    BucketScope, DetachReason, DirectoryWitness, HandleIdentity, HandleKind, StageOwner,
};
use crate::types::GroupId;
use ulid::Ulid;

/// Which embedded file protocol owns a handle open or byte-range lock.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum VfsProtocol {
    Smb,
    Nfs,
}

/// Half-open byte range `[offset, offset + length)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ByteRange {
    pub offset: u64,
    pub length: u64,
}

/// One byte-range lock held or tested on an open file instance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ByteLock {
    pub owner: u64,
    pub offset: u64,
    pub length: u64,
    pub exclusive: bool,
    pub protocol: VfsProtocol,
}

/// Symmetric share access and deny flags checked across SMB and NFS opens.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ShareMode {
    pub read_access: bool,
    pub write_access: bool,
    pub delete_access: bool,
    pub share_read: bool,
    pub share_write: bool,
    pub share_delete: bool,
}

/// How a staged read checks exclusive byte-range locks.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReadLockCheck {
    /// Internal publication or checkpoint read; never blocked by client byte locks.
    #[default]
    Bypass,
    /// Anonymous client read with no lock owner; conflicts with any held exclusive lock.
    Anonymous,
    /// Client read on behalf of one owner; conflicts with any other owner's exclusive lock.
    Owner(u64),
}

/// One contiguous slice of a staged file read image.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StageSlice {
    /// Bytes stored in the volatile stage or immutable checkpoint.
    Written { offset: u64, data: Vec<u8> },
    /// Untouched bytes that must be read from the pinned base object version.
    Base { offset: u64, length: u64 },
    /// Zero-filled hole created by truncating past or extending beyond written data.
    Zero { offset: u64, length: u64 },
}

/// Summary of one staged or unpublished entry visible under a prefix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StagedEntry {
    pub key: String,
    pub instance_id: u64,
    pub handle_id: Option<u64>,
    pub kind: HandleKind,
    pub size: u64,
    pub dirty: bool,
    pub delete_pending: bool,
    pub unpublished_token: Option<Ulid>,
    pub change: u64,
}

/// Live in-memory state of one shared file or directory open instance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InstanceSummary {
    pub instance_id: u64,
    pub key: String,
    pub kind: HandleKind,
    pub handle_id: Option<u64>,
    pub identity: HandleIdentity,
    pub logical_size: u64,
    pub base_limit: u64,
    pub dirty: bool,
    pub delete_pending: bool,
    pub disposition_owner: Option<StageOwner>,
    pub open_count: usize,
    pub writer_count: usize,
    pub detached: Option<DetachReason>,
    pub stage_id: Option<Ulid>,
    pub generation: u64,
    pub change: u64,
    /// The committed object version the base bytes come from; `None` for an unpublished name.
    pub base_version: Option<Ulid>,
    /// Counts every staged write and resize; it never goes back, even across a rebind.
    pub edit_generation: u64,
    /// Whether every open of this instance shares delete access.
    pub allows_delete: bool,
    /// The `edit_generation` captured when the current promoted checkpoint was sealed, if any.
    pub sealed_edit: Option<u64>,
    /// The in-progress checkpoint `(stage_id, generation)` this instance claims, if any.
    pub draft: Option<(Ulid, u64)>,
}

/// One directory witness update applied to open directory instances after a mutation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WitnessUpdate {
    pub key: String,
    pub handle_id: Option<u64>,
    pub witness: DirectoryWitness,
}

/// One checkpoint file discovered on disk during cold-start inventory.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiskStage {
    pub stage_id: Ulid,
    pub generation: u64,
    pub bytes: u64,
    pub corrupt: bool,
}

/// Inputs for opening or joining a shared VFS instance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenRequest {
    pub scope: BucketScope,
    pub key: String,
    pub kind: HandleKind,
    pub handle_id: Option<u64>,
    pub identity: HandleIdentity,
    pub open_id: Ulid,
    pub protocol: VfsProtocol,
    pub share: ShareMode,
    pub delete_on_close: bool,
    pub owner: Option<StageOwner>,
    pub base_size: u64,
    pub group_id: GroupId,
    pub base_version: Option<Ulid>,
}

/// Inputs for rebinding a shared instance after a committed publish or rename.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RebindRequest {
    pub scope: BucketScope,
    pub instance_id: u64,
    pub key: String,
    pub handle_id: Option<u64>,
    pub identity: HandleIdentity,
    pub base_size: Option<u64>,
    pub clear_dirty: bool,
    pub stage_id: Option<Ulid>,
    pub generation: Option<u64>,
    /// Replaces the base version of the instance, like `identity`.
    pub base_version: Option<Ulid>,
    /// With `clear_dirty`, the overlay clears only if no edit followed this edit generation.
    pub expected_edit: Option<u64>,
}

/// State returned when an open handle closes so the operation can decide final cleanup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloseSummary {
    pub instance_id: u64,
    pub key: String,
    pub kind: HandleKind,
    pub remaining_opens: usize,
    pub remaining_writers: usize,
    pub delete_pending: bool,
    pub dirty: bool,
    pub detached: Option<DetachReason>,
    pub disposition_owner: Option<StageOwner>,
    pub stage_id: Option<Ulid>,
    pub generation: u64,
    pub logical_size: u64,
    pub handle_id: Option<u64>,
    pub identity: HandleIdentity,
    pub base_version: Option<Ulid>,
    pub edit_generation: u64,
    /// The owner of the earliest remaining writer open that has one, to take over the stage.
    pub successor_owner: Option<StageOwner>,
}

/// Inputs for restoring a durable stage row into memory during cold-start reconciliation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreRequest {
    pub scope: BucketScope,
    pub handle_id: u64,
    pub key: String,
    pub stage_id: Ulid,
    pub generation: u64,
    pub logical_size: u64,
    pub group_id: GroupId,
    pub identity: HandleIdentity,
    pub detached: Option<DetachReason>,
    pub owner: Option<StageOwner>,
    pub base_version: Option<Ulid>,
}

/// One call to the file server adapter of this node; never persisted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VfsEffect {
    /// Acquires the fair owned namespace permit for one local bucket incarnation.
    AcquirePermit { scope: BucketScope },
    /// Releases a previously acquired namespace permit.
    ReleasePermit { permit_id: Ulid },
    /// Acquires per-instance file guards in ascending instance order.
    AcquireGuards {
        scope: BucketScope,
        instances: Vec<u64>,
    },
    /// Releases previously acquired file guards.
    ReleaseGuard { guard_id: Ulid },
    /// Registers an owned mutation attempt before dispatch so concurrent callers respect it.
    BeginAttempt {
        scope: BucketScope,
        attempt_id: Ulid,
        instances: Vec<u64>,
    },
    /// Marks a mutation attempt resolved and wakes any waiters.
    FinishAttempt { attempt_id: Ulid },
    /// Waits for an in-flight mutation attempt to settle after dropping file guards.
    WaitAttempt { attempt_id: Ulid },
    /// Returns attempt ids still owned by active runners for one bucket scope.
    ActiveAttempts { scope: BucketScope },
    /// Registers a new protocol open on a live link, checking share modes and delete-pending.
    OpenInstance(OpenRequest),
    /// Removes one open handle and reports whether writers, opens, dirty data or deletion remain.
    CloseOpen { scope: BucketScope, open_id: Ulid },
    /// Looks up a live shared instance by exact object or directory key.
    LookupInstance { scope: BucketScope, key: String },
    /// Looks up a live or detached shared instance by durable NFS handle id.
    LookupHandle { scope: BucketScope, handle_id: u64 },
    /// Looks up a live or detached shared instance by its node-local instance id.
    LookupById {
        scope: BucketScope,
        instance_id: u64,
    },
    /// Lists staged and unpublished entries under `prefix` in key order, strictly after `after`
    /// and at most `limit` of them, together with the observer sequence.
    ListStaged {
        scope: BucketScope,
        prefix: String,
        after: Option<String>,
        limit: Option<usize>,
    },
    /// Rebinds a surviving instance after a committed publish or rename.
    RebindInstance(RebindRequest),
    /// Moves the open instance of a handle off its name; its holders keep reading it.
    DetachInstance {
        scope: BucketScope,
        handle_id: u64,
        instance_id: Option<u64>,
        reason: DetachReason,
    },
    /// Detaches an instance whose durable checkpoint moved to retained recovery state,
    /// releasing its checkpoint reference once no open holders remain.
    RetainInstance {
        scope: BucketScope,
        instance_id: u64,
        stage_id: Ulid,
        generation: u64,
        reason: DetachReason,
    },
    /// Advances matching directory witnesses in the open directory registry.
    UpdateWitnesses {
        scope: BucketScope,
        updates: Vec<WitnessUpdate>,
    },
    /// Sets or clears SMB delete-pending disposition on an open link.
    SetDisposition {
        scope: BucketScope,
        instance_id: u64,
        open_id: Ulid,
        pending: bool,
        owner: Option<StageOwner>,
    },
    /// Atomically updates the share reservation of an existing open after checking other opens.
    UpdateShare {
        scope: BucketScope,
        open_id: Ulid,
        share: ShareMode,
    },
    /// Acquires one or more byte-range locks, rolling back on conflict when `atomic` is set.
    LockRanges {
        scope: BucketScope,
        instance_id: u64,
        locks: Vec<ByteLock>,
        atomic: bool,
    },
    /// Releases one or more byte-range locks.
    UnlockRanges {
        scope: BucketScope,
        instance_id: u64,
        locks: Vec<ByteLock>,
    },
    /// Tests whether a conflicting byte-range lock exists without acquiring one.
    TestLock {
        scope: BucketScope,
        instance_id: u64,
        lock: ByteLock,
    },
    /// Reads staged slices and reports untouched base ranges and zero holes; `lock_check` says
    /// which exclusive byte-range locks over the range refuse the read.
    ReadStage {
        scope: BucketScope,
        instance_id: u64,
        offset: u64,
        length: u32,
        lock_check: ReadLockCheck,
    },
    /// Writes bytes to the volatile stage overlay under the instance guard.
    WriteStage {
        scope: BucketScope,
        instance_id: u64,
        owner_id: u64,
        offset: u64,
        data: Vec<u8>,
        append: bool,
        group_id: GroupId,
    },
    /// Resizes a staged file, clamping surviving base length and adjusting scratch budgets.
    TruncateStage {
        scope: BucketScope,
        instance_id: u64,
        owner_id: u64,
        new_size: u64,
        group_id: GroupId,
    },
    /// Claims the one in-progress generation of the instance, creates its new file and reports
    /// untouched base ranges to fill; an existing generation or claim is a `Conflict`.
    BeginCheckpoint {
        scope: BucketScope,
        instance_id: u64,
        stage_id: Ulid,
        generation: u64,
        group_id: GroupId,
    },
    /// Writes one base-object plaintext chunk into an in-progress checkpoint generation.
    FillCheckpoint {
        stage_id: Ulid,
        generation: u64,
        offset: u64,
        data: Vec<u8>,
    },
    /// Applies the volatile overlay onto the checkpoint generation, syncs file and parents.
    SealCheckpoint {
        scope: BucketScope,
        instance_id: u64,
        stage_id: Ulid,
        generation: u64,
    },
    /// Switches the instance to its claimed, synced generation and removes the superseded one
    /// unless another instance or claim still references it.
    PromoteCheckpoint {
        scope: BucketScope,
        instance_id: u64,
        stage_id: Ulid,
        generation: u64,
        old_generation: Option<u64>,
    },
    /// Drops the generation the instance claimed and its files; any other generation is `NotFound`.
    AbortCheckpoint {
        scope: BucketScope,
        instance_id: u64,
        stage_id: Ulid,
        generation: u64,
    },
    /// Releases an instance's in-progress draft claim without removing the sealed checkpoint file,
    /// used when readback proves a durable retained row already references that generation.
    ReleaseDraft {
        scope: BucketScope,
        instance_id: u64,
        stage_id: Ulid,
        generation: u64,
    },
    /// Reads a bounded chunk from a sealed live or retained checkpoint file.
    ReadCheckpoint {
        stage_id: Ulid,
        generation: u64,
        offset: u64,
        length: u32,
    },
    /// Removes a checkpoint file no stage row names any more; a generation an instance reads or
    /// claims is a `Conflict`.
    RemoveCheckpoint { stage_id: Ulid, generation: u64 },
    /// Releases the quota and scratch reservations of a finished attempt.
    ReleaseAttempt { attempt_id: Ulid },
    /// Allocates the next boot-fenced nonrepeating change value for a bucket or exact key.
    NextChange {
        scope: BucketScope,
        key: Option<String>,
    },
    /// Samples current change values and observer sequence to certify snapshot freshness.
    ReadChange {
        scope: BucketScope,
        keys: Vec<String>,
    },
    /// Returns the active 8-byte boot-fenced NFS write verifier.
    WriteVerifier,
    /// Advances the NFS write verifier after any loss of unstable writes.
    AdvanceVerifier,
    /// Serializes a transition to vault-locked encryption against stage admission and plaintext.
    FenceVault { scope: BucketScope },
    /// Releases a vault-transition fence, or keeps it closed on an unknown commit outcome.
    ReleaseFence { fence_id: Ulid, keep_closed: bool },
    /// Scans the stage directory on disk for cold-start reconciliation and orphan classification.
    ScanStages,
    /// Verifies that a referenced checkpoint file exists and matches its expected size.
    CheckStage {
        stage_id: Ulid,
        generation: u64,
        expected_size: u64,
    },
    /// Installs the durably-synced boot generation and write verifier before serving begins.
    ConfigureBoot {
        boot_generation: u64,
        verifier: [u8; 8],
    },
    /// Reconstructs in-memory stage tracking and budgets for a surviving durable stage row. A
    /// detached row only charges its checkpoint and answers `Done`, with no instance.
    RestoreStage(RestoreRequest),
    /// Revokes live sessions, opens and connections bound to a revoked host, mount or key.
    RevokeGrant {
        host_id: Option<Ulid>,
        mount_id: Option<Ulid>,
        access_key: Option<String>,
    },
}

/// Answers `Effect::Vfs`; never persisted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VfsEvent {
    Done,
    PermitAcquired {
        permit_id: Ulid,
    },
    GuardAcquired {
        guard_id: Ulid,
    },
    PendingMutation {
        attempt_id: Ulid,
    },
    Attempts {
        ids: Vec<Ulid>,
    },
    Opened {
        instance_id: u64,
        created: bool,
        size: u64,
        change: u64,
    },
    Closed(CloseSummary),
    Instance(Option<InstanceSummary>),
    StagedList {
        entries: Vec<StagedEntry>,
        sequence: u64,
        /// More matching entries follow the last one returned.
        truncated: bool,
    },
    LockFree,
    LockConflict {
        holder: ByteLock,
    },
    StageRead {
        slices: Vec<StageSlice>,
        logical_size: u64,
        base_limit: u64,
    },
    StageWritten {
        offset: u64,
        written: u32,
        new_size: u64,
        change: u64,
    },
    CheckpointPlan {
        missing_base: Vec<ByteRange>,
        logical_size: u64,
    },
    CheckpointWritten {
        filename: String,
        size: u64,
    },
    CheckpointData {
        data: Vec<u8>,
        eof: bool,
    },
    ChangeValue {
        value: u64,
    },
    ChangeSnapshot {
        sequence: u64,
        values: Vec<u64>,
    },
    Verifier {
        verifier: [u8; 8],
    },
    VaultFenced {
        fence_id: Ulid,
    },
    StageInventory {
        files: Vec<DiskStage>,
    },
    StageChecked {
        valid: bool,
        size: u64,
    },
    Failed(VfsFailure),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VfsFailure {
    /// This node runs no file server adapter.
    Unconfigured,
    /// The scratch volume or budget has no room; the previous stable generation stays usable.
    NoSpace,
    /// Share access or deny mode conflicted with an existing open.
    ShareViolation,
    /// A byte-range lock conflict blocked the read, write, resize or lock request.
    LockConflict,
    /// The target or an ancestor directory is marked delete-pending.
    DeletePending,
    /// The referenced instance, open, permit, guard or checkpoint does not exist.
    NotFound,
    /// Offset or size arithmetic overflowed supported bounds.
    Overflow,
    /// The boot-fenced change counter or verifier sequence reached exhaustion.
    CounterExhausted,
    /// Plaintext stages remain or the bucket is fenced for a vault-locked transition.
    VaultFenced(String),
    /// A referenced checkpoint file is missing or corrupt on disk.
    CorruptStage(String),
    /// Caller violated the permit-before-guard lock ordering rule.
    LockOrder,
    /// Another request claimed this checkpoint generation, or an instance still references it.
    Conflict,
    Io(String),
}
