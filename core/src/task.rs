//! Names the node's background tasks and their persisted timers, effects and events.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::id::NodeId;
use crate::structs::identity::realm::RealmId;
use crate::structs::{SyncPullAck, SyncPullStatus, SyncRefusal};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TaskKey {
    RealmPresence {
        realm_id: RealmId,
        node_id: NodeId,
    },
    SyncPlacements {
        realm_id: RealmId,
        node_id: NodeId,
    },
    #[serde(rename = "DrainDocumentSyncOutbox")]
    DrainSyncOutbox,
    PublishUsageSnapshots,
    PublishNodeInfo,
    #[serde(rename = "DrainMetadataProjectionQueue")]
    DrainProjectionQueue,
    #[serde(rename = "DrainMetadataMaterializationQueue")]
    DrainMaterializationQueue,
    #[serde(rename = "DrainMetadataGraphPruneQueue")]
    DrainPruneQueue,
    #[serde(rename = "DrainBlobReplicationQueue")]
    DrainReplicationQueue,
    #[serde(rename = "DrainReferenceMetadataRefreshQueue")]
    DrainRefreshQueue,
    DrainNotificationOutbox,
    PruneNotifications,
    PublishWatchInterest,
    DrainJobQueue,
    PruneJobs,
    #[serde(rename = "DrainSyncMirrorRepair")]
    DrainMirrorRepair,
    SweepHiddenBlobs,
    #[serde(rename = "DrainBlobCleanupQueue")]
    DrainCleanupQueue,
    RefreshBlobHolders,
    // Unit variant indices are persistent storage keys. Append variants only, because insertion would
    // decode every later persisted timer as a different task.
    #[serde(rename = "DrainBlobReclaimQueue")]
    DrainReclaimQueue,
    #[serde(rename = "DrainJobFamilyOutbox")]
    DrainFamilyOutbox,
    #[serde(rename = "DrainJobWitnessQueue")]
    DrainWitnessQueue,
    DrainDeviceIntake,
    ReconcileSyncedFolders,
    #[serde(rename = "DrainSyncUploadOutbox")]
    DrainUploadOutbox,
    SettleJobTerminals,
    DrainLinkQueue,
    MigrateCompression,
    /// The timed lock of one unlock session. In memory only: a restart locks every
    /// `vault_locked` bucket anyway, so the timer is never persisted.
    LockBucket {
        bucket_id: Ulid,
        generation: u64,
        session_id: Ulid,
    },
    /// Hashes the pending archives of one key generation if it is unlocked. Persisted, so a
    /// completion during an unlock session is promoted even across a restart.
    PromotePending {
        bucket_id: Ulid,
        generation: u64,
    },
    /// Delivers wakes owed to nodes waiting on this node's keys; re-armed while any remain.
    DeliverKeyWakes,
    /// Writes a bucket key audit record whose write failed, until it is stored. Persisted when
    /// storage allows; startup stores these records before it rebuilds the unlock state.
    RecordAudit {
        record: Box<crate::structs::storage::key_audit::BucketAuditRecord>,
    },
    /// Registers the realm with its registry, or withdraws it, on the reporting node.
    PublishRegistration,
    /// Starts the backend imports whose schedule is due; re-armed to the next due time.
    RunBackendImports,
    /// Prunes stale connector reference rows queued by a bounded dependency check.
    PruneConnectorReferences,
    /// Accounts recorded splits, rebuilds split children and splits ranges above thresholds.
    MaintainRanges,
    /// Prunes the job families whose prune index row is due, page by page.
    PruneFamilies,
}

impl TaskKey {
    /// The row key of this timer in the persisted timer keyspace.
    pub fn storage_key(&self) -> Result<crate::types::Key, crate::errors::ConversionError> {
        Ok(postcard::to_allocvec(self)?.into())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedTaskTimer {
    pub key: TaskKey,
    #[serde(rename = "due_at_unix_millis")]
    pub due_unix_millis: u64,
}

/// The message of the error a task effect gets on a node without a task handler.
pub const NO_TASK_HANDLE: &str = "task handle unavailable";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskEffect {
    ResetTimer {
        key: TaskKey,
        after: Duration,
    },
    ShortenTimer {
        key: TaskKey,
        after: Duration,
    },
    CancelTimer {
        key: TaskKey,
    },
    AbortRunningHandlers {
        key: TaskKey,
    },
    /// Arms the timer unless it is armed already or its handler runs.
    ScheduleIdleTimer {
        key: TaskKey,
        after: Duration,
    },
    Pull(PullEffect),
}

/// The synced-folder pulls this node runs, by receipt key; answered by `TaskEvent::Pull`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PullEffect {
    /// Claims a pull of `total` bytes: its progress, or the status of the run holding the key.
    Claim { key: Vec<u8>, total: u64 },
    /// Ends a run; a refusal is kept until the device asked for it once.
    Finish {
        key: Vec<u8>,
        result: Result<SyncPullAck, SyncRefusal>,
    },
    /// Where the run of `key` stands, when this node runs or refused it.
    Status { key: Vec<u8> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PullEvent {
    Claimed(Result<PullProgress, SyncPullStatus>),
    Finished,
    Status(Option<SyncPullStatus>),
}

/// The bytes of a pull that arrived so far, shared with the poll that reports them. A run holds
/// it until it ends, so a dropped run frees its claim.
#[derive(Clone, Debug, Default)]
pub struct PullProgress(pub Arc<AtomicU64>);

/// Two progress counters are the same when they count the same pull.
impl PartialEq for PullProgress {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for PullProgress {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TaskEvent {
    TimerScheduled {
        key: TaskKey,
        after: Duration,
    },
    TimerCancelled {
        key: TaskKey,
    },
    RunningHandlersAborted {
        key: TaskKey,
        count: usize,
    },
    /// Answers `Effect::Background`: whether a free slot let the child start.
    BackgroundStarted {
        started: bool,
    },
    Pull(PullEvent),
    Error {
        key: Option<TaskKey>,
        message: String,
    },
}
