use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::id::NodeId;
use crate::structs::RealmId;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TaskKey {
    RealmPresence { realm_id: RealmId, node_id: NodeId },
    SyncPlacements { realm_id: RealmId, node_id: NodeId },
    DrainDocumentSyncOutbox,
    PublishUsageSnapshots,
    PublishNodeInfo,
    DrainMetadataProjectionQueue,
    DrainMetadataMaterializationQueue,
    DrainMetadataGraphPruneQueue,
    DrainBlobReplicationQueue,
    DrainReferenceMetadataRefreshQueue,
    DrainNotificationOutbox,
    PruneNotifications,
    PublishWatchInterest,
    DrainJobQueue,
    PruneJobs,
    DrainSyncMirrorRepair,
    SweepHiddenBlobs,
    DrainBlobCleanupQueue,
    RefreshBlobHolders,
    // Unit variants encode as their index and that index is the storage key, so
    // a new variant only ever goes at the end: inserting one would make every
    // persisted timer behind it decode as a different task.
    DrainBlobReclaimQueue,
    DrainJobFamilyOutbox,
    DrainJobWitnessQueue,
    DrainDeviceIntake,
    ReconcileSyncedFolders,
    DrainSyncUploadOutbox,
    SettleJobTerminals,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedTaskTimer {
    pub key: TaskKey,
    pub due_at_unix_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskEffect {
    ResetTimer { key: TaskKey, after: Duration },
    ShortenTimer { key: TaskKey, after: Duration },
    CancelTimer { key: TaskKey },
    AbortRunningHandlers { key: TaskKey },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
    Error {
        key: Option<TaskKey>,
        message: String,
    },
}
