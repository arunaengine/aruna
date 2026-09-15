use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::id::NodeId;
use crate::structs::identity::realm::RealmId;

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
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedTaskTimer {
    pub key: TaskKey,
    #[serde(rename = "due_at_unix_millis")]
    pub due_unix_millis: u64,
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
