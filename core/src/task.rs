use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::document::DocumentSyncTarget;
use crate::id::NodeId;
use crate::structs::RealmId;

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
    SyncDocument {
        node_id: NodeId,
        target: DocumentSyncTarget,
        peers: Vec<NodeId>,
    },
    DrainDocumentSyncOutbox,
    DrainMetadataProjectionQueue,
    DrainMetadataMaterializationQueue,
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
