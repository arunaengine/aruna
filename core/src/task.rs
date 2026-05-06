use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::id::NodeId;
use crate::id::TopicId;
use crate::structs::RealmId;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TaskKey {
    TopicAnnounce(TopicId),
    RealmPresence { realm_id: RealmId, node_id: NodeId },
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
