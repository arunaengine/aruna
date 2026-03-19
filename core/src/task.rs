use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::automerge::AutomergeDocumentVariant;
use crate::id::NodeId;
use crate::structs::RealmId;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TaskKey {
    AutomergeAnnounce(AutomergeDocumentVariant),
    RealmPresence { realm_id: RealmId, node_id: NodeId },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskEffect {
    ResetTimer { key: TaskKey, after: Duration },
    CancelTimer { key: TaskKey },
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
    Error {
        key: Option<TaskKey>,
        message: String,
    },
}
