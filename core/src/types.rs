use byteview::ByteView;
use smallvec::SmallVec;
use ulid::Ulid;

use crate::effects::Effect;
pub use crate::user_id::UserId;

/// Event(s)->Operation->Effect(s)
pub type Effects = SmallVec<[Effect; 4]>;
pub type TxnId = Ulid;
pub type Key = ByteView;
pub type Value = ByteView;
pub type KeySpace = String;
pub type GroupId = Ulid;
pub type RoleId = Ulid;

// Re-export the new type-safe identifiers
pub use crate::id::{DhtKeyId, NodeId, NodeIdExt, TopicId};

// Backward compatibility alias - will be removed in future tasks
pub type DhtKey = [u8; 32];
