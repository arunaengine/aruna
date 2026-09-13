//! Crate-wide aliases for the operation contract: effects, transaction and
//! storage key types, and the ULID id aliases.
//!
//! The type-safe identifiers are owned by [`crate::id`] and [`crate::user_id`].
//! The bottom re-exports keep the historical `aruna_core::types::*` paths
//! working; new code names the owning module or the crate-root export.

use byteview::ByteView;
use smallvec::SmallVec;
use ulid::Ulid;

use crate::effects::Effect;

/// Operations consume events and emit effects; handles execute effects and
/// return `Event` result values. Durable domain event records originate from
/// operations/outbox flows, not from API/S3 request handlers.
pub type Effects = SmallVec<[Effect; 4]>;
pub type TxnId = Ulid;
pub type Key = ByteView;
pub type Value = ByteView;
pub type KeySpace = String;
pub type GroupId = Ulid;
pub type RoleId = Ulid;

/// Compatibility re-exports of the type-safe identifiers; `crate::id` and
/// `crate::user_id` own the definitions.
pub use crate::id::{DhtKeyId, NodeId, NodeIdExt, TopicId};
pub use crate::user_id::UserId;
