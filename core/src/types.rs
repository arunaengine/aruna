//! Crate-wide aliases for the operation contract: effects, transaction and
//! storage key types, and the ULID aliases; name [`crate::id`] and
//! [`crate::user_id`] or the crate-root exports for the type-safe identifiers.

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
