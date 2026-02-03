use byteview::ByteView;
use smallvec::SmallVec;
use ulid::Ulid;

use crate::effects::Effect;

/// Event(s)->Operation->Effect(s)
pub type Effects = SmallVec<[Effect; 4]>;
pub type TxnId = Ulid;
pub type Key = ByteView;
pub type Value = ByteView;
pub type KeySpace = String;
pub type GroupId = Ulid;
pub type UserId = Ulid;
pub type RoleId = Ulid;
