use byteview::ByteView;
use smallvec::SmallVec;
use ulid::Ulid;

use crate::effects::Effect;
pub use crate::user_id::{UserId, autosurgeon_user_id};

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

pub mod autosurgeon_ulid {
    use autosurgeon::{Hydrate, HydrateError, Prop, ReadDoc, Reconciler};
    use ulid::Ulid;
    pub fn hydrate<'a, D: ReadDoc>(
        doc: &D,
        obj: &automerge::ObjId,
        prop: Prop<'a>,
    ) -> Result<Ulid, HydrateError> {
        let inner = autosurgeon::bytes::ByteVec::hydrate(doc, obj, prop)?;
        Ok(Ulid::from_bytes(inner.as_slice().try_into().map_err(
            |_| HydrateError::unexpected("&[u8; 16]", "Invalid slice of bytes".to_string()),
        )?))
    }
    pub fn reconcile<R: Reconciler>(ulid: &Ulid, mut reconciler: R) -> Result<(), R::Error> {
        reconciler.bytes(ulid.to_bytes())
    }
}
