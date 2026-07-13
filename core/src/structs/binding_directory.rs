//! The Placement Binding Directory and resolver (spec 6.3.4).
//!
//! The directory is each node's derived, in-memory, two-way index over the
//! immutable Placement Binding set: `handle -> tuple` for O(1) resolution and
//! `tuple -> handles` for provision/create-time reuse. It is fail-closed: two
//! different tuples for one handle mark that handle conflicted and are retained
//! for diagnosis (REQ-META-PLACEMENT-CONFLICT-001/SYNC-001); a winner is never
//! selected by arrival or wall-clock order.

use std::collections::{BTreeSet, HashMap};

use thiserror::Error;
use ulid::Ulid;

use crate::structs::{BindingTuple, DocumentClass, PlacementBinding, PlacementScope};
use crate::structured_id::{
    ALLOCATABLE_HANDLES, BucketId, BucketNotInRange, PlacementHandle, StructuredId,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum BindingError {
    /// Handle absent from the local directory. Routing fails closed as
    /// `503 placement_binding_unavailable`, never a false 404, while the
    /// RealmReplicated binding set may still be incomplete.
    #[error("no placement binding for handle {}", .0.get())]
    Unknown(PlacementHandle),
    /// Two different tuples were observed for one handle. Routing fails closed
    /// as `503 placement_binding_conflict`; no winner is ever selected.
    #[error("placement binding for handle {} is conflicted", .0.get())]
    Conflicted(PlacementHandle),
    /// `bucket >= bucket_count` of the resolved strategy (REQ-META-ID-FORMAT-001).
    #[error(transparent)]
    BucketOutOfRange(BucketNotInRange),
    /// The strategy named by the resolved binding is unknown to the caller.
    #[error("strategy {0} named by the binding is unknown")]
    UnknownStrategy(Ulid),
}

/// A fully resolved id: its base placement plus the validated bucket, ready to
/// feed the holder `select(bucket, strategy, map)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedBinding {
    pub scope: PlacementScope,
    pub document_class: DocumentClass,
    pub strategy_id: Ulid,
    pub bucket: BucketId,
}

/// Outcome of appending a binding to the authoritative set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingAppend {
    /// A new, non-conflicting binding was stored.
    Added,
    /// An identical binding was already present (idempotent no-op).
    Duplicate,
    /// Stored, but it introduces a same-handle tuple conflict. It is retained
    /// (append-only) and its handle resolves fail-closed.
    Conflict,
}

#[derive(Debug, Clone)]
enum HandleState {
    Bound(BindingTuple),
    Conflicted(BTreeSet<BindingTuple>),
}

/// The derived two-way index over the immutable binding set.
#[derive(Debug, Default, Clone)]
pub struct BindingDirectory {
    by_handle: HashMap<PlacementHandle, HandleState>,
    by_tuple: HashMap<BindingTuple, BTreeSet<PlacementHandle>>,
}

impl BindingDirectory {
    /// Rebuilds the directory from the authoritative binding set. Duplicate
    /// entries collapse; same-handle tuple conflicts are retained fail-closed.
    pub fn from_bindings(bindings: &[PlacementBinding]) -> Self {
        let mut directory = Self::default();
        for binding in bindings {
            directory.insert(binding.handle, binding.tuple());
        }
        directory
    }

    fn insert(&mut self, handle: PlacementHandle, tuple: BindingTuple) {
        match self.by_handle.get_mut(&handle) {
            None => {
                self.by_handle.insert(handle, HandleState::Bound(tuple));
                self.by_tuple.entry(tuple).or_default().insert(handle);
            }
            Some(HandleState::Bound(existing)) => {
                let existing = *existing;
                if existing == tuple {
                    return;
                }
                let mut tuples = BTreeSet::new();
                tuples.insert(existing);
                tuples.insert(tuple);
                self.by_handle
                    .insert(handle, HandleState::Conflicted(tuples));
                // A conflicted handle is a valid alias for nothing.
                if let Some(handles) = self.by_tuple.get_mut(&existing) {
                    handles.remove(&handle);
                }
            }
            Some(HandleState::Conflicted(tuples)) => {
                tuples.insert(tuple);
            }
        }
    }

    /// Resolves a handle to its tuple, or a fail-closed `Unknown`/`Conflicted`.
    pub fn resolve(&self, handle: PlacementHandle) -> Result<BindingTuple, BindingError> {
        match self.by_handle.get(&handle) {
            None => Err(BindingError::Unknown(handle)),
            Some(HandleState::Conflicted(_)) => Err(BindingError::Conflicted(handle)),
            Some(HandleState::Bound(tuple)) => Ok(*tuple),
        }
    }

    /// The numerically lowest non-conflicted handle naming this tuple, for
    /// reuse at provision/create time. Conflicted handles are never returned.
    pub fn handle_for(
        &self,
        scope: PlacementScope,
        document_class: DocumentClass,
        strategy_id: Ulid,
    ) -> Option<PlacementHandle> {
        let tuple = BindingTuple {
            scope,
            document_class,
            strategy_id,
        };
        self.by_tuple
            .get(&tuple)
            .and_then(|handles| handles.iter().next().copied())
    }

    /// Resolves a full structured id: `handle -> tuple -> strategy`, then
    /// validates `bucket < bucket_count` fail-closed. `bucket_count_of` yields
    /// the immutable `bucket_count` of the resolved strategy.
    pub fn resolve_id<I, F>(
        &self,
        id: &I,
        bucket_count_of: F,
    ) -> Result<ResolvedBinding, BindingError>
    where
        I: StructuredId,
        F: FnOnce(Ulid) -> Option<u16>,
    {
        let tuple = self.resolve(id.placement_handle())?;
        let bucket_count = bucket_count_of(tuple.strategy_id)
            .ok_or(BindingError::UnknownStrategy(tuple.strategy_id))?;
        id.validate_bucket(bucket_count)
            .map_err(BindingError::BucketOutOfRange)?;
        Ok(ResolvedBinding {
            scope: tuple.scope,
            document_class: tuple.document_class,
            strategy_id: tuple.strategy_id,
            bucket: id.bucket(),
        })
    }

    /// A borrowing resolver façade over this directory (for A4 create/read).
    pub fn resolver(&self) -> BindingResolver<'_> {
        BindingResolver { directory: self }
    }

    /// Count of handles bound to exactly one tuple (REQ-META-PLACEMENT-SCALE-001).
    pub fn allocated(&self) -> usize {
        self.by_handle
            .values()
            .filter(|state| matches!(state, HandleState::Bound(_)))
            .count()
    }

    /// Count of handles observed with conflicting tuples.
    pub fn conflicted(&self) -> usize {
        self.by_handle
            .values()
            .filter(|state| matches!(state, HandleState::Conflicted(_)))
            .count()
    }

    /// Remaining allocatable handles in the 20-bit namespace.
    pub fn remaining(&self) -> u32 {
        let used = u32::try_from(self.by_handle.len()).unwrap_or(u32::MAX);
        ALLOCATABLE_HANDLES.saturating_sub(used)
    }
}

/// A lightweight resolution façade over a [`BindingDirectory`].
pub struct BindingResolver<'a> {
    directory: &'a BindingDirectory,
}

impl<'a> BindingResolver<'a> {
    pub fn new(directory: &'a BindingDirectory) -> Self {
        Self { directory }
    }

    pub fn resolve(&self, handle: PlacementHandle) -> Result<BindingTuple, BindingError> {
        self.directory.resolve(handle)
    }

    pub fn handle_for(
        &self,
        scope: PlacementScope,
        document_class: DocumentClass,
        strategy_id: Ulid,
    ) -> Option<PlacementHandle> {
        self.directory
            .handle_for(scope, document_class, strategy_id)
    }

    pub fn resolve_id<I, F>(
        &self,
        id: &I,
        bucket_count_of: F,
    ) -> Result<ResolvedBinding, BindingError>
    where
        I: StructuredId,
        F: FnOnce(Ulid) -> Option<u16>,
    {
        self.directory.resolve_id(id, bucket_count_of)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::RealmId;
    use crate::structured_id::MetaResourceId;

    fn handle(value: u32) -> PlacementHandle {
        PlacementHandle::new(value).unwrap()
    }

    fn tuple(seed: u8) -> (PlacementScope, DocumentClass, Ulid) {
        (
            PlacementScope::Group(Ulid::from_bytes([seed; 16])),
            DocumentClass::Metadata,
            Ulid::from_bytes([seed.wrapping_add(1); 16]),
        )
    }

    fn binding(h: u32, seed: u8) -> PlacementBinding {
        let (scope, document_class, strategy_id) = tuple(seed);
        PlacementBinding {
            handle: handle(h),
            scope,
            document_class,
            strategy_id,
            allocator_range_id: None,
            allocated_by: None,
            allocated_at_ms: None,
        }
    }

    fn id_with(h: u32, bucket: u16) -> MetaResourceId {
        MetaResourceId::from_parts(1, handle(h), BucketId::new(bucket).unwrap(), 7).unwrap()
    }

    #[test]
    fn resolve_and_reuse() {
        let directory = BindingDirectory::from_bindings(&[binding(10, 1)]);
        let (scope, class, strategy) = tuple(1);

        assert_eq!(
            directory.resolve(handle(10)),
            Ok(BindingTuple {
                scope,
                document_class: class,
                strategy_id: strategy,
            })
        );
        assert_eq!(
            directory.handle_for(scope, class, strategy),
            Some(handle(10))
        );
        assert_eq!(
            directory.resolve(handle(99)),
            Err(BindingError::Unknown(handle(99)))
        );
        assert_eq!(directory.allocated(), 1);
    }

    #[test]
    fn conflict_fails_closed() {
        // Same handle, two different tuples: no winner, both routes fail closed.
        let directory = BindingDirectory::from_bindings(&[binding(10, 1), binding(10, 2)]);

        assert_eq!(
            directory.resolve(handle(10)),
            Err(BindingError::Conflicted(handle(10)))
        );
        let (scope1, class1, strategy1) = tuple(1);
        let (scope2, class2, strategy2) = tuple(2);
        assert_eq!(directory.handle_for(scope1, class1, strategy1), None);
        assert_eq!(directory.handle_for(scope2, class2, strategy2), None);
        assert_eq!(directory.conflicted(), 1);

        // Routing a full id through the conflicted handle also fails closed.
        assert_eq!(
            directory.resolve_id(&id_with(10, 0), |_| Some(64)),
            Err(BindingError::Conflicted(handle(10)))
        );
    }

    #[test]
    fn aliases_share_tuple() {
        // Different handles, same tuple: safe aliases; both resolve, reuse picks
        // the numerically lowest.
        let directory = BindingDirectory::from_bindings(&[binding(20, 1), binding(5, 1)]);
        let (scope, class, strategy) = tuple(1);

        assert!(directory.resolve(handle(20)).is_ok());
        assert!(directory.resolve(handle(5)).is_ok());
        assert_eq!(
            directory.handle_for(scope, class, strategy),
            Some(handle(5))
        );
        assert_eq!(directory.allocated(), 2);
    }

    #[test]
    fn resolution_validates_bucket() {
        let directory = BindingDirectory::from_bindings(&[binding(10, 1)]);
        let (scope, class, strategy) = tuple(1);

        let resolved = directory.resolve_id(&id_with(10, 63), |sid| {
            assert_eq!(sid, strategy);
            Some(64)
        });
        assert_eq!(
            resolved,
            Ok(ResolvedBinding {
                scope,
                document_class: class,
                strategy_id: strategy,
                bucket: BucketId::new(63).unwrap(),
            })
        );

        // bucket == bucket_count is out of range: fail closed.
        assert!(matches!(
            directory.resolve_id(&id_with(10, 64), |_| Some(64)),
            Err(BindingError::BucketOutOfRange(_))
        ));

        // A missing strategy is a fail-closed resolution error too.
        assert_eq!(
            directory.resolve_id(&id_with(10, 0), |_| None),
            Err(BindingError::UnknownStrategy(strategy))
        );
    }

    #[test]
    fn realm_scope_resolves() {
        let scope = PlacementScope::Realm(RealmId([9u8; 32]));
        let strategy_id = Ulid::from_bytes([3u8; 16]);
        let binding = PlacementBinding {
            handle: handle(7),
            scope,
            document_class: DocumentClass::Admin,
            strategy_id,
            allocator_range_id: None,
            allocated_by: None,
            allocated_at_ms: None,
        };
        let directory = BindingDirectory::from_bindings(&[binding]);
        assert_eq!(
            directory.resolver().resolve(handle(7)).map(|t| t.scope),
            Ok(scope)
        );
    }
}
