//! Node-local placement-handle allocation from coordinator-granted ranges.
//! The durable cursor is persisted before returning, so crashes may skip a
//! handle but cannot reissue one.

use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use aruna_core::NodeId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::NODE_STATE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::{
    Actor, DocumentClass, HandleAllocationCursor, HandleRange, PlacementBinding, PlacementScope,
    RealmId,
};
use aruna_core::structured_id::PlacementHandle;
use aruna_core::types::{Effects, Key};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::driver::DriverContext;
use crate::get_realm_config::GetRealmConfigError;
use crate::mutate_realm_placement::{
    MutateRealmPlacementConfig, MutateRealmPlacementError, MutateRealmPlacementOperation,
    RealmPlacementMutation,
};

/// Node-local key for the durable allocation cursor. Kept in the non-replicated
/// node-state keyspace, one record per realm, distinct from the singleton
/// `node_state` record.
fn allocation_cursor_key(realm_id: &RealmId) -> Key {
    let mut bytes = b"handle_allocation_cursor:".to_vec();
    bytes.extend_from_slice(realm_id.as_bytes());
    Key::from(bytes)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// A handle drawn from a granted range, with the provenance a binding must carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AllocatedHandle {
    pub handle: PlacementHandle,
    pub allocator_range_id: Ulid,
    pub allocated_by: NodeId,
    pub allocated_at_ms: u64,
}

#[derive(Debug, Error, PartialEq)]
pub enum HandleAllocationError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("unexpected storage event: {0}")]
    UnexpectedStorageEvent(String),
    #[error("placement_handle_exhausted: node {node} has spent every handle in its granted ranges")]
    PlacementHandleExhausted { node: NodeId },
    #[error(transparent)]
    Append(#[from] MutateRealmPlacementError),
    #[error(transparent)]
    ReadConfig(#[from] GetRealmConfigError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HandleAllocationState {
    Init,
    ReadCursor,
    WriteCursor,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
struct AllocateHandleOperation {
    realm_id: RealmId,
    node_id: NodeId,
    ranges: Vec<HandleRange>,
    allocated_at_ms: u64,
    state: HandleAllocationState,
    output: Option<Result<AllocatedHandle, HandleAllocationError>>,
}

impl AllocateHandleOperation {
    fn new(
        realm_id: RealmId,
        node_id: NodeId,
        ranges: Vec<HandleRange>,
        allocated_at_ms: u64,
    ) -> Self {
        Self {
            realm_id,
            node_id,
            ranges,
            allocated_at_ms,
            state: HandleAllocationState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: HandleAllocationError) -> Effects {
        self.state = HandleAllocationState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected(&mut self, event: Event) -> Effects {
        self.fail(HandleAllocationError::UnexpectedStorageEvent(format!(
            "{event:?}"
        )))
    }
}

impl Operation for AllocateHandleOperation {
    type Output = AllocatedHandle;
    type Error = HandleAllocationError;

    fn start(&mut self) -> Effects {
        self.state = HandleAllocationState::ReadCursor;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: NODE_STATE_KEYSPACE.to_string(),
            key: allocation_cursor_key(&self.realm_id),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            HandleAllocationState::ReadCursor => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    let mut cursor = match value {
                        Some(bytes) => match postcard::from_bytes(&bytes) {
                            Ok(cursor) => cursor,
                            Err(error) => return self.fail(ConversionError::from(error).into()),
                        },
                        None => HandleAllocationCursor::new(),
                    };
                    let Some((handle, allocator_range_id)) = cursor.allocate(&self.ranges) else {
                        return self.fail(HandleAllocationError::PlacementHandleExhausted {
                            node: self.node_id,
                        });
                    };
                    let value = match postcard::to_allocvec(&cursor) {
                        Ok(value) => value,
                        Err(error) => return self.fail(ConversionError::from(error).into()),
                    };
                    self.output = Some(Ok(AllocatedHandle {
                        handle,
                        allocator_range_id,
                        allocated_by: self.node_id,
                        allocated_at_ms: self.allocated_at_ms,
                    }));
                    self.state = HandleAllocationState::WriteCursor;
                    smallvec![Effect::Storage(StorageEffect::Write {
                        key_space: NODE_STATE_KEYSPACE.to_string(),
                        key: allocation_cursor_key(&self.realm_id),
                        value: value.into(),
                        txn_id: None,
                    })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected(other),
            },
            HandleAllocationState::WriteCursor => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => {
                    self.state = HandleAllocationState::Finish;
                    smallvec![]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected(other),
            },
            HandleAllocationState::Init => self.unexpected(event),
            HandleAllocationState::Finish | HandleAllocationState::Error => self.unexpected(event),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            HandleAllocationState::Finish | HandleAllocationState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or_else(|| {
            Err(HandleAllocationError::UnexpectedStorageEvent(
                "handle allocation finalized before completion".to_string(),
            ))
        })
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// Draws the next handle for `node_id` from its granted (non-conflicted) ranges,
/// advancing and durably persisting the cursor before returning. Returns
/// `placement_handle_exhausted` once the node's ranges are spent.
pub async fn allocate_handle(
    context: &DriverContext,
    realm_id: RealmId,
    node_id: NodeId,
) -> Result<AllocatedHandle, HandleAllocationError> {
    let _guard = allocation_lock().lock().await;
    let config = crate::driver::drive(
        crate::get_realm_config::GetRealmConfigOperation::new(realm_id),
        context,
    )
    .await?;
    let ranges = config.handle_range_directory().granted_to(&node_id);
    crate::driver::drive(
        AllocateHandleOperation::new(realm_id, node_id, ranges, now_ms()),
        context,
    )
    .await
}

fn allocation_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

/// Allocates a fresh handle for a `(scope, class, strategy)` and appends its
/// immutable Placement Binding with the three provenance fields set. This is the
/// production path new-scope provisioning uses.
pub async fn allocate_placement_binding(
    context: &DriverContext,
    actor: Actor,
    scope: PlacementScope,
    document_class: DocumentClass,
    strategy_id: Ulid,
) -> Result<PlacementBinding, HandleAllocationError> {
    let allocated = allocate_handle(context, actor.realm_id, actor.node_id).await?;
    let binding = PlacementBinding {
        handle: allocated.handle,
        scope,
        document_class,
        strategy_id,
        allocator_range_id: Some(allocated.allocator_range_id),
        allocated_by: Some(allocated.allocated_by),
        allocated_at_ms: Some(allocated.allocated_at_ms),
    };
    crate::driver::drive(
        MutateRealmPlacementOperation::new(MutateRealmPlacementConfig {
            actor,
            mutation: RealmPlacementMutation::AppendPlacementBinding(binding.clone()),
        }),
        context,
    )
    .await?;
    Ok(binding)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::document::DocumentSyncTarget;
    use aruna_core::events::Event;
    use aruna_core::structs::{
        FIRST_GRANTABLE_HANDLE, HandleRange, RealmConfigDocument, RealmNodeKind,
    };
    use aruna_core::types::UserId;
    use tempfile::tempdir;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn actor(realm_id: RealmId) -> Actor {
        Actor {
            node_id: node(1),
            user_id: UserId::local(Ulid::from_bytes([1; 16]), realm_id),
            realm_id,
        }
    }

    fn context(root: &str) -> DriverContext {
        DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(root).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        }
    }

    async fn seed_range_config(
        context: &DriverContext,
        actor: &Actor,
        range: HandleRange,
    ) -> RealmConfigDocument {
        let mut document = RealmConfigDocument::new(actor.realm_id, Vec::new(), 3);
        document.seed_default_placement();
        document.ensure_node(actor.node_id, RealmNodeKind::Management);
        document.placement_handle_ranges.push(range);
        let target = DocumentSyncTarget::RealmConfig {
            realm_id: actor.realm_id,
        };
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: target.storage_keyspace().to_string(),
                key: target.storage_key(),
                value: document.to_bytes(actor).unwrap().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
        document
    }

    fn range(owner: NodeId, start: u32, end: u32) -> HandleRange {
        HandleRange {
            range_id: Ulid::from_bytes([9; 16]),
            owner,
            start,
            end,
        }
    }

    #[tokio::test]
    async fn allocation_survives_restart() {
        let temp = tempdir().unwrap();
        let context = context(temp.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([60; 32]);
        let actor = actor(realm_id);
        seed_range_config(
            &context,
            &actor,
            range(
                actor.node_id,
                FIRST_GRANTABLE_HANDLE,
                FIRST_GRANTABLE_HANDLE + 1024,
            ),
        )
        .await;

        let first = allocate_handle(&context, realm_id, actor.node_id)
            .await
            .unwrap();
        let second = allocate_handle(&context, realm_id, actor.node_id)
            .await
            .unwrap();
        assert_eq!(first.handle.get(), FIRST_GRANTABLE_HANDLE);
        assert_eq!(second.handle.get(), FIRST_GRANTABLE_HANDLE + 1);
        assert_eq!(first.allocator_range_id, Ulid::from_bytes([9; 16]));

        // "Restart": a fresh allocation reads the persisted cursor and does not
        // re-issue a spent handle.
        let third = allocate_handle(&context, realm_id, actor.node_id)
            .await
            .unwrap();
        assert_eq!(third.handle.get(), FIRST_GRANTABLE_HANDLE + 2);
    }

    #[tokio::test]
    async fn concurrent_allocation_unique() {
        let temp = tempdir().unwrap();
        let context = context(temp.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([64; 32]);
        let actor = actor(realm_id);
        seed_range_config(
            &context,
            &actor,
            range(
                actor.node_id,
                FIRST_GRANTABLE_HANDLE,
                FIRST_GRANTABLE_HANDLE + 1024,
            ),
        )
        .await;

        let (left, right) = tokio::join!(
            allocate_handle(&context, realm_id, actor.node_id),
            allocate_handle(&context, realm_id, actor.node_id),
        );
        let left = left.unwrap();
        let right = right.unwrap();
        assert_ne!(left.handle, right.handle);
        let mut handles = [left.handle.get(), right.handle.get()];
        handles.sort_unstable();
        assert_eq!(
            handles,
            [FIRST_GRANTABLE_HANDLE, FIRST_GRANTABLE_HANDLE + 1]
        );
    }

    #[tokio::test]
    async fn allocation_sets_provenance() {
        let temp = tempdir().unwrap();
        let context = context(temp.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([61; 32]);
        let actor = actor(realm_id);
        let document = seed_range_config(
            &context,
            &actor,
            range(
                actor.node_id,
                FIRST_GRANTABLE_HANDLE,
                FIRST_GRANTABLE_HANDLE + 1024,
            ),
        )
        .await;
        let strategy_id = document.default_strategy_id.unwrap();

        let binding = allocate_placement_binding(
            &context,
            actor.clone(),
            PlacementScope::Realm(realm_id),
            DocumentClass::Metadata,
            strategy_id,
        )
        .await
        .unwrap();

        assert_eq!(binding.allocator_range_id, Some(Ulid::from_bytes([9; 16])));
        assert_eq!(binding.allocated_by, Some(actor.node_id));
        assert!(binding.allocated_at_ms.is_some());
        assert_eq!(binding.handle.get(), FIRST_GRANTABLE_HANDLE);
    }

    #[tokio::test]
    async fn exhaustion_returns_error() {
        let temp = tempdir().unwrap();
        let context = context(temp.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([62; 32]);
        let actor = actor(realm_id);
        // A single-handle range: one allocation succeeds, the next is exhausted.
        seed_range_config(
            &context,
            &actor,
            range(
                actor.node_id,
                FIRST_GRANTABLE_HANDLE,
                FIRST_GRANTABLE_HANDLE + 1,
            ),
        )
        .await;

        assert_eq!(
            allocate_handle(&context, realm_id, actor.node_id)
                .await
                .unwrap()
                .handle
                .get(),
            FIRST_GRANTABLE_HANDLE
        );
        assert!(matches!(
            allocate_handle(&context, realm_id, actor.node_id).await,
            Err(HandleAllocationError::PlacementHandleExhausted { node }) if node == actor.node_id
        ));
    }

    #[tokio::test]
    async fn no_range_exhausted() {
        let temp = tempdir().unwrap();
        let context = context(temp.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([63; 32]);
        let actor = actor(realm_id);
        // A range owned by a different node grants this node nothing.
        seed_range_config(&context, &actor, range(node(2), 1, 1025)).await;

        assert!(matches!(
            allocate_handle(&context, realm_id, actor.node_id).await,
            Err(HandleAllocationError::PlacementHandleExhausted { .. })
        ));
    }
}
