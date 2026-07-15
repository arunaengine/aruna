//! Node-local placement-handle allocation (spec 6.3.4, layer 1).
//!
//! A node mints handles OFFLINE from the disjoint ranges a coordinator granted
//! it (DEC-ONBOARD). The next-unused position is a durable, node-local cursor —
//! never replicated: peers rely on non-overlapping grants, not on knowing this
//! node's progress. The cursor is persisted BEFORE an allocation is acknowledged,
//! so a crash can only ever skip a handle, never re-issue a spent one.
//!
//! This is the production handle source. The caller-supplied
//! `AppendPlacementBinding` mutation stays for internal/test use; production
//! bindings flow through [`allocate_placement_binding`], which stamps the three
//! provenance fields mandatorily. Wiring allocation into document creation is
//! layer 2 and deliberately not done here.

use std::time::{SystemTime, UNIX_EPOCH};

use aruna_core::NodeId;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::StorageEffect;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::NODE_STATE_KEYSPACE;
use aruna_core::structs::{
    Actor, DocumentClass, HandleAllocationCursor, PlacementBinding, PlacementScope,
    RealmConfigDocument, RealmId,
};
use aruna_core::structured_id::PlacementHandle;
use aruna_core::types::Key;
use thiserror::Error;
use ulid::Ulid;

use crate::driver::DriverContext;
use crate::mutate_realm_placement::{
    MutateRealmPlacementConfig, MutateRealmPlacementError, MutateRealmPlacementOperation,
    RealmPlacementMutation,
};

/// Node-local key for the durable allocation cursor. Kept in the non-replicated
/// node-state keyspace, one record per realm, distinct from the singleton
/// `node_state` record.
fn handle_allocation_cursor_key(realm_id: &RealmId) -> Key {
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

#[derive(Debug, Error)]
pub enum HandleAllocationError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("realm config document missing")]
    RealmConfigNotFound,
    #[error("unexpected storage event: {0}")]
    UnexpectedStorageEvent(String),
    #[error("placement_handle_exhausted: node {node} has spent every handle in its granted ranges")]
    PlacementHandleExhausted { node: NodeId },
    #[error(transparent)]
    Append(#[from] MutateRealmPlacementError),
}

async fn read_realm_config(
    context: &DriverContext,
    realm_id: RealmId,
) -> Result<RealmConfigDocument, HandleAllocationError> {
    let target = DocumentSyncTarget::RealmConfig { realm_id };
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key(),
            txn_id: None,
        })
        .await;
    match event {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => Ok(RealmConfigDocument::from_bytes(&bytes)?),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
            Err(HandleAllocationError::RealmConfigNotFound)
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(HandleAllocationError::UnexpectedStorageEvent(format!(
            "{other:?}"
        ))),
    }
}

async fn read_cursor(
    context: &DriverContext,
    realm_id: RealmId,
) -> Result<HandleAllocationCursor, HandleAllocationError> {
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: NODE_STATE_KEYSPACE.to_string(),
            key: handle_allocation_cursor_key(&realm_id),
            txn_id: None,
        })
        .await;
    match event {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => Ok(postcard::from_bytes(&bytes).map_err(ConversionError::from)?),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
            Ok(HandleAllocationCursor::new())
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(HandleAllocationError::UnexpectedStorageEvent(format!(
            "{other:?}"
        ))),
    }
}

async fn persist_cursor(
    context: &DriverContext,
    realm_id: RealmId,
    cursor: &HandleAllocationCursor,
) -> Result<(), HandleAllocationError> {
    let value = postcard::to_allocvec(cursor).map_err(ConversionError::from)?;
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: NODE_STATE_KEYSPACE.to_string(),
            key: handle_allocation_cursor_key(&realm_id),
            value: value.into(),
            txn_id: None,
        })
        .await;
    match event {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(HandleAllocationError::UnexpectedStorageEvent(format!(
            "{other:?}"
        ))),
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
    let config = read_realm_config(context, realm_id).await?;
    let ranges = config.handle_range_directory().granted_to(&node_id);
    let mut cursor = read_cursor(context, realm_id).await?;
    let (handle, allocator_range_id) = cursor
        .allocate(&ranges)
        .ok_or(HandleAllocationError::PlacementHandleExhausted { node: node_id })?;
    // Persist the advanced cursor before acknowledging: a crash after this point
    // wastes the drawn handle at worst, it is never re-issued.
    persist_cursor(context, realm_id, &cursor).await?;
    Ok(AllocatedHandle {
        handle,
        allocator_range_id,
        allocated_by: node_id,
        allocated_at_ms: now_ms(),
    })
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
    use aruna_core::events::Event;
    use aruna_core::structs::{HandleRange, RealmNodeKind};
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
        }
    }

    async fn seed_config_with_range(
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
    async fn allocation_advances_and_survives_restart() {
        let temp = tempdir().unwrap();
        let context = context(temp.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([60; 32]);
        let actor = actor(realm_id);
        seed_config_with_range(&context, &actor, range(actor.node_id, 1, 1025)).await;

        let first = allocate_handle(&context, realm_id, actor.node_id)
            .await
            .unwrap();
        let second = allocate_handle(&context, realm_id, actor.node_id)
            .await
            .unwrap();
        assert_eq!(first.handle.get(), 1);
        assert_eq!(second.handle.get(), 2);
        assert_eq!(first.allocator_range_id, Ulid::from_bytes([9; 16]));

        // "Restart": a fresh allocation reads the persisted cursor and does not
        // re-issue a spent handle.
        let third = allocate_handle(&context, realm_id, actor.node_id)
            .await
            .unwrap();
        assert_eq!(third.handle.get(), 3);
    }

    #[tokio::test]
    async fn allocation_sets_all_provenance_fields() {
        let temp = tempdir().unwrap();
        let context = context(temp.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([61; 32]);
        let actor = actor(realm_id);
        let document =
            seed_config_with_range(&context, &actor, range(actor.node_id, 1, 1025)).await;
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
        assert_eq!(binding.handle.get(), 1);
    }

    #[tokio::test]
    async fn exhaustion_returns_placement_handle_exhausted() {
        let temp = tempdir().unwrap();
        let context = context(temp.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([62; 32]);
        let actor = actor(realm_id);
        // A single-handle range: one allocation succeeds, the next is exhausted.
        seed_config_with_range(&context, &actor, range(actor.node_id, 1, 2)).await;

        assert_eq!(
            allocate_handle(&context, realm_id, actor.node_id)
                .await
                .unwrap()
                .handle
                .get(),
            1
        );
        assert!(matches!(
            allocate_handle(&context, realm_id, actor.node_id).await,
            Err(HandleAllocationError::PlacementHandleExhausted { node }) if node == actor.node_id
        ));
    }

    #[tokio::test]
    async fn no_granted_range_is_exhausted() {
        let temp = tempdir().unwrap();
        let context = context(temp.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([63; 32]);
        let actor = actor(realm_id);
        // A range owned by a different node grants this node nothing.
        seed_config_with_range(&context, &actor, range(node(2), 1, 1025)).await;

        assert!(matches!(
            allocate_handle(&context, realm_id, actor.node_id).await,
            Err(HandleAllocationError::PlacementHandleExhausted { .. })
        ));
    }
}
