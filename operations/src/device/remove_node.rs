//! Evicts an enrolled device by writing a realm config event on a management node.
//! An owner reaches its own devices, a realm admin every device, never an infrastructure node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{DISTRIBUTION_PUBLISHER_KEYSPACE, DOCUMENT_STATE_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState};
use aruna_core::rows::{done, values};
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, reducer_state_key, stale_conflict_deletes,
};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::group_delete::MembershipFence;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{Key, KeySpace, Value};
use thiserror::Error;

use crate::realm::ensure_config::overlay_reducer_state;
use crate::realm::mutate_placement::is_management;
use crate::sync::outbox_record::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};
use aruna_core::placement::target_placement_ref;

/// Which devices the caller may reach. The admin path is authorized on the
/// realm's onboarding admin path before the operation runs, so here it only
/// widens the ownership test; it never reaches an infrastructure node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeviceEvictionScope {
    Owner,
    RealmAdmin,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RemoveNodeConfig {
    /// Acting node and the calling user.
    pub actor: Actor,
    pub node_id: NodeId,
    pub scope: DeviceEvictionScope,
}

/// Removes the device from the realm config in one transaction; the outbox drain is woken after
/// the commit, and the durable outbox row stays retryable if that wake fails.
#[derive(Debug, PartialEq)]
pub struct RemoveNodeOperation {
    config: RemoveNodeConfig,
    state: RemoveNodeState,
    document: Option<RealmConfigDocument>,
}

#[derive(Debug, Clone, PartialEq)]
enum RemoveNodeState {
    ReadCurrent,
    /// Checks that the node publishes no distributed bucket.
    ReadCutoffs {
        document_value: Option<Value>,
        reducer_state_value: Option<Value>,
    },
    WriteDocumentState {
        stale_conflict_deletes: Vec<(KeySpace, Key)>,
    },
    DeleteAdminConflicts,
}

#[derive(Debug, Error, PartialEq)]
pub enum RemoveNodeError {
    #[error("realm membership is frozen while group deletion is pending")]
    DeletionPending,
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    AdminDocumentError(#[from] AdminDocumentError),
    #[error("realm config document missing")]
    ConfigMissing,
    #[error(
        "the node publishes distributed buckets; moving that role needs the later failover procedure"
    )]
    PublisherRemoval,
    #[error("no device {node_id} belongs to this caller")]
    DeviceNotFound { node_id: NodeId },
    #[error("this node is not a realm management node")]
    NotManagementNode,
    #[error("operation did not finish")]
    NotFinished,
}

impl From<IoError> for RemoveNodeError {
    fn from(error: IoError) -> Self {
        Self::StorageError(error.into())
    }
}

impl RemoveNodeOperation {
    pub fn new(config: RemoveNodeConfig) -> Self {
        Self {
            config,
            state: RemoveNodeState::ReadCurrent,
            document: None,
        }
    }

    fn document_ref(&self) -> DocumentTarget {
        DocumentTarget::RealmConfig {
            realm_id: self.config.actor.realm_id,
        }
    }

    fn admin_target(&self) -> AdminDocumentTarget {
        AdminDocumentTarget::RealmConfig {
            realm_id: self.config.actor.realm_id,
        }
    }

    fn read_current(&self) -> IoCall {
        let document = self.document_ref();
        let target = self.admin_target();
        IoCall::Read(vec![
            (
                document.storage_keyspace().to_string(),
                document.storage_key(),
            ),
            (
                DOCUMENT_STATE_KEYSPACE.to_string(),
                reducer_state_key(&target),
            ),
            (
                aruna_core::keyspaces::GROUP_DELETE_KEYSPACE.to_string(),
                MembershipFence::key(self.config.actor.realm_id),
            ),
        ])
    }

    fn read_cutoffs(&mut self, result: IoResult) -> Result<IoCall, RemoveNodeError> {
        let [document_value, reducer_state_value, membership] = values::<3>(result)?;
        let membership = MembershipFence::from_value(membership.as_deref())?;
        if !membership.pending.is_empty() {
            return Err(RemoveNodeError::DeletionPending);
        }
        self.state = RemoveNodeState::ReadCutoffs {
            document_value,
            reducer_state_value,
        };
        Ok(IoCall::Page {
            space: DISTRIBUTION_PUBLISHER_KEYSPACE.to_string(),
            prefix: self.config.node_id.as_bytes().to_vec().into(),
            after: None,
            limit: 1,
        })
    }

    fn removal_writes(
        &mut self,
        document_value: Option<Value>,
        reducer_state_value: Option<Value>,
    ) -> Result<IoCall, RemoveNodeError> {
        let Some(document_value) = document_value else {
            return Err(RemoveNodeError::ConfigMissing);
        };
        let mut document = RealmConfigDocument::from_bytes(&document_value)?;
        // Peers admit a realm-config event only from a management origin, so a
        // removal published anywhere else would diverge instead of replicate.
        if !is_management(&document, self.config.actor.node_id) {
            return Err(RemoveNodeError::NotManagementNode);
        }
        // Ownership is realm state: an infrastructure node is never a device,
        // and another user's device is not an owner's to evict.
        let node_id = self.config.node_id.to_string();
        if !document.nodes.iter().any(|node| {
            node.node_id == node_id
                && match self.config.scope {
                    DeviceEvictionScope::Owner => {
                        node.kind.owner() == Some(self.config.actor.user_id)
                    }
                    DeviceEvictionScope::RealmAdmin => node.kind.owner().is_some(),
                }
        }) {
            return Err(RemoveNodeError::DeviceNotFound {
                node_id: self.config.node_id,
            });
        }

        let target = self.admin_target();
        let previous_reducer_state = reducer_state_value
            .as_ref()
            .map(|value| {
                aruna_core::reducer::decode_reducer_state(value.as_ref())
                    .map_err(ConversionError::from)
            })
            .transpose()?;
        if previous_reducer_state
            .as_ref()
            .is_some_and(|state| state.target != target)
        {
            return Err(AdminDocumentError::TargetMismatch.into());
        }

        let mut reducer_state = previous_reducer_state
            .clone()
            .unwrap_or_else(|| AdminDocumentState::new(target));
        let admin_event = reducer_state.apply_operation(
            &self.config.actor,
            AdminDocumentOperation::ConfigNodeRemoved {
                node_id: self.config.node_id,
                // A publisher is never removed here, so no set needs a cutoff yet.
                cutoffs: Vec::new(),
            },
        )?;
        // The stored document is derived from the reducer, exactly as the
        // replicated overlay derives it on every other node.
        overlay_reducer_state(&mut document, &reducer_state, unix_timestamp_millis());

        let stale_conflict_deletes =
            stale_conflict_deletes(previous_reducer_state.as_ref(), Some(&reducer_state));
        let document_target = self.document_ref();
        let placement = target_placement_ref(&document, &document_target, Default::default());
        let mut writes = vec![
            (
                document_target.storage_keyspace().to_string(),
                document_target.storage_key(),
                document.to_bytes(&self.config.actor)?.into(),
            ),
            reducer_state_entry(&reducer_state)?,
        ];
        let record = new_identified_record(
            admin_event.event_id,
            self.config.actor.node_id,
            document_target,
            Vec::new(),
            DocumentOutboxEvent::admin(admin_event),
            placement,
            false,
        );
        writes.push(outbox_write_entry(&record).map_err(ConversionError::from)?);
        writes.extend(conflict_write_entries(&reducer_state)?);

        self.document = Some(document);
        self.state = RemoveNodeState::WriteDocumentState {
            stale_conflict_deletes,
        };
        Ok(IoCall::Write(writes))
    }

    /// Commits the removal, then wakes the outbox drain.
    fn finish(&mut self) -> Result<Next<RealmConfigDocument>, RemoveNodeError> {
        let output = self.document.take().ok_or(RemoveNodeError::NotFinished)?;
        Ok(Next::Then {
            output,
            calls: vec![IoCall::Effect(Box::new(schedule_drain_effect()))],
        })
    }
}

impl Operation for RemoveNodeOperation {
    type Output = RealmConfigDocument;
    type Error = RemoveNodeError;

    fn first(&mut self) -> Result<Next<RealmConfigDocument>, RemoveNodeError> {
        Ok(Next::Call(self.read_current()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<RealmConfigDocument>, RemoveNodeError> {
        match std::mem::replace(&mut self.state, RemoveNodeState::DeleteAdminConflicts) {
            RemoveNodeState::ReadCurrent => Ok(Next::Call(self.read_cutoffs(result)?)),
            RemoveNodeState::ReadCutoffs {
                document_value,
                reducer_state_value,
            } => {
                let (rows, _) = result.page()?;
                // A cached head is no final cutoff, so a publisher stays until failover.
                if !rows.is_empty() {
                    return Err(RemoveNodeError::PublisherRemoval);
                }
                let writes = self.removal_writes(document_value, reducer_state_value)?;
                Ok(Next::Call(writes))
            }
            RemoveNodeState::WriteDocumentState {
                stale_conflict_deletes,
            } => {
                done(result)?;
                Ok(Next::Call(IoCall::Delete(stale_conflict_deletes)))
            }
            RemoveNodeState::DeleteAdminConflicts => {
                done(result)?;
                self.finish()
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document_repository::GetConfigOperation;
    use crate::driver::{DriverContext, drive};
    use aruna_core::UserId;
    use aruna_core::document::DocumentTarget;
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::structs::identity::realm::{RealmId, RealmNodeKind};
    use ulid::Ulid;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn actor(realm_id: RealmId) -> Actor {
        Actor {
            node_id: node(1),
            user_id: UserId::local(Ulid::from_bytes([1u8; 16]), realm_id),
            realm_id,
        }
    }

    async fn seed(ctx: &DriverContext, actor: &Actor, document: &RealmConfigDocument) {
        let target = DocumentTarget::RealmConfig {
            realm_id: actor.realm_id,
        };
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: target.storage_keyspace().to_string(),
                key: target.storage_key(),
                value: document.to_bytes(actor).unwrap().into(),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected event: {other:?}"),
        }
    }

    fn realm_with_device(realm_id: RealmId, actor: &Actor, owner: UserId) -> RealmConfigDocument {
        let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        document.ensure_node(actor.node_id, RealmNodeKind::Management);
        document.ensure_node(node(2), RealmNodeKind::User { owner });
        document
    }

    fn removal(actor: &Actor, scope: DeviceEvictionScope) -> RemoveNodeConfig {
        RemoveNodeConfig {
            actor: actor.clone(),
            node_id: node(2),
            scope,
        }
    }

    fn at_removal(document: &RealmConfigDocument, actor: &Actor) -> RemoveNodeOperation {
        let mut operation = RemoveNodeOperation::new(removal(actor, DeviceEvictionScope::Owner));
        assert_eq!(operation.transaction(), TransactionMode::Write);
        assert_eq!(operation.first(), Ok(Next::Call(operation.read_current())));
        let stored = Some(document.to_bytes(actor).unwrap().into());
        let current = IoResult::Values(vec![stored, None, None]);
        assert!(matches!(
            operation.next(current),
            Ok(Next::Call(IoCall::Page { limit: 1, .. }))
        ));
        operation
    }

    fn no_publisher() -> IoResult {
        IoResult::Page {
            rows: Vec::new(),
            next: None,
        }
    }

    // The removal writes its rows in the transaction and wakes the drain after the commit.
    #[test]
    fn removal_wakes_drain() {
        let realm_id = RealmId::from_bytes([6u8; 32]);
        let actor = actor(realm_id);
        let document = realm_with_device(realm_id, &actor, actor.user_id);
        let mut operation = at_removal(&document, &actor);
        assert!(matches!(
            operation.next(no_publisher()),
            Ok(Next::Call(IoCall::Write(_)))
        ));
        assert_eq!(
            operation.next(IoResult::Done),
            Ok(Next::Call(IoCall::Delete(Vec::new())))
        );
        let Ok(Next::Then { output, calls }) = operation.next(IoResult::Done) else {
            panic!("expected the document and the drain wake");
        };
        assert_eq!(output.nodes.len(), 1);
        let [IoCall::Effect(wake)] = calls.as_slice() else {
            panic!("expected one wake, got {calls:?}");
        };
        assert!(matches!(**wake, Effect::Task(_)));
    }

    // A pending group deletion, a publisher and a missing config all refuse the removal.
    #[test]
    fn refusals_stop_removal() {
        let realm_id = RealmId::from_bytes([6u8; 32]);
        let actor = actor(realm_id);
        let document = realm_with_device(realm_id, &actor, actor.user_id);
        let mut operation = at_removal(&document, &actor);
        let row = (Key::from(vec![1]), Value::from(vec![2]));
        let publisher = IoResult::Page {
            rows: vec![row],
            next: None,
        };
        assert_eq!(
            operation.next(publisher),
            Err(RemoveNodeError::PublisherRemoval)
        );

        let mut operation = RemoveNodeOperation::new(removal(&actor, DeviceEvictionScope::Owner));
        operation.first().unwrap();
        operation
            .next(IoResult::Values(vec![None, None, None]))
            .unwrap();
        assert_eq!(
            operation.next(no_publisher()),
            Err(RemoveNodeError::ConfigMissing)
        );
    }

    #[tokio::test]
    async fn evicts_owned_device() {
        // The device leaves the stored membership, so it stops being an
        // admitted peer once the configuration is refreshed.
        let (_dir, ctx) = crate::tests::device::context().await;
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let actor = actor(realm_id);
        seed(
            &ctx,
            &actor,
            &realm_with_device(realm_id, &actor, actor.user_id),
        )
        .await;

        let stored = drive(
            RemoveNodeOperation::new(removal(&actor, DeviceEvictionScope::Owner)),
            &ctx,
        )
        .await
        .expect("device removal applies");
        let device = node(2).to_string();
        assert!(stored.nodes.iter().all(|node| node.node_id != device));

        let reread = drive(GetConfigOperation::new(realm_id), &ctx)
            .await
            .expect("config reads");
        assert_eq!(reread.nodes.len(), 1);
    }

    #[tokio::test]
    async fn refuses_foreign_device() {
        // A device owned by somebody else must look absent, not evictable.
        let (_dir, ctx) = crate::tests::device::context().await;
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let actor = actor(realm_id);
        let other = UserId::local(Ulid::from_bytes([9u8; 16]), realm_id);
        seed(&ctx, &actor, &realm_with_device(realm_id, &actor, other)).await;

        let error = drive(
            RemoveNodeOperation::new(removal(&actor, DeviceEvictionScope::Owner)),
            &ctx,
        )
        .await
        .expect_err("a foreign device is refused");
        assert!(matches!(error, RemoveNodeError::DeviceNotFound { .. }));

        let reread = drive(GetConfigOperation::new(realm_id), &ctx)
            .await
            .expect("config reads");
        assert_eq!(reread.nodes.len(), 2);
    }

    #[tokio::test]
    async fn admin_evicts_foreign() {
        // A realm admin reaches a device it does not own, and the owner path is
        // still refused for the same device.
        let (_dir, ctx) = crate::tests::device::context().await;
        let realm_id = RealmId::from_bytes([4u8; 32]);
        let actor = actor(realm_id);
        let other = UserId::local(Ulid::from_bytes([9u8; 16]), realm_id);
        seed(&ctx, &actor, &realm_with_device(realm_id, &actor, other)).await;

        assert!(matches!(
            drive(
                RemoveNodeOperation::new(removal(&actor, DeviceEvictionScope::Owner)),
                &ctx,
            )
            .await,
            Err(RemoveNodeError::DeviceNotFound { .. })
        ));

        let stored = drive(
            RemoveNodeOperation::new(removal(&actor, DeviceEvictionScope::RealmAdmin)),
            &ctx,
        )
        .await
        .expect("an admin evicts any device");
        let device = node(2).to_string();
        assert!(stored.nodes.iter().all(|node| node.node_id != device));
    }

    #[tokio::test]
    async fn admin_spares_infrastructure() {
        // The admin scope widens ownership, not membership: a server node is not
        // a device and stays in the configuration.
        let (_dir, ctx) = crate::tests::device::context().await;
        let realm_id = RealmId::from_bytes([5u8; 32]);
        let actor = actor(realm_id);
        let mut document = realm_with_device(realm_id, &actor, actor.user_id);
        document.nodes.clear();
        document.ensure_node(actor.node_id, RealmNodeKind::Management);
        document.ensure_node(node(2), RealmNodeKind::Server);
        seed(&ctx, &actor, &document).await;

        let error = drive(
            RemoveNodeOperation::new(removal(&actor, DeviceEvictionScope::RealmAdmin)),
            &ctx,
        )
        .await
        .expect_err("a server node is not a device");
        assert!(matches!(error, RemoveNodeError::DeviceNotFound { .. }));

        let reread = drive(GetConfigOperation::new(realm_id), &ctx)
            .await
            .expect("config reads");
        assert_eq!(reread.nodes.len(), 2);
    }

    #[tokio::test]
    async fn refuses_server_node() {
        // Peers reject a realm-config event whose origin is not management, so
        // the eviction must not be published from a server node either.
        let (_dir, ctx) = crate::tests::device::context().await;
        let realm_id = RealmId::from_bytes([3u8; 32]);
        let actor = actor(realm_id);
        let mut document = realm_with_device(realm_id, &actor, actor.user_id);
        document.nodes.clear();
        document.ensure_node(actor.node_id, RealmNodeKind::Server);
        document.ensure_node(
            node(2),
            RealmNodeKind::User {
                owner: actor.user_id,
            },
        );
        seed(&ctx, &actor, &document).await;

        let error = drive(
            RemoveNodeOperation::new(removal(&actor, DeviceEvictionScope::Owner)),
            &ctx,
        )
        .await
        .expect_err("a server node is refused");
        assert_eq!(error, RemoveNodeError::NotManagementNode);
    }

    #[tokio::test]
    async fn removal_refuses_publisher() {
        // A set's publisher is never removed with a possibly stale cutoff.
        use aruna_core::structs::storage::distribution::{DistributionRevision, publisher_key};
        let (_dir, ctx) = crate::tests::device::context().await;
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let actor = actor(realm_id);
        seed(
            &ctx,
            &actor,
            &realm_with_device(realm_id, &actor, actor.user_id),
        )
        .await;
        let revision = DistributionRevision {
            number: 4,
            digest: [5u8; 32],
        };
        ctx.storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: DISTRIBUTION_PUBLISHER_KEYSPACE.to_string(),
                key: publisher_key(&node(2), Ulid::from_bytes([7u8; 16])).into(),
                value: postcard::to_allocvec(&revision).unwrap().into(),
                txn_id: None,
            })
            .await;
        let error = drive(
            RemoveNodeOperation::new(removal(&actor, DeviceEvictionScope::Owner)),
            &ctx,
        )
        .await
        .unwrap_err();
        assert_eq!(error, RemoveNodeError::PublisherRemoval);
        let reread = drive(GetConfigOperation::new(realm_id), &ctx)
            .await
            .unwrap();
        assert!(
            reread
                .nodes
                .iter()
                .any(|entry| entry.node_id == node(2).to_string())
        );
    }
}
