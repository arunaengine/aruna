//! Eviction of an enrolled device.
//!
//! Membership is replicated realm state, so the eviction travels as an ordinary
//! realm-config administrative event: only a management node may originate one.
//! Who may ask is the scope: an owner reaches only the devices the configuration
//! binds to them, a realm admin reaches every enrolled device but never an
//! infrastructure node.

use aruna_core::NodeId;
use aruna_core::admin_document_reducer::{AdminDocumentReducerError, AdminDocumentReducerState};
use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::document::{DocumentSyncOutboxEvent, DocumentSyncTarget};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::ADMIN_DOCUMENT_STATE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::storage_entries::{
    admin_document_conflict_write_entries, admin_document_reducer_state_key,
    admin_document_reducer_state_write_entry, stale_admin_document_conflict_delete_entries,
};
use aruna_core::structs::{Actor, RealmConfigDocument};
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, Key, KeySpace, TxnId, Value};
use aruna_core::util::unix_timestamp_millis;
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;

use crate::document_sync_outbox::{
    new_outbox_record_with_id, outbox_write_entry, schedule_outbox_drain_effect,
};
use crate::ensure_realm_config::overlay_realm_config_reducer_materialization;
use crate::mutate_realm_placement::is_management;
use crate::placement::placement_ref_for_target;

/// Which devices the caller may reach. The admin path is authorized on the
/// realm's onboarding admin path before the operation runs, so here it only
/// widens the ownership test; it never reaches an infrastructure node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeviceEvictionScope {
    Owner,
    RealmAdmin,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RemoveDeviceNodeConfig {
    /// Acting node and the calling user.
    pub actor: Actor,
    pub node_id: NodeId,
    pub scope: DeviceEvictionScope,
}

#[derive(Debug, PartialEq)]
pub struct RemoveDeviceNodeOperation {
    config: RemoveDeviceNodeConfig,
    txn_id: Option<TxnId>,
    state: RemoveDeviceNodeState,
    output: Option<Result<RealmConfigDocument, RemoveDeviceNodeError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum RemoveDeviceNodeState {
    Init,
    StartTransaction,
    ReadCurrent,
    WriteDocumentAndAdminState {
        document: RealmConfigDocument,
        stale_conflict_deletes: Vec<(KeySpace, Key)>,
    },
    DeleteStaleAdminConflicts {
        document: RealmConfigDocument,
    },
    CommitTransaction {
        document: RealmConfigDocument,
    },
    ScheduleDocumentSyncOutboxDrain {
        document: RealmConfigDocument,
    },
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum RemoveDeviceNodeError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    AdminDocumentReducerError(#[from] AdminDocumentReducerError),
    #[error("realm config document missing")]
    RealmConfigNotFound,
    #[error("no device {node_id} belongs to this caller")]
    DeviceNotFound { node_id: NodeId },
    #[error("this node is not a realm management node")]
    NotManagementNode,
    #[error("missing active transaction")]
    MissingTransaction,
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl RemoveDeviceNodeOperation {
    pub fn new(config: RemoveDeviceNodeConfig) -> Self {
        Self {
            config,
            txn_id: None,
            state: RemoveDeviceNodeState::Init,
            output: None,
        }
    }

    fn document_ref(&self) -> DocumentSyncTarget {
        DocumentSyncTarget::RealmConfig {
            realm_id: self.config.actor.realm_id,
        }
    }

    fn admin_target(&self) -> AdminDocumentTarget {
        AdminDocumentTarget::RealmConfig {
            realm_id: self.config.actor.realm_id,
        }
    }

    fn emit_read_current(&mut self, txn_id: TxnId) -> Effects {
        self.txn_id = Some(txn_id);
        self.state = RemoveDeviceNodeState::ReadCurrent;
        let document = self.document_ref();
        let target = self.admin_target();
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (
                    document.storage_keyspace().to_string(),
                    document.storage_key(),
                ),
                (
                    ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
                    admin_document_reducer_state_key(&target),
                ),
            ],
            txn_id: Some(txn_id),
        })]
    }

    fn emit_write_document_and_admin_state(
        &mut self,
        document_value: Option<Value>,
        reducer_state_value: Option<Value>,
    ) -> Result<Effects, RemoveDeviceNodeError> {
        let Some(txn_id) = self.txn_id else {
            return Err(RemoveDeviceNodeError::MissingTransaction);
        };
        let Some(document_value) = document_value else {
            return Err(RemoveDeviceNodeError::RealmConfigNotFound);
        };
        let mut document = RealmConfigDocument::from_bytes(&document_value)?;
        // Peers admit a realm-config event only from a management origin, so a
        // removal published anywhere else would diverge instead of replicate.
        if !is_management(&document, self.config.actor.node_id) {
            return Err(RemoveDeviceNodeError::NotManagementNode);
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
            return Err(RemoveDeviceNodeError::DeviceNotFound {
                node_id: self.config.node_id,
            });
        }

        let target = self.admin_target();
        let previous_reducer_state = reducer_state_value
            .as_ref()
            .map(|value| {
                aruna_core::admin_document_reducer::decode_admin_document_reducer_state(
                    value.as_ref(),
                )
                .map_err(ConversionError::from)
            })
            .transpose()?;
        if previous_reducer_state
            .as_ref()
            .is_some_and(|state| state.target != target)
        {
            return Err(AdminDocumentReducerError::TargetMismatch.into());
        }

        let mut reducer_state = previous_reducer_state
            .clone()
            .unwrap_or_else(|| AdminDocumentReducerState::new(target));
        let admin_event = reducer_state.apply_operation(
            &self.config.actor,
            AdminDocumentOperation::RealmConfigNodeRemoved {
                node_id: self.config.node_id,
            },
        )?;
        // The stored document is derived from the reducer, exactly as the
        // replicated overlay derives it on every other node.
        overlay_realm_config_reducer_materialization(
            &mut document,
            &reducer_state,
            unix_timestamp_millis(),
        );

        let stale_conflict_deletes = stale_admin_document_conflict_delete_entries(
            previous_reducer_state.as_ref(),
            Some(&reducer_state),
        );
        let document_target = self.document_ref();
        let placement = placement_ref_for_target(&document, &document_target, Default::default());
        let mut writes = vec![
            (
                document_target.storage_keyspace().to_string(),
                document_target.storage_key(),
                document.to_bytes(&self.config.actor)?.into(),
            ),
            admin_document_reducer_state_write_entry(&reducer_state)?,
        ];
        let record = new_outbox_record_with_id(
            admin_event.event_id,
            self.config.actor.node_id,
            document_target,
            Vec::new(),
            DocumentSyncOutboxEvent::admin(admin_event),
            placement,
            false,
        );
        writes.push(outbox_write_entry(&record).map_err(ConversionError::from)?);
        writes.extend(admin_document_conflict_write_entries(&reducer_state)?);

        self.output = Some(Ok(document.clone()));
        self.state = RemoveDeviceNodeState::WriteDocumentAndAdminState {
            document,
            stale_conflict_deletes,
        };

        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }

    fn emit_commit_transaction(&mut self, document: RealmConfigDocument) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(RemoveDeviceNodeError::MissingTransaction);
        };
        self.state = RemoveDeviceNodeState::CommitTransaction { document };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn fail(&mut self, error: RemoveDeviceNodeError) -> Effects {
        let cleanup = self.abort();
        self.state = RemoveDeviceNodeState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(RemoveDeviceNodeError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for RemoveDeviceNodeOperation {
    type Output = RealmConfigDocument;
    type Error = RemoveDeviceNodeError;

    fn start(&mut self) -> Effects {
        self.state = RemoveDeviceNodeState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state.clone() {
            RemoveDeviceNodeState::StartTransaction => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.emit_read_current(txn_id)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("transaction start result", format!("{other:?}")),
            },
            RemoveDeviceNodeState::ReadCurrent => match event {
                Event::Storage(StorageEvent::BatchReadResult { values }) => {
                    let [(_, document_value), (_, reducer_state_value)] = values.as_slice() else {
                        return self.unexpected_event(
                            "storage batch read result with realm config and reducer state",
                            format!("{values:?}"),
                        );
                    };
                    match self.emit_write_document_and_admin_state(
                        document_value.clone(),
                        reducer_state_value.clone(),
                    ) {
                        Ok(effects) => effects,
                        Err(error) => self.fail(error),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch read result", format!("{other:?}")),
            },
            RemoveDeviceNodeState::WriteDocumentAndAdminState {
                document,
                stale_conflict_deletes,
            } => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(RemoveDeviceNodeError::MissingTransaction);
                    };
                    if !stale_conflict_deletes.is_empty() {
                        self.state = RemoveDeviceNodeState::DeleteStaleAdminConflicts { document };
                        return smallvec![Effect::Storage(StorageEffect::BatchDelete {
                            deletes: stale_conflict_deletes,
                            txn_id: Some(txn_id),
                        })];
                    }
                    self.emit_commit_transaction(document)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch write result", format!("{other:?}")),
            },
            RemoveDeviceNodeState::DeleteStaleAdminConflicts { document } => match event {
                Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {
                    self.emit_commit_transaction(document)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch delete result", format!("{other:?}")),
            },
            RemoveDeviceNodeState::CommitTransaction { document } => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.txn_id = None;
                    self.state =
                        RemoveDeviceNodeState::ScheduleDocumentSyncOutboxDrain { document };
                    smallvec![schedule_outbox_drain_effect()]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.txn_id = None;
                    self.fail(error.into())
                }
                other => self.unexpected_event("transaction commit result", format!("{other:?}")),
            },
            RemoveDeviceNodeState::ScheduleDocumentSyncOutboxDrain { .. } => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.state = RemoveDeviceNodeState::Finish;
                    smallvec![]
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(error = %message, "Failed to schedule device removal outbox drain; durable outbox remains retryable");
                    self.state = RemoveDeviceNodeState::Finish;
                    smallvec![]
                }
                other => self.unexpected_event(
                    "document sync outbox drain timer schedule",
                    format!("{other:?}"),
                ),
            },
            RemoveDeviceNodeState::Finish
            | RemoveDeviceNodeState::Error
            | RemoveDeviceNodeState::Init => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            RemoveDeviceNodeState::Finish | RemoveDeviceNodeState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .unwrap_or(Err(RemoveDeviceNodeError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::get_realm_config::GetRealmConfigOperation;
    use aruna_core::document::DocumentSyncTarget;
    use aruna_core::events::StorageEvent;
    use aruna_core::structs::{RealmId, RealmNodeKind};
    use aruna_core::types::UserId;
    use tempfile::tempdir;
    use ulid::Ulid;

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
        let target = DocumentSyncTarget::RealmConfig {
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

    fn removal(actor: &Actor, scope: DeviceEvictionScope) -> RemoveDeviceNodeConfig {
        RemoveDeviceNodeConfig {
            actor: actor.clone(),
            node_id: node(2),
            scope,
        }
    }

    #[tokio::test]
    async fn evicts_owned_device() {
        // The device leaves the stored membership, so it stops being an
        // admitted peer once the configuration is refreshed.
        let dir = tempdir().unwrap();
        let ctx = context(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let actor = actor(realm_id);
        seed(
            &ctx,
            &actor,
            &realm_with_device(realm_id, &actor, actor.user_id),
        )
        .await;

        let stored = drive(
            RemoveDeviceNodeOperation::new(removal(&actor, DeviceEvictionScope::Owner)),
            &ctx,
        )
        .await
        .expect("device removal applies");
        let device = node(2).to_string();
        assert!(stored.nodes.iter().all(|node| node.node_id != device));

        let reread = drive(GetRealmConfigOperation::new(realm_id), &ctx)
            .await
            .expect("config reads");
        assert_eq!(reread.nodes.len(), 1);
    }

    #[tokio::test]
    async fn refuses_foreign_device() {
        // A device owned by somebody else must look absent, not evictable.
        let dir = tempdir().unwrap();
        let ctx = context(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let actor = actor(realm_id);
        let other = UserId::local(Ulid::from_bytes([9u8; 16]), realm_id);
        seed(&ctx, &actor, &realm_with_device(realm_id, &actor, other)).await;

        let error = drive(
            RemoveDeviceNodeOperation::new(removal(&actor, DeviceEvictionScope::Owner)),
            &ctx,
        )
        .await
        .expect_err("a foreign device is refused");
        assert!(matches!(
            error,
            RemoveDeviceNodeError::DeviceNotFound { .. }
        ));

        let reread = drive(GetRealmConfigOperation::new(realm_id), &ctx)
            .await
            .expect("config reads");
        assert_eq!(reread.nodes.len(), 2);
    }

    #[tokio::test]
    async fn admin_evicts_foreign() {
        // A realm admin reaches a device it does not own, and the owner path is
        // still refused for the same device.
        let dir = tempdir().unwrap();
        let ctx = context(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([4u8; 32]);
        let actor = actor(realm_id);
        let other = UserId::local(Ulid::from_bytes([9u8; 16]), realm_id);
        seed(&ctx, &actor, &realm_with_device(realm_id, &actor, other)).await;

        assert!(matches!(
            drive(
                RemoveDeviceNodeOperation::new(removal(&actor, DeviceEvictionScope::Owner)),
                &ctx,
            )
            .await,
            Err(RemoveDeviceNodeError::DeviceNotFound { .. })
        ));

        let stored = drive(
            RemoveDeviceNodeOperation::new(removal(&actor, DeviceEvictionScope::RealmAdmin)),
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
        let dir = tempdir().unwrap();
        let ctx = context(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([5u8; 32]);
        let actor = actor(realm_id);
        let mut document = realm_with_device(realm_id, &actor, actor.user_id);
        document.nodes.clear();
        document.ensure_node(actor.node_id, RealmNodeKind::Management);
        document.ensure_node(node(2), RealmNodeKind::Server);
        seed(&ctx, &actor, &document).await;

        let error = drive(
            RemoveDeviceNodeOperation::new(removal(&actor, DeviceEvictionScope::RealmAdmin)),
            &ctx,
        )
        .await
        .expect_err("a server node is not a device");
        assert!(matches!(
            error,
            RemoveDeviceNodeError::DeviceNotFound { .. }
        ));

        let reread = drive(GetRealmConfigOperation::new(realm_id), &ctx)
            .await
            .expect("config reads");
        assert_eq!(reread.nodes.len(), 2);
    }

    #[tokio::test]
    async fn refuses_server_node() {
        // Peers reject a realm-config event whose origin is not management, so
        // the eviction must not be published from a server node either.
        let dir = tempdir().unwrap();
        let ctx = context(dir.path().to_str().unwrap());
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
            RemoveDeviceNodeOperation::new(removal(&actor, DeviceEvictionScope::Owner)),
            &ctx,
        )
        .await
        .expect_err("a server node is refused");
        assert_eq!(error, RemoveDeviceNodeError::NotManagementNode);
    }
}
