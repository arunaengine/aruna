//! Replaces the realm default on/off state of optional portal features.
//! It writes through the shared admin document path, so concurrent changes converge.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState, CONFIG_PORTAL_PATH};
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, reducer_state_key, stale_conflict_deletes,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::{RealmConfigDocument, validate_portal_features};
use aruna_core::structs::placement::policy::document::policy_admin_path;
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, Key, KeySpace, TxnId, Value};
use smallvec::smallvec;
use std::collections::BTreeMap;
use thiserror::Error;
use tracing::warn;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::placement::target_placement_ref;
use crate::realm::mutate_placement::is_management;
use crate::sync::document_outbox::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};

#[derive(Debug, Clone, PartialEq)]
pub struct SetPortalConfig {
    pub actor: Actor,
    /// The caller's own token context, so a path-restricted credential stays
    /// restricted; it is never derived from `actor`.
    pub auth_context: AuthContext,
    /// The complete feature map; it replaces the stored one.
    pub features: BTreeMap<String, bool>,
}

#[derive(Debug, PartialEq)]
pub struct SetPortalOperation {
    config: SetPortalConfig,
    txn_id: Option<TxnId>,
    state: SetPortalState,
    output: Option<Result<RealmConfigDocument, SetPortalError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum SetPortalState {
    Init,
    Auth,
    StartTransaction,
    ReadCurrent,
    WriteDocumentState {
        document: RealmConfigDocument,
        stale_conflict_deletes: Vec<(KeySpace, Key)>,
    },
    DeleteAdminConflicts {
        document: RealmConfigDocument,
    },
    CommitTransaction {
        document: RealmConfigDocument,
    },
    ScheduleSyncDrain {
        document: RealmConfigDocument,
    },
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum SetPortalError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    AdminDocumentError(#[from] AdminDocumentError),
    #[error("realm config document missing")]
    ConfigMissing,
    #[error("caller may not write the realm configuration")]
    Unauthorized,
    #[error("this node is not a realm management node")]
    NotManagementNode,
    #[error("invalid portal features: {reason}")]
    InvalidFeatures { reason: String },
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

impl SetPortalOperation {
    pub fn new(config: SetPortalConfig) -> Self {
        Self {
            config,
            txn_id: None,
            state: SetPortalState::Init,
            output: None,
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

    fn emit_read_current(&mut self, txn_id: TxnId) -> Effects {
        self.txn_id = Some(txn_id);
        self.state = SetPortalState::ReadCurrent;
        let document = self.document_ref();
        let target = self.admin_target();
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (
                    document.storage_keyspace().to_string(),
                    document.storage_key(),
                ),
                (
                    DOCUMENT_STATE_KEYSPACE.to_string(),
                    reducer_state_key(&target),
                ),
            ],
            txn_id: Some(txn_id),
        })]
    }

    fn emit_document_write(
        &mut self,
        document_value: Option<Value>,
        reducer_state_value: Option<Value>,
    ) -> Result<Effects, SetPortalError> {
        let Some(txn_id) = self.txn_id else {
            return Err(SetPortalError::MissingTransaction);
        };
        validate_portal_features(&self.config.features).map_err(|error| {
            SetPortalError::InvalidFeatures {
                reason: error.to_string(),
            }
        })?;

        let Some(document_value) = document_value else {
            return Err(SetPortalError::ConfigMissing);
        };
        let mut document = RealmConfigDocument::from_bytes(&document_value)?;
        if !is_management(&document, self.config.actor.node_id) {
            return Err(SetPortalError::NotManagementNode);
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
            AdminDocumentOperation::ConfigPortalSet {
                features: self.config.features.clone(),
            },
        )?;
        // Materialized reducer state keeps local and replicated conflict outcomes equal.
        apply_reducer_portal(&mut document, &reducer_state);

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

        self.output = Some(Ok(document.clone()));
        self.state = SetPortalState::WriteDocumentState {
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
            return self.fail(SetPortalError::MissingTransaction);
        };
        self.state = SetPortalState::CommitTransaction { document };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn fail(&mut self, error: SetPortalError) -> Effects {
        let cleanup = self.abort();
        self.state = SetPortalState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(SetPortalError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for SetPortalOperation {
    type Output = RealmConfigDocument;
    type Error = SetPortalError;

    fn start(&mut self) -> Effects {
        if self.config.auth_context.realm_id != self.config.actor.realm_id {
            return self.fail(SetPortalError::Unauthorized);
        }
        self.state = SetPortalState::Auth;
        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: self.config.auth_context.clone(),
                path: policy_admin_path(self.config.actor.realm_id),
                required_permission: Permission::WRITE,
            }),
            |allowed| Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }),
        ))]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state.clone() {
            SetPortalState::Auth => match event {
                Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) => {
                    match allowed {
                        Ok(true) => {
                            self.state = SetPortalState::StartTransaction;
                            smallvec![Effect::Storage(StorageEffect::StartTransaction {
                                read: false
                            })]
                        }
                        Ok(false) => self.fail(SetPortalError::Unauthorized),
                        Err(error) => {
                            warn!(error = %error, "Realm portal features authorization check failed");
                            match error {
                                AuthorizationError::StorageError(error) => {
                                    self.fail(SetPortalError::StorageError(error))
                                }
                                _ => self.fail(SetPortalError::Unauthorized),
                            }
                        }
                    }
                }
                other => self.unexpected_event("authorization result", format!("{other:?}")),
            },
            SetPortalState::StartTransaction => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.emit_read_current(txn_id)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("transaction start result", format!("{other:?}")),
            },
            SetPortalState::ReadCurrent => match event {
                Event::Storage(StorageEvent::BatchReadResult { values }) => {
                    let [(_, document_value), (_, reducer_state_value)] = values.as_slice() else {
                        return self.unexpected_event(
                            "storage batch read result with realm config and reducer state",
                            format!("{values:?}"),
                        );
                    };
                    match self
                        .emit_document_write(document_value.clone(), reducer_state_value.clone())
                    {
                        Ok(effects) => effects,
                        Err(error) => self.fail(error),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch read result", format!("{other:?}")),
            },
            SetPortalState::WriteDocumentState {
                document,
                stale_conflict_deletes,
            } => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(SetPortalError::MissingTransaction);
                    };
                    if !stale_conflict_deletes.is_empty() {
                        self.state = SetPortalState::DeleteAdminConflicts { document };
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
            SetPortalState::DeleteAdminConflicts { document } => match event {
                Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {
                    self.emit_commit_transaction(document)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch delete result", format!("{other:?}")),
            },
            SetPortalState::CommitTransaction { document } => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.txn_id = None;
                    self.state = SetPortalState::ScheduleSyncDrain { document };
                    smallvec![schedule_drain_effect()]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.txn_id = None;
                    self.fail(error.into())
                }
                other => self.unexpected_event("transaction commit result", format!("{other:?}")),
            },
            SetPortalState::ScheduleSyncDrain { .. } => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.state = SetPortalState::Finish;
                    smallvec![]
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(error = %message, "Failed to schedule admin document operation outbox drain; durable outbox remains retryable");
                    self.state = SetPortalState::Finish;
                    smallvec![]
                }
                other => self.unexpected_event(
                    "document sync outbox drain timer schedule",
                    format!("{other:?}"),
                ),
            },
            SetPortalState::Finish | SetPortalState::Error | SetPortalState::Init => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, SetPortalState::Finish | SetPortalState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(SetPortalError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

/// Overlays the reducer's materialized portal features onto the document,
/// mirroring the replicated materialization in `net::document_sync`.
fn apply_reducer_portal(document: &mut RealmConfigDocument, reducer_state: &AdminDocumentState) {
    if !reducer_state.conflicts.contains_key(CONFIG_PORTAL_PATH)
        && let Some(features) = reducer_state.materialized_realm_portal()
    {
        document.portal_features = features;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::realm::get_config::GetConfigOperation;
    use aruna_core::UserId;
    use aruna_core::document::DocumentTarget;
    use aruna_core::events::StorageEvent;
    use aruna_core::keyspaces::AUTH_KEYSPACE;
    use aruna_core::structs::identity::realm::{
        RealmAuthorizationDocument, RealmId, RealmNodeKind,
    };
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

    fn actor(realm_id: RealmId) -> Actor {
        Actor {
            node_id: iroh::SecretKey::from_bytes(&[1u8; 32]).public(),
            user_id: UserId::local(Ulid::from_bytes([1u8; 16]), realm_id),
            realm_id,
        }
    }

    fn portal_config(actor: &Actor, features: BTreeMap<String, bool>) -> SetPortalConfig {
        SetPortalConfig {
            actor: actor.clone(),
            auth_context: AuthContext {
                user_id: actor.user_id,
                realm_id: actor.realm_id,
                path_restrictions: None,
                session: None,
            },
            features,
        }
    }

    async fn write(ctx: &DriverContext, key_space: &str, key: Key, value: Vec<u8>) {
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.to_string(),
                key,
                value: value.into(),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected event: {other:?}"),
        }
    }

    /// Seeds a management node config and makes the actor a realm admin.
    async fn seed(ctx: &DriverContext, actor: &Actor) {
        let mut config = RealmConfigDocument::new(actor.realm_id, Vec::new(), 3);
        config.ensure_node(actor.node_id, RealmNodeKind::Management);
        let target = DocumentTarget::RealmConfig {
            realm_id: actor.realm_id,
        };
        let value = config.to_bytes(actor).unwrap();
        write(ctx, target.storage_keyspace(), target.storage_key(), value).await;
        let mut document = RealmAuthorizationDocument::default_realm_doc(actor.realm_id);
        for role in document.roles.values_mut() {
            role.assigned_users.insert(actor.user_id);
        }
        let key = (*actor.realm_id.as_bytes()).into();
        write(ctx, AUTH_KEYSPACE, key, document.to_bytes(actor).unwrap()).await;
    }

    fn features() -> BTreeMap<String, bool> {
        BTreeMap::from([("compute".to_string(), false), ("git".to_string(), true)])
    }

    #[tokio::test]
    async fn stores_portal_features() {
        // A later config read must see the map, or portals would keep the old defaults.
        let dir = tempdir().unwrap();
        let ctx = context(dir.path().to_str().unwrap());
        let actor = actor(RealmId::from_bytes([1u8; 32]));
        seed(&ctx, &actor).await;

        let stored = drive(
            SetPortalOperation::new(portal_config(&actor, features())),
            &ctx,
        )
        .await
        .expect("portal features store");
        assert_eq!(stored.portal_features, features());

        let reread = drive(GetConfigOperation::new(actor.realm_id), &ctx)
            .await
            .expect("config reads");
        assert_eq!(reread.portal_features, features());
    }

    #[tokio::test]
    async fn refuses_invalid_names() {
        let dir = tempdir().unwrap();
        let ctx = context(dir.path().to_str().unwrap());
        let actor = actor(RealmId::from_bytes([1u8; 32]));
        seed(&ctx, &actor).await;

        let invalid = BTreeMap::from([("Compute".to_string(), false)]);
        let error = drive(
            SetPortalOperation::new(portal_config(&actor, invalid)),
            &ctx,
        )
        .await
        .expect_err("an upper case name is refused");
        assert!(matches!(error, SetPortalError::InvalidFeatures { .. }));

        let reread = drive(GetConfigOperation::new(actor.realm_id), &ctx)
            .await
            .expect("config reads");
        assert!(reread.portal_features.is_empty());
    }

    #[test]
    fn start_checks_permission() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let mut operation = SetPortalOperation::new(portal_config(&actor(realm_id), features()));
        let effects = operation.start();
        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
        let emitted = format!("{effects:?}");
        assert!(emitted.contains(&policy_admin_path(realm_id)));
        assert!(emitted.contains("WRITE"));
    }

    #[test]
    fn denied_is_terminal() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let mut operation = SetPortalOperation::new(portal_config(&actor(realm_id), features()));
        operation.start();
        let effects = operation.step(Event::SubOperation(
            SubOperationEvent::AuthorizationResult { allowed: Ok(false) },
        ));
        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert_eq!(operation.finalize(), Err(SetPortalError::Unauthorized));
    }
}
