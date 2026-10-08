//! Replaces the realm federation settings and signs the realm descriptor from them.
//! It writes through the shared admin document path, so concurrent changes converge.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::federation::{
    AcceptedRealms, FederationSettings, RealmDescriptor, RegistrationMode, Signed,
};
use aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState, CONFIG_FEDERATION_PATH};
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, reducer_state_key, stale_conflict_deletes,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext, NodeCapabilities, Permission};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::placement::policy::document::policy_admin_path;
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, Key, KeySpace, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;
use url::Url;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::placement::target_placement_ref;
use crate::realm::mutate_placement::is_management;
use crate::sync::document_outbox::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};

#[derive(Debug, Clone, PartialEq)]
pub struct SetFederationConfig {
    pub actor: Actor,
    /// The caller's own token context, so a path-restricted credential stays
    /// restricted; it is never derived from `actor`.
    pub auth_context: AuthContext,
    /// Signs the descriptor; only a management node passes the node check.
    pub node_capabilities: NodeCapabilities,
    pub name: String,
    pub api_url: Url,
    pub portal_url: Url,
    pub registry_url: Option<Url>,
    pub registration: RegistrationMode,
    pub accepted_realms: AcceptedRealms,
    /// Unix seconds; the descriptor's `issued_at` never goes below the stored one.
    pub now: u64,
}

#[derive(Debug, PartialEq)]
pub struct SetFederationOperation {
    config: SetFederationConfig,
    txn_id: Option<TxnId>,
    state: SetFederationState,
    output: Option<Result<RealmConfigDocument, SetFederationError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum SetFederationState {
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
pub enum SetFederationError {
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
    #[error("invalid federation settings: {reason}")]
    InvalidSettings { reason: String },
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

impl SetFederationOperation {
    pub fn new(config: SetFederationConfig) -> Self {
        Self {
            config,
            txn_id: None,
            state: SetFederationState::Init,
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
        self.state = SetFederationState::ReadCurrent;
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
    ) -> Result<Effects, SetFederationError> {
        let Some(txn_id) = self.txn_id else {
            return Err(SetFederationError::MissingTransaction);
        };
        let Some(document_value) = document_value else {
            return Err(SetFederationError::ConfigMissing);
        };
        let mut document = RealmConfigDocument::from_bytes(&document_value)?;
        if !is_management(&document, self.config.actor.node_id) {
            return Err(SetFederationError::NotManagementNode);
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
        let settings = self.signed_settings(&document)?;
        let admin_event = reducer_state.apply_operation(
            &self.config.actor,
            AdminDocumentOperation::ConfigFederationSet {
                settings: Box::new(settings),
            },
        )?;
        // Materialized reducer state keeps local and replicated conflict outcomes equal.
        apply_reducer_federation(&mut document, &reducer_state);

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
        self.state = SetFederationState::WriteDocumentState {
            document,
            stale_conflict_deletes,
        };

        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }

    fn signed_settings(
        &self,
        document: &RealmConfigDocument,
    ) -> Result<FederationSettings, SetFederationError> {
        let config = &self.config;
        let previous = document
            .federation
            .as_ref()
            .map_or(0, |settings| settings.descriptor.payload.issued_at);
        let descriptor = RealmDescriptor {
            realm_id: config.actor.realm_id,
            name: config.name.clone(),
            description: document.description.clone(),
            api_url: config.api_url.clone(),
            portal_url: config.portal_url.clone(),
            issued_at: config.now.max(previous.saturating_add(1)),
        };
        let invalid =
            |error: aruna_core::federation::FederationError| SetFederationError::InvalidSettings {
                reason: error.to_string(),
            };
        let settings = FederationSettings {
            name: config.name.clone(),
            api_url: config.api_url.clone(),
            portal_url: config.portal_url.clone(),
            registry_url: config.registry_url.clone(),
            registration: config.registration,
            accepted_realms: config.accepted_realms.clone(),
            descriptor: Signed::sign(descriptor, &config.node_capabilities).map_err(invalid)?,
        };
        settings.validate(&config.actor.realm_id).map_err(invalid)?;
        Ok(settings)
    }

    fn emit_commit_transaction(&mut self, document: RealmConfigDocument) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(SetFederationError::MissingTransaction);
        };
        self.state = SetFederationState::CommitTransaction { document };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn fail(&mut self, error: SetFederationError) -> Effects {
        let cleanup = self.abort();
        self.state = SetFederationState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(SetFederationError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for SetFederationOperation {
    type Output = RealmConfigDocument;
    type Error = SetFederationError;

    fn start(&mut self) -> Effects {
        if self.config.auth_context.realm_id != self.config.actor.realm_id {
            return self.fail(SetFederationError::Unauthorized);
        }
        self.state = SetFederationState::Auth;
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
            SetFederationState::Auth => match event {
                Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) => {
                    match allowed {
                        Ok(true) => {
                            self.state = SetFederationState::StartTransaction;
                            smallvec![Effect::Storage(StorageEffect::StartTransaction {
                                read: false
                            })]
                        }
                        Ok(false) => self.fail(SetFederationError::Unauthorized),
                        Err(error) => {
                            warn!(error = %error, "Realm federation authorization check failed");
                            match error {
                                AuthorizationError::StorageError(error) => {
                                    self.fail(SetFederationError::StorageError(error))
                                }
                                _ => self.fail(SetFederationError::Unauthorized),
                            }
                        }
                    }
                }
                other => self.unexpected_event("authorization result", format!("{other:?}")),
            },
            SetFederationState::StartTransaction => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.emit_read_current(txn_id)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("transaction start result", format!("{other:?}")),
            },
            SetFederationState::ReadCurrent => match event {
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
            SetFederationState::WriteDocumentState {
                document,
                stale_conflict_deletes,
            } => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(SetFederationError::MissingTransaction);
                    };
                    if !stale_conflict_deletes.is_empty() {
                        self.state = SetFederationState::DeleteAdminConflicts { document };
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
            SetFederationState::DeleteAdminConflicts { document } => match event {
                Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {
                    self.emit_commit_transaction(document)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch delete result", format!("{other:?}")),
            },
            SetFederationState::CommitTransaction { document } => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.txn_id = None;
                    self.state = SetFederationState::ScheduleSyncDrain { document };
                    smallvec![schedule_drain_effect()]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.txn_id = None;
                    self.fail(error.into())
                }
                other => self.unexpected_event("transaction commit result", format!("{other:?}")),
            },
            SetFederationState::ScheduleSyncDrain { .. } => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.state = SetFederationState::Finish;
                    smallvec![]
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(error = %message, "Failed to schedule admin document operation outbox drain; durable outbox remains retryable");
                    self.state = SetFederationState::Finish;
                    smallvec![]
                }
                other => self.unexpected_event(
                    "document sync outbox drain timer schedule",
                    format!("{other:?}"),
                ),
            },
            SetFederationState::Finish | SetFederationState::Error | SetFederationState::Init => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            SetFederationState::Finish | SetFederationState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(SetFederationError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

/// Overlays the reducer's materialized federation settings onto the document,
/// mirroring the replicated materialization in `net::irokle`.
fn apply_reducer_federation(
    document: &mut RealmConfigDocument,
    reducer_state: &AdminDocumentState,
) {
    if !reducer_state.conflicts.contains_key(CONFIG_FEDERATION_PATH)
        && let Some(settings) = reducer_state.materialized_realm_federation()
    {
        document.federation = Some(settings);
    }
}

