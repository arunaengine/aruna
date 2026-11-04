//! Adds, replaces or removes one realm OIDC provider entry and keeps the list order.
//! It writes through the shared admin document path, so the change reaches every node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::federation::valid_federation_url;
use aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState, config_oidc_path};
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, reducer_state_key, stale_conflict_deletes,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::{OidcProviderConfig, RealmConfigDocument};
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

const MAX_ID_LEN: usize = 64;

#[derive(Debug, Clone, PartialEq)]
pub enum ProviderChange {
    /// Replaces the entry with this id in place, or appends a new one.
    Upsert(OidcProviderConfig),
    Remove(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct SetProviderConfig {
    pub actor: Actor,
    /// The caller's own token context, so a path-restricted credential stays
    /// restricted; it is never derived from `actor`.
    pub auth_context: AuthContext,
    pub change: ProviderChange,
}

#[derive(Debug, PartialEq)]
pub struct SetProviderOperation {
    config: SetProviderConfig,
    txn_id: Option<TxnId>,
    state: SetProviderState,
    output: Option<Result<RealmConfigDocument, SetProviderError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum SetProviderState {
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
pub enum SetProviderError {
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
    #[error("invalid OIDC provider: {reason}")]
    InvalidProvider { reason: String },
    #[error("no OIDC provider with this id")]
    UnknownProvider,
    #[error("the last OIDC provider cannot be removed")]
    LastProvider,
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

/// Checks the id and fields; URLs must be HTTPS, or HTTP to a loopback host for development.
pub fn validate_provider(provider: &OidcProviderConfig) -> Result<(), String> {
    validate_id(&provider.id)?;
    if provider.audience.trim().is_empty() {
        return Err("audience must not be empty".to_string());
    }
    for (field, value) in [
        ("issuer", &provider.issuer),
        ("discovery_url", &provider.discovery_url),
    ] {
        if !Url::parse(value).is_ok_and(|url| valid_federation_url(&url)) {
            return Err(format!("{field} must be an HTTPS URL"));
        }
    }
    Ok(())
}

fn validate_id(id: &str) -> Result<(), String> {
    let allowed = |c: char| c.is_ascii_alphanumeric() || c == '-' || c == '_';
    if id.is_empty() || id.len() > MAX_ID_LEN || !id.chars().all(allowed) {
        return Err(format!(
            "id must hold 1 to {MAX_ID_LEN} characters of a-z, A-Z, 0-9, - and _"
        ));
    }
    Ok(())
}

impl SetProviderOperation {
    pub fn new(config: SetProviderConfig) -> Self {
        Self {
            config,
            txn_id: None,
            state: SetProviderState::Init,
            output: None,
        }
    }

    fn provider_id(&self) -> &str {
        match &self.config.change {
            ProviderChange::Upsert(provider) => &provider.id,
            ProviderChange::Remove(id) => id,
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
        self.state = SetProviderState::ReadCurrent;
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
    ) -> Result<Effects, SetProviderError> {
        let Some(txn_id) = self.txn_id else {
            return Err(SetProviderError::MissingTransaction);
        };
        let Some(document_value) = document_value else {
            return Err(SetProviderError::ConfigMissing);
        };
        let mut document = RealmConfigDocument::from_bytes(&document_value)?;
        if !is_management(&document, self.config.actor.node_id) {
            return Err(SetProviderError::NotManagementNode);
        }
        let operation = match &self.config.change {
            ProviderChange::Upsert(provider) => AdminDocumentOperation::OidcProviderUpserted {
                provider: provider.clone(),
            },
            ProviderChange::Remove(id) => {
                if !document.oidc_providers.iter().any(|p| p.id == *id) {
                    return Err(SetProviderError::UnknownProvider);
                }
                if document.oidc_providers.len() == 1 {
                    return Err(SetProviderError::LastProvider);
                }
                AdminDocumentOperation::OidcProviderRemoved {
                    provider_id: id.clone(),
                }
            }
        };

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
        let admin_event = reducer_state.apply_operation(&self.config.actor, operation)?;
        // Materialized reducer state keeps local and replicated conflict outcomes equal.
        apply_reducer_provider(&mut document, &reducer_state, self.provider_id());

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
        self.state = SetProviderState::WriteDocumentState {
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
            return self.fail(SetProviderError::MissingTransaction);
        };
        self.state = SetProviderState::CommitTransaction { document };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn fail(&mut self, error: SetProviderError) -> Effects {
        let cleanup = self.abort();
        self.state = SetProviderState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(SetProviderError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for SetProviderOperation {
    type Output = RealmConfigDocument;
    type Error = SetProviderError;

    fn start(&mut self) -> Effects {
        if self.config.auth_context.realm_id != self.config.actor.realm_id {
            return self.fail(SetProviderError::Unauthorized);
        }
        let valid = match &self.config.change {
            ProviderChange::Upsert(provider) => validate_provider(provider),
            ProviderChange::Remove(id) => validate_id(id),
        };
        if let Err(reason) = valid {
            return self.fail(SetProviderError::InvalidProvider { reason });
        }
        self.state = SetProviderState::Auth;
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
            SetProviderState::Auth => match event {
                Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) => {
                    match allowed {
                        Ok(true) => {
                            self.state = SetProviderState::StartTransaction;
                            smallvec![Effect::Storage(StorageEffect::StartTransaction {
                                read: false
                            })]
                        }
                        Ok(false) => self.fail(SetProviderError::Unauthorized),
                        Err(error) => {
                            warn!(error = %error, "Realm OIDC provider authorization check failed");
                            match error {
                                AuthorizationError::StorageError(error) => {
                                    self.fail(SetProviderError::StorageError(error))
                                }
                                _ => self.fail(SetProviderError::Unauthorized),
                            }
                        }
                    }
                }
                other => self.unexpected_event("authorization result", format!("{other:?}")),
            },
            SetProviderState::StartTransaction => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.emit_read_current(txn_id)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("transaction start result", format!("{other:?}")),
            },
            SetProviderState::ReadCurrent => match event {
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
            SetProviderState::WriteDocumentState {
                document,
                stale_conflict_deletes,
            } => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(SetProviderError::MissingTransaction);
                    };
                    if !stale_conflict_deletes.is_empty() {
                        self.state = SetProviderState::DeleteAdminConflicts { document };
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
            SetProviderState::DeleteAdminConflicts { document } => match event {
                Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {
                    self.emit_commit_transaction(document)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch delete result", format!("{other:?}")),
            },
            SetProviderState::CommitTransaction { document } => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.txn_id = None;
                    self.state = SetProviderState::ScheduleSyncDrain { document };
                    smallvec![schedule_drain_effect()]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.txn_id = None;
                    self.fail(error.into())
                }
                other => self.unexpected_event("transaction commit result", format!("{other:?}")),
            },
            SetProviderState::ScheduleSyncDrain { .. } => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.state = SetProviderState::Finish;
                    smallvec![]
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(error = %message, "Failed to schedule admin document operation outbox drain; durable outbox remains retryable");
                    self.state = SetProviderState::Finish;
                    smallvec![]
                }
                other => self.unexpected_event(
                    "document sync outbox drain timer schedule",
                    format!("{other:?}"),
                ),
            },
            SetProviderState::Finish | SetProviderState::Error | SetProviderState::Init => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            SetProviderState::Finish | SetProviderState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if !self.is_complete() {
            return Err(SetProviderError::NotFinished);
        }
        self.output.unwrap_or(Err(SetProviderError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

/// Overlays the reducer's outcome for this provider onto the document, mirroring the
/// replicated materialization in `net::document_sync`; a conflict withholds the entry.
fn apply_reducer_provider(
    document: &mut RealmConfigDocument,
    reducer_state: &AdminDocumentState,
    provider_id: &str,
) {
    let conflicted = reducer_state
        .conflicts
        .contains_key(&config_oidc_path(provider_id));
    match reducer_state
        .materialized_oidc_providers()
        .remove(provider_id)
    {
        Some(provider) if !conflicted => document.upsert_provider(provider),
        _ => document.oidc_providers.retain(|p| p.id != provider_id),
    }
}
