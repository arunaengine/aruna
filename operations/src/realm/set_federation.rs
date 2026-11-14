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
    AcceptedRealms, FederationSettings, LoginProvider, RealmDescriptor, RegistrationMode, Signed,
};
use aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::reducer::{
    AdminDocumentError, AdminDocumentState, CONFIG_DESCRIPTION_PATH, CONFIG_FEDERATION_PATH,
};
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, reducer_state_key, stale_conflict_deletes,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext, NodeCapabilities, Permission};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::placement::policy::document::policy_admin_path;
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::{Effects, Key, KeySpace, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;
use url::Url;

use super::COMMIT_ATTEMPTS;
use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::federation::publish::PUBLISH_SOON;
use crate::placement::target_placement_ref;
use crate::realm::mutate_placement::is_management;
use crate::sync::document_outbox::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};

#[derive(Debug, Clone, PartialEq)]
pub struct SetFederationConfig {
    pub actor: Actor,
    /// The caller's own token context, so a path-restricted credential stays
    /// restricted; it is never derived from `actor`. `None` when the node itself acts.
    pub auth_context: Option<AuthContext>,
    /// Signs the descriptor; only a management node passes the node check.
    pub node_capabilities: NodeCapabilities,
    pub name: String,
    /// `None` keeps the stored description, or the realm description at first setup.
    /// A given description also replaces the realm description.
    pub description: Option<String>,
    pub contact: Option<String>,
    /// Ids of configured OIDC providers to show in the descriptor.
    pub login_providers: Vec<String>,
    pub api_url: Url,
    pub portal_url: Url,
    pub registry_url: Option<Url>,
    pub registration: RegistrationMode,
    pub accepted_realms: AcceptedRealms,
    /// Digest of the settings the caller read; `None` requires that none are stored.
    pub expected: Option<String>,
    /// Unix seconds; the descriptor's `issued_at` never goes below the stored one.
    pub now: u64,
}

#[derive(Debug, PartialEq)]
pub struct SetFederationOperation {
    config: SetFederationConfig,
    txn_id: Option<TxnId>,
    attempts: usize,
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
    SchedulePublication,
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
    #[error("the federation settings changed since they were read")]
    SettingsChanged,
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
            attempts: 0,
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
        let current = document
            .federation
            .as_ref()
            .map(FederationSettings::digest)
            .transpose()
            .map_err(|error| SetFederationError::InvalidSettings {
                reason: error.to_string(),
            })?;
        if current != self.config.expected {
            return Err(SetFederationError::SettingsChanged);
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
        let settings = self.signed_settings(&document, &reducer_state)?;
        let admin_event = reducer_state.apply_operation(
            &self.config.actor,
            AdminDocumentOperation::ConfigFederationSet {
                settings: Box::new(settings),
            },
        )?;
        // Materialized reducer state keeps local and replicated conflict outcomes equal.
        apply_reducer_federation(&mut document, &reducer_state);
        let mut admin_events = vec![admin_event];
        if let Some(description) = self.config.description.clone()
            && description != document.description
        {
            admin_events.push(reducer_state.apply_operation(
                &self.config.actor,
                AdminDocumentOperation::ConfigDescriptionSet { description },
            )?);
            if !reducer_state
                .conflicts
                .contains_key(CONFIG_DESCRIPTION_PATH)
                && let Some(description) = reducer_state.materialized_realm_description()
            {
                document.description = description;
            }
        }

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
        for admin_event in admin_events {
            let record = new_identified_record(
                admin_event.event_id,
                self.config.actor.node_id,
                document_target.clone(),
                Vec::new(),
                DocumentOutboxEvent::admin(admin_event),
                placement,
                false,
            );
            writes.push(outbox_write_entry(&record).map_err(ConversionError::from)?);
        }
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
        reducer_state: &AdminDocumentState,
    ) -> Result<FederationSettings, SetFederationError> {
        let config = &self.config;
        let previous = latest_issued(document, reducer_state);
        let description = config.description.clone().unwrap_or_else(|| {
            let stored = document.federation.as_ref();
            stored.map_or_else(|| document.description.clone(), |s| s.description.clone())
        });
        let login_providers = config
            .login_providers
            .iter()
            .map(|id| {
                let provider = document.oidc_providers.iter().find(|p| p.id == *id);
                provider
                    .map(|provider| LoginProvider {
                        id: provider.id.clone(),
                        issuer: provider.issuer.clone(),
                    })
                    .ok_or_else(|| SetFederationError::InvalidSettings {
                        reason: format!("{id}: no such login provider"),
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let descriptor = RealmDescriptor {
            realm_id: config.actor.realm_id,
            name: config.name.clone(),
            description: description.clone(),
            api_url: config.api_url.clone(),
            portal_url: config.portal_url.clone(),
            contact: config.contact.clone(),
            login_providers,
            issued_at: config.now.max(previous.saturating_add(1)),
        };
        let invalid =
            |error: aruna_core::federation::FederationError| SetFederationError::InvalidSettings {
                reason: error.to_string(),
            };
        let settings = FederationSettings {
            name: config.name.clone(),
            description,
            contact: config.contact.clone(),
            login_providers: config.login_providers.clone(),
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

    /// With a registry URL, publishes soon instead of at the next 6 hour renewal.
    fn schedule_publication(&mut self, document: &RealmConfigDocument) -> Effects {
        let has_registry = document
            .federation
            .as_ref()
            .is_some_and(|settings| settings.registry_url.is_some());
        if !has_registry {
            self.state = SetFederationState::Finish;
            return smallvec![];
        }
        self.state = SetFederationState::SchedulePublication;
        smallvec![Effect::Task(TaskEffect::ShortenTimer {
            key: TaskKey::PublishRegistration,
            after: PUBLISH_SOON,
        })]
    }

    fn emit_commit_transaction(&mut self, document: RealmConfigDocument) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(SetFederationError::MissingTransaction);
        };
        self.state = SetFederationState::CommitTransaction { document };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    /// Opens a transaction that reads, validates and applies the change from the start.
    fn start_transaction(&mut self) -> Effects {
        self.attempts += 1;
        self.state = SetFederationState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
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
        let Some(auth_context) = self.config.auth_context.clone() else {
            return self.start_transaction();
        };
        if auth_context.realm_id != self.config.actor.realm_id {
            return self.fail(SetFederationError::Unauthorized);
        }
        self.state = SetFederationState::Auth;
        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context,
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
                        Ok(true) => self.start_transaction(),
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
                Event::Storage(StorageEvent::Error {
                    error: StorageError::TransactionConflict,
                }) if self.attempts < COMMIT_ATTEMPTS => {
                    self.txn_id = None;
                    self.start_transaction()
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.txn_id = None;
                    self.fail(error.into())
                }
                other => self.unexpected_event("transaction commit result", format!("{other:?}")),
            },
            SetFederationState::ScheduleSyncDrain { document } => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.schedule_publication(&document)
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(error = %message, "Failed to schedule admin document operation outbox drain; durable outbox remains retryable");
                    self.schedule_publication(&document)
                }
                other => self.unexpected_event(
                    "document sync outbox drain timer schedule",
                    format!("{other:?}"),
                ),
            },
            SetFederationState::SchedulePublication => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.state = SetFederationState::Finish;
                    smallvec![]
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(error = %message, "Failed to shorten the registry publication timer");
                    self.state = SetFederationState::Finish;
                    smallvec![]
                }
                other => self
                    .unexpected_event("registry publication timer schedule", format!("{other:?}")),
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

/// The highest descriptor issue time among the stored settings and every reducer candidate,
/// so a newly signed descriptor supersedes all sides of a conflict.
fn latest_issued(document: &RealmConfigDocument, reducer_state: &AdminDocumentState) -> u64 {
    let materialized = reducer_state
        .user_subject_ids
        .get(CONFIG_FEDERATION_PATH)
        .and_then(|version| version.value.as_deref());
    let conflicting = reducer_state
        .conflicts
        .get(CONFIG_FEDERATION_PATH)
        .into_iter()
        .flat_map(|conflict| &conflict.values)
        .filter_map(|candidate| candidate.value.as_deref());
    materialized
        .into_iter()
        .chain(conflicting)
        .filter_map(|value| serde_json::from_str::<FederationSettings>(value).ok())
        .chain(document.federation.clone())
        .map(|settings| settings.descriptor.payload.issued_at)
        .max()
        .unwrap_or(0)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::realm::get_config::GetConfigOperation;
    use aruna_core::UserId;
    use aruna_core::admin_documents::AdminDocumentDot;
    use aruna_core::document::DocumentOutboxRecord;
    use aruna_core::events::StorageEvent;
    use aruna_core::keyspaces::{AUTH_KEYSPACE, SYNC_OUTBOX_KEYSPACE};
    use aruna_core::reducer::{AdminConflict, AdminConflictValue};
    use aruna_core::structs::identity::realm::{
        OidcProviderConfig, RealmAuthorizationDocument, RealmId, RealmNodeKind,
    };
    use ed25519_dalek::SigningKey;
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

    fn realm_key() -> SigningKey {
        SigningKey::from_bytes(&[9u8; 32])
    }

    fn actor() -> Actor {
        let realm_id = RealmId::from_bytes(realm_key().verifying_key().to_bytes());
        Actor {
            node_id: iroh::SecretKey::from_bytes(&[1u8; 32]).public(),
            user_id: UserId::local(Ulid::from_bytes([1u8; 16]), realm_id),
            realm_id,
        }
    }

    fn request(actor: &Actor, api_url: &str) -> SetFederationConfig {
        SetFederationConfig {
            actor: actor.clone(),
            auth_context: Some(AuthContext {
                user_id: actor.user_id,
                realm_id: actor.realm_id,
                path_restrictions: None,
                session: None,
            }),
            node_capabilities: NodeCapabilities::management_node(realm_key()).unwrap(),
            name: "Realm".to_string(),
            description: None,
            contact: None,
            login_providers: Vec::new(),
            api_url: Url::parse(api_url).unwrap(),
            portal_url: Url::parse("https://portal.example.org").unwrap(),
            registry_url: None,
            registration: RegistrationMode::Enabled,
            accepted_realms: AcceptedRealms::None,
            expected: None,
            now: 100,
        }
    }

    async fn write(ctx: &DriverContext, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.to_string(),
                key: key.into(),
                value: value.into(),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected event: {other:?}"),
        }
    }

    async fn seed(ctx: &DriverContext, actor: &Actor, kind: RealmNodeKind, admin: bool) {
        let mut document = RealmConfigDocument::new(actor.realm_id, Vec::new(), 3);
        document.ensure_node(actor.node_id, kind);
        let target = DocumentTarget::RealmConfig {
            realm_id: actor.realm_id,
        };
        let value = document.to_bytes(actor).unwrap();
        write(
            ctx,
            target.storage_keyspace(),
            target.storage_key().to_vec(),
            value,
        )
        .await;
        let mut authorization = RealmAuthorizationDocument::default_realm_doc(actor.realm_id);
        for role in authorization.roles.values_mut() {
            if admin {
                role.assigned_users.insert(actor.user_id);
            }
        }
        let value = authorization.to_bytes(actor).unwrap();
        write(
            ctx,
            AUTH_KEYSPACE,
            actor.realm_id.as_bytes().to_vec(),
            value,
        )
        .await;
    }

    async fn stored(ctx: &DriverContext, actor: &Actor) -> Option<FederationSettings> {
        drive(GetConfigOperation::new(actor.realm_id), ctx)
            .await
            .expect("config reads")
            .federation
    }

    #[tokio::test]
    async fn stores_signed_descriptor() {
        // The stored descriptor verifies for the realm and its issue time only grows.
        let dir = tempdir().unwrap();
        let ctx = context(dir.path().to_str().unwrap());
        let actor = actor();
        seed(&ctx, &actor, RealmNodeKind::Management, true).await;

        let mut config = request(&actor, "https://api.example.org");
        drive(SetFederationOperation::new(config.clone()), &ctx)
            .await
            .expect("settings store");
        let first = stored(&ctx, &actor).await.expect("settings stored");
        assert_eq!(first.descriptor.verify(&actor.realm_id), Ok(()));
        assert_eq!(first.descriptor.payload.issued_at, 100);

        config.expected = Some(first.digest().unwrap());
        drive(SetFederationOperation::new(config), &ctx)
            .await
            .expect("settings store again");
        let second = stored(&ctx, &actor).await.expect("settings stored");
        assert_eq!(second.descriptor.payload.issued_at, 101);
    }

    #[tokio::test]
    async fn renames_resign_descriptor() {
        // A rename re-signs the descriptor with the realm key and keeps the other public facts.
        let dir = tempdir().unwrap();
        let ctx = context(dir.path().to_str().unwrap());
        let actor = actor();
        seed(&ctx, &actor, RealmNodeKind::Management, true).await;
        let mut document = RealmConfigDocument::new(actor.realm_id, Vec::new(), 3);
        document.ensure_node(actor.node_id, RealmNodeKind::Management);
        document.oidc_providers.push(OidcProviderConfig {
            id: "default".to_string(),
            issuer: "https://login.example.org/realms/aruna".to_string(),
            audience: "aruna".to_string(),
            discovery_url: String::new(),
        });
        let target = DocumentTarget::RealmConfig {
            realm_id: actor.realm_id,
        };
        let value = document.to_bytes(&actor).unwrap();
        write(
            &ctx,
            target.storage_keyspace(),
            target.storage_key().to_vec(),
            value,
        )
        .await;

        let mut config = request(&actor, "https://api.example.org");
        config.description = Some("About the realm".to_string());
        config.contact = Some("data@example.org".to_string());
        config.login_providers = vec!["default".to_string()];
        drive(SetFederationOperation::new(config.clone()), &ctx)
            .await
            .expect("settings store");
        let first = stored(&ctx, &actor).await.expect("settings stored");
        let issuer = &first.descriptor.payload.login_providers[0].issuer;
        assert_eq!(issuer, "https://login.example.org/realms/aruna");

        config.expected = Some(first.digest().unwrap());
        config.name = "Renamed".to_string();
        config.description = None;
        drive(SetFederationOperation::new(config.clone()), &ctx)
            .await
            .expect("rename stores");
        let renamed = stored(&ctx, &actor).await.expect("settings stored");
        let payload = &renamed.descriptor.payload;
        assert_eq!(renamed.descriptor.verify(&actor.realm_id), Ok(()));
        assert_eq!(payload.realm_id, actor.realm_id);
        assert_eq!(payload.name, "Renamed");
        assert_eq!(payload.description, "About the realm");
        assert_eq!(payload.contact.as_deref(), Some("data@example.org"));
        assert!(payload.issued_at > first.descriptor.payload.issued_at);

        config.expected = Some(renamed.digest().unwrap());
        config.login_providers = vec!["unknown".to_string()];
        let error = drive(SetFederationOperation::new(config), &ctx)
            .await
            .expect_err("an unknown provider is refused");
        assert!(matches!(error, SetFederationError::InvalidSettings { .. }));
        assert_eq!(stored(&ctx, &actor).await, Some(renamed));
    }

    #[test]
    fn issued_above_conflicts() {
        // A conflict candidate newer than the stored descriptor raises the next issue time.
        let actor = actor();
        let config = request(&actor, "https://api.example.org");
        let operation = SetFederationOperation::new(config.clone());
        let empty = RealmConfigDocument::new(actor.realm_id, Vec::new(), 3);
        let settings_at = |issued_at: u64| {
            let mut config = config.clone();
            config.now = issued_at;
            SetFederationOperation::new(config)
                .signed_settings(&empty, &AdminDocumentState::new(operation.admin_target()))
                .unwrap()
        };
        let stored = settings_at(200);
        let candidate = |issued_at| AdminConflictValue {
            value: Some(serde_json::to_string(&settings_at(issued_at)).unwrap()),
            dot: AdminDocumentDot {
                event_id: Ulid::from_bytes([issued_at as u8; 16]),
                origin_node_id: actor.node_id,
                origin_seq: issued_at,
            },
        };
        let mut state = AdminDocumentState::new(operation.admin_target());
        state.conflicts.insert(
            CONFIG_FEDERATION_PATH.to_string(),
            AdminConflict {
                path: CONFIG_FEDERATION_PATH.to_string(),
                values: vec![candidate(500), candidate(300)],
            },
        );
        let mut document = empty.clone();
        document.federation = Some(stored);
        let signed = operation.signed_settings(&document, &state).unwrap();
        assert_eq!(signed.descriptor.payload.issued_at, 501);
    }

    /// Steps to the batch write without I/O and returns the written realm config and admin ops.
    fn written_ops(
        description: Option<&str>,
    ) -> (SetFederationOperation, String, Vec<AdminDocumentOperation>) {
        let actor = actor();
        let mut config = request(&actor, "https://api.example.org");
        config.description = description.map(str::to_string);
        let mut document = RealmConfigDocument::new(actor.realm_id, Vec::new(), 3);
        document.description = "Old_Realm".to_string();
        document.ensure_node(actor.node_id, RealmNodeKind::Management);
        let value = Value::from(document.to_bytes(&actor).unwrap());
        let mut operation = SetFederationOperation::new(config);
        assert!(matches!(
            operation.start().as_slice(),
            [Effect::SubOperation(_)]
        ));
        operation.step(Event::SubOperation(
            SubOperationEvent::AuthorizationResult { allowed: Ok(true) },
        ));
        let txn_id = Ulid::from_bytes([7u8; 16]);
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (Key::from(b"config".as_slice()), Some(value)),
                (Key::from(b"state".as_slice()), None),
            ],
        }));
        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("expected one batch write, got {effects:?}");
        };
        let stored = RealmConfigDocument::from_bytes(&writes[0].2).unwrap();
        let ops = writes
            .iter()
            .filter(|(key_space, _, _)| key_space == SYNC_OUTBOX_KEYSPACE)
            .map(|(_, _, value)| {
                let record: DocumentOutboxRecord = postcard::from_bytes(value).unwrap();
                match record.event {
                    DocumentOutboxEvent::AdminOperation { event, .. } => event.op,
                    other => panic!("expected an admin operation, got {other:?}"),
                }
            })
            .collect();
        (operation, stored.description, ops)
    }

    #[test]
    fn description_sets_realm() {
        // A changed description also replaces the replicated realm description in the same write.
        let (mut operation, description, ops) = written_ops(Some("NFDI4LS"));
        assert_eq!(description, "NFDI4LS");
        assert!(matches!(
            ops.as_slice(),
            [
                AdminDocumentOperation::ConfigFederationSet { .. },
                AdminDocumentOperation::ConfigDescriptionSet { description },
            ] if description == "NFDI4LS"
        ));
        operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        let txn_id = Ulid::from_bytes([7u8; 16]);
        operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
        operation.step(Event::Task(TaskEvent::TimerScheduled {
            key: TaskKey::DrainSyncOutbox,
            after: std::time::Duration::ZERO,
        }));
        assert!(operation.is_complete());
        assert_eq!(operation.finalize().unwrap().description, "NFDI4LS");

        // Without a description, or with the current one, the realm description stays.
        for kept in [None, Some("Old_Realm")] {
            let (_, description, ops) = written_ops(kept);
            assert_eq!(description, "Old_Realm");
            assert!(matches!(
                ops.as_slice(),
                [AdminDocumentOperation::ConfigFederationSet { .. }]
            ));
        }
    }

    #[tokio::test]
    async fn refuses_changed_settings() {
        // A write based on an older read, or a first setup over stored settings, changes nothing.
        let dir = tempdir().unwrap();
        let ctx = context(dir.path().to_str().unwrap());
        let actor = actor();
        seed(&ctx, &actor, RealmNodeKind::Management, true).await;
        let mut config = request(&actor, "https://api.example.org");
        drive(SetFederationOperation::new(config.clone()), &ctx)
            .await
            .expect("first setup stores");
        let first = stored(&ctx, &actor).await.expect("settings stored");

        let error = drive(SetFederationOperation::new(config.clone()), &ctx)
            .await
            .expect_err("a second first setup is refused");
        assert_eq!(error, SetFederationError::SettingsChanged);

        config.expected = Some(first.digest().unwrap());
        drive(SetFederationOperation::new(config.clone()), &ctx)
            .await
            .expect("a write on the current settings stores");
        let second = stored(&ctx, &actor).await.expect("settings stored");

        config.api_url = Url::parse("https://other.example.org").unwrap();
        let error = drive(SetFederationOperation::new(config), &ctx)
            .await
            .expect_err("a write on older settings is refused");
        assert_eq!(error, SetFederationError::SettingsChanged);
        assert_eq!(stored(&ctx, &actor).await, Some(second));
    }

    #[tokio::test]
    async fn node_creates_once() {
        // The node needs no token, and stored settings stay, also a cleared URL and a disabled mode.
        let dir = tempdir().unwrap();
        let ctx = context(dir.path().to_str().unwrap());
        let actor = actor();
        seed(&ctx, &actor, RealmNodeKind::Management, false).await;
        let mut config = request(&actor, "https://api.example.org");
        config.auth_context = None;
        config.registration = RegistrationMode::Disabled;
        drive(SetFederationOperation::new(config.clone()), &ctx)
            .await
            .expect("the node stores the first settings");
        let first = stored(&ctx, &actor).await.expect("settings stored");
        assert_eq!(first.descriptor.verify(&actor.realm_id), Ok(()));

        config.registry_url = Some(Url::parse("https://registry.example.org").unwrap());
        config.registration = RegistrationMode::Enabled;
        let error = drive(SetFederationOperation::new(config), &ctx)
            .await
            .expect_err("stored settings are never replaced");
        assert_eq!(error, SetFederationError::SettingsChanged);
        assert_eq!(stored(&ctx, &actor).await, Some(first));
    }

    #[tokio::test]
    async fn refuses_non_admin() {
        let dir = tempdir().unwrap();
        let ctx = context(dir.path().to_str().unwrap());
        let actor = actor();
        seed(&ctx, &actor, RealmNodeKind::Management, false).await;

        let error = drive(
            SetFederationOperation::new(request(&actor, "https://api.example.org")),
            &ctx,
        )
        .await
        .expect_err("a non-admin is refused");
        assert_eq!(error, SetFederationError::Unauthorized);
        assert_eq!(stored(&ctx, &actor).await, None);
    }

    #[tokio::test]
    async fn refuses_server_node() {
        // Peers reject realm-config events from a server node, so it never writes them.
        let dir = tempdir().unwrap();
        let ctx = context(dir.path().to_str().unwrap());
        let actor = actor();
        seed(&ctx, &actor, RealmNodeKind::Server, true).await;

        let error = drive(
            SetFederationOperation::new(request(&actor, "https://api.example.org")),
            &ctx,
        )
        .await
        .expect_err("a server node is refused");
        assert_eq!(error, SetFederationError::NotManagementNode);
    }

    #[tokio::test]
    async fn refuses_plain_http() {
        let dir = tempdir().unwrap();
        let ctx = context(dir.path().to_str().unwrap());
        let actor = actor();
        seed(&ctx, &actor, RealmNodeKind::Management, true).await;

        let error = drive(
            SetFederationOperation::new(request(&actor, "http://api.example.org")),
            &ctx,
        )
        .await
        .expect_err("plain http is refused");
        assert!(matches!(error, SetFederationError::InvalidSettings { .. }));
        assert_eq!(stored(&ctx, &actor).await, None);
    }
}
