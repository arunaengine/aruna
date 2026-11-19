//! Replaces the realm federation settings and signs the realm descriptor from them.
//! It writes through the shared admin document path, so concurrent changes converge.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::effects::Effect;
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::Event;
use aruna_core::federation::{
    AcceptedRealms, FederationSettings, LoginProvider, PUBLISH_SOON, RealmDescriptor,
    RegistrationMode, Signed,
};
use aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode, boxed_retry};
use aruna_core::reducer::{
    AdminDocumentError, AdminDocumentState, CONFIG_DESCRIPTION_PATH, CONFIG_FEDERATION_PATH,
};
use aruna_core::rows::{done, values};
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, reducer_state_key, stale_conflict_deletes,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext, NodeCapabilities, Permission};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::placement::policy::document::policy_admin_path;
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{Key, KeySpace, Value};
use thiserror::Error;
use tracing::warn;
use url::Url;

use super::COMMIT_ATTEMPTS;
use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::realm::mutate_placement::is_management;
use crate::sync::outbox_record::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};
use aruna_core::placement::target_placement_ref;

use std::sync::Arc;
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

/// Checks the caller's config permission, then writes the change in a write transaction that
/// reruns after a lost conflict, since a background config write can win its commit.
#[derive(Debug, PartialEq)]
pub struct SetFederationOperation {
    config: Arc<SetFederationConfig>,
    authorized: bool,
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
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl From<IoError> for SetFederationError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "SetFederation".to_string(),
                expected,
                got,
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished | IoError::Codec(_) => {
                Self::NotFinished
            }
        }
    }
}

impl SetFederationOperation {
    pub fn new(config: SetFederationConfig) -> Self {
        Self {
            config: Arc::new(config),
            authorized: false,
        }
    }

    fn change(&self) -> IoCall {
        let config = Arc::clone(&self.config);
        let seed = self.config.actor.user_id.to_bytes();
        let build = move || FederationWrite::new(Arc::clone(&config));
        let child = boxed_retry(build, &seed, |result| {
            Event::Child(ChildOutcome::new(result))
        });
        IoCall::Child(child)
    }
}

impl Operation for SetFederationOperation {
    type Output = RealmConfigDocument;
    type Error = SetFederationError;

    /// The node itself acts without a caller context, so only a caller's change is checked.
    fn first(&mut self) -> Result<Next<Self::Output>, SetFederationError> {
        let Some(auth_context) = self.config.auth_context.clone() else {
            self.authorized = true;
            return Ok(Next::Call(self.change()));
        };
        if auth_context.realm_id != self.config.actor.realm_id {
            return Err(SetFederationError::Unauthorized);
        }
        Ok(Next::Call(IoCall::child(CheckPermissionsOperation::new(
            CheckPermissionsConfig {
                auth_context,
                path: policy_admin_path(self.config.actor.realm_id),
                required_permission: Permission::WRITE,
            },
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, SetFederationError> {
        if self.authorized {
            return result
                .child::<RealmConfigDocument, SetFederationError>()?
                .map(Next::Done);
        }
        match result.child::<bool, AuthorizationError>()? {
            Ok(true) => {
                self.authorized = true;
                Ok(Next::Call(self.change()))
            }
            Ok(false) => Err(SetFederationError::Unauthorized),
            Err(AuthorizationError::StorageError(error)) => Err(error.into()),
            Err(error) => {
                warn!(error = %error, "Realm federation authorization check failed");
                Err(SetFederationError::Unauthorized)
            }
        }
    }
}

/// What the config write transaction waits for.
#[derive(Debug, PartialEq)]
enum Stage {
    Read,
    Write,
    Delete,
}

/// Reads the realm config and its admin state, then stores the change and its queued admin
/// events in one write transaction.
#[derive(Debug, PartialEq)]
struct FederationWrite {
    config: Arc<SetFederationConfig>,
    stage: Stage,
    document: Option<RealmConfigDocument>,
    deletes: Vec<(KeySpace, Key)>,
}

impl FederationWrite {
    fn new(config: Arc<SetFederationConfig>) -> Self {
        Self {
            config,
            stage: Stage::Read,
            document: None,
            deletes: Vec::new(),
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

    fn emit_document_write(
        &mut self,
        document_value: Option<Value>,
        reducer_state_value: Option<Value>,
    ) -> Result<Next<RealmConfigDocument>, SetFederationError> {
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

        self.document = Some(document.clone());
        self.deletes = stale_conflict_deletes;
        self.stage = Stage::Write;
        Ok(Next::Call(IoCall::Write(writes)))
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
    fn publication(document: &RealmConfigDocument) -> Option<IoCall> {
        let has_registry = document
            .federation
            .as_ref()
            .is_some_and(|settings| settings.registry_url.is_some());
        let shorten = Effect::Task(TaskEffect::ShortenTimer {
            key: TaskKey::PublishRegistration,
            after: PUBLISH_SOON,
        });
        has_registry.then(|| IoCall::Effect(Box::new(shorten)))
    }

    /// A failed drain or publication timer leaves the outbox and the renewal retryable.
    fn finish(&mut self) -> Result<Next<RealmConfigDocument>, SetFederationError> {
        let output = self
            .document
            .take()
            .ok_or(SetFederationError::NotFinished)?;
        let mut calls = vec![IoCall::Effect(Box::new(schedule_drain_effect()))];
        calls.extend(Self::publication(&output));
        Ok(Next::Then { output, calls })
    }
}

impl Operation for FederationWrite {
    type Output = RealmConfigDocument;
    type Error = SetFederationError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn conflict_retries() -> u32 {
        COMMIT_ATTEMPTS as u32 - 1
    }

    fn first(&mut self) -> Result<Next<Self::Output>, SetFederationError> {
        let document = self.document_ref();
        let target = self.admin_target();
        Ok(Next::Call(IoCall::Read(vec![
            (
                document.storage_keyspace().to_string(),
                document.storage_key(),
            ),
            (
                DOCUMENT_STATE_KEYSPACE.to_string(),
                reducer_state_key(&target),
            ),
        ])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, SetFederationError> {
        match self.stage {
            Stage::Read => {
                let [document, reducer] = values::<2>(result)?;
                self.emit_document_write(document, reducer)
            }
            Stage::Write if !self.deletes.is_empty() => {
                done(result)?;
                self.stage = Stage::Delete;
                Ok(Next::Call(IoCall::Delete(std::mem::take(
                    &mut self.deletes,
                ))))
            }
            Stage::Write | Stage::Delete => {
                done(result)?;
                self.finish()
            }
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
    use crate::document_repository::GetConfigOperation;
    use crate::driver::{DriverContext, drive};
    use crate::testing::path_context;
    use aruna_core::UserId;
    use aruna_core::admin_documents::AdminDocumentDot;
    use aruna_core::document::DocumentOutboxRecord;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::StorageEvent;
    use aruna_core::keyspaces::{AUTH_KEYSPACE, SYNC_OUTBOX_KEYSPACE};
    use aruna_core::reducer::{AdminConflict, AdminConflictValue};
    use aruna_core::structs::identity::realm::{
        OidcProviderConfig, RealmAuthorizationDocument, RealmId, RealmNodeKind,
    };
    use ed25519_dalek::SigningKey;
    use tempfile::tempdir;
    use ulid::Ulid;

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
        let ctx = path_context(dir.path().to_str().unwrap());
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
        let ctx = path_context(dir.path().to_str().unwrap());
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
        let operation = FederationWrite::new(Arc::new(config.clone()));
        let empty = RealmConfigDocument::new(actor.realm_id, Vec::new(), 3);
        let settings_at = |issued_at: u64| {
            let mut config = config.clone();
            config.now = issued_at;
            FederationWrite::new(Arc::new(config))
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

    /// Runs the write to its batch without I/O and returns the written realm config and admin ops.
    fn written_ops(
        description: Option<&str>,
    ) -> (FederationWrite, String, Vec<AdminDocumentOperation>) {
        let actor = actor();
        let mut config = request(&actor, "https://api.example.org");
        config.description = description.map(str::to_string);
        let mut document = RealmConfigDocument::new(actor.realm_id, Vec::new(), 3);
        document.description = "Old_Realm".to_string();
        document.ensure_node(actor.node_id, RealmNodeKind::Management);
        let value = Value::from(document.to_bytes(&actor).unwrap());
        let mut write = FederationWrite::new(Arc::new(config));
        write.first().unwrap();
        let Ok(Next::Call(IoCall::Write(writes))) =
            write.next(IoResult::Values(vec![Some(value), None]))
        else {
            panic!("expected one batch write");
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
        (write, stored.description, ops)
    }

    #[test]
    fn write_signs_description() {
        // The signed descriptor carries the realm description the write stores with it.
        let actor = actor();
        let mut first = RealmConfigDocument::new(actor.realm_id, Vec::new(), 3);
        first.ensure_node(actor.node_id, RealmNodeKind::Management);
        let mut write = FederationWrite::new(Arc::new(request(&actor, "https://api.example.org")));
        let (written, output) = crate::realm::commit_retry::write_once(&mut write, &actor, &first);
        let settings = written.federation.clone().expect("settings written");
        assert_eq!(settings.descriptor.payload.description, written.description);
        assert_eq!(output, written);
        crate::realm::commit_retry::retries_conflicts::<FederationWrite>();
    }

    #[test]
    fn registry_publishes_soon() {
        // Only settings with a registry URL shorten the publication timer after the commit.
        let (mut write, _, _) = written_ops(None);
        let Ok(Next::Then { mut output, calls }) = write.next(IoResult::Done) else {
            panic!("expected the config with the timers");
        };
        assert_eq!(calls.len(), 1);
        let settings = output.federation.as_mut().expect("settings written");
        settings.registry_url = Some(Url::parse("https://registry.example.org").unwrap());
        assert!(FederationWrite::publication(&output).is_some());
    }

    #[test]
    fn node_skips_check() {
        // The node itself acts without a caller context and goes straight to the write.
        let mut config = request(&actor(), "https://api.example.org");
        config.auth_context = None;
        let mut operation = SetFederationOperation::new(config);
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        assert!(operation.authorized);
    }

    #[test]
    fn description_sets_realm() {
        // A changed description also replaces the replicated realm description in the same write.
        let (mut write, description, ops) = written_ops(Some("NFDI4LS"));
        assert_eq!(description, "NFDI4LS");
        assert!(matches!(
            ops.as_slice(),
            [
                AdminDocumentOperation::ConfigFederationSet { .. },
                AdminDocumentOperation::ConfigDescriptionSet { description },
            ] if description == "NFDI4LS"
        ));
        let Ok(Next::Then { output, .. }) = write.next(IoResult::Done) else {
            panic!("expected the config after the commit");
        };
        assert_eq!(output.description, "NFDI4LS");

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
        let ctx = path_context(dir.path().to_str().unwrap());
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
        let ctx = path_context(dir.path().to_str().unwrap());
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
        let ctx = path_context(dir.path().to_str().unwrap());
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
        let ctx = path_context(dir.path().to_str().unwrap());
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
        let ctx = path_context(dir.path().to_str().unwrap());
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
