//! Adds, replaces or removes one realm OIDC provider entry and keeps the list order.
//! It writes through the shared admin document path, so the change reaches every node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::Event;
use aruna_core::federation::valid_federation_url;
use aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode, boxed_retry};
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState, config_oidc_path};
use aruna_core::rows::{done, values};
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, reducer_state_key, stale_conflict_deletes,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::{OidcProviderConfig, RealmConfigDocument};
use aruna_core::structs::placement::policy::document::policy_admin_path;
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

const MAX_ID_LEN: usize = 64;

use std::sync::Arc;
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

/// Checks the caller's config permission, then writes the change in a write transaction that
/// reruns after a lost conflict, since a background config write can win its commit.
#[derive(Debug, PartialEq)]
pub struct SetProviderOperation {
    config: Arc<SetProviderConfig>,
    authorized: bool,
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
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
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

impl From<IoError> for SetProviderError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "SetProvider".to_string(),
                expected,
                got,
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished | IoError::Codec(_) => {
                Self::NotFinished
            }
        }
    }
}

impl SetProviderOperation {
    pub fn new(config: SetProviderConfig) -> Self {
        Self {
            config: Arc::new(config),
            authorized: false,
        }
    }

    fn change(&self) -> IoCall {
        let config = Arc::clone(&self.config);
        let seed = self.config.actor.user_id.to_bytes();
        let build = move || ProviderWrite::new(Arc::clone(&config));
        let child = boxed_retry(build, &seed, |result| {
            Event::Child(ChildOutcome::new(result))
        });
        IoCall::Child(child)
    }
}

impl Operation for SetProviderOperation {
    type Output = RealmConfigDocument;
    type Error = SetProviderError;

    fn first(&mut self) -> Result<Next<Self::Output>, SetProviderError> {
        if self.config.auth_context.realm_id != self.config.actor.realm_id {
            return Err(SetProviderError::Unauthorized);
        }
        let valid = match &self.config.change {
            ProviderChange::Upsert(provider) => validate_provider(provider),
            ProviderChange::Remove(id) => validate_id(id),
        };
        if let Err(reason) = valid {
            return Err(SetProviderError::InvalidProvider { reason });
        }
        Ok(Next::Call(IoCall::child(CheckPermissionsOperation::new(
            CheckPermissionsConfig {
                auth_context: self.config.auth_context.clone(),
                path: policy_admin_path(self.config.actor.realm_id),
                required_permission: Permission::WRITE,
            },
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, SetProviderError> {
        if self.authorized {
            return result
                .child::<RealmConfigDocument, SetProviderError>()?
                .map(Next::Done);
        }
        match result.child::<bool, AuthorizationError>()? {
            Ok(true) => {
                self.authorized = true;
                Ok(Next::Call(self.change()))
            }
            Ok(false) => Err(SetProviderError::Unauthorized),
            Err(AuthorizationError::StorageError(error)) => Err(error.into()),
            Err(error) => {
                warn!(error = %error, "Realm OIDC provider authorization check failed");
                Err(SetProviderError::Unauthorized)
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
struct ProviderWrite {
    config: Arc<SetProviderConfig>,
    stage: Stage,
    document: Option<RealmConfigDocument>,
    deletes: Vec<(KeySpace, Key)>,
}

impl ProviderWrite {
    fn new(config: Arc<SetProviderConfig>) -> Self {
        Self {
            config,
            stage: Stage::Read,
            document: None,
            deletes: Vec::new(),
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

    fn emit_document_write(
        &mut self,
        document_value: Option<Value>,
        reducer_state_value: Option<Value>,
    ) -> Result<Next<RealmConfigDocument>, SetProviderError> {
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

        self.document = Some(document.clone());
        self.deletes = stale_conflict_deletes;
        self.stage = Stage::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    /// A failed drain timer leaves the durable outbox retryable.
    fn finish(&mut self) -> Result<Next<RealmConfigDocument>, SetProviderError> {
        let output = self.document.take().ok_or(SetProviderError::NotFinished)?;
        let calls = vec![IoCall::Effect(Box::new(schedule_drain_effect()))];
        Ok(Next::Then { output, calls })
    }
}

impl Operation for ProviderWrite {
    type Output = RealmConfigDocument;
    type Error = SetProviderError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn conflict_retries() -> u32 {
        COMMIT_ATTEMPTS as u32 - 1
    }

    fn first(&mut self) -> Result<Next<Self::Output>, SetProviderError> {
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

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, SetProviderError> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::structs::identity::realm::{RealmId, RealmNodeKind};
    use ulid::Ulid;

    fn actor() -> Actor {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        Actor {
            node_id: iroh::SecretKey::from_bytes(&[1u8; 32]).public(),
            user_id: UserId::local(Ulid::from_bytes([1u8; 16]), realm_id),
            realm_id,
        }
    }

    fn provider(id: &str, issuer: &str) -> OidcProviderConfig {
        OidcProviderConfig {
            id: id.to_string(),
            issuer: issuer.to_string(),
            audience: "client".to_string(),
            discovery_url: format!("{issuer}/.well-known/openid-configuration"),
        }
    }

    fn operation(change: ProviderChange) -> SetProviderOperation {
        SetProviderOperation::new(config_for(change))
    }

    fn config_for(change: ProviderChange) -> SetProviderConfig {
        let actor = actor();
        SetProviderConfig {
            auth_context: AuthContext {
                user_id: actor.user_id,
                realm_id: actor.realm_id,
                path_restrictions: None,
                session: None,
            },
            actor,
            change,
        }
    }

    /// Runs the write transaction of `change` against a management node config to its
    /// document write; the error when it fails there instead.
    fn written(
        change: ProviderChange,
        providers: Vec<OidcProviderConfig>,
    ) -> Result<RealmConfigDocument, SetProviderError> {
        let actor = actor();
        let mut config = RealmConfigDocument::new(actor.realm_id, providers, 3);
        config.ensure_node(actor.node_id, RealmNodeKind::Management);
        let mut write = ProviderWrite::new(Arc::new(config_for(change)));
        write.first().unwrap();
        let read = IoResult::Values(vec![Some(config.to_bytes(&actor).unwrap().into()), None]);
        match write.next(read)? {
            Next::Call(IoCall::Write(writes)) => {
                Ok(RealmConfigDocument::from_bytes(&writes[0].2).unwrap())
            }
            _ => panic!("expected the config write"),
        }
    }

    fn ids(document: &RealmConfigDocument) -> Vec<&str> {
        document
            .oidc_providers
            .iter()
            .map(|p| p.id.as_str())
            .collect()
    }

    #[test]
    fn allowed_runs_write() {
        // The change runs as a write child only after the permission check allowed it.
        for (allowed, runs) in [(true, true), (false, false)] {
            let mut operation =
                operation(ProviderChange::Upsert(provider("ls", "https://ls.example")));
            assert!(matches!(
                operation.first(),
                Ok(Next::Call(IoCall::Child(_)))
            ));
            let checked = IoResult::Child(ChildOutcome::new(Ok::<_, AuthorizationError>(allowed)));
            let turn = operation.next(checked);
            match runs {
                true => assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_))))),
                false => assert_eq!(turn, Err(SetProviderError::Unauthorized)),
            }
        }
        crate::realm::commit_retry::retries_conflicts::<ProviderWrite>();
    }

    #[test]
    fn rejects_invalid_fields() {
        // Validation runs before any effect, so a bad entry never reaches storage.
        let mut plain_http = provider("ls", "http://ls.example");
        let mut blank_audience = provider("ls", "https://ls.example");
        blank_audience.audience = " ".to_string();
        let bad_id = provider("l s", "https://ls.example");
        plain_http.discovery_url = "https://ls.example/discovery".to_string();
        for invalid in [plain_http, blank_audience, bad_id] {
            let mut operation = operation(ProviderChange::Upsert(invalid));
            assert!(matches!(
                operation.first(),
                Err(SetProviderError::InvalidProvider { .. })
            ));
        }
        assert!(validate_provider(&provider("dev", "http://127.0.0.1:8080")).is_ok());
    }

    #[test]
    fn upsert_keeps_order() {
        let main = provider("main", "https://keycloak.example");
        let ls = provider("ls", "https://ls.example");
        let document = written(ProviderChange::Upsert(ls.clone()), vec![main.clone()]).unwrap();
        assert_eq!(ids(&document), ["main", "ls"]);

        let replaced = provider("main", "https://replaced.example");
        let change = ProviderChange::Upsert(replaced.clone());
        let document = written(change, vec![main, ls.clone()]).unwrap();
        assert_eq!(document.oidc_providers, vec![replaced, ls]);
    }

    #[test]
    fn upsert_completes() {
        // The written config is the output, and it comes only with the drain wake after the commit.
        let actor = actor();
        let main = provider("main", "https://keycloak.example");
        let mut config = RealmConfigDocument::new(actor.realm_id, vec![main], 3);
        config.ensure_node(actor.node_id, RealmNodeKind::Management);
        let change = ProviderChange::Upsert(provider("ls", "https://ls.example"));
        let mut write = ProviderWrite::new(Arc::new(config_for(change)));
        let (written, output) = crate::realm::commit_retry::write_once(&mut write, &actor, &config);
        assert_eq!(ids(&output), ["main", "ls"]);
        assert_eq!(output, written);
    }

    #[test]
    fn remove_guards_last() {
        let main = provider("main", "https://keycloak.example");
        let ls = provider("ls", "https://ls.example");
        let remove = ProviderChange::Remove("ls".to_string());
        let document = written(remove, vec![main.clone(), ls]).unwrap();
        assert_eq!(ids(&document), ["main"]);

        let last = ProviderChange::Remove("main".to_string());
        assert_eq!(
            written(last, vec![main.clone()]),
            Err(SetProviderError::LastProvider)
        );
        let unknown = ProviderChange::Remove("ls".to_string());
        assert_eq!(
            written(unknown, vec![main]),
            Err(SetProviderError::UnknownProvider)
        );
    }
}
