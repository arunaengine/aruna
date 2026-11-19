//! Replaces the realm default on/off state of optional portal features.
//! It writes through the shared admin document path, so concurrent changes converge.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::Event;
use aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode, boxed_retry};
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState, CONFIG_PORTAL_PATH};
use aruna_core::rows::{done, values};
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, reducer_state_key, stale_conflict_deletes,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::{RealmConfigDocument, validate_portal_features};
use aruna_core::structs::placement::policy::document::policy_admin_path;
use aruna_core::types::{Key, KeySpace, Value};
use std::collections::BTreeMap;
use thiserror::Error;
use tracing::warn;

use super::COMMIT_ATTEMPTS;
use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::realm::mutate_placement::is_management;
use crate::sync::outbox_record::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};
use aruna_core::placement::target_placement_ref;

use std::sync::Arc;
#[derive(Debug, Clone, PartialEq)]
pub struct SetPortalConfig {
    pub actor: Actor,
    /// The caller's own token context, so a path-restricted credential stays
    /// restricted; it is never derived from `actor`.
    pub auth_context: AuthContext,
    /// The complete feature map; it replaces the stored one.
    pub features: BTreeMap<String, bool>,
}

/// Checks the caller's config permission, then writes the change in a write transaction that
/// reruns after a lost conflict, since a background config write can win its commit.
#[derive(Debug, PartialEq)]
pub struct SetPortalOperation {
    config: Arc<SetPortalConfig>,
    authorized: bool,
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
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl From<IoError> for SetPortalError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "SetPortal".to_string(),
                expected,
                got,
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished | IoError::Codec(_) => {
                Self::NotFinished
            }
        }
    }
}

impl SetPortalOperation {
    pub fn new(config: SetPortalConfig) -> Self {
        Self {
            config: Arc::new(config),
            authorized: false,
        }
    }

    fn change(&self) -> IoCall {
        let config = Arc::clone(&self.config);
        let seed = self.config.actor.user_id.to_bytes();
        let build = move || PortalWrite::new(Arc::clone(&config));
        let child = boxed_retry(build, &seed, |result| {
            Event::Child(ChildOutcome::new(result))
        });
        IoCall::Child(child)
    }
}

impl Operation for SetPortalOperation {
    type Output = RealmConfigDocument;
    type Error = SetPortalError;

    fn first(&mut self) -> Result<Next<Self::Output>, SetPortalError> {
        if self.config.auth_context.realm_id != self.config.actor.realm_id {
            return Err(SetPortalError::Unauthorized);
        }
        Ok(Next::Call(IoCall::child(CheckPermissionsOperation::new(
            CheckPermissionsConfig {
                auth_context: self.config.auth_context.clone(),
                path: policy_admin_path(self.config.actor.realm_id),
                required_permission: Permission::WRITE,
            },
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, SetPortalError> {
        if self.authorized {
            return result
                .child::<RealmConfigDocument, SetPortalError>()?
                .map(Next::Done);
        }
        match result.child::<bool, AuthorizationError>()? {
            Ok(true) => {
                self.authorized = true;
                Ok(Next::Call(self.change()))
            }
            Ok(false) => Err(SetPortalError::Unauthorized),
            Err(AuthorizationError::StorageError(error)) => Err(error.into()),
            Err(error) => {
                warn!(error = %error, "Realm portal features authorization check failed");
                Err(SetPortalError::Unauthorized)
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
struct PortalWrite {
    config: Arc<SetPortalConfig>,
    stage: Stage,
    document: Option<RealmConfigDocument>,
    deletes: Vec<(KeySpace, Key)>,
}

impl PortalWrite {
    fn new(config: Arc<SetPortalConfig>) -> Self {
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
    ) -> Result<Next<RealmConfigDocument>, SetPortalError> {
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

        self.document = Some(document.clone());
        self.deletes = stale_conflict_deletes;
        self.stage = Stage::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    /// A failed drain timer leaves the durable outbox retryable.
    fn finish(&mut self) -> Result<Next<RealmConfigDocument>, SetPortalError> {
        let output = self.document.take().ok_or(SetPortalError::NotFinished)?;
        let calls = vec![IoCall::Effect(Box::new(schedule_drain_effect()))];
        Ok(Next::Then { output, calls })
    }
}

impl Operation for PortalWrite {
    type Output = RealmConfigDocument;
    type Error = SetPortalError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn conflict_retries() -> u32 {
        COMMIT_ATTEMPTS as u32 - 1
    }

    fn first(&mut self) -> Result<Next<Self::Output>, SetPortalError> {
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

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, SetPortalError> {
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
    use crate::document_repository::GetConfigOperation;
    use crate::driver::{DriverContext, drive};
    use crate::testing::path_context;
    use aruna_core::UserId;
    use aruna_core::document::DocumentTarget;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::StorageEvent;
    use aruna_core::keyspaces::AUTH_KEYSPACE;
    use aruna_core::structs::identity::realm::{
        RealmAuthorizationDocument, RealmId, RealmNodeKind,
    };
    use tempfile::tempdir;
    use ulid::Ulid;

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
        let ctx = path_context(dir.path().to_str().unwrap());
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
        let ctx = path_context(dir.path().to_str().unwrap());
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

    fn checked(allowed: bool) -> Result<Next<RealmConfigDocument>, SetPortalError> {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let mut operation = SetPortalOperation::new(portal_config(&actor(realm_id), features()));
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        operation.next(IoResult::Child(ChildOutcome::new(Ok::<
            _,
            AuthorizationError,
        >(allowed))))
    }

    #[test]
    fn allowed_runs_write() {
        // The change runs as a write child only after the permission check allowed it.
        assert!(matches!(checked(true), Ok(Next::Call(IoCall::Child(_)))));
        assert_eq!(checked(false), Err(SetPortalError::Unauthorized));
    }

    #[test]
    fn write_stores_features() {
        // The features land in the written config, which is the output after the commit.
        let actor = actor(RealmId::from_bytes([1u8; 32]));
        let mut config = RealmConfigDocument::new(actor.realm_id, Vec::new(), 3);
        config.ensure_node(actor.node_id, RealmNodeKind::Management);
        let mut write = PortalWrite::new(Arc::new(portal_config(&actor, features())));
        let (written, output) = crate::realm::commit_retry::write_once(&mut write, &actor, &config);
        assert_eq!(written.portal_features, features());
        assert_eq!(output, written);
        crate::realm::commit_retry::retries_conflicts::<PortalWrite>();
    }
}
