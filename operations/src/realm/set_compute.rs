//! Replaces the realm compute configuration: the location links and the standing group quotas.
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
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState, CONFIG_COMPUTE_PATH};
use aruna_core::rows::{done, values};
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, reducer_state_key, stale_conflict_deletes,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::placement::compute_config::RealmComputeConfig;
use aruna_core::structs::placement::policy::document::policy_admin_path;
use aruna_core::types::{Key, KeySpace, Value};
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
pub struct SetComputeConfig {
    pub actor: Actor,
    /// The caller's own token context, so a path-restricted credential stays
    /// restricted; it is never derived from `actor`.
    pub auth_context: AuthContext,
    /// The complete compute configuration; it replaces the stored one.
    pub compute: RealmComputeConfig,
}

/// Checks the caller's config permission, then writes the change in a write transaction that
/// reruns after a lost conflict, since a background config write can win its commit.
#[derive(Debug, PartialEq)]
pub struct SetComputeOperation {
    config: Arc<SetComputeConfig>,
    authorized: bool,
}

#[derive(Debug, Error, PartialEq)]
pub enum SetComputeError {
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
    #[error("invalid compute configuration: {reason}")]
    InvalidCompute { reason: String },
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl From<IoError> for SetComputeError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "SetCompute".to_string(),
                expected,
                got,
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished | IoError::Codec(_) => {
                Self::NotFinished
            }
        }
    }
}

impl SetComputeOperation {
    pub fn new(config: SetComputeConfig) -> Self {
        Self {
            config: Arc::new(config),
            authorized: false,
        }
    }

    fn change(&self) -> IoCall {
        let config = Arc::clone(&self.config);
        let seed = self.config.actor.user_id.to_bytes();
        let build = move || ComputeWrite::new(Arc::clone(&config));
        let child = boxed_retry(build, &seed, |result| {
            Event::Child(ChildOutcome::new(result))
        });
        IoCall::Child(child)
    }
}

impl Operation for SetComputeOperation {
    type Output = RealmConfigDocument;
    type Error = SetComputeError;

    fn first(&mut self) -> Result<Next<Self::Output>, SetComputeError> {
        if self.config.auth_context.realm_id != self.config.actor.realm_id {
            return Err(SetComputeError::Unauthorized);
        }
        Ok(Next::Call(IoCall::child(CheckPermissionsOperation::new(
            CheckPermissionsConfig {
                auth_context: self.config.auth_context.clone(),
                path: policy_admin_path(self.config.actor.realm_id),
                required_permission: Permission::WRITE,
            },
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, SetComputeError> {
        if self.authorized {
            return result
                .child::<RealmConfigDocument, SetComputeError>()?
                .map(Next::Done);
        }
        match result.child::<bool, AuthorizationError>()? {
            Ok(true) => {
                self.authorized = true;
                Ok(Next::Call(self.change()))
            }
            Ok(false) => Err(SetComputeError::Unauthorized),
            Err(AuthorizationError::StorageError(error)) => Err(error.into()),
            Err(error) => {
                warn!(error = %error, "Realm compute authorization check failed");
                Err(SetComputeError::Unauthorized)
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
struct ComputeWrite {
    config: Arc<SetComputeConfig>,
    stage: Stage,
    document: Option<RealmConfigDocument>,
    deletes: Vec<(KeySpace, Key)>,
}

impl ComputeWrite {
    fn new(config: Arc<SetComputeConfig>) -> Self {
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
    ) -> Result<Next<RealmConfigDocument>, SetComputeError> {
        self.config
            .compute
            .validate()
            .map_err(|error| SetComputeError::InvalidCompute {
                reason: error.to_string(),
            })?;

        let Some(document_value) = document_value else {
            return Err(SetComputeError::ConfigMissing);
        };
        let mut document = RealmConfigDocument::from_bytes(&document_value)?;
        if !is_management(&document, self.config.actor.node_id) {
            return Err(SetComputeError::NotManagementNode);
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
            AdminDocumentOperation::ConfigComputeSet {
                compute: self.config.compute.clone(),
            },
        )?;
        // Materialized reducer state keeps local and replicated compute conflict outcomes equal.
        apply_reducer_compute(&mut document, &reducer_state);

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
    fn finish(&mut self) -> Result<Next<RealmConfigDocument>, SetComputeError> {
        let output = self.document.take().ok_or(SetComputeError::NotFinished)?;
        let calls = vec![IoCall::Effect(Box::new(schedule_drain_effect()))];
        Ok(Next::Then { output, calls })
    }
}

impl Operation for ComputeWrite {
    type Output = RealmConfigDocument;
    type Error = SetComputeError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn conflict_retries() -> u32 {
        COMMIT_ATTEMPTS as u32 - 1
    }

    fn first(&mut self) -> Result<Next<Self::Output>, SetComputeError> {
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

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, SetComputeError> {
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

/// Overlays the reducer's materialized compute configuration onto the document,
/// mirroring the replicated materialization in `net::irokle`.
fn apply_reducer_compute(document: &mut RealmConfigDocument, reducer_state: &AdminDocumentState) {
    if !reducer_state.conflicts.contains_key(CONFIG_COMPUTE_PATH)
        && let Some(compute) = reducer_state.materialized_realm_compute()
    {
        document.compute = compute;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document_repository::GetConfigOperation;
    use crate::driver::{DriverContext, drive};
    use crate::testing::path_context;
    use aruna_core::UserId;
    use aruna_core::compute::quota::ComputeQuota;
    use aruna_core::document::DocumentTarget;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::StorageEvent;
    use aruna_core::keyspaces::AUTH_KEYSPACE;
    use aruna_core::structs::identity::realm::{
        RealmAuthorizationDocument, RealmId, RealmNodeKind,
    };
    use aruna_core::structs::placement::compute_config::{GroupComputeQuota, LocationLink};
    use tempfile::tempdir;
    use ulid::Ulid;

    fn actor(realm_id: RealmId) -> Actor {
        Actor {
            node_id: iroh::SecretKey::from_bytes(&[1u8; 32]).public(),
            user_id: UserId::local(Ulid::from_bytes([1u8; 16]), realm_id),
            realm_id,
        }
    }

    fn auth(actor: &Actor) -> AuthContext {
        AuthContext {
            user_id: actor.user_id,
            realm_id: actor.realm_id,
            path_restrictions: None,
            session: None,
        }
    }

    fn compute_config(actor: &Actor, compute: RealmComputeConfig) -> SetComputeConfig {
        SetComputeConfig {
            actor: actor.clone(),
            auth_context: auth(actor),
            compute,
        }
    }

    /// The realm admin role only grants what the operation checks, so the
    /// permission sub-operation decides on stored rules like production does.
    async fn seed_realm_admin(ctx: &DriverContext, actor: &Actor) {
        let mut document = RealmAuthorizationDocument::default_realm_doc(actor.realm_id);
        for role in document.roles.values_mut() {
            role.assigned_users.insert(actor.user_id);
        }
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: AUTH_KEYSPACE.to_string(),
                key: (*actor.realm_id.as_bytes()).into(),
                value: document.to_bytes(actor).unwrap().into(),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected event: {other:?}"),
        }
    }

    fn management_config(realm_id: RealmId, actor: &Actor) -> RealmConfigDocument {
        let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        document.ensure_node(actor.node_id, RealmNodeKind::Management);
        document
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

    fn configured() -> RealmComputeConfig {
        RealmComputeConfig {
            links: vec![LocationLink {
                from: "eu-west".to_string(),
                to: "us-east".to_string(),
                bandwidth_per_sec: 125_000_000,
            }],
            group_quotas: vec![GroupComputeQuota {
                group_id: Ulid::from_bytes([7u8; 16]),
                quota: ComputeQuota {
                    max_jobs: Some(4),
                    ..ComputeQuota::default()
                },
            }],
            ..RealmComputeConfig::default()
        }
    }

    #[tokio::test]
    async fn stores_compute_config() {
        // The mutation must survive as the document a later read resolves, or
        // the planner and the quota gate would keep deciding on stale values.
        let dir = tempdir().unwrap();
        let ctx = path_context(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let actor = actor(realm_id);
        seed(&ctx, &actor, &management_config(realm_id, &actor)).await;
        seed_realm_admin(&ctx, &actor).await;

        let stored = drive(
            SetComputeOperation::new(compute_config(&actor, configured())),
            &ctx,
        )
        .await
        .expect("compute configuration stores");
        assert_eq!(stored.compute, configured());

        let reread = drive(GetConfigOperation::new(realm_id), &ctx)
            .await
            .expect("config reads");
        assert_eq!(reread.compute.links, configured().links);
        assert_eq!(
            reread.compute.effective_quota(&Ulid::from_bytes([7u8; 16])),
            Ok(ComputeQuota {
                max_jobs: Some(4),
                ..ComputeQuota::default()
            })
        );
    }

    #[tokio::test]
    async fn refuses_invalid_links() {
        // A zero bandwidth would make one transfer estimate infinite, so the
        // mutation is refused instead of clamped.
        let dir = tempdir().unwrap();
        let ctx = path_context(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let actor = actor(realm_id);
        seed(&ctx, &actor, &management_config(realm_id, &actor)).await;
        seed_realm_admin(&ctx, &actor).await;

        let invalid = RealmComputeConfig {
            links: vec![LocationLink {
                from: "eu-west".to_string(),
                to: "us-east".to_string(),
                bandwidth_per_sec: 0,
            }],
            ..RealmComputeConfig::default()
        };
        let error = drive(
            SetComputeOperation::new(compute_config(&actor, invalid)),
            &ctx,
        )
        .await
        .expect_err("a zero bandwidth link is refused");
        assert!(matches!(error, SetComputeError::InvalidCompute { .. }));
    }

    #[tokio::test]
    async fn refuses_unauthorized_caller() {
        // Nothing is written when the caller holds no realm-config write.
        let dir = tempdir().unwrap();
        let ctx = path_context(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let actor = actor(realm_id);
        seed(&ctx, &actor, &management_config(realm_id, &actor)).await;
        let mut document = RealmAuthorizationDocument::default_realm_doc(realm_id);
        document.roles.clear();
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: AUTH_KEYSPACE.to_string(),
                key: (*realm_id.as_bytes()).into(),
                value: document.to_bytes(&actor).unwrap().into(),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected event: {other:?}"),
        }

        let error = drive(
            SetComputeOperation::new(compute_config(&actor, configured())),
            &ctx,
        )
        .await
        .expect_err("an unauthorized caller is refused");
        assert_eq!(error, SetComputeError::Unauthorized);

        let reread = drive(GetConfigOperation::new(realm_id), &ctx)
            .await
            .expect("config reads");
        assert_eq!(reread.compute, RealmComputeConfig::default());
    }

    #[tokio::test]
    async fn refuses_server_node() {
        // A realm admin token still may not write the config on a server node:
        // peers reject a realm-config event whose origin is not management.
        let dir = tempdir().unwrap();
        let ctx = path_context(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([5u8; 32]);
        let actor = actor(realm_id);
        let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        document.ensure_node(actor.node_id, RealmNodeKind::Server);
        seed(&ctx, &actor, &document).await;
        seed_realm_admin(&ctx, &actor).await;

        let error = drive(
            SetComputeOperation::new(compute_config(&actor, configured())),
            &ctx,
        )
        .await
        .expect_err("a server node is refused");
        assert_eq!(error, SetComputeError::NotManagementNode);

        let reread = drive(GetConfigOperation::new(realm_id), &ctx)
            .await
            .expect("config reads");
        assert_eq!(reread.compute, RealmComputeConfig::default());
    }

    fn checked(
        allowed: Result<bool, AuthorizationError>,
    ) -> Result<Next<RealmConfigDocument>, SetComputeError> {
        let actor = actor(RealmId::from_bytes([1u8; 32]));
        let mut operation =
            SetComputeOperation::new(compute_config(&actor, RealmComputeConfig::default()));
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        operation.next(IoResult::Child(ChildOutcome::new(allowed)))
    }

    #[test]
    fn denied_is_terminal() {
        assert_eq!(checked(Ok(false)), Err(SetComputeError::Unauthorized));
    }

    #[test]
    fn capacity_not_denied() {
        // Storage exhaustion inside the check is infrastructure, not a verdict.
        let capacity = AuthorizationError::StorageError(StorageError::CleanupCapacity);
        assert_eq!(
            checked(Err(capacity)),
            Err(SetComputeError::StorageError(StorageError::CleanupCapacity))
        );
    }

    #[test]
    fn allowed_runs_write() {
        // The change runs as a write child that reruns from its read after a lost commit.
        assert!(matches!(
            checked(Ok(true)),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        crate::realm::commit_retry::retries_conflicts::<ComputeWrite>();
    }

    #[test]
    fn write_stores_compute() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let actor = actor(realm_id);
        let config = management_config(realm_id, &actor);
        let mut write = ComputeWrite::new(Arc::new(compute_config(&actor, configured())));
        let (written, output) = crate::realm::commit_retry::write_once(&mut write, &actor, &config);
        assert_eq!(written.compute, configured());
        assert_eq!(output, written);
    }
}
