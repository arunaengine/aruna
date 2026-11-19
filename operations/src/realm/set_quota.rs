//! Sets the realm quota in the realm config document and queues that document for sync.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::Event;
use aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode, boxed_retry};
use aruna_core::reducer::{
    AdminDocumentError, AdminDocumentState, CONFIG_QUOTA_PATH, CONFIG_REPLICAS_PATH,
};
use aruna_core::rows::{done, values};
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, reducer_state_key, stale_conflict_deletes,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::{QuotaConfig, RealmConfigDocument, ReplicaQuota};
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
pub struct SetQuotaConfig {
    pub actor: Actor,
    /// The caller's own token context, so a path-restricted credential stays
    /// restricted; it is never derived from `actor`.
    pub auth_context: AuthContext,
    pub quota: QuotaConfig,
    /// Whether replicas count toward group quotas, published as its own operation.
    pub replicas: ReplicaQuota,
}

/// Checks the caller's config permission, then writes the change in a write transaction that
/// reruns after a lost conflict, since a background config write can win its commit.
#[derive(Debug, PartialEq)]
pub struct SetQuotaOperation {
    config: Arc<SetQuotaConfig>,
    authorized: bool,
}

#[derive(Debug, Error, PartialEq)]
pub enum SetQuotaError {
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
    #[error("invalid quota configuration: {reason}")]
    InvalidQuota { reason: String },
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl From<IoError> for SetQuotaError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "SetQuota".to_string(),
                expected,
                got,
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished | IoError::Codec(_) => {
                Self::NotFinished
            }
        }
    }
}

impl SetQuotaOperation {
    pub fn new(config: SetQuotaConfig) -> Self {
        Self {
            config: Arc::new(config),
            authorized: false,
        }
    }

    fn change(&self) -> IoCall {
        let config = Arc::clone(&self.config);
        let seed = self.config.actor.user_id.to_bytes();
        let build = move || QuotaWrite::new(Arc::clone(&config));
        let child = boxed_retry(build, &seed, |result| {
            Event::Child(ChildOutcome::new(result))
        });
        IoCall::Child(child)
    }
}

impl Operation for SetQuotaOperation {
    type Output = RealmConfigDocument;
    type Error = SetQuotaError;

    fn first(&mut self) -> Result<Next<Self::Output>, SetQuotaError> {
        if self.config.auth_context.realm_id != self.config.actor.realm_id {
            return Err(SetQuotaError::Unauthorized);
        }
        Ok(Next::Call(IoCall::child(CheckPermissionsOperation::new(
            CheckPermissionsConfig {
                auth_context: self.config.auth_context.clone(),
                path: policy_admin_path(self.config.actor.realm_id),
                required_permission: Permission::WRITE,
            },
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, SetQuotaError> {
        if self.authorized {
            return result
                .child::<RealmConfigDocument, SetQuotaError>()?
                .map(Next::Done);
        }
        match result.child::<bool, AuthorizationError>()? {
            Ok(true) => {
                self.authorized = true;
                Ok(Next::Call(self.change()))
            }
            Ok(false) => Err(SetQuotaError::Unauthorized),
            Err(AuthorizationError::StorageError(error)) => Err(error.into()),
            Err(error) => {
                warn!(error = %error, "Realm quota authorization check failed");
                Err(SetQuotaError::Unauthorized)
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
struct QuotaWrite {
    config: Arc<SetQuotaConfig>,
    stage: Stage,
    document: Option<RealmConfigDocument>,
    deletes: Vec<(KeySpace, Key)>,
}

impl QuotaWrite {
    fn new(config: Arc<SetQuotaConfig>) -> Self {
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
    ) -> Result<Next<RealmConfigDocument>, SetQuotaError> {
        validate_quota(&self.config.quota)?;

        let Some(document_value) = document_value else {
            return Err(SetQuotaError::ConfigMissing);
        };
        let mut document = RealmConfigDocument::from_bytes(&document_value)?;
        if !is_management(&document, self.config.actor.node_id) {
            return Err(SetQuotaError::NotManagementNode);
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
            AdminDocumentOperation::ConfigQuotaSet {
                quota: self.config.quota.clone(),
            },
        )?;
        let replicas_event = reducer_state.apply_operation(
            &self.config.actor,
            AdminDocumentOperation::ConfigReplicasSet {
                replicas: self.config.replicas.clone(),
            },
        )?;
        // Materialized reducer state keeps local and replicated quota conflict outcomes equal.
        apply_reducer_quota(&mut document, &reducer_state);

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
        for event in [admin_event, replicas_event] {
            let record = new_identified_record(
                event.event_id,
                self.config.actor.node_id,
                document_target.clone(),
                Vec::new(),
                DocumentOutboxEvent::admin(event),
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

    /// A failed drain timer leaves the durable outbox retryable.
    fn finish(&mut self) -> Result<Next<RealmConfigDocument>, SetQuotaError> {
        let output = self.document.take().ok_or(SetQuotaError::NotFinished)?;
        let calls = vec![IoCall::Effect(Box::new(schedule_drain_effect()))];
        Ok(Next::Then { output, calls })
    }
}

impl Operation for QuotaWrite {
    type Output = RealmConfigDocument;
    type Error = SetQuotaError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn conflict_retries() -> u32 {
        COMMIT_ATTEMPTS as u32 - 1
    }

    fn first(&mut self) -> Result<Next<Self::Output>, SetQuotaError> {
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

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, SetQuotaError> {
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

/// Overlays the reducer's materialized quota onto the config document, mirroring
/// `net::irokle`: only a non-conflicted materialized value updates the stored quota,
/// so a conflicted path leaves the last agreed quota untouched.
fn apply_reducer_quota(document: &mut RealmConfigDocument, reducer_state: &AdminDocumentState) {
    if !reducer_state.conflicts.contains_key(CONFIG_QUOTA_PATH)
        && let Some(quota) = reducer_state.materialized_realm_quota()
    {
        document.quota = quota;
    }
    if !reducer_state.conflicts.contains_key(CONFIG_REPLICAS_PATH)
        && let Some(replicas) = reducer_state.materialized_realm_replicas()
    {
        document.replica_quota = replicas;
    }
}

fn validate_quota(quota: &QuotaConfig) -> Result<(), SetQuotaError> {
    if !(1..=100).contains(&quota.warn_threshold_percent) {
        return Err(SetQuotaError::InvalidQuota {
            reason: format!(
                "warn_threshold_percent must be between 1 and 100, got {}",
                quota.warn_threshold_percent
            ),
        });
    }
    if quota.grace_factor_percent < 100 {
        return Err(SetQuotaError::InvalidQuota {
            reason: format!(
                "grace_factor_percent must be at least 100, got {}",
                quota.grace_factor_percent
            ),
        });
    }
    if quota.devices_per_user == Some(0) {
        return Err(SetQuotaError::InvalidQuota {
            reason: "max_devices_per_user must be greater than zero".to_string(),
        });
    }
    // Zero would silence every enrolled device; unset is how a realm stays
    // uncapped, exactly as for the device cap itself.
    if quota.device_request_rate == Some(0) {
        return Err(SetQuotaError::InvalidQuota {
            reason: "device_requests_per_minute must be greater than zero".to_string(),
        });
    }
    if quota.device_concurrent_pulls == Some(0) {
        return Err(SetQuotaError::InvalidQuota {
            reason: "device_concurrent_pulls must be greater than zero".to_string(),
        });
    }
    let mut seen_groups = BTreeSet::new();
    for over in &quota.group_overrides {
        if !seen_groups.insert(over.group_id) {
            return Err(SetQuotaError::InvalidQuota {
                reason: format!("duplicate group override for group {}", over.group_id),
            });
        }
        if let Some(grace_factor_percent) = over.grace_factor_percent {
            if grace_factor_percent < 100 {
                return Err(SetQuotaError::InvalidQuota {
                    reason: format!(
                        "group override grace_factor_percent must be at least 100, got {grace_factor_percent}"
                    ),
                });
            }
            if over.quota_bytes.is_none() {
                return Err(SetQuotaError::InvalidQuota {
                    reason: format!(
                        "group override for group {} sets grace_factor_percent without quota_bytes; grace is incoherent on an unlimited quota",
                        over.group_id
                    ),
                });
            }
        }
    }
    let mut seen_users = BTreeSet::new();
    for over in &quota.group_cap_overrides {
        if !seen_users.insert(over.user_id) {
            return Err(SetQuotaError::InvalidQuota {
                reason: format!("duplicate user cap override for user {}", over.user_id),
            });
        }
    }
    Ok(())
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
        GroupQuotaOverride, RealmAuthorizationDocument, RealmConfigDocument, RealmId,
        RealmNodeKind, UserCapOverride,
    };
    use tempfile::tempdir;
    use ulid::Ulid;

    fn actor(seed: u8, realm_id: RealmId) -> Actor {
        Actor {
            node_id: iroh::SecretKey::from_bytes(&[seed; 32]).public(),
            user_id: UserId::local(Ulid::from_bytes([seed; 16]), realm_id),
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

    fn quota_config(actor: &Actor, quota: QuotaConfig) -> SetQuotaConfig {
        SetQuotaConfig {
            actor: actor.clone(),
            auth_context: auth(actor),
            quota,
            replicas: ReplicaQuota::default(),
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

    async fn seed_config(ctx: &DriverContext, actor: &Actor, document: &RealmConfigDocument) {
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

    fn custom_quota() -> QuotaConfig {
        QuotaConfig {
            default_quota_bytes: Some(1_000_000),
            grace_factor_percent: 120,
            warn_threshold_percent: 90,
            group_overrides: vec![GroupQuotaOverride {
                group_id: Ulid::from_bytes([7; 16]),
                quota_bytes: Some(2_000_000),
                grace_factor_percent: Some(150),
            }],
            groups_per_user: Some(5),
            group_cap_overrides: vec![UserCapOverride {
                user_id: UserId::local(Ulid::from_bytes([8; 16]), RealmId::from_bytes([1; 32])),
                max_groups: Some(10),
            }],
            devices_per_user: Some(4),
            device_request_rate: Some(600),
            device_concurrent_pulls: Some(8),
        }
    }

    #[tokio::test]
    async fn quota_round_trips() {
        let dir = tempdir().unwrap();
        let ctx = path_context(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([1; 32]);
        let actor = actor(1, realm_id);
        let document = management_config(realm_id, &actor);
        seed_config(&ctx, &actor, &document).await;
        seed_realm_admin(&ctx, &actor).await;

        let quota = custom_quota();
        let replicas = ReplicaQuota {
            count: true,
            groups: [(Ulid::from_bytes([7; 16]), false)].into(),
        };
        let config = SetQuotaConfig {
            replicas: replicas.clone(),
            ..quota_config(&actor, quota.clone())
        };
        let stored = drive(SetQuotaOperation::new(config), &ctx).await.unwrap();
        assert_eq!(stored.quota, quota);
        assert_eq!(stored.replica_quota, replicas);

        let reread = drive(GetConfigOperation::new(realm_id), &ctx)
            .await
            .unwrap();
        assert_eq!(reread.quota, quota);
        assert_eq!(reread.replica_quota, replicas);
        assert_eq!(reread.quota.devices_per_user, Some(4));
        assert_eq!(reread.quota.device_request_rate, Some(600));
        assert_eq!(reread.quota.device_concurrent_pulls, Some(8));
    }

    #[tokio::test]
    async fn rejects_zero_limits() {
        // Zero would silence every enrolled device; unset is the uncapped value.
        let dir = tempdir().unwrap();
        let ctx = path_context(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([1; 32]);
        let actor = actor(1, realm_id);
        let document = management_config(realm_id, &actor);
        seed_config(&ctx, &actor, &document).await;
        seed_realm_admin(&ctx, &actor).await;

        for quota in [
            QuotaConfig {
                device_request_rate: Some(0),
                ..Default::default()
            },
            QuotaConfig {
                device_concurrent_pulls: Some(0),
                ..Default::default()
            },
        ] {
            let error = drive(SetQuotaOperation::new(quota_config(&actor, quota)), &ctx)
                .await
                .unwrap_err();

            assert!(matches!(
                error,
                SetQuotaError::InvalidQuota { reason }
                    if reason.starts_with("device_")
            ));
        }
    }

    #[tokio::test]
    async fn rejects_zero_cap() {
        // A zero `devices_per_user` would enroll nobody; unset means no cap.
        let dir = tempdir().unwrap();
        let ctx = path_context(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([1; 32]);
        let actor = actor(1, realm_id);
        let document = management_config(realm_id, &actor);
        seed_config(&ctx, &actor, &document).await;
        seed_realm_admin(&ctx, &actor).await;

        let quota = QuotaConfig {
            devices_per_user: Some(0),
            ..Default::default()
        };

        let error = drive(SetQuotaOperation::new(quota_config(&actor, quota)), &ctx)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            SetQuotaError::InvalidQuota { reason }
                if reason.contains("max_devices_per_user")
        ));
    }

    #[tokio::test]
    async fn missing_config_fails() {
        let dir = tempdir().unwrap();
        let ctx = path_context(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([2; 32]);
        let actor = actor(2, realm_id);
        seed_realm_admin(&ctx, &actor).await;

        let error = drive(
            SetQuotaOperation::new(quota_config(&actor, QuotaConfig::default())),
            &ctx,
        )
        .await
        .unwrap_err();
        assert_eq!(error, SetQuotaError::ConfigMissing);
    }

    #[tokio::test]
    async fn refuses_server_node() {
        // A realm admin token still may not write the config on a server node.
        let dir = tempdir().unwrap();
        let ctx = path_context(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([5; 32]);
        let actor = actor(5, realm_id);
        let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        document.ensure_node(actor.node_id, RealmNodeKind::Server);
        seed_config(&ctx, &actor, &document).await;
        seed_realm_admin(&ctx, &actor).await;

        let error = drive(
            SetQuotaOperation::new(quota_config(&actor, custom_quota())),
            &ctx,
        )
        .await
        .unwrap_err();
        assert_eq!(error, SetQuotaError::NotManagementNode);
    }

    fn checked(
        allowed: Result<bool, AuthorizationError>,
    ) -> Result<Next<RealmConfigDocument>, SetQuotaError> {
        let actor = actor(1, RealmId::from_bytes([1; 32]));
        let mut operation = SetQuotaOperation::new(quota_config(&actor, custom_quota()));
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        operation.next(IoResult::Child(ChildOutcome::new(allowed)))
    }

    #[test]
    fn denied_is_terminal() {
        assert_eq!(checked(Ok(false)), Err(SetQuotaError::Unauthorized));
    }

    #[test]
    fn capacity_not_denied() {
        // Storage exhaustion inside the check is infrastructure, not a verdict.
        let capacity = AuthorizationError::StorageError(StorageError::CleanupCapacity);
        assert_eq!(
            checked(Err(capacity)),
            Err(SetQuotaError::StorageError(StorageError::CleanupCapacity))
        );
    }

    #[test]
    fn allowed_runs_write() {
        // The change runs as a write child that reruns from its read after a lost commit.
        assert!(matches!(
            checked(Ok(true)),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        crate::realm::commit_retry::retries_conflicts::<QuotaWrite>();
    }

    #[test]
    fn write_keeps_newer() {
        // The write builds on the config and admin state it reads, so what another writer
        // committed before survives, and the config comes out only after the commit.
        let realm_id = RealmId::from_bytes([1; 32]);
        let owner = actor(1, realm_id);
        let other = actor(9, realm_id);
        let mut newer = management_config(realm_id, &owner);
        newer.description = "changed elsewhere".to_string();
        newer.ensure_node(other.node_id, RealmNodeKind::Server);
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let mut reducer = AdminDocumentState::new(target.clone());
        let prior = reducer
            .apply_operation(
                &other,
                AdminDocumentOperation::ConfigDescriptionSet {
                    description: "changed elsewhere".to_string(),
                },
            )
            .unwrap();
        let (_, _, reducer_value) = reducer_state_entry(&reducer).unwrap();
        let document_target = DocumentTarget::RealmConfig { realm_id };
        let document_key = (
            document_target.storage_keyspace().to_string(),
            document_target.storage_key(),
        );
        let reducer_key = (
            DOCUMENT_STATE_KEYSPACE.to_string(),
            reducer_state_key(&target),
        );

        let mut write = QuotaWrite::new(Arc::new(quota_config(&owner, custom_quota())));
        assert_eq!(
            write.first(),
            Ok(Next::Call(IoCall::Read(vec![
                document_key.clone(),
                reducer_key.clone()
            ])))
        );
        let read = IoResult::Values(vec![
            Some(newer.to_bytes(&owner).unwrap().into()),
            Some(reducer_value),
        ]);
        let Ok(Next::Call(IoCall::Write(writes))) = write.next(read) else {
            panic!("the write stores its rows in one batch");
        };
        let value_of = |(space, key): &(String, Key)| {
            writes
                .iter()
                .find(|(write_space, write_key, _)| write_space == space && write_key == key)
                .map(|(_, _, value)| value.clone())
                .expect("the retry rewrites this row")
        };
        let written = RealmConfigDocument::from_bytes(&value_of(&document_key)).unwrap();
        assert_eq!(written.description, "changed elsewhere");
        assert_eq!(
            crate::realm::mutate_placement::node_kind(&written, other.node_id),
            Some(RealmNodeKind::Server)
        );
        assert_eq!(written.quota, custom_quota());
        let state = aruna_core::reducer::decode_reducer_state(&value_of(&reducer_key)).unwrap();
        assert!(state.applied_event_ids.contains(&prior.event_id));
        assert_eq!(state.applied_event_ids.len(), 3);

        let Ok(Next::Then { output, .. }) = write.next(IoResult::Done) else {
            panic!("expected the config with the drain wake");
        };
        assert_eq!(output.description, "changed elsewhere");
    }

    #[test]
    fn foreign_token_denied() {
        let realm_id = RealmId::from_bytes([1; 32]);
        let actor = actor(1, realm_id);
        let mut config = quota_config(&actor, custom_quota());
        config.auth_context.realm_id = RealmId::from_bytes([2; 32]);
        let mut operation = SetQuotaOperation::new(config);
        assert_eq!(operation.first(), Err(SetQuotaError::Unauthorized));
    }

    #[test]
    fn rejects_warn_threshold() {
        let too_low = QuotaConfig {
            warn_threshold_percent: 0,
            ..QuotaConfig::default()
        };
        assert!(matches!(
            validate_quota(&too_low),
            Err(SetQuotaError::InvalidQuota { .. })
        ));
        let too_high = QuotaConfig {
            warn_threshold_percent: 101,
            ..QuotaConfig::default()
        };
        assert!(matches!(
            validate_quota(&too_high),
            Err(SetQuotaError::InvalidQuota { .. })
        ));
    }

    #[test]
    fn rejects_low_grace() {
        let quota = QuotaConfig {
            grace_factor_percent: 99,
            ..QuotaConfig::default()
        };
        assert!(matches!(
            validate_quota(&quota),
            Err(SetQuotaError::InvalidQuota { .. })
        ));
    }

    #[test]
    fn rejects_duplicate_group() {
        let mut quota = QuotaConfig::default();
        let group_id = Ulid::from_bytes([3; 16]);
        quota.group_overrides = vec![
            GroupQuotaOverride {
                group_id,
                quota_bytes: Some(1),
                grace_factor_percent: None,
            },
            GroupQuotaOverride {
                group_id,
                quota_bytes: Some(2),
                grace_factor_percent: None,
            },
        ];
        assert!(matches!(
            validate_quota(&quota),
            Err(SetQuotaError::InvalidQuota { .. })
        ));
    }

    #[test]
    fn rejects_duplicate_user() {
        let mut quota = QuotaConfig::default();
        let user_id = UserId::local(Ulid::from_bytes([4; 16]), RealmId::from_bytes([1; 32]));
        quota.group_cap_overrides = vec![
            UserCapOverride {
                user_id,
                max_groups: Some(1),
            },
            UserCapOverride {
                user_id,
                max_groups: Some(2),
            },
        ];
        assert!(matches!(
            validate_quota(&quota),
            Err(SetQuotaError::InvalidQuota { .. })
        ));
    }

    #[test]
    fn rejects_override_grace() {
        let quota = QuotaConfig {
            group_overrides: vec![GroupQuotaOverride {
                group_id: Ulid::from_bytes([5; 16]),
                quota_bytes: Some(1),
                grace_factor_percent: Some(99),
            }],
            ..QuotaConfig::default()
        };
        assert!(matches!(
            validate_quota(&quota),
            Err(SetQuotaError::InvalidQuota { .. })
        ));
    }

    #[test]
    fn accepts_override_grace() {
        let quota = QuotaConfig {
            group_overrides: vec![GroupQuotaOverride {
                group_id: Ulid::from_bytes([5; 16]),
                quota_bytes: Some(1),
                grace_factor_percent: Some(100),
            }],
            ..QuotaConfig::default()
        };
        assert!(validate_quota(&quota).is_ok());
    }

    #[test]
    fn rejects_unlimited_grace() {
        let quota = QuotaConfig {
            group_overrides: vec![GroupQuotaOverride {
                group_id: Ulid::from_bytes([6; 16]),
                quota_bytes: None,
                grace_factor_percent: Some(110),
            }],
            ..QuotaConfig::default()
        };
        assert!(matches!(
            validate_quota(&quota),
            Err(SetQuotaError::InvalidQuota { .. })
        ));
    }

    #[test]
    fn accepts_default_quota() {
        assert!(validate_quota(&QuotaConfig::default()).is_ok());
    }

    fn quota_conflict_state(
        realm_id: RealmId,
        quota_a: QuotaConfig,
        quota_b: QuotaConfig,
    ) -> AdminDocumentState {
        use aruna_core::admin_documents::{
            AdminDocumentClock, AdminDocumentEvent, AdminDocumentTarget,
        };

        fn node(seed: u8) -> aruna_core::NodeId {
            iroh::SecretKey::from_bytes(&[seed; 32]).public()
        }

        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let mut state = AdminDocumentState::new(target.clone());
        let quota_event =
            |seed: u8, origin: aruna_core::NodeId, quota: QuotaConfig| AdminDocumentEvent {
                event_id: Ulid::from_bytes([seed; 16]),
                target: target.clone(),
                origin_node_id: origin,
                origin_seq: 1,
                observed: AdminDocumentClock::default(),
                actor: actor(seed, realm_id),
                op: AdminDocumentOperation::ConfigQuotaSet { quota },
            };
        // Two concurrent events (neither observes the other) with divergent quota
        // values conflict on the quota path.
        state.apply(&quota_event(10, node(1), quota_a)).unwrap();
        state.apply(&quota_event(11, node(2), quota_b)).unwrap();
        state
    }

    #[test]
    fn conflicted_quota_skipped() {
        let realm_id = RealmId::from_bytes([9; 32]);
        let quota_a = QuotaConfig {
            default_quota_bytes: Some(1_000),
            ..QuotaConfig::default()
        };
        let quota_b = QuotaConfig {
            default_quota_bytes: Some(2_000),
            ..QuotaConfig::default()
        };
        let state = quota_conflict_state(realm_id, quota_a, quota_b);
        assert!(state.conflicts.contains_key(CONFIG_QUOTA_PATH));
        assert!(state.materialized_realm_quota().is_none());

        let original = QuotaConfig {
            default_quota_bytes: Some(42),
            ..QuotaConfig::default()
        };
        let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        document.quota = original.clone();
        apply_reducer_quota(&mut document, &state);
        assert_eq!(
            document.quota, original,
            "conflicted quota path must not clobber the stored quota"
        );
    }

    #[test]
    fn materialized_quota_applied() {
        use aruna_core::admin_documents::{
            AdminDocumentClock, AdminDocumentEvent, AdminDocumentTarget,
        };

        let realm_id = RealmId::from_bytes([9; 32]);
        let quota = QuotaConfig {
            default_quota_bytes: Some(1_000),
            ..QuotaConfig::default()
        };
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let mut state = AdminDocumentState::new(target.clone());
        state
            .apply(&AdminDocumentEvent {
                event_id: Ulid::from_bytes([10; 16]),
                target,
                origin_node_id: iroh::SecretKey::from_bytes(&[1; 32]).public(),
                origin_seq: 1,
                observed: AdminDocumentClock::default(),
                actor: actor(1, realm_id),
                op: AdminDocumentOperation::ConfigQuotaSet {
                    quota: quota.clone(),
                },
            })
            .unwrap();

        let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        apply_reducer_quota(&mut document, &state);
        assert_eq!(document.quota, quota);
    }
}
