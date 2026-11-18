//! Replaces the realm's deny-only request policy set, optionally guarded by its expected hash.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::Event;
use aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode, boxed_retry};
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState, CONFIG_POLICIES_PATH};
use aruna_core::request_policy::{RequestPolicy, policy_set_hash, validate_policy_set};
use aruna_core::rows::done;
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, reducer_state_key, stale_conflict_deletes,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::placement::policy::document::policy_admin_path;
use aruna_core::types::{Key, KeySpace, Value};
use thiserror::Error;
use tracing::warn;

use super::COMMIT_ATTEMPTS;
use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::auth::epoch_due::DueMarks;
use crate::realm::mutate_placement::is_management;
use crate::sync::outbox_record::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};
use aruna_core::placement::target_placement_ref;

#[derive(Debug, Clone, PartialEq)]
pub struct SetPoliciesConfig {
    pub actor: Actor,
    /// The caller's own token context, so a path-restricted credential stays
    /// restricted; it is never derived from `actor`.
    pub auth_context: AuthContext,
    pub policies: Vec<RequestPolicy>,
    /// When set, the write applies only if the stored set still hashes to it,
    /// compared inside the write transaction to close the check/use window.
    pub expected_hash: Option<[u8; 32]>,
}

/// Replaces the realm's deny-only request policy set through the admin document machinery, so it
/// replicates realm-wide and merges last-writer-wins as one value. The write reruns after a lost
/// conflict, since a background config write can win its commit.
#[derive(Debug, PartialEq)]
pub struct SetPoliciesOperation {
    config: SetPoliciesConfig,
    authorized: bool,
}

#[derive(Debug, Error, PartialEq)]
pub enum SetPoliciesError {
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
    #[error("stored policy set changed")]
    StaleHash,
    #[error("invalid policy set: {reason}")]
    InvalidPolicies { reason: String },
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl From<IoError> for SetPoliciesError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "SetPolicies".to_string(),
                expected,
                got,
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished | IoError::Codec(_) => {
                Self::NotFinished
            }
        }
    }
}

impl SetPoliciesOperation {
    pub fn new(config: SetPoliciesConfig) -> Self {
        Self {
            config,
            authorized: false,
        }
    }

    fn change(&self) -> IoCall {
        let config = self.config.clone();
        let seed = self.config.actor.user_id.to_bytes();
        let build = move || PoliciesWrite::new(config.clone());
        let child = boxed_retry(build, &seed, |result| {
            Event::Child(ChildOutcome::new(result))
        });
        IoCall::Child(child)
    }
}

impl Operation for SetPoliciesOperation {
    type Output = RealmConfigDocument;
    type Error = SetPoliciesError;

    fn first(&mut self) -> Result<Next<RealmConfigDocument>, SetPoliciesError> {
        if self.config.auth_context.realm_id != self.config.actor.realm_id {
            return Err(SetPoliciesError::Unauthorized);
        }
        Ok(Next::Call(IoCall::child(CheckPermissionsOperation::new(
            CheckPermissionsConfig {
                auth_context: self.config.auth_context.clone(),
                path: policy_admin_path(self.config.actor.realm_id),
                required_permission: Permission::WRITE,
            },
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<RealmConfigDocument>, SetPoliciesError> {
        if self.authorized {
            return result
                .child::<RealmConfigDocument, SetPoliciesError>()?
                .map(Next::Done);
        }
        match result.child::<bool, AuthorizationError>()? {
            Ok(true) => {
                self.authorized = true;
                Ok(Next::Call(self.change()))
            }
            Ok(false) => Err(SetPoliciesError::Unauthorized),
            Err(error) => {
                warn!(error = %error, "Realm policy authorization check failed");
                match error {
                    AuthorizationError::StorageError(error) => Err(error.into()),
                    _ => Err(SetPoliciesError::Unauthorized),
                }
            }
        }
    }
}

/// What the policy write transaction waits for.
#[derive(Debug, PartialEq)]
enum Stage {
    Read,
    Write,
    Delete,
    Marks,
}

/// Reads, validates and stores the policy set in one write transaction, and marks key epochs due
/// in it, since a new deny can narrow access.
#[derive(Debug, PartialEq)]
struct PoliciesWrite {
    config: SetPoliciesConfig,
    stage: Stage,
    document: Option<RealmConfigDocument>,
    deletes: Vec<(KeySpace, Key)>,
    marks: DueMarks,
}

impl PoliciesWrite {
    fn new(config: SetPoliciesConfig) -> Self {
        Self {
            config,
            stage: Stage::Read,
            document: None,
            deletes: Vec::new(),
            marks: DueMarks::new(None, aruna_core::time::unix_timestamp_millis()),
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

    fn emit_write_state(
        &mut self,
        document_value: Option<Value>,
        reducer_state_value: Option<Value>,
    ) -> Result<Next<RealmConfigDocument>, SetPoliciesError> {
        validate_policy_set(&self.config.policies)
            .map_err(|reason| SetPoliciesError::InvalidPolicies { reason })?;

        let Some(document_value) = document_value else {
            return Err(SetPoliciesError::ConfigMissing);
        };
        let mut document = RealmConfigDocument::from_bytes(&document_value)?;
        if !is_management(&document, self.config.actor.node_id) {
            return Err(SetPoliciesError::NotManagementNode);
        }

        if let Some(expected) = self.config.expected_hash
            && policy_set_hash(&document.request_policies) != expected
        {
            return Err(SetPoliciesError::StaleHash);
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
            AdminDocumentOperation::ConfigPoliciesSet {
                policies: self.config.policies.clone(),
            },
        )?;
        // Mirror the replicated overlay: a conflicted policies path leaves the
        // last agreed set in place instead of clobbering it with the input.
        apply_reducer_policies(&mut document, &reducer_state);

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

        self.document = Some(document);
        self.deletes = stale_conflict_deletes;
        self.stage = Stage::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    /// A failed drain timer leaves the durable outbox retryable.
    fn finish(&mut self) -> Result<Next<RealmConfigDocument>, SetPoliciesError> {
        let output = self.document.take().ok_or(SetPoliciesError::NotFinished)?;
        let calls = vec![IoCall::Effect(Box::new(schedule_drain_effect()))];
        Ok(Next::Then { output, calls })
    }
}

impl Operation for PoliciesWrite {
    type Output = RealmConfigDocument;
    type Error = SetPoliciesError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn conflict_retries() -> u32 {
        COMMIT_ATTEMPTS as u32 - 1
    }

    fn first(&mut self) -> Result<Next<RealmConfigDocument>, SetPoliciesError> {
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

    fn next(&mut self, result: IoResult) -> Result<Next<RealmConfigDocument>, SetPoliciesError> {
        match self.stage {
            Stage::Read => {
                let [document, reducer] = aruna_core::rows::values::<2>(result)?;
                self.emit_write_state(document, reducer)
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
                self.stage = Stage::Marks;
                Ok(Next::Call(self.marks.first()))
            }
            Stage::Marks => match self.marks.next(result, true) {
                Ok(Some(call)) => Ok(Next::Call(call)),
                Ok(None) => self.finish(),
                Err(error) => Err(StorageError::WriteError(error.to_string()).into()),
            },
        }
    }
}

/// Overlays the reducer's materialized policy set onto the config document,
/// mirroring the replicated materialization in `net::irokle`.
fn apply_reducer_policies(document: &mut RealmConfigDocument, reducer_state: &AdminDocumentState) {
    if !reducer_state.conflicts.contains_key(CONFIG_POLICIES_PATH)
        && let Some(policies) = reducer_state.materialized_realm_policies()
    {
        document.request_policies = policies;
    }
}

#[cfg(test)]
mod tests {
    use super::{
        COMMIT_ATTEMPTS, PoliciesWrite, SetPoliciesConfig, SetPoliciesError, SetPoliciesOperation,
    };
    use crate::document_repository::GetConfigOperation;
    use crate::driver::{DriverContext, drive};
    use crate::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use aruna_core::UserId;
    use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
    use aruna_core::document::DocumentTarget;
    use aruna_core::effects::StorageEffect;
    use aruna_core::errors::{AuthorizationError, StorageError};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::AUTH_KEYSPACE;
    use aruna_core::operation::{Operation, TransactionMode};
    use aruna_core::request_policy::RequestPolicy;
    use aruna_core::structs::identity::auth::{Actor, AuthContext};
    use aruna_core::structs::identity::realm::{
        RealmAuthorizationDocument, RealmConfigDocument, RealmId, RealmNodeKind,
    };
    use aruna_storage::storage::FjallStorage;
    use aruna_tasks::TaskHandle;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn actor(realm_id: RealmId) -> Actor {
        Actor {
            node_id: iroh::SecretKey::from_bytes(&[3u8; 32]).public(),
            user_id: UserId::local(Ulid::from_bytes([4u8; 16]), realm_id),
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

    fn policies_config(
        actor: &Actor,
        policies: Vec<RequestPolicy>,
        expected_hash: Option<[u8; 32]>,
    ) -> SetPoliciesConfig {
        SetPoliciesConfig {
            actor: actor.clone(),
            auth_context: auth(actor),
            policies,
            expected_hash,
        }
    }

    /// Realm creation leaves the admin role unassigned, so the fixture claims it
    /// for the actor the permission sub-operation then decides on.
    async fn seed_realm_admin(context: &DriverContext, actor: &Actor) {
        let mut document = RealmAuthorizationDocument::default_realm_doc(actor.realm_id);
        for role in document.roles.values_mut() {
            role.assigned_users.insert(actor.user_id);
        }
        match context
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

    fn policy(expression: &str) -> RequestPolicy {
        RequestPolicy {
            policy_id: Ulid::from_bytes([7u8; 16]),
            name: "no-writes".to_string(),
            kind: aruna_core::request_policy::PolicyKind::Deny,
            when: None,
            expression: expression.to_string(),
            enabled: true,
        }
    }

    async fn setup_realm() -> (tempfile::TempDir, DriverContext, Actor) {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
            outbound_handle: None,
        };
        let realm_id = RealmId([21u8; 32]);
        let actor = actor(realm_id);
        drive(
            CreateRealmOperation::new(CreateRealmConfig {
                actor: actor.clone(),
                realm_description: "policy realm".to_string(),
                oidc_providers: Vec::new(),
                node_location: None,
                node_weight: None,
                node_labels: Default::default(),
            }),
            &context,
        )
        .await
        .unwrap();
        seed_realm_admin(&context, &actor).await;
        (dir, context, actor)
    }

    /// Replaces the stored realm config with one that ranks the actor's node as
    /// a plain server, which realm creation never produces.
    async fn seed_server_config(context: &DriverContext, actor: &Actor) {
        let mut document = RealmConfigDocument::new(actor.realm_id, Vec::new(), 3);
        document.ensure_node(actor.node_id, RealmNodeKind::Server);
        let target = DocumentTarget::RealmConfig {
            realm_id: actor.realm_id,
        };
        match context
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

    #[tokio::test]
    async fn refuses_server_node() {
        // A realm admin token still may not write the config on a server node:
        // peers reject a realm-config event whose origin is not management.
        let (_dir, context, actor) = setup_realm().await;
        seed_server_config(&context, &actor).await;

        let error = drive(
            SetPoliciesOperation::new(policies_config(
                &actor,
                vec![policy("permission == 'write'")],
                None,
            )),
            &context,
        )
        .await
        .expect_err("a server node is refused");
        assert!(matches!(error, SetPoliciesError::NotManagementNode));

        let read = drive(GetConfigOperation::new(actor.realm_id), &context)
            .await
            .unwrap();
        assert!(read.request_policies.is_empty());
    }

    #[tokio::test]
    async fn stores_policy_set() {
        // The set lands on the stored realm config and a later read returns it.
        let (_dir, context, actor) = setup_realm().await;
        let policies = vec![policy("permission == 'write'")];
        let document = drive(
            SetPoliciesOperation::new(policies_config(&actor, policies.clone(), None)),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(document.request_policies, policies);

        let read = drive(GetConfigOperation::new(actor.realm_id), &context)
            .await
            .unwrap();
        assert_eq!(read.request_policies, policies);
    }

    #[tokio::test]
    async fn stale_hash_aborts() {
        // A concurrent write moves the stored hash; the second write must abort
        // on its stale expected_hash and leave the first set in place.
        use aruna_core::request_policy::policy_set_hash;
        let (_dir, context, actor) = setup_realm().await;
        let empty_hash = policy_set_hash(&[]);
        let first = vec![policy("permission == 'write'")];
        drive(
            SetPoliciesOperation::new(policies_config(&actor, first.clone(), Some(empty_hash))),
            &context,
        )
        .await
        .unwrap();

        let stale = drive(
            SetPoliciesOperation::new(policies_config(
                &actor,
                vec![policy("permission == 'read'")],
                Some(empty_hash),
            )),
            &context,
        )
        .await;
        assert!(matches!(stale, Err(SetPoliciesError::StaleHash)));

        let read = drive(GetConfigOperation::new(actor.realm_id), &context)
            .await
            .unwrap();
        assert_eq!(read.request_policies, first);
    }

    #[tokio::test]
    async fn rejects_invalid_set() {
        // An uncompilable expression is refused at administration time.
        let (_dir, context, actor) = setup_realm().await;
        let result = drive(
            SetPoliciesOperation::new(policies_config(
                &actor,
                vec![policy("path.startsWith(")],
                None,
            )),
            &context,
        )
        .await;
        assert!(matches!(
            result,
            Err(SetPoliciesError::InvalidPolicies { .. })
        ));
    }

    #[tokio::test]
    async fn refuses_unauthorized_caller() {
        // A realm member without the admin role writes nothing.
        let (_dir, context, actor) = setup_realm().await;
        let outsider = Actor {
            user_id: UserId::local(Ulid::from_bytes([9u8; 16]), actor.realm_id),
            ..actor.clone()
        };
        let error = drive(
            SetPoliciesOperation::new(policies_config(
                &outsider,
                vec![policy("permission == 'write'")],
                None,
            )),
            &context,
        )
        .await
        .expect_err("an unauthorized caller is refused");
        assert_eq!(error, SetPoliciesError::Unauthorized);

        let read = drive(GetConfigOperation::new(actor.realm_id), &context)
            .await
            .unwrap();
        assert!(read.request_policies.is_empty());
    }

    fn checked(
        allowed: Result<bool, AuthorizationError>,
    ) -> Result<Next<RealmConfigDocument>, SetPoliciesError> {
        let realm_id = RealmId([21u8; 32]);
        let actor = actor(realm_id);
        let mut operation = SetPoliciesOperation::new(policies_config(&actor, Vec::new(), None));
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        operation.next(IoResult::Child(ChildOutcome::new(allowed)))
    }

    #[test]
    fn conflict_reruns_change() {
        // A commit lost to a background config write reruns the whole change, reading again.
        let conflict = SetPoliciesError::from(IoError::Conflict);
        assert!(PoliciesWrite::is_conflict(&conflict));
        assert_eq!(
            PoliciesWrite::conflict_retries(),
            COMMIT_ATTEMPTS as u32 - 1
        );
        let actor = actor(RealmId([21u8; 32]));
        let write = PoliciesWrite::new(policies_config(&actor, Vec::new(), None));
        assert_eq!(write.transaction(), TransactionMode::Write);
    }

    #[test]
    fn denied_is_terminal() {
        assert_eq!(checked(Ok(false)), Err(SetPoliciesError::Unauthorized));
    }

    #[test]
    fn capacity_not_denied() {
        // Storage exhaustion inside the check is infrastructure, not a verdict.
        let capacity = AuthorizationError::StorageError(StorageError::CleanupCapacity);
        assert_eq!(
            checked(Err(capacity)),
            Err(SetPoliciesError::StorageError(
                StorageError::CleanupCapacity
            ))
        );
    }

    #[test]
    fn allowed_starts_write() {
        assert!(matches!(
            checked(Ok(true)),
            Ok(Next::Call(IoCall::Child(_)))
        ));
    }
}
