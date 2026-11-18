//! Replaces a group's deny-only request policy set, optionally only if the old hash matches.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::Event;
use aruna_core::keyspaces::{DOCUMENT_STATE_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode, boxed_retry};
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState, GROUP_POLICIES_PATH};
use aruna_core::request_policy::{RequestPolicy, policy_set_hash, validate_policy_set};
use aruna_core::rows::done;
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, reducer_state_key, stale_conflict_deletes,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::group::GroupAuthorizationDocument;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::types::{GroupId, Key, KeySpace, Value};
use byteview::ByteView;
use thiserror::Error;
use tracing::warn;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::auth::epoch_due::DueMarks;
use crate::sync::outbox_record::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};
use aruna_core::placement::fence::WriteFence;
use aruna_core::placement::target_placement_ref;

#[derive(Debug, Clone, PartialEq)]
pub struct SetGroupConfig {
    pub actor: Actor,
    /// The caller's own token context, so a path-restricted credential stays
    /// restricted; it is never derived from `actor`.
    pub auth_context: AuthContext,
    pub group_id: GroupId,
    pub policies: Vec<RequestPolicy>,
    /// When set, the write applies only if the stored set still hashes to it,
    /// compared inside the write transaction to close the check/use window.
    pub expected_hash: Option<[u8; 32]>,
}

/// Replaces a group's deny-only request policy set through the group admin document machinery,
/// so the value replicates as one last-writer-wins register on the group authorization document.
/// The write reruns after a lost conflict, since background writes touch the same rows.
#[derive(Debug, PartialEq)]
pub struct SetGroupOperation {
    config: SetGroupConfig,
    authorized: bool,
}

#[derive(Debug, Error, PartialEq)]
pub enum SetGroupError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    AdminDocumentError(#[from] AdminDocumentError),
    #[error("group authorization document missing")]
    GroupMissing,
    #[error("caller may not write the group configuration")]
    Unauthorized,
    #[error("stored policy set changed")]
    StaleHash,
    #[error("invalid policy set: {reason}")]
    InvalidPolicies { reason: String },
    #[error("the group's bucket cut over to a new holder set; retry the change")]
    PlacementFenced,
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl From<IoError> for SetGroupError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "SetGroup".to_string(),
                expected,
                got,
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished | IoError::Codec(_) => {
                Self::NotFinished
            }
        }
    }
}

impl SetGroupOperation {
    pub fn new(config: SetGroupConfig) -> Self {
        Self {
            config,
            authorized: false,
        }
    }

    fn admin_config_path(&self) -> String {
        format!(
            "/{}/g/{}/admin/config",
            self.config.actor.realm_id, self.config.group_id
        )
    }

    fn change(&self) -> IoCall {
        let config = self.config.clone();
        let seed = self.config.group_id.to_bytes();
        let build = move || GroupPoliciesWrite::new(config.clone());
        let child = boxed_retry(build, &seed, |result| {
            Event::Child(ChildOutcome::new(result))
        });
        IoCall::Child(child)
    }
}

impl Operation for SetGroupOperation {
    type Output = GroupAuthorizationDocument;
    type Error = SetGroupError;

    fn first(&mut self) -> Result<Next<GroupAuthorizationDocument>, SetGroupError> {
        if self.config.auth_context.realm_id != self.config.actor.realm_id {
            return Err(SetGroupError::Unauthorized);
        }
        Ok(Next::Call(IoCall::child(CheckPermissionsOperation::new(
            CheckPermissionsConfig {
                auth_context: self.config.auth_context.clone(),
                path: self.admin_config_path(),
                required_permission: Permission::WRITE,
            },
        ))))
    }

    fn next(
        &mut self,
        result: IoResult,
    ) -> Result<Next<GroupAuthorizationDocument>, SetGroupError> {
        if self.authorized {
            return result
                .child::<GroupAuthorizationDocument, SetGroupError>()?
                .map(Next::Done);
        }
        match result.child::<bool, AuthorizationError>()? {
            Ok(true) => {
                self.authorized = true;
                Ok(Next::Call(self.change()))
            }
            Ok(false) => Err(SetGroupError::Unauthorized),
            Err(error) => {
                warn!(error = %error, "Group policy authorization check failed");
                match error {
                    AuthorizationError::StorageError(error) => Err(error.into()),
                    _ => Err(SetGroupError::Unauthorized),
                }
            }
        }
    }
}

/// What the group policy transaction waits for.
#[derive(Debug, PartialEq)]
enum Stage {
    Read,
    Write,
    Delete,
    Fence,
    Marks,
}

/// Reads, validates and stores the group's policy set in one write transaction, takes the bucket
/// fence, and marks the group's key epochs due, since a new deny can narrow access.
#[derive(Debug, PartialEq)]
struct GroupPoliciesWrite {
    config: SetGroupConfig,
    stage: Stage,
    /// Bucket the authorization row publishes onto, read inside the write transaction.
    fence: WriteFence,
    document: Option<GroupAuthorizationDocument>,
    deletes: Vec<(KeySpace, Key)>,
    marks: DueMarks,
}

impl GroupPoliciesWrite {
    fn new(config: SetGroupConfig) -> Self {
        let marks = DueMarks::new(
            Some(config.group_id),
            aruna_core::time::unix_timestamp_millis(),
        );
        Self {
            config,
            stage: Stage::Read,
            fence: Default::default(),
            document: None,
            deletes: Vec::new(),
            marks,
        }
    }

    fn document_ref(&self) -> DocumentTarget {
        DocumentTarget::GroupAuthorization {
            group_id: self.config.group_id,
        }
    }

    fn admin_target(&self) -> AdminDocumentTarget {
        AdminDocumentTarget::Group {
            group_id: self.config.group_id,
        }
    }

    fn emit_write_state(
        &mut self,
        document_value: Option<Value>,
        reducer_state_value: Option<Value>,
        realm_config_value: Option<Value>,
    ) -> Result<Next<GroupAuthorizationDocument>, SetGroupError> {
        validate_policy_set(&self.config.policies)
            .map_err(|reason| SetGroupError::InvalidPolicies { reason })?;

        let Some(document_value) = document_value else {
            return Err(SetGroupError::GroupMissing);
        };
        let mut document = GroupAuthorizationDocument::from_bytes(&document_value)?;

        if let Some(expected) = self.config.expected_hash
            && policy_set_hash(&document.policies) != expected
        {
            return Err(SetGroupError::StaleHash);
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
            AdminDocumentOperation::GroupPoliciesSet {
                policies: self.config.policies.clone(),
            },
        )?;
        apply_reducer_policies(&mut document, &reducer_state);

        let stale_conflict_deletes =
            stale_conflict_deletes(previous_reducer_state.as_ref(), Some(&reducer_state));
        let document_target = self.document_ref();
        let realm_config = realm_config_value
            .as_deref()
            .map(RealmConfigDocument::from_bytes)
            .transpose()?;
        let placement = realm_config
            .as_ref()
            .map(|config| target_placement_ref(config, &document_target, Default::default()))
            .unwrap_or(PlacementRef::NIL);
        let realm_id = self.config.actor.realm_id;
        if let Some(config) = realm_config.as_ref() {
            self.fence.add(realm_id, config, [placement]);
        }
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
        )
        .fenced_at(self.fence.generation(&realm_id, &placement));
        writes.push(outbox_write_entry(&record).map_err(ConversionError::from)?);
        writes.extend(conflict_write_entries(&reducer_state)?);

        self.document = Some(document);
        self.deletes = stale_conflict_deletes;
        self.stage = Stage::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    /// Takes the bucket's fence inside the transaction before committing, so a departing
    /// holder's close rejects or conflicts this write.
    fn after_writes(&mut self) -> Next<GroupAuthorizationDocument> {
        if self.fence.is_empty() {
            return self.marks();
        }
        self.stage = Stage::Fence;
        Next::Call(IoCall::Read(self.fence.reads()))
    }

    fn marks(&mut self) -> Next<GroupAuthorizationDocument> {
        self.stage = Stage::Marks;
        Next::Call(self.marks.first())
    }

    /// A failed drain timer leaves the durable outbox retryable.
    fn finish(&mut self) -> Result<Next<GroupAuthorizationDocument>, SetGroupError> {
        let output = self.document.take().ok_or(SetGroupError::NotFinished)?;
        let calls = vec![IoCall::Effect(Box::new(schedule_drain_effect()))];
        Ok(Next::Then { output, calls })
    }
}

impl Operation for GroupPoliciesWrite {
    type Output = GroupAuthorizationDocument;
    type Error = SetGroupError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn conflict_retries() -> u32 {
        u32::from(super::CONFLICT_RETRIES)
    }

    fn first(&mut self) -> Result<Next<GroupAuthorizationDocument>, SetGroupError> {
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
            (
                REALM_CONFIG_KEYSPACE.to_string(),
                ByteView::from(*self.config.actor.realm_id.as_bytes()),
            ),
        ])))
    }

    fn next(
        &mut self,
        result: IoResult,
    ) -> Result<Next<GroupAuthorizationDocument>, SetGroupError> {
        match self.stage {
            Stage::Read => {
                let [document, reducer, realm_config] = aruna_core::rows::values::<3>(result)?;
                self.emit_write_state(document, reducer, realm_config)
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
                Ok(self.after_writes())
            }
            Stage::Fence => {
                let IoResult::Values(values) = result else {
                    return Err(SetGroupError::NotFinished);
                };
                let keys = self.fence.reads().into_iter().map(|(_, key)| key);
                let rows: Vec<_> = keys.zip(values).collect();
                if !self.fence.admits(&rows) {
                    return Err(SetGroupError::PlacementFenced);
                }
                Ok(self.marks())
            }
            Stage::Marks => match self.marks.next(result, true) {
                Ok(Some(call)) => Ok(Next::Call(call)),
                Ok(None) => self.finish(),
                Err(error) => Err(StorageError::WriteError(error.to_string()).into()),
            },
        }
    }
}

/// Overlays the reducer's materialized policy set onto the group auth document,
/// mirroring the replicated materialization in `net::irokle`.
fn apply_reducer_policies(
    document: &mut GroupAuthorizationDocument,
    reducer_state: &AdminDocumentState,
) {
    if !reducer_state.conflicts.contains_key(GROUP_POLICIES_PATH)
        && let Some(policies) = reducer_state.materialized_group_policies()
    {
        document.policies = policies;
    }
}

#[cfg(test)]
mod tests {
    use super::{GroupPoliciesWrite, SetGroupConfig, SetGroupError, SetGroupOperation};
    use crate::document_repository::{GetGroupConfig, GetGroupOperation};
    use crate::driver::{DriverContext, drive};
    use crate::groups::create_group::{CreateGroupConfig, CreateGroupOperation};
    use aruna_core::UserId;
    use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
    use aruna_core::effects::StorageEffect;
    use aruna_core::errors::{AuthorizationError, StorageError};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::AUTH_KEYSPACE;
    use aruna_core::operation::{Operation, TransactionMode};
    use aruna_core::request_policy::RequestPolicy;
    use aruna_core::structs::identity::auth::{Actor, AuthContext};
    use aruna_core::structs::identity::group::GroupAuthorizationDocument;
    use aruna_core::structs::identity::realm::{RealmAuthorizationDocument, RealmId};
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

    fn group_config(
        actor: &Actor,
        group_id: Ulid,
        policies: Vec<RequestPolicy>,
        expected_hash: Option<[u8; 32]>,
    ) -> SetGroupConfig {
        SetGroupConfig {
            actor: actor.clone(),
            auth_context: auth(actor),
            group_id,
            policies,
            expected_hash,
        }
    }

    /// The group creator holds the group admin role, but the permission
    /// sub-operation also needs the realm authorization document to exist.
    async fn seed_realm_doc(context: &DriverContext, actor: &Actor) {
        let document = RealmAuthorizationDocument::default_realm_doc(actor.realm_id);
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
            policy_id: Ulid::from_bytes([9u8; 16]),
            name: "no-writes".to_string(),
            kind: aruna_core::request_policy::PolicyKind::Deny,
            when: None,
            expression: expression.to_string(),
            enabled: true,
        }
    }

    async fn setup_group() -> (tempfile::TempDir, DriverContext, Actor, Ulid) {
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
        let realm_id = RealmId([23u8; 32]);
        let actor = actor(realm_id);
        let (group, _) = drive(
            CreateGroupOperation::new(CreateGroupConfig {
                actor: actor.clone(),
                display_name: "policy group".to_string(),
                owner_cap: None,
            }),
            &context,
        )
        .await
        .unwrap();
        seed_realm_doc(&context, &actor).await;
        (dir, context, actor, group.group_id)
    }

    #[tokio::test]
    async fn stores_group_policies() {
        // The set lands on the group auth doc and a later read returns it.
        let (_dir, context, actor, group_id) = setup_group().await;
        let policies = vec![policy("permission == 'write'")];
        let document = drive(
            SetGroupOperation::new(group_config(&actor, group_id, policies.clone(), None)),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(document.policies, policies);

        let (_, auth_doc) = drive(
            GetGroupOperation::new(GetGroupConfig { group_id }),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(auth_doc.policies, policies);
    }

    #[tokio::test]
    async fn stale_hash_aborts() {
        // A concurrent write moves the stored hash; the second write must abort
        // on its stale expected_hash and leave the first set in place.
        use aruna_core::request_policy::policy_set_hash;
        let (_dir, context, actor, group_id) = setup_group().await;
        let empty_hash = policy_set_hash(&[]);
        let first = vec![policy("permission == 'write'")];
        drive(
            SetGroupOperation::new(group_config(
                &actor,
                group_id,
                first.clone(),
                Some(empty_hash),
            )),
            &context,
        )
        .await
        .unwrap();

        let stale = drive(
            SetGroupOperation::new(group_config(
                &actor,
                group_id,
                vec![policy("permission == 'read'")],
                Some(empty_hash),
            )),
            &context,
        )
        .await;
        assert!(matches!(stale, Err(SetGroupError::StaleHash)));

        let (_, auth_doc) = drive(
            GetGroupOperation::new(GetGroupConfig { group_id }),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(auth_doc.policies, first);
    }

    #[tokio::test]
    async fn rejects_invalid_set() {
        // An uncompilable expression is refused at administration time.
        let (_dir, context, actor, group_id) = setup_group().await;
        let result = drive(
            SetGroupOperation::new(group_config(
                &actor,
                group_id,
                vec![policy("path.startsWith(")],
                None,
            )),
            &context,
        )
        .await;
        assert!(matches!(result, Err(SetGroupError::InvalidPolicies { .. })));
    }

    #[tokio::test]
    async fn refuses_unauthorized_caller() {
        // A realm member outside the group's admin role writes nothing.
        let (_dir, context, actor, group_id) = setup_group().await;
        let outsider = Actor {
            user_id: UserId::local(Ulid::from_bytes([8u8; 16]), actor.realm_id),
            ..actor.clone()
        };
        let error = drive(
            SetGroupOperation::new(group_config(
                &outsider,
                group_id,
                vec![policy("permission == 'write'")],
                None,
            )),
            &context,
        )
        .await
        .expect_err("an unauthorized caller is refused");
        assert_eq!(error, SetGroupError::Unauthorized);

        let (_, auth_doc) = drive(
            GetGroupOperation::new(GetGroupConfig { group_id }),
            &context,
        )
        .await
        .unwrap();
        assert!(auth_doc.policies.is_empty());
    }

    fn checked(
        allowed: Result<bool, AuthorizationError>,
    ) -> Result<Next<GroupAuthorizationDocument>, SetGroupError> {
        let realm_id = RealmId([23u8; 32]);
        let actor = actor(realm_id);
        let group_id = Ulid::from_bytes([5u8; 16]);
        let mut operation =
            SetGroupOperation::new(group_config(&actor, group_id, Vec::new(), None));
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        operation.next(IoResult::Child(ChildOutcome::new(allowed)))
    }

    #[test]
    fn retries_commit_conflict() {
        // A lost commit reruns the whole write, rereading every row, up to the retry bound.
        let conflict = SetGroupError::from(IoError::Conflict);
        assert!(GroupPoliciesWrite::is_conflict(&conflict));
        assert_eq!(
            GroupPoliciesWrite::conflict_retries(),
            u32::from(crate::groups::CONFLICT_RETRIES)
        );
        let actor = actor(RealmId([24u8; 32]));
        let config = group_config(&actor, Ulid::from_bytes([10u8; 16]), Vec::new(), None);
        assert_eq!(
            GroupPoliciesWrite::new(config).transaction(),
            TransactionMode::Write
        );
    }

    #[test]
    fn denied_is_terminal() {
        assert_eq!(checked(Ok(false)), Err(SetGroupError::Unauthorized));
    }

    #[test]
    fn capacity_not_denied() {
        // Storage exhaustion inside the check is infrastructure, not a verdict.
        let capacity = AuthorizationError::StorageError(StorageError::CleanupCapacity);
        assert_eq!(
            checked(Err(capacity)),
            Err(SetGroupError::StorageError(StorageError::CleanupCapacity))
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
