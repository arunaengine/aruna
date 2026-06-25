use crate::document_sync_outbox::{
    new_outbox_record_with_id, outbox_write_entry, schedule_outbox_drain_effect,
};
use crate::replicate_documents::replicate_documents_effect;
use aruna_core::admin_document_reducer::{AdminDocumentReducerError, AdminDocumentReducerState};
use aruna_core::admin_documents::{
    AdminDocumentEvent, AdminDocumentOperation, AdminDocumentRoleDefinition, AdminDocumentTarget,
};
use aruna_core::document::{DocumentSyncOutboxEvent, DocumentSyncTarget};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::storage_entries::{
    admin_document_conflict_write_entries, admin_document_reducer_state_write_entry,
};
use aruna_core::structs::{Actor, Group, GroupAuthorizationDocument, Role};
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, Key, UserId, Value};
use smallvec::smallvec;
use std::collections::HashSet;
use thiserror::Error;
use tracing::{trace, warn};
use ulid::Ulid;

#[derive(Clone, Debug, PartialEq)]
pub struct CreateGroupConfig {
    pub actor: Actor,
    pub display_name: String,
}

#[derive(PartialEq)]
pub struct CreateGroupOperation {
    config: CreateGroupConfig,
    group: Option<Group>,
    auth_doc: Option<GroupAuthorizationDocument>,
    state: CreateGroupState,
    txn_id: Option<Ulid>,
    output: Option<Result<(Group, GroupAuthorizationDocument), CreateGroupError>>,
}

impl std::fmt::Debug for CreateGroupOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CreateGroupOperation")
            .field("config", &self.config)
            .field("group", &self.group)
            .field("auth_doc", &self.auth_doc)
            .field("state", &self.state)
            .field("txn_id", &self.txn_id)
            .field("output", &self.output)
            .finish()
    }
}

impl CreateGroupOperation {
    pub fn new(config: CreateGroupConfig) -> Self {
        CreateGroupOperation {
            config,
            group: None,
            auth_doc: None,
            state: CreateGroupState::Init,
            txn_id: None,
            output: None,
        }
    }
    #[tracing::instrument(name = "group.create.emit_group", level = "debug", skip(self), fields(state = ?self.state, group_name = %self.config.display_name))]
    fn emit_create_group(&mut self) -> Result<Effects, CreateGroupError> {
        let group_id = Ulid::new();
        let auth_doc = GroupAuthorizationDocument::new_default_group_doc(
            self.config.actor.user_id,
            self.config.actor.realm_id,
            group_id,
        );
        let group = Group {
            roles: auth_doc.roles.keys().copied().collect(),
            display_name: self.config.display_name.clone(),
            group_id,
            realm_id: self.config.actor.realm_id,
        };

        self.auth_doc = Some(auth_doc);
        self.group = Some(group.clone());

        trace!(
            event = "group.create.started",
            group_id = %group.group_id,
            realm_id = %group.realm_id,
            user_id = %self.config.actor.user_id,
            "Creating group"
        );

        let key = group_id.to_bytes().into();

        let value = group.to_bytes(&self.config.actor)?.into();

        Ok(smallvec![Effect::Storage(StorageEffect::Write {
            key_space: GROUP_KEYSPACE.to_string(),
            key,
            value,
            txn_id: self.txn_id,
        })])
    }

    #[tracing::instrument(name = "group.create.emit_auth_doc", level = "debug", skip(self), fields(state = ?self.state))]
    fn emit_create_auth_doc(&mut self) -> Result<Effects, CreateGroupError> {
        let txn_id = self.txn_id.ok_or(CreateGroupError::NoTransactionFound)?;

        let group_id = self
            .group
            .as_ref()
            .ok_or(CreateGroupError::GroupNotFound)?
            .group_id;
        let auth_doc = self
            .auth_doc
            .as_ref()
            .ok_or(CreateGroupError::AuthDocNotFound)?;

        let key = group_id.to_bytes().into();
        let value = auth_doc.to_bytes(&self.config.actor)?.into();
        let mut writes = vec![(AUTH_KEYSPACE.to_string(), key, value)];
        writes.extend(self.admin_reducer_seed_writes()?);

        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }

    fn admin_reducer_seed_writes(&self) -> Result<Vec<(String, Key, Value)>, CreateGroupError> {
        let group_id = self
            .group
            .as_ref()
            .ok_or(CreateGroupError::GroupNotFound)?
            .group_id;
        let auth_doc = self
            .auth_doc
            .as_ref()
            .ok_or(CreateGroupError::AuthDocNotFound)?;
        let target = AdminDocumentTarget::Group { group_id };
        let mut reducer_state = AdminDocumentReducerState::new(target);
        let mut admin_events = Vec::new();
        let roles = sorted_roles(auth_doc);

        admin_events.push(apply_admin_reducer_operation(
            &mut reducer_state,
            &self.config.actor,
            AdminDocumentOperation::GroupCreated {
                realm_id: self.config.actor.realm_id,
                display_name: self.config.display_name.clone(),
            },
        )?);

        for role in &roles {
            admin_events.push(apply_admin_reducer_operation(
                &mut reducer_state,
                &self.config.actor,
                AdminDocumentOperation::GroupRoleCreated {
                    role: AdminDocumentRoleDefinition::from(*role),
                },
            )?);
        }

        let admin_role = roles
            .iter()
            .find(|role| role.name == "admin")
            .ok_or(CreateGroupError::AdminRoleNotFound)?;
        for user_id in sorted_user_ids(&admin_role.assigned_users) {
            admin_events.push(apply_admin_reducer_operation(
                &mut reducer_state,
                &self.config.actor,
                AdminDocumentOperation::GroupRoleUserAssignmentAdded {
                    role_id: admin_role.role_id,
                    user_id,
                },
            )?);
        }

        let document_target = DocumentSyncTarget::GroupAuthorization { group_id };
        let mut writes = vec![admin_document_reducer_state_write_entry(&reducer_state)?];
        for event in admin_events {
            let record = new_outbox_record_with_id(
                event.event_id,
                self.config.actor.node_id,
                document_target.clone(),
                Vec::new(),
                DocumentSyncOutboxEvent::AdminOperation {
                    event: Box::new(event),
                },
            );
            writes.push(outbox_write_entry(&record).map_err(ConversionError::from)?);
        }
        writes.extend(admin_document_conflict_write_entries(&reducer_state)?);

        Ok(writes)
    }

    fn emit_legacy_document_replication(&mut self) -> Effects {
        if let Some(group) = &self.group
            && self.auth_doc.is_some()
        {
            self.state = CreateGroupState::ReplicateDocuments;
            trace!(
                event = "group.create.announce_group",
                group_id = %group.group_id,
                realm_id = %group.realm_id,
                user_id = %self.config.actor.user_id,
                "Announcing group"
            );
            smallvec![replicate_documents_effect(
                self.config.actor.realm_id,
                self.config.actor.node_id,
                vec![
                    DocumentSyncTarget::Group {
                        group_id: group.group_id,
                    },
                    DocumentSyncTarget::GroupAuthorization {
                        group_id: group.group_id,
                    },
                ],
            )]
        } else {
            self.fail(CreateGroupError::GroupNotFound)
        }
    }

    #[tracing::instrument(name = "group.create.fail", level = "debug", skip(self), fields(state = ?self.state, error = %err))]
    fn fail(&mut self, err: CreateGroupError) -> Effects {
        self.state = CreateGroupState::Error;
        self.output = Some(Err(err));
        smallvec![]
    }

    #[tracing::instrument(name = "group.create.fail_with_cleanup", level = "debug", skip(self, cleanup_effects), fields(state = ?self.state, error = %err))]
    fn fail_with_cleanup(&mut self, err: CreateGroupError, cleanup_effects: Effects) -> Effects {
        self.state = CreateGroupState::Error;
        self.output = Some(Err(err));
        cleanup_effects
    }

    #[tracing::instrument(name = "group.create.unexpected_event", level = "debug", skip(self, got), fields(current_state = ?self.state, expected, got = %got))]
    fn unexpected_event(
        &mut self,
        state: CreateGroupState,
        expected: &'static str,
        got: String,
    ) -> Effects {
        let cleanup_effects = self.abort();
        self.fail_with_cleanup(
            CreateGroupError::UnexpectedEvent {
                state,
                expected,
                got,
            },
            cleanup_effects,
        )
    }

    #[tracing::instrument(name = "group.create.fail_on_storage_error", level = "trace", skip(self, event), fields(state = ?self.state, event = ?event))]
    fn fail_on_storage_error(&mut self, event: Event) -> Result<Event, Effects> {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return Err(self.fail(error.into()));
        }

        Ok(event)
    }

    #[tracing::instrument(name = "group.create.handle_start_transaction", level = "debug", skip(self, event), fields(state = ?self.state, event = ?event))]
    fn handle_start_transaction(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected_event(
                CreateGroupState::StartTransaction,
                "Event::Storage(StorageEvent::TransactionStarted)",
                got,
            );
        };

        self.state = CreateGroupState::CreateGroup;
        self.txn_id = Some(txn_id);
        match self.emit_create_group() {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    #[tracing::instrument(name = "group.create.handle_group_write", level = "debug", skip(self, event), fields(state = ?self.state, event = ?event))]
    fn handle_create_group(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.unexpected_event(
                CreateGroupState::CreateGroup,
                "Event::Storage(StorageEvent::WriteResult)",
                got,
            );
        };

        self.state = CreateGroupState::CreateRoles;
        match self.emit_create_auth_doc() {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    #[tracing::instrument(name = "group.create.handle_auth_write", level = "debug", skip(self, event), fields(state = ?self.state, event = ?event))]
    fn handle_create_roles(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
            return self.unexpected_event(
                CreateGroupState::CreateRoles,
                "Event::Storage(StorageEvent::BatchWriteResult)",
                got,
            );
        };

        self.state = CreateGroupState::CommitTransaction;
        if let Some(txn_id) = self.txn_id {
            smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
        } else {
            self.fail(CreateGroupError::NoTransactionFound)
        }
    }

    #[tracing::instrument(name = "group.create.handle_commit", level = "debug", skip(self, event), fields(state = ?self.state, event = ?event))]
    fn handle_commit_transaction(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.unexpected_event(
                CreateGroupState::CommitTransaction,
                "Event::Storage(StorageEvent::TransactionCommitted)",
                got,
            );
        };

        if self.group.is_some() && self.auth_doc.is_some() {
            self.state = CreateGroupState::ScheduleDocumentSyncOutboxDrain;
            smallvec![schedule_outbox_drain_effect()]
        } else {
            self.fail(CreateGroupError::GroupNotFound)
        }
    }

    fn handle_schedule_document_sync_outbox_drain(&mut self, event: Event) -> Effects {
        match event {
            Event::Task(TaskEvent::TimerScheduled { .. }) => {
                self.emit_legacy_document_replication()
            }
            Event::Task(TaskEvent::Error { message, .. }) => {
                warn!(error = %message, "Failed to schedule document sync outbox drain; continuing with legacy group creation replication");
                self.emit_legacy_document_replication()
            }
            other => self.unexpected_event(
                CreateGroupState::ScheduleDocumentSyncOutboxDrain,
                "Event::Task(TaskEvent::TimerScheduled)",
                format!("{other:?}"),
            ),
        }
    }

    #[tracing::instrument(name = "group.create.handle_replicate", level = "debug", skip(self, event), fields(state = ?self.state, event = ?event))]
    fn handle_replicate_documents(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::SubOperation(SubOperationEvent::DocumentSyncResult { result }) = event else {
            return self.unexpected_event(
                CreateGroupState::ReplicateDocuments,
                "Event::SubOperation(SubOperationEvent::DocumentSyncResult)",
                got,
            );
        };

        if let Err(error) = result {
            return self.fail(CreateGroupError::DocumentSync(error));
        }

        if let Some(group) = &self.group
            && let Some(auth) = &self.auth_doc
        {
            trace!(
                event = "group.create.completed",
                group_id = %group.group_id,
                realm_id = %group.realm_id,
                "Created and replicated group"
            );
            self.state = CreateGroupState::Finish;
            self.output = Some(Ok((group.clone(), auth.clone())));
            smallvec![]
        } else {
            self.fail(CreateGroupError::GroupNotFound)
        }
    }
}

fn apply_admin_reducer_operation(
    state: &mut AdminDocumentReducerState,
    actor: &Actor,
    op: AdminDocumentOperation,
) -> Result<AdminDocumentEvent, AdminDocumentReducerError> {
    let observed = state.clock.clone();
    let event = AdminDocumentEvent {
        event_id: Ulid::new(),
        target: state.target.clone(),
        origin_node_id: actor.node_id,
        origin_seq: observed.sequence_for(&actor.node_id) + 1,
        observed,
        actor: actor.clone(),
        op,
    };
    state.apply(&event)?;
    Ok(event)
}

fn sorted_roles(auth_doc: &GroupAuthorizationDocument) -> Vec<&Role> {
    let mut roles: Vec<_> = auth_doc.roles.values().collect();
    roles.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.role_id.cmp(&right.role_id))
    });
    roles
}

fn sorted_user_ids(user_ids: &HashSet<UserId>) -> Vec<UserId> {
    let mut user_ids: Vec<_> = user_ids.iter().copied().collect();
    user_ids.sort();
    user_ids
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CreateGroupState {
    Init,
    StartTransaction,
    CreateGroup,
    CreateRoles,
    CommitTransaction,
    ScheduleDocumentSyncOutboxDrain,
    ReplicateDocuments,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateGroupError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    AdminDocumentReducerError(#[from] AdminDocumentReducerError),
    #[error("document sync failed: {0}")]
    DocumentSync(String),
    #[error("No auth doc found")]
    AuthDocNotFound,
    #[error("No admin role found")]
    AdminRoleNotFound,
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("No group found")]
    GroupNotFound,
    #[error("Creating Group did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: CreateGroupState,
        expected: &'static str,
        got: String,
    },
}

impl Operation for CreateGroupOperation {
    type Output = (Group, GroupAuthorizationDocument);

    type Error = CreateGroupError;

    #[tracing::instrument(name = "group.create.start", level = "debug", skip(self), fields(group_name = %self.config.display_name))]
    fn start(&mut self) -> Effects {
        self.state = CreateGroupState::StartTransaction;

        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    #[tracing::instrument(name = "group.create.step", level = "debug", skip(self, event), fields(state = ?self.state, event = ?event))]
    fn step(&mut self, event: Event) -> Effects {
        let event = match self.fail_on_storage_error(event) {
            Ok(event) => event,
            Err(effects) => return effects,
        };

        match self.state {
            CreateGroupState::StartTransaction => self.handle_start_transaction(event),
            CreateGroupState::CreateGroup => self.handle_create_group(event),
            CreateGroupState::CreateRoles => self.handle_create_roles(event),
            CreateGroupState::CommitTransaction => self.handle_commit_transaction(event),
            CreateGroupState::ScheduleDocumentSyncOutboxDrain => {
                self.handle_schedule_document_sync_outbox_drain(event)
            }
            CreateGroupState::ReplicateDocuments => self.handle_replicate_documents(event),
            CreateGroupState::Init | CreateGroupState::Finish | CreateGroupState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            CreateGroupState::Finish | CreateGroupState::Error
        )
    }

    #[tracing::instrument(name = "group.create.finalize", level = "debug", skip(self), fields(state = ?self.state))]
    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(CreateGroupError::NotFinished)?
    }

    #[tracing::instrument(name = "group.create.abort", level = "debug", skip(self), fields(state = ?self.state, txn_id = ?self.txn_id))]
    fn abort(&mut self) -> Effects {
        match self.txn_id {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

#[cfg(test)]
mod test {
    use crate::create_group::{CreateGroupConfig, CreateGroupOperation};
    use crate::driver::{DriverContext, drive};
    use aruna_core::UserId;
    use aruna_core::admin_document_reducer::AdminDocumentReducerState;
    use aruna_core::admin_documents::{
        AdminDocumentOperation, AdminDocumentRoleDefinition, AdminDocumentTarget,
    };
    use aruna_core::document::{
        DocumentSyncOutboxEvent, DocumentSyncOutboxRecord, DocumentSyncTarget,
    };
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{
        ADMIN_DOCUMENT_STATE_KEYSPACE, AUTH_KEYSPACE, DOCUMENT_SYNC_OUTBOX_KEYSPACE, GROUP_KEYSPACE,
    };
    use aruna_core::operation::Operation;
    use aruna_core::structs::{Actor, Group, GroupAuthorizationDocument, RealmId};
    use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
    use aruna_core::types::{Key, KeySpace, TxnId, Value};
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use std::collections::{BTreeSet, HashSet};
    use std::time::Duration;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn actor(realm_id: RealmId, node_seed: u8, user_seed: u8) -> Actor {
        Actor {
            node_id: iroh::SecretKey::from_bytes(&[node_seed; 32]).public(),
            user_id: UserId::local(Ulid::from_bytes([user_seed; 16]), realm_id),
            realm_id,
        }
    }

    fn config(actor: Actor) -> CreateGroupConfig {
        CreateGroupConfig {
            actor,
            display_name: "Test group".to_string(),
        }
    }

    fn batch_writes(effects: &[Effect], txn_id: TxnId) -> &Vec<(KeySpace, Key, Value)> {
        match effects.first().unwrap() {
            Effect::Storage(StorageEffect::BatchWrite {
                writes,
                txn_id: effect_txn_id,
            }) => {
                assert_eq!(*effect_txn_id, Some(txn_id));
                writes
            }
            other => panic!("unexpected effect: {other:?}"),
        }
    }

    fn write_values<'a>(writes: &'a [(KeySpace, Key, Value)], keyspace: &str) -> Vec<&'a Value> {
        writes
            .iter()
            .filter(|(candidate, _, _)| candidate == keyspace)
            .map(|(_, _, value)| value)
            .collect()
    }

    fn operation_ready_to_schedule(actor: Actor, txn_id: TxnId) -> CreateGroupOperation {
        let auth_doc = GroupAuthorizationDocument::new_default_group_doc(
            actor.user_id,
            actor.realm_id,
            Ulid::from_bytes([9; 16]),
        );
        let group = Group {
            display_name: "Test group".to_string(),
            group_id: auth_doc.group_id,
            realm_id: actor.realm_id,
            roles: auth_doc.roles.keys().copied().collect(),
        };
        let mut operation = CreateGroupOperation::new(config(actor));
        operation.txn_id = Some(txn_id);
        operation.group = Some(group);
        operation.auth_doc = Some(auth_doc);
        operation.state = super::CreateGroupState::CommitTransaction;
        operation
    }

    #[test]
    fn seeds_group_reducer_state_and_admin_outbox_in_order() {
        let realm_id = RealmId::from_bytes([2; 32]);
        let actor = actor(realm_id, 3, 4);
        let txn_id = TxnId::new();
        let mut operation = CreateGroupOperation::new(config(actor.clone()));
        operation.txn_id = Some(txn_id);

        let group_effects = operation.emit_create_group().unwrap();
        let group = operation.group.as_ref().unwrap().clone();
        let auth_doc = operation.auth_doc.as_ref().unwrap().clone();
        match group_effects.first().unwrap() {
            Effect::Storage(StorageEffect::Write {
                key_space,
                value,
                txn_id: effect_txn_id,
                ..
            }) => {
                assert_eq!(key_space, GROUP_KEYSPACE);
                assert_eq!(*effect_txn_id, Some(txn_id));
                let stored_group = Group::from_bytes(value.as_ref()).unwrap();
                assert_eq!(
                    stored_group.roles,
                    auth_doc.roles.keys().copied().collect::<HashSet<_>>()
                );
                assert!(!stored_group.roles.is_empty());
            }
            other => panic!("unexpected group write effect: {other:?}"),
        }

        let effects = operation.emit_create_auth_doc().unwrap();
        let writes = batch_writes(&effects, txn_id);
        let target = AdminDocumentTarget::Group {
            group_id: group.group_id,
        };
        let reducer_state = postcard::from_bytes::<AdminDocumentReducerState>(
            write_values(writes, ADMIN_DOCUMENT_STATE_KEYSPACE)[0].as_ref(),
        )
        .unwrap();
        let outbox_records = write_values(writes, DOCUMENT_SYNC_OUTBOX_KEYSPACE)
            .into_iter()
            .map(|value| postcard::from_bytes::<DocumentSyncOutboxRecord>(value.as_ref()).unwrap())
            .collect::<Vec<_>>();
        let stored_auth =
            GroupAuthorizationDocument::from_bytes(write_values(writes, AUTH_KEYSPACE)[0].as_ref())
                .unwrap();

        assert_eq!(stored_auth, auth_doc);
        assert_eq!(reducer_state.target, target);
        assert_eq!(
            reducer_state.materialized_group_roles(),
            auth_doc.roles.keys().copied().collect::<BTreeSet<_>>()
        );
        assert_eq!(
            reducer_state.materialized_group_display_name().as_deref(),
            Some(group.display_name.as_str())
        );
        assert_eq!(
            reducer_state.materialized_group_realm_id(),
            Some(group.realm_id)
        );
        assert!(reducer_state.conflicts.is_empty());

        let admin_role = auth_doc
            .roles
            .values()
            .find(|role| role.name == "admin")
            .unwrap();
        assert!(
            reducer_state.materialized_group_role_user_assignments()[&admin_role.role_id]
                .contains(&actor.user_id)
        );

        assert_eq!(outbox_records.len(), auth_doc.roles.len() + 2);
        assert!(outbox_records.iter().all(|record| {
            record.target
                == (DocumentSyncTarget::GroupAuthorization {
                    group_id: group.group_id,
                })
        }));
        let events = outbox_records
            .iter()
            .map(|record| match &record.event {
                DocumentSyncOutboxEvent::AdminOperation { event } => event.as_ref(),
                other => panic!("unexpected outbox event: {other:?}"),
            })
            .collect::<Vec<_>>();
        assert!(matches!(
            &events.first().unwrap().op,
            AdminDocumentOperation::GroupCreated {
                realm_id,
                display_name,
            } if *realm_id == group.realm_id && display_name == &group.display_name
        ));
        let role_names = events[1..=auth_doc.roles.len()]
            .iter()
            .map(|event| match &event.op {
                AdminDocumentOperation::GroupRoleCreated { role } => role.name.as_str(),
                other => panic!("unexpected role seed event: {other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(role_names, vec!["admin", "user", "viewer"]);
        for (index, event) in events.iter().enumerate() {
            assert_eq!(event.target, target);
            assert_eq!(event.origin_seq, index as u64 + 1);
            assert_eq!(event.observed.sequence_for(&actor.node_id), index as u64);
        }
        assert!(matches!(
            &events.last().unwrap().op,
            AdminDocumentOperation::GroupRoleUserAssignmentAdded { role_id, user_id }
                if *role_id == admin_role.role_id && *user_id == actor.user_id
        ));
        assert!(
            events[1..=auth_doc.roles.len()]
                .iter()
                .all(|event| matches!(
                    &event.op,
                    AdminDocumentOperation::GroupRoleCreated { role }
                        if auth_doc.roles.get(&role.role_id).is_some_and(|source| {
                            role == &AdminDocumentRoleDefinition::from(source)
                        })
                ))
        );
    }

    #[test]
    fn schedules_outbox_drain_before_legacy_replication_and_continues_on_error() {
        let realm_id = RealmId::from_bytes([5; 32]);
        let actor = actor(realm_id, 6, 7);
        let txn_id = TxnId::new();
        let mut operation = operation_ready_to_schedule(actor, txn_id);

        let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
        assert_eq!(
            operation.state,
            super::CreateGroupState::ScheduleDocumentSyncOutboxDrain
        );
        assert_eq!(
            effects.first(),
            Some(&Effect::Task(TaskEffect::ResetTimer {
                key: TaskKey::DrainDocumentSyncOutbox,
                after: Duration::ZERO,
            }))
        );

        let effects = operation.step(Event::Task(TaskEvent::Error {
            key: Some(TaskKey::DrainDocumentSyncOutbox),
            message: "schedule failed".to_string(),
        }));
        assert_eq!(operation.state, super::CreateGroupState::ReplicateDocuments);
        assert!(matches!(effects.first(), Some(Effect::SubOperation(_))));
    }

    #[tokio::test]
    pub async fn test_group_creation() {
        let random_path = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(random_path.path().to_str().unwrap()).unwrap();
        let net_handle = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..NetConfig::default()
            },
            storage_handle.clone(),
        )
        .await
        .unwrap();
        let task_handle = TaskHandle::new();

        let context = DriverContext {
            storage_handle,
            blob_handle: None,
            net_handle: Some(net_handle.clone()),
            metadata_handle: None,
            task_handle: Some(task_handle),
        };

        let realm_id = aruna_core::structs::RealmId([0u8; 32]);
        let user_id = UserId::local(Ulid::new(), realm_id);
        let node_id = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let group_config = CreateGroupConfig {
            actor: Actor {
                node_id,
                user_id,
                realm_id,
            },
            display_name: "Test group".to_string(),
        };
        let group_operation = CreateGroupOperation::new(group_config.clone());
        let result = drive(group_operation, &context).await.unwrap();
        assert_eq!(result.0.display_name, group_config.display_name);
        assert_eq!(result.0.realm_id, group_config.actor.realm_id);
        assert_eq!(
            result.0.roles,
            result.1.roles.keys().copied().collect::<HashSet<_>>()
        );
        assert_eq!(result.0.roles.len(), 3);
        assert!(
            result
                .0
                .roles
                .iter()
                .all(|r| result.1.roles.iter().any(|(id, _)| id == r))
        );
        assert!(result.1.roles.iter().any(|(_id, role)| {
            role.name == "admin"
                && role
                    .assigned_users
                    .iter()
                    .any(|user| user == &group_config.actor.user_id)
        }));
        assert!(
            result
                .1
                .roles
                .iter()
                .any(|(_id, role)| { role.name == "user" })
        );
        assert!(
            result
                .1
                .roles
                .iter()
                .any(|(_id, role)| { role.name == "viewer" })
        );

        net_handle.shutdown().await;
    }
}
