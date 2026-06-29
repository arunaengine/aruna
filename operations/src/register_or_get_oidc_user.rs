use aruna_core::admin_document_reducer::{AdminDocumentReducerError, AdminDocumentReducerState};
use aruna_core::admin_documents::{
    AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget,
};
use aruna_core::document::{
    DocumentSyncChange, DocumentSyncChangeKind, DocumentSyncOutboxEvent, DocumentSyncRevision,
    DocumentSyncTarget,
};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::storage_entries::{
    admin_document_reducer_state_write_entry, document_sync_revision_write_entry,
};
use aruna_core::structs::{Actor, User, oidc_subject_key};
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, TxnId, UserId};
use aruna_core::{USER_KEYSPACE, USER_SUBJECT_INDEX_KEYSPACE};
use byteview::ByteView;
use chrono::Utc;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::document_sync_outbox::{
    new_outbox_record_with_id, outbox_write_entry, schedule_outbox_drain_effect,
};
use crate::user_subject_index::rewrite_subject_index_effects;
#[derive(Clone, Debug, PartialEq)]
pub struct RegisterOrGetOidcUserInput {
    pub actor: Actor,
    pub issuer: String,
    pub subject_id: String,
    pub name: String,
    pub user_id: UserId,
}

#[derive(Debug, PartialEq)]
pub struct RegisterOrGetOidcUserOperation {
    input: RegisterOrGetOidcUserInput,
    state: RegisterOrGetOidcUserState,
    output: Option<Result<User, RegisterOrGetOidcUserError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum RegisterOrGetOidcUserState {
    Init,
    StartTransaction,
    ReadSubjectIndex { txn_id: TxnId },
    ReadExistingUser { txn_id: TxnId },
    WriteUserAndDocumentRevision { txn_id: TxnId, user: User },
    WriteSubjectIndex { txn_id: TxnId, user: User },
    CommitTransaction { user: User, announce: bool },
    ScheduleAdminDocumentOutboxDrain { user: User },
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum RegisterOrGetOidcUserError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    AdminDocumentReducerError(#[from] AdminDocumentReducerError),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
    #[error("registration did not finish")]
    NotFinished,
}

impl RegisterOrGetOidcUserOperation {
    pub fn new(input: RegisterOrGetOidcUserInput) -> Self {
        Self {
            input,
            state: RegisterOrGetOidcUserState::Init,
            output: None,
        }
    }

    fn subject_key(&self) -> Result<String, RegisterOrGetOidcUserError> {
        Ok(oidc_subject_key(
            &self.input.issuer,
            &self.input.subject_id,
        )?)
    }

    fn fail_on_storage_error(&mut self, event: Event) -> Result<Event, Effects> {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return Err(self.fail(error.into()));
        }
        Ok(event)
    }

    fn fail(&mut self, error: RegisterOrGetOidcUserError) -> Effects {
        let cleanup = self.abort();
        self.state = RegisterOrGetOidcUserState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        self.fail(RegisterOrGetOidcUserError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            got,
        })
    }

    fn handle_start_txn(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected_event(
                "Event::Storage(StorageEvent::TransactionStarted { txn_id })",
                got,
            );
        };

        match self.emit_read_subject_index(txn_id) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_read_subject_index(
        &mut self,
        txn_id: TxnId,
    ) -> Result<Effects, RegisterOrGetOidcUserError> {
        self.state = RegisterOrGetOidcUserState::ReadSubjectIndex { txn_id };
        let key = ByteView::from(self.subject_key()?.into_bytes());
        Ok(smallvec![Effect::Storage(StorageEffect::Read {
            key_space: USER_SUBJECT_INDEX_KEYSPACE.to_string(),
            key,
            txn_id: Some(txn_id),
        })])
    }

    fn handle_read_subject_index(&mut self, event: Event, txn_id: TxnId) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected_event(
                "Event::Storage(StorageEvent::TransactionStarted { txn_id })",
                got,
            );
        };

        match value {
            Some(value) => self.emit_read_existing_user(txn_id, value),
            None => match self.emit_create_user(txn_id) {
                Ok(effects) => effects,
                Err(err) => self.fail(err),
            },
        }
    }

    fn emit_read_existing_user(&mut self, txn_id: TxnId, user_id: ByteView) -> Effects {
        self.state = RegisterOrGetOidcUserState::ReadExistingUser { txn_id };
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: USER_KEYSPACE.to_string(),
            key: user_id,
            txn_id: Some(txn_id),
        })]
    }

    fn emit_create_user(&mut self, txn_id: TxnId) -> Result<Effects, RegisterOrGetOidcUserError> {
        let subject_id = self.subject_key()?;
        let user = User {
            user_id: self.input.user_id,
            name: self.input.name.clone(),
            subject_ids: vec![subject_id.clone()],
            alias_user_ids: Default::default(),
            attributes: Default::default(),
        };

        self.state = RegisterOrGetOidcUserState::WriteUserAndDocumentRevision {
            txn_id,
            user: user.clone(),
        };
        let document_target = DocumentSyncTarget::User {
            user_id: self.input.user_id,
        };
        let admin_target = AdminDocumentTarget::User {
            user_id: self.input.user_id,
        };
        let document_revision = initial_user_document_sync_change(&self.input.actor);
        let mut reducer_state = AdminDocumentReducerState::new(admin_target);
        let admin_events = seed_user_admin_events(&mut reducer_state, &self.input, subject_id)?;
        let mut writes = vec![
            (
                USER_KEYSPACE.to_string(),
                ByteView::from(self.input.user_id.to_bytes()),
                ByteView::from(user.to_bytes(&self.input.actor)?),
            ),
            document_sync_revision_write_entry(&document_target, &document_revision)?,
            admin_document_reducer_state_write_entry(&reducer_state)?,
        ];
        for event in &admin_events {
            let record = new_outbox_record_with_id(
                event.event_id,
                self.input.actor.node_id,
                document_target.clone(),
                Vec::new(),
                DocumentSyncOutboxEvent::AdminOperation {
                    event: Box::new(event.clone()),
                },
            );
            writes.push(outbox_write_entry(&record).map_err(ConversionError::from)?);
        }

        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }

    fn handle_write_user(&mut self, event: Event, txn_id: TxnId, user: User) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
            return self
                .unexpected_event("Event::Storage(StorageEvent::BatchWriteResult { .. })", got);
        };

        match self.emit_write_subject_index(txn_id, user) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_write_subject_index(
        &mut self,
        txn_id: TxnId,
        user: User,
    ) -> Result<Effects, RegisterOrGetOidcUserError> {
        self.state = RegisterOrGetOidcUserState::WriteSubjectIndex {
            txn_id,
            user: user.clone(),
        };
        let effects = rewrite_subject_index_effects(None, &user, txn_id)?;
        Ok(effects)
    }

    fn handle_write_subject_index(&mut self, event: Event, txn_id: TxnId, user: User) -> Effects {
        let got = format!("{event:?}");
        match event {
            Event::Storage(StorageEvent::WriteResult { .. })
            | Event::Storage(StorageEvent::BatchWriteResult { .. })
            | Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {}
            _ => {
                return self.unexpected_event(
                    "Event::Storage(StorageEvent::BatchWriteResult { .. })",
                    got,
                );
            }
        }

        self.emit_commit(txn_id, user, true)
    }

    fn handle_read_existing_user(&mut self, event: Event, txn_id: TxnId) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected_event(
                "Event::Storage(StorageEvent::ReadResult { value, .. })",
                got,
            );
        };

        match self.emit_parsed_user_and_commit(txn_id, value) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_parsed_user_and_commit(
        &mut self,
        txn_id: TxnId,
        value: Option<ByteView>,
    ) -> Result<Effects, RegisterOrGetOidcUserError> {
        let user = User::from_bytes(&value.ok_or_else(|| {
            RegisterOrGetOidcUserError::ConversionError(ConversionError::FromStrError(
                "missing user value".to_string(),
            ))
        })?)?;
        Ok(self.emit_commit(txn_id, user, false))
    }

    fn emit_commit(&mut self, txn_id: TxnId, user: User, announce: bool) -> Effects {
        self.state = RegisterOrGetOidcUserState::CommitTransaction { user, announce };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_commit_txn(&mut self, event: Event, user: User, announce: bool) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.unexpected_event(
                "Event::Storage(StorageEvent::TransactionCommitted { .. })",
                got,
            );
        };

        if announce {
            self.state = RegisterOrGetOidcUserState::ScheduleAdminDocumentOutboxDrain { user };
            smallvec![schedule_outbox_drain_effect()]
        } else {
            self.emit_finish(user)
        }
    }

    fn handle_schedule_admin_document_outbox_drain(&mut self, event: Event, user: User) -> Effects {
        match event {
            Event::Task(TaskEvent::TimerScheduled { .. })
            | Event::Task(TaskEvent::Error { .. }) => self.emit_finish(user),
            other => self.unexpected_event(
                "admin document outbox drain timer schedule",
                format!("{other:?}"),
            ),
        }
    }

    fn emit_finish(&mut self, user: User) -> Effects {
        self.state = RegisterOrGetOidcUserState::Finish;
        self.output = Some(Ok(user));
        smallvec![]
    }
}

impl Operation for RegisterOrGetOidcUserOperation {
    type Output = User;
    type Error = RegisterOrGetOidcUserError;

    fn start(&mut self) -> Effects {
        self.state = RegisterOrGetOidcUserState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match self.fail_on_storage_error(event) {
            Ok(event) => event,
            Err(effects) => return effects,
        };

        match self.state.clone() {
            RegisterOrGetOidcUserState::StartTransaction => self.handle_start_txn(event),
            RegisterOrGetOidcUserState::ReadSubjectIndex { txn_id } => {
                self.handle_read_subject_index(event, txn_id)
            }
            RegisterOrGetOidcUserState::WriteUserAndDocumentRevision { txn_id, user } => {
                self.handle_write_user(event, txn_id, user)
            }
            RegisterOrGetOidcUserState::WriteSubjectIndex { txn_id, user } => {
                self.handle_write_subject_index(event, txn_id, user)
            }
            RegisterOrGetOidcUserState::ReadExistingUser { txn_id } => {
                self.handle_read_existing_user(event, txn_id)
            }
            RegisterOrGetOidcUserState::CommitTransaction { user, announce } => {
                self.handle_commit_txn(event, user, announce)
            }
            RegisterOrGetOidcUserState::ScheduleAdminDocumentOutboxDrain { user } => {
                self.handle_schedule_admin_document_outbox_drain(event, user)
            }
            RegisterOrGetOidcUserState::Init
            | RegisterOrGetOidcUserState::Finish
            | RegisterOrGetOidcUserState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            RegisterOrGetOidcUserState::Finish | RegisterOrGetOidcUserState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(RegisterOrGetOidcUserError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            RegisterOrGetOidcUserState::ReadSubjectIndex { txn_id }
            | RegisterOrGetOidcUserState::ReadExistingUser { txn_id }
            | RegisterOrGetOidcUserState::WriteUserAndDocumentRevision { txn_id, .. }
            | RegisterOrGetOidcUserState::WriteSubjectIndex { txn_id, .. } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => smallvec![],
        }
    }
}

fn seed_user_admin_events(
    state: &mut AdminDocumentReducerState,
    input: &RegisterOrGetOidcUserInput,
    subject_id: String,
) -> Result<Vec<AdminDocumentEvent>, AdminDocumentReducerError> {
    let mut events = Vec::with_capacity(2);
    for operation in [
        AdminDocumentOperation::UserNameSet {
            name: input.name.clone(),
        },
        AdminDocumentOperation::UserSubjectIdAdded { subject_id },
    ] {
        let event = apply_admin_reducer_operation(state, &input.actor, operation)?;
        events.push(event);
    }

    Ok(events)
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

fn current_timestamp_ms() -> u64 {
    u64::try_from(Utc::now().timestamp_millis()).unwrap_or_default()
}

fn initial_user_document_sync_change(actor: &Actor) -> DocumentSyncChange {
    let updated_at_ms = current_timestamp_ms();
    DocumentSyncChange {
        base: None,
        current: DocumentSyncRevision {
            generation: updated_at_ms,
            event_id: Ulid::new(),
            actor: actor.node_id,
            updated_at_ms,
        },
        kind: DocumentSyncChangeKind::Upsert,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        RegisterOrGetOidcUserInput, RegisterOrGetOidcUserOperation, RegisterOrGetOidcUserState,
    };
    use aruna_core::UserId;
    use aruna_core::admin_document_reducer::AdminDocumentReducerState;
    use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
    use aruna_core::document::{
        DocumentSyncChange, DocumentSyncChangeKind, DocumentSyncOutboxEvent,
        DocumentSyncOutboxRecord, DocumentSyncTarget,
    };
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::operation::Operation;
    use aruna_core::storage_entries::{
        admin_document_reducer_state_key, document_sync_revision_key,
    };
    use aruna_core::structs::{Actor, User, oidc_subject_key};
    use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
    use aruna_core::types::TxnId;
    use ulid::Ulid;

    #[tokio::test]
    async fn creates_user_subject_index_and_announces_user_document() {
        let realm_id = aruna_core::structs::RealmId([3u8; 32]);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[4u8; 32]).public(),
            user_id: UserId::nil(realm_id),
            realm_id,
        };
        let user_id = UserId::local(Ulid::new(), realm_id);
        let mut operation = RegisterOrGetOidcUserOperation::new(RegisterOrGetOidcUserInput {
            actor: actor.clone(),
            issuer: "https://issuer.example".to_string(),
            subject_id: "subject-1".to_string(),
            name: "alice".to_string(),
            user_id,
        });
        let expected_user = User {
            user_id,
            name: "alice".to_string(),
            subject_ids: vec![oidc_subject_key("https://issuer.example", "subject-1").unwrap()],
            alias_user_ids: Default::default(),
            attributes: Default::default(),
        };

        let effects = operation.start();
        assert!(matches!(
            effects.first().unwrap(),
            Effect::Storage(StorageEffect::StartTransaction { read: false })
        ));

        let txn_id = TxnId::new();
        let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        assert!(matches!(
            effects.first().unwrap(),
            Effect::Storage(StorageEffect::Read { .. })
        ));
        assert_eq!(
            operation.state,
            RegisterOrGetOidcUserState::ReadSubjectIndex { txn_id }
        );

        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: oidc_subject_key("https://issuer.example", "subject-1")
                .unwrap()
                .into_bytes()
                .into(),
            value: None,
        }));
        let subject_id = oidc_subject_key("https://issuer.example", "subject-1").unwrap();
        let admin_target = AdminDocumentTarget::User { user_id };
        let document_target = DocumentSyncTarget::User { user_id };
        match effects.first().unwrap() {
            Effect::Storage(StorageEffect::BatchWrite { writes, txn_id: id }) => {
                assert_eq!(*id, Some(txn_id));
                assert_eq!(writes.len(), 5);
                let user_write = writes
                    .iter()
                    .find(|(keyspace, _, _)| keyspace == aruna_core::USER_KEYSPACE)
                    .expect("user write is included");
                let stored_user = User::from_bytes(user_write.2.as_ref()).unwrap();
                assert_eq!(stored_user, expected_user);
                assert!(writes.iter().any(
                    |(keyspace, _, _)| keyspace == aruna_core::DOCUMENT_SYNC_REVISION_KEYSPACE
                ));
                let reducer_state_write = writes
                    .iter()
                    .find(|(keyspace, _, _)| keyspace == aruna_core::ADMIN_DOCUMENT_STATE_KEYSPACE)
                    .expect("reducer state write is included");
                assert_eq!(
                    reducer_state_write.1,
                    admin_document_reducer_state_key(&admin_target)
                );
                let reducer_state: AdminDocumentReducerState =
                    postcard::from_bytes(reducer_state_write.2.as_ref()).unwrap();
                assert_eq!(reducer_state.target, admin_target);
                assert_eq!(
                    reducer_state.materialized_user_name(),
                    Some("alice".to_string())
                );
                assert_eq!(reducer_state.materialized_user_subject_ids().len(), 1);
                assert!(
                    reducer_state
                        .materialized_user_subject_ids()
                        .contains(&subject_id)
                );
                assert_eq!(reducer_state.applied_event_ids.len(), 2);
                assert_eq!(reducer_state.clock.sequence_for(&actor.node_id), 2);

                let outbox_records: Vec<DocumentSyncOutboxRecord> = writes
                    .iter()
                    .filter(|(keyspace, _, _)| {
                        keyspace == aruna_core::DOCUMENT_SYNC_OUTBOX_KEYSPACE
                    })
                    .map(|(_, _, value)| postcard::from_bytes(value).unwrap())
                    .collect();
                assert_eq!(outbox_records.len(), 2);
                let mut saw_name = false;
                let mut saw_subject = false;
                for record in outbox_records {
                    assert_eq!(record.target, document_target);
                    assert_eq!(record.node_id, actor.node_id);
                    assert!(record.peers.is_empty());
                    let DocumentSyncOutboxEvent::AdminOperation { event } = record.event else {
                        panic!("unexpected outbox event");
                    };
                    assert_eq!(record.outbox_id, event.event_id);
                    assert_eq!(event.target, admin_target);
                    assert_eq!(event.origin_node_id, actor.node_id);
                    match event.op {
                        AdminDocumentOperation::UserNameSet { name } => {
                            assert_eq!(name, "alice");
                            assert_eq!(event.origin_seq, 1);
                            saw_name = true;
                        }
                        AdminDocumentOperation::UserSubjectIdAdded {
                            subject_id: event_subject,
                        } => {
                            assert_eq!(event_subject, subject_id);
                            assert_eq!(event.origin_seq, 2);
                            saw_subject = true;
                        }
                        other => panic!("unexpected admin operation: {other:?}"),
                    }
                }
                assert!(saw_name);
                assert!(saw_subject);
            }
            other => panic!("unexpected user materialization effect: {other:?}"),
        }

        let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        match effects.first().unwrap() {
            Effect::Storage(StorageEffect::BatchWrite { writes, .. }) => {
                assert_eq!(writes.len(), 1);
                assert_eq!(writes[0].2.as_ref(), user_id.to_storage_key().as_slice());
            }
            other => panic!("unexpected subject index write effect: {other:?}"),
        }

        let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: vec![(
                aruna_core::USER_SUBJECT_INDEX_KEYSPACE.to_string(),
                oidc_subject_key("https://issuer.example", "subject-1")
                    .unwrap()
                    .into_bytes()
                    .into(),
            )],
        }));
        assert!(matches!(
            effects.first().unwrap(),
            Effect::Storage(StorageEffect::CommitTransaction { .. })
        ));

        let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
        assert!(matches!(
            effects.first().unwrap(),
            Effect::Task(TaskEffect::ResetTimer {
                key: TaskKey::DrainDocumentSyncOutbox,
                after: std::time::Duration::ZERO,
            })
        ));

        let effects = operation.step(Event::Task(TaskEvent::TimerScheduled {
            key: TaskKey::DrainDocumentSyncOutbox,
            after: std::time::Duration::ZERO,
        }));
        assert!(effects.is_empty());
        assert_eq!(operation.finalize().unwrap(), expected_user);
    }

    #[tokio::test]
    async fn writes_document_sync_revision_sidecar_with_new_user() {
        let realm_id = aruna_core::structs::RealmId([7u8; 32]);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[8u8; 32]).public(),
            user_id: UserId::nil(realm_id),
            realm_id,
        };
        let user_id = UserId::local(Ulid::new(), realm_id);
        let mut operation = RegisterOrGetOidcUserOperation::new(RegisterOrGetOidcUserInput {
            actor: actor.clone(),
            issuer: "https://issuer.example".to_string(),
            subject_id: "subject-3".to_string(),
            name: "carol".to_string(),
            user_id,
        });
        let document_target = DocumentSyncTarget::User { user_id };

        operation.start();
        let txn_id = TxnId::new();
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: oidc_subject_key("https://issuer.example", "subject-3")
                .unwrap()
                .into_bytes()
                .into(),
            value: None,
        }));

        let [
            Effect::Storage(StorageEffect::BatchWrite {
                writes,
                txn_id: Some(write_txn_id),
            }),
        ] = effects.as_slice()
        else {
            panic!("expected new user batch write, got {effects:?}");
        };
        assert_eq!(*write_txn_id, txn_id);
        assert_eq!(writes.len(), 5);
        assert!(
            writes
                .iter()
                .any(|(keyspace, _, _)| keyspace == aruna_core::USER_KEYSPACE)
        );

        let (revision_key, revision): (_, DocumentSyncChange) = writes
            .iter()
            .find(|(keyspace, _, _)| keyspace == aruna_core::DOCUMENT_SYNC_REVISION_KEYSPACE)
            .map(|(_, key, value)| {
                (
                    key,
                    postcard::from_bytes(value).expect("revision sidecar decodes"),
                )
            })
            .expect("revision sidecar write exists");
        assert_eq!(revision_key, &document_sync_revision_key(&document_target));
        assert_eq!(revision.base, None);
        assert_eq!(revision.current.actor, actor.node_id);
        assert!(revision.current.generation > 0);
        assert!(revision.current.updated_at_ms > 0);
        assert_eq!(revision.kind, DocumentSyncChangeKind::Upsert);
    }

    #[tokio::test]
    async fn existing_user_is_returned_without_new_announcement() {
        let realm_id = aruna_core::structs::RealmId([5u8; 32]);
        let user_id = UserId::local(Ulid::new(), realm_id);
        let existing_user = User {
            user_id,
            name: "bob".to_string(),
            subject_ids: vec![oidc_subject_key("https://issuer.example", "subject-2").unwrap()],
            alias_user_ids: Default::default(),
            attributes: Default::default(),
        };
        let mut operation = RegisterOrGetOidcUserOperation::new(RegisterOrGetOidcUserInput {
            actor: Actor {
                node_id: iroh::SecretKey::from_bytes(&[6u8; 32]).public(),
                user_id: UserId::nil(realm_id),
                realm_id,
            },
            issuer: "https://issuer.example".to_string(),
            subject_id: "subject-2".to_string(),
            name: "ignored".to_string(),
            user_id: UserId::local(Ulid::new(), realm_id),
        });

        operation.start();
        let txn_id = TxnId::new();
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: oidc_subject_key("https://issuer.example", "subject-2")
                .unwrap()
                .into_bytes()
                .into(),
            value: Some(user_id.to_storage_key().into()),
        }));
        assert!(matches!(
            effects.first().unwrap(),
            Effect::Storage(StorageEffect::Read { .. })
        ));

        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: user_id.to_bytes().into(),
            value: Some(
                existing_user
                    .to_bytes(&Actor {
                        node_id: iroh::SecretKey::from_bytes(&[6u8; 32]).public(),
                        user_id: UserId::nil(aruna_core::structs::RealmId([5u8; 32])),
                        realm_id: aruna_core::structs::RealmId([5u8; 32]),
                    })
                    .unwrap()
                    .into(),
            ),
        }));
        assert!(matches!(
            effects.first().unwrap(),
            Effect::Storage(StorageEffect::CommitTransaction { .. })
        ));

        let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
        assert!(effects.is_empty());
        assert_eq!(operation.finalize().unwrap(), existing_user);
    }
}
