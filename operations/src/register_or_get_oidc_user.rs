use aruna_core::automerge::AutomergeDocumentVariant;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{Actor, RealmConfigDocument, User, oidc_subject_key};
use aruna_core::types::{Effects, TxnId, UserId};
use aruna_core::{USER_KEYSPACE, USER_SUBJECT_INDEX_KEYSPACE};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;

use crate::announce::AnnounceTopicOperation;
use crate::automerge::repository::{read_effect, write_effect};

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
    WriteUser { txn_id: TxnId, user: User },
    WriteSubjectIndex { txn_id: TxnId, user: User },
    ReadRealmConfig { txn_id: TxnId, user: User },
    WriteRealmConfig { txn_id: TxnId, user: User },
    CommitTransaction { user: User, announce: bool },
    AnnounceUser { user: User },
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum RegisterOrGetOidcUserError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("topic announcement failed: {0}")]
    TopicAnnouncement(String),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
    #[error("registration did not finish")]
    NotFinished,
    #[error("{0} not found")]
    NotFound(String),
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
        let user = User {
            user_id: self.input.user_id,
            name: self.input.name.clone(),
            subject_ids: vec![self.subject_key()?],
        };

        self.state = RegisterOrGetOidcUserState::WriteUser {
            txn_id,
            user: user.clone(),
        };
        Ok(smallvec![Effect::Storage(StorageEffect::Write {
            key_space: USER_KEYSPACE.to_string(),
            key: ByteView::from(self.input.user_id.to_bytes()),
            value: ByteView::from(user.to_bytes(&self.input.actor)?),
            txn_id: Some(txn_id),
        })])
    }

    fn handle_write_user(&mut self, event: Event, txn_id: TxnId, user: User) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.unexpected_event(
                "Event::Storage(StorageEvent::ReadResult { value, .. })",
                got,
            );
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
        let key = ByteView::from(self.subject_key()?.into_bytes());
        Ok(smallvec![Effect::Storage(StorageEffect::Write {
            key_space: USER_SUBJECT_INDEX_KEYSPACE.to_string(),
            key,
            value: ByteView::from(user.user_id.to_string().into_bytes()),
            txn_id: Some(txn_id),
        })])
    }

    fn handle_write_subject_index(&mut self, event: Event, txn_id: TxnId, user: User) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.unexpected_event("Event::Storage(StorageEvent::WriteResult { .. })", got);
        };

        self.emit_read_realm_config(txn_id, user)
    }

    fn realm_config_ref(&self) -> AutomergeDocumentVariant {
        AutomergeDocumentVariant::RealmConfig {
            realm_id: self.input.actor.realm_id,
        }
    }

    fn emit_read_realm_config(&mut self, txn_id: TxnId, user: User) -> Effects {
        self.state = RegisterOrGetOidcUserState::ReadRealmConfig {
            txn_id,
            user: user.clone(),
        };
        smallvec![read_effect(&self.realm_config_ref(), Some(txn_id))]
    }

    fn handle_read_realm_config(&mut self, event: Event, txn_id: TxnId, user: User) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected_event(
                "Event::Storage(StorageEvent::ReadResult { value, .. })",
                got,
            );
        };

        match self.emit_write_realm_config(txn_id, user, value) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_write_realm_config(
        &mut self,
        txn_id: TxnId,
        user: User,
        value: Option<ByteView>,
    ) -> Result<Effects, RegisterOrGetOidcUserError> {
        let mut document = RealmConfigDocument::from_bytes(
            &value
                .ok_or_else(|| RegisterOrGetOidcUserError::NotFound("RealmConfig".to_string()))?,
        )?;
        if !document
            .users
            .iter()
            .any(|existing| existing.user_id == user.user_id)
        {
            document.users.push(user.clone());
        }
        let bytes = document.to_bytes(&self.input.actor)?;
        self.state = RegisterOrGetOidcUserState::WriteRealmConfig {
            txn_id,
            user: user.clone(),
        };

        Ok(smallvec![write_effect(
            &self.realm_config_ref(),
            bytes,
            Some(txn_id)
        )])
    }

    fn handle_write_realm_config(&mut self, event: Event, txn_id: TxnId, user: User) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.unexpected_event("Event::Storage(StorageEvent::WriteResult { .. })", got);
        };
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
        let user = User::from_bytes(
            &value.ok_or_else(|| RegisterOrGetOidcUserError::NotFound("User".to_string()))?,
        )?;
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
            self.emit_announce(user)
        } else {
            self.emit_finish(user)
        }
    }

    fn emit_announce(&mut self, user: User) -> Effects {
        self.state = RegisterOrGetOidcUserState::AnnounceUser { user };
        smallvec![Effect::SubOperation(boxed_suboperation(
            AnnounceTopicOperation::new(
                AutomergeDocumentVariant::RealmConfig {
                    realm_id: self.input.actor.realm_id,
                }
                .topic_id(),
                self.input.actor.node_id,
            ),
            |result| Event::SubOperation(SubOperationEvent::TopicAnnouncementResult {
                result: result.map_err(|error| error.to_string()),
            }),
        ))]
    }

    fn handle_announce_user(&mut self, event: Event, user: User) -> Effects {
        let got = format!("{event:?}");
        let Event::SubOperation(SubOperationEvent::TopicAnnouncementResult { result }) = event
        else {
            return self.unexpected_event(
                "Event::SubOperation(SubOperationEvent::TopicAnnouncementResult { result })",
                got,
            );
        };

        match result {
            Ok(_) => self.emit_finish(user),
            Err(err) => self.fail(RegisterOrGetOidcUserError::TopicAnnouncement(err)),
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
            RegisterOrGetOidcUserState::WriteUser { txn_id, user } => {
                self.handle_write_user(event, txn_id, user)
            }
            RegisterOrGetOidcUserState::WriteSubjectIndex { txn_id, user } => {
                self.handle_write_subject_index(event, txn_id, user)
            }
            RegisterOrGetOidcUserState::ReadRealmConfig { txn_id, user } => {
                self.handle_read_realm_config(event, txn_id, user)
            }
            RegisterOrGetOidcUserState::WriteRealmConfig { txn_id, user } => {
                self.handle_write_realm_config(event, txn_id, user)
            }
            RegisterOrGetOidcUserState::ReadExistingUser { txn_id } => {
                self.handle_read_existing_user(event, txn_id)
            }
            RegisterOrGetOidcUserState::CommitTransaction { user, announce } => {
                self.handle_commit_txn(event, user, announce)
            }
            RegisterOrGetOidcUserState::AnnounceUser { user } => {
                self.handle_announce_user(event, user)
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
            | RegisterOrGetOidcUserState::WriteUser { txn_id, .. }
            | RegisterOrGetOidcUserState::WriteSubjectIndex { txn_id, .. }
            | RegisterOrGetOidcUserState::ReadRealmConfig { txn_id, .. }
            | RegisterOrGetOidcUserState::WriteRealmConfig { txn_id, .. } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => smallvec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        RegisterOrGetOidcUserInput, RegisterOrGetOidcUserOperation, RegisterOrGetOidcUserState,
    };
    use aruna_core::UserId;
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
    use aruna_core::operation::Operation;
    use aruna_core::structs::{Actor, RealmConfigDocument, User, oidc_subject_key};
    use aruna_core::types::TxnId;
    use ulid::Ulid;

    #[tokio::test]
    async fn creates_user_subject_index_and_realm_config_entry() {
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
        match effects.first().unwrap() {
            Effect::Storage(StorageEffect::Write { value, .. }) => {
                let stored_user = User::from_bytes(value).unwrap();
                assert_eq!(stored_user, expected_user);
            }
            other => panic!("unexpected user write effect: {other:?}"),
        }

        let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
            key: user_id.to_bytes().into(),
        }));
        match effects.first().unwrap() {
            Effect::Storage(StorageEffect::Write { value, .. }) => {
                assert_eq!(
                    String::from_utf8(value.to_vec()).unwrap(),
                    user_id.to_string()
                );
            }
            other => panic!("unexpected subject index write effect: {other:?}"),
        }

        let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
            key: oidc_subject_key("https://issuer.example", "subject-1")
                .unwrap()
                .into_bytes()
                .into(),
        }));
        assert!(matches!(
            effects.first().unwrap(),
            Effect::Storage(StorageEffect::Read { .. })
        ));

        let realm_config = RealmConfigDocument::default_for_realm(realm_id, vec![]);
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: realm_id.as_bytes().to_vec().into(),
            value: Some(realm_config.to_bytes(&actor).unwrap().into()),
        }));
        match effects.first().unwrap() {
            Effect::Storage(StorageEffect::Write { value, .. }) => {
                let stored_config = RealmConfigDocument::from_bytes(value).unwrap();
                assert_eq!(stored_config.users, vec![expected_user.clone()]);
            }
            other => panic!("unexpected realm config write effect: {other:?}"),
        }

        let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
            key: realm_id.as_bytes().to_vec().into(),
        }));
        assert!(matches!(
            effects.first().unwrap(),
            Effect::Storage(StorageEffect::CommitTransaction { .. })
        ));

        let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
        assert!(matches!(effects.first().unwrap(), Effect::SubOperation(_)));

        let effects = operation.step(Event::SubOperation(
            SubOperationEvent::TopicAnnouncementResult { result: Ok(()) },
        ));
        assert!(effects.is_empty());
        assert_eq!(operation.finalize().unwrap(), expected_user);
    }

    #[tokio::test]
    async fn existing_user_is_returned_without_duplicate_realm_config_entry() {
        let realm_id = aruna_core::structs::RealmId([5u8; 32]);
        let user_id = UserId::local(Ulid::new(), realm_id);
        let existing_user = User {
            user_id,
            name: "bob".to_string(),
            subject_ids: vec![oidc_subject_key("https://issuer.example", "subject-2").unwrap()],
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
            value: Some(user_id.to_string().into_bytes().into()),
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
