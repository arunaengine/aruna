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
}

impl RegisterOrGetOidcUserOperation {
    pub fn new(input: RegisterOrGetOidcUserInput) -> Self {
        Self {
            input,
            state: RegisterOrGetOidcUserState::Init,
            output: None,
        }
    }

    fn subject_key(&self) -> String {
        oidc_subject_key(&self.input.issuer, &self.input.subject_id)
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

    fn emit_read_subject_index(&mut self, txn_id: TxnId) -> Effects {
        self.state = RegisterOrGetOidcUserState::ReadSubjectIndex { txn_id };
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: USER_SUBJECT_INDEX_KEYSPACE.to_string(),
            key: ByteView::from(self.subject_key().into_bytes()),
            txn_id: Some(txn_id),
        })]
    }

    fn emit_read_existing_user(&mut self, txn_id: TxnId, user_id: UserId) -> Effects {
        self.state = RegisterOrGetOidcUserState::ReadExistingUser { txn_id };
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: USER_KEYSPACE.to_string(),
            key: ByteView::from(user_id.to_bytes()),
            txn_id: Some(txn_id),
        })]
    }

    fn emit_create_user(&mut self, txn_id: TxnId) -> Result<Effects, RegisterOrGetOidcUserError> {
        let user = User {
            user_id: self.input.user_id,
            name: self.input.name.clone(),
            subject_ids: vec![self.subject_key()],
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

    fn emit_write_subject_index(&mut self, txn_id: TxnId, user: User) -> Effects {
        self.state = RegisterOrGetOidcUserState::WriteSubjectIndex {
            txn_id,
            user: user.clone(),
        };
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: USER_SUBJECT_INDEX_KEYSPACE.to_string(),
            key: ByteView::from(self.subject_key().into_bytes()),
            value: ByteView::from(user.user_id.to_string().into_bytes()),
            txn_id: Some(txn_id),
        })]
    }

    fn realm_config_ref(&self) -> AutomergeDocumentVariant {
        AutomergeDocumentVariant::RealmConfig {
            realm_id: self.input.actor.realm_id.clone(),
        }
    }

    fn emit_read_realm_config(&mut self, txn_id: TxnId, user: User) -> Effects {
        self.state = RegisterOrGetOidcUserState::ReadRealmConfig {
            txn_id,
            user: user.clone(),
        };
        smallvec![read_effect(&self.realm_config_ref(), Some(txn_id))]
    }

    fn emit_write_realm_config(&mut self, txn_id: TxnId, user: User, bytes: Vec<u8>) -> Effects {
        self.state = RegisterOrGetOidcUserState::WriteRealmConfig {
            txn_id,
            user: user.clone(),
        };
        smallvec![write_effect(&self.realm_config_ref(), bytes, Some(txn_id))]
    }

    fn emit_commit(&mut self, txn_id: TxnId, user: User, announce: bool) -> Effects {
        self.state = RegisterOrGetOidcUserState::CommitTransaction { user, announce };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn emit_announce(&mut self, user: User) -> Effects {
        self.state = RegisterOrGetOidcUserState::AnnounceUser { user };
        smallvec![Effect::SubOperation(boxed_suboperation(
            AnnounceTopicOperation::new(
                AutomergeDocumentVariant::RealmConfig {
                    realm_id: self.input.actor.realm_id.clone(),
                }
                .topic_id(),
                self.input.actor.node_id,
            ),
            |result| Event::SubOperation(SubOperationEvent::TopicAnnouncementResult {
                result: result.map_err(|error| error.to_string()),
            }),
        ))]
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
        match self.state.clone() {
            RegisterOrGetOidcUserState::StartTransaction => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.emit_read_subject_index(txn_id)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("transaction start result", format!("{other:?}")),
            },
            RegisterOrGetOidcUserState::ReadSubjectIndex { txn_id } => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => match value {
                    Some(value) => {
                        let Some(user_id) = std::str::from_utf8(value.as_ref())
                            .ok()
                            .and_then(|value| UserId::from_string(value).ok())
                        else {
                            return self.fail(RegisterOrGetOidcUserError::ConversionError(
                                ConversionError::InvalidUserId,
                            ));
                        };

                        self.emit_read_existing_user(txn_id, user_id)
                    }
                    None => match self.emit_create_user(txn_id) {
                        Ok(effects) => effects,
                        Err(error) => self.fail(error),
                    },
                },
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("subject index read result", format!("{other:?}")),
            },
            RegisterOrGetOidcUserState::ReadExistingUser { txn_id } => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    let Some(value) = value else {
                        return match self.emit_create_user(txn_id) {
                            Ok(effects) => effects,
                            Err(error) => self.fail(error),
                        };
                    };

                    let user = match User::from_bytes(&value) {
                        Ok(user) => user,
                        Err(error) => return self.fail(error.into()),
                    };

                    self.emit_commit(txn_id, user, false)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("existing user read result", format!("{other:?}")),
            },
            RegisterOrGetOidcUserState::WriteUser { txn_id, user } => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => {
                    self.emit_write_subject_index(txn_id, user)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("user write result", format!("{other:?}")),
            },
            RegisterOrGetOidcUserState::WriteSubjectIndex { txn_id, user } => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => {
                    self.emit_read_realm_config(txn_id, user)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("subject index write result", format!("{other:?}")),
            },
            RegisterOrGetOidcUserState::ReadRealmConfig { txn_id, user } => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    let mut document = match value.as_deref() {
                        Some(bytes) => match RealmConfigDocument::from_bytes(bytes) {
                            Ok(document) => document,
                            Err(error) => return self.fail(error.into()),
                        },
                        None => RealmConfigDocument::default_for_realm(
                            self.input.actor.realm_id.clone(),
                        ),
                    };
                    if !document
                        .users
                        .iter()
                        .any(|existing| existing.user_id == user.user_id)
                    {
                        document.users.push(user.clone());
                    }
                    let bytes = match document.to_bytes(&self.input.actor) {
                        Ok(bytes) => bytes,
                        Err(error) => return self.fail(error.into()),
                    };

                    self.emit_write_realm_config(txn_id, user, bytes)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("realm config read result", format!("{other:?}")),
            },
            RegisterOrGetOidcUserState::WriteRealmConfig { txn_id, user } => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => {
                    self.emit_commit(txn_id, user, true)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("realm config write result", format!("{other:?}")),
            },
            RegisterOrGetOidcUserState::CommitTransaction { user, announce } => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    if announce {
                        self.emit_announce(user)
                    } else {
                        self.state = RegisterOrGetOidcUserState::Finish;
                        self.output = Some(Ok(user));
                        smallvec![]
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("transaction commit result", format!("{other:?}")),
            },
            RegisterOrGetOidcUserState::AnnounceUser { user } => match event {
                Event::SubOperation(SubOperationEvent::TopicAnnouncementResult { result }) => {
                    match result {
                        Ok(()) => {
                            self.state = RegisterOrGetOidcUserState::Finish;
                            self.output = Some(Ok(user));
                            smallvec![]
                        }
                        Err(error) => {
                            self.fail(RegisterOrGetOidcUserError::TopicAnnouncement(error))
                        }
                    }
                }
                other => self.unexpected_event("topic announcement result", format!("{other:?}")),
            },
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
            user_id: Ulid::from_bytes([0u8; 16]),
            realm_id: realm_id.clone(),
        };
        let user_id = Ulid::new();
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
            subject_ids: vec![oidc_subject_key("https://issuer.example", "subject-1")],
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
                .into_bytes()
                .into(),
        }));
        assert!(matches!(
            effects.first().unwrap(),
            Effect::Storage(StorageEffect::Read { .. })
        ));

        let realm_config = RealmConfigDocument::default_for_realm(realm_id.clone());
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
        let user_id = Ulid::new();
        let existing_user = User {
            user_id,
            name: "bob".to_string(),
            subject_ids: vec![oidc_subject_key("https://issuer.example", "subject-2")],
        };
        let mut operation = RegisterOrGetOidcUserOperation::new(RegisterOrGetOidcUserInput {
            actor: Actor {
                node_id: iroh::SecretKey::from_bytes(&[6u8; 32]).public(),
                user_id: Ulid::from_bytes([0u8; 16]),
                realm_id,
            },
            issuer: "https://issuer.example".to_string(),
            subject_id: "subject-2".to_string(),
            name: "ignored".to_string(),
            user_id: Ulid::new(),
        });

        operation.start();
        let txn_id = TxnId::new();
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: oidc_subject_key("https://issuer.example", "subject-2")
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
                        user_id: Ulid::from_bytes([0u8; 16]),
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
