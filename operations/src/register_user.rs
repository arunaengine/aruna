use aruna_core::effects::Effect;
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{Actor, AuthContext, Permission, User};
use aruna_core::types::{Effects, UserId};
use smallvec::smallvec;
use thiserror::Error;

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::create_user_record::{
    CreateUserRecordError, CreateUserRecordInput, CreateUserRecordOperation,
};

#[derive(Clone, Debug, PartialEq)]
pub struct RegisterUserInput {
    pub actor: Actor,
    pub user_id: UserId,
    pub name: String,
    pub subject_ids: Vec<String>,
}

#[derive(PartialEq)]
pub struct RegisterUserOperation {
    input: RegisterUserInput,
    create_user: CreateUserRecordOperation,
    state: RegisterUserState,
    output: Option<Result<User, RegisterUserError>>,
}

impl std::fmt::Debug for RegisterUserOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisterUserOperation")
            .field("input", &self.input)
            .field("state", &self.state)
            .field("output", &self.output)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RegisterUserState {
    Init,
    Auth,
    CreateUser,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum RegisterUserError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("AnnouncementError: `{0}`")]
    AnnouncementError(String),
    #[error("automerge announcement failed: {0}")]
    AutomergeState(String),
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("No realm config found")]
    NoRealmConfigFound,
    #[error("Unauthorized")]
    Unauthorized,
    #[error(transparent)]
    CheckPermissionsError(#[from] AuthorizationError),
    #[error(transparent)]
    CreateUserRecordError(#[from] CreateUserRecordError),
    #[error("Register user did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: RegisterUserState,
        expected: &'static str,
        got: String,
    },
}

impl RegisterUserOperation {
    pub fn new(input: RegisterUserInput) -> Self {
        RegisterUserOperation {
            create_user: CreateUserRecordOperation::new(CreateUserRecordInput {
                actor: input.actor.clone(),
                user_id: input.user_id,
                name: input.name.clone(),
                subject_ids: input.subject_ids.clone(),
            }),
            input,
            state: RegisterUserState::Init,
            output: None,
        }
    }
    fn auth_context(&self) -> AuthContext {
        AuthContext {
            user_id: self.input.actor.user_id,
            realm_id: self.input.actor.realm_id.clone(),
            path_restrictions: None,
        }
    }

    fn fail(&mut self, err: RegisterUserError) -> Effects {
        self.state = RegisterUserState::Error;
        self.output = Some(Err(err));
        self.abort()
    }

    fn fail_with_cleanup(&mut self, err: RegisterUserError, cleanup_effects: Effects) -> Effects {
        self.state = RegisterUserState::Error;
        self.output = Some(Err(err));
        cleanup_effects
    }

    fn unexpected_event(
        &mut self,
        state: RegisterUserState,
        expected: &'static str,
        got: String,
    ) -> Effects {
        let cleanup_effects = self.abort();
        self.fail_with_cleanup(
            RegisterUserError::UnexpectedEvent {
                state,
                expected,
                got,
            },
            cleanup_effects,
        )
    }

    fn fail_on_storage_error(&mut self, event: Event) -> Result<Event, Effects> {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return Err(self.fail(error.into()));
        }

        Ok(event)
    }

    fn handle_authorization(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::SubOperation(SubOperationEvent::AuthorizationResult)",
                got,
            );
        };

        match self.emit_create_user(allowed) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_create_user(
        &mut self,
        auth_result: Result<bool, AuthorizationError>,
    ) -> Result<Effects, RegisterUserError> {
        if auth_result? {
            self.state = RegisterUserState::CreateUser;
            Ok(self.create_user.start())
        } else {
            Err(RegisterUserError::Unauthorized)
        }
    }
}

impl Operation for RegisterUserOperation {
    type Output = User;
    type Error = RegisterUserError;

    fn start(&mut self) -> Effects {
        self.state = RegisterUserState::Auth;

        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: self.auth_context(),
                path: format!(
                    "/{}/admin/u/{}",
                    self.input.actor.realm_id, self.input.user_id
                ),
                required_permission: Permission::WRITE,
            }),
            |result| Event::SubOperation(SubOperationEvent::AuthorizationResult {
                allowed: result
            }),
        ))]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match self.fail_on_storage_error(event) {
            Ok(event) => event,
            Err(effects) => return effects,
        };

        match self.state.clone() {
            RegisterUserState::Auth => self.handle_authorization(event),
            RegisterUserState::CreateUser => {
                let effects = self.create_user.step(event);
                if self.create_user.is_complete() {
                    let finalized = std::mem::replace(
                        &mut self.create_user,
                        CreateUserRecordOperation::new(CreateUserRecordInput {
                            actor: self.input.actor.clone(),
                            user_id: self.input.user_id,
                            name: self.input.name.clone(),
                            subject_ids: self.input.subject_ids.clone(),
                        }),
                    )
                    .finalize();
                    match finalized {
                        Ok(user) => {
                            self.state = RegisterUserState::Finish;
                            self.output = Some(Ok(user));
                        }
                        Err(err) => return self.fail(err.into()),
                    }
                }
                effects
            }
            RegisterUserState::Init | RegisterUserState::Finish | RegisterUserState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            RegisterUserState::Finish | RegisterUserState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or_else(|| RegisterUserError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            RegisterUserState::CreateUser => self.create_user.abort(),
            _ => smallvec![],
        }
    }
}

#[cfg(test)]
mod test {
    use crate::register_user::{RegisterUserInput, RegisterUserOperation, RegisterUserState};
    use aruna_core::effects::Effect;
    use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
    use aruna_core::keyspaces::{REALM_CONFIG_KEYSPACE, USER_KEYSPACE, USER_SUBJECT_INDEX_KEYSPACE};
    use aruna_core::operation::Operation;
    use aruna_core::structs::{Actor, RealmConfigDocument, User};
    use aruna_core::types::TxnId;
    use byteview::ByteView;
    use ulid::Ulid;

    #[tokio::test]
    pub async fn test_user_registration() {
        //
        // Inputs
        //
        let actor_user_id = Ulid::new();
        let registered_user_id = Ulid::new();
        let realm_id = aruna_core::structs::RealmId([0u8; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let actor = Actor {
            node_id,
            user_id: actor_user_id,
            realm_id: realm_id.clone(),
        };
        let register_user_input = RegisterUserInput {
            actor: actor.clone(),
            user_id: registered_user_id,
            name: "test_user".to_string(),
            subject_ids: vec!["sub-1".to_string(), "sub-2".to_string()],
        };
        let expected_user = User {
            user_id: registered_user_id,
            name: register_user_input.name.clone(),
            subject_ids: register_user_input.subject_ids.clone(),
        };

        //
        // Steps
        //
        let mut register_user_operation = RegisterUserOperation::new(register_user_input.clone());
        assert_eq!(register_user_operation.state, RegisterUserState::Init);

        let effects = register_user_operation.start();
        let auth_effect = effects.first().unwrap();
        assert!(matches!(auth_effect, Effect::SubOperation(_)));
        assert_eq!(register_user_operation.state, RegisterUserState::Auth);

        let effects = register_user_operation.step(Event::SubOperation(
            SubOperationEvent::AuthorizationResult { allowed: Ok(true) },
        ));
        let txn_effect = effects.first().unwrap();
        assert_eq!(
            txn_effect,
            &Effect::Storage(aruna_core::effects::StorageEffect::StartTransaction { read: false })
        );
        assert_eq!(register_user_operation.state, RegisterUserState::CreateUser);

        let txn_id = TxnId::new();
        let effects = register_user_operation.step(Event::Storage(
            aruna_core::events::StorageEvent::TransactionStarted { txn_id },
        ));
        let create_user_effect = effects.first().unwrap();
        match create_user_effect {
            Effect::Storage(aruna_core::effects::StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: effect_txn_id,
            }) => {
                assert_eq!(key_space, USER_KEYSPACE);
                assert_eq!(key, &registered_user_id.to_bytes().into());
                assert_eq!(effect_txn_id, &Some(txn_id));
                let stored_user = User::from_bytes(value).unwrap();
                assert_eq!(stored_user, expected_user);
            }
            other => panic!("unexpected create user effect: {other:?}"),
        }
        assert_eq!(
            register_user_operation.state,
            RegisterUserState::CreateUser
        );

        let effects = register_user_operation.step(Event::Storage(StorageEvent::WriteResult {
            key: registered_user_id.to_bytes().into(),
        }));
        let write_subject_index_effect = effects.first().unwrap();
        match write_subject_index_effect {
            Effect::Storage(aruna_core::effects::StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: effect_txn_id,
            }) => {
                assert_eq!(key_space, USER_SUBJECT_INDEX_KEYSPACE);
                assert_eq!(key.as_ref(), b"sub-1");
                assert_eq!(value.as_ref(), registered_user_id.to_string().as_bytes());
                assert_eq!(effect_txn_id, &Some(txn_id));
            }
            other => panic!("unexpected write subject index effect: {other:?}"),
        }
        assert_eq!(
            register_user_operation.state,
            RegisterUserState::CreateUser
        );

        let effects = register_user_operation.step(Event::Storage(StorageEvent::WriteResult {
            key: ByteView::from("sub-1".as_bytes().to_vec()),
        }));
        let second_subject_index_effect = effects.first().unwrap();
        match second_subject_index_effect {
            Effect::Storage(aruna_core::effects::StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: effect_txn_id,
            }) => {
                assert_eq!(key_space, USER_SUBJECT_INDEX_KEYSPACE);
                assert_eq!(key.as_ref(), b"sub-2");
                assert_eq!(value.as_ref(), registered_user_id.to_string().as_bytes());
                assert_eq!(effect_txn_id, &Some(txn_id));
            }
            other => panic!("unexpected second subject index effect: {other:?}"),
        }

        let effects = register_user_operation.step(Event::Storage(StorageEvent::WriteResult {
            key: ByteView::from("sub-2".as_bytes().to_vec()),
        }));
        let read_realm_config_effect = effects.first().unwrap();
        match read_realm_config_effect {
            Effect::Storage(aruna_core::effects::StorageEffect::Read {
                key_space,
                txn_id: effect_txn_id,
                ..
            }) => {
                assert_eq!(key_space, REALM_CONFIG_KEYSPACE);
                assert_eq!(effect_txn_id, &Some(txn_id));
            }
            other => panic!("unexpected read realm config effect: {other:?}"),
        }

        let realm_config = RealmConfigDocument::default_for_realm(realm_id.clone());
        let effects = register_user_operation.step(Event::Storage(StorageEvent::ReadResult {
            key: realm_id.as_bytes().to_vec().into(),
            value: Some(realm_config.to_bytes(&actor).unwrap().into()),
        }));
        let write_realm_config_effect = effects.first().unwrap();
        match write_realm_config_effect {
            Effect::Storage(aruna_core::effects::StorageEffect::Write {
                key_space,
                value,
                txn_id: effect_txn_id,
                ..
            }) => {
                assert_eq!(key_space, REALM_CONFIG_KEYSPACE);
                assert_eq!(effect_txn_id, &Some(txn_id));
                let stored_config = RealmConfigDocument::from_bytes(value).unwrap();
                assert_eq!(stored_config.users, vec![expected_user.clone()]);
            }
            other => panic!("unexpected write realm config effect: {other:?}"),
        }

        let effects = register_user_operation.step(Event::Storage(StorageEvent::WriteResult {
            key: realm_id.as_bytes().to_vec().into(),
        }));
        let commit_transaction_effect = effects.first().unwrap();
        match commit_transaction_effect {
            Effect::Storage(aruna_core::effects::StorageEffect::CommitTransaction {
                txn_id: effect_txn_id,
            }) => {
                assert_eq!(effect_txn_id, &txn_id);
            }
            other => panic!("unexpected commit transaction effect: {other:?}"),
        }
        assert_eq!(
            register_user_operation.state,
            RegisterUserState::CreateUser
        );

        let effects =
            register_user_operation.step(Event::Storage(StorageEvent::TransactionCommitted {
                txn_id,
            }));
        let announce_user_effect = effects.first().unwrap();
        assert!(matches!(announce_user_effect, Effect::SubOperation(_)));

        let effects = register_user_operation.step(Event::SubOperation(
            SubOperationEvent::TopicAnnouncementResult { result: Ok(()) },
        ));
        assert!(effects.is_empty());
        assert_eq!(register_user_operation.state, RegisterUserState::Finish);
        assert_eq!(register_user_operation.finalize().unwrap(), expected_user);
    }

    #[tokio::test]
    pub async fn test_user_inserted_once_into_realm_config() {
        let actor_user_id = Ulid::new();
        let registered_user_id = Ulid::new();
        let realm_id = aruna_core::structs::RealmId([1u8; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
        let actor = Actor {
            node_id,
            user_id: actor_user_id,
            realm_id: realm_id.clone(),
        };
        let expected_user = User {
            user_id: registered_user_id,
            name: "existing".to_string(),
            subject_ids: vec!["sub".to_string()],
        };
        let mut operation = RegisterUserOperation::new(RegisterUserInput {
            actor: actor.clone(),
            user_id: registered_user_id,
            name: expected_user.name.clone(),
            subject_ids: expected_user.subject_ids.clone(),
        });

        operation.start();
        operation.step(Event::SubOperation(
            SubOperationEvent::AuthorizationResult { allowed: Ok(true) },
        ));
        let txn_id = TxnId::new();
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        operation.step(Event::Storage(StorageEvent::WriteResult {
            key: registered_user_id.to_bytes().into(),
        }));
        let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
            key: ByteView::from("sub".as_bytes().to_vec()),
        }));
        assert!(matches!(
            effects.first().unwrap(),
            Effect::Storage(aruna_core::effects::StorageEffect::Read { .. })
        ));

        let existing_config = RealmConfigDocument {
            realm_id: realm_id.clone(),
            metadata_replication: RealmConfigDocument::default_for_realm(realm_id.clone())
                .metadata_replication,
            oidc_providers: vec![],
            users: vec![expected_user.clone()],
        };
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: realm_id.as_bytes().to_vec().into(),
            value: Some(existing_config.to_bytes(&actor).unwrap().into()),
        }));

        match effects.first().unwrap() {
            Effect::Storage(aruna_core::effects::StorageEffect::Write { value, .. }) => {
                let stored_config = RealmConfigDocument::from_bytes(value).unwrap();
                assert_eq!(stored_config.users, vec![expected_user]);
            }
            other => panic!("unexpected realm config write effect: {other:?}"),
        }
    }
}
