use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{Actor, AuthContext, Permission, User};
use aruna_core::types::{Effects, TxnId, UserId};
use aruna_core::{AutomergeDocumentVariant, USER_KEYSPACE};
use smallvec::smallvec;
use thiserror::Error;

use crate::announce::AnnounceTopicOperation;
use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};

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
    StartTxn,
    CreateUser { txn_id: TxnId, user: User },
    CommitTxn { user: User },
    AnnounceUser { user: User },
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
    #[error("Unauthorized")]
    Unauthorized,
    #[error(transparent)]
    CheckPermissionsError(#[from] AuthorizationError),
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

        match self.emit_start_transaction(allowed) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_start_transaction(
        &mut self,
        auth_result: Result<bool, AuthorizationError>,
    ) -> Result<Effects, RegisterUserError> {
        if auth_result? {
            self.state = RegisterUserState::StartTxn;
            Ok(smallvec![Effect::Storage(
                StorageEffect::StartTransaction { read: false }
            )])
        } else {
            Err(RegisterUserError::Unauthorized)
        }
    }

    fn handle_start_txn(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::TransactionStarted)",
                got,
            );
        };

        match self.emit_create_user(txn_id) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_create_user(&mut self, txn_id: TxnId) -> Result<Effects, RegisterUserError> {
        let user = User {
            user_id: self.input.user_id.clone(),
            name: self.input.name.clone(),
            subject_ids: self.input.subject_ids.clone(),
        };

        let key = self.input.user_id.to_bytes().into();
        let value = user.to_bytes(&self.input.actor)?.into();

        self.state = RegisterUserState::CreateUser { txn_id, user };

        Ok(smallvec![Effect::Storage(StorageEffect::Write {
            key_space: USER_KEYSPACE.to_string(),
            key,
            value,
            txn_id: Some(txn_id),
        })])
    }

    fn handle_create_user(&mut self, event: Event, txn_id: TxnId, user: User) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::WriteResult { .. })",
                got,
            );
        };

        match self.emit_commit_txn(txn_id, user) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_commit_txn(&mut self, txn_id: TxnId, user: User) -> Result<Effects, RegisterUserError> {
        self.state = RegisterUserState::CommitTxn { user };

        Ok(smallvec![Effect::Storage(
            StorageEffect::CommitTransaction { txn_id }
        )])
    }

    fn handle_commit_txn(&mut self, event: Event, user: User) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::TransactionCommitted { .. })",
                got,
            );
        };

        match self.emit_announce_user(user) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_announce_user(&mut self, user: User) -> Result<Effects, RegisterUserError> {
        self.state = RegisterUserState::AnnounceUser { user };

        let suboperation = boxed_suboperation(
            AnnounceTopicOperation::new(
                AutomergeDocumentVariant::RealmConfig {
                    realm_id: self.input.actor.realm_id.clone(),
                }
                .topic_id(),
                self.input.actor.node_id,
            ),
            |result| {
                Event::SubOperation(SubOperationEvent::TopicAnnouncementResult {
                    result: result.map_err(|error| error.to_string()),
                })
            },
        );
        Ok(smallvec![Effect::SubOperation(suboperation)])
    }

    fn handle_announce_user(&mut self, event: Event, user: User) -> Effects {
        let got = format!("{event:?}");
        let Event::SubOperation(SubOperationEvent::TopicAnnouncementResult { result }) = event
        else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::SubOperation(SubOperationEvent::TopicAnnouncementResult { result })",
                got,
            );
        };

        self.state = RegisterUserState::Finish;
        self.output = Some(Ok(user));
        match result {
            Ok(_) => smallvec![],
            Err(err) => self.fail(RegisterUserError::AnnouncementError(err)),
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
            RegisterUserState::StartTxn => self.handle_start_txn(event),
            RegisterUserState::CreateUser { txn_id, user } => {
                self.handle_create_user(event, txn_id, user)
            }
            RegisterUserState::CommitTxn { user } => self.handle_commit_txn(event, user),
            RegisterUserState::AnnounceUser { user } => self.handle_announce_user(event, user),
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
            RegisterUserState::CreateUser { txn_id, .. } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => smallvec![],
        }
    }
}

#[cfg(test)]
mod test {
    use crate::register_user::{RegisterUserInput, RegisterUserOperation, RegisterUserState};
    use aruna_core::effects::Effect;
    use aruna_core::events::{Event, SubOperationEvent};
    use aruna_core::keyspaces::USER_KEYSPACE;
    use aruna_core::operation::Operation;
    use aruna_core::structs::{Actor, User};
    use aruna_core::types::TxnId;
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
        assert_eq!(register_user_operation.state, RegisterUserState::StartTxn);

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
            RegisterUserState::CreateUser {
                txn_id,
                user: expected_user.clone(),
            }
        );

        let effects = register_user_operation.step(Event::Storage(
            aruna_core::events::StorageEvent::WriteResult {
                key: registered_user_id.to_bytes().into(),
            },
        ));
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
            RegisterUserState::CommitTxn {
                user: expected_user.clone(),
            }
        );

        let effects = register_user_operation.step(Event::Storage(
            aruna_core::events::StorageEvent::TransactionCommitted { txn_id },
        ));
        let announce_user_effect = effects.first().unwrap();
        assert!(matches!(announce_user_effect, Effect::SubOperation(_)));
        assert_eq!(
            register_user_operation.state,
            RegisterUserState::AnnounceUser {
                user: expected_user.clone(),
            }
        );

        let effects = register_user_operation.step(Event::SubOperation(
            SubOperationEvent::TopicAnnouncementResult { result: Ok(()) },
        ));
        assert!(effects.is_empty());
        assert_eq!(register_user_operation.state, RegisterUserState::Finish);
        assert_eq!(register_user_operation.finalize().unwrap(), expected_user);
    }
}
