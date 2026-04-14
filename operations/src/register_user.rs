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
    AnnounceUser,
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
    #[error("Adding user to group did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: RegisterUserState,
        expected: &'static str,
        got: String,
    },
}

impl RegisterUserOperation {
    fn new(input: RegisterUserInput) -> Self {
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
                RegisterUserState::Auth,
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
                RegisterUserState::Auth,
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

    fn handle_create_user(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.unexpected_event(
                RegisterUserState::Auth,
                "Event::Storage(StorageEvent::WriteResult { .. })",
                got,
            );
        };

        match self.emit_commit_txn() {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_commit_txn(&mut self) -> Result<Effects, RegisterUserError> {
        let RegisterUserState::CreateUser { txn_id, ref user } = self.state else {
            return Err(RegisterUserError::NoTransactionFound);
        };

        self.state = RegisterUserState::CommitTxn { user: user.clone() };

        Ok(smallvec![Effect::Storage(
            StorageEffect::CommitTransaction { txn_id }
        )])
    }

    fn handle_commit_txn(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.unexpected_event(
                RegisterUserState::Auth,
                "Event::Storage(StorageEvent::TransactionCommitted { .. })",
                got,
            );
        };

        match self.emit_announce_user() {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_announce_user(&mut self) -> Result<Effects, RegisterUserError> {
        self.state = RegisterUserState::AnnounceUser;

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

    fn handle_announce_user(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::SubOperation(SubOperationEvent::TopicAnnouncementResult { result }) = event
        else {
            return self.unexpected_event(
                RegisterUserState::Auth,
                "Event::SubOperation(SubOperationEvent::TopicAnnouncementResult { result })",
                got,
            );
        };

        self.state = RegisterUserState::Finish;

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

        match self.state {
            RegisterUserState::Auth => self.handle_authorization(event),
            RegisterUserState::StartTxn => self.handle_start_txn(event),
            RegisterUserState::CreateUser { .. } => self.handle_create_user(event),
            RegisterUserState::CommitTxn { .. } => self.handle_commit_txn(event),
            RegisterUserState::AnnounceUser { .. } => self.handle_announce_user(event),
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
    #[tokio::test]
    pub async fn test_user_registration() {}
}
