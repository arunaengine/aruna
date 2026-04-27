use aruna_core::USER_KEYSPACE;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{AuthContext, Permission, RealmId, User};
use aruna_core::types::{Effects, UserId};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};

#[derive(Clone, Debug, PartialEq)]
pub struct GetUserInput {
    pub auth_context: AuthContext,
    pub self_realm_id: RealmId,
    pub user_id: String,
}

#[derive(Debug, PartialEq)]
pub struct GetUserOperation {
    input: GetUserInput,
    state: GetUserState,
    output: Option<Result<User, GetUserError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum GetUserState {
    Init,
    Auth,
    ReadExistingUser,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum GetUserError {
    #[error("Unauthorized")]
    Unauthorized,
    #[error(transparent)]
    AuthorizationError(#[from] AuthorizationError),
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
    #[error("registration did not finish")]
    NotFinished,
    #[error("User not found")]
    UserNotFound,
}

impl GetUserOperation {
    pub fn new(input: GetUserInput) -> Self {
        Self {
            input,
            state: GetUserState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: GetUserError) -> Effects {
        let cleanup = self.abort();
        self.state = GetUserState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn fail_on_storage_error(&mut self, event: Event) -> Result<Event, Effects> {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return Err(self.fail(error.into()));
        }
        Ok(event)
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        self.fail(GetUserError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            got,
        })
    }

    fn handle_auth_result(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::SubOperation(aruna_core::events::SubOperationEvent::AuthorizationResult {
            allowed,
        }) = event
        else {
            return self.unexpected_event(
                "Event::SubOperation(aruna_core::events::SubOperationEvent::AuthorizationResult { allowed })",
                got,
            );
        };

        match self.emit_read_existing_user(allowed) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_read_existing_user(
        &mut self,
        allowed: Result<bool, AuthorizationError>,
    ) -> Result<Effects, GetUserError> {
        if allowed? {
            self.state = GetUserState::ReadExistingUser;
            let user_id = UserId::from_string(&self.input.user_id)?;
            let key = user_id.to_bytes().into();
            Ok(smallvec![Effect::Storage(StorageEffect::Read {
                key_space: USER_KEYSPACE.to_string(),
                key,
                txn_id: None
            })])
        } else {
            Err(GetUserError::Unauthorized)
        }
    }

    fn handle_read_existing_user(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected_event(
                "Event::Storage(StorageEvent::ReadResult { key, value })",
                got,
            );
        };

        match self.emit_finish(value) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_finish(&mut self, value: Option<ByteView>) -> Result<Effects, GetUserError> {
        let user = User::from_bytes(&value.ok_or_else(|| GetUserError::UserNotFound)?)?;
        self.state = GetUserState::Finish;
        self.output = Some(Ok(user));
        Ok(smallvec![])
    }
}

impl Operation for GetUserOperation {
    type Output = User;
    type Error = GetUserError;

    fn start(&mut self) -> Effects {
        self.state = GetUserState::Auth;
        let auth_config = CheckPermissionsConfig {
            auth_context: self.input.auth_context.clone(),
            path: format!(
                "/{}/admin/u/{}",
                self.input.self_realm_id, self.input.user_id
            ),
            required_permission: Permission::READ,
        };
        let auth_operation =
            boxed_suboperation(CheckPermissionsOperation::new(auth_config), |result| {
                Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed: result })
            });

        smallvec![Effect::SubOperation(auth_operation)]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match self.fail_on_storage_error(event) {
            Ok(event) => event,
            Err(effects) => return effects,
        };
        match self.state.clone() {
            GetUserState::Auth => self.handle_auth_result(event),
            GetUserState::ReadExistingUser => self.handle_read_existing_user(event),
            GetUserState::Init | GetUserState::Finish | GetUserState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, GetUserState::Finish | GetUserState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(GetUserError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
