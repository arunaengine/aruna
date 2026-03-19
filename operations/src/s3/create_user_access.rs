use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::USER_ACCESS_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::{UserAccess, UserIdentity};
use aruna_core::types::{Effects, GroupId};
use rand::distr::Alphanumeric;
use rand::{Rng, rng};
use smallvec::smallvec;
use std::time::{Duration, SystemTime};
use thiserror::Error;
use ulid::Ulid;

const DEFAULT_CREDENTIAL_TTL: Duration = Duration::from_secs(24 * 60 * 60 * 365);

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CreateUserAccessState {
    Init,
    CreateUserAccess(UserAccess),
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateUserAccessError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Invalid state [{current:?}] - expected [{expected:?}]")]
    InvalidState {
        current: CreateUserAccessState,
        expected: String,
    },
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received:?}]")]
    InvalidStateEvent {
        state: CreateUserAccessState,
        expected: &'static str,
        received: Event,
    },
    #[error("No user access found")]
    NotFound,
    #[error("User access creation not finished")]
    NotFinished,
    #[error("User access creation failed")]
    CreateUserAccessFailed,
    #[error("User access creation aborted")]
    CreateUserAccessAborted,
}

#[derive(Debug, PartialEq)]
pub struct CreateUserAccessOperation {
    user_identity: UserIdentity,
    group_id: GroupId,
    state: CreateUserAccessState,
    output: Result<(String, UserAccess), CreateUserAccessError>,
}

impl CreateUserAccessOperation {
    pub fn new(user_identity: UserIdentity, group_id: GroupId) -> Self {
        Self {
            user_identity,
            group_id,
            state: CreateUserAccessState::Init,
            output: Err(CreateUserAccessError::NotFinished),
        }
    }

    fn handle_init(&mut self) -> Effects {
        if let CreateUserAccessState::Init = self.state {
            let key_id = Ulid::new().to_string();
            let access_key = match UserAccess::build_access_key(&self.user_identity, &key_id) {
                Ok(access_key) => access_key,
                Err(err) => return self.handle_error(err.into()),
            };
            let access = UserAccess {
                access_key: access_key.clone(),
                user_identity: self.user_identity.clone(),
                group_id: self.group_id,
                secret: rng()
                    .sample_iter(&Alphanumeric)
                    .take(30)
                    .map(char::from)
                    .collect::<String>(),
                expiry: SystemTime::now() + DEFAULT_CREDENTIAL_TTL,
            };
            let bytes = match access.to_bytes() {
                Ok(bytes) => bytes,
                Err(err) => return self.handle_error(err.into()),
            };

            self.state = CreateUserAccessState::CreateUserAccess(access);
            smallvec![Effect::Storage(StorageEffect::Write {
                key_space: USER_ACCESS_KEYSPACE.to_string(),
                key: access_key.as_bytes().into(),
                value: bytes.into(),
                txn_id: None,
            })]
        } else {
            self.abort()
        }
    }

    fn handle_user_access_created(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { key }) = event else {
            return self.handle_error(CreateUserAccessError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::WriteResult)",
                received: event,
            });
        };

        let CreateUserAccessState::CreateUserAccess(access) = &self.state else {
            return self.handle_error(CreateUserAccessError::InvalidState {
                current: self.state.clone(),
                expected: "CreateUserAccessState::CreateUserAccess(..)".to_string(),
            });
        };

        let access_key = match String::from_utf8(key.to_vec()) {
            Ok(access_key) => access_key,
            Err(err) => {
                return self.handle_error(CreateUserAccessError::ConversionError(err.into()));
            }
        };
        self.output = Ok((access_key, access.clone()));
        self.state = CreateUserAccessState::Finish;
        smallvec![]
    }

    pub fn handle_error(&mut self, error: CreateUserAccessError) -> Effects {
        self.state = CreateUserAccessState::Error;
        self.output = Err(error);
        self.abort()
    }
}

impl Operation for CreateUserAccessOperation {
    type Output = Result<(String, UserAccess), CreateUserAccessError>;
    type Error = CreateUserAccessError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            CreateUserAccessState::Init => self.handle_init(),
            CreateUserAccessState::CreateUserAccess(_) => self.handle_user_access_created(event),
            CreateUserAccessState::Finish => smallvec![],
            CreateUserAccessState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            CreateUserAccessState::Finish | CreateUserAccessState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if CreateUserAccessState::Error == self.state {
            self.output?;
            return Err(CreateUserAccessError::CreateUserAccessFailed);
        }
        Ok(self.output)
    }

    fn abort(&mut self) -> Effects {
        let Ok((access_key, _)) = self.output.as_ref() else {
            return smallvec![];
        };
        let access_key = access_key.clone();

        self.output = Err(CreateUserAccessError::CreateUserAccessAborted);
        smallvec![Effect::Storage(StorageEffect::Delete {
            key_space: USER_ACCESS_KEYSPACE.to_string(),
            key: access_key.as_bytes().into(),
            txn_id: None,
        })]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::{RealmId, UserIdentity};

    fn make_user_identity() -> UserIdentity {
        UserIdentity {
            user_id: Ulid::new(),
            realm_key: RealmId([0u8; 32]),
        }
    }

    #[test]
    fn test_create_user_access_happy_path() {
        let user_identity = make_user_identity();
        let group_id = Ulid::new();
        let mut op = CreateUserAccessOperation::new(user_identity.clone(), group_id);

        // 1. Start -> Should transition to CreateUserAccess and emit Storage::Write
        let effects = op.start();
        assert_eq!(effects.len(), 1);
        assert!(matches!(
            op.state,
            CreateUserAccessState::CreateUserAccess(_)
        ));

        let Effect::Storage(StorageEffect::Write {
            key_space,
            key,
            value: _,
            txn_id: None,
        }) = &effects[0]
        else {
            panic!("Expected Storage::Write effect");
        };
        assert_eq!(key_space, USER_ACCESS_KEYSPACE);

        // Verify the stored access matches the user identity and group
        let CreateUserAccessState::CreateUserAccess(ref access) = op.state else {
            panic!("Expected CreateUserAccess state");
        };
        assert_eq!(access.user_identity, user_identity);
        assert_eq!(access.group_id, group_id);
        assert_eq!(access.secret.len(), 30);

        // 2. Feed WriteResult -> Should transition to Finish
        let write_key = key.clone();
        let event = Event::Storage(StorageEvent::WriteResult { key: write_key });
        let effects = op.step(event);
        assert_eq!(effects.len(), 0);
        assert_eq!(op.state, CreateUserAccessState::Finish);
        assert!(op.is_complete());

        // 3. Finalize -> Should return Ok with (String, UserAccess)
        let result = op.finalize();
        assert!(result.is_ok());
        let inner = result.unwrap();
        assert!(inner.is_ok());
        let (access_key, returned_access) = inner.unwrap();
        assert_eq!(returned_access.user_identity, user_identity);
        assert_eq!(returned_access.group_id, group_id);
        assert_eq!(returned_access.access_key, access_key);
    }

    #[test]
    fn test_create_user_access_invalid_steps() {
        let user_identity = make_user_identity();
        let group_id = Ulid::new();

        // 1. Invalid state: start twice -> second start calls abort since state is not Init
        let mut op = CreateUserAccessOperation::new(user_identity.clone(), group_id);
        op.start();
        // State is now CreateUserAccess; calling start again calls handle_init which calls abort.
        // abort returns empty effects (output is still Err since WriteResult not yet received).
        let effects = op.start();
        assert!(effects.is_empty());
        // State remains CreateUserAccess (abort only transitions to Error when output is Ok)
        assert!(matches!(
            op.state,
            CreateUserAccessState::CreateUserAccess(_)
        ));

        // 2. Invalid event at CreateUserAccess state (wrong event type)
        let mut op = CreateUserAccessOperation::new(user_identity.clone(), group_id);
        op.start();
        // Feed a ReadResult instead of WriteResult
        let key = Ulid::new().to_bytes().into();
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key,
            value: None,
        }));
        assert!(effects.is_empty());
        assert_eq!(op.state, CreateUserAccessState::Error);
        assert!(matches!(
            op.finalize().unwrap_err(),
            CreateUserAccessError::InvalidStateEvent { .. }
        ));
    }
}
