use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::UserAccess;
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GetUserAccessState {
    Init,
    GetUserAccess,
    Finish,
    Error,
}

#[derive(Debug, Error)]
pub enum GetUserAccessError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Invalid state [{current:?}] - expected [{expected:?}]")]
    InvalidState {
        current: GetUserAccessState,
        expected: GetUserAccessState,
    },
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received:?}]")]
    InvalidStateEvent {
        state: GetUserAccessState,
        expected: &'static str,
        received: Event,
    },
    #[error("No user access found")]
    NotFound,
    #[error("GetUserAccess failed")]
    GetUserAccessFailed,
}

pub struct GetUserAccessOperation {
    access_key_id: String,
    state: GetUserAccessState,
    output: Option<Result<UserAccess, GetUserAccessError>>,
}

impl GetUserAccessOperation {
    pub fn new(access_key_id: String) -> Self {
        Self {
            access_key_id,
            state: GetUserAccessState::Init,
            output: None,
        }
    }

    fn handle_init(&mut self) -> Effects {
        if let GetUserAccessState::Init = self.state {
            self.state = GetUserAccessState::GetUserAccess;
            smallvec![Effect::Storage(StorageEffect::Read {
                key_space: "user_access".to_string(),
                key: self.access_key_id.as_bytes().into(),
                txn_id: None,
            })]
        } else {
            self.abort()
        }
    }

    fn handle_user_access_received(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::ReadResult { value, .. }) = event {
            let output = value.map(|value| {
                UserAccess::from_bytes(&value).map_err(GetUserAccessError::ConversionError)
            });
            self.state = GetUserAccessState::Finish;
            self.output = output;
            smallvec![]
        } else {
            self.emit_error(GetUserAccessError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            })
        }
    }

    pub fn emit_error(&mut self, error: GetUserAccessError) -> Effects {
        self.state = GetUserAccessState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }
}

impl Operation for GetUserAccessOperation {
    type Output = Option<Result<UserAccess, GetUserAccessError>>;
    type Error = GetUserAccessError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            GetUserAccessState::Init => self.handle_init(),
            GetUserAccessState::GetUserAccess => self.handle_user_access_received(event),
            GetUserAccessState::Finish => smallvec![],
            GetUserAccessState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            GetUserAccessState::Finish | GetUserAccessState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if GetUserAccessState::Error == self.state {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(GetUserAccessError::GetUserAccessFailed);
        }
        Ok(self.output)
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod test {
    use crate::driver::DriverContext;
    use crate::driver::drive;
    use crate::s3::get_user_access::GetUserAccessOperation;
    use crate::s3::get_user_access::UserAccess;
    use aruna_core::USER_ACCESS_KEYSPACE;
    use aruna_core::effects::StorageEffect;
    use aruna_core::structs::RealmId;
    use aruna_core::structs::UserIdentity;
    use aruna_storage::storage;
    use tempfile::tempdir;
    use ulid::Ulid;

    #[tokio::test]
    pub async fn test_get_user_access() {
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let storage_handle = storage::FjallStorage::open(temp_root).unwrap();

        let access_key_id = Ulid::new().to_string();
        let user_access = UserAccess {
            user_id: UserIdentity {
                user_id: Ulid::new(),
                realm_key: RealmId([0u8; 32]),
            },
            group_id: Ulid::new(),
            secret: "SECRET_KEY".to_string(),
        };

        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: USER_ACCESS_KEYSPACE.to_string(),
                key: access_key_id.as_bytes().into(),
                value: user_access.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;

        let driver_ctx = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
        };

        let operation = GetUserAccessOperation::new(access_key_id.to_string());
        let result = drive(operation, &driver_ctx)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert_eq!(result, user_access);
    }
}
