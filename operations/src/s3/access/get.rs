use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::USER_ACCESS_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::UserAccess;
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GetAccessState {
    Init,
    GetUserAccess,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum GetAccessError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Invalid state [{current:?}] - expected [{expected:?}]")]
    InvalidState {
        current: GetAccessState,
        expected: GetAccessState,
    },
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received:?}]")]
    InvalidStateEvent {
        state: GetAccessState,
        expected: &'static str,
        received: Event,
    },
    #[error("No user access found")]
    NotFound,
    #[error("GetUserAccess failed")]
    GetUserAccessFailed,
    #[error("operation did not finish")]
    NotFinished,
}

#[derive(Debug, PartialEq)]
pub struct GetAccessOperation {
    access_key_id: String,
    state: GetAccessState,
    output: Option<Result<UserAccess, GetAccessError>>,
}

impl GetAccessOperation {
    pub fn new(access_key_id: String) -> Self {
        Self {
            access_key_id,
            state: GetAccessState::Init,
            output: None,
        }
    }

    fn handle_init(&mut self) -> Effects {
        if let GetAccessState::Init = self.state {
            self.state = GetAccessState::GetUserAccess;
            smallvec![Effect::Storage(StorageEffect::Read {
                key_space: USER_ACCESS_KEYSPACE.to_string(),
                key: self.access_key_id.as_bytes().into(),
                txn_id: None,
            })]
        } else {
            self.abort()
        }
    }

    fn access_received(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::ReadResult { value, .. }) = event {
            self.state = GetAccessState::Finish;
            self.output = Some(match value {
                Some(value) => {
                    UserAccess::from_bytes(&value).map_err(GetAccessError::ConversionError)
                }
                None => Err(GetAccessError::NotFound),
            });
            smallvec![]
        } else {
            self.emit_error(GetAccessError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            })
        }
    }

    pub fn emit_error(&mut self, error: GetAccessError) -> Effects {
        self.state = GetAccessState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }
}

impl Operation for GetAccessOperation {
    type Output = UserAccess;
    type Error = GetAccessError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = &event {
            return self.emit_error(error.clone().into());
        }
        match self.state {
            GetAccessState::Init => self.handle_init(),
            GetAccessState::GetUserAccess => self.access_received(event),
            GetAccessState::Finish => smallvec![],
            GetAccessState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, GetAccessState::Finish | GetAccessState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.output {
            Some(Ok(value)) => Ok(value),
            Some(Err(error)) => Err(error),
            None => Err(GetAccessError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod test {
    use crate::driver::DriverContext;
    use crate::driver::drive;
    use crate::s3::access::get::GetAccessOperation;
    use crate::s3::access::get::UserAccess;
    use aruna_core::effects::StorageEffect;
    use aruna_core::keyspaces::USER_ACCESS_KEYSPACE;
    use aruna_storage::storage;
    use tempfile::tempdir;
    use ulid::Ulid;

    #[tokio::test]
    pub async fn gets_user_access() {
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let storage_handle = storage::FjallStorage::open(temp_root).unwrap();

        let user_identity = Default::default();
        let access_key_id = UserAccess::build_access_key(&Ulid::generate().to_string()).unwrap();
        let user_access = UserAccess {
            access_key: access_key_id.clone(),
            user_identity,
            group_id: Ulid::generate(),
            secret: aruna_core::credential_encryption::EncryptedS3Secret::empty(),
            expiry: std::time::SystemTime::now() + std::time::Duration::from_secs(3600),
            path_restrictions: None,
            issued_by: [0u8; 32],
            revoked_at: None,
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
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let operation = GetAccessOperation::new(access_key_id.to_string());
        let result = drive(operation, &driver_ctx).await.unwrap();

        assert_eq!(result, user_access);
    }
}
