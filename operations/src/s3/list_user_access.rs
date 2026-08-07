use super::access_index::{decode_index, owner_key};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{USER_ACCESS_KEYSPACE, USER_ACCESS_OWNER_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::UserAccess;
use aruna_core::types::{Effects, UserId};
use smallvec::smallvec;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq)]
pub struct ListUserAccessInput {
    pub user_identity: UserId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListUserAccessState {
    Init,
    ReadOwnerIndex,
    ReadCredentials,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ListUserAccessError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received:?}]")]
    InvalidStateEvent {
        state: ListUserAccessState,
        expected: &'static str,
        received: Event,
    },
    #[error("credential owner index is inconsistent")]
    IndexInconsistent,
    #[error("ListUserAccess failed")]
    ListUserAccessFailed,
}

#[derive(Debug, PartialEq)]
pub struct ListUserAccessOperation {
    input: ListUserAccessInput,
    access_keys: Vec<String>,
    credentials: Vec<UserAccess>,
    state: ListUserAccessState,
    output: Option<Result<Vec<UserAccess>, ListUserAccessError>>,
}

impl ListUserAccessOperation {
    pub fn new(input: ListUserAccessInput) -> Self {
        Self {
            input,
            access_keys: Vec::new(),
            credentials: Vec::new(),
            state: ListUserAccessState::Init,
            output: None,
        }
    }

    fn emit_error(&mut self, error: ListUserAccessError) -> Effects {
        self.state = ListUserAccessState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn handle_init(&mut self) -> Effects {
        self.state = ListUserAccessState::ReadOwnerIndex;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: USER_ACCESS_OWNER_KEYSPACE.to_string(),
            key: owner_key(self.input.user_identity),
            txn_id: None,
        })]
    }

    fn handle_index(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(ListUserAccessError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };
        let index = match decode_index(value.as_ref()) {
            Ok(index) => index,
            Err(error) => return self.emit_error(error.into()),
        };
        if index.is_empty() {
            self.state = ListUserAccessState::Finish;
            self.output = Some(Ok(Vec::new()));
            return smallvec![];
        }
        self.access_keys = index.into_iter().collect();
        self.state = ListUserAccessState::ReadCredentials;
        let reads = self
            .access_keys
            .iter()
            .map(|access_key| {
                (
                    USER_ACCESS_KEYSPACE.to_string(),
                    access_key.as_bytes().into(),
                )
            })
            .collect();
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })]
    }

    fn handle_credentials(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.emit_error(ListUserAccessError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::BatchReadResult)",
                received: event,
            });
        };
        if values.len() != self.access_keys.len() {
            return self.emit_error(ListUserAccessError::IndexInconsistent);
        }
        for (key, value) in values {
            let Some(value) = value else {
                return self.emit_error(ListUserAccessError::IndexInconsistent);
            };
            let access = match UserAccess::from_bytes(value.as_ref()) {
                Ok(access) => access,
                Err(error) => return self.emit_error(error.into()),
            };
            if access.access_key.as_bytes() != key.as_ref()
                || !self.access_keys.contains(&access.access_key)
                || access.user_identity != self.input.user_identity
            {
                return self.emit_error(ListUserAccessError::IndexInconsistent);
            }
            self.credentials.push(access);
        }
        self.state = ListUserAccessState::Finish;
        self.output = Some(Ok(std::mem::take(&mut self.credentials)));
        smallvec![]
    }
}

impl Operation for ListUserAccessOperation {
    type Output = Vec<UserAccess>;
    type Error = ListUserAccessError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ListUserAccessState::Init => self.handle_init(),
            ListUserAccessState::ReadOwnerIndex => self.handle_index(event),
            ListUserAccessState::ReadCredentials => self.handle_credentials(event),
            ListUserAccessState::Finish | ListUserAccessState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ListUserAccessState::Finish | ListUserAccessState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == ListUserAccessState::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(ListUserAccessError::ListUserAccessFailed);
        }
        self.output.unwrap_or_else(|| Ok(Vec::new()))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::access_index::{MAX_ACTIVE_CREDENTIALS, encode_index, owner_key};
    use aruna_core::credential_seal::SealedS3Secret;
    use aruna_core::structs::RealmId;
    use std::time::{Duration, SystemTime};
    use ulid::Ulid;

    #[test]
    fn lists_indexed() {
        let user_identity = UserId::local(Ulid::generate(), RealmId([1; 32]));
        let keys = (0..MAX_ACTIVE_CREDENTIALS)
            .map(|index| format!("key{index}"))
            .collect::<std::collections::BTreeSet<_>>();
        let mut operation = ListUserAccessOperation::new(ListUserAccessInput { user_identity });
        operation.start();
        let index = encode_index(&keys).unwrap();
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: owner_key(user_identity),
            value: Some(index),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::BatchRead { reads, .. })]
                if reads.len() == MAX_ACTIVE_CREDENTIALS
        ));
        let values = keys
            .iter()
            .map(|key| {
                let access = UserAccess {
                    access_key: key.clone(),
                    user_identity,
                    group_id: Ulid::generate(),
                    secret: SealedS3Secret::empty(),
                    expiry: SystemTime::now() + Duration::from_secs(60),
                    path_restrictions: None,
                    issued_by: [0; 32],
                    revoked_at: None,
                };
                (key.clone().into(), Some(access.to_bytes().unwrap().into()))
            })
            .collect();
        operation.step(Event::Storage(StorageEvent::BatchReadResult { values }));
        let credentials = operation.finalize().unwrap();
        assert_eq!(credentials.len(), MAX_ACTIVE_CREDENTIALS);
    }
}
