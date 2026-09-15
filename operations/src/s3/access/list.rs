use super::index::{decode_index, owner_key};
use aruna_core::UserId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{ACCESS_OWNER_KEYSPACE, USER_ACCESS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::blob::UserAccess;
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq)]
pub struct ListUserInput {
    pub user_identity: UserId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListUserState {
    Init,
    StartTransaction,
    ReadOwnerIndex,
    ReadCredentials,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ListUserError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received:?}]")]
    InvalidStateEvent {
        state: ListUserState,
        expected: &'static str,
        received: Event,
    },
    #[error("credential owner index is inconsistent")]
    IndexInconsistent,
    #[error("ListUserAccess failed")]
    ListAccessFailed,
}

#[derive(Debug, PartialEq)]
pub struct ListUserOperation {
    input: ListUserInput,
    access_keys: Vec<String>,
    credentials: Vec<UserAccess>,
    txn_id: Option<ulid::Ulid>,
    state: ListUserState,
    output: Option<Result<Vec<UserAccess>, ListUserError>>,
}

impl ListUserOperation {
    pub fn new(input: ListUserInput) -> Self {
        Self {
            input,
            access_keys: Vec::new(),
            credentials: Vec::new(),
            txn_id: None,
            state: ListUserState::Init,
            output: None,
        }
    }

    fn emit_error(&mut self, error: ListUserError) -> Effects {
        let effects = self.abort();
        self.state = ListUserState::Error;
        self.output = Some(Err(error));
        effects
    }

    fn handle_init(&mut self) -> Effects {
        self.state = ListUserState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: true
        })]
    }

    fn handle_transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.emit_error(ListUserError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionStarted)",
                received: event,
            });
        };
        self.txn_id = Some(txn_id);
        self.state = ListUserState::ReadOwnerIndex;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: ACCESS_OWNER_KEYSPACE.to_string(),
            key: owner_key(self.input.user_identity),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_index(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(ListUserError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };
        let index = match decode_index(value.as_ref()) {
            Ok(index) => index,
            Err(error) => return self.emit_error(error.into()),
        };
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(ListUserError::StorageError(
                StorageError::TransactionNotFound,
            ));
        };
        if index.is_empty() {
            self.output = Some(Ok(Vec::new()));
            self.state = ListUserState::CommitTransaction;
            return smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })];
        }
        self.access_keys = index.into_iter().collect();
        self.state = ListUserState::ReadCredentials;
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
            txn_id: Some(txn_id),
        })]
    }

    fn handle_credentials(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.emit_error(ListUserError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::BatchReadResult)",
                received: event,
            });
        };
        if values.len() != self.access_keys.len() {
            return self.emit_error(ListUserError::IndexInconsistent);
        }
        for (key, value) in values {
            let Some(value) = value else {
                return self.emit_error(ListUserError::IndexInconsistent);
            };
            let access = match UserAccess::from_bytes(value.as_ref()) {
                Ok(access) => access,
                Err(error) => return self.emit_error(error.into()),
            };
            if access.access_key.as_bytes() != key.as_ref()
                || !self.access_keys.contains(&access.access_key)
                || access.user_identity != self.input.user_identity
            {
                return self.emit_error(ListUserError::IndexInconsistent);
            }
            self.credentials.push(access);
        }
        self.state = ListUserState::Finish;
        self.output = Some(Ok(std::mem::take(&mut self.credentials)));
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(ListUserError::StorageError(
                StorageError::TransactionNotFound,
            ));
        };
        self.state = ListUserState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_transaction_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.emit_error(ListUserError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                received: event,
            });
        };
        self.txn_id = None;
        self.state = ListUserState::Finish;
        smallvec![]
    }
}

impl Operation for ListUserOperation {
    type Output = Vec<UserAccess>;
    type Error = ListUserError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.emit_error(error.into());
        }
        match self.state {
            ListUserState::Init => self.handle_init(),
            ListUserState::StartTransaction => self.handle_transaction_started(event),
            ListUserState::ReadOwnerIndex => self.handle_index(event),
            ListUserState::ReadCredentials => self.handle_credentials(event),
            ListUserState::CommitTransaction => self.handle_transaction_committed(event),
            ListUserState::Finish | ListUserState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ListUserState::Finish | ListUserState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == ListUserState::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(ListUserError::ListAccessFailed);
        }
        self.output.unwrap_or_else(|| Ok(Vec::new()))
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            })
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use crate::s3::access::index::{MAX_ACTIVE_CREDENTIALS, encode_index, owner_key};
    use aruna_core::credential_encryption::EncryptedS3Secret;
    use aruna_core::structs::identity::realm::RealmId;
    use std::time::{Duration, SystemTime};
    use ulid::Ulid;

    #[test]
    fn lists_indexed() {
        let user_identity = UserId::local(Ulid::from_parts(1, 1), RealmId([1; 32]));
        let keys = (0..MAX_ACTIVE_CREDENTIALS)
            .map(|index| format!("key{index}"))
            .collect::<std::collections::BTreeSet<_>>();
        let mut operation = ListUserOperation::new(ListUserInput { user_identity });
        assert!(matches!(
            operation.start().as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction {
                read: true
            })]
        ));
        let txn_id = Ulid::from_parts(2, 2);
        assert!(matches!(
            operation
                .step(Event::Storage(StorageEvent::TransactionStarted { txn_id }))
                .as_slice(),
            [Effect::Storage(StorageEffect::Read {
                txn_id: Some(observed), ..
            })] if *observed == txn_id
        ));
        let index = encode_index(&keys).unwrap();
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: owner_key(user_identity),
            value: Some(index),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::BatchRead {
                reads,
                txn_id: Some(observed),
            })]
                if *observed == txn_id && reads.len() == MAX_ACTIVE_CREDENTIALS
        ));
        let values = keys
            .iter()
            .map(|key| {
                let access = UserAccess {
                    access_key: key.clone(),
                    user_identity,
                    group_id: Ulid::from_parts(3, 3),
                    secret: EncryptedS3Secret::empty(),
                    expiry: SystemTime::UNIX_EPOCH
                        + Duration::from_secs(1600000060)
                        + Duration::from_secs(60),
                    path_restrictions: None,
                    issued_by: [0; 32],
                    revoked_at: None,
                };
                (key.clone().into(), Some(access.to_bytes().unwrap().into()))
            })
            .collect();
        assert!(matches!(
            operation
                .step(Event::Storage(StorageEvent::BatchReadResult { values }))
                .as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { txn_id: observed })]
                if *observed == txn_id
        ));
        operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
        let credentials = operation.finalize().unwrap();
        assert_eq!(credentials.len(), MAX_ACTIVE_CREDENTIALS);
    }
}
