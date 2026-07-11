use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::USER_ACCESS_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::UserAccess;
use aruna_core::types::Effects;
use smallvec::smallvec;
use std::time::SystemTime;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RevokeUserAccessState {
    Init,
    StartTransaction,
    ReadUserAccess,
    WriteUserAccess,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum RevokeUserAccessError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Credential not found")]
    NotFound,
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("Invalid operation state")]
    InvalidOperationState,
    #[error("RevokeUserAccess failed")]
    RevokeUserAccessFailed,
}

#[derive(Debug, PartialEq)]
pub struct RevokeUserAccessOperation {
    access_key: String,
    state: RevokeUserAccessState,
    txn_id: Option<ulid::Ulid>,
    output: Option<Result<UserAccess, RevokeUserAccessError>>,
}

impl RevokeUserAccessOperation {
    pub fn new(access_key: String) -> Self {
        Self {
            access_key,
            state: RevokeUserAccessState::Init,
            txn_id: None,
            output: None,
        }
    }

    fn emit_error(&mut self, error: RevokeUserAccessError) -> Effects {
        self.state = RevokeUserAccessState::Error;
        self.output = Some(Err(error));
        self.abort()
    }

    fn handle_init(&mut self) -> Effects {
        self.state = RevokeUserAccessState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn handle_transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.emit_error(RevokeUserAccessError::InvalidOperationState);
        };

        self.txn_id = Some(txn_id);
        self.state = RevokeUserAccessState::ReadUserAccess;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: USER_ACCESS_KEYSPACE.to_string(),
            key: self.access_key.as_bytes().into(),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_user_access_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(RevokeUserAccessError::InvalidOperationState);
        };
        let Some(value) = value else {
            return self.emit_error(RevokeUserAccessError::NotFound);
        };

        let mut access = match UserAccess::from_bytes(value.as_ref()) {
            Ok(access) => access,
            Err(err) => return self.emit_error(err.into()),
        };
        access.revoked_at = Some(SystemTime::now());
        let bytes = match access.to_bytes() {
            Ok(bytes) => bytes,
            Err(err) => return self.emit_error(err.into()),
        };

        self.output = Some(Ok(access));
        self.state = RevokeUserAccessState::WriteUserAccess;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: USER_ACCESS_KEYSPACE.to_string(),
            key: self.access_key.as_bytes().into(),
            value: bytes.into(),
            txn_id: self.txn_id,
        })]
    }

    fn handle_user_access_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.emit_error(RevokeUserAccessError::InvalidOperationState);
        };
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(RevokeUserAccessError::NoTransactionFound);
        };

        self.state = RevokeUserAccessState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_transaction_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.emit_error(RevokeUserAccessError::InvalidOperationState);
        };

        self.txn_id = None;
        self.state = RevokeUserAccessState::Finish;
        smallvec![]
    }
}

impl Operation for RevokeUserAccessOperation {
    type Output = Option<Result<UserAccess, RevokeUserAccessError>>;
    type Error = RevokeUserAccessError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            RevokeUserAccessState::Init => self.handle_init(),
            RevokeUserAccessState::StartTransaction => self.handle_transaction_started(event),
            RevokeUserAccessState::ReadUserAccess => self.handle_user_access_read(event),
            RevokeUserAccessState::WriteUserAccess => self.handle_user_access_written(event),
            RevokeUserAccessState::CommitTransaction => self.handle_transaction_committed(event),
            RevokeUserAccessState::Finish => smallvec![],
            RevokeUserAccessState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            RevokeUserAccessState::Finish | RevokeUserAccessState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == RevokeUserAccessState::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(RevokeUserAccessError::RevokeUserAccessFailed);
        }
        Ok(self.output)
    }

    fn abort(&mut self) -> Effects {
        self.txn_id.map_or_else(smallvec::SmallVec::new, |txn_id| {
            smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_core::structs::UserAccess;
    use aruna_storage::storage;
    use std::time::Duration;
    use tempfile::tempdir;
    use ulid::Ulid;

    #[tokio::test]
    async fn test_revoke_user_access() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        let access_key = "test-access-key".to_string();
        let user_access = UserAccess {
            access_key: access_key.clone(),
            user_identity: Default::default(),
            group_id: Ulid::r#gen(),
            secret: "secret".to_string(),
            expiry: SystemTime::now() + Duration::from_secs(3600),
            path_restrictions: None,
            issued_by: [0u8; 32],
            revoked_at: None,
        };

        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: USER_ACCESS_KEYSPACE.to_string(),
                key: access_key.clone().into(),
                value: user_access.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;

        let result = drive(RevokeUserAccessOperation::new(access_key), &driver_ctx)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(result.revoked_at.is_some());
    }
}
