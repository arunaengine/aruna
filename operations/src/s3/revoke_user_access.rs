use crate::replicate_documents::replicate_documents_effect;
use aruna_core::NodeId;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::USER_ACCESS_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::UserAccess;
use aruna_core::types::Effects;
use smallvec::smallvec;
use std::time::SystemTime;
use thiserror::Error;
use tracing::warn;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RevokeUserAccessState {
    Init,
    StartTransaction,
    ReadUserAccess,
    WriteUserAccess,
    CommitTransaction,
    ReplicateAccess,
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
    mutated: bool,
    output: Option<Result<UserAccess, RevokeUserAccessError>>,
}

impl RevokeUserAccessOperation {
    pub fn new(access_key: String) -> Self {
        Self {
            access_key,
            state: RevokeUserAccessState::Init,
            txn_id: None,
            mutated: false,
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
        if access.revoked_at.is_some() {
            let Some(txn_id) = self.txn_id else {
                return self.emit_error(RevokeUserAccessError::NoTransactionFound);
            };
            self.output = Some(Ok(access));
            self.state = RevokeUserAccessState::CommitTransaction;
            return smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })];
        }
        access.revoked_at = Some(SystemTime::now());
        let bytes = match access.to_bytes() {
            Ok(bytes) => bytes,
            Err(err) => return self.emit_error(err.into()),
        };

        self.output = Some(Ok(access));
        self.mutated = true;
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
        if self.mutated {
            return self.replicate_revocation();
        }
        self.state = RevokeUserAccessState::Finish;
        smallvec![]
    }

    /// Propagates the revoked credential realm-wide over the same shared topic
    /// its creation used, so the revocation takes effect on every realm node.
    fn replicate_revocation(&mut self) -> Effects {
        let Some(Ok(access)) = self.output.as_ref() else {
            self.state = RevokeUserAccessState::Finish;
            return smallvec![];
        };
        let realm_id = access.user_identity.realm_id;
        let Ok(local_node_id) = NodeId::from_bytes(&access.issued_by) else {
            self.state = RevokeUserAccessState::Finish;
            return smallvec![];
        };
        self.state = RevokeUserAccessState::ReplicateAccess;
        smallvec![replicate_documents_effect(
            realm_id,
            local_node_id,
            vec![DocumentSyncTarget::UserAccess {
                realm_id,
                access_key: self.access_key.clone(),
            }],
        )]
    }

    fn handle_access_replicated(&mut self, event: Event) -> Effects {
        if let Event::SubOperation(SubOperationEvent::DocumentSyncResult { result }) = event
            && let Err(error) = result
        {
            warn!(%error, "revocation replication failed; revoked locally");
        }
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
            RevokeUserAccessState::ReplicateAccess => self.handle_access_replicated(event),
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
        self.txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_core::structs::{RealmId, UserAccess};
    use aruna_core::types::UserId;
    use aruna_storage::storage;
    use std::time::Duration;
    use tempfile::tempdir;
    use ulid::Ulid;

    #[test]
    fn revoke_replicates_after_commit() {
        // A real revocation queues realm-wide propagation of the revoked record.
        let issued_by = *iroh::SecretKey::from_bytes(&[9u8; 32]).public().as_bytes();
        let access = UserAccess {
            access_key: "user:key".to_string(),
            user_identity: UserId::new(Ulid::from_bytes([2; 16]), RealmId([1; 32])),
            group_id: Ulid::r#gen(),
            secret: "secret".to_string(),
            expiry: SystemTime::now() + Duration::from_secs(60),
            path_restrictions: None,
            issued_by,
            revoked_at: None,
        };
        let mut op = RevokeUserAccessOperation::new(access.access_key.clone());
        op.start();
        op.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::r#gen(),
        }));
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: access.access_key.as_bytes().to_vec().into(),
            value: Some(access.to_bytes().unwrap().into()),
        }));
        op.step(Event::Storage(StorageEvent::WriteResult {
            key: access.access_key.as_bytes().to_vec().into(),
        }));

        let effects = op.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: Ulid::r#gen(),
        }));
        assert_eq!(op.state, RevokeUserAccessState::ReplicateAccess);
        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));

        let effects = op.step(Event::SubOperation(SubOperationEvent::DocumentSyncResult {
            result: Ok(()),
        }));
        assert!(effects.is_empty());
        assert_eq!(op.state, RevokeUserAccessState::Finish);
    }

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
            compute_handle: None,
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
