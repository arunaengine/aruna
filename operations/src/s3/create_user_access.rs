use aruna_core::UserId;
use aruna_core::compute::Secret;
use aruna_core::credential_encryption::{
    CredentialEncryptionKey, EncryptedS3Secret, EncryptionError,
};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{USER_ACCESS_KEYSPACE, USER_ACCESS_OWNER_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::permission_path::{RestrictionLimitError, validate_restriction_limits};
use aruna_core::structs::{PathRestriction, UserAccess};
use aruna_core::types::{Effects, GroupId};
use rand::distr::Alphanumeric;
use rand::{RngExt, rng};
use smallvec::smallvec;
use std::time::{Duration, SystemTime};
use thiserror::Error;
use ulid::Ulid;

use super::access_index::{MAX_ACTIVE_CREDENTIALS, decode_index, encode_index, owner_key};

pub const DEFAULT_CREDENTIAL_TTL: Duration = Duration::from_secs(24 * 60 * 60 * 365);

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CreateUserAccessState {
    Init,
    StartTransaction,
    ReadOwnerIndex,
    ReadCredentials {
        index: std::collections::BTreeSet<String>,
        replace: bool,
    },
    DeleteStale {
        index: std::collections::BTreeSet<String>,
    },
    WriteCredentials,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateUserAccessError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    RestrictionLimit(#[from] RestrictionLimitError),
    #[error(transparent)]
    Encryption(#[from] EncryptionError),
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
    #[error("active credential limit reached")]
    LimitReached,
    #[error("credential owner index is inconsistent")]
    IndexInconsistent,
    #[error("User access creation not finished")]
    NotFinished,
    #[error("User access creation failed")]
    CreateUserAccessFailed,
    #[error("User access creation aborted")]
    CreateUserAccessAborted,
}

#[derive(Debug, PartialEq)]
pub struct CreateUserAccessConfig {
    pub user_identity: UserId,
    pub group_id: GroupId,
    pub expiry: SystemTime,
    pub path_restrictions: Option<Vec<PathRestriction>>,
    pub issued_by: [u8; 32],
}

#[derive(Debug, PartialEq)]
pub struct CreateUserAccessOperation {
    config: CreateUserAccessConfig,
    key_id: String,
    encryption_key: CredentialEncryptionKey,
    pending_secret: Option<Secret>,
    access: Option<UserAccess>,
    txn_id: Option<ulid::Ulid>,
    state: CreateUserAccessState,
    output: Result<(String, Secret, UserAccess), CreateUserAccessError>,
}

impl CreateUserAccessOperation {
    pub fn new(config: CreateUserAccessConfig, encryption_key: CredentialEncryptionKey) -> Self {
        Self::new_with_key(config, Ulid::generate().to_string(), encryption_key)
    }

    pub fn new_with_key(
        config: CreateUserAccessConfig,
        key_id: String,
        encryption_key: CredentialEncryptionKey,
    ) -> Self {
        Self {
            config,
            key_id,
            encryption_key,
            pending_secret: None,
            access: None,
            txn_id: None,
            state: CreateUserAccessState::Init,
            output: Err(CreateUserAccessError::NotFinished),
        }
    }

    fn handle_init(&mut self) -> Effects {
        if !matches!(self.state, CreateUserAccessState::Init) {
            return self.abort();
        }
        if let Some(restrictions) = self.config.path_restrictions.as_deref()
            && let Err(err) = validate_restriction_limits(restrictions)
        {
            return self.handle_error(err.into());
        }
        let access_key = match UserAccess::build_access_key(&self.key_id) {
            Ok(access_key) => access_key,
            Err(err) => return self.handle_error(err.into()),
        };
        let plaintext = rng()
            .sample_iter(&Alphanumeric)
            .take(30)
            .map(char::from)
            .collect::<String>();
        let mut access = UserAccess {
            access_key,
            user_identity: self.config.user_identity,
            group_id: self.config.group_id,
            secret: EncryptedS3Secret::empty(),
            expiry: self.config.expiry,
            path_restrictions: self.config.path_restrictions.clone(),
            issued_by: self.config.issued_by,
            revoked_at: None,
        };
        if let Err(err) = access.encrypt_secret(&self.encryption_key, &plaintext) {
            return self.handle_error(err.into());
        }

        self.pending_secret = Some(Secret::new(plaintext));
        self.access = Some(access);
        self.state = CreateUserAccessState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn handle_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.handle_error(CreateUserAccessError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionStarted)",
                received: event,
            });
        };
        self.txn_id = Some(txn_id);
        self.state = CreateUserAccessState::ReadOwnerIndex;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: USER_ACCESS_OWNER_KEYSPACE.to_string(),
            key: owner_key(self.config.user_identity),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_index(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.handle_error(CreateUserAccessError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };
        let index = match decode_index(value.as_ref()) {
            Ok(index) => index,
            Err(error) => return self.handle_error(error.into()),
        };
        let Some(txn_id) = self.txn_id else {
            return self.handle_error(CreateUserAccessError::CreateUserAccessFailed);
        };
        let Some(new_access) = self.access.as_ref() else {
            return self.handle_error(CreateUserAccessError::CreateUserAccessFailed);
        };
        let replace = index.contains(&new_access.access_key);
        let mut reads: Vec<_> = index
            .iter()
            .map(|access_key| {
                (
                    USER_ACCESS_KEYSPACE.to_string(),
                    access_key.as_bytes().into(),
                )
            })
            .collect();
        if !replace {
            reads.push((
                USER_ACCESS_KEYSPACE.to_string(),
                new_access.access_key.as_bytes().into(),
            ));
        }
        self.state = CreateUserAccessState::ReadCredentials { index, replace };
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: Some(txn_id),
        })]
    }

    fn handle_credentials(
        &mut self,
        event: Event,
        index: std::collections::BTreeSet<String>,
        replace: bool,
    ) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.handle_error(CreateUserAccessError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::BatchReadResult)",
                received: event,
            });
        };
        if values.len() != index.len() + usize::from(!replace) {
            return self.handle_error(CreateUserAccessError::IndexInconsistent);
        }

        let Some(new_access) = self.access.as_ref() else {
            return self.handle_error(CreateUserAccessError::CreateUserAccessFailed);
        };
        let now = SystemTime::now();
        let mut active = std::collections::BTreeSet::new();
        let mut stale = Vec::new();
        for (key, value) in values {
            if !replace && key.as_ref() == new_access.access_key.as_bytes() {
                if value.is_some() {
                    return self.handle_error(CreateUserAccessError::IndexInconsistent);
                }
                continue;
            }
            let Some(value) = value else {
                return self.handle_error(CreateUserAccessError::IndexInconsistent);
            };
            let access = match UserAccess::from_bytes(value.as_ref()) {
                Ok(access) => access,
                Err(error) => return self.handle_error(error.into()),
            };
            if access.user_identity != self.config.user_identity
                || access.access_key.as_bytes() != key.as_ref()
                || !index.contains(&access.access_key)
            {
                return self.handle_error(CreateUserAccessError::IndexInconsistent);
            }
            let stale_record = access.is_revoked() || access.is_expired(now);
            if replace && access.access_key == new_access.access_key && !stale_record {
                return self.handle_error(CreateUserAccessError::IndexInconsistent);
            }
            if !stale_record {
                active.insert(access.access_key);
            } else {
                stale.push(access.access_key);
            }
        }
        if active.len() >= MAX_ACTIVE_CREDENTIALS {
            return self.handle_error(CreateUserAccessError::LimitReached);
        }
        active.insert(new_access.access_key.clone());
        if !stale.is_empty() {
            let Some(txn_id) = self.txn_id else {
                return self.handle_error(CreateUserAccessError::CreateUserAccessFailed);
            };
            self.state = CreateUserAccessState::DeleteStale { index: active };
            return smallvec![Effect::Storage(StorageEffect::BatchDelete {
                deletes: stale
                    .into_iter()
                    .map(|access_key| {
                        (
                            USER_ACCESS_KEYSPACE.to_string(),
                            access_key.as_bytes().into(),
                        )
                    })
                    .collect(),
                txn_id: Some(txn_id),
            })];
        }
        self.write_credentials(active)
    }

    fn handle_stale_deleted(
        &mut self,
        event: Event,
        index: std::collections::BTreeSet<String>,
    ) -> Effects {
        let Event::Storage(StorageEvent::BatchDeleteResult { .. }) = event else {
            return self.handle_error(CreateUserAccessError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::BatchDeleteResult)",
                received: event,
            });
        };
        self.write_credentials(index)
    }

    fn write_credentials(&mut self, index: std::collections::BTreeSet<String>) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.handle_error(CreateUserAccessError::CreateUserAccessFailed);
        };
        let Some(access) = self.access.as_ref() else {
            return self.handle_error(CreateUserAccessError::CreateUserAccessFailed);
        };
        let bytes = match access.to_bytes() {
            Ok(bytes) => bytes,
            Err(err) => return self.handle_error(err.into()),
        };
        let index_value = match encode_index(&index) {
            Ok(value) => value,
            Err(err) => return self.handle_error(err.into()),
        };
        self.state = CreateUserAccessState::WriteCredentials;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes: vec![
                (
                    USER_ACCESS_KEYSPACE.to_string(),
                    access.access_key.as_bytes().into(),
                    bytes.into(),
                ),
                (
                    USER_ACCESS_OWNER_KEYSPACE.to_string(),
                    owner_key(self.config.user_identity),
                    index_value,
                ),
            ],
            txn_id: Some(txn_id),
        })]
    }

    fn handle_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
            return self.handle_error(CreateUserAccessError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::BatchWriteResult)",
                received: event,
            });
        };

        let Some(access) = self.access.clone() else {
            return self.handle_error(CreateUserAccessError::CreateUserAccessFailed);
        };
        let Some(secret) = self.pending_secret.take() else {
            return self.handle_error(CreateUserAccessError::CreateUserAccessFailed);
        };
        self.output = Ok((access.access_key.clone(), secret, access));
        let Some(txn_id) = self.txn_id else {
            return self.handle_error(CreateUserAccessError::CreateUserAccessFailed);
        };
        self.state = CreateUserAccessState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_committed(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                self.txn_id = None;
                self.state = CreateUserAccessState::Finish;
                smallvec![]
            }
            Event::Storage(StorageEvent::Error { error }) => {
                if !matches!(&error, &StorageError::QueueFull) {
                    self.txn_id = None;
                }
                self.handle_error(error.into())
            }
            other => self.handle_error(CreateUserAccessError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                received: other,
            }),
        }
    }

    pub fn handle_error(&mut self, error: CreateUserAccessError) -> Effects {
        self.state = CreateUserAccessState::Error;
        self.output = Err(error);
        self.abort()
    }
}

impl Operation for CreateUserAccessOperation {
    type Output = Result<(String, Secret, UserAccess), CreateUserAccessError>;
    type Error = CreateUserAccessError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            CreateUserAccessState::Init => self.handle_init(),
            CreateUserAccessState::StartTransaction => self.handle_started(event),
            CreateUserAccessState::ReadOwnerIndex => self.handle_index(event),
            CreateUserAccessState::ReadCredentials { ref index, replace } => {
                self.handle_credentials(event, index.clone(), replace)
            }
            CreateUserAccessState::DeleteStale { ref index } => {
                self.handle_stale_deleted(event, index.clone())
            }
            CreateUserAccessState::WriteCredentials => self.handle_written(event),
            CreateUserAccessState::CommitTransaction => self.handle_committed(event),
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
    use crate::s3::access_index::owner_key;

    fn test_issuer() -> [u8; 32] {
        *iroh::SecretKey::from_bytes(&[9u8; 32]).public().as_bytes()
    }

    fn test_key() -> CredentialEncryptionKey {
        CredentialEncryptionKey::derive(&[9u8; 32])
    }

    fn make_config(user_identity: UserId, group_id: GroupId) -> CreateUserAccessConfig {
        CreateUserAccessConfig {
            user_identity,
            group_id,
            expiry: SystemTime::now() + DEFAULT_CREDENTIAL_TTL,
            path_restrictions: None,
            issued_by: test_issuer(),
        }
    }

    fn make_user_identity() -> UserId {
        UserId::default()
    }

    #[test]
    fn test_create_user_access_happy_path() {
        let user_identity = make_user_identity();
        let group_id = Ulid::generate();
        let mut op =
            CreateUserAccessOperation::new(make_config(user_identity, group_id), test_key());

        // Start opens the transaction before the owner index is checked.
        let effects = op.start();
        assert_eq!(effects.len(), 1);
        assert_eq!(op.state, CreateUserAccessState::StartTransaction);
        assert!(matches!(
            effects[0],
            Effect::Storage(StorageEffect::StartTransaction { read: false })
        ));

        let txn_id = Ulid::generate();
        let effects = op.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        let Effect::Storage(StorageEffect::Read {
            key_space,
            txn_id: Some(read_txn),
            ..
        }) = &effects[0]
        else {
            panic!("Expected owner index read");
        };
        assert_eq!(key_space, aruna_core::keyspaces::USER_ACCESS_OWNER_KEYSPACE);
        assert_eq!(*read_txn, txn_id);

        let Some(access) = op.access.as_ref() else {
            panic!("Expected generated access");
        };
        assert_eq!(access.user_identity, user_identity);
        assert_eq!(access.group_id, group_id);
        assert_eq!(access.open_secret(&test_key()).unwrap().len(), 30);
        assert_eq!(access.path_restrictions, None);
        assert_eq!(access.issued_by, test_issuer());
        assert_eq!(access.revoked_at, None);

        // An empty index still probes the fresh key for a collision in the txn.
        let access_key = access.access_key.clone();
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: owner_key(user_identity),
            value: None,
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::BatchRead { txn_id: Some(id), .. })] if *id == txn_id
        ));
        let effects = op.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![(access_key.as_bytes().into(), None)],
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::BatchWrite { txn_id: Some(id), .. })] if *id == txn_id
        ));
        let effects = op.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { txn_id: id })] if *id == txn_id
        ));
        let effects = op.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
        assert!(effects.is_empty());
        assert_eq!(op.state, CreateUserAccessState::Finish);
        assert!(op.is_complete());

        // 4. Finalize -> Should return Ok with (access_key, plaintext, UserAccess)
        let result = op.finalize();
        assert!(result.is_ok());
        let inner = result.unwrap();
        assert!(inner.is_ok());
        let (access_key, plaintext, returned_access) = inner.unwrap();
        assert_eq!(returned_access.user_identity, user_identity);
        assert_eq!(returned_access.group_id, group_id);
        assert_eq!(returned_access.access_key, access_key);
        // The one-time plaintext opens the stored ciphertext on the issuing key.
        assert_eq!(
            returned_access.open_secret(&test_key()).unwrap(),
            plaintext.expose()
        );
    }

    #[test]
    fn replaces_stale() {
        let user_identity = make_user_identity();
        let stale_key = "newkey".to_string();
        let mut op = CreateUserAccessOperation::new_with_key(
            make_config(user_identity, Ulid::generate()),
            "newkey".to_string(),
            test_key(),
        );
        op.start();
        let txn_id = Ulid::generate();
        op.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: owner_key(user_identity),
            value: Some(
                encode_index(&std::collections::BTreeSet::from([stale_key.clone()])).unwrap(),
            ),
        }));
        let stale = UserAccess {
            access_key: stale_key.clone(),
            user_identity,
            group_id: Ulid::generate(),
            secret: EncryptedS3Secret::empty(),
            expiry: SystemTime::UNIX_EPOCH,
            path_restrictions: None,
            issued_by: test_issuer(),
            revoked_at: None,
        };
        let effects = op.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![(
                stale_key.clone().into(),
                Some(stale.to_bytes().unwrap().into()),
            )],
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::BatchDelete { deletes, txn_id: Some(id) })]
                if *id == txn_id && deletes.len() == 1
        ));
        let effects = op.step(Event::Storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::BatchWrite { txn_id: Some(id), .. })]
                if *id == txn_id
        ));
    }

    #[test]
    fn rejects_active_collision() {
        let user_identity = make_user_identity();
        let mut op = CreateUserAccessOperation::new_with_key(
            make_config(user_identity, Ulid::generate()),
            "newkey".to_string(),
            test_key(),
        );
        op.start();
        let txn_id = Ulid::generate();
        op.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: owner_key(user_identity),
            value: Some(
                encode_index(&std::collections::BTreeSet::from(["newkey".to_string()])).unwrap(),
            ),
        }));
        let access = UserAccess {
            access_key: "newkey".to_string(),
            user_identity,
            group_id: Ulid::generate(),
            secret: EncryptedS3Secret::empty(),
            expiry: SystemTime::now() + Duration::from_secs(60),
            path_restrictions: None,
            issued_by: test_issuer(),
            revoked_at: None,
        };
        let effects = op.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![(
                "newkey".to_string().into(),
                Some(access.to_bytes().unwrap().into()),
            )],
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id: id })]
                if *id == txn_id
        ));
        assert!(matches!(
            op.finalize().unwrap_err(),
            CreateUserAccessError::IndexInconsistent
        ));
    }

    #[test]
    fn rejects_full_index() {
        let user_identity = make_user_identity();
        let keys = (0..MAX_ACTIVE_CREDENTIALS)
            .map(|index| format!("key{index}"))
            .collect::<std::collections::BTreeSet<_>>();
        let mut op = CreateUserAccessOperation::new_with_key(
            make_config(user_identity, Ulid::generate()),
            "newkey".to_string(),
            test_key(),
        );
        op.start();
        let txn_id = Ulid::generate();
        op.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: owner_key(user_identity),
            value: Some(encode_index(&keys).unwrap()),
        }));
        let mut values = keys
            .iter()
            .map(|key| {
                let access = UserAccess {
                    access_key: key.clone(),
                    user_identity,
                    group_id: Ulid::generate(),
                    secret: EncryptedS3Secret::empty(),
                    expiry: SystemTime::now() + Duration::from_secs(60),
                    path_restrictions: None,
                    issued_by: test_issuer(),
                    revoked_at: None,
                };
                (key.clone().into(), Some(access.to_bytes().unwrap().into()))
            })
            .collect::<Vec<_>>();
        values.push(("newkey".to_string().into(), None));
        let effects = op.step(Event::Storage(StorageEvent::BatchReadResult { values }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id: id })] if *id == txn_id
        ));
        assert!(matches!(
            op.finalize().unwrap_err(),
            CreateUserAccessError::LimitReached
        ));
    }

    #[test]
    fn rejects_oversized_restrictions() {
        // Over the cap the operation fails with no storage write emitted.
        use aruna_core::permission_path::MAX_TOKEN_RESTRICTIONS;
        use aruna_core::structs::Permission;
        let restrictions = (0..=MAX_TOKEN_RESTRICTIONS)
            .map(|index| PathRestriction {
                pattern: format!("/r/{index}/**"),
                permission: Permission::READ,
            })
            .collect::<Vec<_>>();
        let mut config = make_config(make_user_identity(), Ulid::generate());
        config.path_restrictions = Some(restrictions);
        let mut op = CreateUserAccessOperation::new(config, test_key());

        let effects = op.start();
        assert!(effects.is_empty());
        assert_eq!(op.state, CreateUserAccessState::Error);
        assert!(matches!(
            op.finalize().unwrap_err(),
            CreateUserAccessError::RestrictionLimit(_)
        ));
    }

    #[test]
    fn rejects_invalid_steps() {
        let user_identity = make_user_identity();
        let group_id = Ulid::generate();

        // Starting twice does not bypass the transaction state.
        let mut op =
            CreateUserAccessOperation::new(make_config(user_identity, group_id), test_key());
        op.start();
        let effects = op.start();
        assert!(effects.is_empty());
        assert_eq!(op.state, CreateUserAccessState::StartTransaction);

        // A wrong event aborts the open transaction and fails closed.
        let mut op =
            CreateUserAccessOperation::new(make_config(user_identity, group_id), test_key());
        op.start();
        let key = Ulid::generate().to_bytes().into();
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
