use aruna_core::UserId;
use aruna_core::compute::Secret;
use aruna_core::credential_seal::{CredentialSealKey, SealError, SealedS3Secret};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::USER_ACCESS_KEYSPACE;
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

pub const DEFAULT_CREDENTIAL_TTL: Duration = Duration::from_secs(24 * 60 * 60 * 365);

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
    #[error(transparent)]
    RestrictionLimit(#[from] RestrictionLimitError),
    #[error(transparent)]
    Seal(#[from] SealError),
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
    seal_key: CredentialSealKey,
    pending_secret: Option<Secret>,
    state: CreateUserAccessState,
    output: Result<(String, Secret, UserAccess), CreateUserAccessError>,
}

impl CreateUserAccessOperation {
    pub fn new(config: CreateUserAccessConfig, seal_key: CredentialSealKey) -> Self {
        Self::new_with_key(config, Ulid::generate().to_string(), seal_key)
    }

    pub fn new_with_key(
        config: CreateUserAccessConfig,
        key_id: String,
        seal_key: CredentialSealKey,
    ) -> Self {
        Self {
            config,
            key_id,
            seal_key,
            pending_secret: None,
            state: CreateUserAccessState::Init,
            output: Err(CreateUserAccessError::NotFinished),
        }
    }

    fn handle_init(&mut self) -> Effects {
        if let CreateUserAccessState::Init = self.state {
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
                access_key: access_key.clone(),
                user_identity: self.config.user_identity,
                group_id: self.config.group_id,
                secret: SealedS3Secret::empty(),
                expiry: self.config.expiry,
                path_restrictions: self.config.path_restrictions.clone(),
                issued_by: self.config.issued_by,
                revoked_at: None,
            };
            if let Err(err) = access.seal_secret(&self.seal_key, &plaintext) {
                return self.handle_error(err.into());
            }
            let bytes = match access.to_bytes() {
                Ok(bytes) => bytes,
                Err(err) => return self.handle_error(err.into()),
            };

            self.pending_secret = Some(Secret::new(plaintext));
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
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
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
        let access = access.clone();
        let Some(secret) = self.pending_secret.take() else {
            return self.handle_error(CreateUserAccessError::CreateUserAccessFailed);
        };
        // Issuer-local by design: the credential is never replicated, so the
        // plaintext is handed back only here, once, at issuance.
        self.output = Ok((access.access_key.clone(), secret, access));
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
    type Output = Result<(String, Secret, UserAccess), CreateUserAccessError>;
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
        let Ok((access_key, _, _)) = self.output.as_ref() else {
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

    fn test_issuer() -> [u8; 32] {
        *iroh::SecretKey::from_bytes(&[9u8; 32]).public().as_bytes()
    }

    fn test_seal_key() -> CredentialSealKey {
        CredentialSealKey::derive(&[9u8; 32])
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
            CreateUserAccessOperation::new(make_config(user_identity, group_id), test_seal_key());

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
        assert_eq!(access.open_secret(&test_seal_key()).unwrap().len(), 30);
        assert_eq!(access.path_restrictions, None);
        assert_eq!(access.issued_by, test_issuer());
        assert_eq!(access.revoked_at, None);

        // 2. Feed WriteResult -> issuer-local: no replication, straight to Finish
        let write_key = key.clone();
        let event = Event::Storage(StorageEvent::WriteResult { key: write_key });
        let effects = op.step(event);
        assert_eq!(effects.len(), 0);
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
            returned_access.open_secret(&test_seal_key()).unwrap(),
            plaintext.expose()
        );
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
        let mut op = CreateUserAccessOperation::new(config, test_seal_key());

        let effects = op.start();
        assert!(effects.is_empty());
        assert_eq!(op.state, CreateUserAccessState::Error);
        assert!(matches!(
            op.finalize().unwrap_err(),
            CreateUserAccessError::RestrictionLimit(_)
        ));
    }

    #[test]
    fn test_create_user_access_invalid_steps() {
        let user_identity = make_user_identity();
        let group_id = Ulid::generate();

        // 1. Invalid state: start twice -> second start calls abort since state is not Init
        let mut op =
            CreateUserAccessOperation::new(make_config(user_identity, group_id), test_seal_key());
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
        let mut op =
            CreateUserAccessOperation::new(make_config(user_identity, group_id), test_seal_key());
        op.start();
        // Feed a ReadResult instead of WriteResult
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
