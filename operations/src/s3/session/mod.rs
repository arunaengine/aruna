mod create;
mod get;
mod purge;
mod refresh;
mod touch;

pub use create::{CreateS3SessionConfig, CreateS3SessionOperation};
pub use get::GetS3SessionOperation;
pub use purge::{PurgeS3SessionsOperation, PurgeS3SessionsResult};
pub use refresh::{RefreshS3SessionConfig, RefreshS3SessionOperation};
pub use touch::{TouchS3SessionConfig, TouchS3SessionOperation};

use crate::driver::{DriverContext, drive};
use aruna_core::compute::Secret;
use aruna_core::credential_seal::{CredentialSealKey, SealError, SealedS3Secret};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::Event;
use aruna_core::permission_path::{RestrictionLimitError, validate_restriction_limits};
use aruna_core::shutdown::Shutdown;
use aruna_core::structs::{PathRestriction, S3Session};
use aruna_core::types::{GroupId, Key, UserId, Value};
use byteview::ByteView;
use rand::distr::Alphanumeric;
use rand::{RngExt, rng};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tracing::warn;

pub const MAX_GROUP_SESSIONS: usize = 4;
pub const PURGE_BATCH: usize = 256;
const SWEEP_INTERVAL: Duration = Duration::from_secs(60);
const SIGNING_SECRET_LEN: usize = 30;
const SESSION_TOKEN_LEN: usize = 64;

#[derive(Debug, Error, PartialEq)]
pub enum S3SessionError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Restriction(#[from] RestrictionLimitError),
    #[error(transparent)]
    Seal(#[from] SealError),
    #[error("session access key is invalid")]
    InvalidAccessKey,
    #[error("session expiry is invalid")]
    InvalidExpiry,
    #[error("active session limit reached")]
    LimitReached,
    #[error("session index is inconsistent")]
    IndexInconsistent,
    #[error("session not found")]
    NotFound,
    #[error("session token is invalid")]
    InvalidToken,
    #[error("session has expired")]
    Expired,
    #[error("session belongs to another issuer")]
    WrongIssuer,
    #[error("session belongs to another user")]
    WrongOwner,
    #[error("session belongs to another group")]
    WrongGroup,
    #[error("session is not in its refresh window")]
    TooEarly,
    #[error("session has not been used since it was issued")]
    Idle,
    #[error("session operation is not finished")]
    NotFinished,
    #[error("session operation failed")]
    Failed,
    #[error("state {state} expected {expected}, received {received:?}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        received: Event,
    },
}

impl S3SessionError {
    fn expected(&self) -> bool {
        matches!(
            self,
            Self::InvalidAccessKey
                | Self::InvalidExpiry
                | Self::LimitReached
                | Self::NotFound
                | Self::InvalidToken
                | Self::Expired
                | Self::WrongIssuer
                | Self::WrongOwner
                | Self::WrongGroup
                | Self::TooEarly
                | Self::Idle
        )
    }
}

#[derive(Debug, PartialEq)]
pub struct S3SessionCredentials {
    pub access_key_id: String,
    pub secret_access_key: Secret,
    pub session_token: Secret,
    pub session: S3Session,
}

fn owner_key(user_identity: UserId, group_id: GroupId) -> Key {
    let mut key = user_identity.to_storage_key();
    key.extend_from_slice(&group_id.to_bytes());
    key.into()
}

fn decode_index(value: Option<&ByteView>) -> Result<BTreeSet<String>, S3SessionError> {
    let Some(value) = value else {
        return Ok(BTreeSet::new());
    };
    let index: BTreeSet<String> =
        postcard::from_bytes(value.as_ref()).map_err(ConversionError::from)?;
    if index.len() > MAX_GROUP_SESSIONS
        || index
            .iter()
            .any(|access_key| !S3Session::valid_access_key(access_key))
    {
        return Err(S3SessionError::IndexInconsistent);
    }
    Ok(index)
}

fn encode_index(index: &BTreeSet<String>) -> Result<Value, S3SessionError> {
    if index.len() > MAX_GROUP_SESSIONS
        || index
            .iter()
            .any(|access_key| !S3Session::valid_access_key(access_key))
    {
        return Err(S3SessionError::IndexInconsistent);
    }
    Ok(ByteView::from(
        postcard::to_allocvec(index).map_err(ConversionError::from)?,
    ))
}

fn expiry_secs(expiry: SystemTime) -> Result<u64, S3SessionError> {
    expiry
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| S3SessionError::InvalidExpiry)
}

fn expiry_key(expiry: SystemTime, access_key: &str) -> Result<Key, S3SessionError> {
    if !S3Session::valid_access_key(access_key) {
        return Err(S3SessionError::InvalidAccessKey);
    }
    let mut key = Vec::with_capacity(8 + access_key.len());
    key.extend_from_slice(&expiry_secs(expiry)?.to_be_bytes());
    key.extend_from_slice(access_key.as_bytes());
    Ok(key.into())
}

fn expiry_parts(key: &[u8]) -> Option<(u64, String)> {
    let seconds = u64::from_be_bytes(key.get(..8)?.try_into().ok()?);
    let access_key = std::str::from_utf8(key.get(8..)?).ok()?.to_string();
    S3Session::valid_access_key(&access_key).then_some((seconds, access_key))
}

fn generate_secret(length: usize) -> Secret {
    Secret::new(
        rng()
            .sample_iter(&Alphanumeric)
            .take(length)
            .map(char::from)
            .collect::<String>(),
    )
}

#[allow(clippy::too_many_arguments)]
fn build_session(
    access_key: String,
    user_identity: UserId,
    group_id: GroupId,
    expiry: SystemTime,
    path_restrictions: Option<Vec<PathRestriction>>,
    issued_by: [u8; 32],
    seal_key: &CredentialSealKey,
) -> Result<S3SessionCredentials, S3SessionError> {
    if let Some(restrictions) = path_restrictions.as_deref() {
        validate_restriction_limits(restrictions)?;
    }
    if !S3Session::valid_access_key(&access_key) {
        return Err(S3SessionError::InvalidAccessKey);
    }
    let secret_access_key = generate_secret(SIGNING_SECRET_LEN);
    let session_token = generate_secret(SESSION_TOKEN_LEN);
    let mut session = S3Session {
        access_key: access_key.clone(),
        user_identity,
        group_id,
        secret: SealedS3Secret::empty(),
        token_hash: S3Session::hash_token(session_token.expose()),
        expiry,
        path_restrictions,
        issued_by,
        last_used_at: None,
    };
    session.seal_secret(seal_key, secret_access_key.expose())?;
    Ok(S3SessionCredentials {
        access_key_id: access_key,
        secret_access_key,
        session_token,
        session,
    })
}

pub fn spawn_session_sweep(context: Arc<DriverContext>, shutdown: &Shutdown) {
    let token = shutdown.token();
    shutdown.spawn(async move {
        loop {
            tokio::select! {
                _ = token.cancelled() => return,
                _ = tokio::time::sleep(SWEEP_INTERVAL) => {}
            }
            loop {
                match drive(
                    PurgeS3SessionsOperation::new(SystemTime::now()),
                    context.as_ref(),
                )
                .await
                {
                    Ok(result) if result.scanned == PURGE_BATCH && result.removed > 0 => {
                        tokio::task::yield_now().await
                    }
                    Ok(_) => break,
                    Err(error) => {
                        warn!(error = ?error, "S3 session expiry sweep failed");
                        break;
                    }
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::create_user_access::{CreateUserAccessConfig, CreateUserAccessOperation};
    use crate::s3::list_user_access::{ListUserAccessInput, ListUserAccessOperation};
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{S3_SESSION_EXPIRY_KEYSPACE, S3_SESSION_OWNER_KEYSPACE};
    use aruna_core::structs::{RealmId, S3_SESSION_MAX_TTL};
    use aruna_storage::FjallStorage;
    use tempfile::TempDir;
    use ulid::Ulid;

    fn test_context() -> (TempDir, DriverContext) {
        let directory = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(directory.path().to_str().unwrap()).unwrap();
        (
            directory,
            DriverContext {
                storage_handle: storage,
                net_handle: None,
                blob_handle: None,
                metadata_handle: None,
                task_handle: None,
                compute_handle: None,
            },
        )
    }

    fn session_config(
        user_identity: UserId,
        group_id: GroupId,
        now: SystemTime,
        expiry: SystemTime,
        issued_by: [u8; 32],
    ) -> CreateS3SessionConfig {
        CreateS3SessionConfig {
            user_identity,
            group_id,
            now,
            expiry,
            path_restrictions: None,
            issued_by,
        }
    }

    #[test]
    fn index_is_bounded() {
        let index = (0..=MAX_GROUP_SESSIONS)
            .map(|_| S3Session::build_access_key(&Ulid::generate().to_string()).unwrap())
            .collect();
        assert_eq!(encode_index(&index), Err(S3SessionError::IndexInconsistent));
    }

    #[test]
    fn owner_key_scoped() {
        let user = UserId::local(Ulid::from_bytes([2u8; 16]), RealmId::from_bytes([1u8; 32]));
        let first = owner_key(user, Ulid::from_bytes([3u8; 16]));
        let second = owner_key(user, Ulid::from_bytes([4u8; 16]));
        assert_ne!(first, second);
        assert_eq!(first.len(), 64);
    }

    #[tokio::test]
    async fn refresh_at_boundary() {
        let (_directory, context) = test_context();
        let seal_key = CredentialSealKey::derive(&[7u8; 32]);
        let user = UserId::local(Ulid::from_bytes([2u8; 16]), RealmId::from_bytes([1u8; 32]));
        let group = Ulid::from_bytes([3u8; 16]);
        let issuer = [4u8; 32];
        let start = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let issued = drive(
            CreateS3SessionOperation::new(
                session_config(user, group, start, start + S3_SESSION_MAX_TTL, issuer),
                seal_key.clone(),
            ),
            &context,
        )
        .await
        .unwrap();
        drive(
            TouchS3SessionOperation::new(TouchS3SessionConfig {
                access_key: issued.access_key_id.clone(),
                token_hash: S3Session::hash_token(issued.session_token.expose()),
                now: start + Duration::from_secs(60),
                issued_by: issuer,
            }),
            &context,
        )
        .await
        .unwrap();

        let early = start + Duration::from_secs(55 * 60 - 1);
        let error = drive(
            RefreshS3SessionOperation::new(
                RefreshS3SessionConfig {
                    access_key: issued.access_key_id.clone(),
                    user_identity: user,
                    group_id: group,
                    now: early,
                    expiry: early + S3_SESSION_MAX_TTL,
                    path_restrictions: None,
                    issued_by: issuer,
                },
                seal_key.clone(),
            ),
            &context,
        )
        .await
        .unwrap_err();
        assert_eq!(error, S3SessionError::TooEarly);

        let boundary = start + Duration::from_secs(55 * 60);
        let refreshed = drive(
            RefreshS3SessionOperation::new(
                RefreshS3SessionConfig {
                    access_key: issued.access_key_id.clone(),
                    user_identity: user,
                    group_id: group,
                    now: boundary,
                    expiry: boundary + S3_SESSION_MAX_TTL,
                    path_restrictions: None,
                    issued_by: issuer,
                },
                seal_key,
            ),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(refreshed.access_key_id, issued.access_key_id);
        assert_ne!(
            refreshed.secret_access_key.expose(),
            issued.secret_access_key.expose()
        );
        assert_ne!(
            refreshed.session_token.expose(),
            issued.session_token.expose()
        );
        assert_eq!(refreshed.session.last_used_at, None);

        let error = drive(
            TouchS3SessionOperation::new(TouchS3SessionConfig {
                access_key: refreshed.access_key_id,
                token_hash: S3Session::hash_token(issued.session_token.expose()),
                now: boundary + Duration::from_secs(1),
                issued_by: issuer,
            }),
            &context,
        )
        .await
        .unwrap_err();
        assert_eq!(error, S3SessionError::InvalidToken);
    }

    #[tokio::test]
    async fn purge_removes_expired() {
        let (_directory, context) = test_context();
        let user = UserId::local(Ulid::from_bytes([2u8; 16]), RealmId::from_bytes([1u8; 32]));
        let group = Ulid::from_bytes([3u8; 16]);
        let issuer = [4u8; 32];
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let issued = drive(
            CreateS3SessionOperation::new(
                session_config(user, group, now, now + Duration::from_secs(100), issuer),
                CredentialSealKey::derive(&[7u8; 32]),
            ),
            &context,
        )
        .await
        .unwrap();

        let result = drive(
            PurgeS3SessionsOperation::new(now + Duration::from_secs(100)),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(result.purged, 1);
        assert_eq!(
            drive(GetS3SessionOperation::new(issued.access_key_id), &context)
                .await
                .unwrap(),
            None
        );
        let owner = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: S3_SESSION_OWNER_KEYSPACE.to_string(),
                key: owner_key(user, group),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            owner,
            Event::Storage(StorageEvent::ReadResult { value: None, .. })
        ));
        let expiry = context
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: S3_SESSION_EXPIRY_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 1,
                txn_id: None,
            })
            .await;
        assert!(matches!(
            expiry,
            Event::Storage(StorageEvent::IterResult { values, .. }) if values.is_empty()
        ));
    }

    #[tokio::test]
    async fn sessions_bypass_cap() {
        let (_directory, context) = test_context();
        let realm = RealmId::from_bytes([1u8; 32]);
        let user = UserId::local(Ulid::from_bytes([2u8; 16]), realm);
        let group = Ulid::from_bytes([3u8; 16]);
        let far_future = SystemTime::UNIX_EPOCH + Duration::from_secs(4_000_000_000);
        for _ in 0..16 {
            drive(
                CreateUserAccessOperation::new(
                    CreateUserAccessConfig {
                        user_identity: user,
                        group_id: group,
                        expiry: far_future,
                        path_restrictions: None,
                        issued_by: [4u8; 32],
                    },
                    CredentialSealKey::derive(&[7u8; 32]),
                ),
                &context,
            )
            .await
            .unwrap()
            .unwrap();
        }
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let session = drive(
            CreateS3SessionOperation::new(
                session_config(user, group, now, now + Duration::from_secs(600), [4u8; 32]),
                CredentialSealKey::derive(&[7u8; 32]),
            ),
            &context,
        )
        .await
        .unwrap();
        let listed = drive(
            ListUserAccessOperation::new(ListUserAccessInput {
                user_identity: user,
            }),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(listed.len(), 16);
        assert!(
            listed
                .iter()
                .all(|credential| credential.access_key != session.access_key_id)
        );

        for _ in 1..MAX_GROUP_SESSIONS {
            drive(
                CreateS3SessionOperation::new(
                    session_config(user, group, now, now + Duration::from_secs(600), [4u8; 32]),
                    CredentialSealKey::derive(&[7u8; 32]),
                ),
                &context,
            )
            .await
            .unwrap();
        }
        let error = drive(
            CreateS3SessionOperation::new(
                session_config(user, group, now, now + Duration::from_secs(600), [4u8; 32]),
                CredentialSealKey::derive(&[7u8; 32]),
            ),
            &context,
        )
        .await
        .unwrap_err();
        assert_eq!(error, S3SessionError::LimitReached);
    }
}
