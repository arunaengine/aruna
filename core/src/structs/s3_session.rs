use crate::auth::credential_hash;
use crate::credential_encryption::{
    CredentialEncryptionKey, EncryptedS3Secret, EncryptionError, credential_aad,
};
use crate::errors::ConversionError;
use crate::structs::{PathRestriction, UserAccess};
use crate::types::{GroupId, UserId};
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};
use ulid::Ulid;

pub const S3_SESSION_MAX_TTL: Duration = Duration::from_secs(60 * 60);
pub const S3_SESSION_REFRESH_WINDOW: Duration = Duration::from_secs(5 * 60);
pub const S3_SESSION_ACCESS_PREFIX: &str = "ASIA";

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct S3Session {
    pub access_key: String,
    pub user_identity: UserId,
    pub group_id: GroupId,
    pub secret: EncryptedS3Secret,
    pub token_hash: String,
    pub expiry: SystemTime,
    pub path_restrictions: Option<Vec<PathRestriction>>,
    pub issued_by: [u8; 32],
    pub last_used_at: Option<SystemTime>,
}

impl S3Session {
    pub fn build_access_key(key_id: &str) -> Result<String, ConversionError> {
        let key_id = key_id.parse::<Ulid>()?.to_string();
        Ok(format!("{S3_SESSION_ACCESS_PREFIX}{key_id}"))
    }

    pub fn is_session_key(access_key: &str) -> bool {
        access_key.starts_with(S3_SESSION_ACCESS_PREFIX)
    }

    pub fn valid_access_key(access_key: &str) -> bool {
        access_key
            .strip_prefix(S3_SESSION_ACCESS_PREFIX)
            .and_then(|key_id| key_id.parse::<Ulid>().ok())
            .is_some_and(|key_id| format!("{S3_SESSION_ACCESS_PREFIX}{key_id}") == access_key)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }

    pub fn is_expired(&self, now: SystemTime) -> bool {
        self.expiry <= now
    }

    pub fn can_refresh(&self, now: SystemTime) -> bool {
        !self.is_expired(now)
            && self
                .expiry
                .duration_since(now)
                .is_ok_and(|remaining| remaining <= S3_SESSION_REFRESH_WINDOW)
    }

    pub fn token_matches(&self, token_hash: &str) -> bool {
        self.token_hash == token_hash
    }

    pub fn credential_aad(&self) -> Vec<u8> {
        credential_aad(
            &self.access_key,
            self.user_identity,
            self.group_id,
            self.issued_by,
            self.expiry,
        )
    }

    pub fn encrypt_secret(
        &mut self,
        key: &CredentialEncryptionKey,
        plaintext: &str,
    ) -> Result<(), EncryptionError> {
        self.secret = EncryptedS3Secret::encrypt(key, plaintext, &self.credential_aad())?;
        Ok(())
    }

    pub fn open_secret(&self, key: &CredentialEncryptionKey) -> Result<String, EncryptionError> {
        self.secret.open(key, &self.credential_aad())
    }

    pub fn as_user_access(&self) -> UserAccess {
        UserAccess {
            access_key: self.access_key.clone(),
            user_identity: self.user_identity,
            group_id: self.group_id,
            secret: self.secret.clone(),
            expiry: self.expiry,
            path_restrictions: self.path_restrictions.clone(),
            issued_by: self.issued_by,
            revoked_at: None,
        }
    }

    pub fn hash_token(token: &str) -> String {
        credential_hash(token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::RealmId;

    fn session(expiry: SystemTime) -> S3Session {
        S3Session {
            access_key: S3Session::build_access_key("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            user_identity: UserId::local(
                Ulid::from_bytes([2u8; 16]),
                RealmId::from_bytes([1u8; 32]),
            ),
            group_id: Ulid::from_bytes([3u8; 16]),
            secret: EncryptedS3Secret::empty(),
            token_hash: S3Session::hash_token("token"),
            expiry,
            path_restrictions: None,
            issued_by: [4u8; 32],
            last_used_at: None,
        }
    }

    #[test]
    fn key_namespace_distinct() {
        let key = S3Session::build_access_key("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        assert!(S3Session::is_session_key(&key));
        assert!(S3Session::valid_access_key(&key));
        assert!(!S3Session::is_session_key("01ARZ3NDEKTSV4RRFFQ69G5FAV"));
    }

    #[test]
    fn refresh_at_boundary() {
        let start = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let session = session(start + S3_SESSION_MAX_TTL);

        assert!(!session.can_refresh(start + Duration::from_secs(54 * 60 + 59)));
        assert!(session.can_refresh(start + Duration::from_secs(55 * 60)));
        assert!(!session.can_refresh(start + S3_SESSION_MAX_TTL));
    }

    #[test]
    fn token_is_exact() {
        let session = session(SystemTime::UNIX_EPOCH + Duration::from_secs(2_000));
        assert!(session.token_matches(&S3Session::hash_token("token")));
        assert!(!session.token_matches(&S3Session::hash_token("Token")));
    }
}
