use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use ulid::Ulid;

use crate::errors::ConversionError;
use crate::structs::RealmId;
use crate::types::Key;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct UserId {
    pub user_ulid: Ulid,
    pub realm_id: RealmId,
}

impl UserId {
    #[inline]
    pub fn new(user_ulid: Ulid, realm_id: RealmId) -> Self {
        Self {
            user_ulid,
            realm_id,
        }
    }

    #[inline]
    pub fn local(user_ulid: Ulid, realm_id: RealmId) -> Self {
        Self::new(user_ulid, realm_id)
    }

    #[inline]
    pub fn nil(realm_id: RealmId) -> Self {
        Self::new(Ulid::from_bytes([0u8; 16]), realm_id)
    }

    #[inline]
    pub fn is_nil(&self) -> bool {
        self.user_ulid.is_nil()
    }

    #[inline]
    pub fn is_nil_in(&self, realm_id: RealmId) -> bool {
        self.is_nil() && self.realm_id == realm_id
    }

    #[inline]
    pub fn to_storage_key(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(48);
        bytes.extend_from_slice(self.realm_id.as_bytes());
        bytes.extend_from_slice(&self.user_ulid.to_bytes());
        bytes
    }

    #[inline]
    pub fn to_bytes(&self) -> Vec<u8> {
        self.to_storage_key()
    }

    #[inline]
    pub fn from_storage_key(bytes: &[u8]) -> Result<Self, ConversionError> {
        if bytes.len() != 48 {
            return Err(ConversionError::InvalidLength(format!(
                "expected 48-byte user storage key, got {} bytes",
                bytes.len()
            )));
        }
        let realm_id = RealmId::from_bytes(
            bytes[..32]
                .try_into()
                .map_err(|_| ConversionError::InvalidUserId)?,
        );
        let user_ulid = Ulid::from_bytes(
            bytes[32..]
                .try_into()
                .map_err(|_| ConversionError::InvalidUserId)?,
        );
        Ok(Self {
            user_ulid,
            realm_id,
        })
    }

    #[inline]
    pub fn storage_prefix(realm_id: RealmId) -> Key {
        realm_id.as_bytes().to_vec().into()
    }

    #[inline]
    pub fn from_string(value: &str) -> Result<Self, ConversionError> {
        Self::from_str(value)
    }
}

impl fmt::Display for UserId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.user_ulid, self.realm_id)
    }
}

impl FromStr for UserId {
    type Err = ConversionError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (user, realm) = value
            .split_once('@')
            .ok_or(ConversionError::InvalidUserId)?;
        Ok(Self {
            user_ulid: Ulid::from_string(user)?,
            realm_id: RealmId::from_base64(realm)?,
        })
    }
}

impl Default for UserId {
    fn default() -> Self {
        Self::new(Ulid::from_bytes([0u8; 16]), RealmId([0u8; 32]))
    }
}

#[cfg(test)]
mod tests {
    use super::UserId;
    use crate::structs::RealmId;
    use std::str::FromStr;
    use ulid::Ulid;

    #[test]
    fn user_id_roundtrips_through_string() {
        let user_id = UserId::new(Ulid::new(), RealmId([7u8; 32]));
        assert_eq!(UserId::from_str(&user_id.to_string()).unwrap(), user_id);
    }

    #[test]
    fn user_id_roundtrips_through_storage_key() {
        let user_id = UserId::new(Ulid::new(), RealmId([9u8; 32]));
        assert_eq!(
            UserId::from_storage_key(&user_id.to_storage_key()).unwrap(),
            user_id
        );
    }

    #[test]
    fn nil_user_id_is_realm_scoped() {
        let realm_id = RealmId([1u8; 32]);
        let other_realm_id = RealmId([2u8; 32]);
        let user_id = UserId::nil(realm_id);

        assert!(user_id.is_nil());
        assert!(user_id.is_nil_in(realm_id));
        assert!(!user_id.is_nil_in(other_realm_id));
        assert!(!UserId::local(Ulid::new(), realm_id).is_nil());
    }
}
