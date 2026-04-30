use crate::errors::ConversionError;
use crate::types::{GroupId, UserId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::time::SystemTime;
use ulid::Ulid;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SourceConnectorKind {
    Http,
    S3,
    Webdav,
    Ftp,
    ArunaNative,
}

impl SourceConnectorKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::S3 => "s3",
            Self::Webdav => "webdav",
            Self::Ftp => "ftp",
            Self::ArunaNative => "aruna_native",
        }
    }
}

impl fmt::Display for SourceConnectorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceConnector {
    pub connector_id: Ulid,
    pub group_id: GroupId,
    pub name: String,
    pub kind: SourceConnectorKind,
    pub public_config: HashMap<String, String>,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
    pub created_by: UserId,
}

impl SourceConnector {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        connector_id: Ulid,
        group_id: GroupId,
        name: String,
        kind: SourceConnectorKind,
        public_config: HashMap<String, String>,
        created_at: SystemTime,
        updated_at: SystemTime,
        created_by: UserId,
    ) -> Self {
        Self {
            connector_id,
            group_id,
            name,
            kind,
            public_config,
            created_at,
            updated_at,
            created_by,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceConnectorSecret {
    pub connector_id: Ulid,
    pub secret_config: HashMap<String, String>,
    pub updated_at: SystemTime,
}

impl SourceConnectorSecret {
    pub fn new(
        connector_id: Ulid,
        secret_config: HashMap<String, String>,
        updated_at: SystemTime,
    ) -> Option<Self> {
        if secret_config.is_empty() {
            return None;
        }

        Some(Self {
            connector_id,
            secret_config,
            updated_at,
        })
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::UserId;
    use crate::structs::RealmId;

    fn test_user_id(seed: u8) -> UserId {
        UserId::local(Ulid::from_bytes([seed; 16]), RealmId([seed; 32]))
    }

    #[test]
    fn source_connector_roundtrip_preserves_public_config() {
        let connector = SourceConnector::new(
            Ulid::from_bytes([1u8; 16]),
            Ulid::from_bytes([2u8; 16]),
            "refdata".to_string(),
            SourceConnectorKind::S3,
            HashMap::from([
                ("bucket".to_string(), "reads".to_string()),
                ("endpoint".to_string(), "https://s3.example.com".to_string()),
                ("region".to_string(), "eu-central-1".to_string()),
            ]),
            SystemTime::UNIX_EPOCH,
            SystemTime::UNIX_EPOCH,
            test_user_id(3),
        );

        let restored = SourceConnector::from_bytes(&connector.to_bytes().unwrap()).unwrap();

        assert_eq!(connector, restored);
        assert_eq!(restored.kind, SourceConnectorKind::S3);
    }

    #[test]
    fn empty_secret_config_does_not_create_secret_record() {
        let secret = SourceConnectorSecret::new(
            Ulid::from_bytes([4u8; 16]),
            HashMap::new(),
            SystemTime::UNIX_EPOCH,
        );

        assert!(secret.is_none());
    }

    #[test]
    fn public_record_serialization_stays_redacted() {
        let connector = SourceConnector::new(
            Ulid::from_bytes([5u8; 16]),
            Ulid::from_bytes([6u8; 16]),
            "webdav".to_string(),
            SourceConnectorKind::Webdav,
            HashMap::from([(
                "endpoint".to_string(),
                "https://webdav.example.com".to_string(),
            )]),
            SystemTime::UNIX_EPOCH,
            SystemTime::UNIX_EPOCH,
            test_user_id(7),
        );
        let secret = SourceConnectorSecret::new(
            connector.connector_id,
            HashMap::from([
                ("username".to_string(), "alice".to_string()),
                ("password".to_string(), "super-secret".to_string()),
            ]),
            SystemTime::UNIX_EPOCH,
        )
        .unwrap();

        let public_bytes = connector.to_bytes().unwrap();
        let secret_bytes = secret.to_bytes().unwrap();

        assert!(
            !public_bytes
                .windows("super-secret".len())
                .any(|window| window == b"super-secret")
        );
        assert!(
            secret_bytes
                .windows("super-secret".len())
                .any(|window| window == b"super-secret")
        );
    }
}
