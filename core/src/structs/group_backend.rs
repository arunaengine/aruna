use crate::errors::ConversionError;
use crate::structs::CleanupStrategy;
use crate::types::{GroupId, UserId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::time::SystemTime;
use ulid::Ulid;

/// The closed set of tenant-registrable write backends. WebDAV is absent by
/// design: its opendal writer rejects the second chunk of a stream.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum GroupBackendKind {
    S3,
    Gcs,
    Azblob,
    Azdls,
    B2,
}

impl GroupBackendKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::S3 => "s3",
            Self::Gcs => "gcs",
            Self::Azblob => "azblob",
            Self::Azdls => "azdls",
            Self::B2 => "b2",
        }
    }

    pub const ALL: [Self; 5] = [Self::S3, Self::Gcs, Self::Azblob, Self::Azdls, Self::B2];
}

impl fmt::Display for GroupBackendKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for GroupBackendKind {
    type Err = ConversionError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "s3" => Ok(Self::S3),
            "gcs" => Ok(Self::Gcs),
            "azblob" => Ok(Self::Azblob),
            "azdls" => Ok(Self::Azdls),
            "b2" => Ok(Self::B2),
            other => Err(ConversionError::FromStrError(format!(
                "unknown group backend kind `{other}`"
            ))),
        }
    }
}

/// A tenant-registered write backend on the group's own object store. Secrets
/// live in a separate record and are never part of this one.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GroupStorageBackend {
    pub backend_id: Ulid,
    pub group_id: GroupId,
    pub name: String,
    pub kind: GroupBackendKind,
    pub public_config: HashMap<String, String>,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
    pub created_by: UserId,
    /// Set while a deletion drains its writers: routing stops choosing the
    /// backend and every write that reads this record in its own transaction
    /// refuses or conflicts.
    pub disabled: bool,
    /// Tenant storage holds tenant data, so this defaults to `Retain`; the
    /// tenant opts into reclaim and no operator setting overrides it.
    pub cleanup: CleanupStrategy,
}

impl GroupStorageBackend {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct GroupStorageBackendSecret {
    pub backend_id: Ulid,
    pub secret_config: HashMap<String, String>,
    pub updated_at: SystemTime,
}

/// Redacted on purpose: this record reaches effect and error formatting, and
/// its values are live credentials.
impl fmt::Debug for GroupStorageBackendSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GroupStorageBackendSecret")
            .field("backend_id", &self.backend_id)
            .field("keys", &self.secret_config.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl GroupStorageBackendSecret {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use super::{GroupBackendKind, GroupStorageBackend, GroupStorageBackendSecret};
    use crate::UserId;
    use crate::structs::CleanupStrategy;
    use crate::structs::RealmId;
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::time::SystemTime;
    use ulid::Ulid;

    #[test]
    fn secret_stays_separate() {
        // The public record must never carry credential material.
        let backend_id = Ulid::from_bytes([1u8; 16]);
        let record = GroupStorageBackend {
            backend_id,
            group_id: Ulid::from_bytes([2u8; 16]),
            name: "tenant-s3".to_string(),
            kind: GroupBackendKind::S3,
            public_config: HashMap::from([("bucket".to_string(), "data".to_string())]),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            created_by: UserId::local(Ulid::from_bytes([3u8; 16]), RealmId([4u8; 32])),
            disabled: false,
            cleanup: CleanupStrategy::Retain,
        };
        let secret = GroupStorageBackendSecret {
            backend_id,
            secret_config: HashMap::from([(
                "secret_access_key".to_string(),
                "super-secret".to_string(),
            )]),
            updated_at: SystemTime::UNIX_EPOCH,
        };

        let public_bytes = record.to_bytes().unwrap();

        assert!(
            !public_bytes
                .windows("super-secret".len())
                .any(|window| window == b"super-secret")
        );
        assert_eq!(
            GroupStorageBackend::from_bytes(&public_bytes).unwrap(),
            record
        );
        assert_eq!(
            GroupStorageBackendSecret::from_bytes(&secret.to_bytes().unwrap()).unwrap(),
            secret
        );
    }

    #[test]
    fn debug_hides_secrets() {
        let secret = GroupStorageBackendSecret {
            backend_id: Ulid::from_bytes([1u8; 16]),
            secret_config: HashMap::from([("account_key".to_string(), "hunter2".to_string())]),
            updated_at: SystemTime::UNIX_EPOCH,
        };

        let rendered = format!("{secret:?}");

        assert!(!rendered.contains("hunter2"));
        assert!(rendered.contains("account_key"));
    }

    #[test]
    fn parses_known_kinds() {
        for kind in GroupBackendKind::ALL {
            assert_eq!(GroupBackendKind::from_str(kind.as_str()).unwrap(), kind);
        }
        assert!(GroupBackendKind::from_str("webdav").is_err());
    }
}
