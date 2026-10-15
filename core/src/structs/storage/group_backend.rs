//! Defines the storage backends a group may register and the secrets stored with them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::UserId;
use crate::compute::SecretBytes;
use crate::errors::ConversionError;
use crate::node_vault::{VaultEntry, VaultPurpose};
use crate::structs::storage::blob::MULTIPART_PART_PREFIX;
use crate::structs::storage::cleanup::CleanupStrategy;
use crate::types::GroupId;
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

    /// The config key naming the tenant's own container; nothing is ever minted.
    pub const fn container_key(self) -> &'static str {
        match self {
            Self::S3 | Self::Gcs | Self::B2 => "bucket",
            Self::Azblob => "container",
            Self::Azdls => "filesystem",
        }
    }
}

/// Where registration writes and removes its probe object.
pub const PROBE_PREFIX: &str = "_aruna_backend_probe";

/// Public config key Aruna sets: the bucket of the default rule on a storage without a bucket.
pub const DEFAULT_BUCKET: &str = "default_bucket";

/// Public config key prefix Aruna sets per checked bucket: `created` or `found`.
pub const BUCKET_STATE: &str = "bucket:";

/// Public config key prefix Aruna sets per bucket that ever held the default rule; Aruna's
/// own files may still live there.
pub const LAYOUT_BUCKET: &str = "layout:";

/// Object keys longer than this are refused by S3, GCS, Azure and B2.
pub const MAX_KEY_LEN: usize = 1024;

/// The bucket a path names on a storage without a bucket: its first folder.
pub fn path_bucket(path: &str) -> Option<&str> {
    path.split_once('/')
        .map(|(bucket, _)| bucket)
        .filter(|bucket| !bucket.is_empty())
}

/// S3 bucket naming: 3 to 63 lowercase letters, digits, dots and hyphens, starting and
/// ending with a letter or digit, without `..`.
pub fn valid_bucket(name: &str) -> bool {
    let edge = |byte: Option<&u8>| byte.is_some_and(|byte| byte.is_ascii_alphanumeric());
    (3..=63).contains(&name.len())
        && name.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'.' || byte == b'-'
        })
        && edge(name.as_bytes().first())
        && edge(name.as_bytes().last())
        && !name.contains("..")
}

/// Whether `key` lies where Aruna keeps its own objects: below a probe folder or one of
/// `folders`.
pub fn reserved_path(folders: &[String], key: &str) -> bool {
    std::iter::once(PROBE_PREFIX)
        .chain(folders.iter().map(String::as_str))
        .any(|folder| {
            key.strip_prefix(folder)
                .is_some_and(|rest| rest.starts_with('/'))
        })
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
pub struct GroupStorage {
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

impl GroupStorage {
    /// Whether `key` lies where Aruna writes its own objects on this backend.
    pub fn is_reserved(&self, key: &str) -> bool {
        reserved_path(&self.reserved_folders(), key)
    }

    /// The tenant's own container, which also holds Aruna's own layout.
    pub fn container(&self) -> Option<&str> {
        self.public_config
            .get(self.kind.container_key())
            .map(String::as_str)
    }

    /// An S3 storage without a bucket: the first folder of each path names the bucket.
    pub fn splits(&self) -> bool {
        self.kind == GroupBackendKind::S3 && self.container().is_none()
    }

    /// The bucket of the default rule on a storage without a bucket.
    pub fn default_bucket(&self) -> Option<&str> {
        self.public_config.get(DEFAULT_BUCKET).map(String::as_str)
    }

    /// The public config entries Aruna sets itself; a replaced record keeps them.
    pub fn managed_config(&self) -> impl Iterator<Item = (String, String)> + '_ {
        self.public_config
            .iter()
            .filter(|(key, _)| {
                key.as_str() == DEFAULT_BUCKET
                    || key.starts_with(BUCKET_STATE)
                    || key.starts_with(LAYOUT_BUCKET)
            })
            .map(|(key, value)| (key.clone(), value.clone()))
    }

    /// Buckets checked for a rule, where Aruna may write and delete.
    pub fn checked_buckets(&self) -> impl Iterator<Item = &str> {
        self.public_config
            .keys()
            .filter_map(|key| key.strip_prefix(BUCKET_STATE))
    }

    /// Buckets that hold or held the default rule, so Aruna's own files may live there.
    pub fn layout_buckets(&self) -> Vec<&str> {
        let former = self
            .public_config
            .keys()
            .filter_map(|key| key.strip_prefix(LAYOUT_BUCKET));
        let mut buckets: Vec<&str> = self.default_bucket().into_iter().chain(former).collect();
        buckets.sort_unstable();
        buckets.dedup();
        buckets
    }

    /// Where the parts of an upload wait: the default bucket, else the first checked bucket.
    pub fn parts_bucket(&self) -> Option<&str> {
        self.default_bucket()
            .or_else(|| self.checked_buckets().min())
    }

    /// Folders where Aruna keeps its own objects: the container, or on a storage without a
    /// bucket every default bucket and the probe and parts folders of each checked bucket.
    pub fn reserved_folders(&self) -> Vec<String> {
        if let Some(container) = self.container() {
            return vec![container.to_string()];
        }
        let own = [PROBE_PREFIX, MULTIPART_PART_PREFIX];
        let checked = self
            .checked_buckets()
            .flat_map(|bucket| own.map(|folder| format!("{bucket}/{folder}")));
        self.layout_buckets()
            .into_iter()
            .map(str::to_string)
            .chain(checked)
            .collect()
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct GroupStorageSecret {
    pub backend_id: Ulid,
    pub secret_config: HashMap<String, String>,
    pub updated_at: SystemTime,
}

/// Redacted on purpose: this record reaches effect and error formatting, and
/// its values are live credentials.
impl fmt::Debug for GroupStorageSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GroupStorageSecret")
            .field("backend_id", &self.backend_id)
            .field("keys", &self.secret_config.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl GroupStorageSecret {
    /// The node vault record that holds the secret of `backend_id`.
    pub fn vault_entry(backend_id: Ulid) -> VaultEntry {
        VaultEntry::new(VaultPurpose::GroupBackend, backend_id)
    }

    pub fn to_secret(&self) -> Result<SecretBytes, ConversionError> {
        Ok(SecretBytes::new(self.to_bytes()?))
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
    use super::{GroupBackendKind, GroupStorage, GroupStorageSecret};
    use crate::UserId;
    use crate::structs::identity::realm::RealmId;
    use crate::structs::storage::cleanup::CleanupStrategy;
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::time::SystemTime;
    use ulid::Ulid;

    #[test]
    fn secret_stays_separate() {
        // The public record must never carry credential material.
        let backend_id = Ulid::from_bytes([1u8; 16]);
        let record = GroupStorage {
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
        let secret = GroupStorageSecret {
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
        assert_eq!(GroupStorage::from_bytes(&public_bytes).unwrap(), record);
        assert_eq!(
            GroupStorageSecret::from_bytes(&secret.to_bytes().unwrap()).unwrap(),
            secret
        );
    }

    #[test]
    fn debug_hides_secrets() {
        let secret = GroupStorageSecret {
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

    #[test]
    fn reserves_own_namespace() {
        let record = GroupStorage {
            backend_id: Ulid::from_bytes([1u8; 16]),
            group_id: Ulid::from_bytes([2u8; 16]),
            name: "tenant-az".to_string(),
            kind: GroupBackendKind::Azblob,
            public_config: HashMap::from([("container".to_string(), "data".to_string())]),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            created_by: UserId::default(),
            disabled: false,
            cleanup: CleanupStrategy::Retain,
        };
        assert!(record.is_reserved("data/bucket/key_01"));
        assert!(record.is_reserved("_aruna_backend_probe/01"));
        assert!(!record.is_reserved("database/a.csv"));
        assert!(!record.is_reserved("raw/data/a.csv"));
    }

    #[test]
    fn reserves_default_bucket() {
        let mut record = GroupStorage {
            backend_id: Ulid::from_bytes([1u8; 16]),
            group_id: Ulid::from_bytes([2u8; 16]),
            name: "lab".to_string(),
            kind: GroupBackendKind::S3,
            public_config: HashMap::from([("endpoint".to_string(), "https://s3".to_string())]),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            created_by: UserId::default(),
            disabled: false,
            cleanup: CleanupStrategy::Retain,
        };
        assert!(record.splits());
        assert!(!record.is_reserved("lab-default/a.csv"));
        record
            .public_config
            .insert(super::DEFAULT_BUCKET.to_string(), "lab-default".to_string());
        assert!(record.is_reserved("lab-default/a.csv"));
        assert!(!record.is_reserved("lab-raw/a.csv"));
        // A former default bucket and the parts folder of a checked bucket stay Aruna's.
        record
            .public_config
            .insert("layout:lab-old".to_string(), "true".to_string());
        record
            .public_config
            .insert("bucket:lab-raw".to_string(), "found".to_string());
        assert!(record.is_reserved("lab-old/a.csv"));
        assert!(record.is_reserved("lab-raw/_parts/01/00001_01"));
        assert!(!record.is_reserved("lab-raw/parts/a.csv"));
        assert_eq!(record.parts_bucket(), Some("lab-default"));
        record
            .public_config
            .insert("bucket".to_string(), "fixed".to_string());
        assert!(!record.splits());
    }

    #[test]
    fn checks_bucket_names() {
        for name in ["abc", "aruna-lab-default", "a.b-1", &"a".repeat(63)] {
            assert!(super::valid_bucket(name), "{name}");
        }
        for name in [
            "ab",
            "Lab",
            "-lab",
            "lab-",
            "a..b",
            "a_b",
            "a/b",
            &"a".repeat(64),
        ] {
            assert!(!super::valid_bucket(name), "{name}");
        }
    }
}
