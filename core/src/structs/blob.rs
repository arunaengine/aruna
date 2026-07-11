use crate::errors::{BlobError, ConversionError};
use crate::structs::checksum::HASH_BLAKE3;
use crate::structs::{PathRestriction, RealmId, SourceMetadata, VersionSourceBinding};
use crate::types::{GroupId, NodeId, UserId};
use byteview::ByteView;
use core::fmt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Display;
use std::path::{Component, Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, SystemTime};
use ulid::Ulid;

const ACCESS_KEY_MAX_LEN: usize = 128;

pub fn ensure_confined_relative_path(path: &Path) -> Result<(), ConversionError> {
    for component in path.components() {
        match component {
            Component::Normal(part) => {
                let part = part.to_str().ok_or(ConversionError::OsStringError)?;
                if part.chars().any(|c| c.is_control()) {
                    return Err(ConversionError::UnsafePath(
                        "path component contains control characters".to_string(),
                    ));
                }
            }
            Component::CurDir => {}
            Component::ParentDir => {
                return Err(ConversionError::UnsafePath(
                    "path must not contain parent-directory (`..`) components".to_string(),
                ));
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(ConversionError::UnsafePath(
                    "path must be relative to the backend root".to_string(),
                ));
            }
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum Backend {
    #[default]
    S3,
    HTTP,
    Postgres,
    FileSystem,
}

impl FromStr for Backend {
    type Err = ConversionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "s3" => Ok(Backend::S3),
            "http" => Ok(Backend::HTTP),
            "postgres" => Ok(Backend::Postgres),
            "filesystem" => Ok(Backend::FileSystem),
            _ => Err(ConversionError::FromStrError(format!(
                "unknown backend {}",
                s
            ))),
        }
    }
}

impl Display for Backend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Backend::S3 => write!(f, "s3"),
            Backend::HTTP => write!(f, "http"),
            Backend::Postgres => write!(f, "postgres"),
            Backend::FileSystem => write!(f, "filesystem"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlobTimeoutConfig {
    pub control_plane_connect_timeout: Duration,
    pub control_plane_io_timeout: Duration,
    pub transfer_idle_timeout: Duration,
}

impl Default for BlobTimeoutConfig {
    fn default() -> Self {
        Self {
            control_plane_connect_timeout: Duration::from_secs(30),
            control_plane_io_timeout: Duration::from_secs(30),
            transfer_idle_timeout: Duration::from_secs(30 * 60),
        }
    }
}

#[derive(Clone, Debug)]
pub struct BackendConfig {
    pub backend_type: Backend,
    pub root: String,
    pub service_config: HashMap<String, String>,
    pub bucket_prefix: Option<String>,
    pub max_bucket_size: Option<u64>,
    pub multipart_bucket: Option<String>,
    pub timeouts: BlobTimeoutConfig,
}

#[derive(Clone, Debug)]
pub struct BackendBucket {
    pub name: String,
    pub load: u64,
}

impl TryFrom<(ByteView, ByteView)> for BackendBucket {
    type Error = ConversionError;

    fn try_from(value: (ByteView, ByteView)) -> Result<Self, Self::Error> {
        let (bucket, load) = value;

        Ok(BackendBucket {
            name: String::from_utf8(bucket.to_vec())?,
            load: u64::from_le_bytes(load.as_ref().try_into()?),
        })
    }
}

impl From<(String, u64)> for BackendBucket {
    fn from((name, size): (String, u64)) -> Self {
        Self { name, load: size }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BackendLocation {
    pub root: String,
    pub storage_bucket: String,
    pub backend_path: String,
    pub ulid: Ulid,
    pub compressed: bool,
    pub encrypted: bool,
    pub created_by: UserId,
    pub created_at: SystemTime,
    pub staging: bool,
    pub partial: bool,
    pub blob_size: u64,
    pub hashes: HashMap<String, Vec<u8>>,
}

impl Display for BackendLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let path = PathBuf::from(&self.root)
            .join(&self.storage_bucket)
            .join(&self.backend_path);
        write!(
            f,
            "{}",
            path.into_os_string()
                .into_string()
                .map_err(|_| fmt::Error)?
        )
    }
}

impl BackendLocation {
    fn confined_relative_path(&self) -> Result<PathBuf, ConversionError> {
        let path = PathBuf::from(&self.storage_bucket).join(&self.backend_path);
        ensure_confined_relative_path(&path)?;
        Ok(path)
    }

    pub fn get_full_path(&self) -> Result<String, ConversionError> {
        PathBuf::from(&self.root)
            .join(self.confined_relative_path()?)
            .into_os_string()
            .into_string()
            .map_err(|_| ConversionError::OsStringError)
    }

    pub fn get_storage_path(&self) -> Result<String, BlobError> {
        Ok(self
            .confined_relative_path()?
            .into_os_string()
            .into_string()
            .map_err(|_| ConversionError::OsStringError)?)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(&self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }

    pub fn get_blake3(&self) -> Option<&[u8]> {
        self.hashes.get(HASH_BLAKE3).map(|h| h.as_slice())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketCorsConfiguration {
    pub rules: Vec<BucketCorsRule>,
}

impl BucketCorsConfiguration {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketCorsRule {
    pub id: Option<String>,
    pub allowed_origins: Vec<String>,
    pub allowed_methods: Vec<String>,
    pub allowed_headers: Vec<String>,
    pub expose_headers: Vec<String>,
    pub max_age_seconds: Option<i32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketInfo {
    pub group_id: Ulid,
    pub created_at: SystemTime,
    pub created_by: UserId,
    pub cors_configuration: Option<BucketCorsConfiguration>,
}

impl BucketInfo {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(&self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BlobHeadKey {
    pub bucket: String,
    pub key: String,
}

impl BlobHeadKey {
    pub fn new(bucket: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            key: key.into(),
        }
    }

    pub fn bucket_prefix(bucket: &str) -> Result<Vec<u8>, ConversionError> {
        Ok(format!("{bucket}/").into_bytes())
    }

    pub fn object_prefix(bucket: &str, key: &str) -> Result<Vec<u8>, ConversionError> {
        Ok(format!("{bucket}/{key}").into_bytes())
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Self::object_prefix(&self.bucket, &self.key)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        let raw = String::from_utf8(bytes.to_vec())?;
        let (bucket, key) = raw.split_once('/').ok_or_else(|| {
            ConversionError::FromStrError("blob head key is missing the bucket separator".into())
        })?;
        Ok(Self::new(bucket, key))
    }
}

pub fn blob_group_permission_path(realm_id: RealmId, group_id: GroupId, node_id: NodeId) -> String {
    format!("/{realm_id}/g/{group_id}/data/{node_id}")
}

pub fn blob_bucket_permission_path(
    realm_id: RealmId,
    group_id: GroupId,
    node_id: NodeId,
    bucket: &str,
) -> String {
    format!(
        "{}/{}",
        blob_group_permission_path(realm_id, group_id, node_id),
        bucket
    )
}

pub fn blob_object_permission_path(
    realm_id: RealmId,
    group_id: GroupId,
    node_id: NodeId,
    bucket: &str,
    key: &str,
) -> String {
    format!(
        "{}/{}",
        blob_bucket_permission_path(realm_id, group_id, node_id, bucket),
        key
    )
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HashPathIndexKey {
    pub blake3_hash: [u8; 32],
    pub version_id: Ulid,
    pub realm_id: RealmId,
    pub group_id: GroupId,
    pub node_id: NodeId,
    pub bucket: String,
    pub key: String,
}

#[derive(Serialize)]
struct HashPathIndexKeyPrefix {
    blake3_hash: [u8; 32],
}

impl HashPathIndexKey {
    pub fn new(
        blake3_hash: [u8; 32],
        version_id: Ulid,
        realm_id: RealmId,
        group_id: GroupId,
        node_id: NodeId,
        bucket: impl Into<String>,
        key: impl Into<String>,
    ) -> Self {
        Self {
            blake3_hash,
            version_id,
            realm_id,
            group_id,
            node_id,
            bucket: bucket.into(),
            key: key.into(),
        }
    }

    pub fn from_blake3_hash(
        hash: &[u8],
        version_id: Ulid,
        realm_id: RealmId,
        group_id: GroupId,
        node_id: NodeId,
        bucket: impl Into<String>,
        key: impl Into<String>,
    ) -> Result<Self, ConversionError> {
        Ok(Self::new(
            hash.try_into()?,
            version_id,
            realm_id,
            group_id,
            node_id,
            bucket,
            key,
        ))
    }

    pub fn hash_prefix(hash: &[u8]) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(&HashPathIndexKeyPrefix {
            blake3_hash: hash.try_into()?,
        })?)
    }

    pub fn permission_path(&self) -> String {
        blob_object_permission_path(
            self.realm_id,
            self.group_id,
            self.node_id,
            &self.bucket,
            &self.key,
        )
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(&self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionKey {
    pub bucket: String,
    pub key: String,
    pub version_id: Ulid,
}

#[derive(Serialize)]
struct VersionKeyPrefix<'a> {
    bucket: &'a str,
    key: &'a str,
}

#[derive(Serialize)]
struct BucketVersionKeyPrefix<'a> {
    bucket: &'a str,
}

impl VersionKey {
    pub fn new(bucket: impl Into<String>, key: impl Into<String>, version_id: Ulid) -> Self {
        Self {
            bucket: bucket.into(),
            key: key.into(),
            version_id,
        }
    }

    pub fn object_prefix(bucket: &str, key: &str) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(&VersionKeyPrefix { bucket, key })?)
    }

    pub fn bucket_prefix(bucket: &str) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(&BucketVersionKeyPrefix { bucket })?)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(&self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CurrentVersionPointer {
    pub version_id: Ulid,
    pub generation: u64,
}

impl CurrentVersionPointer {
    pub fn new(version_id: Ulid) -> Self {
        Self {
            version_id,
            generation: 1,
        }
    }

    pub fn new_with_generation(version_id: Ulid, generation: u64) -> Self {
        Self {
            version_id,
            generation,
        }
    }

    pub fn next_for(existing: Option<&Self>, version_id: Ulid) -> Self {
        Self::new_with_generation(
            version_id,
            existing
                .map(|pointer| pointer.generation.saturating_add(1))
                .unwrap_or(1),
        )
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(&self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BlobVersion {
    pub created_at: SystemTime,
    pub created_by: UserId,
    pub state: BlobVersionState,
}

impl BlobVersion {
    pub fn materialized(
        blob_hash: [u8; 32],
        created_at: SystemTime,
        created_by: UserId,
        source: Option<VersionSourceBinding>,
    ) -> Self {
        Self {
            created_at,
            created_by,
            state: BlobVersionState::Materialized { blob_hash, source },
        }
    }

    pub fn deleted(created_at: SystemTime, created_by: UserId) -> Self {
        Self {
            created_at,
            created_by,
            state: BlobVersionState::Deleted,
        }
    }

    pub fn reference(
        source: VersionSourceBinding,
        cached_metadata: SourceMetadata,
        created_at: SystemTime,
        created_by: UserId,
        last_refresh: SystemTime,
    ) -> Self {
        Self {
            created_at,
            created_by,
            state: BlobVersionState::Reference {
                source,
                cached_metadata,
                last_refresh,
            },
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(&self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }

    pub fn blob_hash(&self) -> Option<&[u8; 32]> {
        self.state.blob_hash()
    }

    pub fn source_binding(&self) -> Option<&VersionSourceBinding> {
        self.state.source_binding()
    }

    pub fn is_deleted(&self) -> bool {
        self.state.is_deleted()
    }

    pub fn is_materialized(&self) -> bool {
        self.state.is_materialized()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BlobVersionState {
    Materialized {
        blob_hash: [u8; 32],
        source: Option<VersionSourceBinding>,
    },
    Reference {
        source: VersionSourceBinding,
        cached_metadata: SourceMetadata,
        last_refresh: SystemTime,
    },
    Deleted,
}

impl BlobVersionState {
    pub fn blob_hash(&self) -> Option<&[u8; 32]> {
        match self {
            Self::Materialized { blob_hash, .. } => Some(blob_hash),
            Self::Reference { .. } | Self::Deleted => None,
        }
    }

    pub fn source_binding(&self) -> Option<&VersionSourceBinding> {
        match self {
            Self::Materialized { source, .. } => source.as_ref(),
            Self::Reference { source, .. } => Some(source),
            Self::Deleted => None,
        }
    }

    pub fn is_deleted(&self) -> bool {
        matches!(self, Self::Deleted)
    }

    pub fn is_materialized(&self) -> bool {
        matches!(self, Self::Materialized { .. })
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct UserAccess {
    pub access_key: String,
    pub user_identity: UserId,
    pub group_id: Ulid,
    pub secret: String,
    pub expiry: SystemTime,
    pub path_restrictions: Option<Vec<PathRestriction>>,
    pub issued_by: [u8; 32],
    pub revoked_at: Option<SystemTime>,
}

impl UserAccess {
    pub fn build_access_key(
        user_identity: &UserId,
        key_id: &str,
    ) -> Result<String, ConversionError> {
        let access_key = format!("{user_identity}:{key_id}");
        if access_key.len() > ACCESS_KEY_MAX_LEN {
            return Err(ConversionError::InvalidLength(format!(
                "access key must be <= {ACCESS_KEY_MAX_LEN} characters"
            )));
        }
        Ok(access_key)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(&self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }

    pub fn is_expired(&self, now: SystemTime) -> bool {
        self.expiry <= now
    }

    pub fn is_revoked(&self) -> bool {
        self.revoked_at.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BlobHeadKey, BlobVersion, BucketCorsConfiguration, BucketCorsRule, CurrentVersionPointer,
        HashPathIndexKey, blob_bucket_permission_path, blob_group_permission_path,
        blob_object_permission_path,
    };
    use crate::NodeId;
    use crate::structs::{
        PortableSourceDescriptor, RealmId, SourceConnectorKind, SourceMetadata, StagingStrategy,
        VersionSourceBinding,
    };
    use crate::types::UserId;
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::time::SystemTime;
    use ulid::Ulid;

    #[test]
    fn current_version_pointer_roundtrip_preserves_fields() {
        let pointer = CurrentVersionPointer::new_with_generation(Ulid::from_bytes([7u8; 16]), 42);

        let restored = CurrentVersionPointer::from_bytes(&pointer.to_bytes().unwrap()).unwrap();

        assert_eq!(pointer, restored);
    }

    #[test]
    fn blob_head_key_roundtrip_preserves_fields_and_bucket_prefix() {
        let key = BlobHeadKey::new("bucket", "nested/path.txt");

        let restored = BlobHeadKey::from_bytes(&key.to_bytes().unwrap()).unwrap();
        let prefix = BlobHeadKey::bucket_prefix("bucket").unwrap();

        assert_eq!(key, restored);
        assert!(key.to_bytes().unwrap().starts_with(&prefix));
    }

    #[test]
    fn blob_head_key_object_prefix_roundtrip() {
        let prefix = BlobHeadKey::object_prefix("bucket", "rare/").unwrap();
        let key = BlobHeadKey::new("bucket", "rare/").to_bytes().unwrap();
        assert_eq!(prefix, key);
    }

    #[test]
    fn blob_head_key_object_prefix_rejects_wrong_bucket() {
        let key = BlobHeadKey::new("bucket_b", "docs/file.txt")
            .to_bytes()
            .unwrap();
        let prefix = BlobHeadKey::object_prefix("bucket_a", "docs/").unwrap();
        assert!(!key.starts_with(&prefix));
    }

    #[test]
    fn blob_head_key_byte_order_matches_lexicographic_key_order() {
        let short = BlobHeadKey::new("bucket", "b").to_bytes().unwrap();
        let long = BlobHeadKey::new("bucket", "aa").to_bytes().unwrap();
        assert!(long < short);
    }

    #[test]
    fn blob_head_key_prefix_range_is_contiguous() {
        let prefix = BlobHeadKey::object_prefix("bucket", "rare/").unwrap();
        let inside = BlobHeadKey::new("bucket", "rare/1").to_bytes().unwrap();
        let outside = BlobHeadKey::new("bucket", "rare0").to_bytes().unwrap();
        assert!(inside.starts_with(&prefix));
        assert!(!outside.starts_with(&prefix));
        assert!(outside > inside);
    }

    #[test]
    fn hash_path_index_key_roundtrip_preserves_fields_and_hash_prefix() {
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let group_id = Ulid::from_bytes([3u8; 16]);
        let node_id =
            NodeId::from_str("ae58ff8833241ac82d6ff7611046ed67b5072d142c588d0063e942d9a75502b6")
                .unwrap();
        let key = HashPathIndexKey::new(
            [7u8; 32],
            Ulid::from_bytes([8u8; 16]),
            realm_id,
            group_id,
            node_id,
            "bucket",
            "nested/path.txt",
        );

        let restored = HashPathIndexKey::from_bytes(&key.to_bytes().unwrap()).unwrap();
        let prefix = HashPathIndexKey::hash_prefix(&[7u8; 32]).unwrap();

        assert_eq!(key, restored);
        assert_eq!(restored.version_id, Ulid::from_bytes([8u8; 16]));
        assert!(key.to_bytes().unwrap().starts_with(&prefix));
        assert_eq!(
            key.permission_path(),
            blob_object_permission_path(realm_id, group_id, node_id, "bucket", "nested/path.txt")
        );
    }

    #[test]
    fn blob_permission_path_builders_use_canonical_format() {
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let group_id = Ulid::from_bytes([3u8; 16]);
        let node_id =
            NodeId::from_str("ae58ff8833241ac82d6ff7611046ed67b5072d142c588d0063e942d9a75502b6")
                .unwrap();

        assert_eq!(
            blob_group_permission_path(realm_id, group_id, node_id),
            format!("/{realm_id}/g/{group_id}/data/{node_id}")
        );
        assert_eq!(
            blob_bucket_permission_path(realm_id, group_id, node_id, "bucket"),
            format!("/{realm_id}/g/{group_id}/data/{node_id}/bucket")
        );
        assert_eq!(
            blob_object_permission_path(realm_id, group_id, node_id, "bucket", "nested/path.txt"),
            format!("/{realm_id}/g/{group_id}/data/{node_id}/bucket/nested/path.txt")
        );
    }

    #[test]
    fn blob_version_roundtrip_preserves_all_states() {
        let created_at = SystemTime::UNIX_EPOCH;
        let created_by = UserId::default();
        let binding = VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::S3,
                public_config: HashMap::from([(
                    "endpoint".to_string(),
                    "https://s3.example.com".to_string(),
                )]),
                source_path: "dataset/run-1/file.txt".to_string(),
                version_selector: Some("v1".to_string()),
                capabilities: vec!["versioned".to_string()],
                origin_node_id: None,
            },
            connector_id: Some(Ulid::from_bytes([9u8; 16])),
        };
        let reference_metadata = SourceMetadata {
            content_length: 42,
            content_type: Some("text/plain".to_string()),
            etag: Some("etag".to_string()),
            last_modified: Some(SystemTime::UNIX_EPOCH),
            source_version: None,
        };

        let versions = vec![
            BlobVersion::materialized([1u8; 32], created_at, created_by, Some(binding.clone())),
            BlobVersion::reference(
                binding.clone(),
                reference_metadata,
                created_at,
                created_by,
                SystemTime::UNIX_EPOCH,
            ),
            BlobVersion::deleted(created_at, created_by),
        ];

        for version in versions {
            let restored = BlobVersion::from_bytes(&version.to_bytes().unwrap()).unwrap();
            assert_eq!(version, restored);
        }

        let materialized = BlobVersion::materialized([1u8; 32], created_at, created_by, None);
        assert_eq!(materialized.blob_hash(), Some(&[1u8; 32]));
        assert!(materialized.is_materialized());
        assert!(!materialized.is_deleted());

        let deleted = BlobVersion::deleted(created_at, created_by);
        assert!(deleted.blob_hash().is_none());
        assert!(!deleted.is_materialized());
        assert!(deleted.is_deleted());
    }

    #[test]
    fn ensure_confined_relative_path_matrix() {
        use super::ensure_confined_relative_path;
        use crate::errors::ConversionError;
        use std::path::Path;

        for ok in [
            "bucket/object",
            "bucket/nested/object.bin",
            "bucket/./object",
        ] {
            assert!(ensure_confined_relative_path(Path::new(ok)).is_ok());
        }
        for bad in [
            "../escape",
            "bucket/../../escape",
            "/absolute/path",
            "bucket/../../../etc/passwd",
        ] {
            assert!(matches!(
                ensure_confined_relative_path(Path::new(bad)),
                Err(ConversionError::UnsafePath(_))
            ));
        }
    }

    #[test]
    fn get_storage_path_rejects_traversal_in_backend_path() {
        use crate::errors::{BlobError, ConversionError};
        use crate::structs::BackendLocation;

        let mut location = BackendLocation {
            root: "/data".to_string(),
            storage_bucket: "bucket".to_string(),
            backend_path: "object.bin".to_string(),
            ulid: Ulid::new(),
            compressed: false,
            encrypted: false,
            created_by: UserId::default(),
            created_at: SystemTime::now(),
            staging: false,
            partial: false,
            blob_size: 0,
            hashes: HashMap::new(),
        };
        assert!(location.get_storage_path().is_ok());
        assert!(location.get_full_path().is_ok());

        location.backend_path = "../../etc/passwd".to_string();
        assert!(matches!(
            location.get_storage_path(),
            Err(BlobError::ConversionError(ConversionError::UnsafePath(_)))
        ));
        assert!(matches!(
            location.get_full_path(),
            Err(ConversionError::UnsafePath(_))
        ));
    }

    #[test]
    fn bucket_cors_configuration_roundtrip_preserves_rules() {
        let config = BucketCorsConfiguration {
            rules: vec![BucketCorsRule {
                id: Some("rule-1".to_string()),
                allowed_origins: vec!["https://example.org".to_string()],
                allowed_methods: vec!["GET".to_string(), "PUT".to_string()],
                allowed_headers: vec!["authorization".to_string()],
                expose_headers: vec!["etag".to_string()],
                max_age_seconds: Some(600),
            }],
        };

        let restored = BucketCorsConfiguration::from_bytes(&config.to_bytes().unwrap()).unwrap();

        assert_eq!(config, restored);
    }
}
