use crate::errors::{BlobError, ConversionError};
use crate::structs::checksum::HASH_BLAKE3;
use crate::structs::{
    PathRestriction, RealmId, SourceMetadata, VersionSourceBinding, VersionState,
};
use crate::types::UserId;
use byteview::ByteView;
use core::fmt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Display;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::{Duration, SystemTime};
use ulid::Ulid;

const ACCESS_KEY_MAX_LEN: usize = 128;

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
    pub fn get_full_path(&self) -> Result<String, ConversionError> {
        PathBuf::from(&self.root)
            .join(&self.storage_bucket)
            .join(&self.backend_path)
            .into_os_string()
            .into_string()
            .map_err(|_| ConversionError::OsStringError)
    }

    pub fn get_storage_path(&self) -> Result<String, BlobError> {
        Ok(PathBuf::from(&self.storage_bucket)
            .join(&self.backend_path)
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
pub struct BucketInfo {
    pub group_id: Ulid,
    pub created_at: SystemTime,
    pub created_by: UserId,
}

impl BucketInfo {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(&self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LookupKey {
    Blake3Hash([u8; 32]),
    Object { bucket: String, key: String },
}

impl LookupKey {
    pub fn from_blake3_hash(hash: &[u8]) -> Result<Self, ConversionError> {
        Ok(Self::Blake3Hash(hash.try_into()?))
    }

    pub fn object(bucket: impl Into<String>, key: impl Into<String>) -> Self {
        Self::Object {
            bucket: bucket.into(),
            key: key.into(),
        }
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
pub enum Location {
    Real(BackendLocation),
    Deleted,
}

impl Location {
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
}

impl CurrentVersionPointer {
    pub fn new(version_id: Ulid) -> Self {
        Self { version_id }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(&self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct VersionMetadata {
    pub version_id: Ulid,
    pub state: VersionState,
    pub created_at: SystemTime,
    pub created_by: UserId,
}

impl VersionMetadata {
    pub fn materialized(
        version_id: Ulid,
        location: BackendLocation,
        created_at: SystemTime,
        created_by: UserId,
        source: Option<VersionSourceBinding>,
    ) -> Self {
        Self {
            version_id,
            state: VersionState::Materialized { location, source },
            created_at,
            created_by,
        }
    }

    pub fn deleted(version_id: Ulid, created_at: SystemTime, created_by: UserId) -> Self {
        Self {
            version_id,
            state: VersionState::Deleted,
            created_at,
            created_by,
        }
    }

    pub fn reference(
        version_id: Ulid,
        source: VersionSourceBinding,
        cached_metadata: SourceMetadata,
        created_at: SystemTime,
        created_by: UserId,
        last_refresh: SystemTime,
    ) -> Self {
        Self {
            version_id,
            state: VersionState::Reference {
                source,
                cached_metadata,
                last_refresh,
            },
            created_at,
            created_by,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(&self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }

    pub fn materialized_location(&self) -> Option<&BackendLocation> {
        self.state.materialized_location()
    }

    pub fn lookup_location(&self) -> Option<Location> {
        self.state.lookup_location()
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

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct UserIdentity {
    pub user_id: UserId,
    pub realm_key: RealmId,
}

impl Display for UserIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.user_id, self.realm_key)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct UserAccess {
    pub access_key: String,
    pub user_identity: UserIdentity,
    pub group_id: Ulid,
    pub secret: String,
    pub expiry: SystemTime,
    pub path_restrictions: Option<Vec<PathRestriction>>,
    pub issued_by: [u8; 32],
    pub revoked_at: Option<SystemTime>,
}

impl UserAccess {
    pub fn build_access_key(
        user_identity: &UserIdentity,
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
    use super::CurrentVersionPointer;
    use ulid::Ulid;

    #[test]
    fn current_version_pointer_roundtrip_preserves_version_id() {
        let pointer = CurrentVersionPointer::new(Ulid::from_bytes([7u8; 16]));

        let restored = CurrentVersionPointer::from_bytes(&pointer.to_bytes().unwrap()).unwrap();

        assert_eq!(pointer, restored);
    }
}
