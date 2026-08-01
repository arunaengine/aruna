use crate::credential_seal::{CredentialSealKey, SealError, SealedS3Secret, credential_aad};
use crate::errors::{BlobError, ConversionError};
use crate::structs::checksum::HASH_BLAKE3;
use crate::structs::{
    BucketReplicationConfig, GroupBackendKind, PathRestriction, RealmId, SourceMetadata,
    StorageRoutingRule, VersionSourceBinding,
};
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
pub const HIDDEN_BLOB_PREFIX: &str = "_jobs";
/// Reserved container prefix holding in-flight multipart parts, so parts never
/// share a namespace with tenant-written keys.
pub const MULTIPART_PART_PREFIX: &str = "_parts";
pub const OBJECT_CONTENT_TYPE_KEY: &str = "aruna.internal.content-type";

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
    FileSystem,
    /// A tenant-registered backend. Never nameable in the backends file: it is
    /// synthesized from a `GroupStorageBackend` record.
    Group(GroupBackendKind),
}

impl FromStr for Backend {
    type Err = ConversionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "s3" => Ok(Backend::S3),
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
            Backend::FileSystem => write!(f, "filesystem"),
            Backend::Group(kind) => write!(f, "group:{kind}"),
        }
    }
}

/// Names the storage backend a stored record lives on. Node backends are named
/// by the operator's backends file; group backends by their record id.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum BackendRef {
    Node(String),
    Group(Ulid),
}

impl BackendRef {
    pub const DEFAULT_NODE_NAME: &str = "default";

    pub fn node_default() -> Self {
        Self::Node(Self::DEFAULT_NODE_NAME.to_string())
    }

    /// Stable byte encoding used to qualify keyspace entries per backend.
    pub fn key_bytes(&self) -> Vec<u8> {
        match self {
            Self::Node(name) => format!("n:{name}").into_bytes(),
            Self::Group(id) => format!("g:{id}").into_bytes(),
        }
    }

    pub fn from_key_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        let text = std::str::from_utf8(bytes)
            .map_err(|error| ConversionError::FromStrError(error.to_string()))?;
        match text.split_at_checked(2) {
            Some(("n:", name)) => Ok(Self::Node(name.to_string())),
            Some(("g:", id)) => Ok(Self::Group(id.parse()?)),
            _ => Err(ConversionError::FromStrError(format!(
                "unknown backend key `{text}`"
            ))),
        }
    }
}

impl Display for BackendRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Node(name) => write!(f, "node:{name}"),
            Self::Group(id) => write!(f, "group:{id}"),
        }
    }
}

/// Outcome of routing resolution: the chosen backend plus the storage class it
/// carried at that moment. Both are stamped on the record, never re-derived.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResolvedBackend {
    pub backend: BackendRef,
    pub storage_class: Option<String>,
}

impl ResolvedBackend {
    pub fn new(backend: BackendRef, storage_class: Option<String>) -> Self {
        Self {
            backend,
            storage_class,
        }
    }

    pub fn node_default() -> Self {
        Self::new(BackendRef::node_default(), None)
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

/// Deferred blob housekeeping written atomically with a completed upload and
/// drained outside the request path.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BlobCleanupWork {
    DeleteBlob {
        location: BackendLocation,
    },
    RegisterDht {
        blake3: [u8; 32],
        realm_id: RealmId,
        ttl_ms: u64,
    },
    /// Bytes written for a transaction whose commit outcome was never learned.
    /// They are deleted only once `owner` proves the commit never landed, so a
    /// commit that did land keeps the copy it already owns.
    ReconcileWrite {
        location: BackendLocation,
        owner: WriteOwner,
    },
    /// A bucket slot reserved before the external write finished. The marker
    /// is cleared only after the physical copy is proven owned or released.
    ReconcileReservation {
        location: BackendLocation,
    },
}

/// The record a write's commit would have made the owner of its bytes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum WriteOwner {
    Blob {
        blake3: [u8; 32],
        realm_id: RealmId,
        ttl_ms: u64,
    },
    UploadPart {
        upload_id: Ulid,
        part_number: u16,
    },
}

impl BlobCleanupWork {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(&self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }

    /// The physical object this row is about, for the variants that name one.
    pub fn location(&self) -> Option<&BackendLocation> {
        match self {
            Self::DeleteBlob { location }
            | Self::ReconcileWrite { location, .. }
            | Self::ReconcileReservation { location } => Some(location),
            Self::RegisterDht { .. } => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BackendLocation {
    pub backend: BackendRef,
    /// Storage class resolved at write time. Stamped so no later recount ever
    /// re-derives it from the node's current backends file.
    pub storage_class: Option<String>,
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

    /// Whether both name the same physical object. The path carries a per-write
    /// id, so this distinguishes two copies of identical content.
    pub fn same_object(&self, other: &Self) -> bool {
        self.backend == other.backend
            && self.root == other.root
            && self.storage_bucket == other.storage_bucket
            && self.backend_path == other.backend_path
    }
}

/// Names one physical copy: the content hash followed by the backend holding
/// it. Deduplication therefore stays inside the backend a write routed to, and
/// every backend keeps at most one copy of a hash.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlobLocationKey {
    pub blake3_hash: [u8; 32],
    pub backend: BackendRef,
}

impl BlobLocationKey {
    pub fn new(blake3_hash: [u8; 32], backend: BackendRef) -> Self {
        Self {
            blake3_hash,
            backend,
        }
    }

    pub fn from_blake3(hash: &[u8], backend: BackendRef) -> Result<Self, ConversionError> {
        Ok(Self::new(hash.try_into()?, backend))
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut key = self.blake3_hash.to_vec();
        key.extend_from_slice(&self.backend.key_bytes());
        key
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        let (hash, backend) = bytes.split_at_checked(32).ok_or_else(|| {
            ConversionError::InvalidLength("blob location key is too short".to_string())
        })?;
        Ok(Self::new(
            hash.try_into()?,
            BackendRef::from_key_bytes(backend)?,
        ))
    }
}

/// Durable evidence that a stored copy failed hash/bao verification (§8.2). Keyed
/// by (hash, backend) so re-detecting the same corrupt copy overwrites its row.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BlobQuarantineRecord {
    pub blake3: [u8; 32],
    pub backend: BackendRef,
    pub reason: String,
    pub detected_at_ms: u64,
}

impl BlobQuarantineRecord {
    pub fn new(blake3: [u8; 32], backend: BackendRef, reason: String, detected_at_ms: u64) -> Self {
        Self {
            blake3,
            backend,
            reason,
            detected_at_ms,
        }
    }

    pub fn key(&self) -> Vec<u8> {
        BlobLocationKey::new(self.blake3, self.backend.clone()).to_bytes()
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct HiddenBlobKey {
    pub backend: BackendRef,
    pub root: String,
    pub storage_bucket: String,
    pub backend_path: String,
}

impl HiddenBlobKey {
    pub fn new(
        backend: BackendRef,
        root: String,
        storage_bucket: String,
        backend_path: String,
    ) -> Result<Self, ConversionError> {
        let key = Self {
            backend,
            root,
            storage_bucket,
            backend_path,
        };
        key.namespace()?;
        Ok(key)
    }

    pub fn namespace(&self) -> Result<Ulid, ConversionError> {
        let path = Path::new(&self.backend_path);
        ensure_confined_relative_path(path)?;
        let mut components = path.components().filter_map(|component| match component {
            Component::Normal(part) => part.to_str(),
            _ => None,
        });
        if components.next() != Some(HIDDEN_BLOB_PREFIX) {
            return Err(ConversionError::UnsafePath(
                "path is outside the hidden blob namespace".to_string(),
            ));
        }
        let namespace = components.next().ok_or_else(|| {
            ConversionError::UnsafePath("hidden blob namespace is missing an id".to_string())
        })?;
        if components.next().is_none() {
            return Err(ConversionError::UnsafePath(
                "hidden blob namespace is missing a blob path".to_string(),
            ));
        }
        namespace.parse().map_err(|_| {
            ConversionError::UnsafePath("hidden blob namespace id is invalid".to_string())
        })
    }

    pub fn get_storage_path(&self) -> Result<String, ConversionError> {
        let path = PathBuf::from(&self.storage_bucket).join(&self.backend_path);
        ensure_confined_relative_path(&path)?;
        self.namespace()?;
        path.into_os_string()
            .into_string()
            .map_err(|_| ConversionError::OsStringError)
    }
}

impl TryFrom<&BackendLocation> for HiddenBlobKey {
    type Error = ConversionError;

    fn try_from(location: &BackendLocation) -> Result<Self, Self::Error> {
        Self::new(
            location.backend.clone(),
            location.root.clone(),
            location.storage_bucket.clone(),
            location.backend_path.clone(),
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HiddenBlobEntry {
    pub key: HiddenBlobKey,
    pub modified_at: Option<SystemTime>,
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
    pub replication: Option<BucketReplicationConfig>,
    /// Bucket, prefix and exact-key write routing rules, most specific first at
    /// resolution time. Empty means the group default decides.
    pub storage_routing: Vec<StorageRoutingRule>,
}

impl BucketInfo {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(&self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }

    /// What makes this the same bucket record: what a write authorized against
    /// must not have changed. The mutable configuration is excluded on purpose,
    /// so an admin edit cannot abort a long-running write.
    pub fn identity(&self) -> (Ulid, SystemTime, UserId) {
        (self.group_id, self.created_at, self.created_by)
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
    pub metadata: HashMap<String, String>,
    /// The authenticated node that published this version. Set for replicated
    /// versions so the record is accountable to the asserting node rather than
    /// only to the manifest's self-asserted `created_by`.
    #[serde(default)]
    pub published_by: Option<NodeId>,
}

impl BlobVersion {
    pub fn materialized(
        blob_hash: [u8; 32],
        backend: BackendRef,
        created_at: SystemTime,
        created_by: UserId,
        source: Option<VersionSourceBinding>,
    ) -> Self {
        Self {
            created_at,
            created_by,
            state: BlobVersionState::Materialized {
                blob_hash,
                backend,
                source,
            },
            metadata: HashMap::new(),
            published_by: None,
        }
    }

    pub fn deleted(created_at: SystemTime, created_by: UserId) -> Self {
        Self {
            created_at,
            created_by,
            state: BlobVersionState::Deleted,
            metadata: HashMap::new(),
            published_by: None,
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
            metadata: HashMap::new(),
            published_by: None,
        }
    }

    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn with_publisher(mut self, node_id: NodeId) -> Self {
        self.published_by = Some(node_id);
        self
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

    pub fn blob_backend(&self) -> Option<&BackendRef> {
        self.state.blob_backend()
    }

    pub fn location_key(&self) -> Option<BlobLocationKey> {
        self.state.location_key()
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
        /// Backend the write routed to. Stamped here so a read never has to
        /// re-derive routing or guess which physical copy the object owns.
        backend: BackendRef,
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

    pub fn blob_backend(&self) -> Option<&BackendRef> {
        match self {
            Self::Materialized { backend, .. } => Some(backend),
            Self::Reference { .. } | Self::Deleted => None,
        }
    }

    pub fn location_key(&self) -> Option<BlobLocationKey> {
        match self {
            Self::Materialized {
                blob_hash, backend, ..
            } => Some(BlobLocationKey::new(*blob_hash, backend.clone())),
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
    /// Authenticated ciphertext of the S3 secret, sealed with an issuer-local
    /// key and bound to this record's identity. The plaintext never rests here.
    pub secret: SealedS3Secret,
    pub expiry: SystemTime,
    pub path_restrictions: Option<Vec<PathRestriction>>,
    pub issued_by: [u8; 32],
    pub revoked_at: Option<SystemTime>,
}

impl UserAccess {
    /// Access keys are the key id itself, kept strictly alphanumeric so every
    /// S3 client and the CSI mount driver accept them verbatim.
    pub fn build_access_key(key_id: &str) -> Result<String, ConversionError> {
        if key_id.is_empty() || key_id.len() > ACCESS_KEY_MAX_LEN {
            return Err(ConversionError::InvalidLength(format!(
                "access key must be 1..={ACCESS_KEY_MAX_LEN} characters"
            )));
        }
        if !key_id.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
            return Err(ConversionError::FromStrError(
                "access key must be alphanumeric".to_string(),
            ));
        }
        Ok(key_id.to_string())
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

    /// AAD binding the sealed secret to this record's identity fields.
    pub fn credential_aad(&self) -> Vec<u8> {
        credential_aad(
            &self.access_key,
            self.user_identity,
            self.group_id,
            self.issued_by,
            self.expiry,
        )
    }

    /// Seals `plaintext` into `self.secret`, bound to this record's identity.
    pub fn seal_secret(
        &mut self,
        key: &CredentialSealKey,
        plaintext: &str,
    ) -> Result<(), SealError> {
        self.secret = SealedS3Secret::seal(key, plaintext, &self.credential_aad())?;
        Ok(())
    }

    /// Recovers the plaintext secret; only the issuing node's key can succeed.
    pub fn open_secret(&self, key: &CredentialSealKey) -> Result<String, SealError> {
        self.secret.open(key, &self.credential_aad())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Backend, BackendRef, BlobHeadKey, BlobLocationKey, BlobVersion, BucketCorsConfiguration,
        BucketCorsRule, CurrentVersionPointer, HashPathIndexKey, HiddenBlobKey,
        blob_bucket_permission_path, blob_group_permission_path, blob_object_permission_path,
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
    fn parses_backend_names() {
        // The backends file accepts these two spellings and nothing else.
        assert_eq!(Backend::from_str("s3").unwrap(), Backend::S3);
        assert_eq!(
            Backend::from_str("filesystem").unwrap(),
            Backend::FileSystem
        );
        assert_eq!(Backend::S3.to_string(), "s3");
        assert_eq!(Backend::FileSystem.to_string(), "filesystem");
        Backend::from_str("http").unwrap_err();
        Backend::from_str("postgres").unwrap_err();
    }

    // The S3 auth layer rejects revoked/expired credentials via these predicates.
    #[test]
    fn access_status_predicates() {
        use super::UserAccess;
        use std::time::Duration;

        let now = SystemTime::now();
        let base = UserAccess {
            access_key: "access".into(),
            user_identity: UserId::local(Ulid::generate(), RealmId::from_bytes([1u8; 32])),
            group_id: Ulid::generate(),
            secret: crate::credential_seal::SealedS3Secret::empty(),
            expiry: now + Duration::from_secs(60),
            path_restrictions: None,
            issued_by: [0u8; 32],
            revoked_at: None,
        };
        assert!(!base.is_expired(now));
        assert!(!base.is_revoked());

        let mut expired = base.clone();
        expired.expiry = now - Duration::from_secs(1);
        assert!(expired.is_expired(now));

        let mut revoked = base.clone();
        revoked.revoked_at = Some(now);
        assert!(revoked.is_revoked());
    }

    // Sealed credentials open only on the issuing node and only while unmoved.
    #[test]
    fn sealed_binds_record() {
        use super::UserAccess;
        use crate::credential_seal::{CredentialSealKey, SealedS3Secret};
        use std::time::Duration;

        let issuer = *iroh::SecretKey::from_bytes(&[9u8; 32]).public().as_bytes();
        let key = CredentialSealKey::derive(&[9u8; 32]);
        let mut access = UserAccess {
            access_key: "AKIA".into(),
            user_identity: UserId::local(Ulid::generate(), RealmId::from_bytes([1u8; 32])),
            group_id: Ulid::generate(),
            secret: SealedS3Secret::empty(),
            expiry: SystemTime::now() + Duration::from_secs(60),
            path_restrictions: None,
            issued_by: issuer,
            revoked_at: None,
        };
        access.seal_secret(&key, "the-secret").unwrap();
        assert_eq!(access.open_secret(&key).unwrap(), "the-secret");

        let other = CredentialSealKey::derive(&[1u8; 32]);
        assert!(access.open_secret(&other).is_err());

        let mut moved = access.clone();
        moved.group_id = Ulid::generate();
        assert!(moved.open_secret(&key).is_err());
    }

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
    fn key_separates_backends() {
        // One hash on two backends must produce two distinct, decodable keys.
        let node = BlobLocationKey::new([7u8; 32], BackendRef::node_default());
        let group = BlobLocationKey::new([7u8; 32], BackendRef::Group(Ulid::from_bytes([4u8; 16])));

        assert_ne!(node.to_bytes(), group.to_bytes());
        assert_eq!(BlobLocationKey::from_bytes(&node.to_bytes()).unwrap(), node);
        assert_eq!(
            BlobLocationKey::from_bytes(&group.to_bytes()).unwrap(),
            group
        );
        assert!(node.to_bytes().starts_with(&[7u8; 32]));
        assert!(group.to_bytes().starts_with(&[7u8; 32]));
    }

    #[test]
    fn key_rejects_garbage() {
        // A short or unknown-backend location key must never decode.
        assert!(BlobLocationKey::from_bytes(&[1u8; 20]).is_err());
        let mut unknown = [2u8; 32].to_vec();
        unknown.extend_from_slice(b"x:name");
        assert!(BlobLocationKey::from_bytes(&unknown).is_err());
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
            BlobVersion::materialized(
                [1u8; 32],
                BackendRef::node_default(),
                created_at,
                created_by,
                Some(binding.clone()),
            )
            .with_metadata(HashMap::from([(
                "mtime".to_string(),
                "1753272000.123456789".to_string(),
            )])),
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

        let materialized = BlobVersion::materialized(
            [1u8; 32],
            BackendRef::node_default(),
            created_at,
            created_by,
            None,
        );
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
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/data".to_string(),
            storage_bucket: "bucket".to_string(),
            backend_path: "object.bin".to_string(),
            ulid: Ulid::generate(),
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
    fn location_keeps_stamp() {
        use crate::structs::BackendLocation;

        let location = BackendLocation {
            backend: BackendRef::Group(Ulid::from_bytes([6u8; 16])),
            storage_class: Some("cold".to_string()),
            root: "/data".to_string(),
            storage_bucket: "bucket".to_string(),
            backend_path: "object.bin".to_string(),
            ulid: Ulid::from_bytes([2u8; 16]),
            compressed: false,
            encrypted: false,
            created_by: UserId::default(),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 7,
            hashes: HashMap::new(),
        };

        let restored = BackendLocation::from_bytes(&location.to_bytes().unwrap()).unwrap();

        assert_eq!(location, restored);
    }

    #[test]
    fn hidden_key_validates() {
        let namespace = Ulid::from_bytes([4u8; 16]);
        let key = HiddenBlobKey::new(
            BackendRef::node_default(),
            "/data".to_string(),
            "storage".to_string(),
            format!("_jobs/{namespace}/input_01"),
        )
        .unwrap();

        assert_eq!(key.namespace().unwrap(), namespace);
        assert_eq!(
            key.get_storage_path().unwrap(),
            format!("storage/_jobs/{namespace}/input_01")
        );
    }

    #[test]
    fn hidden_key_rejects() {
        for path in [
            "bucket/object",
            "_jobs/not-a-ulid/input",
            "_jobs/01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "_jobs/01ARZ3NDEKTSV4RRFFQ69G5FAV/../escape",
        ] {
            assert!(
                HiddenBlobKey::new(
                    BackendRef::node_default(),
                    "/data".to_string(),
                    "storage".to_string(),
                    path.to_string(),
                )
                .is_err()
            );
        }
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
