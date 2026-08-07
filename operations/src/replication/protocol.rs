use aruna_blob::hash::Hasher;
use aruna_core::errors::ConversionError;
use aruna_core::id::NodeId;
use aruna_core::structs::checksum::ChecksumAlgorithm;
use aruna_core::structs::{
    ArunaArn, AuthContext, BackendLocation, MultipartChecksumType, MultipartObjectPart,
    MultipartObjectSummary, RealmId, ReplicationItemKind, ReplicationNegotiationResult,
    SourceMetadata, VersionSourceBinding, VersionedObjectArn,
};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
use ulid::Ulid;

const VERSION_REPLICATION_MAGIC: &[u8; 4] = b"vrp1";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VersionReplicationManifest {
    pub bucket: String,
    pub key: String,
    pub version_id: Ulid,
    pub group_id: aruna_core::types::GroupId,
    pub kind: ReplicationItemKind,
    pub created_at: std::time::SystemTime,
    pub created_by: aruna_core::types::UserId,
    pub current_version: bool,
    pub current_version_generation: Option<u64>,
    pub auth_context: AuthContext,
    pub blob: Option<MaterializedBlobInfo>,
    pub source: Option<VersionSourceBinding>,
    pub multipart: Option<MultipartObjectReplicationMetadata>,
    pub reference_intent: bool,
    pub origin: Option<SyncOrigin>,
    pub upstream_sources: Vec<ArunaArn>,
    pub writer_auth_context: Option<AuthContext>,
    pub reference_metadata: Option<SourceMetadata>,
    pub metadata: HashMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncOrigin {
    pub relationship_id: Ulid,
    pub hop_count: u8,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MaterializedBlobInfo {
    pub hash: [u8; 32],
    pub size: u64,
    pub compressed: bool,
    pub encrypted: bool,
    pub location: BackendLocation,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BaoReadTarget {
    ExactVersion(VersionedObjectArn),
    Blake3([u8; 32]),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BaoReadRequest {
    pub auth_context: AuthContext,
    pub realm_id: aruna_core::structs::RealmId,
    pub target: BaoReadTarget,
    pub expected_blake3: Option<[u8; 32]>,
    pub metadata_only: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BaoReadRefusal {
    RealmPeerDenied,
    InvalidTarget,
    NotFound,
    ReadDenied,
    HashMismatch,
    BackendFailure,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MultipartObjectReplicationMetadata {
    pub summary: MultipartObjectSummary,
    pub parts: Vec<MultipartObjectPart>,
    pub checksum_type: MultipartChecksumType,
}

impl<'de> Deserialize<'de> for MultipartObjectReplicationMetadata {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct WireMetadata {
            summary: MultipartObjectSummary,
            parts: Vec<MultipartObjectPart>,
            checksum_type: MultipartChecksumType,
        }

        let mut metadata = WireMetadata::deserialize(deserializer)?;
        metadata.summary.composite_hashes = hashes_from_parts(&metadata.parts);
        Ok(Self {
            summary: metadata.summary,
            parts: metadata.parts,
            checksum_type: metadata.checksum_type,
        })
    }
}

fn hashes_from_parts(parts: &[MultipartObjectPart]) -> HashMap<String, Vec<u8>> {
    let mut composite_hashes = HashMap::new();
    for algorithm in [
        ChecksumAlgorithm::Md5,
        ChecksumAlgorithm::Sha1,
        ChecksumAlgorithm::Sha256,
        ChecksumAlgorithm::Crc32,
        ChecksumAlgorithm::Crc32c,
        ChecksumAlgorithm::Crc64Nvme,
    ] {
        let mut combined = Vec::new();
        for part in parts {
            let Some(digest) = part.hashes.get(algorithm.hash_key()) else {
                combined.clear();
                break;
            };
            combined.extend_from_slice(digest);
        }
        if combined.is_empty() {
            continue;
        }

        let hashes = Hasher::new_with_bytes(&combined).finalize();
        let digest = match algorithm {
            ChecksumAlgorithm::Md5 => hashes.md5.to_vec(),
            ChecksumAlgorithm::Sha1 => hashes.sha1.to_vec(),
            ChecksumAlgorithm::Sha256 => hashes.sha256.to_vec(),
            ChecksumAlgorithm::Crc32 => hashes.crc32.to_vec(),
            ChecksumAlgorithm::Crc32c => hashes.crc32c.to_vec(),
            ChecksumAlgorithm::Crc64Nvme => hashes.crc64nvme.to_vec(),
        };
        composite_hashes.insert(algorithm.hash_key().to_string(), digest);
    }
    composite_hashes
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum VersionReplicationMessage {
    VersionManifest(VersionReplicationManifest),
    VersionNegotiationResponse(ReplicationNegotiationResult),
    VersionApplyComplete,
    VersionApplyRejected(String),
    BaoReadRequest(BaoReadRequest),
    BaoReadAccepted {
        size: u64,
        blake3: [u8; 32],
    },
    BaoReadRefused(BaoReadRefusal),
    LocationSummaryRequest(LocationSummaryRequest),
    LocationSummaryResponse(LocationSummary),
    /// The asking user has no read access to the bucket this node holds the
    /// copy under, so the caller learns nothing except that it was refused.
    LocationSummaryDenied,
}

/// Read-only question a node asks a peer: do you hold this version, and on
/// what storage? Never mutates the peer.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LocationSummaryRequest {
    pub realm_id: RealmId,
    pub bucket: String,
    pub key: String,
    pub version_id: Option<Ulid>,
    pub auth_context: AuthContext,
}

/// What a copy sits on. Node-managed copies carry the storage class only: the
/// operator's backend names stay out of object-scoped answers.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum LocationCopyStorage {
    NodeManaged {
        storage_class: Option<String>,
    },
    GroupBackend {
        backend_id: Ulid,
        name: Option<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LocationSummary {
    /// The version the answering node resolved; `None` means it knows none.
    pub version_id: Option<Ulid>,
    pub held: bool,
    pub storage: Option<LocationCopyStorage>,
    /// Whether the resolved version carries stored bytes at all. A delete
    /// marker or a reference-only version never will, anywhere.
    pub materialized: bool,
}

impl LocationSummary {
    pub fn absent() -> Self {
        Self {
            version_id: None,
            held: false,
            storage: None,
            materialized: false,
        }
    }
}

impl VersionReplicationMessage {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        let mut bytes = VERSION_REPLICATION_MAGIC.to_vec();
        bytes.extend(postcard::to_allocvec(self)?);
        Ok(bytes)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        let payload = bytes
            .strip_prefix(VERSION_REPLICATION_MAGIC)
            .ok_or_else(|| {
                ConversionError::FromStrError(
                    "invalid version replication message prefix".to_string(),
                )
            })?;
        Ok(postcard::from_bytes(payload)?)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ReplicationMode {
    Live,
    OnDemand,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VersionReplicationRequest {
    pub bucket: String,
    pub key: String,
    pub version_id: Ulid,
    pub source_group_id: aruna_core::types::GroupId,
    pub target_node_id: NodeId,
    pub auth_context: AuthContext,
    pub mode: ReplicationMode,
}

#[cfg(test)]
mod tests {
    use super::{
        BaoReadRefusal, BaoReadRequest, BaoReadTarget, MaterializedBlobInfo,
        MultipartObjectReplicationMetadata, SyncOrigin, VERSION_REPLICATION_MAGIC,
        VersionReplicationManifest, VersionReplicationMessage,
    };
    use aruna_blob::hash::Hasher;
    use aruna_core::UserId;
    use aruna_core::errors::ConversionError;
    use aruna_core::structs::checksum::HASH_SHA256;
    use aruna_core::structs::{
        ArunaArn, AuthContext, MultipartChecksumType, MultipartObjectPart, MultipartObjectSummary,
        RealmId, ReplicationItemKind, VersionSourceBinding,
    };
    use std::collections::HashMap;
    use std::time::SystemTime;
    use ulid::Ulid;

    fn test_realm_id() -> RealmId {
        RealmId::from_bytes([7u8; 32])
    }

    fn test_user_id() -> UserId {
        UserId::nil(test_realm_id())
    }

    fn make_manifest() -> VersionReplicationManifest {
        VersionReplicationManifest {
            bucket: "bucket".to_string(),
            key: "path/file.txt".to_string(),
            version_id: Ulid::generate(),
            group_id: Ulid::generate(),
            kind: ReplicationItemKind::DeleteMarker,
            created_at: SystemTime::now(),
            created_by: test_user_id(),
            current_version: true,
            current_version_generation: Some(1),
            auth_context: AuthContext {
                user_id: test_user_id(),
                realm_id: test_realm_id(),
                path_restrictions: None,
            },
            blob: None,
            source: None,
            multipart: None,
            reference_intent: false,
            origin: None,
            upstream_sources: Vec::new(),
            writer_auth_context: None,
            reference_metadata: None,
            metadata: HashMap::new(),
        }
    }

    #[test]
    fn bao_frames_roundtrip() {
        let request = VersionReplicationMessage::BaoReadRequest(BaoReadRequest {
            auth_context: AuthContext {
                user_id: test_user_id(),
                realm_id: test_realm_id(),
                path_restrictions: None,
            },
            realm_id: test_realm_id(),
            target: BaoReadTarget::Blake3([8u8; 32]),
            expected_blake3: Some([8u8; 32]),
            metadata_only: false,
        });
        let accepted = VersionReplicationMessage::BaoReadAccepted {
            size: 42,
            blake3: [8u8; 32],
        };
        let refused = VersionReplicationMessage::BaoReadRefused(BaoReadRefusal::ReadDenied);

        for message in [request, accepted, refused] {
            assert_eq!(
                VersionReplicationMessage::from_bytes(&message.to_bytes().unwrap()).unwrap(),
                message
            );
        }
    }

    #[test]
    fn version_replication_messages_roundtrip_with_magic_prefix() {
        let mut manifest = make_manifest();
        manifest.origin = Some(SyncOrigin {
            relationship_id: Ulid::from(7u128),
            hop_count: 3,
        });
        manifest.upstream_sources.push(
            ArunaArn::s3_bucket(
                test_realm_id(),
                iroh::SecretKey::from_bytes(&[3u8; 32]).public(),
                "source",
            )
            .unwrap(),
        );
        manifest.reference_metadata = Some(aruna_core::structs::SourceMetadata {
            content_length: 42,
            content_type: Some("text/plain".to_string()),
            etag: Some("etag-1".to_string()),
            last_modified: Some(SystemTime::UNIX_EPOCH),
            source_version: None,
        });
        manifest
            .metadata
            .insert("mtime".to_string(), "1753272000.123456789".to_string());
        let message = VersionReplicationMessage::VersionManifest(manifest);
        let bytes = message.to_bytes().unwrap();

        assert_eq!(
            VersionReplicationMessage::from_bytes(&bytes).unwrap(),
            message
        );
    }

    #[test]
    fn version_replication_messages_reject_invalid_prefix() {
        let message = VersionReplicationMessage::VersionManifest(make_manifest());
        let mut bytes = message.to_bytes().unwrap();
        bytes[0] = b'x';

        assert_eq!(
            VersionReplicationMessage::from_bytes(&bytes).unwrap_err(),
            ConversionError::FromStrError("invalid version replication message prefix".to_string())
        );
    }

    #[test]
    fn rejects_truncated_manifest() {
        let mut bytes = VersionReplicationMessage::VersionManifest(make_manifest())
            .to_bytes()
            .unwrap();
        bytes.pop();
        assert!(VersionReplicationMessage::from_bytes(&bytes).is_err());
    }

    #[test]
    fn preserves_vrp1_hashes() {
        let part_digests = [vec![1u8; 32], vec![2u8; 32]];
        let parts = part_digests
            .iter()
            .enumerate()
            .map(|(index, digest)| MultipartObjectPart {
                part_number: u16::try_from(index + 1).unwrap(),
                size: 5,
                hashes: HashMap::from([(HASH_SHA256.to_string(), digest.clone())]),
            })
            .collect::<Vec<_>>();
        let combined = part_digests.concat();
        let composite_sha256 = Hasher::new_with_bytes(&combined).finalize().sha256.to_vec();
        let mut manifest = make_manifest();
        manifest.multipart = Some(MultipartObjectReplicationMetadata {
            summary: MultipartObjectSummary {
                checksum_type: MultipartChecksumType::Composite,
                part_count: parts.len(),
                composite_hashes: HashMap::from([(
                    HASH_SHA256.to_string(),
                    composite_sha256.clone(),
                )]),
            },
            parts,
            checksum_type: MultipartChecksumType::Composite,
        });

        let bytes = VersionReplicationMessage::VersionManifest(manifest)
            .to_bytes()
            .unwrap();
        let VersionReplicationMessage::VersionManifest(decoded) =
            VersionReplicationMessage::from_bytes(&bytes).unwrap()
        else {
            panic!("expected version manifest")
        };
        assert_eq!(
            decoded
                .multipart
                .unwrap()
                .summary
                .composite_hashes
                .get(HASH_SHA256),
            Some(&composite_sha256)
        );
        assert!(!decoded.reference_intent);
    }
}
