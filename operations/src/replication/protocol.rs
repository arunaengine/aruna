use aruna_blob::hash::Hasher;
use aruna_core::errors::ConversionError;
use aruna_core::id::NodeId;
use aruna_core::structs::checksum::ChecksumAlgorithm;
use aruna_core::structs::{
    AuthContext, BackendLocation, MultipartChecksumType, MultipartObjectPart,
    MultipartObjectSummary, ReplicationItemKind, ReplicationNegotiationResult,
    VersionSourceBinding,
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
    pub kind: ReplicationItemKind,
    pub created_at: std::time::SystemTime,
    pub created_by: aruna_core::types::UserId,
    pub current_version: bool,
    pub current_version_generation: Option<u64>,
    pub auth_context: AuthContext,
    pub blob: Option<MaterializedBlobInfo>,
    pub source: Option<VersionSourceBinding>,
    pub multipart: Option<MultipartObjectReplicationMetadata>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MaterializedBlobInfo {
    pub hash: [u8; 32],
    pub size: u64,
    pub compressed: bool,
    pub encrypted: bool,
    pub location: BackendLocation,
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
    pub target_node_id: NodeId,
    pub auth_context: AuthContext,
    pub mode: ReplicationMode,
}

#[cfg(test)]
mod tests {
    use super::{
        MaterializedBlobInfo, MultipartObjectReplicationMetadata, VERSION_REPLICATION_MAGIC,
        VersionReplicationManifest, VersionReplicationMessage,
    };
    use aruna_blob::hash::Hasher;
    use aruna_core::UserId;
    use aruna_core::errors::ConversionError;
    use aruna_core::structs::checksum::HASH_SHA256;
    use aruna_core::structs::{
        AuthContext, MultipartChecksumType, MultipartObjectPart, MultipartObjectSummary, RealmId,
        ReplicationItemKind, VersionSourceBinding,
    };
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use std::time::SystemTime;
    use ulid::Ulid;

    #[derive(Serialize, Deserialize)]
    enum LegacyVersionReplicationMessage {
        VersionManifest(LegacyVersionReplicationManifest),
    }

    #[derive(Serialize, Deserialize)]
    struct LegacyVersionReplicationManifest {
        bucket: String,
        key: String,
        version_id: Ulid,
        kind: ReplicationItemKind,
        created_at: SystemTime,
        created_by: UserId,
        current_version: bool,
        current_version_generation: Option<u64>,
        auth_context: AuthContext,
        blob: Option<MaterializedBlobInfo>,
        source: Option<VersionSourceBinding>,
        multipart: Option<LegacyMultipartObjectReplicationMetadata>,
    }

    #[derive(Serialize, Deserialize)]
    struct LegacyMultipartObjectReplicationMetadata {
        summary: LegacyMultipartObjectSummary,
        parts: Vec<MultipartObjectPart>,
        checksum_type: MultipartChecksumType,
    }

    #[derive(Serialize, Deserialize)]
    struct LegacyMultipartObjectSummary {
        checksum_type: MultipartChecksumType,
        part_count: usize,
    }

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
            version_id: Ulid::r#gen(),
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
        }
    }

    #[test]
    fn version_replication_messages_roundtrip_with_magic_prefix() {
        let message = VersionReplicationMessage::VersionManifest(make_manifest());
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
        let legacy: LegacyVersionReplicationMessage =
            postcard::from_bytes(bytes.strip_prefix(VERSION_REPLICATION_MAGIC).unwrap()).unwrap();
        let LegacyVersionReplicationMessage::VersionManifest(legacy_manifest) = &legacy;
        let legacy_multipart = legacy_manifest.multipart.as_ref().unwrap();
        assert_eq!(legacy_multipart.summary.part_count, 2);
        assert_eq!(legacy_multipart.parts.len(), 2);

        let mut legacy_bytes = VERSION_REPLICATION_MAGIC.to_vec();
        legacy_bytes.extend(postcard::to_allocvec(&legacy).unwrap());
        let VersionReplicationMessage::VersionManifest(decoded) =
            VersionReplicationMessage::from_bytes(&legacy_bytes).unwrap()
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
    }
}
