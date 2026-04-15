use aruna_core::errors::ConversionError;
use aruna_core::id::NodeId;
use aruna_core::structs::{
    BackendLocation, BucketInfo, MultipartChecksumType, MultipartObjectPart,
    MultipartObjectSummary, ReplicationItemKind, ReplicationNegotiationResult, UserIdentity,
};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

const VERSION_REPLICATION_MAGIC: &[u8; 4] = b"vrp1";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VersionReplicationManifest {
    pub bucket: String,
    pub key: String,
    pub version_id: Ulid,
    pub kind: ReplicationItemKind,
    pub bucket_info: BucketInfo,
    pub created_at: std::time::SystemTime,
    pub created_by: aruna_core::types::UserId,
    pub current_version: bool,
    pub user_identity: UserIdentity,
    pub blob: Option<MaterializedBlobInfo>,
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MultipartObjectReplicationMetadata {
    pub summary: MultipartObjectSummary,
    pub parts: Vec<MultipartObjectPart>,
    pub checksum_type: MultipartChecksumType,
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LiveReplicationRequest {
    pub bucket: String,
    pub key: String,
    pub version_id: Ulid,
    pub target_node_id: NodeId,
    pub bucket_info: BucketInfo,
    pub user_identity: UserIdentity,
}

#[cfg(test)]
mod tests {
    use super::{VersionReplicationManifest, VersionReplicationMessage};
    use aruna_core::errors::ConversionError;
    use aruna_core::structs::{BucketInfo, RealmId, ReplicationItemKind, UserIdentity};
    use std::time::SystemTime;
    use ulid::Ulid;

    fn make_manifest() -> VersionReplicationManifest {
        VersionReplicationManifest {
            bucket: "bucket".to_string(),
            key: "path/file.txt".to_string(),
            version_id: Ulid::new(),
            kind: ReplicationItemKind::DeleteMarker,
            bucket_info: BucketInfo {
                group_id: Ulid::new(),
                created_at: SystemTime::now(),
                created_by: Ulid::new(),
            },
            created_at: SystemTime::now(),
            created_by: Ulid::new(),
            current_version: true,
            user_identity: UserIdentity {
                user_id: Ulid::new(),
                realm_key: RealmId::from_bytes([7u8; 32]),
            },
            blob: None,
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
}
