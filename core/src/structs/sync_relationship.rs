use crate::errors::ConversionError;
use crate::structs::{ArunaArn, ArunaArnType};
use crate::types::UserId;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use ulid::Ulid;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncMode {
    Once,
    Reference,
    Continuous,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncState {
    Enabled,
    Paused,
    Failed { reason: String },
    /// Serving-only stub left behind when a reference relationship is
    /// deleted: the target retains `BlobVersion::Reference` records that
    /// authorize reads through this relationship id, so the source keeps
    /// honoring native reference requests. Detached relationships are hidden
    /// from the management API and never queue or mirror new work.
    Detached,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncCounters {
    pub versions_synced: u64,
    pub bytes_synced: u64,
    pub failures: u64,
    pub consecutive_failures: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncStatusSnapshot {
    pub last_synced_at: Option<SystemTime>,
    pub last_error: Option<String>,
    pub counters: SyncCounters,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
/// Target conflicts follow the replication manifest timeline: the greatest
/// `(generation, version_id)` pair is the current version.
pub struct SyncRelationship {
    pub id: Ulid,
    pub source: ArunaArn,
    pub target: ArunaArn,
    pub mode: SyncMode,
    pub replicate_deletes: bool,
    pub created_by: UserId,
    pub created_at: SystemTime,
    pub state: SyncState,
    pub status: SyncStatusSnapshot,
}

impl SyncRelationship {
    pub fn validate(&self) -> Result<(), ConversionError> {
        validate_endpoint(&self.source, "source")?;
        validate_endpoint(&self.target, "target")?;
        if self.source == self.target {
            return Err(ConversionError::FromStrError(
                "sync source and target must differ".to_string(),
            ));
        }
        Ok(())
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        self.validate()?;
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        let relationship: Self = postcard::from_bytes(bytes)?;
        relationship.validate()?;
        Ok(relationship)
    }
}

pub fn sync_relationship_prefix(bucket: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(bucket.len() + 1);
    key.extend_from_slice(bucket.as_bytes());
    key.push(0);
    key
}

pub fn sync_relationship_key(bucket: &str, id: Ulid) -> Vec<u8> {
    let mut key = sync_relationship_prefix(bucket);
    key.extend_from_slice(&id.to_bytes());
    key
}

fn validate_endpoint(arn: &ArunaArn, endpoint: &str) -> Result<(), ConversionError> {
    if arn.resource_type != ArunaArnType::S3 || arn.bucket().is_none_or(str::is_empty) {
        return Err(ConversionError::FromStrError(format!(
            "sync {endpoint} must be a canonical S3 ARN"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::RealmId;
    use crate::{NodeId, UserId};

    fn test_node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn relationship() -> SyncRelationship {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        SyncRelationship {
            id: Ulid::from_bytes([2u8; 16]),
            source: ArunaArn::s3_object_prefix(
                realm_id,
                test_node(3),
                "source-bucket",
                "nested/prefix/",
            )
            .unwrap(),
            target: ArunaArn::s3_bucket(realm_id, test_node(4), "target-bucket").unwrap(),
            mode: SyncMode::Continuous,
            replicate_deletes: true,
            created_by: UserId::local(Ulid::from_bytes([5u8; 16]), realm_id),
            created_at: SystemTime::UNIX_EPOCH,
            state: SyncState::Enabled,
            status: SyncStatusSnapshot::default(),
        }
    }

    #[test]
    fn relationship_roundtrips() {
        let relationship = relationship();
        let restored = SyncRelationship::from_bytes(&relationship.to_bytes().unwrap()).unwrap();

        assert_eq!(restored, relationship);
        assert_eq!(
            ArunaArn::parse(&restored.source.to_string()).unwrap(),
            restored.source
        );
        assert_eq!(
            ArunaArn::parse(&restored.target.to_string()).unwrap(),
            restored.target
        );
    }

    #[test]
    fn rejects_non_s3() {
        let mut relationship = relationship();
        relationship.source = ArunaArn::new(
            relationship.source.realm_id,
            relationship.source.node_id,
            ArunaArnType::ContentHash,
            "abc123",
        )
        .unwrap();

        assert!(relationship.to_bytes().is_err());
    }

    #[test]
    fn rejects_same_endpoint() {
        let mut relationship = relationship();
        relationship.target = relationship.source.clone();

        assert!(relationship.to_bytes().is_err());
    }

    #[test]
    fn keys_group_bucket() {
        let first = Ulid::from_bytes([1u8; 16]);
        let second = Ulid::from_bytes([2u8; 16]);
        let prefix = sync_relationship_prefix("bucket");

        assert!(sync_relationship_key("bucket", first).starts_with(&prefix));
        assert!(sync_relationship_key("bucket", second).starts_with(&prefix));
        assert!(!sync_relationship_key("bucket-other", first).starts_with(&prefix));
    }
}
