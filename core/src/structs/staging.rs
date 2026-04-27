use crate::id::NodeId;
use crate::structs::{SourceConnectorKind, SourceMetadata};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StagingStrategy {
    Reference,
    Snapshot,
    Sync,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PortableSourceDescriptor {
    pub kind: SourceConnectorKind,
    pub public_config: HashMap<String, String>,
    pub source_path: String,
    pub version_selector: Option<String>,
    pub capabilities: Vec<String>,
    pub origin_node_id: Option<NodeId>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct VersionSourceBinding {
    pub strategy: StagingStrategy,
    pub descriptor: PortableSourceDescriptor,
    pub connector_id: Option<Ulid>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum VersionState {
    Materialized {
        location: BackendLocation,
        source: Option<VersionSourceBinding>,
    },
    Reference {
        source: VersionSourceBinding,
        cached_metadata: SourceMetadata,
        last_refresh: SystemTime,
    },
    Deleted,
}

use crate::structs::BackendLocation;
use std::time::SystemTime;

impl VersionState {
    pub fn materialized_location(&self) -> Option<&BackendLocation> {
        match self {
            Self::Materialized { location, .. } => Some(location),
            Self::Reference { .. } | Self::Deleted => None,
        }
    }

    pub fn lookup_location(&self) -> Option<Location> {
        match self {
            Self::Materialized { location, .. } => Some(Location::Real(location.clone())),
            Self::Deleted => Some(Location::Deleted),
            Self::Reference { .. } => None,
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

use crate::structs::Location;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::UserId;
    use crate::structs::SourceMetadata;
    use ulid::Ulid;

    #[test]
    fn portable_source_descriptor_roundtrip_preserves_payload() {
        let descriptor = PortableSourceDescriptor {
            kind: SourceConnectorKind::S3,
            public_config: HashMap::from([
                ("endpoint".to_string(), "https://s3.example.com".to_string()),
                ("bucket".to_string(), "reads".to_string()),
            ]),
            source_path: "dataset/run-1/file.txt".to_string(),
            version_selector: Some("v1".to_string()),
            capabilities: vec!["versioned".to_string()],
            origin_node_id: None,
        };

        let restored: PortableSourceDescriptor =
            postcard::from_bytes(&postcard::to_allocvec(&descriptor).unwrap()).unwrap();

        assert_eq!(descriptor, restored);
    }

    #[test]
    fn version_source_binding_roundtrip_preserves_strategy_and_connector_id() {
        let binding = VersionSourceBinding {
            strategy: StagingStrategy::Snapshot,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::Http,
                public_config: HashMap::from([(
                    "endpoint".to_string(),
                    "https://example.org".to_string(),
                )]),
                source_path: "file.txt".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
            },
            connector_id: Some(Ulid::from_bytes([4u8; 16])),
        };

        let restored: VersionSourceBinding =
            postcard::from_bytes(&postcard::to_allocvec(&binding).unwrap()).unwrap();

        assert_eq!(binding, restored);
    }

    #[test]
    fn version_state_reports_lookup_location_for_materialized_and_deleted() {
        let materialized = VersionState::Materialized {
            location: BackendLocation {
                root: "/tmp".to_string(),
                storage_bucket: "bucket".to_string(),
                backend_path: "path".to_string(),
                ulid: Ulid::nil(),
                compressed: false,
                encrypted: false,
                created_by: UserId::default(),
                created_at: SystemTime::UNIX_EPOCH,
                staging: false,
                partial: false,
                blob_size: 1,
                hashes: HashMap::new(),
            },
            source: None,
        };
        let deleted = VersionState::Deleted;

        assert!(matches!(
            materialized.lookup_location(),
            Some(Location::Real(_))
        ));
        assert_eq!(deleted.lookup_location(), Some(Location::Deleted));
        assert!(deleted.materialized_location().is_none());
    }

    #[test]
    fn reference_state_exposes_source_binding_without_lookup_location() {
        let binding = VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::Webdav,
                public_config: HashMap::from([(
                    "endpoint".to_string(),
                    "https://webdav.example.com".to_string(),
                )]),
                source_path: "folder/file.txt".to_string(),
                version_selector: None,
                capabilities: vec!["head".to_string()],
                origin_node_id: None,
            },
            connector_id: None,
        };
        let state = VersionState::Reference {
            source: binding.clone(),
            cached_metadata: SourceMetadata {
                content_length: 5,
                content_type: Some("text/plain".to_string()),
                etag: Some("etag".to_string()),
                last_modified: Some(SystemTime::UNIX_EPOCH),
            },
            last_refresh: SystemTime::UNIX_EPOCH,
        };

        assert_eq!(state.lookup_location(), None);
        assert_eq!(state.source_binding(), Some(&binding));
    }
}
