use crate::id::NodeId;
use crate::structs::SourceConnectorKind;
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

#[cfg(test)]
mod tests {
    use super::*;
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
}
