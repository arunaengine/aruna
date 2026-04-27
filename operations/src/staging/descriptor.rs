use aruna_core::NodeId;
use aruna_core::structs::{
    PortableSourceDescriptor, SourceConnector, StagingStrategy, VersionSourceBinding,
};
use ulid::Ulid;

pub fn build_portable_source_descriptor(
    connector: &SourceConnector,
    source_path: String,
    origin_node_id: Option<NodeId>,
) -> PortableSourceDescriptor {
    PortableSourceDescriptor {
        kind: connector.kind,
        public_config: connector.public_config.clone(),
        source_path,
        version_selector: None,
        capabilities: Vec::new(),
        origin_node_id,
    }
}

pub fn build_version_source_binding(
    strategy: StagingStrategy,
    connector: &SourceConnector,
    source_path: String,
    origin_node_id: Option<NodeId>,
    connector_id: Option<Ulid>,
) -> VersionSourceBinding {
    VersionSourceBinding {
        strategy,
        descriptor: build_portable_source_descriptor(connector, source_path, origin_node_id),
        connector_id,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::time::SystemTime;
    use ulid::Ulid;

    fn sample_connector() -> SourceConnector {
        SourceConnector::new(
            Ulid::from_bytes([1u8; 16]),
            Ulid::from_bytes([2u8; 16]),
            "external-http".to_string(),
            SourceConnectorKind::Http,
            HashMap::from([
                ("endpoint".to_string(), "https://example.org".to_string()),
                ("root".to_string(), "/datasets".to_string()),
            ]),
            SystemTime::UNIX_EPOCH,
            SystemTime::UNIX_EPOCH,
            Default::default(),
        )
    }

    use aruna_core::structs::SourceConnectorKind;

    #[test]
    fn build_descriptor_copies_public_config_and_source_path() {
        let descriptor = build_portable_source_descriptor(
            &sample_connector(),
            "run-1/file.txt".to_string(),
            None,
        );

        assert_eq!(descriptor.kind, SourceConnectorKind::Http);
        assert_eq!(descriptor.source_path, "run-1/file.txt");
        assert_eq!(
            descriptor.public_config.get("root").map(String::as_str),
            Some("/datasets")
        );
    }

    #[test]
    fn build_binding_wraps_descriptor_with_strategy() {
        let binding = build_version_source_binding(
            StagingStrategy::Snapshot,
            &sample_connector(),
            "folder/blob.bin".to_string(),
            None,
            Some(Ulid::from_bytes([9u8; 16])),
        );

        assert_eq!(binding.strategy, StagingStrategy::Snapshot);
        assert_eq!(binding.descriptor.source_path, "folder/blob.bin");
        assert_eq!(binding.connector_id, Some(Ulid::from_bytes([9u8; 16])));
    }
}
