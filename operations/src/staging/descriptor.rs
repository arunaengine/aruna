use aruna_core::NodeId;
use aruna_core::structs::{
    PortableSourceDescriptor, SourceConnector, SourceMetadata, StagingStrategy,
    VersionSourceBinding,
};
use ulid::Ulid;

pub fn build_portable_source_descriptor(
    connector: &SourceConnector,
    metadata: &SourceMetadata,
    source_path: String,
    origin_node_id: Option<NodeId>,
) -> PortableSourceDescriptor {
    PortableSourceDescriptor {
        kind: connector.kind,
        public_config: connector.public_config.clone(),
        source_path,
        version_selector: source_version_selector(metadata),
        capabilities: source_capabilities(metadata),
        origin_node_id,
    }
}

pub fn build_version_source_binding(
    strategy: StagingStrategy,
    connector: &SourceConnector,
    metadata: &SourceMetadata,
    source_path: String,
    origin_node_id: Option<NodeId>,
    connector_id: Option<Ulid>,
) -> VersionSourceBinding {
    VersionSourceBinding {
        strategy,
        descriptor: build_portable_source_descriptor(
            connector,
            metadata,
            source_path,
            origin_node_id,
        ),
        connector_id,
    }
}

fn source_version_selector(metadata: &SourceMetadata) -> Option<String> {
    non_empty(metadata.source_version.as_deref()).map(|version| format!("version:{version}"))
}

fn source_capabilities(metadata: &SourceMetadata) -> Vec<String> {
    let mut capabilities = vec!["head".to_string(), "read".to_string()];
    if non_empty(metadata.source_version.as_deref()).is_some() {
        capabilities.push("versioned".to_string());
    }
    if non_empty(metadata.etag.as_deref()).is_some() {
        capabilities.push("etag".to_string());
    }
    if metadata.last_modified.is_some() {
        capabilities.push("last_modified".to_string());
    }
    if non_empty(metadata.content_type.as_deref()).is_some() {
        capabilities.push("content_type".to_string());
    }
    capabilities
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then_some(trimmed)
    })
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

    fn sample_metadata() -> SourceMetadata {
        SourceMetadata {
            content_length: 42,
            content_type: Some("application/octet-stream".to_string()),
            etag: Some("etag-1".to_string()),
            last_modified: Some(SystemTime::UNIX_EPOCH),
            source_version: None,
        }
    }

    #[test]
    fn build_descriptor_copies_public_config_and_source_path() {
        let metadata = sample_metadata();
        let descriptor = build_portable_source_descriptor(
            &sample_connector(),
            &metadata,
            "run-1/file.txt".to_string(),
            None,
        );

        assert_eq!(descriptor.kind, SourceConnectorKind::Http);
        assert_eq!(descriptor.source_path, "run-1/file.txt");
        assert_eq!(
            descriptor.public_config.get("root").map(String::as_str),
            Some("/datasets")
        );
        assert_eq!(descriptor.version_selector, None);
    }

    #[test]
    fn build_binding_wraps_descriptor_with_strategy() {
        let metadata = sample_metadata();
        let binding = build_version_source_binding(
            StagingStrategy::Snapshot,
            &sample_connector(),
            &metadata,
            "folder/blob.bin".to_string(),
            None,
            Some(Ulid::from_bytes([9u8; 16])),
        );

        assert_eq!(binding.strategy, StagingStrategy::Snapshot);
        assert_eq!(binding.descriptor.source_path, "folder/blob.bin");
        assert_eq!(binding.connector_id, Some(Ulid::from_bytes([9u8; 16])));
    }

    #[test]
    fn build_descriptor_records_observed_source_version_and_capabilities() {
        let mut metadata = sample_metadata();
        metadata.source_version = Some("v42".to_string());

        let descriptor = build_portable_source_descriptor(
            &sample_connector(),
            &metadata,
            "run-1/file.txt".to_string(),
            None,
        );

        assert_eq!(descriptor.version_selector.as_deref(), Some("version:v42"));
        assert_eq!(
            descriptor.capabilities,
            vec![
                "head".to_string(),
                "read".to_string(),
                "versioned".to_string(),
                "etag".to_string(),
                "last_modified".to_string(),
                "content_type".to_string(),
            ]
        );
    }
}
