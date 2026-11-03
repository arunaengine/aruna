//! Builds the portable source descriptor and version binding from a connector and metadata.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::execution::source_connector::{SourceConnector, SourceConnectorKind};
use aruna_core::structs::execution::staging::{
    PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
};
use ulid::Ulid;

pub fn build_source_descriptor(
    connector: &SourceConnector,
    metadata: &SourceMetadata,
    source_path: String,
    origin_node_id: Option<NodeId>,
) -> PortableSourceDescriptor {
    // What a source can do follows from its observation, so no capability is stored; only an
    // Aruna source is read through the node that holds it.
    let native = connector.kind == SourceConnectorKind::ArunaNative;
    PortableSourceDescriptor {
        kind: connector.kind,
        public_config: connector.public_config.clone(),
        source_path,
        version_selector: source_version_selector(metadata),
        capabilities: Vec::new(),
        origin_node_id: origin_node_id.filter(|_| native),
        namespace: connector.namespace.clone(),
        root: None,
    }
}

pub fn build_source_binding(
    strategy: StagingStrategy,
    connector: &SourceConnector,
    metadata: &SourceMetadata,
    source_path: String,
    origin_node_id: Option<NodeId>,
    connector_id: Option<Ulid>,
) -> VersionSourceBinding {
    VersionSourceBinding {
        strategy,
        descriptor: build_source_descriptor(connector, metadata, source_path, origin_node_id),
        connector_id,
    }
}

fn source_version_selector(metadata: &SourceMetadata) -> Option<String> {
    non_empty(metadata.source_version.as_deref()).map(|version| format!("version:{version}"))
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then_some(trimmed)
    })
}

#[cfg(test)]
mod pure_tests {
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

    use aruna_core::structs::execution::source_connector::SourceConnectorKind;

    fn sample_metadata() -> SourceMetadata {
        SourceMetadata {
            content_length: 42,
            content_type: Some("application/octet-stream".to_string()),
            etag: Some("etag-1".to_string()),
            last_modified: Some(SystemTime::UNIX_EPOCH),
            source_version: None,
            digests: Default::default(),
        }
    }

    #[test]
    fn descriptor_copies_config() {
        let metadata = sample_metadata();
        let descriptor = build_source_descriptor(
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
    fn binding_wraps_descriptor() {
        let metadata = sample_metadata();
        let binding = build_source_binding(
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
    fn descriptor_records_source() {
        let mut metadata = sample_metadata();
        metadata.source_version = Some("v42".to_string());

        let descriptor = build_source_descriptor(
            &sample_connector(),
            &metadata,
            "run-1/file.txt".to_string(),
            None,
        );

        assert_eq!(descriptor.version_selector.as_deref(), Some("version:v42"));
        assert!(descriptor.capabilities.is_empty());
    }
}
