//! Staging strategy and the portable source descriptor an object version binds to.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::id::NodeId;
use crate::structs::execution::source_connector::SourceConnectorKind;
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
    /// Digest of the credential principal when it selects what the address names, as a login
    /// directory; the same on every node, never a connector id or a secret.
    pub namespace: Option<String>,
    /// The digest of the source root this node keeps once in `source_root_records`; while set,
    /// `public_config` holds only the object's own settings and the root holds the rest.
    pub root: Option<[u8; 32]>,
}

/// Adds a source to each file whose source path starts with `from`: the same path with `to` in
/// place of `from`, read through `connector_id`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceMapping {
    pub connector_id: Ulid,
    #[serde(default)]
    pub from: String,
    #[serde(default)]
    pub to: String,
}

impl SourceMapping {
    /// The other source's path of `path`, when the mapping covers it.
    pub fn map(&self, path: &str) -> Option<String> {
        let rest = path.strip_prefix(&self.from)?;
        Some(format!("{}{rest}", self.to))
    }
}

/// Another source of the same bytes, equal to the binding's first one.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceMirror {
    pub descriptor: PortableSourceDescriptor,
    pub connector_id: Option<Ulid>,
}

/// The sources of a version. All are equal: a read may use any source that proves the bytes.
/// The first one is the source whose validators the version's observation records.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct VersionSourceBinding {
    pub strategy: StagingStrategy,
    pub descriptor: PortableSourceDescriptor,
    pub connector_id: Option<Ulid>,
    pub mirrors: Vec<SourceMirror>,
}

impl VersionSourceBinding {
    pub fn source_count(&self) -> usize {
        1 + self.mirrors.len()
    }

    /// The source at `slot` alone, as a binding without mirrors; slot 0 is the first source.
    pub fn source_at(&self, slot: usize) -> Option<VersionSourceBinding> {
        let (descriptor, connector_id) = match slot {
            0 => (&self.descriptor, self.connector_id),
            _ => {
                let mirror = self.mirrors.get(slot - 1)?;
                (&mirror.descriptor, mirror.connector_id)
            }
        };
        Some(VersionSourceBinding {
            strategy: self.strategy.clone(),
            descriptor: descriptor.clone(),
            connector_id,
            mirrors: Vec::new(),
        })
    }

    /// The binding with the source at `slot` first and the others after it in their order.
    pub fn promoted(&self, slot: usize) -> Option<VersionSourceBinding> {
        let mut first = self.source_at(slot)?;
        first.mirrors = (0..self.source_count())
            .filter(|other| *other != slot)
            .filter_map(|other| self.source_at(other))
            .map(|source| SourceMirror {
                descriptor: source.descriptor,
                connector_id: source.connector_id,
            })
            .collect();
        Some(first)
    }

    /// The binding another node reads with its own connectors, as connector ids are node-local.
    pub fn without_connectors(mut self) -> Self {
        self.connector_id = None;
        for mirror in &mut self.mirrors {
            mirror.connector_id = None;
        }
        self
    }

    /// The connectors of all sources, each once.
    pub fn connector_ids(&self) -> Vec<Ulid> {
        let mut ids: Vec<Ulid> = (self.connector_id.into_iter())
            .chain(self.mirrors.iter().filter_map(|mirror| mirror.connector_id))
            .collect();
        ids.sort();
        ids.dedup();
        ids
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ulid::Ulid;

    #[test]
    fn portable_source_payload() {
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
            namespace: None,
            root: None,
        };

        let restored: PortableSourceDescriptor =
            postcard::from_bytes(&postcard::to_allocvec(&descriptor).unwrap()).unwrap();

        assert_eq!(descriptor, restored);
    }

    #[test]
    fn promotes_sources() {
        let source = |path: &str, connector: u8| SourceMirror {
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::Http,
                public_config: HashMap::new(),
                source_path: path.to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
                namespace: None,
                root: None,
            },
            connector_id: Some(Ulid::from_bytes([connector; 16])),
        };
        let [a, b, c] = [source("a", 1), source("b", 2), source("c", 1)];
        let binding = VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: a.descriptor.clone(),
            connector_id: a.connector_id,
            mirrors: vec![b.clone(), c.clone()],
        };
        let promoted = binding.promoted(2).unwrap();
        assert_eq!(promoted.descriptor, c.descriptor);
        assert_eq!(promoted.mirrors, [a, b.clone()]);
        let alone = binding.source_at(1).unwrap();
        assert_eq!((alone.descriptor, alone.mirrors.len()), (b.descriptor, 0));
        assert!(binding.source_at(3).is_none());
        assert_eq!(binding.connector_ids().len(), 2);
    }

    #[test]
    fn version_source_id() {
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
                namespace: None,
                root: None,
            },
            connector_id: Some(Ulid::from_bytes([4u8; 16])),
            mirrors: Vec::new(),
        };

        let restored: VersionSourceBinding =
            postcard::from_bytes(&postcard::to_allocvec(&binding).unwrap()).unwrap();

        assert_eq!(binding, restored);
    }
}
