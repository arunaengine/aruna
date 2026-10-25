//! Names the source roots of a set's external references: the connector configuration without
//! the object path, so a set has few of them. A member that takes over a range checks every
//! root it holds versions of before it completes the transfer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::errors::ConversionError;
use crate::structs::execution::source_connector::SourceConnectorKind;
use crate::structs::execution::staging::{
    PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
};

const ROOT_DOMAIN: &[u8] = b"aruna-source-root-v1";
/// Follows a root's row key in the row that marks the root checked.
const CHECKED_TAG: u8 = 0x01;

/// One source root: the connector kind, its public configuration and the login namespace.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceRoot {
    pub kind: SourceConnectorKind,
    pub public_config: BTreeMap<String, String>,
    pub namespace: Option<String>,
}

impl SourceRoot {
    pub fn digest(&self) -> Result<[u8; 32], ConversionError> {
        let mut hasher = blake3::Hasher::new();
        hasher.update(ROOT_DOMAIN);
        hasher.update(&postcard::to_allocvec(self)?);
        Ok(*hasher.finalize().as_bytes())
    }
}

/// A root a member holds versions of, with one source path below it that a check probes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RootEntry {
    pub root: SourceRoot,
    pub probe_path: String,
}

impl RootEntry {
    /// The root of an external reference; native and device-local sources have none.
    pub fn of(source: &VersionSourceBinding) -> Option<Self> {
        let descriptor = &source.descriptor;
        if matches!(
            descriptor.kind,
            SourceConnectorKind::ArunaNative | SourceConnectorKind::LocalDirectory
        ) {
            return None;
        }
        let config = descriptor.public_config.iter();
        Some(Self {
            root: SourceRoot {
                kind: descriptor.kind,
                public_config: config.map(|(k, v)| (k.clone(), v.clone())).collect(),
                namespace: descriptor.namespace.clone(),
            },
            probe_path: descriptor.source_path.clone(),
        })
    }

    /// The reference a check resolves: the probe path under the root, naming no connector, so
    /// the checking member reads it through its own connector of the root.
    pub fn probe(&self) -> VersionSourceBinding {
        let root = &self.root;
        VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: root.kind,
                public_config: root.public_config.clone().into_iter().collect(),
                source_path: self.probe_path.clone(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
                namespace: root.namespace.clone(),
            },
            connector_id: None,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// The row of one root of a set: set id, then the root digest.
pub fn root_key(set_id: Ulid, digest: &[u8; 32]) -> Vec<u8> {
    let mut key = set_id.to_bytes().to_vec();
    key.extend_from_slice(digest);
    key
}

/// The row that marks a root of a set checked; it sorts right after the root's row.
pub fn checked_key(set_id: Ulid, digest: &[u8; 32]) -> Vec<u8> {
    let mut key = root_key(set_id, digest);
    key.push(CHECKED_TAG);
    key
}

/// The root digest a row names, and whether the row marks it checked.
pub fn decode_key(key: &[u8]) -> Option<([u8; 32], bool)> {
    let digest = <[u8; 32]>::try_from(key.get(16..48)?).ok()?;
    match key.get(48..) {
        Some([]) => Some((digest, false)),
        Some([CHECKED_TAG]) => Some((digest, true)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn source(kind: SourceConnectorKind, path: &str) -> VersionSourceBinding {
        VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind,
                public_config: HashMap::from([
                    ("endpoint".to_string(), "https://s3.example.org".to_string()),
                    ("bucket".to_string(), "bakrep-1".to_string()),
                ]),
                source_path: path.to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
                namespace: None,
            },
            connector_id: Some(Ulid::from_bytes([1; 16])),
        }
    }

    #[test]
    fn root_ignores_object_path() {
        // Every object of one source bucket shares one root; the connector id plays no part.
        let first = RootEntry::of(&source(SourceConnectorKind::S3, "a/1.fa")).unwrap();
        let second = RootEntry::of(&source(SourceConnectorKind::S3, "b/2.fa")).unwrap();
        assert_eq!(first.root.digest().unwrap(), second.root.digest().unwrap());
        let mut other = source(SourceConnectorKind::S3, "a/1.fa");
        other.descriptor.namespace = Some("login".to_string());
        let other = RootEntry::of(&other).unwrap();
        assert_ne!(first.root.digest().unwrap(), other.root.digest().unwrap());
        let probe = first.probe();
        assert_eq!(probe.connector_id, None);
        assert_eq!(probe.descriptor.source_path, "a/1.fa");
        let encoded = first.to_bytes().unwrap();
        assert_eq!(RootEntry::from_bytes(&encoded).unwrap(), first);
    }

    #[test]
    fn native_sources_have_no_root() {
        assert_eq!(
            RootEntry::of(&source(SourceConnectorKind::ArunaNative, "k")),
            None
        );
        assert_eq!(
            RootEntry::of(&source(SourceConnectorKind::LocalDirectory, "k")),
            None
        );
    }

    #[test]
    fn checked_marker_follows_root() {
        let (set_id, digest) = (Ulid::from_bytes([2; 16]), [3; 32]);
        let (root, checked) = (root_key(set_id, &digest), checked_key(set_id, &digest));
        assert!(root < checked && checked < root_key(set_id, &[4; 32]));
        assert_eq!(decode_key(&root), Some((digest, false)));
        assert_eq!(decode_key(&checked), Some((digest, true)));
        assert_eq!(decode_key(&root[..40]), None);
    }
}
