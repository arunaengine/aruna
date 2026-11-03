//! Links: a version in one bucket that names one exact version elsewhere. A link grants
//! nothing; each read is authorized at the target for the caller.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;

use ulid::Ulid;

use crate::structs::execution::source_connector::SourceConnectorKind;
use crate::structs::execution::staging::{
    PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
};
use crate::structs::identity::realm::RealmId;
use crate::structs::storage::replication::VersionedObjectArn;
use crate::types::GroupId;

/// The CopyObject header that writes a link instead of a copy, with the value `link`.
pub const COPY_MODE_HEADER: &str = "x-aruna-copy-mode";
/// Names the node the copy source lives on; absent means the node that serves the request.
pub const SOURCE_NODE_HEADER: &str = "x-aruna-copy-source-node";
/// The public setting a link keeps its target's group under.
const GROUP_KEY: &str = "group";
const VERSION_PREFIX: &str = "version:";

/// The exact version a link names and the group of its bucket.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LinkTarget {
    pub arn: VersionedObjectArn,
    pub group_id: GroupId,
}

impl LinkTarget {
    /// The source binding a link version stores.
    pub fn binding(&self) -> VersionSourceBinding {
        VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::ArunaLink,
                public_config: HashMap::from([(GROUP_KEY.to_string(), self.group_id.to_string())]),
                source_path: format!("{}/{}", self.arn.bucket, self.arn.key),
                version_selector: Some(format!("{VERSION_PREFIX}{}", self.arn.version)),
                capabilities: Vec::new(),
                origin_node_id: Some(self.arn.node_id),
                namespace: None,
                root: None,
            },
            connector_id: None,
        }
    }

    /// The target a stored link binding names; `None` for any other binding.
    pub fn of(binding: &VersionSourceBinding, realm_id: RealmId) -> Option<Self> {
        let descriptor = &binding.descriptor;
        if descriptor.kind != SourceConnectorKind::ArunaLink {
            return None;
        }
        let (bucket, key) = descriptor.source_path.split_once('/')?;
        let version = descriptor
            .version_selector
            .as_deref()?
            .strip_prefix(VERSION_PREFIX)
            .and_then(|version| Ulid::from_string(version).ok())?;
        let group_id = descriptor
            .public_config
            .get(GROUP_KEY)
            .and_then(|group| Ulid::from_string(group).ok())?;
        Some(Self {
            arn: VersionedObjectArn {
                realm_id,
                node_id: descriptor.origin_node_id?,
                bucket: bucket.to_string(),
                key: key.to_string(),
                version,
            },
            group_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binding_round_trip() {
        // A link binding names exactly its target and nothing else parses as one.
        let realm_id = RealmId::from_bytes([1; 32]);
        let target = LinkTarget {
            arn: VersionedObjectArn {
                realm_id,
                node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
                bucket: "bakta".to_string(),
                key: "db/bakta.db".to_string(),
                version: Ulid::from_parts(3, 3),
            },
            group_id: Ulid::from_parts(4, 4),
        };
        let binding = target.binding();
        assert_eq!(LinkTarget::of(&binding, realm_id), Some(target));
        let mut other = binding;
        other.descriptor.kind = SourceConnectorKind::S3;
        assert_eq!(LinkTarget::of(&other, realm_id), None);
    }
}
