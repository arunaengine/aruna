//! The files a shared dataset names on this node, read from its RO-Crate.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::id::NodeId;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::data::identity::{DataIdentity, text_values};
use serde_json::Value as JsonValue;
use ulid::Ulid;

use crate::metadata::public_preview::is_file;

/// Where a dataset file's bytes are on this node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FilePlace {
    Object {
        bucket: String,
        key: String,
        version_id: Option<Ulid>,
    },
    /// Only a content hash; the local hash index names the object.
    Hash([u8; 32]),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatasetFile {
    pub entity_id: String,
    pub name: Option<String>,
    pub place: FilePlace,
}

/// The crate's file entities this node can serve, in graph order. Files of other nodes,
/// of other realms and outside Aruna are left out.
pub fn dataset_files(rocrate: &JsonValue, realm_id: RealmId, node_id: NodeId) -> Vec<DatasetFile> {
    let Some(graph) = rocrate.get("@graph").and_then(JsonValue::as_array) else {
        return Vec::new();
    };
    graph
        .iter()
        .filter_map(|entity| {
            let entity = entity.as_object()?;
            let entity_id = entity.get("@id")?.as_str()?;
            if !is_file(entity.get("@type")?) {
                return None;
            }
            let identity = DataIdentity::read(entity_id, &text_values(entity.get("contentUrl")));
            let place = match (&identity.exact, &identity.location, identity.hash) {
                (Some(exact), _, _) => (exact.realm_id == realm_id && exact.node_id == node_id)
                    .then(|| FilePlace::Object {
                        bucket: exact.bucket.clone(),
                        key: exact.key.clone(),
                        version_id: Some(exact.version),
                    })?,
                (None, Some(location), _) => FilePlace::Object {
                    bucket: location.bucket.clone(),
                    key: location.key.clone(),
                    version_id: None,
                },
                (None, None, Some(hash)) => FilePlace::Hash(hash),
                (None, None, None) => return None,
            };
            let name = entity
                .get("name")
                .and_then(JsonValue::as_str)
                .map(str::to_string);
            Some(DatasetFile {
                entity_id: entity_id.to_string(),
                name,
                place,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::storage::replication::VersionedObjectArn;
    use serde_json::json;

    #[test]
    fn keeps_local_files() {
        let realm = RealmId::from_bytes([1; 32]);
        let here = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let there = iroh::SecretKey::from_bytes(&[3; 32]).public();
        let version = Ulid::from_bytes([4; 16]);
        let arn = |node| {
            VersionedObjectArn::new(realm, node, "data", "a.txt", version)
                .unwrap()
                .to_string()
        };
        let crate_json = json!({"@graph": [
            {"@id": "./", "@type": "Dataset"},
            {"@id": arn(here), "@type": "File", "name": "A"},
            {"@id": arn(there), "@type": "File"},
            {"@id": "b.txt", "@type": "File", "contentUrl": "s3://data/in/b.txt"},
            {"@id": "https://example.org/c.txt", "@type": "File"},
        ]});
        let files = dataset_files(&crate_json, realm, here);
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].name.as_deref(), Some("A"));
        assert_eq!(
            files[0].place,
            FilePlace::Object {
                bucket: "data".into(),
                key: "a.txt".into(),
                version_id: Some(version),
            }
        );
        assert_eq!(
            files[1].place,
            FilePlace::Object {
                bucket: "data".into(),
                key: "in/b.txt".into(),
                version_id: None,
            }
        );
    }
}
