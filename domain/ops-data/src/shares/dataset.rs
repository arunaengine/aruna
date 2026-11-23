//! The files a shared dataset names on this node and in the distributed buckets this node is a
//! member of, read from its RO-Crate.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::id::NodeId;
use aruna_core::repo_layout::root_id;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::data::identity::{DataIdentity, text_values};
use aruna_core::structs::storage::data::link::DataLink;
use aruna_core::structs::storage::global_id::GlobalId;
use serde_json::Value as JsonValue;
use std::collections::{BTreeSet, HashSet};
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
    /// Only a content hash; the local hash index or a member set's content index names it.
    Hash([u8; 32]),
    /// A folder or a whole bucket of this node; every key below `prefix` belongs to it.
    Prefix { bucket: String, prefix: String },
    /// A distributed bucket's version by its global id; its set must be one in `sets`.
    Version(Ulid),
}

/// A distributed bucket this node is a member of: its set, the one bucket name of its members,
/// and the member nodes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemberSet {
    pub set_id: Ulid,
    pub bucket: String,
    pub nodes: Vec<NodeId>,
}

impl MemberSet {
    fn names(sets: &[Self], node_id: NodeId, bucket: &str) -> bool {
        (sets.iter()).any(|set| set.bucket == bucket && set.nodes.contains(&node_id))
    }
}

/// The buckets and sets the root's parts name, so the caller can read which are member sets.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NamedSets {
    pub buckets: BTreeSet<String>,
    pub sets: BTreeSet<Ulid>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatasetFile {
    pub entity_id: String,
    pub name: Option<String>,
    pub place: FilePlace,
}

/// The ids the root's `hasPart` names directly; the root is `graph_iri`, `./` or the entity
/// the metadata descriptor is `about`, as for linked parts.
fn root_parts(graph: &[JsonValue], graph_iri: &str) -> HashSet<String> {
    let mut roots = HashSet::from([graph_iri.to_string(), "./".to_string()]);
    if let Some(about) = root_id(&serde_json::json!({ "@graph": graph })) {
        roots.insert(about);
    }
    let reference = |value: &JsonValue| {
        value
            .as_str()
            .or_else(|| value.get("@id").and_then(JsonValue::as_str))
            .map(str::to_string)
    };
    graph
        .iter()
        .filter(|entity| {
            let id = entity.get("@id").and_then(JsonValue::as_str);
            id.is_some_and(|id| roots.contains(id))
        })
        .filter_map(|root| root.get("hasPart"))
        .flat_map(|parts| match parts.as_array() {
            Some(parts) => parts.iter().filter_map(reference).collect::<Vec<_>>(),
            None => reference(parts).into_iter().collect(),
        })
        .collect()
}

/// The buckets and sets of this realm the root's parts name.
pub fn named_sets(rocrate: &JsonValue, graph_iri: &str, realm_id: RealmId) -> NamedSets {
    let mut named = NamedSets::default();
    let Some(graph) = rocrate.get("@graph").and_then(JsonValue::as_array) else {
        return named;
    };
    let parts = root_parts(graph, graph_iri);
    for entity in graph {
        let Some(entity_id) = entity.get("@id").and_then(JsonValue::as_str) else {
            continue;
        };
        if !parts.contains(entity_id) {
            continue;
        }
        match DataLink::parse(entity_id) {
            Some(DataLink::NodePrefix {
                realm_id: realm,
                bucket,
                ..
            }) if realm == realm_id => {
                named.buckets.insert(bucket);
            }
            Some(DataLink::SetPrefix {
                realm_id: realm,
                set_id,
                ..
            }) if realm == realm_id => {
                named.sets.insert(set_id);
            }
            _ => {}
        }
        let urls = text_values(entity.get("contentUrl"));
        if let Some(exact) = DataIdentity::read(entity_id, &urls).exact
            && exact.realm_id == realm_id
        {
            named.buckets.insert(exact.bucket);
        }
    }
    named
}

/// The parts the root's `hasPart` names directly that this node can serve: files, folders and
/// buckets, in graph order, on this node or another member of one of `sets`. Other mentions,
/// other nodes and realms, other distributed buckets and data outside Aruna are left out.
pub fn dataset_files(
    rocrate: &JsonValue,
    graph_iri: &str,
    (realm_id, node_id): (RealmId, NodeId),
    sets: &[MemberSet],
) -> Vec<DatasetFile> {
    let Some(graph) = rocrate.get("@graph").and_then(JsonValue::as_array) else {
        return Vec::new();
    };
    let parts = root_parts(graph, graph_iri);
    let served =
        |node: NodeId, bucket: &str| node == node_id || MemberSet::names(sets, node, bucket);
    graph
        .iter()
        .filter_map(|entity| {
            let entity = entity.as_object()?;
            let entity_id = entity.get("@id")?.as_str()?;
            if !parts.contains(entity_id) {
                return None;
            }
            let kind = entity.get("@type")?;
            let name = entity
                .get("name")
                .and_then(JsonValue::as_str)
                .map(str::to_string);
            let folder = |bucket, prefix| DatasetFile {
                entity_id: entity_id.to_string(),
                name: name.clone(),
                place: FilePlace::Prefix { bucket, prefix },
            };
            match DataLink::parse(entity_id) {
                Some(DataLink::NodePrefix {
                    realm_id: realm,
                    node_id: node,
                    bucket,
                    prefix,
                }) => {
                    let here = realm == realm_id && served(node, &bucket);
                    return (here && is_part(kind)).then(|| folder(bucket, prefix));
                }
                Some(DataLink::SetPrefix {
                    realm_id: realm,
                    set_id,
                    prefix,
                }) => {
                    let set = (sets.iter()).find(|set| set.set_id == set_id);
                    let set = set.filter(|_| realm == realm_id && is_part(kind))?;
                    return Some(folder(set.bucket.clone(), prefix));
                }
                _ => {}
            }
            if !is_file(kind) {
                return None;
            }
            let identity = DataIdentity::read(entity_id, &text_values(entity.get("contentUrl")));
            let version = match identity.global {
                Some(GlobalId::Version {
                    realm_id: realm,
                    version_id,
                }) if realm == realm_id => Some(version_id),
                _ => None,
            };
            let place = match (&identity.exact, &identity.location, version, identity.hash) {
                (Some(exact), ..) => (exact.realm_id == realm_id
                    && served(exact.node_id, &exact.bucket))
                .then(|| FilePlace::Object {
                    bucket: exact.bucket.clone(),
                    key: exact.key.clone(),
                    version_id: Some(exact.version),
                })?,
                (None, Some(location), ..) => FilePlace::Object {
                    bucket: location.bucket.clone(),
                    key: location.key.clone(),
                    version_id: None,
                },
                (None, None, Some(version), _) => FilePlace::Version(version),
                (None, None, None, Some(hash)) => FilePlace::Hash(hash),
                (None, None, None, None) => return None,
            };
            Some(DatasetFile {
                entity_id: entity_id.to_string(),
                name,
                place,
            })
        })
        .collect()
}

/// A `Dataset` (folder or bucket) or a file entity.
fn is_part(kind: &JsonValue) -> bool {
    let dataset = |value: &JsonValue| value.as_str() == Some("Dataset");
    is_file(kind)
        || dataset(kind)
        || kind
            .as_array()
            .is_some_and(|kinds| kinds.iter().any(dataset))
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
        let runs = format!("arn:aruna:{realm}:{here}:s3/data/runs/");
        let remote = format!("arn:aruna:{realm}:{there}:s3/data/");
        let crate_json = json!({"@graph": [
            {"@id": "./", "@type": "Dataset", "hasPart": [
                {"@id": arn(here)}, {"@id": arn(there)}, {"@id": "b.txt"},
                {"@id": "https://example.org/c.txt"}, {"@id": runs}, {"@id": remote},
            ]},
            {"@id": arn(here), "@type": "File", "name": "A"},
            {"@id": arn(there), "@type": "File"},
            {"@id": "b.txt", "@type": "File", "contentUrl": "s3://data/in/b.txt"},
            {"@id": "https://example.org/c.txt", "@type": "File"},
            {"@id": runs, "@type": "Dataset"},
            {"@id": remote, "@type": "Dataset"},
        ]});
        let files = dataset_files(&crate_json, "https://w3id.org/aruna/x", (realm, here), &[]);
        assert_eq!(files.len(), 3);
        assert_eq!(
            files[2].place,
            FilePlace::Prefix {
                bucket: "data".into(),
                prefix: "runs/".into(),
            }
        );
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

    // Parts of a distributed bucket this node is a member of are served here, by set, by
    // another member's ARN or by global version; another set stays metadata only.
    #[test]
    fn member_sets_served() {
        let realm = RealmId::from_bytes([1; 32]);
        let here = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let member = iroh::SecretKey::from_bytes(&[3; 32]).public();
        let (set_id, other_set) = (Ulid::from_bytes([5; 16]), Ulid::from_bytes([6; 16]));
        let version = Ulid::from_bytes([4; 16]);
        let set_url = |set_id| {
            let prefix = "runs/".to_string();
            GlobalId::Set {
                realm_id: realm,
                set_id,
                prefix,
            }
            .to_url()
        };
        let folder = format!("arn:aruna:{realm}:{member}:s3/spire/in/");
        let other = format!("arn:aruna:{realm}:{member}:s3/elsewhere/in/");
        let arn = VersionedObjectArn::new(realm, member, "spire", "a.txt", version).unwrap();
        let global = GlobalId::Version {
            realm_id: realm,
            version_id: version,
        }
        .to_url();
        let parts = [
            set_url(set_id),
            set_url(other_set),
            folder.clone(),
            other,
            arn.to_string(),
        ];
        let mut graph = vec![json!({"@id": "./", "@type": "Dataset", "hasPart": parts
            .iter()
            .chain(std::iter::once(&global))
            .map(|id| json!({"@id": id}))
            .collect::<Vec<_>>()})];
        for id in &parts[..4] {
            graph.push(json!({"@id": id, "@type": "Dataset"}));
        }
        graph.push(json!({"@id": parts[4], "@type": "File"}));
        graph.push(json!({"@id": global, "@type": "File"}));
        let crate_json = json!({ "@graph": graph });
        let named = named_sets(&crate_json, "https://w3id.org/aruna/x", realm);
        assert_eq!(named.sets, BTreeSet::from([set_id, other_set]));
        assert!(named.buckets.contains("spire") && named.buckets.contains("elsewhere"));
        let sets = [MemberSet {
            set_id,
            bucket: "spire".into(),
            nodes: vec![here, member],
        }];
        let files = dataset_files(
            &crate_json,
            "https://w3id.org/aruna/x",
            (realm, here),
            &sets,
        );
        let places: Vec<_> = files.into_iter().map(|file| file.place).collect();
        let prefix = |prefix: &str| FilePlace::Prefix {
            bucket: "spire".into(),
            prefix: prefix.into(),
        };
        let object = FilePlace::Object {
            bucket: "spire".into(),
            key: "a.txt".into(),
            version_id: Some(version),
        };
        let expected = [
            prefix("runs/"),
            prefix("in/"),
            object,
            FilePlace::Version(version),
        ];
        assert_eq!(places, expected);
        assert!(
            dataset_files(&crate_json, "https://w3id.org/aruna/x", (realm, here), &[])
                .iter()
                .all(|file| matches!(file.place, FilePlace::Version(_)))
        );
    }

    #[test]
    fn only_root_parts() {
        // A bucket the dataset is only based on, or a file no root part names, stays closed.
        let realm = RealmId::from_bytes([1; 32]);
        let here = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let bucket = format!("arn:aruna:{realm}:{here}:s3/private/");
        let folder = format!("arn:aruna:{realm}:{here}:s3/data/shared/");
        let crate_json = json!({"@graph": [
            {"@id": "ro-crate-metadata.json", "about": {"@id": "https://w3id.org/aruna/x"}},
            {"@id": "https://w3id.org/aruna/x", "@type": "Dataset",
                "hasPart": {"@id": folder}, "isBasedOn": {"@id": bucket}},
            {"@id": bucket, "@type": "Dataset"},
            {"@id": folder, "@type": "Dataset", "hasPart": [{"@id": "nested.txt"}]},
            {"@id": "nested.txt", "@type": "File", "contentUrl": "s3://private/nested.txt"},
            {"@id": "loose.txt", "@type": "File", "contentUrl": "s3://private/loose.txt"},
        ]});
        let files = dataset_files(
            &crate_json,
            "https://w3id.org/aruna/other",
            (realm, here),
            &[],
        );
        assert_eq!(files.len(), 1);
        assert_eq!(
            files[0].place,
            FilePlace::Prefix {
                bucket: "data".into(),
                prefix: "shared/".into(),
            }
        );
    }
}
