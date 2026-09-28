//! Builds and reads data entity identities: content address `@id`, `s3://` location and path.
//! Writers use the normalized form; readers accept every older form as well.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::structs::identity::realm::RealmId;
use crate::structs::storage::replication::{
    ArunaArn, ArunaArnType, VersionedObjectArn, W3idIdentifier,
};
use serde_json::{Map, Value};
use std::collections::BTreeSet;

pub const CONTENT_URL: &str = "contentUrl";
pub const LOCAL_PATH: &str = "localPath";

/// An object named by an `s3://<bucket>/<key>` URL on the node that reads it.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct ObjectLocation {
    pub bucket: String,
    pub key: String,
}

impl ObjectLocation {
    pub fn parse(value: &str) -> Option<Self> {
        let (bucket, key) = value.strip_prefix("s3://")?.split_once('/')?;
        (!bucket.is_empty() && !key.is_empty()).then(|| Self {
            bucket: bucket.to_string(),
            key: key.to_string(),
        })
    }

    pub fn to_url(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.key)
    }
}

/// What the `@id` and `contentUrl` values of a data entity say about its Aruna object.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DataIdentity {
    pub exact: Option<VersionedObjectArn>,
    pub hash: Option<[u8; 32]>,
    /// The realm a content hash ARN names; `None` for a content hash W3ID.
    pub hash_realm: Option<RealmId>,
    pub location: Option<ObjectLocation>,
}

impl DataIdentity {
    /// Reads every supported form: versioned ARN or its W3ID, content hash W3ID or ARN,
    /// and `s3://` URLs. The first location found wins.
    pub fn read(entity_id: &str, content_urls: &[String]) -> Self {
        let mut identity = Self::default();
        for value in std::iter::once(entity_id).chain(content_urls.iter().map(String::as_str)) {
            if let Ok(identifier) = W3idIdentifier::parse(value) {
                match identifier {
                    W3idIdentifier::ContentHash(hash) => identity.hash = Some(hash),
                    W3idIdentifier::VersionedObject(exact) => identity.exact = Some(exact),
                }
            } else if let Ok(exact) = VersionedObjectArn::parse(value) {
                identity.exact = Some(exact);
            } else if let Ok(arn) = ArunaArn::parse(value)
                && arn.resource_type == ArunaArnType::ContentHash
                && let Some(hash) = parse_hash(&arn.path)
            {
                identity.hash = Some(hash);
                identity.hash_realm = Some(arn.realm_id);
            } else if identity.location.is_none() {
                identity.location = ObjectLocation::parse(value);
            }
        }
        identity
    }

    /// Whether any value names an Aruna object.
    pub fn is_aruna(&self) -> bool {
        self.exact.is_some() || self.hash.is_some() || self.location.is_some()
    }
}

/// A lowercase hex BLAKE3 digest, optionally prefixed with `blake3/`.
pub fn parse_hash(value: &str) -> Option<[u8; 32]> {
    let value = value.strip_prefix("blake3/").unwrap_or(value);
    let lower = value
        .bytes()
        .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'));
    if value.len() != 64 || !lower {
        return None;
    }
    let mut hash = [0; 32];
    hex::decode_to_slice(value, &mut hash).ok()?;
    Some(hash)
}

/// The content address W3ID of a BLAKE3 digest.
pub fn content_id(hash: [u8; 32]) -> String {
    W3idIdentifier::ContentHash(hash).to_w3id()
}

/// The normalized `@id`: the content address, or the `s3://` URL when another entity of
/// the same crate already uses that content address. Records the chosen id in `used`.
pub fn normalized_id(
    hash: [u8; 32],
    location: &ObjectLocation,
    used: &mut BTreeSet<String>,
) -> String {
    let content = content_id(hash);
    let id = if used.contains(&content) {
        location.to_url()
    } else {
        content
    };
    used.insert(id.clone());
    id
}

/// The values a writer stores for one data entity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataEntity {
    pub id: String,
    pub location: ObjectLocation,
    pub local_path: Option<String>,
    pub size: Option<u64>,
    pub encoding_format: Option<String>,
}

impl DataEntity {
    /// Writes the normalized values into an entity object, keeping its other properties.
    pub fn apply(&self, entity: &mut Map<String, Value>) {
        entity.insert("@id".into(), Value::String(self.id.clone()));
        entity.insert(CONTENT_URL.into(), Value::String(self.location.to_url()));
        match &self.local_path {
            Some(path) => entity.insert(LOCAL_PATH.into(), Value::String(path.clone())),
            None => entity.remove(LOCAL_PATH),
        };
        if let Some(size) = self.size {
            entity.insert("contentSize".into(), Value::String(size.to_string()));
        }
        if let Some(format) = &self.encoding_format {
            entity.insert("encodingFormat".into(), Value::String(format.clone()));
        }
    }
}

/// The text values of a compact JSON-LD property, including `{"@id": ...}` references.
pub fn text_values(value: Option<&Value>) -> Vec<String> {
    match value {
        Some(Value::String(value)) => vec![value.clone()],
        Some(Value::Array(values)) => values
            .iter()
            .flat_map(|value| text_values(Some(value)))
            .collect(),
        Some(Value::Object(value)) => value
            .get("@id")
            .and_then(Value::as_str)
            .map(str::to_string)
            .into_iter()
            .collect(),
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::NodeId;
    use serde_json::json;
    use std::str::FromStr;
    use ulid::Ulid;

    fn realm() -> RealmId {
        RealmId::from_bytes([4; 32])
    }

    fn node() -> NodeId {
        NodeId::from_str("ae58ff8833241ac82d6ff7611046ed67b5072d142c588d0063e942d9a75502b6")
            .expect("node id")
    }

    #[test]
    fn reads_all_forms() {
        let hash = [7; 32];
        let exact =
            VersionedObjectArn::new(realm(), node(), "b", "k/a.csv", Ulid::from(1)).unwrap();
        let location = ObjectLocation {
            bucket: "b".into(),
            key: "k/a.csv".into(),
        };
        let normalized = DataIdentity::read(&content_id(hash), &[location.to_url()]);
        assert_eq!(normalized.hash, Some(hash));
        assert_eq!(normalized.location, Some(location.clone()));
        let versioned = DataIdentity::read(&exact.to_w3id(), &[content_id(hash)]);
        assert_eq!(versioned.exact, Some(exact.clone()));
        assert_eq!(versioned.hash, Some(hash));
        assert_eq!(
            DataIdentity::read(&exact.to_string(), &[]).exact,
            Some(exact)
        );
        let url = DataIdentity::read("s3://b/k/a.csv", &[]);
        assert_eq!((url.location, url.hash), (Some(location), None));
        let arn = format!(
            "arn:aruna:{}:{}:ch/blake3/{}",
            realm(),
            node(),
            hex::encode(hash)
        );
        let by_arn = DataIdentity::read(&arn, &[]);
        assert_eq!(
            (by_arn.hash, by_arn.hash_realm),
            (Some(hash), Some(realm()))
        );
        let relative = DataIdentity::read("data/a.csv", &[]);
        assert!(!relative.is_aruna());
        assert_eq!(ObjectLocation::parse("s3://b/"), None);
        assert_eq!(ObjectLocation::parse("s3:///k"), None);
    }

    #[test]
    fn duplicate_content_id() {
        let first = ObjectLocation {
            bucket: "b".into(),
            key: "a".into(),
        };
        let second = ObjectLocation {
            bucket: "b".into(),
            key: "c".into(),
        };
        let mut used = BTreeSet::new();
        assert_eq!(
            normalized_id([1; 32], &first, &mut used),
            content_id([1; 32])
        );
        assert_eq!(normalized_id([1; 32], &second, &mut used), "s3://b/c");
        let read = DataIdentity::read("s3://b/c", &[]);
        assert_eq!(read.location, Some(second));
    }

    #[test]
    fn applies_entity_values() {
        let mut entity = json!({"@id": "data/a.csv", "@type": "File", "name": "a.csv",
            "localPath": "old"})
        .as_object()
        .cloned()
        .unwrap();
        DataEntity {
            id: content_id([2; 32]),
            location: ObjectLocation {
                bucket: "datasets-g".into(),
                key: "doc/data/a.csv".into(),
            },
            local_path: Some("data/a.csv".into()),
            size: Some(4),
            encoding_format: None,
        }
        .apply(&mut entity);
        assert_eq!(
            Value::Object(entity),
            json!({"@id": content_id([2; 32]), "@type": "File", "name": "a.csv",
                "contentUrl": "s3://datasets-g/doc/data/a.csv", "localPath": "data/a.csv",
                "contentSize": "4"})
        );
    }

    #[test]
    fn parses_hash_text() {
        let hex = "ab".repeat(32);
        assert_eq!(parse_hash(&hex), Some([0xab; 32]));
        assert_eq!(parse_hash(&format!("blake3/{hex}")), Some([0xab; 32]));
        assert_eq!(parse_hash(&hex.to_uppercase()), None);
        assert_eq!(parse_hash("ab"), None);
    }
}
