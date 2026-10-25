// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{DataLink, LinkScope, document_key, posting_document, posting_key};
use crate::structs::identity::realm::RealmId;
use crate::structs::storage::global_id::GlobalId;
use ulid::Ulid;

fn realm() -> RealmId {
    RealmId::from_bytes([7; 32])
}

fn node() -> crate::NodeId {
    iroh::SecretKey::from_bytes(&[3; 32]).public()
}

fn id(seed: u8) -> Ulid {
    Ulid::from_bytes([seed; 16])
}

#[test]
fn spellings_share_key() {
    let global = GlobalId::Version {
        realm_id: realm(),
        version_id: id(1),
    };
    let from_url = DataLink::parse(&global.to_url()).unwrap();
    let from_path = DataLink::parse(&global.to_string()).unwrap();
    assert_eq!(from_url, from_path);
    assert_eq!(from_url.key(), from_path.key());
    assert_eq!(from_url.to_id(), global.to_url());
}

#[test]
fn reads_prefix_forms() {
    let set = GlobalId::Set {
        realm_id: realm(),
        set_id: id(2),
        prefix: "raw/2026/".to_string(),
    };
    let link = DataLink::parse(&set.to_url()).unwrap();
    let (scope, prefix) = link.scope().unwrap();
    assert_eq!(prefix, "raw/2026/");
    assert_eq!(scope.link(prefix), link);

    let arn = format!("arn:aruna:{}:{}:s3/lab/raw/", realm(), node());
    let ordinary = DataLink::parse(&arn).unwrap();
    assert_eq!(ordinary.to_id(), arn);
    let root = DataLink::parse(&format!("arn:aruna:{}:{}:s3/lab", realm(), node())).unwrap();
    assert_eq!(root.scope().unwrap().1, "");
}

#[test]
fn ignores_other_values() {
    // Node ARN versions stay in the IRI index; plain text and s3 URLs are no links.
    let version = format!("arn:aruna:{}:{}:s3/lab/a.txt@{}", realm(), node(), id(4));
    for value in [
        version.as_str(),
        "s3://lab/a.txt",
        "hello",
        "/not/a/global/id",
    ] {
        assert_eq!(DataLink::parse(value), None, "{value}");
    }
}

#[test]
fn prefix_lengths_apart() {
    // `ab` must never be found by a probe for `abc`, and lengths sort ascending.
    let scope = LinkScope::Set {
        realm_id: realm(),
        set_id: id(5),
    };
    let short = scope.prefix_key("ab");
    let long = scope.prefix_key("abc");
    assert!(!long.starts_with(&short));
    assert!(short < long);
    assert_eq!(scope.length_of(&posting_key(&long, id(9))), Some(3));
    assert!(scope.length_start(3) <= long && scope.length_start(4) > long);
}

#[test]
fn scopes_never_overlap() {
    let one = LinkScope::Node {
        realm_id: realm(),
        node_id: node(),
        bucket: "ab".to_string(),
    };
    let two = LinkScope::Node {
        realm_id: realm(),
        node_id: node(),
        bucket: "abc".to_string(),
    };
    assert!(!two.prefix_key("").starts_with(&one.key()));
}

#[test]
fn posting_names_document() {
    let link = DataLink::Hash([1; 32]);
    let key = posting_key(&link.key(), id(6));
    assert_eq!(posting_document(&key), Some(id(6)));
    assert!(document_key(id(6), &key).starts_with(&id(6).to_bytes()));
}
