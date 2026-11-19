//! Drives the link fanouts, part resolution, derived `isPartOf` and file queries without I/O.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::SystemTime;

use super::*;
use aruna_core::UserId;
use aruna_core::structs::storage::format::Compression;

use super::super::MetadataFanoutStats;

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn info() -> BucketInfo {
    BucketInfo {
        group_id: Ulid::from_bytes([90; 16]),
        created_at: SystemTime::UNIX_EPOCH,
        created_by: UserId::nil(realm()),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: None,
    }
}

fn bucket(result: Result<BucketInfo, GetBucketError>) -> IoResult {
    IoResult::Child(ChildOutcome::new(result))
}

fn node_link(owner: NodeId) -> DataLink {
    let scope = LinkScope::Node {
        realm_id: realm(),
        node_id: owner,
        bucket: "data".to_string(),
    };
    scope.link("reads/")
}

#[test]
fn foreign_part_resolves() {
    let mut resolve = resolve_part(node(1), node_link(node(2)));
    let Ok(Next::Done(target)) = resolve.first() else {
        panic!("expected the owner without a lookup");
    };
    assert_eq!((target.node_id, target.held_here), (Some(node(2)), false));
    assert_eq!(target.bucket.as_deref(), Some("data"));
    assert!(target.read_paths.is_empty());
}

#[test]
fn part_reads_bucket() {
    let mut resolve = resolve_part(node(1), node_link(node(1)));
    assert!(matches!(resolve.first(), Ok(Next::Call(IoCall::Child(_)))));
    let Ok(Next::Done(target)) = resolve.next(bucket(Ok(info()))) else {
        panic!("expected the owned part");
    };
    assert!(target.held_here);
    assert_eq!(target.prefix, "reads/");
    assert_eq!(target.read_paths.len(), 1);
}

#[test]
fn unknown_set_unnamed() {
    let scope = LinkScope::Set {
        realm_id: realm(),
        set_id: Ulid::from_parts(4, 4),
    };
    let mut resolve = resolve_part(node(1), scope.link(""));
    assert!(matches!(resolve.first(), Ok(Next::Call(IoCall::Read(_)))));
    let Ok(Next::Done(target)) = resolve.next(IoResult::Values(vec![None])) else {
        panic!("expected a part without a bucket");
    };
    assert_eq!((target.bucket, target.held_here), (None, false));
}

#[test]
fn missing_bucket_refused() {
    let mut part = part_of((realm(), node(1)), (None, None), "data", "reads/a");
    part.first().unwrap();
    let missing = part.next(bucket(Err(GetBucketError::NotFound)));
    assert!(matches!(missing, Err(MetadataApiError::NotFound)));
}

#[test]
fn part_asks_links() {
    let mut part = part_of((realm(), node(1)), (None, None), "data", "reads/a");
    part.first().unwrap();
    assert!(matches!(
        part.next(bucket(Ok(info()))),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let page = LinkPage {
        next_after: Some(Ulid::from_parts(5, 5)),
        complete: true,
        ..LinkPage::default()
    };
    let answer: Result<LinkPage, MetadataApiError> = Ok(page);
    let Ok(Next::Done(found)) = part.next(IoResult::Child(ChildOutcome::new(answer))) else {
        panic!("expected the derived isPartOf");
    };
    assert!(found.more && found.complete);
}

#[test]
fn file_queries_prefixes() {
    let mut queries = file_queries((realm(), node(1)), "data", "reads/a", None);
    queries.first().unwrap();
    assert!(matches!(
        queries.next(bucket(Ok(info()))),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let version = ObjectVersion {
        version_id: None,
        version: None,
    };
    let answer: Result<ObjectVersion, ObjectVersionError> = Ok(version);
    let Ok(Next::Done(found)) = queries.next(IoResult::Child(ChildOutcome::new(answer))) else {
        panic!("expected the file queries");
    };
    let scope = bucket_scope(realm(), node(1), "data", &info());
    let prefixes = LinkQuery::KeyPrefixes {
        scope,
        key: "reads/a".to_string(),
    };
    assert_eq!(found, vec![prefixes]);
}

#[test]
fn links_merge_pages() {
    let request = LinkNodeRequest {
        queries: Vec::new(),
        parts_only: false,
        after: None,
        limit: 5,
        confirm: None,
    };
    let mut links = links_distributed((realm(), node(1)), None, None, request);
    assert!(matches!(links.first(), Ok(Next::Call(IoCall::Child(_)))));
    let stats = MetadataFanoutStats {
        nodes_failed: 1,
        ..MetadataFanoutStats::default()
    };
    let parts = vec![(node(1), LinkNodePage::default())];
    let answer: Result<FanoutParts<LinkNodePage>, MetadataApiError> = Ok((parts, stats));
    let Ok(Next::Done(page)) = links.next(IoResult::Child(ChildOutcome::new(answer))) else {
        panic!("expected the merged page");
    };
    assert_eq!(page.nodes_failed, 1);
    assert!(!page.complete);
}
