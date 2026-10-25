//! Tests distributed bucket search through range indexes: coordinated ranges only, the
//! examined budget, incomplete ranges, authorization, prefix windows and realm-wide skips.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::{SystemTime, UNIX_EPOCH};

use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction, Permission};
use aruna_core::structs::storage::blob::{BucketInfo, object_permission_path};
use aruna_core::structs::storage::format::Compression;
use tempfile::TempDir;

use super::super::search_local_objects;
use super::super::tests::{Fixture, input, seed_materialized, setup, write_value};
use super::*;
use crate::distribution::key_index::{IndexedKey, KeyChange};
use crate::distribution::test_record::{binding, record, store_record};

struct Distributed {
    fixture: Fixture,
    _index_dir: TempDir,
    indexes: Arc<RangeIndexes>,
    binding: DistributionBinding,
}

/// A bucket "spire" whose lower range (keys before "m") member 1 coordinates; `keys` get
/// live heads and, in generation `state`, index documents.
async fn distributed(state: IndexState, keys: &[&str]) -> Distributed {
    let fixture = setup().await;
    let (snapshot, bytes) = record(true);
    store_record(&fixture.context.storage_handle, snapshot.set_id(), bytes).await;
    let binding = binding(&snapshot, 1, 5);
    let bucket = BucketInfo {
        group_id: fixture.visible_group,
        created_at: UNIX_EPOCH,
        created_by: fixture.owner,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: Some(binding.clone()),
    };
    let bucket = bucket.to_bytes().unwrap();
    write_value(
        &fixture.context,
        S3_BUCKET_KEYSPACE,
        b"spire".to_vec(),
        bucket,
    )
    .await;
    let lower = snapshot.config.ranges[0].range_id;
    let index_dir = tempfile::tempdir().unwrap();
    let indexes = Arc::new(RangeIndexes::new(index_dir.path()));
    let mut changes = Vec::new();
    for (seed, key) in keys.iter().enumerate() {
        seed_materialized(&fixture, "spire", key, UNIX_EPOCH, seed as u8 + 10).await;
        changes.push(KeyChange::Put(IndexedKey {
            key: key.to_string(),
            size: 42,
            version_id: Ulid::nil(),
        }));
    }
    let set_id = binding.set_id;
    indexes
        .apply(set_id, lower, state.generation(), changes)
        .await
        .unwrap();
    let state_row = state.to_bytes().unwrap();
    write_value(
        &fixture.context,
        SEARCH_INDEX_KEYSPACE,
        state_key(set_id, lower),
        state_row,
    )
    .await;
    Distributed {
        fixture,
        _index_dir: index_dir,
        indexes,
        binding,
    }
}

fn owner(fixture: &Fixture) -> AuthContext {
    AuthContext {
        user_id: fixture.owner,
        realm_id: fixture.realm_id,
        path_restrictions: None,
        session: None,
    }
}

impl Distributed {
    async fn search(&self, input: SearchObjectsInput, budget: usize) -> SearchNodePage {
        let search = RangeSearch {
            binding: self.binding.clone(),
            indexes: Some(self.indexes.clone()),
            budget,
        };
        (search_ranges(&self.fixture.context, &input, search).await).unwrap()
    }

    fn input(&self, auth: AuthContext, query: &str) -> SearchObjectsInput {
        let mut input = input(&self.fixture, auth, query, 10);
        input.bucket = Some("spire".to_string());
        input
    }
}

fn keys(page: &SearchNodePage) -> Vec<&str> {
    page.hits.iter().map(|hit| hit.hit.key.as_str()).collect()
}

#[tokio::test]
async fn searches_coordinated_ranges() {
    let ready = IndexState::Ready { generation: 1 };
    let test = distributed(ready, &["a/needle-1.txt", "b/needle-2.txt", "c/other.txt"]).await;
    // A live head the index does not hold is never found by a scan.
    seed_materialized(&test.fixture, "spire", "d/needle-3.txt", UNIX_EPOCH, 3).await;

    let page = test
        .search(test.input(owner(&test.fixture), "needle"), 100)
        .await;
    assert_eq!(keys(&page), ["a/needle-1.txt", "b/needle-2.txt"]);
    assert_eq!(page.next_start_after, None);
    assert_eq!(
        page.remote_ranges, 1,
        "member 2 coordinates the upper range"
    );
    assert_eq!(page.incomplete_ranges, 0);
    assert_eq!(page.hits[0].hit.size, Some(42));
}

#[tokio::test]
async fn denied_pages_bounded() {
    let ready = IndexState::Ready { generation: 1 };
    let test = distributed(ready, &["a/needle-1", "b/needle-2", "c/needle-3"]).await;
    let fixture = &test.fixture;
    let allowed = object_permission_path(
        fixture.realm_id,
        fixture.visible_group,
        fixture.node_id,
        "spire",
        "elsewhere",
    );
    let denied = AuthContext {
        path_restrictions: Some(vec![PathRestriction {
            pattern: allowed,
            permission: Permission::READ,
        }]),
        ..owner(fixture)
    };

    let first = test.search(test.input(denied.clone(), "needle"), 2).await;
    assert!(first.hits.is_empty());
    let cursor = first
        .next_start_after
        .expect("the budget ends the page early");
    // The position of a key the caller may not see is sealed, never readable.
    assert!(sealed_position(&cursor));
    let hidden = b"b/needle-2";
    assert!(!cursor.windows(hidden.len()).any(|part| part == hidden));
    assert_eq!(
        open_position(&cursor).unwrap(),
        BlobHeadKey::new("spire", "b/needle-2").to_bytes().unwrap()
    );

    let mut next = test.input(denied, "needle");
    next.start_after = Some(cursor);
    let second = test.search(next, 2).await;
    assert!(second.hits.is_empty());
    assert_eq!(
        second.next_start_after, None,
        "the last key ends the ranges"
    );
}

#[tokio::test]
async fn building_range_incomplete() {
    let building = IndexState::Building {
        generation: 1,
        cursor: None,
    };
    let test = distributed(building, &["a/needle-1"]).await;
    let page = test
        .search(test.input(owner(&test.fixture), "needle"), 100)
        .await;
    assert!(page.hits.is_empty(), "a building index is not searched");
    assert_eq!(page.incomplete_ranges, 1);
    assert_eq!(page.next_start_after, None);
}

#[tokio::test]
async fn prefix_window_continues() {
    let ready = IndexState::Ready { generation: 1 };
    let test = distributed(ready, &["job/a", "job/b", "job0/c", "jo/d"]).await;
    let mut prefix = test.input(owner(&test.fixture), "job/");
    prefix.key_match = ObjectKeyMatch::Prefix;
    prefix.limit = 1;
    let first = test.search(prefix.clone(), 100).await;
    assert_eq!(keys(&first), ["job/a"]);
    prefix.start_after = first.next_start_after;
    let second = test.search(prefix, 100).await;
    assert_eq!(keys(&second), ["job/b"]);
    assert_eq!(second.next_start_after, None);
}

#[test]
fn computes_prefix_end() {
    assert_eq!(prefix_end("run/").as_deref(), Some("run0"));
    assert_eq!(prefix_end("a\u{D7FF}").as_deref(), Some("a\u{E000}"));
    assert_eq!(prefix_end("a\u{10FFFF}").as_deref(), Some("b"));
    assert_eq!(prefix_end("\u{10FFFF}"), None);
}

#[tokio::test]
async fn ordinary_prefix_search() {
    let fixture = setup().await;
    for (seed, key) in [(1, "aaa/x"), (2, "pre/1"), (3, "pre/2"), (4, "zzz/pre/3")] {
        seed_materialized(&fixture, "visible", key, UNIX_EPOCH, seed).await;
    }
    let mut prefix = input(&fixture, owner(&fixture), "pre/", 10);
    prefix.key_match = ObjectKeyMatch::Prefix;
    prefix.bucket = Some("visible".to_string());
    let page = search_local_objects(&fixture.context, prefix)
        .await
        .unwrap();
    assert_eq!(keys(&page), ["pre/1", "pre/2"]);
    assert_eq!(page.next_start_after, None);
}

#[tokio::test]
async fn realm_skips_distributed() {
    let ready = IndexState::Ready { generation: 1 };
    let test = distributed(ready, &["needle-spire"]).await;
    let fixture = &test.fixture;
    seed_materialized(fixture, "visible", "needle-visible", UNIX_EPOCH, 2).await;
    let mut realm = input(fixture, owner(fixture), "needle", 10);
    realm.as_of = SystemTime::now();
    let page = search_local_objects(&fixture.context, realm).await.unwrap();
    assert_eq!(keys(&page), ["needle-visible"]);
}
