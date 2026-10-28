//! Tests the range index feeder against real storage and temporary index directories: the
//! first build, feeding, guarded row deletion, resumed builds and dropped ranges.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::ops::Bound;

use super::*;
use crate::distribution::key_index::KeyQuery;
use crate::distribution::test_record::{record, searched, store_record};
use crate::distribution::test_store::{apply, context, get, install, open, purge, range, write};
use aruna_core::structs::storage::distribution::inventory::head_position;
use tempfile::TempDir;

/// The set of [`searched`], which keeps a search index.
fn set() -> Ulid {
    searched().0.set_id()
}

fn range_id() -> Ulid {
    Ulid::from_bytes([1; 16])
}

struct Fixture {
    _dir: TempDir,
    _index_dir: TempDir,
    handle: StorageHandle,
    indexes: Arc<RangeIndexes>,
}

async fn fixture() -> Fixture {
    let (dir, handle) = open();
    store_record(&handle, set(), searched().1).await;
    install(&handle, set(), &[range(1, "", None)]).await;
    let index_dir = tempfile::tempdir().unwrap();
    let indexes = Arc::new(RangeIndexes::new(index_dir.path()));
    Fixture {
        _dir: dir,
        _index_dir: index_dir,
        handle,
        indexes,
    }
}

async fn state(handle: &StorageHandle) -> Option<IndexState> {
    let value = get(handle, SEARCH_INDEX_KEYSPACE, state_key(set(), range_id())).await;
    value.map(|value| IndexState::from_bytes(&value).unwrap())
}

async fn keys(indexes: &Arc<RangeIndexes>, generation: u64) -> Vec<String> {
    let query = KeyQuery {
        start: Bound::Unbounded,
        end: Bound::Unbounded,
        names: None,
        meta: None,
        limit: 100,
    };
    (indexes.search(set(), range_id(), generation, query).await)
        .unwrap()
        .keys
}

#[tokio::test]
async fn builds_then_feeds() {
    let fixture = fixture().await;
    let handle = &fixture.handle;
    assert!(apply(handle, write(set(), "a/one", 1, 3)).await.unwrap());
    assert!(apply(handle, write(set(), "b/two", 2, 4)).await.unwrap());
    let mut feeder = Feeder::new(fixture.indexes.clone(), FeedLimits::default());

    assert!(feeder.pass(&context(handle)).await);
    assert_eq!(
        state(handle).await,
        Some(IndexState::Ready { generation: 1 })
    );
    assert_eq!(keys(&fixture.indexes, 1).await, ["a/one", "b/two"]);

    assert!(apply(handle, write(set(), "c/three", 3, 5)).await.unwrap());
    assert!(apply(handle, purge(set(), "a/one", &[1])).await.unwrap());
    assert!(feeder.pass(&context(handle)).await);
    assert_eq!(keys(&fixture.indexes, 1).await, ["b/two", "c/three"]);
    let row = get(
        handle,
        SEARCH_FEED_KEYSPACE,
        head_position(set(), "c/three"),
    )
    .await;
    assert!(row.is_none(), "an applied feed row is dropped");
}

#[tokio::test]
async fn changed_rows_stay() {
    let fixture = fixture().await;
    let handle = &fixture.handle;
    assert!(apply(handle, write(set(), "k", 1, 3)).await.unwrap());
    let key = head_position(set(), "k");
    let stored = get(handle, SEARCH_FEED_KEYSPACE, key.clone())
        .await
        .unwrap();

    let stale = vec![(Key::from(key.clone()), Value::from(b"older".to_vec()))];
    assert_eq!(drop_settled(&handle.bulk(), stale).await.unwrap(), 0);
    assert!(
        get(handle, SEARCH_FEED_KEYSPACE, key.clone())
            .await
            .is_some(),
        "a row a writer changed after it was read stays"
    );
    let seen = vec![(Key::from(key.clone()), Value::from(stored))];
    assert_eq!(drop_settled(&handle.bulk(), seen).await.unwrap(), 1);
    assert!(get(handle, SEARCH_FEED_KEYSPACE, key).await.is_none());
}

#[tokio::test]
async fn build_resumes_cursor() {
    let fixture = fixture().await;
    let handle = &fixture.handle;
    for (seed, key) in [(1, "a"), (2, "b"), (3, "c")] {
        assert!(apply(handle, write(set(), key, seed, 1)).await.unwrap());
    }
    let limits = FeedLimits {
        rebuild_rows: 1,
        rebuild_pages: 1,
        ..FeedLimits::default()
    };
    let mut feeder = Feeder::new(fixture.indexes.clone(), limits);
    assert!(feeder.pass(&context(handle)).await);
    let Some(IndexState::Building {
        generation: 1,
        cursor,
    }) = state(handle).await
    else {
        panic!("one page leaves the index building");
    };
    assert!(cursor.is_some(), "the build keeps its durable cursor");

    // A fresh feeder resumes from the stored cursor, as after a restart.
    let mut feeder = Feeder::new(fixture.indexes.clone(), limits);
    for _ in 0..16 {
        feeder.pass(&context(handle)).await;
        if state(handle).await == Some(IndexState::Ready { generation: 1 }) {
            break;
        }
    }
    assert_eq!(
        state(handle).await,
        Some(IndexState::Ready { generation: 1 })
    );
    assert_eq!(keys(&fixture.indexes, 1).await, ["a", "b", "c"]);
}

// Feed rows of keys the build has not read yet settle unapplied; the build indexes them later.
#[tokio::test]
async fn build_skips_unread() {
    let fixture = fixture().await;
    let handle = &fixture.handle;
    for (seed, key) in [(1, "a"), (2, "b"), (3, "c")] {
        assert!(apply(handle, write(set(), key, seed, 1)).await.unwrap());
    }
    let limits = FeedLimits {
        rebuild_rows: 1,
        rebuild_pages: 1,
        ..FeedLimits::default()
    };
    let mut feeder = Feeder::new(fixture.indexes.clone(), limits);
    feeder.pass(&context(handle)).await;
    assert_eq!(keys(&fixture.indexes, 1).await, ["a"]);

    assert!(apply(handle, write(set(), "a", 4, 2)).await.unwrap());
    assert!(apply(handle, write(set(), "c", 5, 2)).await.unwrap());
    feeder.pass(&context(handle)).await;
    let row = |key| get(handle, SEARCH_FEED_KEYSPACE, head_position(set(), key));
    assert!(row("a").await.is_none(), "a read key's row is applied");
    assert!(row("c").await.is_none(), "an unread key's row settles");
    assert_eq!(keys(&fixture.indexes, 1).await, ["a"]);

    for _ in 0..16 {
        if state(handle).await == Some(IndexState::Ready { generation: 1 }) {
            break;
        }
        feeder.pass(&context(handle)).await;
    }
    assert_eq!(
        state(handle).await,
        Some(IndexState::Ready { generation: 1 })
    );
    assert_eq!(keys(&fixture.indexes, 1).await, ["a", "b", "c"]);
}

#[tokio::test]
async fn missing_index_rebuilds() {
    let fixture = fixture().await;
    let handle = &fixture.handle;
    let mut feeder = Feeder::new(fixture.indexes.clone(), FeedLimits::default());
    feeder.pass(&context(handle)).await;
    assert_eq!(
        state(handle).await,
        Some(IndexState::Ready { generation: 1 })
    );
    fixture
        .indexes
        .prune(set(), range_id(), None)
        .await
        .unwrap();

    assert!(apply(handle, write(set(), "k", 1, 3)).await.unwrap());
    feeder.pass(&context(handle)).await;
    let mut ready = state(handle).await;
    for _ in 0..4 {
        if ready == Some(IndexState::Ready { generation: 2 }) {
            break;
        }
        feeder.pass(&context(handle)).await;
        ready = state(handle).await;
    }
    assert_eq!(ready, Some(IndexState::Ready { generation: 2 }));
    assert_eq!(keys(&fixture.indexes, 2).await, ["k"]);
}

#[tokio::test]
async fn drops_gone_ranges() {
    let fixture = fixture().await;
    let handle = &fixture.handle;
    let gone = Ulid::from_bytes([9; 16]);
    let state = IndexState::Ready { generation: 1 };
    let key = state_key(set(), gone);
    put_row(
        &handle.bulk(),
        SEARCH_INDEX_KEYSPACE,
        key.clone(),
        state.to_bytes().unwrap(),
    )
    .await
    .unwrap();
    let changes = vec![KeyChange::Remove("k".to_string())];
    fixture
        .indexes
        .apply(set(), gone, 1, changes)
        .await
        .unwrap();

    let mut feeder = Feeder::new(fixture.indexes.clone(), FeedLimits::default());
    feeder.pass(&context(handle)).await;
    assert!(get(handle, SEARCH_INDEX_KEYSPACE, key).await.is_none());
    assert!(!fixture.indexes.exists(set(), gone, 1).await.unwrap());
}

#[tokio::test]
async fn split_reindexes_children() {
    use crate::distribution::split::{SplitConfig, SplitOperation};
    use crate::driver::drive;

    let fixture = fixture().await;
    let handle = &fixture.handle;
    assert!(apply(handle, write(set(), "a", 1, 3)).await.unwrap());
    assert!(apply(handle, write(set(), "x", 2, 4)).await.unwrap());
    let mut feeder = Feeder::new(fixture.indexes.clone(), FeedLimits::default());
    feeder.pass(&context(handle)).await;
    assert_eq!(
        state(handle).await,
        Some(IndexState::Ready { generation: 1 })
    );

    let (left, right) = (Ulid::from_bytes([2; 16]), Ulid::from_bytes([3; 16]));
    let split = SplitOperation::new(SplitConfig {
        set_id: set(),
        range_id: range_id(),
        split_key: "m".to_string(),
        left,
        right,
    });
    drive(split, &context(handle)).await.unwrap();
    let child = |range: Ulid| state_key(set(), range);
    for _ in 0..8 {
        feeder.pass(&context(handle)).await;
    }
    // The retired parent's index is gone; each child is built from its own keys.
    assert!(state(handle).await.is_none());
    for (range, expected) in [(left, "a"), (right, "x")] {
        let value = get(handle, SEARCH_INDEX_KEYSPACE, child(range))
            .await
            .unwrap();
        let IndexState::Ready { generation } = IndexState::from_bytes(&value).unwrap() else {
            panic!("the child index is built");
        };
        let query = KeyQuery {
            start: Bound::Unbounded,
            end: Bound::Unbounded,
            names: None,
            meta: None,
            limit: 10,
        };
        let found = fixture
            .indexes
            .search(set(), range, generation, query)
            .await;
        assert_eq!(found.unwrap().keys, [expected]);
    }
}

// A set without a search index gets no range index, and its feed rows are dropped unapplied.
#[tokio::test]
async fn search_off_skips() {
    let (_dir, handle) = open();
    let (snapshot, bytes) = record(true);
    let set_id = snapshot.set_id();
    store_record(&handle, set_id, bytes).await;
    install(&handle, set_id, &[range(1, "", None)]).await;
    assert!(apply(&handle, write(set_id, "a/one", 1, 3)).await.unwrap());
    let index_dir = tempfile::tempdir().unwrap();
    let indexes = Arc::new(RangeIndexes::new(index_dir.path()));
    let mut feeder = Feeder::new(indexes.clone(), FeedLimits::default());

    feeder.pass(&context(&handle)).await;

    let state = get(
        &handle,
        SEARCH_INDEX_KEYSPACE,
        state_key(set_id, range_id()),
    )
    .await;
    assert!(state.is_none());
    let feed = get(
        &handle,
        SEARCH_FEED_KEYSPACE,
        head_position(set_id, "a/one"),
    )
    .await;
    assert!(feed.is_none());
    assert!(!indexes.exists(set_id, range_id(), 1).await.unwrap());
}

/// A searchable head of `key` whose version row names `organism_name` and `taxid`.
async fn described(handle: &StorageHandle, key: &str, seed: u8, organism: &str) {
    use crate::distribution::inventory::{AddedVersion, HeadChange, InventoryChange};
    use aruna_core::structs::execution::table::{SEARCH_EXACT_KEY, SEARCH_TEXT_KEY};
    use aruna_core::structs::storage::blob::BackendRef;
    use aruna_core::structs::storage::distribution::OriginalEpoch;
    use aruna_core::structs::storage::distribution::inventory::VersionEntry;
    use aruna_core::structs::storage::format::EncodingClass;
    let version_id = Ulid::from_bytes([seed; 16]);
    let entry = VersionEntry {
        identity: Some([seed; 32]),
        searchable: true,
        ..VersionEntry::default()
    };
    let mut change = InventoryChange::new(set(), key);
    let (epoch, generation) = (OriginalEpoch(1), 1);
    change.added.push(AddedVersion {
        version_id,
        epoch,
        generation,
        entry,
    });
    change.head = HeadChange::Set(HeadEntry {
        version_id,
        epoch,
        generation,
        version: entry,
    });
    let update = crate::distribution::inventory::InventoryUpdate::new(change);
    assert!(apply(handle, update).await.unwrap());
    let metadata = std::collections::HashMap::from([
        ("organism_name".to_string(), organism.to_string()),
        ("taxid".to_string(), format!("{seed}")),
        (SEARCH_TEXT_KEY.to_string(), "organism_name".to_string()),
        (SEARCH_EXACT_KEY.to_string(), "taxid".to_string()),
    ]);
    let version = BlobVersion::materialized(
        [seed; 32],
        BackendRef::node_default(),
        EncodingClass::Raw,
        std::time::SystemTime::UNIX_EPOCH,
        Default::default(),
        None,
    )
    .with_metadata(metadata);
    let version_key = VersionKey::new("spire", key, Ulid::from_bytes([seed; 16]));
    let row = (version_key.to_bytes().unwrap(), version.to_bytes().unwrap());
    put_row(&handle.bulk(), BLOB_VERSIONS_KEYSPACE, row.0, row.1)
        .await
        .unwrap();
}

async fn matching(indexes: &Arc<RangeIndexes>, text: &str, exact: &str) -> Vec<String> {
    let set_of = |term: &str| {
        (!term.is_empty())
            .then(|| term.to_string())
            .into_iter()
            .collect()
    };
    let query = KeyQuery {
        start: Bound::Unbounded,
        end: Bound::Unbounded,
        names: None,
        meta: Some(MetaTerms {
            text: set_of(text),
            exact: set_of(exact),
        }),
        limit: 100,
    };
    (indexes.search(set(), range_id(), 1, query).await)
        .unwrap()
        .keys
}

// The build and the feed both index a searchable head from its own version's selections,
// without the job that wrote it; other keys stay out of metadata queries.
#[tokio::test]
async fn metadata_indexed_versions() {
    let fixture = fixture().await;
    let handle = &fixture.handle;
    described(handle, "coli/", 7, "Escherichia coli").await;
    assert!(
        apply(handle, write(set(), "coli/genomic.fna", 8, 4))
            .await
            .unwrap()
    );
    let mut feeder = Feeder::new(fixture.indexes.clone(), FeedLimits::default());
    assert!(feeder.pass(&context(handle)).await);
    assert_eq!(matching(&fixture.indexes, "coli", "").await, ["coli/"]);

    described(handle, "salmonella/", 9, "Salmonella enterica").await;
    assert!(feeder.pass(&context(handle)).await);
    assert_eq!(
        matching(&fixture.indexes, "", "taxid=9").await,
        ["salmonella/"]
    );
    assert!(
        matching(&fixture.indexes, "", "organism_name=Escherichia coli")
            .await
            .is_empty()
    );
    assert_eq!(keys(&fixture.indexes, 1).await.len(), 3);
}
