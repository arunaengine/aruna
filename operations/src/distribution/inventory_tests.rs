//! Tests inventory updates against real storage: rows, counters, replays, range lookup at
//! bounds, cursor routing, split states and concurrent writers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::distribution::test_store::{
    apply, begin, commit, counters, get, has_row, install, open, range, stage,
};
use aruna_core::structs::storage::distribution::accounting::{Cursor, Rebuild};
use aruna_storage::storage::StorageHandle;

fn set() -> Ulid {
    Ulid::from_bytes([9; 16])
}

fn id(seed: u8) -> Ulid {
    Ulid::from_bytes([seed; 16])
}

/// Stored content of `size` bytes; its identity follows the size, so sizes tell contents apart.
fn stored(size: u64) -> VersionEntry {
    VersionEntry {
        marker: false,
        size,
        referenced: false,
        identity: Some([size.to_le_bytes()[0]; 32]),
        pending: false,
    }
}

/// A write of a new current version `seed` at `generation` with `size` bytes.
fn put(key: &str, seed: u8, generation: u64, size: u64) -> InventoryUpdate {
    let mut change = InventoryChange::new(set(), key);
    change.added.push(AddedVersion {
        version_id: id(seed),
        epoch: OriginalEpoch(1),
        generation,
        entry: stored(size),
    });
    change.head = HeadChange::Set(HeadEntry {
        version_id: id(seed),
        epoch: OriginalEpoch(1),
        generation,
        version: stored(size),
    });
    InventoryUpdate::new(change)
}

fn active(range_seed: u8) -> Vec<u8> {
    generation_key(set(), id(range_seed), 1)
}

async fn single_range(handle: &StorageHandle) {
    install(handle, set(), &[range(1, "", None)]).await;
}

async fn write_account(handle: &StorageHandle, range_seed: u8, account: &RangeAccount) {
    let event = handle
        .send_storage_effect(StorageEffect::Write {
            key_space: RANGE_ACCOUNT_KEYSPACE.to_string(),
            key: account_key(set(), id(range_seed)).into(),
            value: account.to_bytes().unwrap().into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}

#[tokio::test]
async fn counts_versions_and_heads() {
    let (_dir, handle) = open();
    single_range(&handle).await;
    assert!(apply(&handle, put("a", 1, 1, 10)).await.unwrap());
    assert!(apply(&handle, put("a", 2, 2, 4)).await.unwrap());
    assert!(apply(&handle, put("b", 3, 1, 7)).await.unwrap());

    let total = counters(&handle, active(1)).await;
    assert_eq!((total.objects, total.current_bytes), (2, 11));
    assert_eq!((total.versions, total.logical_bytes), (3, 21));
    assert_eq!(total.max_epoch, OriginalEpoch(1));
    let position = version_position(set(), "a", OriginalEpoch(1), 1, id(1));
    assert!(has_row(&handle, position.clone()).await);
    let index = get(&handle, VERSION_INDEX_KEYSPACE, index_key(set(), id(1))).await;
    let index = IndexEntry::from_bytes(&index.unwrap()).unwrap();
    assert_eq!(
        index.position, position,
        "the index resolves the id to its position"
    );
    let head = get(&handle, INVENTORY_KEYSPACE, head_position(set(), "a")).await;
    assert_eq!(
        HeadEntry::from_bytes(&head.unwrap()).unwrap().version_id,
        id(2)
    );
}

#[tokio::test]
async fn replay_changes_nothing() {
    let (_dir, handle) = open();
    single_range(&handle).await;
    assert!(apply(&handle, put("a", 1, 1, 10)).await.unwrap());
    let before = counters(&handle, active(1)).await;
    assert!(apply(&handle, put("a", 1, 1, 10)).await.unwrap());
    let mut late = put("a", 1, 7, 10);
    late.change.head = HeadChange::Keep;
    assert!(
        apply(&handle, late).await.unwrap(),
        "another generation is still a replay"
    );
    assert_eq!(counters(&handle, active(1)).await, before);
}

#[tokio::test]
async fn refuses_foreign_id() {
    let (_dir, handle) = open();
    single_range(&handle).await;
    assert!(apply(&handle, put("a", 1, 1, 10)).await.unwrap());
    let result = apply(&handle, put("b", 1, 1, 10)).await;
    assert_eq!(result, Err(InventoryError::Collision(id(1))));
}

/// The same id under the same key and epoch with other content is refused; an entry whose
/// content is not known yet takes the known identity.
#[tokio::test]
async fn refuses_other_content() {
    let (_dir, handle) = open();
    single_range(&handle).await;
    assert!(apply(&handle, put("a", 1, 1, 10)).await.unwrap());
    let result = apply(&handle, put("a", 1, 1, 11)).await;
    assert_eq!(result, Err(InventoryError::Collision(id(1))));
    let mut pending = put("a", 2, 2, 0);
    if let HeadChange::Set(head) = &mut pending.change.head {
        (head.version.identity, head.version.pending) = (None, true);
    }
    let entry = &mut pending.change.added[0].entry;
    (entry.identity, entry.pending) = (None, true);
    assert!(apply(&handle, pending.clone()).await.unwrap());
    assert!(
        apply(&handle, put("a", 2, 2, 0)).await.unwrap(),
        "the hash becomes known"
    );
    let index = get(&handle, VERSION_INDEX_KEYSPACE, index_key(set(), id(2))).await;
    let index = IndexEntry::from_bytes(&index.unwrap()).unwrap();
    assert_eq!(index.version.identity, Some([0; 32]));
    assert!(
        apply(&handle, pending).await.unwrap(),
        "a late pending replay changes nothing"
    );
    let again = get(&handle, VERSION_INDEX_KEYSPACE, index_key(set(), id(2))).await;
    assert_eq!(IndexEntry::from_bytes(&again.unwrap()).unwrap(), index);

    // A marker and a version never share an id, and stored content never loses its identity.
    let mut marker = put("a", 2, 2, 0);
    let entry = &mut marker.change.added[0].entry;
    (entry.marker, entry.identity) = (true, None);
    marker.change.head = HeadChange::Keep;
    assert_eq!(
        apply(&handle, marker).await,
        Err(InventoryError::Collision(id(2)))
    );
    let mut unknown = put("a", 2, 2, 0);
    unknown.change.added[0].entry.identity = None;
    unknown.change.head = HeadChange::Keep;
    assert_eq!(
        apply(&handle, unknown).await,
        Err(InventoryError::Collision(id(2)))
    );
}

#[tokio::test]
async fn purge_returns_to_zero() {
    let (_dir, handle) = open();
    single_range(&handle).await;
    assert!(apply(&handle, put("a", 1, 1, 10)).await.unwrap());
    let mut marker = InventoryChange::new(set(), "a");
    let entry = VersionEntry {
        marker: true,
        size: 0,
        referenced: false,
        identity: None,
        pending: false,
    };
    marker.added.push(AddedVersion {
        version_id: id(2),
        epoch: OriginalEpoch(1),
        generation: 2,
        entry,
    });
    marker.head = HeadChange::Set(HeadEntry {
        version_id: id(2),
        epoch: OriginalEpoch(1),
        generation: 2,
        version: entry,
    });
    assert!(apply(&handle, InventoryUpdate::new(marker)).await.unwrap());
    let total = counters(&handle, active(1)).await;
    assert_eq!(
        (total.objects, total.versions, total.delete_markers),
        (0, 1, 1)
    );
    let sample = SampleEntry::of_key("a").row_key(set(), id(1));
    let sampled = || counters(&handle, sample_count_key(set(), id(1)));
    assert_eq!(sampled().await.sampled, 1);
    assert!(
        get(&handle, KEY_SAMPLE_KEYSPACE, sample.clone())
            .await
            .is_some()
    );

    let mut purge = InventoryChange::new(set(), "a");
    purge.removed = vec![id(1), id(2), id(3)];
    purge.head = HeadChange::Remove;
    assert!(apply(&handle, InventoryUpdate::new(purge)).await.unwrap());
    let total = counters(&handle, active(1)).await;
    assert_eq!(
        total,
        RangeCounters {
            max_epoch: OriginalEpoch(1),
            ..Default::default()
        }
    );
    assert!(!has_row(&handle, head_position(set(), "a")).await);
    assert!(
        get(&handle, VERSION_INDEX_KEYSPACE, index_key(set(), id(1)))
            .await
            .is_none()
    );
    assert!(
        get(&handle, KEY_SAMPLE_KEYSPACE, sample).await.is_none(),
        "the key left the sample"
    );
    assert_eq!(sampled().await.sampled, 0);
}

#[tokio::test]
async fn seeks_range_at_bounds() {
    let (_dir, handle) = open();
    install(
        &handle,
        set(),
        &[range(1, "", Some("m")), range(2, "m", None)],
    )
    .await;
    assert!(apply(&handle, put("l\u{10FFFF}", 1, 1, 1)).await.unwrap());
    assert!(apply(&handle, put("m", 2, 1, 2)).await.unwrap());
    assert_eq!(counters(&handle, active(1)).await.current_bytes, 1);
    assert_eq!(
        counters(&handle, active(2)).await.current_bytes,
        2,
        "an upper bound is exclusive"
    );

    let other = Ulid::from_bytes([10; 16]);
    let mut stray = put("a", 3, 1, 1);
    stray.change.set_id = other;
    let missing = apply(&handle, stray).await;
    assert!(matches!(missing, Err(InventoryError::RangeMissing { .. })));
}

/// A contribution behind the rebuild cursor also lands in the rebuild row; one ahead of it
/// is left for the walk.
#[tokio::test]
async fn routes_behind_cursor() {
    let (_dir, handle) = open();
    single_range(&handle).await;
    let mut account = RangeAccount::fresh(String::new(), None);
    account.rebuild = Some(Rebuild {
        generation: 2,
        versions: Cursor::At(head_position(set(), "m")),
        uploads: Cursor::Start,
    });
    write_account(&handle, 1, &account).await;
    assert!(apply(&handle, put("a", 1, 1, 3)).await.unwrap());
    assert!(apply(&handle, put("z", 2, 1, 5)).await.unwrap());
    let mut upload = InventoryChange::new(set(), "a");
    upload.upload = Some((id(3), true));
    assert!(apply(&handle, InventoryUpdate::new(upload)).await.unwrap());

    let rebuild = counters(&handle, generation_key(set(), id(1), 2)).await;
    assert_eq!(
        (rebuild.objects, rebuild.versions, rebuild.current_bytes),
        (1, 1, 3)
    );
    assert_eq!(rebuild.open_uploads, 0, "the upload walk has not started");
    let total = counters(&handle, active(1)).await;
    assert_eq!((total.objects, total.open_uploads), (2, 1));
}

#[tokio::test]
async fn routes_split_states() {
    let (_dir, handle) = open();
    single_range(&handle).await;
    let mut child = RangeAccount::fresh(String::new(), None);
    child.charge = Charge::Child {
        parent: id(5),
        sibling: id(6),
        frozen: 1,
    };
    write_account(&handle, 1, &child).await;
    assert!(apply(&handle, put("a", 1, 1, 3)).await.unwrap());
    assert_eq!(
        counters(&handle, pending_key(set(), id(1))).await.objects,
        1
    );
    assert!(counters(&handle, active(1)).await.is_zero());

    child.charge = Charge::Retired {
        generation: 1,
        left: id(5),
        right: id(6),
    };
    write_account(&handle, 1, &child).await;
    let retired = apply(&handle, put("b", 2, 1, 3)).await;
    assert_eq!(retired, Err(InventoryError::RangeRetired(id(1))));
}

/// Writers of different keys never conflict on the counter rows, in either commit order.
#[tokio::test]
async fn concurrent_writers_commit() {
    let (_dir, handle) = open();
    single_range(&handle).await;
    let first = begin(&handle).await;
    let second = begin(&handle).await;
    stage(&handle, first, &mut put("a", 1, 1, 3)).await.unwrap();
    stage(&handle, second, &mut put("b", 2, 1, 4))
        .await
        .unwrap();
    assert!(commit(&handle, second).await);
    assert!(commit(&handle, first).await);
    let total = counters(&handle, active(1)).await;
    assert_eq!((total.objects, total.current_bytes), (2, 7));
}

/// A writer that read the account row before a change of it conflicts and adds nothing.
#[tokio::test]
async fn account_change_conflicts() {
    let (_dir, handle) = open();
    single_range(&handle).await;
    let writer = begin(&handle).await;
    stage(&handle, writer, &mut put("a", 1, 1, 3))
        .await
        .unwrap();
    let mut account = RangeAccount::fresh(String::new(), None);
    account.rebuild = Some(Rebuild::new(2));
    write_account(&handle, 1, &account).await;
    assert!(!commit(&handle, writer).await);
    assert!(counters(&handle, active(1)).await.is_zero());
}

/// Opening an open upload or closing a closed one changes no counter.
#[tokio::test]
async fn uploads_count_once() {
    let (_dir, handle) = open();
    single_range(&handle).await;
    let upload = |opened| {
        let mut change = InventoryChange::new(set(), "k");
        change.upload = Some((id(3), opened));
        InventoryUpdate::new(change)
    };
    for opened in [true, true] {
        assert!(apply(&handle, upload(opened)).await.unwrap());
    }
    assert_eq!(counters(&handle, active(1)).await.open_uploads, 1);
    for opened in [false, false] {
        assert!(apply(&handle, upload(opened)).await.unwrap());
    }
    assert_eq!(counters(&handle, active(1)).await.open_uploads, 0);
}

/// Marks the sample of range 1 full and stores its largest entry with `priority`.
async fn fill_sample(handle: &StorageHandle, priority: u64) -> Vec<u8> {
    let txn_id = begin(handle).await;
    let full = RangeCounters {
        sampled: i128::from(SAMPLE_SIZE),
        ..RangeCounters::default()
    };
    let count = sample_count_key(set(), id(1));
    let event = handle
        .send_storage_effect(StorageEffect::AddCounters {
            key_space: RANGE_COUNTER_KEYSPACE.to_string(),
            deltas: vec![(count.into(), full)],
            txn_id,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));
    assert!(commit(handle, txn_id).await);
    let largest = SampleEntry {
        priority,
        key: "largest".to_string(),
    };
    let row = largest.row_key(set(), id(1));
    write_sample(handle, row.clone(), None).await;
    row
}

async fn write_sample(handle: &StorageHandle, row: Vec<u8>, txn_id: Option<TxnId>) {
    let event = handle
        .send_storage_effect(StorageEffect::Write {
            key_space: KEY_SAMPLE_KEYSPACE.to_string(),
            key: row.into(),
            value: Vec::new().into(),
            txn_id,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}

/// A key that cannot enter a full sample never reads its largest entry in the transaction,
/// so a concurrent sample change does not conflict with the write.
#[tokio::test]
async fn full_sample_no_conflict() {
    let (_dir, handle) = open();
    single_range(&handle).await;
    fill_sample(&handle, 0).await;
    let writer = begin(&handle).await;
    stage(&handle, writer, &mut put("a", 1, 1, 1))
        .await
        .unwrap();
    let other = begin(&handle).await;
    let smaller = SampleEntry {
        priority: 0,
        key: "another".to_string(),
    };
    write_sample(&handle, smaller.row_key(set(), id(1)), Some(other)).await;
    assert!(commit(&handle, other).await);
    assert!(
        commit(&handle, writer).await,
        "the writer read no sample row"
    );
    let taken = SampleEntry::of_key("a").row_key(set(), id(1));
    assert!(get(&handle, KEY_SAMPLE_KEYSPACE, taken).await.is_none());
}

/// A full sample takes a new key only in place of its largest entry, so its size stays.
#[tokio::test]
async fn full_sample_evicts() {
    let (_dir, handle) = open();
    single_range(&handle).await;
    let count = sample_count_key(set(), id(1));
    let row = fill_sample(&handle, u64::MAX).await;

    assert!(apply(&handle, put("a", 1, 1, 1)).await.unwrap());
    let taken = SampleEntry::of_key("a").row_key(set(), id(1));
    assert!(get(&handle, KEY_SAMPLE_KEYSPACE, taken).await.is_some());
    assert!(
        get(&handle, KEY_SAMPLE_KEYSPACE, row).await.is_none(),
        "the largest left"
    );
    assert_eq!(
        counters(&handle, count).await.sampled,
        i128::from(SAMPLE_SIZE)
    );
}

/// Updates of one transaction count each other's admissions, so a nearly full sample takes
/// the second new key only in place of its largest entry.
#[tokio::test]
async fn counts_own_admissions() {
    use crate::node::usage_stats::UsageCounterUpdate;
    use aruna_core::structs::storage::usage::UsageDelta;

    let (_dir, handle) = open();
    single_range(&handle).await;
    fill_sample(&handle, u64::MAX).await;
    let txn_id = begin(&handle).await;
    let almost = RangeCounters {
        sampled: -1,
        ..RangeCounters::default()
    };
    let count = sample_count_key(set(), id(1));
    handle
        .send_storage_effect(StorageEffect::AddCounters {
            key_space: RANGE_COUNTER_KEYSPACE.to_string(),
            deltas: vec![(count.clone().into(), almost)],
            txn_id,
        })
        .await;
    assert!(commit(&handle, txn_id).await);

    let changes = [put("a", 1, 1, 1), put("b", 2, 1, 1)].map(|update| update.change().clone());
    let mut update =
        UsageCounterUpdate::for_group(Ulid::nil(), UsageDelta::default()).with_inventories(changes);
    let txn_id = begin(&handle).await;
    let mut effects = update.start(txn_id);
    while let Some(effect) = effects.pop() {
        let event = crate::distribution::test_store::send(&handle, effect).await;
        match update.step(event, txn_id).unwrap() {
            Some(next) => effects = next,
            None => break,
        }
    }
    assert!(commit(&handle, txn_id).await);
    assert_eq!(
        counters(&handle, count).await.sampled,
        i128::from(SAMPLE_SIZE)
    );
}

/// A key staged earlier in the transaction may be the sample's largest entry, so a later key
/// below it enters in its place although it ranks above the committed largest entry.
#[tokio::test]
async fn sees_staged_entry() {
    use crate::node::usage_stats::UsageCounterUpdate;
    use aruna_core::structs::storage::usage::UsageDelta;

    let (_dir, handle) = open();
    single_range(&handle).await;
    let (first, second) = ("a", "b");
    let (high, low) =
        match SampleEntry::of_key(first).priority > SampleEntry::of_key(second).priority {
            true => (first, second),
            false => (second, first),
        };
    let floor = SampleEntry::of_key(low).priority.saturating_sub(1);
    fill_sample(&handle, floor).await;
    let txn_id = begin(&handle).await;
    let almost = RangeCounters {
        sampled: -1,
        ..RangeCounters::default()
    };
    let count = sample_count_key(set(), id(1));
    handle
        .send_storage_effect(StorageEffect::AddCounters {
            key_space: RANGE_COUNTER_KEYSPACE.to_string(),
            deltas: vec![(count.clone().into(), almost)],
            txn_id,
        })
        .await;
    assert!(commit(&handle, txn_id).await);

    let changes = [put(high, 1, 1, 1), put(low, 2, 1, 1)].map(|update| update.change().clone());
    let mut update =
        UsageCounterUpdate::for_group(Ulid::nil(), UsageDelta::default()).with_inventories(changes);
    let txn_id = begin(&handle).await;
    let mut effects = update.start(txn_id);
    while let Some(effect) = effects.pop() {
        let event = crate::distribution::test_store::send(&handle, effect).await;
        match update.step(event, txn_id).unwrap() {
            Some(next) => effects = next,
            None => break,
        }
    }
    assert!(commit(&handle, txn_id).await);
    let row = |key: &str| SampleEntry::of_key(key).row_key(set(), id(1));
    assert!(get(&handle, KEY_SAMPLE_KEYSPACE, row(low)).await.is_some());
    assert!(
        get(&handle, KEY_SAMPLE_KEYSPACE, row(high)).await.is_none(),
        "the staged largest entry left"
    );
    assert_eq!(
        counters(&handle, count).await.sampled,
        i128::from(SAMPLE_SIZE)
    );
}
