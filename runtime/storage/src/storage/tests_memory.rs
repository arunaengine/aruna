//! Tests that memory storage answers every storage effect as Fjall storage does.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_clock::testing::poll_until;
use aruna_core::compute::{SecretBytes, SharedSecret};
use aruna_core::credential_encryption::CredentialEncryptionKey;
use aruna_core::effects::{IterEnd, IterStart, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, CONNECTOR_SECRET_KEYSPACE, REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::node::vault::{NodeVaultKey, VaultEntry, VaultPurpose};
use aruna_core::structs::storage::distribution::OriginalEpoch;
use aruna_core::structs::storage::distribution::accounting::RangeCounters;
use aruna_core::structs::storage::usage::{
    BucketDelta, USAGE_COMPLETE_KEY, USAGE_REBUILD_KEY, UsageDelta, usage_shadow_key,
};
use byteview::ByteView;
use tempfile::tempdir;
use ulid::Ulid;

use super::handle::StorageHandle;
use super::hot_cache::cached_slot;
use super::memory::MemoryStorage;
use super::worker::FjallStorage;

const SPACE: &str = "rows";
const OTHER: &str = "other";
const USAGE: &str = "usage_stats";
const COUNTERS: &str = "range_counters";

/// Runs `scenario` on Fjall storage and on memory storage; both must answer alike.
async fn parity(scenario: impl AsyncFn(&StorageHandle) -> Vec<String>) -> Vec<String> {
    let dir = tempdir().unwrap();
    let fjall = FjallStorage::open_test(dir.path().to_str().unwrap()).unwrap();
    let expected = scenario(&fjall).await;
    let memory = scenario(&MemoryStorage::open()).await;
    assert_eq!(expected, memory, "memory storage answers as Fjall storage");
    memory
}

/// Sends effects and records each answer without transaction ids, which differ between runs.
struct Session<'a> {
    handle: &'a StorageHandle,
    trace: Vec<String>,
}

impl<'a> Session<'a> {
    fn new(handle: &'a StorageHandle) -> Self {
        Self {
            handle,
            trace: Vec::new(),
        }
    }

    async fn send(&mut self, effect: StorageEffect) -> StorageEvent {
        let Event::Storage(event) = self.handle.send_storage_effect(effect).await else {
            panic!("storage event expected");
        };
        self.trace.push(match &event {
            StorageEvent::TransactionStarted { .. } => "started".to_string(),
            StorageEvent::TransactionAborted { .. } => "aborted".to_string(),
            StorageEvent::TransactionCommitted { sequence, .. } => format!("committed {sequence}"),
            event => format!("{event:?}"),
        });
        event
    }

    async fn begin(&mut self, read: bool) -> Ulid {
        match self.send(StorageEffect::StartTransaction { read }).await {
            StorageEvent::TransactionStarted { txn_id } => txn_id,
            other => panic!("transaction did not start: {other:?}"),
        }
    }

    fn note(&mut self, text: String) {
        self.trace.push(text);
    }
}

fn bytes(text: &str) -> ByteView {
    ByteView::from(text.as_bytes())
}

fn put(key: &str, value: &str, txn_id: Option<Ulid>) -> StorageEffect {
    StorageEffect::Write {
        key_space: SPACE.to_string(),
        key: bytes(key),
        value: bytes(value),
        txn_id,
    }
}

fn get(key: &str, txn_id: Option<Ulid>) -> StorageEffect {
    StorageEffect::Read {
        key_space: SPACE.to_string(),
        key: bytes(key),
        txn_id,
    }
}

fn remove(key: &str, txn_id: Option<Ulid>) -> StorageEffect {
    StorageEffect::Delete {
        key_space: SPACE.to_string(),
        key: bytes(key),
        txn_id,
    }
}

fn iter(
    prefix: &str,
    start: Option<IterStart>,
    limit: usize,
    txn_id: Option<Ulid>,
) -> StorageEffect {
    StorageEffect::Iter {
        key_space: SPACE.to_string(),
        prefix: (!prefix.is_empty()).then(|| bytes(prefix)),
        start,
        limit,
        txn_id,
    }
}

fn range(
    start: IterStart,
    end: IterEnd,
    (limit, max_bytes): (usize, usize),
    txn_id: Option<Ulid>,
) -> StorageEffect {
    StorageEffect::IterRange {
        key_space: SPACE.to_string(),
        start,
        end,
        limit,
        max_bytes,
        txn_id,
    }
}

fn last(prefix: &str, txn_id: Option<Ulid>) -> StorageEffect {
    StorageEffect::Last {
        key_space: SPACE.to_string(),
        prefix: (!prefix.is_empty()).then(|| bytes(prefix)),
        txn_id,
    }
}

fn commit(txn_id: Ulid) -> StorageEffect {
    StorageEffect::CommitTransaction { txn_id }
}

fn abort(txn_id: Ulid) -> StorageEffect {
    StorageEffect::AbortTransaction { txn_id }
}

fn read_value(event: &StorageEvent) -> Option<&[u8]> {
    match event {
        StorageEvent::ReadResult { value, .. } => value.as_deref(),
        other => panic!("read result expected, got {other:?}"),
    }
}

fn page_keys(event: &StorageEvent) -> Vec<&[u8]> {
    match event {
        StorageEvent::IterResult { values, .. } => values.iter().map(|(key, _)| &key[..]).collect(),
        other => panic!("page expected, got {other:?}"),
    }
}

fn is_error(event: &StorageEvent, expected: StorageError) -> bool {
    matches!(event, StorageEvent::Error { error } if *error == expected)
}

async fn rows_and_batches(handle: &StorageHandle) -> Vec<String> {
    let mut session = Session::new(handle);
    session.send(put("a", "1", None)).await;
    session.send(put("b", "2", None)).await;
    assert_eq!(
        read_value(&session.send(get("a", None)).await),
        Some(&b"1"[..])
    );
    assert_eq!(read_value(&session.send(get("missing", None)).await), None);
    let reads = [(SPACE, "a"), (SPACE, "missing"), (OTHER, "a"), (SPACE, "b")];
    let reads = reads
        .map(|(space, key)| (space.to_string(), bytes(key)))
        .to_vec();
    session
        .send(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })
        .await;
    session.send(remove("a", None)).await;
    assert_eq!(read_value(&session.send(get("a", None)).await), None);
    let writes = vec![
        (SPACE.to_string(), bytes("c"), bytes("3")),
        (OTHER.to_string(), bytes("d"), bytes("4")),
    ];
    session
        .send(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await;
    let deletes = vec![
        (SPACE.to_string(), bytes("b")),
        (SPACE.to_string(), bytes("zz")),
    ];
    session
        .send(StorageEffect::BatchDelete {
            deletes,
            txn_id: None,
        })
        .await;
    assert_eq!(
        page_keys(&session.send(iter("", None, 10, None)).await),
        [b"c"]
    );
    session.send(StorageEffect::SyncAll).await;
    session.trace
}

#[tokio::test]
async fn rows_match_fjall() {
    parity(rows_and_batches).await;
}

async fn pages_and_cursors(handle: &StorageHandle) -> Vec<String> {
    let mut session = Session::new(handle);
    for key in ["p/1", "p/2", "p/3", "p/4", "p/5", "q/1", "r"] {
        session
            .send(put(key, &format!("value of {key}"), None))
            .await;
    }
    let edge = StorageEffect::Write {
        key_space: SPACE.to_string(),
        key: ByteView::from(&[0xff, 0xff][..]),
        value: bytes("edge"),
        txn_id: None,
    };
    session.send(edge).await;
    let after = |key: &str| Some(IterStart::After(bytes(key)));
    let at = |key: &str| Some(IterStart::At(bytes(key)));
    let first = session.send(iter("p/", None, 2, None)).await;
    assert_eq!(page_keys(&first), [b"p/1", b"p/2"]);
    session.send(iter("p/", after("p/2"), 2, None)).await;
    session.send(iter("p/", at("p/4"), 10, None)).await;
    session.send(iter("p/", after("a"), 10, None)).await;
    session.send(iter("p/", after("q"), 10, None)).await;
    session.send(iter("p/", after("p/5"), 10, None)).await;
    session.send(iter("", at("q/1"), 10, None)).await;
    session.send(iter("", None, 0, None)).await;
    session.send(iter("zz", None, 10, None)).await;
    let top = StorageEffect::Iter {
        key_space: SPACE.to_string(),
        prefix: Some(ByteView::from(&[0xff][..])),
        start: None,
        limit: 10,
        txn_id: None,
    };
    session.send(top).await;
    let (at, after) = (
        |key: &str| IterStart::At(bytes(key)),
        |key: &str| IterStart::After(bytes(key)),
    );
    let (before, through) = (
        |key: &str| IterEnd::Before(bytes(key)),
        |key: &str| IterEnd::Through(bytes(key)),
    );
    let wide = (100, usize::MAX);
    let page = session
        .send(range(at("p/1"), before("p/4"), wide, None))
        .await;
    assert_eq!(page_keys(&page), [b"p/1", b"p/2", b"p/3"]);
    session
        .send(range(after("p/1"), through("p/4"), wide, None))
        .await;
    session
        .send(range(at("p/1"), through("r"), (2, usize::MAX), None))
        .await;
    session
        .send(range(at("p/1"), through("r"), (100, 1), None))
        .await;
    session
        .send(range(at("p/1"), through("r"), (100, 30), None))
        .await;
    session
        .send(range(at("p/1"), through("r"), (100, 15), None))
        .await;
    session
        .send(range(at("p/3"), before("p/3"), wide, None))
        .await;
    session
        .send(range(at("p/3"), through("p/3"), wide, None))
        .await;
    session
        .send(range(after("p/3"), through("p/3"), wide, None))
        .await;
    session
        .send(range(after("p/4"), before("p/2"), wide, None))
        .await;
    session
        .send(range(at("p/1"), through("r"), (0, usize::MAX), None))
        .await;
    assert_eq!(page_keys(&session.send(last("p/", None)).await), [b"p/5"]);
    session.send(last("q", None)).await;
    session.send(last("", None)).await;
    session.send(last("zz", None)).await;
    session.trace
}

#[tokio::test]
async fn pages_match_fjall() {
    parity(pages_and_cursors).await;
}

async fn own_writes(handle: &StorageHandle) -> Vec<String> {
    let mut session = Session::new(handle);
    for key in ["p/1", "p/2", "p/3"] {
        session.send(put(key, "committed", None)).await;
    }
    let txn = session.begin(false).await;
    session.send(put("p/4", "new", Some(txn))).await;
    session.send(remove("p/2", Some(txn))).await;
    session.send(put("p/1", "changed", Some(txn))).await;
    let page = session.send(iter("p/", None, 10, Some(txn))).await;
    assert_eq!(page_keys(&page), [b"p/1", b"p/3", b"p/4"]);
    session
        .send(iter(
            "p/",
            Some(IterStart::After(bytes("p/1"))),
            1,
            Some(txn),
        ))
        .await;
    let wide = (100, usize::MAX);
    let start = IterStart::At(bytes("p/"));
    session
        .send(range(start, IterEnd::Before(bytes("q")), wide, Some(txn)))
        .await;
    assert_eq!(
        page_keys(&session.send(last("p/", Some(txn))).await),
        [b"p/4"]
    );
    session.send(remove("p/4", Some(txn))).await;
    assert_eq!(
        page_keys(&session.send(last("p/", Some(txn))).await),
        [b"p/3"]
    );
    assert_eq!(read_value(&session.send(get("p/2", Some(txn))).await), None);
    let reads = vec![
        (SPACE.to_string(), bytes("p/1")),
        (SPACE.to_string(), bytes("p/2")),
    ];
    let txn_id = Some(txn);
    session
        .send(StorageEffect::BatchRead { reads, txn_id })
        .await;
    let outside = session.send(iter("p/", None, 10, None)).await;
    assert_eq!(page_keys(&outside), [b"p/1", b"p/2", b"p/3"]);
    session.send(commit(txn)).await;
    session.send(iter("p/", None, 10, None)).await;
    session.trace
}

#[tokio::test]
async fn overlay_matches_fjall() {
    parity(own_writes).await;
}

async fn fixed_snapshots(handle: &StorageHandle) -> Vec<String> {
    let mut session = Session::new(handle);
    session.send(put("k", "v1", None)).await;
    let reader = session.begin(true).await;
    let writer = session.begin(false).await;
    session.send(get("k", Some(writer))).await;
    session.send(put("k", "v2", None)).await;
    let other = session.begin(false).await;
    session.send(put("k", "v3", Some(other))).await;
    session.send(put("n", "new", Some(other))).await;
    session.send(commit(other)).await;
    for txn_id in [Some(reader), Some(writer)] {
        assert_eq!(
            read_value(&session.send(get("k", txn_id)).await),
            Some(&b"v1"[..])
        );
        assert_eq!(
            page_keys(&session.send(iter("", None, 10, txn_id)).await),
            [b"k"]
        );
        session.send(last("", txn_id)).await;
    }
    assert_eq!(
        read_value(&session.send(get("k", None)).await),
        Some(&b"v3"[..])
    );
    session.send(put("w", "write", Some(reader))).await;
    session.send(commit(reader)).await;
    session.send(abort(writer)).await;
    session.send(get("k", Some(reader))).await;
    session.trace
}

#[tokio::test]
async fn snapshots_match_fjall() {
    parity(fixed_snapshots).await;
}

/// Runs a transaction that reads with `read`, lets `change` commit, then writes and commits.
async fn race(
    session: &mut Session<'_>,
    read: StorageEffect,
    change: StorageEffect,
) -> StorageEvent {
    let txn = session.begin(false).await;
    let mut read = read;
    if let Some(slot) = read.txn_slot() {
        *slot = Some(txn);
    }
    session.send(read).await;
    session.send(change).await;
    session.send(put("x", "loser", Some(txn))).await;
    session.send(commit(txn)).await
}

async fn read_conflicts(handle: &StorageHandle) -> Vec<String> {
    let mut session = Session::new(handle);
    session.send(put("k", "v", None)).await;
    let conflict = |event: StorageEvent| is_error(&event, StorageError::TransactionConflict);
    assert!(conflict(
        race(&mut session, get("k", None), put("k", "w", None)).await
    ));
    assert!(conflict(
        race(&mut session, get("k", None), remove("k", None)).await
    ));
    let phantom = put("p/new", "row", None);
    assert!(conflict(
        race(&mut session, iter("p/", None, 10, None), phantom).await
    ));
    assert!(!conflict(
        race(&mut session, get("a", None), put("b", "w", None)).await
    ));
    let wide = (100, usize::MAX);
    let interval = || {
        range(
            IterStart::At(bytes("m/1")),
            IterEnd::Before(bytes("m/3")),
            wide,
            None,
        )
    };
    assert!(!conflict(
        race(&mut session, interval(), put("m/5", "w", None)).await
    ));
    assert!(conflict(
        race(&mut session, interval(), put("m/2", "w", None)).await
    ));
    assert!(conflict(
        race(&mut session, last("l/", None), put("l/9", "w", None)).await
    ));
    let batch = StorageEffect::BatchRead {
        reads: vec![(SPACE.to_string(), bytes("z"))],
        txn_id: None,
    };
    assert!(conflict(
        race(&mut session, batch, put("z", "w", None)).await
    ));

    // A transaction that changed nothing commits without a conflict check.
    let quiet = session.begin(false).await;
    session.send(get("k", Some(quiet))).await;
    session.send(put("k", "again", None)).await;
    session.send(commit(quiet)).await;

    // Blind writes of one key never conflict; the last commit wins.
    let (first, second) = (session.begin(false).await, session.begin(false).await);
    session.send(put("w", "first", Some(first))).await;
    session.send(put("w", "second", Some(second))).await;
    session.send(commit(second)).await;
    session.send(commit(first)).await;
    assert_eq!(
        read_value(&session.send(get("w", None)).await),
        Some(&b"first"[..])
    );
    session.note(format!("pending {}", handle.pending_transactions()));
    session.trace
}

#[tokio::test]
async fn conflicts_match_fjall() {
    parity(read_conflicts).await;
}

async fn aborted_writes(handle: &StorageHandle) -> Vec<String> {
    let mut session = Session::new(handle);
    let txn = session.begin(false).await;
    session.send(put("a", "1", Some(txn))).await;
    session.send(abort(txn)).await;
    assert_eq!(read_value(&session.send(get("a", None)).await), None);
    session.send(abort(txn)).await;
    session.send(commit(txn)).await;
    session.send(get("a", Some(txn))).await;
    session.send(put("a", "2", Some(Ulid::nil()))).await;
    session.note(format!("pending {}", handle.pending_transactions()));

    let owner = handle.start_transaction(false).await.unwrap();
    let owned = owner.id();
    session.send(put("b", "1", owned)).await;
    drop(owner);
    // Fjall ends the dropped owner's cleanup entry on its worker thread.
    let ended = async || (handle.pending_transactions() == 0).then_some(());
    assert!(poll_until(Duration::from_secs(5), ended).await.is_some());
    session.send(get("b", None)).await;
    session.send(get("b", owned)).await;
    session.note(format!("pending {}", handle.pending_transactions()));
    session.trace
}

#[tokio::test]
async fn aborts_match_fjall() {
    parity(aborted_writes).await;
}

fn objects(objects: i128) -> UsageDelta {
    UsageDelta {
        objects,
        ..UsageDelta::default()
    }
}

fn add_usage(key: &str, delta: UsageDelta, txn_id: Ulid) -> StorageEffect {
    StorageEffect::AddUsage {
        key_space: USAGE.to_string(),
        deltas: vec![(bytes(key), delta)],
        buckets: vec![(bytes("bucket/a"), BucketDelta::default())],
        txn_id,
    }
}

fn add_counters(objects: i128, epoch: u64, txn_id: Ulid) -> StorageEffect {
    let delta = RangeCounters {
        objects,
        max_epoch: OriginalEpoch(epoch),
        ..RangeCounters::default()
    };
    StorageEffect::AddCounters {
        key_space: COUNTERS.to_string(),
        deltas: vec![(bytes("row"), delta)],
        txn_id,
    }
}

fn read_in(key_space: &str, key: &[u8], txn_id: Option<Ulid>) -> StorageEffect {
    StorageEffect::Read {
        key_space: key_space.to_string(),
        key: ByteView::from(key),
        txn_id,
    }
}

async fn counter_rows(handle: &StorageHandle) -> Vec<String> {
    let mut session = Session::new(handle);
    let first = session.begin(false).await;
    let second = session.begin(false).await;
    session.send(add_usage("group/a", objects(4), first)).await;
    session.send(add_usage("group/a", objects(1), first)).await;
    session.send(add_counters(3, 1, first)).await;
    session.send(add_counters(-5, 4, second)).await;
    session
        .send(add_usage("group/a", objects(-7), second))
        .await;
    session.send(commit(second)).await;
    session.send(commit(first)).await;
    session.send(read_in(USAGE, b"group/a", None)).await;
    session.send(read_in(USAGE, b"bucket/a", None)).await;
    session.send(read_in(COUNTERS, b"row", None)).await;

    let loser = session.begin(false).await;
    session.send(get("guard", Some(loser))).await;
    session.send(add_counters(7, 0, loser)).await;
    session.send(put("guard", "1", None)).await;
    session.send(commit(loser)).await;
    session.send(read_in(COUNTERS, b"row", None)).await;

    let reader = session.begin(true).await;
    session.send(add_counters(1, 0, reader)).await;
    session.send(add_counters(1, 0, Ulid::nil())).await;
    session.send(abort(reader)).await;
    session.trace
}

#[tokio::test]
async fn counters_match_fjall() {
    parity(counter_rows).await;
}

fn source_put(key: &str, value: &str, txn_id: Option<Ulid>) -> StorageEffect {
    StorageEffect::Write {
        key_space: S3_BUCKET_KEYSPACE.to_string(),
        key: bytes(key),
        value: bytes(value),
        txn_id,
    }
}

async fn usage_rebuild(handle: &StorageHandle) -> Vec<String> {
    let mut session = Session::new(handle);
    let setup = session.begin(false).await;
    session.send(add_usage("group/a", objects(4), setup)).await;
    session.send(commit(setup)).await;
    session.send(source_put("s/1", "a", None)).await;
    let rebuild = || StorageEffect::BeginUsageRebuild {
        key_space: USAGE.to_string(),
    };
    let view = match session.send(rebuild()).await {
        StorageEvent::TransactionStarted { txn_id } => txn_id,
        other => panic!("rebuild did not begin: {other:?}"),
    };
    let later = session.begin(false).await;
    session.send(add_usage("group/a", objects(-1), later)).await;
    session.send(commit(later)).await;
    session.send(source_put("s/1", "b", None)).await;
    session.send(source_put("s/2", "new", None)).await;
    let changer = session.begin(false).await;
    session.send(source_put("s/1", "c", Some(changer))).await;
    session.send(source_put("s/3", "txn", Some(changer))).await;
    session.send(commit(changer)).await;

    let source = |key: &str| read_in(S3_BUCKET_KEYSPACE, key.as_bytes(), Some(view));
    assert_eq!(
        read_value(&session.send(source("s/1")).await),
        Some(&b"a"[..])
    );
    assert_eq!(read_value(&session.send(source("s/2")).await), None);
    let reads = ["s/1", "s/2", "s/3"].map(|key| (S3_BUCKET_KEYSPACE.to_string(), bytes(key)));
    let txn_id = Some(view);
    session
        .send(StorageEffect::BatchRead {
            reads: reads.to_vec(),
            txn_id,
        })
        .await;
    let walk = |start: Option<IterStart>, limit| StorageEffect::Iter {
        key_space: S3_BUCKET_KEYSPACE.to_string(),
        prefix: Some(bytes("s/")),
        start,
        limit,
        txn_id,
    };
    let page = session.send(walk(None, 10)).await;
    assert_eq!(page_keys(&page), [b"s/1"]);
    session.send(walk(None, 0)).await;
    session
        .send(walk(Some(IterStart::At(bytes("s/1"))), 1))
        .await;
    session.send(read_in(USAGE, b"group/a", txn_id)).await;
    let range = StorageEffect::IterRange {
        key_space: USAGE.to_string(),
        start: IterStart::At(bytes("a")),
        end: IterEnd::Through(bytes("z")),
        limit: 10,
        max_bytes: usize::MAX,
        txn_id,
    };
    session.send(range).await;
    let mut tail = last("", txn_id);
    if let StorageEffect::Last { key_space, .. } = &mut tail {
        *key_space = USAGE.to_string();
    }
    session.send(tail).await;
    session
        .send(read_in(USAGE, &usage_shadow_key(b"group/a"), None))
        .await;
    session.send(read_in(USAGE, USAGE_REBUILD_KEY, None)).await;
    session.send(commit(view)).await;

    let finish = || StorageEffect::FinishUsageRebuild {
        key_space: USAGE.to_string(),
    };
    session.send(finish()).await;
    session.send(read_in(USAGE, b"group/a", None)).await;
    session.send(read_in(USAGE, USAGE_COMPLETE_KEY, None)).await;
    session.send(read_in(USAGE, USAGE_REBUILD_KEY, None)).await;
    session
        .send(read_in(USAGE, &usage_shadow_key(b"group/a"), None))
        .await;
    session.send(finish()).await;
    session.send(rebuild()).await;
    session.trace
}

#[tokio::test]
async fn rebuild_matches_fjall() {
    parity(usage_rebuild).await;
}

fn config_put(txn_id: Option<Ulid>) -> StorageEffect {
    StorageEffect::Write {
        key_space: REALM_CONFIG_KEYSPACE.to_string(),
        key: bytes("realm"),
        value: bytes("config"),
        txn_id,
    }
}

async fn config_counts(handle: &StorageHandle) -> Vec<String> {
    let mut session = Session::new(handle);
    session.send(config_put(None)).await;
    let changed = session.begin(false).await;
    session.send(config_put(Some(changed))).await;
    session.note(format!("before commit {}", handle.config_changes()));
    session.send(commit(changed)).await;
    let aborted = session.begin(false).await;
    session.send(config_put(Some(aborted))).await;
    session.send(abort(aborted)).await;
    let loser = session.begin(false).await;
    session.send(get("guard", Some(loser))).await;
    session.send(config_put(Some(loser))).await;
    session.send(put("guard", "1", None)).await;
    session.send(commit(loser)).await;
    let unrelated = session.begin(false).await;
    session.send(put("other", "1", Some(unrelated))).await;
    session.send(commit(unrelated)).await;
    let delete = StorageEffect::Delete {
        key_space: REALM_CONFIG_KEYSPACE.to_string(),
        key: bytes("realm"),
        txn_id: None,
    };
    session.send(delete).await;
    assert_eq!(handle.config_changes(), 3);
    session.note(format!("changes {}", handle.config_changes()));
    session.trace
}

#[tokio::test]
async fn config_matches_fjall() {
    parity(config_counts).await;
}

fn auth(key: &str, value: Option<&str>, txn_id: Option<Ulid>) -> StorageEffect {
    let (key_space, key) = (AUTH_KEYSPACE.to_string(), bytes(key));
    match value {
        Some(value) => StorageEffect::Write {
            key_space,
            key,
            value: bytes(value),
            txn_id,
        },
        None => StorageEffect::Read {
            key_space,
            key,
            txn_id,
        },
    }
}

/// Puts a value the store does not hold, so a read that returns it came from the cache.
fn plant(handle: &StorageHandle, key: &str, value: &str) {
    let cache = handle.cache.as_deref().expect("the handle keeps a cache");
    let row = (cached_slot(AUTH_KEYSPACE).unwrap(), bytes(key));
    cache.fill(cache.generation(), [(row, Some(bytes(value)))]);
}

async fn cached_reads(handle: &StorageHandle) -> Vec<String> {
    let mut session = Session::new(handle);
    session.send(auth("token", Some("stored"), None)).await;
    plant(handle, "token", "planted");
    let hit = session.send(auth("token", None, None)).await;
    assert_eq!(
        read_value(&hit),
        Some(&b"planted"[..]),
        "a hit skips the store"
    );
    session.send(auth("token", Some("written"), None)).await;
    session.send(auth("token", None, None)).await;
    plant(handle, "token", "planted");
    let txn = session.begin(false).await;
    let bypass = session.send(auth("token", None, Some(txn))).await;
    assert_eq!(
        read_value(&bypass),
        Some(&b"written"[..]),
        "transactions bypass the cache"
    );
    session
        .send(auth("token", Some("committed"), Some(txn)))
        .await;
    session.send(auth("token", None, None)).await;
    session.send(commit(txn)).await;
    let after = session.send(auth("token", None, None)).await;
    assert_eq!(
        read_value(&after),
        Some(&b"committed"[..]),
        "a commit drops the row"
    );
    plant(handle, "token", "planted");
    let delete = StorageEffect::Delete {
        key_space: AUTH_KEYSPACE.to_string(),
        key: bytes("token"),
        txn_id: None,
    };
    session.send(delete).await;
    session.send(auth("token", None, None)).await;
    plant(handle, "other", "planted");
    let reads = ["token", "other"].map(|key| (AUTH_KEYSPACE.to_string(), bytes(key)));
    session
        .send(StorageEffect::BatchRead {
            reads: reads.to_vec(),
            txn_id: None,
        })
        .await;
    session.trace
}

#[tokio::test]
async fn cache_matches_fjall() {
    parity(cached_reads).await;
}

const CANARY: &[u8] = b"canary-0b6e";

async fn sealed_rows(handle: &StorageHandle) -> Vec<String> {
    let mut session = Session::new(handle);
    let entry = VaultEntry::new(VaultPurpose::NamedSecret, Ulid::from_bytes([7; 16]));
    let vault_read = || StorageEffect::VaultRead {
        entry,
        txn_id: None,
    };
    session.send(vault_read()).await;
    handle.open_vault(NodeVaultKey::derive(&[3; 32]));
    let secret = SharedSecret::new(SecretBytes::new(CANARY.to_vec()));
    let txn_id = None;
    session
        .send(StorageEffect::VaultWrite {
            entry,
            secret,
            txn_id,
        })
        .await;
    match session.send(vault_read()).await {
        StorageEvent::VaultResult {
            secret: Some(secret),
            ..
        } => {
            assert_eq!(secret.expose(), CANARY);
        }
        other => panic!("vault result expected, got {other:?}"),
    }
    session
        .send(StorageEffect::VaultDelete { entry, txn_id })
        .await;
    session.send(vault_read()).await;

    handle.seal_secrets(CredentialEncryptionKey::derive(&[3; 32]));
    let row = |value: Option<&[u8]>| match value {
        Some(value) => StorageEffect::Write {
            key_space: CONNECTOR_SECRET_KEYSPACE.to_string(),
            key: bytes("row-a"),
            value: ByteView::from(value),
            txn_id,
        },
        None => read_in(CONNECTOR_SECRET_KEYSPACE, b"row-a", txn_id),
    };
    session.send(row(Some(CANARY))).await;
    assert_eq!(read_value(&session.send(row(None)).await), Some(CANARY));
    let compiled = session
        .send(StorageEffect::CompilePolicies(vec![Vec::new()]))
        .await;
    assert!(matches!(compiled, StorageEvent::PoliciesCompiled(_)));
    session.trace
}

#[tokio::test]
async fn sealing_matches_fjall() {
    parity(sealed_rows).await;
}

#[tokio::test]
async fn memory_rows_sealed() {
    let handle = MemoryStorage::open();
    handle.seal_secrets(CredentialEncryptionKey::derive(&[3; 32]));
    let write = StorageEffect::Write {
        key_space: CONNECTOR_SECRET_KEYSPACE.to_string(),
        key: bytes("row-a"),
        value: ByteView::from(CANARY),
        txn_id: None,
    };
    handle.send_storage_effect(write).await;
    let memory = handle.memory.as_deref().expect("a memory handle");
    let raw = memory.run(read_in(CONNECTOR_SECRET_KEYSPACE, b"row-a", None));
    let stored = read_value(&raw).expect("the row is stored");
    assert!(!stored.windows(CANARY.len()).any(|window| window == CANARY));
}

async fn closed_writes(handle: &StorageHandle) -> Vec<String> {
    let mut session = Session::new(handle);
    session.send(put("a", "1", None)).await;
    let open = session.begin(false).await;
    session.send(put("b", "1", Some(open))).await;
    handle.fence_mutations();
    session.send(put("c", "1", Some(open))).await;
    session.send(get("b", Some(open))).await;
    session.send(put("d", "1", None)).await;
    session.send(get("a", None)).await;
    session.note(format!("pending {}", handle.pending_transactions()));
    handle.close_writes();
    session.send(put("e", "1", None)).await;
    session
        .send(StorageEffect::StartTransaction { read: false })
        .await;
    let reader = session.begin(true).await;
    session.send(get("a", Some(reader))).await;
    session.send(abort(reader)).await;
    session.send(StorageEffect::SyncAll).await;
    assert!(handle.sync_all().await.is_ok());
    let metrics = handle.snapshot_metrics();
    session.note(format!(
        "rejected {} closed {}",
        metrics.rejected_writes, metrics.closed
    ));
    session.note(format!("pending {}", handle.pending_transactions()));
    session.trace
}

#[tokio::test]
async fn fences_match_fjall() {
    parity(closed_writes).await;
}

#[tokio::test]
async fn memory_close_returns() {
    let handle = MemoryStorage::open();
    handle.send_storage_effect(put("a", "1", None)).await;
    assert_eq!(handle.in_flight(), 0);
    assert!(handle.accepts_writes().await);
    assert!(handle.database().is_none());
    handle.close().await;
}
