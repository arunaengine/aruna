//! Tests that a backlog larger than a reference page arrives exactly once, in head order, and
//! that a lost answer is replayed without counting twice.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Mutex;
use std::time::SystemTime;

use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, REPLICATION_JOB_KEYSPACE,
    REPLICATION_OBLIGATION_KEYSPACE, S3_BUCKET_KEYSPACE, USAGE_STATS_KEYSPACE,
};
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::blob::{
    BlobHeadKey, BlobVersion, BucketInfo, CurrentVersionPointer, VersionKey,
};
use aruna_core::structs::storage::distribution::{DistributionSnapshot, OriginalEpoch};
use aruna_core::structs::storage::usage::{BucketCounters, usage_bucket_key};
use aruna_core::types::{Key, Value};
use aruna_storage::StorageHandle;
use tempfile::TempDir;
use ulid::Ulid;

use super::*;
use crate::distribution::test_record::{binding, realm, replicated, secret, store_record};
use crate::distribution::test_store::{context, install, open};
use crate::replication::page::apply::tests::source;
use crate::replication::queue::{LiveObligationRecord, drain_batch, live_obligation_entry};

struct Node {
    _dir: TempDir,
    handle: StorageHandle,
}

/// A node holding the set's record and its member bucket of `seed`.
async fn node(snapshot: &DistributionSnapshot, bytes: &[u8], seed: u8) -> Node {
    let (dir, handle) = open();
    store_record(&handle, snapshot.set_id(), bytes.to_vec()).await;
    install(&handle, snapshot.set_id(), &snapshot.config.ranges).await;
    let bucket = BucketInfo {
        group_id: snapshot.config.owner_group,
        created_at: SystemTime::UNIX_EPOCH,
        created_by: Default::default(),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Default::default(),
        distribution: Some(binding(snapshot, seed, 5)),
    };
    let row = (
        S3_BUCKET_KEYSPACE.to_string(),
        Key::from(b"spire".to_vec()),
        Value::from(bucket.to_bytes().unwrap()),
    );
    write(&handle, vec![row]).await;
    Node { _dir: dir, handle }
}

async fn write(handle: &StorageHandle, writes: Vec<(String, Key, Value)>) {
    for chunk in writes.chunks(2048) {
        let write = StorageEffect::BatchWrite {
            writes: chunk.to_vec(),
            txn_id: None,
        };
        let event = handle.send_storage_effect(write).await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::BatchWriteResult { .. })
        ));
    }
}

/// Every row of a keyspace.
async fn rows(handle: &StorageHandle, key_space: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    let (mut all, mut start) = (Vec::new(), None);
    loop {
        let iter = StorageEffect::Iter {
            key_space: key_space.to_string(),
            prefix: None,
            start: start
                .take()
                .map(|key: Vec<u8>| IterStart::After(key.into())),
            limit: 4096,
            txn_id: None,
        };
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = handle.send_storage_effect(iter).await
        else {
            panic!("iteration failed");
        };
        all.extend(values.into_iter().map(|(k, v)| (k.to_vec(), v.to_vec())));
        match next_start_after {
            Some(next) => start = Some(next.to_vec()),
            None => return all,
        }
    }
}

/// A reference version of `key` committed by the coordinator at `generation`, with its head
/// when `head` and the obligation an import writes with it.
fn committed(key: &str, id: u128, generation: u64, head: bool) -> Vec<(String, Key, Value)> {
    let version_id = Ulid::from_parts(1_700_000_000_000, id);
    let metadata = SourceMetadata {
        content_length: 10,
        content_type: None,
        etag: Some(format!("etag-{key}-{id}")),
        last_modified: None,
        source_version: None,
        digests: Default::default(),
    };
    let created = SystemTime::UNIX_EPOCH;
    let version =
        BlobVersion::reference(source(key), metadata, created, Default::default(), created)
            .with_epoch(OriginalEpoch(0))
            .with_generation(generation);
    let version_key = VersionKey::new("spire", key, version_id)
        .to_bytes()
        .unwrap();
    let mut rows = vec![(
        BLOB_VERSIONS_KEYSPACE.to_string(),
        version_key.into(),
        version.to_bytes().unwrap().into(),
    )];
    if head {
        let pointer = CurrentVersionPointer::new_with_generation(version_id, generation);
        rows.push((
            BLOB_HEAD_KEYSPACE.to_string(),
            BlobHeadKey::new("spire", key).to_bytes().unwrap().into(),
            pointer.to_bytes().unwrap().into(),
        ));
    }
    let obligation = LiveObligationRecord::new(
        secret(1).public(),
        AuthContext::anonymous(realm()),
        "spire".to_string(),
        key.to_string(),
        version_id,
        false,
    );
    rows.push(live_obligation_entry(&obligation).unwrap());
    rows
}

/// Applies every page on the member; the first `lost` answers are lost after the commit.
struct Member<'a> {
    handle: &'a StorageHandle,
    lost: Mutex<usize>,
    pages: Mutex<usize>,
}

impl PageSender for Member<'_> {
    async fn send(&self, _node: NodeId, page: ReferencePage) -> Result<PageAnswer, String> {
        *self.pages.lock().unwrap() += 1;
        let input = ApplyInput {
            page,
            publisher: secret(1).public(),
            realm_id: realm(),
        };
        let answer =
            page_answer(drive(PageApplyOperation::new(input), &context(self.handle)).await);
        let mut lost = self.lost.lock().unwrap();
        if *lost > 0 {
            *lost -= 1;
            return Err("the answer was lost".to_string());
        }
        Ok(answer)
    }
}

async fn counters(handle: &StorageHandle) -> BucketCounters {
    let read = StorageEffect::Read {
        key_space: USAGE_STATS_KEYSPACE.to_string(),
        key: usage_bucket_key("spire").into(),
        txn_id: None,
    };
    match handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|value| BucketCounters::from_bytes(&value).unwrap())
            .unwrap_or_default(),
        other => panic!("unexpected read {other:?}"),
    }
}

#[tokio::test]
async fn backlog_arrives_once() {
    // More obligations than one page reads become jobs and pages until none is owed, and the
    // member stores and counts every record exactly once.
    let (snapshot, bytes) = replicated(true, 1);
    let coordinator = node(&snapshot, &bytes, 1).await;
    let member = node(&snapshot, &bytes, 2).await;
    let count = PAGE_RECORDS + 900;
    let mut writes = Vec::new();
    for index in 0..count {
        writes.extend(committed(&format!("a/{index}"), index as u128 + 1, 1, true));
    }
    // Two versions of one key: the newer one holds the head on both sides.
    writes.extend(committed("b", 900_001, 1, false));
    writes.extend(committed("b", 900_002, 2, true));
    write(&coordinator.handle, writes).await;
    let sender = Member {
        handle: &member.handle,
        lost: Mutex::new(0),
        pages: Mutex::new(0),
    };
    let context = context(&coordinator.handle);
    for _ in 0..16 {
        let result = drain_batch(&context, &sender).await.unwrap();
        let owed = rows(&coordinator.handle, REPLICATION_JOB_KEYSPACE)
            .await
            .len()
            + rows(&coordinator.handle, REPLICATION_OBLIGATION_KEYSPACE)
                .await
                .len();
        if owed == 0 && !result.has_more_due {
            break;
        }
    }
    assert!(
        rows(&coordinator.handle, REPLICATION_JOB_KEYSPACE)
            .await
            .is_empty()
    );
    assert!(
        rows(&coordinator.handle, REPLICATION_OBLIGATION_KEYSPACE)
            .await
            .is_empty()
    );
    let stored = rows(&member.handle, BLOB_VERSIONS_KEYSPACE).await.len();
    assert_eq!(stored, count + 2, "every record arrived");
    let counted = counters(&member.handle).await;
    assert_eq!(
        counted.versions,
        count as u64 + 2,
        "no record was counted twice"
    );
    assert_eq!(counted.objects, count as u64 + 1);
    let head = BlobHeadKey::new("spire", "b").to_bytes().unwrap();
    let heads = rows(&member.handle, BLOB_HEAD_KEYSPACE).await;
    let (_, pointer) = heads.iter().find(|(key, _)| *key == head).unwrap();
    let pointer = CurrentVersionPointer::from_bytes(pointer).unwrap();
    assert_eq!(
        pointer.version_id,
        Ulid::from_parts(1_700_000_000_000, 900_002)
    );
    let pages = *sender.pages.lock().unwrap();
    assert!(pages <= 4, "records travel in pages, {pages} sent");
}

#[tokio::test]
async fn lost_answer_replays() {
    // A page committed on the member whose answer was lost is sent again: the member answers
    // from what it stored, and the coordinator retires the jobs only then.
    let (snapshot, bytes) = replicated(true, 1);
    let coordinator = node(&snapshot, &bytes, 1).await;
    let member = node(&snapshot, &bytes, 2).await;
    let mut writes = Vec::new();
    for index in 0..40u128 {
        writes.extend(committed(&format!("c/{index}"), index + 1, 1, true));
    }
    write(&coordinator.handle, writes).await;
    let sender = Member {
        handle: &member.handle,
        lost: Mutex::new(1),
        pages: Mutex::new(0),
    };
    let context = context(&coordinator.handle);
    let result = drain_batch(&context, &sender).await.unwrap();
    assert_eq!(result.failed, 40, "the unanswered page is kept");
    let jobs = rows(&coordinator.handle, REPLICATION_JOB_KEYSPACE).await;
    assert_eq!(jobs.len(), 40, "every job waits for its acknowledgement");
    let counted = counters(&member.handle).await;
    assert_eq!(counted.versions, 40, "the member committed the page");
    // The retry is due later; it is sent as soon as it is due.
    let jobs = (jobs.into_iter())
        .map(|(key, value)| (key, BlobJobRecord::from_bytes(&value).unwrap()))
        .collect();
    let report = deliver_pages(&context, jobs, &sender).await.unwrap();
    assert_eq!(report.delivered, 40);
    assert!(
        rows(&coordinator.handle, REPLICATION_JOB_KEYSPACE)
            .await
            .is_empty()
    );
    assert_eq!(
        counters(&member.handle).await,
        counted,
        "the replay counts nothing"
    );
}
