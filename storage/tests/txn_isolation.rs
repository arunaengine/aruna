//! A transaction that stays open across many other snapshots must still see
//! its commit refused when a concurrent transaction changed what it read.
//! fjall 3.1.8 closed a committing transaction's snapshot twice, so a snapshot
//! sharing its sequence number counted as closed, the GC watermark passed it
//! and the conflict records it needed were pruned: the stale write won.
use aruna_core::effects::StorageEffect;
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::types::{Key, TxnId};
use aruna_storage::{FjallStorage, StorageHandle};

const KEYSPACE: &str = "admin_document_state";

async fn start_txn(storage: &StorageHandle, read: bool) -> TxnId {
    match storage
        .send_storage_effect(StorageEffect::StartTransaction { read })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        other => panic!("unexpected start event: {other:?}"),
    }
}

async fn write(storage: &StorageHandle, key: &str, value: &str, txn_id: Option<TxnId>) {
    let event = storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes: vec![(
                KEYSPACE.to_string(),
                Key::from(key.as_bytes().to_vec()),
                value.as_bytes().to_vec().into(),
            )],
            txn_id,
        })
        .await;
    assert!(
        matches!(event, Event::Storage(StorageEvent::BatchWriteResult { .. })),
        "unexpected write event: {event:?}"
    );
}

async fn read(storage: &StorageHandle, key: &str, txn_id: Option<TxnId>) -> Option<Vec<u8>> {
    match storage
        .send_storage_effect(StorageEffect::BatchRead {
            reads: vec![(KEYSPACE.to_string(), Key::from(key.as_bytes().to_vec()))],
            txn_id,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchReadResult { values }) => values
            .into_iter()
            .next()
            .and_then(|(_, value)| value.map(|bytes| bytes.as_ref().to_vec())),
        other => panic!("unexpected read event: {other:?}"),
    }
}

async fn commit(storage: &StorageHandle, txn_id: TxnId) -> Event {
    storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
}

fn committed(event: &Event) -> bool {
    matches!(
        event,
        Event::Storage(StorageEvent::TransactionCommitted { .. })
    )
}

#[tokio::test]
async fn long_txn_conflicts() {
    let dir = tempfile::tempdir().expect("temp dir");
    let storage = FjallStorage::open_test(dir.path().to_str().expect("path")).expect("storage");
    write(&storage, "state", "v0", None).await;

    let long = start_txn(&storage, false).await;
    assert_eq!(
        read(&storage, "state", Some(long)).await.as_deref(),
        Some(b"v0".as_slice())
    );
    // A transaction opened at the same sequence number, committed while the
    // long one is still open.
    let sibling = start_txn(&storage, false).await;
    write(&storage, "sibling", "s", Some(sibling)).await;
    assert!(committed(&commit(&storage, sibling).await));

    let other = start_txn(&storage, false).await;
    assert_eq!(
        read(&storage, "state", Some(other)).await.as_deref(),
        Some(b"v0".as_slice())
    );
    write(&storage, "state", "other", Some(other)).await;
    assert!(committed(&commit(&storage, other).await));

    // Enough unrelated traffic for the snapshot tracker to run its periodic
    // garbage collection, followed by commits that prune the oracle's records.
    for index in 0..50u32 {
        write(&storage, &format!("noise{index}"), "n", None).await;
    }
    for _ in 0..10_050u32 {
        let snapshot = start_txn(&storage, true).await;
        storage
            .send_storage_effect(StorageEffect::AbortTransaction { txn_id: snapshot })
            .await;
    }
    for index in 0..5u32 {
        write(&storage, &format!("late{index}"), "l", None).await;
    }

    write(&storage, "state", "long", Some(long)).await;
    let outcome = commit(&storage, long).await;
    assert!(
        matches!(
            outcome,
            Event::Storage(StorageEvent::Error {
                error: StorageError::TransactionConflict
            })
        ),
        "the stale transaction must not commit: {outcome:?}"
    );
    assert_eq!(
        read(&storage, "state", None).await.as_deref(),
        Some(b"other".as_slice())
    );
}
