//! Runs the parking of distribution snapshots whose publisher the realm config does not know
//! yet. The retention, capacity, retry and authority decisions are the pure rules in core.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::keyspaces::{DISTRIBUTION_PARKED_KEYSPACE, PARKED_INDEX_KEYSPACE};
use aruna_core::structs::storage::distribution::parking::{
    MAX_PARKED, ParkDecision, ParkedEntry, RetryDecision, park, retry,
};
use aruna_core::structs::storage::distribution::{SignedSnapshot, publisher_key};

/// Whether a snapshot was handled, or must be revisited because the parking bound is reached.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::document_sync) enum Handled {
    Done,
    Full,
}

/// Merges the snapshot into its publisher and set's parked entry in one transaction.
pub(in crate::document_sync) async fn park_snapshot(
    storage: &StorageHandle,
    event: &AdminDocumentEvent,
    signed: &SignedSnapshot,
) -> Result<Handled> {
    let snapshot = &signed.snapshot;
    let key: ByteView = publisher_key(&snapshot.root.publisher, snapshot.set_id()).into();
    for _ in 0..APPLY_CONFLICT_ATTEMPTS {
        tokio::task::yield_now().await;
        let txn_id = start_storage_transaction(storage).await?;
        let decided = match parking_inputs(storage, &key, txn_id).await {
            Ok((existing, count)) => park(existing.as_ref(), count, event.clone())
                .map_err(|error| NetError::Bootstrap(error.to_string())),
            Err(error) => Err(error),
        };
        let entry = match decided {
            Ok(ParkDecision::Store(entry)) => entry,
            Ok(ParkDecision::Unchanged) => {
                return abort_txn(storage, txn_id).await.map(|()| Handled::Done);
            }
            Ok(ParkDecision::Full) => {
                warn!(set_id = %snapshot.set_id(), "Parked distribution snapshots reached their bound");
                return abort_txn(storage, txn_id).await.map(|()| Handled::Full);
            }
            Err(error) => return Err(abort_error(storage, txn_id, error).await),
        };
        let body = postcard::to_allocvec(&*entry)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        let writes = vec![
            (
                PARKED_INDEX_KEYSPACE.to_string(),
                key.clone(),
                entry.number().to_be_bytes().to_vec().into(),
            ),
            (
                DISTRIBUTION_PARKED_KEYSPACE.to_string(),
                key.clone(),
                body.into(),
            ),
        ];
        match replace_batch_in(storage, txn_id, Vec::new(), writes).await {
            Ok(()) => return Ok(Handled::Done),
            Err(NetError::Storage(StorageError::TransactionConflict)) => {
                let _ = abort_txn(storage, txn_id).await;
            }
            Err(error) => {
                let _ = abort_txn(storage, txn_id).await;
                return Err(error);
            }
        }
    }
    Err(NetError::Dht(
        "parking a distribution snapshot exhausted its conflict retries".to_string(),
    ))
}

/// Retries every parked entry of the realm against one read of the realm config.
pub(in crate::document_sync) async fn retry_parked(
    storage: &StorageHandle,
    realm_id: RealmId,
) -> Result<()> {
    let membership = Membership::read(storage, realm_id).await?;
    for (key, _) in parked_rows(storage, None).await? {
        let entry = storage_read_from(
            storage,
            DISTRIBUTION_PARKED_KEYSPACE.to_string(),
            key.clone(),
        )
        .await?
        .and_then(|bytes| postcard::from_bytes::<ParkedEntry>(&bytes).ok());
        let Some(entry) = entry else {
            unpark(storage, key).await?;
            continue;
        };
        let AdminDocumentTarget::Distribution {
            realm_id: event_realm,
            set_id,
        } = entry.head.target
        else {
            unpark(storage, key).await?;
            continue;
        };
        if event_realm != realm_id {
            continue;
        }
        match retry(&entry, |signed| membership.authority(signed)) {
            RetryDecision::Wait => continue,
            RetryDecision::Drop(reason) => {
                warn!(%set_id, %reason, "Dropping a parked distribution snapshot");
            }
            RetryDecision::Install(events) => {
                // A conflict installs after its head, so the view keeps the equivocation.
                for event in &events {
                    install_snapshot(storage, event, set_id).await?;
                }
            }
        }
        unpark(storage, key).await?;
    }
    Ok(())
}

/// The parked entry of one key and the number of parked keys, read in the transaction.
async fn parking_inputs(
    storage: &StorageHandle,
    key: &ByteView,
    txn_id: TxnId,
) -> Result<(Option<ParkedEntry>, usize)> {
    let existing = transaction_read(
        storage,
        DISTRIBUTION_PARKED_KEYSPACE.to_string(),
        key.clone(),
        Some(txn_id),
    )
    .await?
    .and_then(|bytes| postcard::from_bytes::<ParkedEntry>(&bytes).ok());
    let count = parked_rows(storage, Some(txn_id)).await?.len();
    Ok((existing, count))
}

async fn parked_rows(
    storage: &StorageHandle,
    txn_id: Option<TxnId>,
) -> Result<Vec<(ByteView, Value)>> {
    match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: PARKED_INDEX_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: MAX_PARKED,
            txn_id,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => Ok(values),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(NetError::Dht(format!(
            "unexpected storage event while listing parked snapshots: {other:?}"
        ))),
    }
}

async fn unpark(storage: &StorageHandle, key: ByteView) -> Result<()> {
    let deletes = vec![
        (PARKED_INDEX_KEYSPACE.to_string(), key.clone()),
        (DISTRIBUTION_PARKED_KEYSPACE.to_string(), key),
    ];
    match storage
        .send_storage_effect(StorageEffect::BatchDelete {
            deletes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(NetError::Dht(format!(
            "unexpected storage event while unparking a snapshot: {other:?}"
        ))),
    }
}
