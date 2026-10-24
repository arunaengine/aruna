//! Keeps distribution snapshots whose publisher the realm config does not know yet: bounded,
//! durable, the highest revision per publisher and set, retried after realm config changes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::keyspaces::{DISTRIBUTION_PARKED_KEYSPACE, PARKED_INDEX_KEYSPACE};
use aruna_core::structs::storage::distribution::{SignedSnapshot, publisher_key};

/// At most this many snapshots wait for their publisher's membership.
pub(in crate::document_sync) const MAX_PARKED: usize = 256;

/// Keeps the snapshot unless a higher revision of its publisher and set already waits.
/// When the bound is reached a new publisher and set waits for the next delivery instead.
pub(in crate::document_sync) async fn park_snapshot(
    storage: &StorageHandle,
    event: &AdminDocumentEvent,
    signed: &SignedSnapshot,
) -> Result<()> {
    let snapshot = &signed.snapshot;
    let key: ByteView = publisher_key(&snapshot.root.publisher, snapshot.set_id()).into();
    let body =
        postcard::to_allocvec(event).map_err(|error| NetError::Bootstrap(error.to_string()))?;
    for _ in 0..APPLY_CONFLICT_ATTEMPTS {
        tokio::task::yield_now().await;
        let txn_id = start_storage_transaction(storage).await?;
        let parked = match parked_number(storage, &key, txn_id).await {
            Ok(parked) => parked,
            Err(error) => return Err(abort_error(storage, txn_id, error).await),
        };
        if parked.is_some_and(|number| number >= snapshot.number) {
            return abort_txn(storage, txn_id).await;
        }
        if parked.is_none() {
            let rows = match parked_rows(storage, Some(txn_id)).await {
                Ok(rows) => rows,
                Err(error) => return Err(abort_error(storage, txn_id, error).await),
            };
            if rows.len() >= MAX_PARKED {
                warn!(set_id = %snapshot.set_id(), "Parked distribution snapshots reached their bound");
                return abort_txn(storage, txn_id).await;
            }
        }
        let writes = vec![
            (
                PARKED_INDEX_KEYSPACE.to_string(),
                key.clone(),
                snapshot.number.to_be_bytes().to_vec().into(),
            ),
            (
                DISTRIBUTION_PARKED_KEYSPACE.to_string(),
                key.clone(),
                body.clone().into(),
            ),
        ];
        match replace_batch_in(storage, txn_id, Vec::new(), writes).await {
            Ok(()) => return Ok(()),
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

/// Retries every parked snapshot of the realm against the current realm config: installs
/// the ones whose publisher may publish them, drops the ones without authority.
pub(in crate::document_sync) async fn retry_parked(
    storage: &StorageHandle,
    realm_id: RealmId,
) -> Result<()> {
    for (key, _) in parked_rows(storage, None).await? {
        let event = storage_read_from(
            storage,
            DISTRIBUTION_PARKED_KEYSPACE.to_string(),
            key.clone(),
        )
        .await?
        .and_then(|bytes| postcard::from_bytes::<AdminDocumentEvent>(&bytes).ok());
        let parked = event
            .as_ref()
            .and_then(|event| match (&event.target, &event.op) {
                (
                    AdminDocumentTarget::Distribution {
                        realm_id: event_realm,
                        set_id,
                    },
                    AdminDocumentOperation::DistributionPublished { snapshot },
                ) => Some((*event_realm, *set_id, snapshot)),
                _ => None,
            });
        let Some((event_realm, set_id, snapshot)) = parked else {
            unpark(storage, key).await?;
            continue;
        };
        if event_realm != realm_id {
            continue;
        }
        match publisher_authority(storage, realm_id, snapshot).await? {
            Authority::Unknown => continue,
            Authority::Ended(reason) => {
                warn!(%set_id, %reason, "Dropping a parked distribution snapshot");
            }
            Authority::Current => {
                if let Some(event) = event.as_ref() {
                    install_snapshot(storage, event, set_id).await?;
                }
            }
        }
        unpark(storage, key).await?;
    }
    Ok(())
}

async fn parked_number(
    storage: &StorageHandle,
    key: &ByteView,
    txn_id: TxnId,
) -> Result<Option<u64>> {
    let value = transaction_read(
        storage,
        PARKED_INDEX_KEYSPACE.to_string(),
        key.clone(),
        Some(txn_id),
    )
    .await?;
    Ok(value
        .and_then(|value| <[u8; 8]>::try_from(value.as_ref()).ok())
        .map(u64::from_be_bytes))
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
