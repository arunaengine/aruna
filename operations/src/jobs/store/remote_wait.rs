//! Remote waits on this node's bucket keys and the wakes owed to the waiting nodes.
//! A wake row stays until the waiting node acknowledges it; delivery may repeat.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::encryption::BucketKeyRef;

use super::*;

/// Prefix of a registration: key reference, waiting node, job.
const REMOTE_PREFIX: u8 = b'r';
/// Prefix of an owed wake: waiting node, job, key reference.
const DELIVERY_PREFIX: u8 = b'd';

fn registration_key(key: BucketKeyRef, waiter: NodeId, job_id: JobId) -> Key {
    ByteView::from(
        [
            &[REMOTE_PREFIX][..],
            &key.key(),
            waiter.as_bytes(),
            &job_id.to_bytes(),
        ]
        .concat(),
    )
}

fn delivery_key(waiter: NodeId, job_id: JobId, key: BucketKeyRef) -> Key {
    ByteView::from(
        [
            &[DELIVERY_PREFIX][..],
            waiter.as_bytes(),
            &job_id.to_bytes(),
            &key.key(),
        ]
        .concat(),
    )
}

/// Waiting node, job and key of a delivery row.
fn parse_delivery(row: &[u8]) -> Option<(NodeId, JobId, BucketKeyRef)> {
    let rest = row.strip_prefix(&[DELIVERY_PREFIX])?;
    let (node, rest) = rest.split_first_chunk::<32>()?;
    let (job, rest) = rest.split_first_chunk::<16>()?;
    let waiter = NodeId::from_bytes(node).ok()?;
    let key = BucketKeyRef::from_key(rest).ok()?;
    Some((waiter, JobId::from_bytes(*job), key))
}

/// Records that `waiter` parks `job_id` on `key`. The caller checks the registry afterwards and
/// answers "already available" when the key is unlocked, so a racing unlock is never missed.
pub async fn register_remote_wait(
    storage: &StorageHandle,
    key: BucketKeyRef,
    waiter: NodeId,
    job_id: JobId,
) -> Result<(), String> {
    let row = registration_key(key, waiter, job_id);
    batch_write(
        storage,
        vec![(JOB_KEY_WAIT_KEYSPACE.to_string(), row, empty_value())],
        None,
    )
    .await
}

/// Turns one page of registrations on `key` into owed wakes, atomically per page.
/// Reports whether a further page may exist.
pub async fn queue_remote_wakes(
    storage: &StorageHandle,
    key: BucketKeyRef,
) -> Result<bool, JobMutationError> {
    let prefix = ByteView::from([&[REMOTE_PREFIX][..], &key.key()].concat());
    for attempt in 0..MUTATE_MAX_ATTEMPTS {
        let txn_id = start_write_txn(storage)
            .await
            .map_err(JobMutationError::Storage)?;
        let page = iter_prefix_page(
            storage,
            JOB_KEY_WAIT_KEYSPACE,
            Some(prefix.clone()),
            None,
            WAKE_PAGE,
            Some(txn_id),
        )
        .await;
        let rows = match page {
            Ok((rows, _)) => rows,
            Err(error) => {
                abort_txn(storage, txn_id).await;
                return Err(JobMutationError::Storage(error));
            }
        };
        let mut writes = Vec::new();
        let mut deletes = Vec::new();
        for (row, _) in &rows {
            let rest = &row[prefix.len()..];
            let parsed = rest.split_first_chunk::<32>().and_then(|(node, job)| {
                let job: [u8; 16] = job.try_into().ok()?;
                Some((NodeId::from_bytes(node).ok()?, JobId::from_bytes(job)))
            });
            if let Some((waiter, job_id)) = parsed {
                let owed = delivery_key(waiter, job_id, key);
                writes.push((JOB_KEY_WAIT_KEYSPACE.to_string(), owed, empty_value()));
            }
            deletes.push((JOB_KEY_WAIT_KEYSPACE.to_string(), row.clone()));
        }
        let written = match batch_write(storage, writes, Some(txn_id)).await {
            Ok(()) => batch_delete(storage, deletes, Some(txn_id)).await,
            Err(error) => Err(error),
        };
        if let Err(error) = written {
            abort_txn(storage, txn_id).await;
            return Err(JobMutationError::Storage(error));
        }
        match commit_write(storage, txn_id, attempt, "remote wake exhausted retries").await? {
            CommitStep::Committed => return Ok(rows.len() == WAKE_PAGE),
            CommitStep::Retry => continue,
        }
    }
    Err(JobMutationError::Storage(
        "remote wake exhausted retries".to_string(),
    ))
}

/// One page of owed wakes, oldest waiting node first.
pub async fn owed_wakes(
    storage: &StorageHandle,
) -> Result<Vec<(NodeId, JobId, BucketKeyRef)>, String> {
    let prefix = ByteView::from(vec![DELIVERY_PREFIX]);
    let (rows, _) = iter_prefix_page(
        storage,
        JOB_KEY_WAIT_KEYSPACE,
        Some(prefix),
        None,
        WAKE_PAGE,
        None,
    )
    .await?;
    Ok(rows
        .iter()
        .filter_map(|(row, _)| parse_delivery(row))
        .collect())
}

/// Drops an owed wake once the waiting node acknowledged it.
pub async fn ack_wake(
    storage: &StorageHandle,
    waiter: NodeId,
    job_id: JobId,
    key: BucketKeyRef,
) -> Result<(), String> {
    let row = delivery_key(waiter, job_id, key);
    batch_delete(
        storage,
        vec![(JOB_KEY_WAIT_KEYSPACE.to_string(), row)],
        None,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_storage::FjallStorage;

    #[tokio::test]
    async fn wake_owed_until_ack() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let waiter = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let key = BucketKeyRef::new(Ulid::from_bytes([8; 16]), 1);
        let other = BucketKeyRef::new(Ulid::from_bytes([8; 16]), 2);
        let job_id = JobId::from_bytes([5; 16]);
        register_remote_wait(&storage, key, waiter, job_id)
            .await
            .unwrap();
        register_remote_wait(&storage, other, waiter, job_id)
            .await
            .unwrap();

        assert!(!queue_remote_wakes(&storage, key).await.unwrap());
        assert_eq!(
            owed_wakes(&storage).await.unwrap(),
            vec![(waiter, job_id, key)]
        );
        // A repeated unlock finds nothing left to queue for this key.
        assert!(!queue_remote_wakes(&storage, key).await.unwrap());
        assert_eq!(owed_wakes(&storage).await.unwrap().len(), 1);

        ack_wake(&storage, waiter, job_id, key).await.unwrap();
        assert!(owed_wakes(&storage).await.unwrap().is_empty());
    }
}
