use super::*;

impl DocumentSyncService {
    pub(super) async fn storage_read(
        &self,
        key_space: String,
        key: ByteView,
    ) -> Result<Option<Value>> {
        storage_read_from(&self.storage, key_space, key).await
    }

    pub(super) async fn storage_write(
        &self,
        key_space: String,
        key: ByteView,
        value: Value,
    ) -> Result<()> {
        match self
            .storage
            .send_storage_effect(StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
            Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
            other => Err(NetError::Dht(format!(
                "unexpected storage event while applying document sync write: {other:?}"
            ))),
        }
    }

    pub(super) async fn storage_batch_write(
        &self,
        writes: Vec<(String, ByteView, Value)>,
    ) -> Result<()> {
        storage_batch_write_to(&self.storage, writes).await
    }
}

pub(super) async fn abort_txn(storage: &StorageHandle, txn_id: TxnId) -> Result<()> {
    match storage
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionAborted { txn_id: aborted })
            if aborted == txn_id =>
        {
            Ok(())
        }
        // A conflicted commit consumes the transaction; the abort's goal is met.
        Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionNotFound,
        }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(NetError::Dht(format!(
            "unexpected storage event while aborting transaction: {other:?}"
        ))),
    }
}

pub(super) async fn abort_error(
    storage: &StorageHandle,
    txn_id: TxnId,
    error: NetError,
) -> NetError {
    match abort_txn(storage, txn_id).await {
        Ok(()) => error,
        Err(abort) => NetError::Dht(format!("{error}; transaction abort failed: {abort}")),
    }
}

pub(super) async fn storage_read_from(
    storage: &StorageHandle,
    key_space: String,
    key: ByteView,
) -> Result<Option<Value>> {
    storage_read_from_transaction(storage, key_space, key, None).await
}

pub(super) async fn storage_read_from_transaction(
    storage: &StorageHandle,
    key_space: String,
    key: ByteView,
    txn_id: Option<TxnId>,
) -> Result<Option<Value>> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space,
            key,
            txn_id,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(NetError::Dht(format!(
            "unexpected storage event while applying document sync read: {other:?}"
        ))),
    }
}

pub(super) async fn start_storage_transaction(storage: &StorageHandle) -> Result<TxnId> {
    match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => Ok(txn_id),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(NetError::Dht(format!(
            "unexpected storage event while starting document sync transaction: {other:?}"
        ))),
    }
}

pub(super) async fn storage_batch_write_to(
    storage: &StorageHandle,
    writes: Vec<(String, ByteView, Value)>,
) -> Result<()> {
    match storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(NetError::Dht(format!(
            "unexpected storage event while applying document sync batch write: {other:?}"
        ))),
    }
}

pub(super) async fn storage_batch_delete_and_write_transactionally(
    storage: &StorageHandle,
    deletes: Vec<(String, ByteView)>,
    writes: Vec<(String, ByteView, Value)>,
) -> Result<()> {
    // Same conflict discipline as the realm-config applies: a transient SSI
    // conflict must never abort inbound stream processing, or ops land
    // without meta and the topic wedges.
    let mut last = None;
    for _ in 0..APPLY_CONFLICT_ATTEMPTS {
        tokio::task::yield_now().await;
        let txn_id = start_storage_transaction(storage).await?;
        match storage_batch_delete_and_write_in_transaction(
            storage,
            txn_id,
            deletes.clone(),
            writes.clone(),
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(NetError::Storage(StorageError::TransactionConflict)) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                last = Some(NetError::Storage(StorageError::TransactionConflict));
            }
            Err(error) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(error);
            }
        }
    }
    Err(last.unwrap_or_else(|| NetError::Dht("apply conflict retries exhausted".to_string())))
}

pub(super) async fn storage_batch_delete_and_write_in_transaction(
    storage: &StorageHandle,
    txn_id: TxnId,
    deletes: Vec<(String, ByteView)>,
    writes: Vec<(String, ByteView, Value)>,
) -> Result<()> {
    if !deletes.is_empty() {
        match storage
            .send_storage_effect(StorageEffect::BatchDelete {
                deletes,
                txn_id: Some(txn_id),
            })
            .await
        {
            Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {}
            Event::Storage(StorageEvent::Error { error }) => {
                return Err(error.into());
            }
            other => {
                return Err(NetError::Dht(format!(
                    "unexpected storage event while applying document sync transactional batch delete: {other:?}"
                )));
            }
        }
    }

    match storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
        Event::Storage(StorageEvent::Error { error }) => {
            return Err(error.into());
        }
        other => {
            return Err(NetError::Dht(format!(
                "unexpected storage event while applying document sync transactional batch write: {other:?}"
            )));
        }
    }

    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(NetError::Dht(format!(
            "unexpected storage event while committing document sync apply transaction: {other:?}"
        ))),
    }
}

#[cfg(test)]
pub(super) async fn storage_batch_delete_to(
    storage: &StorageHandle,
    deletes: Vec<(String, ByteView)>,
) -> Result<()> {
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
            "unexpected storage event while applying document sync batch delete: {other:?}"
        ))),
    }
}
