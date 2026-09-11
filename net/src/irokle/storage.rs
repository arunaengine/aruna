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
