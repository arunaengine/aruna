use aruna_core::effects::StorageEffect;
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_storage::StorageHandle;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

#[derive(Debug, Error)]
pub(super) enum MetadataQueueStorageError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("unexpected storage event while processing metadata queue: {0}")]
    UnexpectedEvent(String),
}

pub(super) async fn start_write_transaction(
    storage: &StorageHandle,
) -> Result<Ulid, MetadataQueueStorageError> {
    match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => Ok(txn_id),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataQueueStorageError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

pub(super) async fn commit_storage_transaction(
    storage: &StorageHandle,
    txn_id: Ulid,
) -> Result<(), MetadataQueueStorageError> {
    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataQueueStorageError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

pub(super) async fn abort_storage_transaction_best_effort(
    storage: &StorageHandle,
    txn_id: Ulid,
    storage_error_message: &'static str,
    unexpected_event_message: &'static str,
) {
    match storage
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionAborted { .. }) => {}
        Event::Storage(StorageEvent::Error { error }) => {
            warn!(error = %error, txn_id = %txn_id, "{}", storage_error_message);
        }
        other => {
            warn!(event = ?other, txn_id = %txn_id, "{}", unexpected_event_message);
        }
    }
}
