use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::PERSISTENT_ID_MAPPING_KEYSPACE;
use aruna_core::structs::{PersistentIdMapping, persistent_id_key};
use aruna_core::types::TxnId;
use byteview::ByteView;
use thiserror::Error;
use ulid::Ulid;

use crate::driver::DriverContext;

#[derive(Debug, Error, PartialEq)]
pub enum PersistentIdError {
    #[error(transparent)]
    Storage(StorageError),
    #[error(transparent)]
    Conversion(ConversionError),
}

pub fn read_mapping_effect(document_id: Ulid, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: PERSISTENT_ID_MAPPING_KEYSPACE.to_string(),
        key: ByteView::from(persistent_id_key(document_id)),
        txn_id,
    })
}

pub fn write_mapping_effect(
    mapping: &PersistentIdMapping,
    txn_id: Option<TxnId>,
) -> Result<Effect, ConversionError> {
    Ok(Effect::Storage(StorageEffect::Write {
        key_space: PERSISTENT_ID_MAPPING_KEYSPACE.to_string(),
        key: ByteView::from(persistent_id_key(mapping.target)),
        value: mapping.to_bytes()?.into(),
        txn_id,
    }))
}

pub fn parse_mapping_read(event: Event) -> Result<Option<PersistentIdMapping>, PersistentIdError> {
    match event {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|bytes| {
                PersistentIdMapping::from_bytes(bytes.as_ref())
                    .map_err(PersistentIdError::Conversion)
            })
            .transpose(),
        Event::Storage(StorageEvent::Error { error }) => Err(PersistentIdError::Storage(error)),
        _ => Err(PersistentIdError::Storage(StorageError::ReadError)),
    }
}

pub async fn read_mapping(
    ctx: &DriverContext,
    document_id: Ulid,
) -> Result<Option<PersistentIdMapping>, PersistentIdError> {
    let event = ctx
        .storage_handle
        .send_effect(read_mapping_effect(document_id, None))
        .await;
    parse_mapping_read(event)
}

async fn write_mapping(
    ctx: &DriverContext,
    mapping: &PersistentIdMapping,
) -> Result<(), PersistentIdError> {
    let effect = write_mapping_effect(mapping, None).map_err(PersistentIdError::Conversion)?;
    match ctx.storage_handle.send_effect(effect).await {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(PersistentIdError::Storage(error)),
        _ => Err(PersistentIdError::Storage(StorageError::ReadError)),
    }
}

/// Flip an Active PID mapping for a document to Withdrawn.
///
/// A no-op when the document was never minted (returns `false`) or is already
/// withdrawn. This upholds the persistence guarantee: a minted PID becomes a
/// permanent 410 tombstone, never a 404 or a reused id. Best-effort at the delete
/// site; the landing route also treats an Active mapping whose target is gone as
/// withdrawn, so a lost write here still cannot 404 a minted PID.
pub async fn withdraw_persistent_id(
    ctx: &DriverContext,
    document_id: Ulid,
    withdrawn_at_ms: u64,
) -> Result<bool, PersistentIdError> {
    let Some(mut mapping) = read_mapping(ctx, document_id).await? else {
        return Ok(false);
    };
    if !mapping.is_active() {
        return Ok(false);
    }
    mapping.withdraw(withdrawn_at_ms);
    write_mapping(ctx, &mapping).await?;
    Ok(true)
}

/// Register a Conceptual PID for a document, idempotently. Returns the mapping and
/// whether it was newly created.
pub async fn mint_persistent_id(
    ctx: &DriverContext,
    document_id: Ulid,
    minted_by: aruna_core::types::UserId,
    minted_at_ms: u64,
) -> Result<(PersistentIdMapping, bool), PersistentIdError> {
    if let Some(existing) = read_mapping(ctx, document_id).await? {
        return Ok((existing, false));
    }
    let mapping = PersistentIdMapping::conceptual(document_id, minted_at_ms, minted_by);
    write_mapping(ctx, &mapping).await?;
    Ok((mapping, true))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::{PersistentIdStatus, RealmId};
    use aruna_storage::storage;
    use tempfile::tempdir;

    fn context() -> (DriverContext, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let context = DriverContext {
            storage_handle: storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        (context, dir)
    }

    fn user() -> aruna_core::types::UserId {
        aruna_core::types::UserId::local(Ulid::from_bytes([2; 16]), RealmId([3; 32]))
    }

    #[tokio::test]
    async fn mint_is_idempotent() {
        let (ctx, _dir) = context();
        let id = Ulid::from_bytes([5; 16]);
        let (first, minted_first) = mint_persistent_id(&ctx, id, user(), 1).await.unwrap();
        assert!(minted_first);
        let (second, minted_second) = mint_persistent_id(&ctx, id, user(), 9).await.unwrap();
        assert!(!minted_second);
        assert_eq!(first.pid, second.pid);
        assert_eq!(second.minted_at_ms, 1);
    }

    #[tokio::test]
    async fn withdraw_flips_active_and_noops_when_unminted() {
        let (ctx, _dir) = context();
        let id = Ulid::from_bytes([6; 16]);
        assert!(!withdraw_persistent_id(&ctx, id, 10).await.unwrap());

        mint_persistent_id(&ctx, id, user(), 1).await.unwrap();
        assert!(withdraw_persistent_id(&ctx, id, 10).await.unwrap());
        let mapping = read_mapping(&ctx, id).await.unwrap().unwrap();
        assert_eq!(mapping.status, PersistentIdStatus::Withdrawn);
        assert!(!withdraw_persistent_id(&ctx, id, 20).await.unwrap());
    }
}
