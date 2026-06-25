use std::sync::Arc;
use std::time::SystemTime;

use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::BLOB_VERSIONS_KEYSPACE;
use aruna_core::structs::{BlobVersion, BlobVersionState, SourceMetadata, VersionKey};
use ulid::Ulid;

use crate::driver::DriverContext;

#[derive(Debug)]
pub struct ReferenceMetadataRefresh {
    pub bucket: String,
    pub key: String,
    pub version_id: Ulid,
    pub metadata: SourceMetadata,
    pub refreshed_at: SystemTime,
}

pub async fn refresh_reference_metadata(
    context: Arc<DriverContext>,
    refresh: ReferenceMetadataRefresh,
) -> Result<(), String> {
    let version_key = VersionKey::new(&refresh.bucket, &refresh.key, refresh.version_id)
        .to_bytes()
        .map_err(|err| err.to_string())?;
    let txn_id = match context
        .storage_handle
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.to_string()),
        other => return Err(format!("unexpected start transaction event: {other:?}")),
    };

    let refreshed_value = match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: version_key.clone().into(),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => {
            let version = match BlobVersion::from_bytes(value.as_ref()) {
                Ok(version) => version,
                Err(err) => {
                    abort_reference_refresh(&context, txn_id).await;
                    return Err(err.to_string());
                }
            };
            let BlobVersion {
                created_at,
                created_by,
                state,
            } = version;
            let BlobVersionState::Reference {
                source,
                last_refresh,
                ..
            } = state
            else {
                abort_reference_refresh(&context, txn_id).await;
                return Ok(());
            };
            if refresh.refreshed_at <= last_refresh {
                None
            } else {
                Some(
                    match BlobVersion::reference(
                        source,
                        refresh.metadata,
                        created_at,
                        created_by,
                        refresh.refreshed_at,
                    )
                    .to_bytes()
                    {
                        Ok(value) => value,
                        Err(err) => {
                            abort_reference_refresh(&context, txn_id).await;
                            return Err(err.to_string());
                        }
                    },
                )
            }
        }
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
            abort_reference_refresh(&context, txn_id).await;
            return Ok(());
        }
        Event::Storage(StorageEvent::Error { error }) => {
            abort_reference_refresh(&context, txn_id).await;
            return Err(error.to_string());
        }
        other => {
            abort_reference_refresh(&context, txn_id).await;
            return Err(format!("unexpected version read event: {other:?}"));
        }
    };

    if let Some(refreshed_value) = refreshed_value {
        match context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: version_key.into(),
                value: refreshed_value.into(),
                txn_id: Some(txn_id),
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            Event::Storage(StorageEvent::Error { error }) => {
                abort_reference_refresh(&context, txn_id).await;
                return Err(error.to_string());
            }
            other => {
                abort_reference_refresh(&context, txn_id).await;
                return Err(format!("unexpected version write event: {other:?}"));
            }
        }
    }

    match context
        .storage_handle
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => {
            abort_reference_refresh(&context, txn_id).await;
            Err(error.to_string())
        }
        other => {
            abort_reference_refresh(&context, txn_id).await;
            Err(format!("unexpected commit event: {other:?}"))
        }
    }
}

async fn abort_reference_refresh(context: &DriverContext, txn_id: Ulid) {
    let _ = context
        .storage_handle
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await;
}
