use crate::driver::{DriverContext, drive};
use crate::staging::descriptor::build_version_source_binding;
use crate::staging::head_source::{
    HeadStagingSourceError, HeadStagingSourceInput, HeadStagingSourceOperation,
};
use aruna_core::effects::StorageEffect;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{S3_CURRENT_VERSION_KEYSPACE, S3_VERSION_KEYSPACE};
use aruna_core::structs::{
    CurrentVersionPointer, RealmId, SourceConnector, SourceMetadata, StagingStrategy, VersionKey,
    VersionMetadata, VersionSourceBinding,
};
use aruna_core::types::{GroupId, NodeId, UserId};
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializeReferenceInput {
    pub group_id: GroupId,
    pub user_id: UserId,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub connector_id: Ulid,
    pub source_path: String,
    pub bucket: String,
    pub key: String,
}

#[derive(Debug, PartialEq)]
pub struct MaterializeReferenceResult {
    pub connector: SourceConnector,
    pub source_metadata: SourceMetadata,
    pub version_source: VersionSourceBinding,
    pub version_id: Ulid,
}

#[derive(Debug, Error, PartialEq)]
pub enum MaterializeReferenceError {
    #[error(transparent)]
    Head(#[from] HeadStagingSourceError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
}

pub async fn materialize_reference(
    context: &DriverContext,
    input: MaterializeReferenceInput,
) -> Result<MaterializeReferenceResult, MaterializeReferenceError> {
    let head_result = drive(
        HeadStagingSourceOperation::new(HeadStagingSourceInput {
            group_id: input.group_id,
            connector_id: input.connector_id,
            source_path: input.source_path.clone(),
        }),
        context,
    )
    .await?;

    let version_source = build_version_source_binding(
        StagingStrategy::Reference,
        &head_result.connector,
        &head_result.metadata,
        input.source_path,
        Some(input.node_id),
        Some(head_result.connector.connector_id),
    );
    let version_id = Ulid::new();
    let now = SystemTime::now();

    let txn_id = match context
        .storage_handle
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        _ => return Err(StorageError::WriteError.into()),
    };

    let result: Result<(), MaterializeReferenceError> = async {
        let current_key = LookupKey::object(&input.bucket, &input.key).to_bytes()?;
        let existing_pointer = match context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: S3_CURRENT_VERSION_KEYSPACE.to_string(),
                key: current_key.clone().into(),
                txn_id: Some(txn_id),
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value
                .as_ref()
                .map(|value| CurrentVersionPointer::from_bytes(value.as_ref()))
                .transpose()?,
            Event::Storage(StorageEvent::Error { error }) => {
                return Err(MaterializeReferenceError::Storage(error));
            }
            _ => return Err(MaterializeReferenceError::Storage(StorageError::ReadError)),
        };
        match context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: S3_CURRENT_VERSION_KEYSPACE.to_string(),
                key: current_key.into(),
                value: CurrentVersionPointer::next_for(existing_pointer.as_ref(), version_id)
                    .to_bytes()?
                    .into(),
                txn_id: Some(txn_id),
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            Event::Storage(StorageEvent::Error { error }) => {
                return Err(MaterializeReferenceError::Storage(error));
            }
            _ => return Err(MaterializeReferenceError::Storage(StorageError::WriteError)),
        }

        let version_key = VersionKey::new(&input.bucket, &input.key, version_id).to_bytes()?;
        let version_value = VersionMetadata::reference(
            version_id,
            version_source.clone(),
            head_result.metadata.clone(),
            now,
            input.user_id,
            now,
        )
        .to_bytes()?;

        match context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: S3_VERSION_KEYSPACE.to_string(),
                key: version_key.into(),
                value: version_value.into(),
                txn_id: Some(txn_id),
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            Event::Storage(StorageEvent::Error { error }) => {
                return Err(MaterializeReferenceError::Storage(error));
            }
            _ => return Err(MaterializeReferenceError::Storage(StorageError::WriteError)),
        }

        match context
            .storage_handle
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await
        {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
            Event::Storage(StorageEvent::Error { error }) => {
                Err(MaterializeReferenceError::Storage(error))
            }
            _ => Err(MaterializeReferenceError::Storage(StorageError::WriteError)),
        }
    }
    .await;

    if result.is_err() {
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
            .await;
    }

    result?;

    Ok(MaterializeReferenceResult {
        connector: head_result.connector,
        source_metadata: head_result.metadata,
        version_source,
        version_id,
    })
}

use aruna_core::structs::LookupKey;
