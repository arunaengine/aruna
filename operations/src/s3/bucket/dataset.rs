//! Reads the current metadata of a local bucket and the description its dataset gives it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::StorageEffect;
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BUCKET_CREATION_KEYSPACE, BUCKET_METADATA_KEYSPACE};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{BucketInfo, incarnation};
use aruna_core::structs::storage::bucket_metadata::{BucketMetadata, StoredMetadata};
use aruna_core::structs::storage::dataset_description::{DatasetDescription, describe};
use thiserror::Error;
use ulid::Ulid;

use crate::driver::DriverContext;
use crate::metadata::api::MetadataApiError;
use crate::metadata::api::export_types::{
    ExportMetadataRequest, ExportMetadataResult, RoCrateExportView,
};
use crate::metadata::forward::read::export_rocrate_routed;
use aruna_core::metadata::AuthToken;

/// The current metadata of a local bucket: its distributed set's settings, or the row of this
/// bucket's current incarnation. `None` when there is none.
pub async fn bucket_metadata(
    context: &DriverContext,
    bucket: &str,
    info: &BucketInfo,
) -> Result<Option<BucketMetadata>, String> {
    if let Some(binding) = &info.distribution {
        return crate::distribution::change::set_metadata(context, binding.set_id).await;
    }
    let key: aruna_core::types::Key = bucket.as_bytes().to_vec().into();
    let reads = vec![
        (BUCKET_METADATA_KEYSPACE.to_string(), key.clone()),
        (BUCKET_CREATION_KEYSPACE.to_string(), key),
    ];
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })
        .await;
    let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
        return Err(format!("bucket metadata read failed: {}", event.label()));
    };
    let mut values = values.into_iter().map(|(_, value)| value);
    let (row, creation) = (values.next().flatten(), values.next().flatten());
    current_row(creation.as_ref(), row.as_ref()).map_err(|error| error.to_string())
}

/// The metadata a stored row gives the bucket's current incarnation.
pub(super) fn current_row(
    creation: Option<&aruna_core::types::Value>,
    row: Option<&aruna_core::types::Value>,
) -> Result<Option<BucketMetadata>, ConversionError> {
    let Some(row) = row else {
        return Ok(None);
    };
    let stored = StoredMetadata::from_bytes(row.as_ref())?;
    let current = incarnation(creation.map(|value| value.as_ref()));
    Ok((stored.incarnation == current).then_some(stored.metadata))
}

/// Why a bucket's dataset gives no description.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum DescriptionError {
    #[error("the dataset does not exist or the caller may not read it")]
    Unreadable,
    #[error("{0}")]
    Invalid(String),
    #[error("the dataset could not be read now")]
    Unavailable,
}

/// Bytes the summary of a dataset may have; only its root entity is read.
const SUMMARY_BYTES: u64 = 16 * 1024 * 1024;

/// What the dataset `dataset` says about a bucket, read from its holders with the caller's own
/// access, so a reader never sees a description it may not read.
pub async fn dataset_description(
    context: &DriverContext,
    realm_id: RealmId,
    dataset: Ulid,
    (auth, token): (Option<&AuthContext>, Option<AuthToken>),
) -> Result<DatasetDescription, DescriptionError> {
    let request = ExportMetadataRequest {
        document_id: dataset,
        auth: auth.cloned(),
        view: RoCrateExportView::Summary,
        limit: None,
        offset: None,
        after: None,
    };
    let context = std::sync::Arc::new(context.clone());
    match export_rocrate_routed(&context, realm_id, request, token, SUMMARY_BYTES).await {
        Ok(ExportMetadataResult::Summary { jsonld, .. }) => {
            describe(&jsonld).map_err(DescriptionError::Invalid)
        }
        Err(
            MetadataApiError::NotFound
            | MetadataApiError::Forbidden
            | MetadataApiError::Unauthorized
            | MetadataApiError::BadRequest,
        ) => Err(DescriptionError::Unreadable),
        Ok(_) | Err(_) => Err(DescriptionError::Unavailable),
    }
}
