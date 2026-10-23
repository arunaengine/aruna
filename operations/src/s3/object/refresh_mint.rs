//! Records a source change a refresh confirmed as a successor of the current reference version.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::SystemTime;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::structs::storage::blob::{
    BlobHeadKey, BlobVersion, BlobVersionState, BucketInfo, CurrentVersionPointer, VersionKey,
};
use aruna_core::structs::storage::usage::UsageDelta;
use aruna_core::types::{Key, TxnId, Value};
use byteview::ByteView;
use ulid::Ulid;

use crate::connectors::repository::connector_reference_write;
use crate::driver::DriverContext;
use crate::node::usage_stats::UsageCounterUpdate;
use crate::replication::protocol::ReferenceAdvance;
use crate::replication::queue::{LiveObligationRecord, live_obligation_entry};
use crate::s3::object::get::MAX_AUTO_ADVANCES;
use crate::s3::object::metadata::ReferenceRefresh;

/// Writes, in `txn_id`, the successor carrying the refresh's observation when the refreshed
/// version is still the head. `false` when nothing was written.
pub(crate) async fn mint_successor(
    context: &DriverContext,
    txn_id: TxnId,
    refresh: &ReferenceRefresh,
    predecessor: &BlobVersion,
) -> Result<bool, String> {
    let BlobVersionState::Reference {
        source,
        advance_count,
        ..
    } = &predecessor.state
    else {
        return Ok(false);
    };
    if *advance_count >= MAX_AUTO_ADVANCES {
        return Ok(false);
    }
    let head_key = BlobHeadKey::new(&refresh.bucket, &refresh.key)
        .to_bytes()
        .map_err(|error| error.to_string())?;
    let pointer = read_row(context, txn_id, BLOB_HEAD_KEYSPACE, head_key.clone().into())
        .await?
        .map(|value| CurrentVersionPointer::from_bytes(value.as_ref()))
        .transpose()
        .map_err(|error| error.to_string())?;
    let Some(pointer) = pointer.filter(|pointer| pointer.version_id == refresh.version_id) else {
        return Ok(false);
    };
    let bucket_key = ByteView::from(refresh.bucket.as_bytes().to_vec());
    let Some(bucket) = read_row(context, txn_id, S3_BUCKET_KEYSPACE, bucket_key)
        .await?
        .map(|value| BucketInfo::from_bytes(value.as_ref()))
        .transpose()
        .map_err(|error| error.to_string())?
    else {
        return Ok(false);
    };

    let now = SystemTime::now();
    let version_id = Ulid::generate();
    let successor = BlobVersion::reference(
        source.clone(),
        refresh.metadata.clone(),
        now,
        predecessor.created_by,
        now,
    )
    .with_metadata(predecessor.metadata.clone())
    .with_advance_count(advance_count + 1)
    .with_policies(predecessor.placement_policies.clone())
    .map_err(|error| error.to_string())?;
    let version_key = VersionKey::new(&refresh.bucket, &refresh.key, version_id);
    let next = CurrentVersionPointer::next_for(Some(&pointer), version_id)
        .map_err(|error| error.to_string())?;
    let obligation = LiveObligationRecord::new(
        refresh.node_id,
        refresh.auth_context.clone(),
        refresh.bucket.clone(),
        refresh.key.clone(),
        version_id,
        false,
    )
    .with_reference_advance(ReferenceAdvance {
        generation: next.generation,
        predecessor: pointer.version_id,
    });
    let encode = |error: aruna_core::errors::ConversionError| error.to_string();
    let mut writes = vec![
        (
            BLOB_VERSIONS_KEYSPACE.to_string(),
            version_key.to_bytes().map_err(encode)?.into(),
            successor.to_bytes().map_err(encode)?.into(),
        ),
        (
            BLOB_HEAD_KEYSPACE.to_string(),
            head_key.into(),
            next.to_bytes().map_err(encode)?.into(),
        ),
        live_obligation_entry(&obligation).map_err(encode)?,
    ];
    writes.extend(connector_reference_write(&successor, &version_key).map_err(encode)?);
    match send(
        context,
        StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        },
    )
    .await?
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
        other => return Err(format!("unexpected successor write event: {other:?}")),
    }

    // The successor books its own size; an offered directory observation is released.
    let released = predecessor
        .offered_size(&refresh.bucket, &refresh.key)
        .unwrap_or(0);
    let delta = UsageDelta {
        referenced_bytes: i128::from(refresh.metadata.content_length) - i128::from(released),
        ..Default::default()
    };
    let mut update = UsageCounterUpdate::for_group(bucket.group_id, delta);
    if !update.is_noop() {
        let mut effects = update.start(txn_id);
        while let Some(Effect::Storage(effect)) = effects.pop() {
            let event = send(context, effect).await?;
            match update
                .step(event, txn_id)
                .map_err(|error| error.to_string())?
            {
                Some(next) => effects = next,
                None => break,
            }
        }
    }
    Ok(true)
}

async fn read_row(
    context: &DriverContext,
    txn_id: TxnId,
    key_space: &str,
    key: Key,
) -> Result<Option<Value>, String> {
    let read = StorageEffect::Read {
        key_space: key_space.to_string(),
        key,
        txn_id: Some(txn_id),
    };
    match send(context, read).await? {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value),
        other => Err(format!("unexpected read event: {other:?}")),
    }
}

async fn send(context: &DriverContext, effect: StorageEffect) -> Result<Event, String> {
    match context.storage_handle.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        event => Ok(event),
    }
}
