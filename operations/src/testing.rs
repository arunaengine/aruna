//! Builds storage-only driver contexts, raw storage reads and writes, and signed tokens for tests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::structs::identity::auth::TokenClaims;
use aruna_storage::StorageHandle;
use byteview::ByteView;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};

use crate::driver::DriverContext;

/// A driver context with storage and no other plane.
pub fn test_context(storage: StorageHandle) -> DriverContext {
    DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    }
}

/// Writes `writes` in one batch outside a transaction; panics on any other answer.
pub async fn write_entries(storage: &StorageHandle, writes: Vec<(String, ByteView, ByteView)>) {
    match storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
        other => panic!("unexpected storage event: {other:?}"),
    }
}

/// The stored value of `key`; panics on any answer but a read result.
pub async fn read_value(storage: &StorageHandle, keyspace: &str, key: Vec<u8>) -> Option<ByteView> {
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = storage
        .send_storage_effect(StorageEffect::Read {
            key_space: keyspace.to_string(),
            key: key.into(),
            txn_id: None,
        })
        .await
    else {
        panic!("unexpected storage read event")
    };
    value
}

/// An EdDSA bearer token of `claims` signed with `signing_key`.
pub fn sign_token(signing_key: &SigningKey, claims: &TokenClaims) -> String {
    let key_pem = signing_key.to_pkcs8_pem(LineEnding::LF).unwrap();
    encode(
        &Header::new(Algorithm::EdDSA),
        claims,
        &EncodingKey::from_ed_pem(key_pem.as_bytes()).unwrap(),
    )
    .unwrap()
}

/// Replays the whole event log into the registry projection.
pub async fn replay_event_log(
    context: &DriverContext,
) -> Result<usize, crate::metadata::projection::MetadataProjectionError> {
    use crate::metadata::projection::{ReplayPageOperation, max_clock_skew};
    let local_node = context.net_handle.as_ref().map(|net| net.node_id());
    let mut after = None;
    let mut projected = 0usize;
    loop {
        let page = ReplayPageOperation::new(after, local_node, max_clock_skew());
        let (count, next) = crate::driver::drive(page, context).await?;
        projected = projected.saturating_add(count);
        match next {
            Some(next) => after = Some(next),
            None => return Ok(projected),
        }
    }
}

/// Runs one pass of the pending projection drain.
pub async fn drain_projection_queue(
    context: &DriverContext,
) -> Result<
    crate::metadata::projection::PendingProjectionResult,
    crate::metadata::projection::MetadataProjectionError,
> {
    use crate::metadata::projection::{DrainProjectionOperation, max_clock_skew};
    let local_node = context.net_handle.as_ref().map(|net| net.node_id());
    let drain = DrainProjectionOperation::new(local_node, max_clock_skew());
    crate::driver::drive(drain, context).await
}

/// Projects carried events.
pub async fn project_create_events(
    context: &DriverContext,
    events: Vec<aruna_core::metadata::MetadataEventRecord>,
    local_node_id: Option<aruna_core::NodeId>,
) -> Result<usize, crate::metadata::projection::MetadataProjectionError> {
    use crate::metadata::projection::{ProjectEventsOperation, max_clock_skew};
    let project = ProjectEventsOperation::new(events, local_node_id, max_clock_skew());
    crate::driver::drive(project, context).await
}

/// Projects logged events by their document and event ids.
pub async fn project_logged_events(
    context: &DriverContext,
    targets: impl IntoIterator<Item = (ulid::Ulid, ulid::Ulid)>,
) -> Result<usize, crate::metadata::projection::MetadataProjectionError> {
    use crate::metadata::projection::{ProjectLoggedOperation, max_clock_skew};
    let local_node = context.net_handle.as_ref().map(|net| net.node_id());
    let project = ProjectLoggedOperation::new(targets, false, local_node, max_clock_skew());
    Ok(crate::driver::drive(project, context).await?.projected)
}

/// Projects one logged event.
pub async fn project_logged_event(
    context: &DriverContext,
    document_id: ulid::Ulid,
    event_id: ulid::Ulid,
) -> Result<(), crate::metadata::projection::MetadataProjectionError> {
    project_logged_events(context, [(document_id, event_id)])
        .await
        .map(|_| ())
}

/// Wakes the projection drain after `after`.
pub async fn schedule_projection_drain(
    context: &DriverContext,
    after: std::time::Duration,
) -> Result<(), crate::metadata::projection::MetadataProjectionError> {
    let wake = crate::metadata::projection::ProjectionWakeOperation::new(after);
    crate::driver::drive(wake, context).await
}
