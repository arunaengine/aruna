//! Builds storage-only driver contexts, raw storage reads and writes, and signed tokens for tests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::document::{DocumentOutboxRecord, DocumentReconcileResult};
use aruna_core::document_apply::ApplyError;
use aruna_core::document_apply::reconcile::ReconcileTopicsOperation;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::row_plan::PlanOperation;
use aruna_core::structs::identity::auth::TokenClaims;
use aruna_core::structs::identity::realm::RealmId;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};

use crate::driver::{DriverContext, drive};
use crate::sync::document_outbox::{
    OutboxPagePlan, OutboxReadBatch, OutboxTailsOperation, OutboxTimerOperation, ReadOutboxPlan,
};
use crate::sync::reconcile::SyncTopicsOperation;

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

/// The outbox record stored at `key`.
pub async fn read_outbox_record(
    storage: &StorageHandle,
    key: &[u8],
) -> Result<Option<DocumentOutboxRecord>, ApplyError> {
    let plan = ReadOutboxPlan { key: key.to_vec() };
    drive(PlanOperation::new(plan), &test_context(storage.clone())).await
}

/// One page of outbox records under `prefix`.
pub async fn read_outbox_records(
    storage: &StorageHandle,
    prefix: &[u8],
    start_after: Option<Vec<u8>>,
    limit: usize,
) -> Result<OutboxReadBatch, ApplyError> {
    let plan = OutboxPagePlan {
        prefix: prefix.to_vec(),
        start_after,
        limit,
    };
    drive(PlanOperation::new(plan), &test_context(storage.clone())).await
}

/// The last key of every outbox stream that holds a record.
pub async fn read_outbox_tails(
    storage: &StorageHandle,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ApplyError> {
    drive(OutboxTailsOperation::new(), &test_context(storage.clone())).await
}

/// Arms the outbox drain on `tasks` as a restarting node does.
pub async fn restore_outbox_timers(storage: &StorageHandle, tasks: &TaskHandle) {
    let mut context = test_context(storage.clone());
    context.task_handle = Some(tasks.clone());
    drive(OutboxTimerOperation::new(), &context)
        .await
        .expect("outbox timer restores");
}

fn context_realm(context: &DriverContext) -> RealmId {
    *context
        .net_handle
        .as_ref()
        .expect("the context has a net handle")
        .realm_id()
}

/// Pulls `topics` from `peers` and reconciles them.
pub async fn sync_topics(
    context: &DriverContext,
    topics: Vec<irokle::TopicId>,
    peers: Vec<NodeId>,
) -> Result<DocumentReconcileResult, ApplyError> {
    let sync = SyncTopicsOperation::new(context_realm(context), topics, peers);
    drive(sync, context).await
}

/// Pulls one topic from `peers` and reconciles it; a topic that cannot be fetched fails.
pub async fn sync_topic(
    context: &DriverContext,
    topic: irokle::TopicId,
    peers: Vec<NodeId>,
) -> Result<DocumentReconcileResult, ApplyError> {
    let sync = SyncTopicsOperation::single(context_realm(context), topic, peers);
    drive(sync, context).await
}

/// Reconciles what `topics` already hold.
pub async fn reconcile_topics(
    context: &DriverContext,
    topics: Vec<irokle::TopicId>,
) -> Result<DocumentReconcileResult, ApplyError> {
    let reconcile = ReconcileTopicsOperation::new(context_realm(context), topics);
    drive(reconcile, context).await
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
