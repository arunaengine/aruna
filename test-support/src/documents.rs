//! Syncs and reconciles document topics and restores the outbox timers, as a node does.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::document::DocumentReconcileResult;
use aruna_core::document_apply::ApplyError;
use aruna_core::document_apply::reconcile::ReconcileTopicsOperation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_drivers::driver::{DriverContext, drive};
use aruna_drivers::testing::test_context;
use aruna_operations::sync::document_outbox::OutboxTimerOperation;
use aruna_operations::sync::reconcile::SyncTopicsOperation;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;

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
