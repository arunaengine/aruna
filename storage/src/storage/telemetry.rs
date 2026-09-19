//! Builds tracing spans and stable kind names for storage effects and events.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::StorageEvent;
use tracing::{Span, debug_span, field};

pub(super) fn storage_effect_span(effect: &StorageEffect) -> Span {
    let span = debug_span!(
        "storage.effect",
        "otel.kind" = "internal",
        operation = storage_effect_kind(effect),
        key_space = field::Empty,
        txn_id = field::Empty,
        key_len = field::Empty,
        value_len = field::Empty,
        cursor_len = field::Empty,
        batch_len = field::Empty,
        limit = field::Empty,
        read = field::Empty,
        queue_wait_ms = field::Empty,
        service_ms = field::Empty,
        total_elapsed_ms = field::Empty,
        path = field::Empty,
        persist_mode = field::Empty,
        commit_ms = field::Empty,
        persist_ms = field::Empty,
        result = field::Empty,
    );
    record_effect_fields(&span, effect);
    span
}

pub(super) fn record_effect_fields(span: &Span, effect: &StorageEffect) {
    match effect {
        StorageEffect::StartTransaction { read } => {
            span.record("read", *read);
        }
        StorageEffect::CommitTransaction { txn_id }
        | StorageEffect::AbortTransaction { txn_id } => {
            span.record("txn_id", field::display(txn_id));
        }
        StorageEffect::Read {
            key_space,
            key,
            txn_id,
        }
        | StorageEffect::Delete {
            key_space,
            key,
            txn_id,
        } => {
            span.record("key_space", field::display(key_space));
            span.record("key_len", key.as_ref().len() as u64);
            if let Some(txn_id) = txn_id {
                span.record("txn_id", field::display(txn_id));
            }
        }
        StorageEffect::Write {
            key_space,
            key,
            value,
            txn_id,
        } => {
            span.record("key_space", field::display(key_space));
            span.record("key_len", key.as_ref().len() as u64);
            span.record("value_len", value.as_ref().len() as u64);
            if let Some(txn_id) = txn_id {
                span.record("txn_id", field::display(txn_id));
            }
        }
        StorageEffect::BatchRead { reads, txn_id } => {
            span.record("batch_len", reads.len() as u64);
            if let Some(txn_id) = txn_id {
                span.record("txn_id", field::display(txn_id));
            }
        }
        StorageEffect::BatchWrite { writes, txn_id } => {
            span.record("batch_len", writes.len() as u64);
            if let Some(txn_id) = txn_id {
                span.record("txn_id", field::display(txn_id));
            }
        }
        StorageEffect::AddUsage {
            key_space,
            deltas,
            txn_id,
        } => {
            span.record("key_space", field::display(key_space));
            span.record("batch_len", deltas.len() as u64);
            span.record("txn_id", field::display(txn_id));
        }
        StorageEffect::BatchDelete { deletes, txn_id } => {
            span.record("batch_len", deletes.len() as u64);
            if let Some(txn_id) = txn_id {
                span.record("txn_id", field::display(txn_id));
            }
        }
        StorageEffect::Iter {
            key_space,
            prefix,
            start,
            limit,
            txn_id,
        } => {
            span.record("key_space", field::display(key_space));
            if let Some(prefix) = prefix {
                span.record("key_len", prefix.as_ref().len() as u64);
            }
            if let Some(start) = start {
                span.record("cursor_len", start.key().as_ref().len() as u64);
            }
            span.record("limit", *limit as u64);
            if let Some(txn_id) = txn_id {
                span.record("txn_id", field::display(txn_id));
            }
        }
        StorageEffect::Last {
            key_space,
            prefix,
            txn_id,
        } => {
            span.record("key_space", field::display(key_space));
            if let Some(prefix) = prefix {
                span.record("key_len", prefix.as_ref().len() as u64);
            }
            if let Some(txn_id) = txn_id {
                span.record("txn_id", field::display(txn_id));
            }
        }
        StorageEffect::SyncAll => {}
    }
}

pub(super) fn storage_effect_kind(effect: &StorageEffect) -> &'static str {
    match effect {
        StorageEffect::StartTransaction { .. } => "start_transaction",
        StorageEffect::CommitTransaction { .. } => "commit_transaction",
        StorageEffect::Read { .. } => "read",
        StorageEffect::BatchRead { .. } => "batch_read",
        StorageEffect::Write { .. } => "write",
        StorageEffect::BatchWrite { .. } => "batch_write",
        StorageEffect::AddUsage { .. } => "add_usage",
        StorageEffect::Delete { .. } => "delete",
        StorageEffect::BatchDelete { .. } => "batch_delete",
        StorageEffect::AbortTransaction { .. } => "abort_transaction",
        StorageEffect::SyncAll => "sync_all",
        StorageEffect::Iter { .. } => "iter",
        StorageEffect::Last { .. } => "last",
    }
}

pub(super) fn effect_kind(effect: &Effect) -> &'static str {
    match effect {
        Effect::Storage(storage_effect) => storage_effect_kind(storage_effect),
        Effect::Blob(_) => "blob",
        Effect::StagingSource(_) => "staging_source",
        Effect::LocalFile(_) => "local_file",
        Effect::Net(_) => "net",
        Effect::Metadata(_) => "metadata",
        Effect::SubOperation(_) => "suboperation",
        Effect::Task(_) => "task",
        Effect::Search() => "search",
        Effect::Stream() => "stream",
    }
}

pub(super) fn storage_event_kind(event: &StorageEvent) -> &'static str {
    match event {
        StorageEvent::TransactionStarted { .. } => "transaction_started",
        StorageEvent::TransactionCommitted { .. } => "transaction_committed",
        StorageEvent::TransactionAborted { .. } => "transaction_aborted",
        StorageEvent::ReadResult { .. } => "read_result",
        StorageEvent::BatchReadResult { .. } => "batch_read_result",
        StorageEvent::WriteResult { .. } => "write_result",
        StorageEvent::BatchWriteResult { .. } => "batch_write_result",
        StorageEvent::DeleteResult { .. } => "delete_result",
        StorageEvent::BatchDeleteResult { .. } => "batch_delete_result",
        StorageEvent::SyncAllFinished => "sync_all_finished",
        StorageEvent::IterResult { .. } => "iter_result",
        StorageEvent::Error { .. } => "error",
    }
}
