//! Keeps bucket key audit records whose write failed until they are stored: a retry timer per
//! record, persisted when storage allows and stored first at startup. Records are non-secret.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::driver::DriverContext;
use crate::tasks::task_persistence::delete_persisted_timer;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BUCKET_AUDIT_KEYSPACE, TASK_TIMER_KEYSPACE};
use aruna_core::structs::storage::key_audit::BucketAuditRecord;
use aruna_core::task::{PersistedTaskTimer, TaskEffect, TaskKey};
use std::time::Duration;
use tracing::warn;

/// Writes of an audit record inside its operation before the record moves to a retry timer.
pub(crate) const AUDIT_ATTEMPTS: u32 = 3;
/// How long a record that could not be stored waits before its next write.
pub const AUDIT_RETRY: Duration = Duration::from_secs(30);

/// The timer that keeps `record` until it is stored.
pub fn retry_timer(record: &BucketAuditRecord) -> TaskKey {
    TaskKey::RecordAudit {
        record: Box::new(record.clone()),
    }
}

/// Arms the retry timer of `record`.
pub fn retry_effect(record: &BucketAuditRecord) -> Effect {
    Effect::Task(TaskEffect::ResetTimer {
        key: retry_timer(record),
        after: AUDIT_RETRY,
    })
}

/// Stores the record a `RecordAudit` timer keeps; its event id makes a repeated write idempotent.
pub async fn store_record(context: &DriverContext, key: &TaskKey) -> Result<(), String> {
    let TaskKey::RecordAudit { record } = key else {
        return Err("not an audit record timer".to_string());
    };
    let value = record.to_bytes().map_err(|error| error.to_string())?;
    let write = StorageEffect::Write {
        key_space: BUCKET_AUDIT_KEYSPACE.to_string(),
        key: record.key().into(),
        value: value.into(),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(write).await {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected audit record answer: {other:?}")),
    }
}

/// Stores every audit record a persisted timer still keeps, before startup rebuilds the unlock
/// state from the audit trail. Answers the records stored.
pub async fn resume_records(context: &DriverContext) -> Result<usize, String> {
    let mut start = None;
    let mut stored = 0;
    loop {
        let scan = StorageEffect::Iter {
            key_space: TASK_TIMER_KEYSPACE.to_string(),
            prefix: None,
            start: start.take().map(IterStart::After),
            limit: 256,
            txn_id: None,
        };
        let (values, next) = match context.storage_handle.send_storage_effect(scan).await {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => return Err(error.to_string()),
            other => return Err(format!("unexpected timer scan answer: {other:?}")),
        };
        for (_, value) in values {
            let Ok(timer) = postcard::from_bytes::<PersistedTaskTimer>(value.as_ref()) else {
                continue;
            };
            if !matches!(timer.key, TaskKey::RecordAudit { .. }) {
                continue;
            }
            match store_record(context, &timer.key).await {
                Ok(()) => {
                    delete_persisted_timer(&context.storage_handle, &timer.key).await;
                    stored += 1;
                }
                // The timer stays persisted and retries once the task runtime restores it.
                Err(error) => warn!(error = %error, "Kept audit record still not stored"),
            }
        }
        match next {
            Some(next) => start = Some(next),
            None => return Ok(stored),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::storage::key_audit::{AuditAction, AuditOutcome, next_event_id};
    use aruna_storage::storage;
    use tempfile::tempdir;
    use ulid::Ulid;

    #[tokio::test]
    async fn kept_records_resume() {
        let dir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let record = BucketAuditRecord {
            event_id: next_event_id(5),
            bucket_id: Ulid::from_bytes([4; 16]),
            at_ms: 5,
            action: AuditAction::Lock,
            actor: None,
            node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
            generation: Some(1),
            session_id: Some(Ulid::from_bytes([7; 16])),
            intent_id: None,
            deadline_ms: None,
            reason: None,
            outcome: AuditOutcome::Applied,
        };
        // A crash left only the persisted retry timer of a lock record.
        let arm = TaskEffect::ResetTimer {
            key: retry_timer(&record),
            after: AUDIT_RETRY,
        };
        let persist = crate::tasks::task_persistence::persist_task_effect(&storage_handle, &arm);
        persist.await.unwrap();
        assert_eq!(resume_records(&context).await, Ok(1));
        let read = StorageEffect::Read {
            key_space: BUCKET_AUDIT_KEYSPACE.to_string(),
            key: record.key().into(),
            txn_id: None,
        };
        let Event::Storage(StorageEvent::ReadResult { value, .. }) =
            storage_handle.send_storage_effect(read).await
        else {
            panic!("missing read result");
        };
        let stored = BucketAuditRecord::from_bytes(&value.unwrap()).unwrap();
        assert_eq!(stored, record);
        // The timer is gone, so a second start stores nothing again.
        assert_eq!(resume_records(&context).await, Ok(0));
    }
}
