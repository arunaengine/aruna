//! Writes new notification records into the outbox and schedules the outbox drain.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, SubOperationEvent};
use aruna_core::id_mint::IdMint;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::storage_entries::outbox_write_entry;
use aruna_core::structs::execution::notification::{
    NotificationOutboxRecord, NotificationRecord, schedule_drain_effect,
};
use aruna_core::types::{Key, KeySpace, Value};
use thiserror::Error;
use tracing::warn;

#[derive(Debug, Error, PartialEq)]
pub enum EmitNotificationsError {
    #[error(transparent)]
    Storage(StorageError),
    #[error(transparent)]
    Io(IoError),
}

impl From<IoError> for EmitNotificationsError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            error => Self::Io(error),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct EmitNotificationsInput {
    pub records: Vec<NotificationRecord>,
}

/// Writes the records as outbox rows in one batch, then schedules the outbox drain after it. A
/// failed schedule is logged: the drain's own re-arm or the next emission picks the rows up.
#[derive(Debug, PartialEq)]
pub struct EmitNotificationsOperation {
    records: Vec<NotificationRecord>,
    now_ms: u64,
    ids: IdMint,
}

impl EmitNotificationsOperation {
    pub fn new(input: EmitNotificationsInput) -> Self {
        Self {
            records: input.records,
            now_ms: 0,
            ids: IdMint::default(),
        }
    }

    /// The outbox rows, each with a new outbox id.
    fn writes(&mut self) -> Vec<(KeySpace, Key, Value)> {
        let mut writes = Vec::with_capacity(self.records.len());
        for record in std::mem::take(&mut self.records) {
            let outbox = NotificationOutboxRecord {
                outbox_id: self.ids.mint(self.now_ms),
                record,
            };
            match outbox_write_entry(&outbox) {
                Ok(entry) => writes.push(entry),
                Err(error) => warn!(%error, "Skipping unserializable notification outbox row"),
            }
        }
        writes
    }
}

impl Operation for EmitNotificationsOperation {
    type Output = ();
    type Error = EmitNotificationsError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn seed(&mut self, entropy: u128) {
        self.ids = IdMint::new(entropy);
    }

    fn first(&mut self) -> Result<Next<()>, Self::Error> {
        let writes = self.writes();
        if writes.is_empty() {
            return Ok(Next::Done(()));
        }
        Ok(Next::Call(IoCall::Write(writes)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, Self::Error> {
        result.done()?;
        Ok(Next::Then {
            output: (),
            calls: vec![IoCall::from(schedule_drain_effect())],
        })
    }
}

pub fn emit_notifications_effect(records: Vec<NotificationRecord>) -> Effect {
    Effect::SubOperation(boxed_suboperation(
        EmitNotificationsOperation::new(EmitNotificationsInput { records }),
        |result| match result {
            Ok(()) => Event::SubOperation(SubOperationEvent::NotificationsEmitted),
            Err(error) => Event::SubOperation(SubOperationEvent::NotificationsFailed {
                reason: error.to_string(),
            }),
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::StorageEvent;
    use aruna_core::keyspaces::NOTIFICATION_OUTBOX_KEYSPACE;
    use aruna_core::structs::execution::notification::{
        NotificationClass, NotificationKind, notification_outbox_key,
    };
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::task::{TaskEffect, TaskKey};
    use aruna_core::user::id::UserId;
    use aruna_runner::driver::{DriverContext, drive};
    use aruna_scheduler::{InboundTaskHandler, TaskHandle};
    use aruna_storage::storage;
    use async_trait::async_trait;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use ulid::Ulid;

    fn context_with_storage() -> (tempfile::TempDir, DriverContext) {
        aruna_runner::testing::temp_context()
    }

    fn user(realm: u8, seed: u8) -> UserId {
        UserId::new(Ulid::from_bytes([seed; 16]), RealmId([realm; 32]))
    }

    fn record(
        recipient: UserId,
        class: NotificationClass,
        created_at_ms: u64,
    ) -> NotificationRecord {
        NotificationRecord::new(
            Ulid::generate(),
            recipient,
            class,
            NotificationKind::AddedToGroup {
                group_id: Ulid::generate(),
                actor_user_id: user(recipient.realm_id.0[0], 200),
            },
            created_at_ms,
        )
    }

    fn unexpected(expected: &'static str, result: &IoResult) -> IoError {
        IoError::Unexpected {
            expected,
            got: result.label(),
        }
    }

    struct RecordingTaskHandler {
        seen: mpsc::Sender<TaskKey>,
    }

    #[async_trait]
    impl InboundTaskHandler for RecordingTaskHandler {
        async fn handle_timer(&self, key: TaskKey) {
            let _ = self.seen.send(key).await;
        }
    }

    fn make_record(realm: u8, user_seed: u8) -> NotificationRecord {
        record(user(realm, user_seed), NotificationClass::Direct, 1_000)
    }

    async fn read_outbox_rows(context: &DriverContext) -> Vec<(Vec<u8>, NotificationOutboxRecord)> {
        match context
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: NOTIFICATION_OUTBOX_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 1024,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values
                .into_iter()
                .map(|(key, value)| (key.to_vec(), postcard::from_bytes(&value).unwrap()))
                .collect(),
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn emit_writes_outbox() {
        let (_tempdir, context) = context_with_storage();
        let records = vec![make_record(1, 2), make_record(1, 3)];
        let result = drive(
            EmitNotificationsOperation::new(EmitNotificationsInput {
                records: records.clone(),
            }),
            &context,
        )
        .await;
        assert_eq!(result, Ok(()));

        let rows = read_outbox_rows(&context).await;
        assert_eq!(rows.len(), 2);
        let stored: Vec<NotificationRecord> =
            rows.iter().map(|(_, row)| row.record.clone()).collect();
        for record in &records {
            assert!(stored.contains(record));
        }
        for (key, row) in &rows {
            assert_eq!(
                key.as_slice(),
                notification_outbox_key(row.outbox_id).as_ref()
            );
        }
    }

    #[tokio::test]
    async fn empty_input_noop() {
        let (_tempdir, context) = context_with_storage();
        let result = drive(
            EmitNotificationsOperation::new(EmitNotificationsInput { records: vec![] }),
            &context,
        )
        .await;
        assert_eq!(result, Ok(()));
        assert!(read_outbox_rows(&context).await.is_empty());
    }

    /// The rows are written, then the drain is scheduled after them.
    #[test]
    fn write_then_schedule() {
        let mut operation = EmitNotificationsOperation::new(EmitNotificationsInput {
            records: vec![make_record(1, 2)],
        });
        operation.stamp(7_000);
        operation.seed(42);
        let outbox_id = IdMint::new(42).mint(7_000);
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Write(writes)))
                if writes.len() == 1 && writes[0].1 == notification_outbox_key(outbox_id)
        ));
        assert_eq!(
            operation.next(IoResult::Done),
            Ok(Next::Then {
                output: (),
                calls: vec![IoCall::from(Effect::Task(TaskEffect::ResetTimer {
                    key: TaskKey::DrainNotificationOutbox,
                    after: Duration::ZERO,
                }))],
            })
        );
    }

    // A storage failure must not reach the parent as a successful emission.
    #[test]
    fn storage_failure_reported() {
        let boom = StorageError::WriteError("boom".to_string());
        assert_eq!(
            EmitNotificationsError::from(IoError::Storage(boom.clone())),
            EmitNotificationsError::Storage(boom)
        );
        let mut operation = EmitNotificationsOperation::new(EmitNotificationsInput {
            records: vec![make_record(1, 2)],
        });
        operation.first().unwrap();
        assert!(matches!(
            operation.next(IoResult::Woke),
            Err(EmitNotificationsError::Io(IoError::Unexpected { .. }))
        ));
    }

    #[derive(Debug, PartialEq)]
    struct HostOperation {
        record: Option<NotificationRecord>,
    }

    impl Operation for HostOperation {
        type Output = Event;
        type Error = IoError;

        fn first(&mut self) -> Result<Next<Event>, IoError> {
            let record = self.record.take().expect("record present in first");
            let Effect::SubOperation(emit) = emit_notifications_effect(vec![record]) else {
                panic!("emit is a sub-operation");
            };
            Ok(Next::Call(IoCall::Child(emit)))
        }

        fn next(&mut self, result: IoResult) -> Result<Next<Event>, IoError> {
            match result {
                IoResult::Event(event) => Ok(Next::Done(*event)),
                other => Err(unexpected("the emit event", &other)),
            }
        }
    }

    #[tokio::test]
    async fn wrapper_maps_event() {
        let (_tempdir, context) = context_with_storage();
        let host = HostOperation {
            record: Some(make_record(1, 2)),
        };
        let observed = drive(host, &context).await.unwrap();
        assert_eq!(
            observed,
            Event::SubOperation(SubOperationEvent::NotificationsEmitted)
        );
        assert_eq!(read_outbox_rows(&context).await.len(), 1);
    }

    #[tokio::test]
    async fn duplicate_drive_repeats() {
        let (_tempdir, context) = context_with_storage();
        let record = make_record(1, 2);
        for _ in 0..2 {
            drive(
                EmitNotificationsOperation::new(EmitNotificationsInput {
                    records: vec![record.clone()],
                }),
                &context,
            )
            .await
            .unwrap();
        }

        let rows = read_outbox_rows(&context).await;
        assert_eq!(rows.len(), 2);
        let ids: std::collections::HashSet<ulid::Ulid> =
            rows.iter().map(|(_, row)| row.outbox_id).collect();
        assert_eq!(ids.len(), 2);
        for (_, row) in &rows {
            assert_eq!(row.record, record);
        }
    }

    #[tokio::test]
    async fn emit_schedules_drain() {
        let storage_handle = storage::memory::MemoryStorage::open();
        let task_handle = TaskHandle::new();
        let (seen_tx, mut seen_rx) = mpsc::channel(1);
        task_handle
            .set_inbound_handler(Arc::new(RecordingTaskHandler { seen: seen_tx }))
            .await;
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            graph_handle: None,
            task_handle: Some(task_handle),
            compute_handle: None,
            outbound_handle: None,
        };

        let result = drive(
            EmitNotificationsOperation::new(EmitNotificationsInput {
                records: vec![make_record(1, 2)],
            }),
            &context,
        )
        .await;
        assert_eq!(result, Ok(()));

        let key = tokio::time::timeout(Duration::from_secs(1), seen_rx.recv())
            .await
            .expect("drain timer should fire")
            .expect("recording handler should receive timer key");
        assert_eq!(key, TaskKey::DrainNotificationOutbox);
        assert_eq!(read_outbox_rows(&context).await.len(), 1);
    }
}
