//! Writes new notification records into the outbox and schedules the outbox drain.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, SubOperationEvent};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::storage_entries::outbox_write_entry;
use aruna_core::structs::execution::notification::{NotificationOutboxRecord, NotificationRecord};
use aruna_core::task::TaskEvent;
use aruna_core::types::{Key, KeySpace, Value};
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::notifications::outbox_record::schedule_drain_effect;
use crate::notifications::unexpected;

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

/// Writes the records as outbox rows in one batch, then schedules the outbox drain. A failed
/// schedule is logged: the drain's own re-arm or the next emission picks the rows up.
#[derive(Debug, PartialEq)]
pub struct EmitNotificationsOperation {
    writes: Vec<(KeySpace, Key, Value)>,
    scheduling: bool,
}

impl EmitNotificationsOperation {
    /// Builds the outbox rows; each gets a new outbox id here, so `first` and `next` stay
    /// deterministic.
    pub fn new(input: EmitNotificationsInput) -> Self {
        let mut writes = Vec::with_capacity(input.records.len());
        for record in input.records {
            let outbox = NotificationOutboxRecord {
                outbox_id: Ulid::generate(),
                record,
            };
            match outbox_write_entry(&outbox) {
                Ok(entry) => writes.push(entry),
                Err(error) => warn!(%error, "Skipping unserializable notification outbox row"),
            }
        }
        Self {
            writes,
            scheduling: false,
        }
    }
}

impl Operation for EmitNotificationsOperation {
    type Output = ();
    type Error = EmitNotificationsError;

    fn first(&mut self) -> Result<Next<()>, Self::Error> {
        if self.writes.is_empty() {
            return Ok(Next::Done(()));
        }
        Ok(Next::Call(IoCall::Write(std::mem::take(&mut self.writes))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, Self::Error> {
        match (self.scheduling, result) {
            (false, IoResult::Done) => {
                self.scheduling = true;
                Ok(Next::Call(IoCall::Effect(schedule_drain_effect())))
            }
            (true, IoResult::Event(Event::Task(event))) => {
                if let TaskEvent::Error { message, .. } = event {
                    warn!(message = %message, "Failed to schedule notification outbox drain after emit");
                }
                Ok(Next::Done(()))
            }
            (_, other) => Err(unexpected("an outbox write or a drain schedule", &other).into()),
        }
    }
}

pub(crate) fn emit_notifications_effect(records: Vec<NotificationRecord>) -> Effect {
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
    use crate::driver::{DriverContext, drive};
    use crate::tests::notifications::{context_with_storage, record, user};
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::StorageEvent;
    use aruna_core::keyspaces::NOTIFICATION_OUTBOX_KEYSPACE;
    use aruna_core::structs::execution::notification::{
        NotificationClass, notification_outbox_key,
    };
    use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
    use aruna_storage::storage;
    use aruna_tasks::{InboundTaskHandler, TaskHandle};
    use async_trait::async_trait;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::tempdir;
    use tokio::sync::mpsc;

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

    /// The rows are written, then the drain is scheduled; a failed schedule still succeeds.
    #[test]
    fn write_then_schedule() {
        for answer in [
            TaskEvent::TimerScheduled {
                key: TaskKey::DrainNotificationOutbox,
                after: Duration::ZERO,
            },
            TaskEvent::Error {
                key: None,
                message: "scheduler unavailable".to_string(),
            },
        ] {
            let mut operation = EmitNotificationsOperation::new(EmitNotificationsInput {
                records: vec![make_record(1, 2)],
            });
            assert!(matches!(
                operation.first(),
                Ok(Next::Call(IoCall::Write(writes))) if writes.len() == 1
            ));
            match operation.next(IoResult::Done) {
                Ok(Next::Call(IoCall::Effect(Effect::Task(TaskEffect::ResetTimer {
                    key,
                    after,
                })))) => {
                    assert_eq!(key, TaskKey::DrainNotificationOutbox);
                    assert_eq!(after, Duration::ZERO);
                }
                other => panic!("expected a drain schedule, got {other:?}"),
            }
            let answer = IoResult::Event(Event::Task(answer));
            assert_eq!(operation.next(answer), Ok(Next::Done(())));
        }
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
                IoResult::Event(event) => Ok(Next::Done(event)),
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
        let ids: std::collections::HashSet<Ulid> =
            rows.iter().map(|(_, row)| row.outbox_id).collect();
        assert_eq!(ids.len(), 2);
        for (_, row) in &rows {
            assert_eq!(row.record, record);
        }
    }

    #[tokio::test]
    async fn emit_schedules_drain() {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let task_handle = TaskHandle::new();
        let (seen_tx, mut seen_rx) = mpsc::channel(1);
        task_handle
            .set_inbound_handler(Arc::new(RecordingTaskHandler { seen: seen_tx }))
            .await;
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
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
