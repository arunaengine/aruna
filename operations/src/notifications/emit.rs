use std::convert::Infallible;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::storage_entries::notification_outbox_write_entry;
use aruna_core::structs::{NotificationOutboxRecord, NotificationRecord};
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, Key, KeySpace, TxnId, Value};
use smallvec::smallvec;
use tracing::warn;
use ulid::Ulid;

use crate::notifications::outbox::schedule_notification_outbox_drain_effect;

#[derive(Clone, Debug, PartialEq)]
pub struct EmitNotificationsInput {
    pub records: Vec<NotificationRecord>,
}

#[derive(Debug, PartialEq)]
pub struct EmitNotificationsOperation {
    input: EmitNotificationsInput,
    state: EmitNotificationsState,
}

#[derive(Debug, Clone, PartialEq)]
pub enum EmitNotificationsState {
    Init,
    StartTransaction,
    WriteOutbox { txn_id: TxnId },
    CommitTransaction { txn_id: TxnId },
    ScheduleDrain,
    Finish,
}

impl EmitNotificationsOperation {
    pub fn new(input: EmitNotificationsInput) -> Self {
        Self {
            input,
            state: EmitNotificationsState::Init,
        }
    }

    fn drop_emission(&mut self, event_desc: String, txn_id: Option<TxnId>) -> Effects {
        warn!(state = ?self.state, event = %event_desc, "Dropping notification emission");
        self.state = EmitNotificationsState::Finish;
        match txn_id {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }

    fn handle_start_transaction(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.drop_emission(got, None);
        };

        let mut writes: Vec<(KeySpace, Key, Value)> = Vec::with_capacity(self.input.records.len());
        for record in &self.input.records {
            let outbox_record = NotificationOutboxRecord {
                outbox_id: Ulid::new(),
                record: record.clone(),
            };
            match notification_outbox_write_entry(&outbox_record) {
                Ok(entry) => writes.push(entry),
                Err(error) => warn!(%error, "Skipping unserializable notification outbox row"),
            }
        }

        if writes.is_empty() {
            self.state = EmitNotificationsState::Finish;
            return smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })];
        }

        self.state = EmitNotificationsState::WriteOutbox { txn_id };
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })]
    }

    fn handle_write_outbox(&mut self, event: Event, txn_id: TxnId) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
            return self.drop_emission(got, Some(txn_id));
        };
        self.state = EmitNotificationsState::CommitTransaction { txn_id };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_commit_transaction(&mut self, event: Event, txn_id: TxnId) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.drop_emission(got, Some(txn_id));
        };
        self.state = EmitNotificationsState::ScheduleDrain;
        smallvec![schedule_notification_outbox_drain_effect()]
    }

    fn handle_schedule_drain(&mut self, event: Event) -> Effects {
        if let Event::Task(TaskEvent::Error { message, .. }) = event {
            warn!(message = %message, "Failed to schedule notification outbox drain after emit");
        }
        self.state = EmitNotificationsState::Finish;
        smallvec![]
    }
}

impl Operation for EmitNotificationsOperation {
    type Output = ();
    type Error = Infallible;

    fn start(&mut self) -> Effects {
        if self.input.records.is_empty() {
            self.state = EmitNotificationsState::Finish;
            return smallvec![];
        }
        self.state = EmitNotificationsState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state.clone() {
            EmitNotificationsState::StartTransaction => self.handle_start_transaction(event),
            EmitNotificationsState::WriteOutbox { txn_id } => {
                self.handle_write_outbox(event, txn_id)
            }
            EmitNotificationsState::CommitTransaction { txn_id } => {
                self.handle_commit_transaction(event, txn_id)
            }
            EmitNotificationsState::ScheduleDrain => self.handle_schedule_drain(event),
            EmitNotificationsState::Init | EmitNotificationsState::Finish => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        self.state == EmitNotificationsState::Finish
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        Ok(())
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            EmitNotificationsState::WriteOutbox { txn_id }
            | EmitNotificationsState::CommitTransaction { txn_id } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => smallvec![],
        }
    }
}

pub fn emit_notifications_effect(records: Vec<NotificationRecord>) -> Effect {
    Effect::SubOperation(boxed_suboperation(
        EmitNotificationsOperation::new(EmitNotificationsInput { records }),
        |result| match result {
            Ok(()) => Event::SubOperation(SubOperationEvent::NotificationsEmitted),
            Err(never) => match never {},
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_core::errors::StorageError;
    use aruna_core::keyspaces::NOTIFICATION_OUTBOX_KEYSPACE;
    use aruna_core::structs::{
        NotificationClass, NotificationKind, RealmId, notification_outbox_key,
    };
    use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
    use aruna_core::types::UserId;
    use aruna_storage::storage;
    use aruna_tasks::{InboundTaskHandler, TaskHandle};
    use async_trait::async_trait;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::{TempDir, tempdir};
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

    fn make_user(realm: u8, user: u8) -> UserId {
        UserId::new(Ulid::from_bytes([user; 16]), RealmId([realm; 32]))
    }

    fn make_record(realm: u8, user: u8) -> NotificationRecord {
        NotificationRecord::new(
            make_user(realm, user),
            NotificationClass::Direct,
            NotificationKind::AddedToGroup {
                group_id: Ulid::new(),
                actor_user_id: make_user(realm, 100),
            },
            1_000,
        )
    }

    fn context_with_storage() -> (TempDir, DriverContext) {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        };
        (tempdir, context)
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
    async fn emit_writes_outbox_rows_and_finishes_ok() {
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
    async fn empty_input_is_a_no_op() {
        let (_tempdir, context) = context_with_storage();
        let result = drive(
            EmitNotificationsOperation::new(EmitNotificationsInput { records: vec![] }),
            &context,
        )
        .await;
        assert_eq!(result, Ok(()));
        assert!(read_outbox_rows(&context).await.is_empty());
    }

    #[test]
    fn storage_error_at_start_transaction_finishes_ok() {
        let mut operation = EmitNotificationsOperation::new(EmitNotificationsInput {
            records: vec![make_record(1, 2)],
        });
        let effects = operation.start();
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })]
        ));

        let effects = operation.step(Event::Storage(StorageEvent::Error {
            error: StorageError::WriteError,
        }));
        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert_eq!(operation.finalize(), Ok(()));
    }

    #[test]
    fn storage_error_at_batch_write_aborts_and_finishes_ok() {
        let mut operation = EmitNotificationsOperation::new(EmitNotificationsInput {
            records: vec![make_record(1, 2)],
        });
        operation.start();
        let txn_id = TxnId::new();
        let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::BatchWrite { .. })]
        ));

        let effects = operation.step(Event::Storage(StorageEvent::Error {
            error: StorageError::WriteError,
        }));
        match effects.as_slice() {
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id: aborted })] => {
                assert_eq!(*aborted, txn_id);
            }
            other => panic!("expected a single abort effect, got {other:?}"),
        }

        let effects = operation.step(Event::Storage(StorageEvent::TransactionAborted { txn_id }));
        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert_eq!(operation.finalize(), Ok(()));
    }

    #[test]
    fn storage_error_at_commit_finishes_ok() {
        let mut operation = EmitNotificationsOperation::new(EmitNotificationsInput {
            records: vec![make_record(1, 2)],
        });
        operation.start();
        let txn_id = TxnId::new();
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: vec![],
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { .. })]
        ));

        let effects = operation.step(Event::Storage(StorageEvent::Error {
            error: StorageError::WriteError,
        }));
        match effects.as_slice() {
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id: aborted })] => {
                assert_eq!(*aborted, txn_id);
            }
            other => panic!("expected a single abort effect, got {other:?}"),
        }
        assert!(operation.is_complete());
        assert_eq!(operation.finalize(), Ok(()));
    }

    #[test]
    fn unexpected_event_is_swallowed() {
        let mut operation = EmitNotificationsOperation::new(EmitNotificationsInput {
            records: vec![make_record(1, 2)],
        });
        operation.start();
        let txn_id = TxnId::new();
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));

        let effects = operation.step(Event::Search());
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        assert!(operation.is_complete());
        assert_eq!(operation.finalize(), Ok(()));
    }

    #[derive(Debug, PartialEq)]
    struct HostOperation {
        record: Option<NotificationRecord>,
        observed: Option<Event>,
    }

    impl HostOperation {
        fn new(record: NotificationRecord) -> Self {
            Self {
                record: Some(record),
                observed: None,
            }
        }
    }

    impl Operation for HostOperation {
        type Output = Event;
        type Error = Infallible;

        fn start(&mut self) -> Effects {
            let record = self.record.take().expect("record present in start");
            smallvec![emit_notifications_effect(vec![record])]
        }

        fn step(&mut self, event: Event) -> Effects {
            self.observed = Some(event);
            smallvec![]
        }

        fn is_complete(&self) -> bool {
            self.observed.is_some()
        }

        fn finalize(self) -> Result<Self::Output, Self::Error> {
            Ok(self
                .observed
                .expect("host operation should observe the emit event"))
        }

        fn abort(&mut self) -> Effects {
            smallvec![]
        }
    }

    #[tokio::test]
    async fn suboperation_wrapper_maps_to_notifications_emitted() {
        let (_tempdir, context) = context_with_storage();
        let observed = drive(HostOperation::new(make_record(1, 2)), &context)
            .await
            .unwrap();
        assert_eq!(
            observed,
            Event::SubOperation(SubOperationEvent::NotificationsEmitted)
        );
        assert_eq!(read_outbox_rows(&context).await.len(), 1);
    }

    #[tokio::test]
    async fn duplicate_drive_is_not_deduplicated() {
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

    #[test]
    fn commit_schedules_drain_then_finishes() {
        let mut operation = EmitNotificationsOperation::new(EmitNotificationsInput {
            records: vec![make_record(1, 2)],
        });
        operation.start();
        let txn_id = TxnId::new();
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: vec![],
        }));
        let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
        match effects.as_slice() {
            [Effect::Task(TaskEffect::ResetTimer { key, after })] => {
                assert_eq!(*key, TaskKey::DrainNotificationOutbox);
                assert_eq!(*after, Duration::ZERO);
            }
            other => panic!("expected a drain schedule effect, got {other:?}"),
        }
        assert!(!operation.is_complete());

        let effects = operation.step(Event::Task(TaskEvent::TimerScheduled {
            key: TaskKey::DrainNotificationOutbox,
            after: Duration::ZERO,
        }));
        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert_eq!(operation.finalize(), Ok(()));
    }

    #[test]
    fn commit_schedule_drain_swallows_task_error() {
        let mut operation = EmitNotificationsOperation::new(EmitNotificationsInput {
            records: vec![make_record(1, 2)],
        });
        operation.start();
        let txn_id = TxnId::new();
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: vec![],
        }));
        operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));

        let effects = operation.step(Event::Task(TaskEvent::Error {
            key: None,
            message: "scheduler unavailable".to_string(),
        }));
        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert_eq!(operation.finalize(), Ok(()));
    }

    #[tokio::test]
    async fn emit_schedules_drain_timer_after_commit_end_to_end() {
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
            task_handle: Some(task_handle),
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
