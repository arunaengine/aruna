use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::NOTIFICATION_INBOX_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::storage_entries::notification_inbox_update_entry;
use aruna_core::structs::{NotificationRecord, invert_timestamp_ms, notification_inbox_prefix};
use aruna_core::types::{Effects, Key, KeySpace, UserId, Value};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

pub const MARK_READ_SCAN_PAGE_SIZE: usize = 512;
pub const MARK_READ_MAX_IDS: usize = 512;

#[derive(Clone, Debug, PartialEq)]
pub struct MarkReadInput {
    pub recipient: UserId,
    pub ids: Vec<Ulid>,
    pub up_to_ms: Option<u64>,
    pub now_ms: u64,
}

#[derive(Debug, PartialEq)]
pub struct MarkReadOutput {
    pub marked: usize,
}

#[derive(Debug, PartialEq)]
pub struct MarkReadOperation {
    input: MarkReadInput,
    state: MarkReadState,
    marked: usize,
    ids_remaining: usize,
    pending_next: Option<Key>,
    too_many_ids: bool,
    output: Option<Result<MarkReadOutput, MarkReadError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum MarkReadState {
    Init,
    Scan,
    Write,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum MarkReadError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("mark read id count exceeds cap {max}")]
    TooManyIds { max: usize },
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
    #[error("mark read did not finish")]
    NotFinished,
}

impl MarkReadOperation {
    pub fn new(mut input: MarkReadInput) -> Self {
        let too_many_ids = input.ids.len() > MARK_READ_MAX_IDS;
        if !too_many_ids {
            input.ids.sort_unstable();
            input.ids.dedup();
        }
        let ids_remaining = input.ids.len();
        Self {
            input,
            state: MarkReadState::Init,
            marked: 0,
            ids_remaining,
            pending_next: None,
            too_many_ids,
            output: None,
        }
    }

    fn fail(&mut self, error: MarkReadError) -> Effects {
        self.state = MarkReadState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn fail_on_storage_error(&mut self, event: Event) -> Result<Event, Effects> {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return Err(self.fail(error.into()));
        }
        Ok(event)
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        self.fail(MarkReadError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            got,
        })
    }

    fn scan_effect(&self, start: Option<IterStart>) -> Effect {
        Effect::Storage(StorageEffect::Iter {
            key_space: NOTIFICATION_INBOX_KEYSPACE.to_string(),
            prefix: Some(notification_inbox_prefix(self.input.recipient)),
            start,
            limit: MARK_READ_SCAN_PAGE_SIZE,
            txn_id: None,
        })
    }

    fn finish(&mut self) -> Effects {
        self.state = MarkReadState::Finish;
        self.output = Some(Ok(MarkReadOutput {
            marked: self.marked,
        }));
        smallvec![]
    }

    fn handle_scan(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return self.unexpected_event("Event::Storage(StorageEvent::IterResult)", got);
        };

        let mut writes: Vec<(KeySpace, Key, Value)> = Vec::new();
        for (_, value) in values {
            let mut record = match NotificationRecord::from_bytes(&value) {
                Ok(record) => record,
                Err(error) => return self.fail(error.into()),
            };
            let by_id = self
                .input
                .ids
                .binary_search(&record.notification_id)
                .is_ok();
            if by_id && self.ids_remaining > 0 {
                self.ids_remaining -= 1;
            }
            let by_time = self
                .input
                .up_to_ms
                .is_some_and(|up_to_ms| record.created_at_ms <= up_to_ms);
            if record.read_at_ms.is_none() && (by_id || by_time) {
                record.read_at_ms = Some(self.input.now_ms);
                match notification_inbox_update_entry(&record) {
                    Ok(entry) => writes.push(entry),
                    Err(error) => return self.fail(error.into()),
                }
                self.marked += 1;
            }
        }

        let all_ids_found = self.input.up_to_ms.is_none() && self.ids_remaining == 0;
        let next = if all_ids_found {
            None
        } else {
            next_start_after
        };

        if !writes.is_empty() {
            self.pending_next = next;
            self.state = MarkReadState::Write;
            return smallvec![Effect::Storage(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            })];
        }

        match next {
            Some(next) => smallvec![self.scan_effect(Some(IterStart::After(next)))],
            None => self.finish(),
        }
    }

    fn handle_write(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
            return self.unexpected_event("Event::Storage(StorageEvent::BatchWriteResult)", got);
        };
        match self.pending_next.take() {
            Some(next) => {
                self.state = MarkReadState::Scan;
                smallvec![self.scan_effect(Some(IterStart::After(next)))]
            }
            None => self.finish(),
        }
    }
}

impl Operation for MarkReadOperation {
    type Output = MarkReadOutput;
    type Error = MarkReadError;

    fn start(&mut self) -> Effects {
        if self.too_many_ids {
            return self.fail(MarkReadError::TooManyIds {
                max: MARK_READ_MAX_IDS,
            });
        }
        if self.input.ids.is_empty() && self.input.up_to_ms.is_none() {
            return self.finish();
        }
        self.state = MarkReadState::Scan;
        let start = match (self.input.up_to_ms, self.input.ids.is_empty()) {
            (Some(up_to_ms), true) => {
                let mut seek = Vec::with_capacity(72);
                seek.extend_from_slice(&self.input.recipient.to_storage_key());
                seek.extend_from_slice(&invert_timestamp_ms(up_to_ms).to_be_bytes());
                seek.extend_from_slice(&[0u8; 16]);
                Some(IterStart::At(ByteView::from(seek)))
            }
            _ => None,
        };
        smallvec![self.scan_effect(start)]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match self.fail_on_storage_error(event) {
            Ok(event) => event,
            Err(effects) => return effects,
        };

        match self.state {
            MarkReadState::Scan => self.handle_scan(event),
            MarkReadState::Write => self.handle_write(event),
            MarkReadState::Init | MarkReadState::Finish | MarkReadState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, MarkReadState::Finish | MarkReadState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(MarkReadError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::notifications::inbox::upsert_inbox_records;
    use aruna_core::keyspaces::NOTIFICATION_INBOX_PRUNE_INDEX_KEYSPACE;
    use aruna_core::structs::{NotificationClass, NotificationKind, RealmId};
    use aruna_storage::storage::{FjallStorage, StorageHandle};
    use tempfile::{TempDir, tempdir};

    fn context_with_storage() -> (TempDir, DriverContext) {
        let tempdir = tempdir().unwrap();
        let storage_handle = FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        };
        (tempdir, context)
    }

    fn user(realm: u8, seed: u8) -> UserId {
        UserId::new(Ulid::from_bytes([seed; 16]), RealmId([realm; 32]))
    }

    fn record(recipient: UserId, created_at_ms: u64) -> NotificationRecord {
        NotificationRecord::new(
            recipient,
            NotificationClass::Direct,
            NotificationKind::AddedToGroup {
                group_id: Ulid::new(),
                actor_user_id: user(recipient.realm_id.0[0], 200),
            },
            created_at_ms,
        )
    }

    async fn seed(storage: &StorageHandle, records: &[NotificationRecord]) {
        assert_eq!(
            upsert_inbox_records(storage, records).await,
            Ok(records.len())
        );
    }

    async fn read_all(storage: &StorageHandle, recipient: UserId) -> Vec<NotificationRecord> {
        match storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: NOTIFICATION_INBOX_KEYSPACE.to_string(),
                prefix: Some(notification_inbox_prefix(recipient)),
                start: None,
                limit: 4096,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values
                .into_iter()
                .map(|(_, value)| NotificationRecord::from_bytes(&value).unwrap())
                .collect(),
            other => panic!("unexpected event: {other:?}"),
        }
    }

    async fn read_prune_index(storage: &StorageHandle) -> Vec<(Vec<u8>, Vec<u8>)> {
        match storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: NOTIFICATION_INBOX_PRUNE_INDEX_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 4096,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values
                .into_iter()
                .map(|(key, value)| (key.to_vec(), value.to_vec()))
                .collect(),
            other => panic!("unexpected event: {other:?}"),
        }
    }

    async fn mark(context: &DriverContext, input: MarkReadInput) -> usize {
        drive(MarkReadOperation::new(input), context)
            .await
            .unwrap()
            .marked
    }

    #[tokio::test]
    async fn mark_by_ids_is_idempotent() {
        let (_tempdir, context) = context_with_storage();
        let recipient = user(1, 1);
        let records: Vec<_> = (1..=3).map(|ts| record(recipient, ts * 10)).collect();
        seed(&context.storage_handle, &records).await;
        let ids = vec![records[0].notification_id, records[1].notification_id];

        assert_eq!(
            mark(
                &context,
                MarkReadInput {
                    recipient,
                    ids: ids.clone(),
                    up_to_ms: None,
                    now_ms: 777,
                },
            )
            .await,
            2
        );

        let stored = read_all(&context.storage_handle, recipient).await;
        for record in &stored {
            if ids.contains(&record.notification_id) {
                assert_eq!(record.read_at_ms, Some(777));
            } else {
                assert_eq!(record.read_at_ms, None);
            }
        }

        assert_eq!(
            mark(
                &context,
                MarkReadInput {
                    recipient,
                    ids,
                    up_to_ms: None,
                    now_ms: 999,
                },
            )
            .await,
            0
        );
        let after = read_all(&context.storage_handle, recipient).await;
        assert!(after.iter().all(|r| r.read_at_ms != Some(999)));
    }

    #[tokio::test]
    async fn mark_up_to_timestamp() {
        let (_tempdir, context) = context_with_storage();
        let recipient = user(1, 1);
        let records: Vec<_> = [10, 20, 30]
            .into_iter()
            .map(|ts| record(recipient, ts))
            .collect();
        seed(&context.storage_handle, &records).await;

        assert_eq!(
            mark(
                &context,
                MarkReadInput {
                    recipient,
                    ids: Vec::new(),
                    up_to_ms: Some(20),
                    now_ms: 555,
                },
            )
            .await,
            2
        );

        let stored = read_all(&context.storage_handle, recipient).await;
        for record in &stored {
            if record.created_at_ms <= 20 {
                assert_eq!(record.read_at_ms, Some(555));
            } else {
                assert_eq!(record.read_at_ms, None);
            }
        }
    }

    #[tokio::test]
    async fn mark_ids_and_up_to_combined() {
        let (_tempdir, context) = context_with_storage();
        let recipient = user(1, 1);
        let records: Vec<_> = [10, 20, 30, 40]
            .into_iter()
            .map(|ts| record(recipient, ts))
            .collect();
        seed(&context.storage_handle, &records).await;
        // up_to_ms covers t=10; id covers t=40 (also in range would double-count).
        let ids = vec![records[3].notification_id, records[0].notification_id];

        assert_eq!(
            mark(
                &context,
                MarkReadInput {
                    recipient,
                    ids,
                    up_to_ms: Some(10),
                    now_ms: 42,
                },
            )
            .await,
            2
        );

        let stored = read_all(&context.storage_handle, recipient).await;
        let marked: Vec<u64> = stored
            .iter()
            .filter(|r| r.read_at_ms == Some(42))
            .map(|r| r.created_at_ms)
            .collect();
        assert_eq!(marked.len(), 2);
        assert!(marked.contains(&10));
        assert!(marked.contains(&40));
    }

    #[tokio::test]
    async fn mark_unknown_ids_is_noop() {
        let (_tempdir, context) = context_with_storage();
        let recipient = user(1, 1);
        seed(
            &context.storage_handle,
            &[record(recipient, 10), record(recipient, 20)],
        )
        .await;

        assert_eq!(
            mark(
                &context,
                MarkReadInput {
                    recipient,
                    ids: vec![Ulid::from_bytes([250u8; 16])],
                    up_to_ms: None,
                    now_ms: 1,
                },
            )
            .await,
            0
        );
        let stored = read_all(&context.storage_handle, recipient).await;
        assert!(stored.iter().all(|r| r.read_at_ms.is_none()));
    }

    #[test]
    fn rejects_too_many_ids() {
        let recipient = user(1, 1);
        let ids = (0..=MARK_READ_MAX_IDS).map(|_| Ulid::new()).collect();
        let mut operation = MarkReadOperation::new(MarkReadInput {
            recipient,
            ids,
            up_to_ms: None,
            now_ms: 1,
        });

        assert!(operation.start().is_empty());
        assert_eq!(
            operation.finalize(),
            Err(MarkReadError::TooManyIds {
                max: MARK_READ_MAX_IDS
            })
        );
    }

    #[tokio::test]
    async fn mark_is_recipient_scoped() {
        let (_tempdir, context) = context_with_storage();
        let alice = user(1, 1);
        let bob = user(1, 2);
        let shared_id = Ulid::from_bytes([77u8; 16]);
        let mut alice_record = record(alice, 10);
        alice_record.notification_id = shared_id;
        let mut bob_record = record(bob, 10);
        bob_record.notification_id = shared_id;
        seed(&context.storage_handle, &[alice_record, bob_record]).await;

        assert_eq!(
            mark(
                &context,
                MarkReadInput {
                    recipient: alice,
                    ids: vec![shared_id],
                    up_to_ms: None,
                    now_ms: 5,
                },
            )
            .await,
            1
        );

        let alice_stored = read_all(&context.storage_handle, alice).await;
        assert_eq!(alice_stored[0].read_at_ms, Some(5));
        let bob_stored = read_all(&context.storage_handle, bob).await;
        assert_eq!(bob_stored[0].read_at_ms, None);
    }

    #[tokio::test]
    async fn mark_does_not_touch_prune_index() {
        let (_tempdir, context) = context_with_storage();
        let recipient = user(1, 1);
        let records: Vec<_> = [10, 20, 30]
            .into_iter()
            .map(|ts| record(recipient, ts))
            .collect();
        seed(&context.storage_handle, &records).await;

        let before = read_prune_index(&context.storage_handle).await;
        assert_eq!(
            mark(
                &context,
                MarkReadInput {
                    recipient,
                    ids: Vec::new(),
                    up_to_ms: Some(30),
                    now_ms: 5,
                },
            )
            .await,
            3
        );
        let after = read_prune_index(&context.storage_handle).await;
        assert_eq!(before, after);
    }
}
