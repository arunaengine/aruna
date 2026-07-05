use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::NOTIFICATION_INBOX_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::{NotificationRecord, notification_inbox_prefix};
use aruna_core::types::{Effects, Key, UserId};
use smallvec::smallvec;
use thiserror::Error;

pub const UNREAD_COUNT_CAP: usize = 100;
pub const UNREAD_SCAN_MAX_ROWS: usize = 2_000;

#[derive(Clone, Debug, PartialEq)]
pub struct UnreadCountInput {
    pub recipient: UserId,
}

#[derive(Debug, PartialEq)]
pub struct UnreadCountOutput {
    pub count: usize,
    pub capped: bool,
}

#[derive(Debug, PartialEq)]
pub struct UnreadCountOperation {
    input: UnreadCountInput,
    state: UnreadCountState,
    count: usize,
    examined: usize,
    output: Option<Result<UnreadCountOutput, UnreadCountError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum UnreadCountState {
    Init,
    Scan,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum UnreadCountError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
    #[error("unread count did not finish")]
    NotFinished,
}

impl UnreadCountOperation {
    pub fn new(input: UnreadCountInput) -> Self {
        Self {
            input,
            state: UnreadCountState::Init,
            count: 0,
            examined: 0,
            output: None,
        }
    }

    fn fail(&mut self, error: UnreadCountError) -> Effects {
        self.state = UnreadCountState::Error;
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
        self.fail(UnreadCountError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            got,
        })
    }

    fn emit_scan(&mut self, start: Option<Key>) -> Effects {
        self.state = UnreadCountState::Scan;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: NOTIFICATION_INBOX_KEYSPACE.to_string(),
            prefix: Some(notification_inbox_prefix(self.input.recipient)),
            start: start.map(IterStart::After),
            limit: UNREAD_COUNT_CAP,
            txn_id: None,
        })]
    }

    fn finish(&mut self, capped: bool) -> Effects {
        self.state = UnreadCountState::Finish;
        self.output = Some(Ok(UnreadCountOutput {
            count: self.count,
            capped,
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

        let page_len = values.len();
        for (index, (_, value)) in values.into_iter().enumerate() {
            let record = match NotificationRecord::from_bytes(&value) {
                Ok(record) => record,
                Err(error) => return self.fail(error.into()),
            };
            self.examined += 1;
            if record.read_at_ms.is_none() {
                self.count += 1;
                if self.count == UNREAD_COUNT_CAP {
                    let more = index + 1 < page_len || next_start_after.is_some();
                    return self.finish(more);
                }
            }
        }

        if self.examined >= UNREAD_SCAN_MAX_ROWS && next_start_after.is_some() {
            return self.finish(true);
        }
        match next_start_after {
            Some(next) => self.emit_scan(Some(next)),
            None => self.finish(false),
        }
    }
}

impl Operation for UnreadCountOperation {
    type Output = UnreadCountOutput;
    type Error = UnreadCountError;

    fn start(&mut self) -> Effects {
        self.emit_scan(None)
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match self.fail_on_storage_error(event) {
            Ok(event) => event,
            Err(effects) => return effects,
        };

        match self.state {
            UnreadCountState::Scan => self.handle_scan(event),
            UnreadCountState::Init | UnreadCountState::Finish | UnreadCountState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            UnreadCountState::Finish | UnreadCountState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(UnreadCountError::NotFinished)?
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
    use aruna_core::structs::{NotificationClass, NotificationKind, RealmId};
    use aruna_storage::storage::{FjallStorage, StorageHandle};
    use std::collections::VecDeque;
    use tempfile::{TempDir, tempdir};
    use ulid::Ulid;

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

    fn record(recipient: UserId, created_at_ms: u64, read: bool) -> NotificationRecord {
        let mut record = NotificationRecord::new(
            recipient,
            NotificationClass::Direct,
            NotificationKind::AddedToGroup {
                group_id: Ulid::new(),
                actor_user_id: user(recipient.realm_id.0[0], 200),
            },
            created_at_ms,
        );
        if read {
            record.read_at_ms = Some(1);
        }
        record
    }

    async fn seed(storage: &StorageHandle, records: &[NotificationRecord]) {
        assert_eq!(
            upsert_inbox_records(storage, records).await,
            Ok(records.len())
        );
    }

    async fn count(context: &DriverContext, recipient: UserId) -> UnreadCountOutput {
        drive(
            UnreadCountOperation::new(UnreadCountInput { recipient }),
            context,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn unread_counts_only_unread() {
        let (_tempdir, context) = context_with_storage();
        let recipient = user(1, 1);
        let mut records = Vec::new();
        for ts in 0..3 {
            records.push(record(recipient, 100 + ts, false));
        }
        for ts in 0..2 {
            records.push(record(recipient, 200 + ts, true));
        }
        seed(&context.storage_handle, &records).await;

        assert_eq!(
            count(&context, recipient).await,
            UnreadCountOutput {
                count: 3,
                capped: false
            }
        );
    }

    #[tokio::test]
    async fn unread_caps_at_100() {
        let (_tempdir, context) = context_with_storage();
        let recipient = user(1, 1);
        let records: Vec<_> = (0..130).map(|ts| record(recipient, ts, false)).collect();
        seed(&context.storage_handle, &records).await;

        assert_eq!(
            count(&context, recipient).await,
            UnreadCountOutput {
                count: 100,
                capped: true
            }
        );
    }

    #[tokio::test]
    async fn unread_exactly_100_is_not_capped() {
        let (_tempdir, context) = context_with_storage();
        let recipient = user(1, 1);
        let records: Vec<_> = (0..100).map(|ts| record(recipient, ts, false)).collect();
        seed(&context.storage_handle, &records).await;

        assert_eq!(
            count(&context, recipient).await,
            UnreadCountOutput {
                count: 100,
                capped: false
            }
        );
    }

    #[tokio::test]
    async fn unread_scans_across_pages() {
        let (_tempdir, context) = context_with_storage();
        let recipient = user(1, 1);
        let alternating: Vec<_> = (0..250)
            .map(|ts| record(recipient, ts, ts % 2 == 1))
            .collect();
        seed(&context.storage_handle, &alternating).await;
        assert_eq!(
            count(&context, recipient).await,
            UnreadCountOutput {
                count: 100,
                capped: true
            }
        );

        let (_tempdir2, context2) = context_with_storage();
        let sparse: Vec<_> = (0..240)
            .map(|ts| record(recipient, ts, ts % 4 != 0))
            .collect();
        let expected_unread = sparse.iter().filter(|r| r.read_at_ms.is_none()).count();
        assert_eq!(expected_unread, 60);
        seed(&context2.storage_handle, &sparse).await;
        assert_eq!(
            count(&context2, recipient).await,
            UnreadCountOutput {
                count: 60,
                capped: false
            }
        );
    }

    #[tokio::test]
    async fn unread_scan_work_is_bounded() {
        let (_tempdir, context) = context_with_storage();
        let recipient = user(1, 1);
        let total = UNREAD_SCAN_MAX_ROWS + 100;
        let records: Vec<_> = (0..total)
            .map(|ts| record(recipient, ts as u64, ts + 5 < total))
            .collect();
        let unread = records.iter().filter(|r| r.read_at_ms.is_none()).count();
        assert_eq!(unread, 5);
        seed(&context.storage_handle, &records).await;

        let mut operation = UnreadCountOperation::new(UnreadCountInput { recipient });
        let mut iters = 0usize;
        let mut queue: VecDeque<Effect> = operation.start().into_iter().collect();
        while !operation.is_complete() {
            while let Some(effect) = queue.pop_front() {
                let Effect::Storage(storage_effect) = effect else {
                    panic!("unread must only emit storage effects");
                };
                if matches!(storage_effect, StorageEffect::Iter { .. }) {
                    iters += 1;
                }
                let event = context
                    .storage_handle
                    .send_storage_effect(storage_effect)
                    .await;
                queue.extend(operation.step(event));
            }
            if queue.is_empty() && !operation.is_complete() {
                break;
            }
        }
        let output = operation.finalize().unwrap();
        assert_eq!(
            output,
            UnreadCountOutput {
                count: 5,
                capped: true
            }
        );
        assert!(iters <= UNREAD_SCAN_MAX_ROWS / UNREAD_COUNT_CAP);
    }
}
