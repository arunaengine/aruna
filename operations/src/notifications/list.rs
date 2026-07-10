use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::NOTIFICATION_INBOX_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::{
    NotificationRecord, notification_inbox_cursor, notification_inbox_prefix,
    parse_notification_inbox_key,
};
use aruna_core::types::{Effects, Key, UserId, Value};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;

pub const LIST_NOTIFICATIONS_MAX_LIMIT: usize = 200;

#[derive(Clone, Debug, PartialEq)]
pub struct ListNotificationsInput {
    pub recipient: UserId,
    pub cursor: Option<Vec<u8>>,
    pub limit: usize,
}

#[derive(Debug, PartialEq)]
pub struct ListNotificationsOutput {
    pub records: Vec<NotificationRecord>,
    pub next_cursor: Option<Vec<u8>>,
}

#[derive(Debug, PartialEq)]
pub struct ListNotificationsOperation {
    input: ListNotificationsInput,
    state: ListNotificationsState,
    output: Option<Result<ListNotificationsOutput, ListNotificationsError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum ListNotificationsState {
    Init,
    List,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ListNotificationsError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("invalid cursor")]
    InvalidCursor,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
    #[error("list notifications did not finish")]
    NotFinished,
}

impl ListNotificationsOperation {
    pub fn new(mut input: ListNotificationsInput) -> Self {
        input.limit = input.limit.clamp(1, LIST_NOTIFICATIONS_MAX_LIMIT);
        Self {
            input,
            state: ListNotificationsState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: ListNotificationsError) -> Effects {
        self.state = ListNotificationsState::Error;
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
        self.fail(ListNotificationsError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            got,
        })
    }

    fn handle_list(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            return self.unexpected_event("Event::Storage(StorageEvent::IterResult)", got);
        };
        match self.collect(values) {
            Ok(effects) => effects,
            Err(error) => self.fail(error),
        }
    }

    fn collect(&mut self, values: Vec<(Key, Value)>) -> Result<Effects, ListNotificationsError> {
        let mut records = Vec::with_capacity(values.len());
        for (key, value) in values {
            let (recipient, _, _) = parse_notification_inbox_key(&key)?;
            if recipient != self.input.recipient {
                warn!(
                    ?recipient,
                    "Skipping notification row with foreign recipient"
                );
                continue;
            }
            records.push(NotificationRecord::from_bytes(&value)?);
        }

        let next_cursor = if records.len() > self.input.limit {
            records.truncate(self.input.limit);
            records.last().map(|record| {
                notification_inbox_cursor(record.created_at_ms, record.notification_id)
            })
        } else {
            None
        };

        self.state = ListNotificationsState::Finish;
        self.output = Some(Ok(ListNotificationsOutput {
            records,
            next_cursor,
        }));
        Ok(smallvec![])
    }
}

impl Operation for ListNotificationsOperation {
    type Output = ListNotificationsOutput;
    type Error = ListNotificationsError;

    fn start(&mut self) -> Effects {
        if matches!(&self.input.cursor, Some(cursor) if cursor.len() != 24) {
            return self.fail(ListNotificationsError::InvalidCursor);
        }

        self.state = ListNotificationsState::List;
        let prefix = notification_inbox_prefix(self.input.recipient);
        let start = self.input.cursor.as_ref().map(|cursor| {
            let mut key = Vec::with_capacity(72);
            key.extend_from_slice(prefix.as_ref());
            key.extend_from_slice(cursor);
            IterStart::After(ByteView::from(key))
        });
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: NOTIFICATION_INBOX_KEYSPACE.to_string(),
            prefix: Some(prefix),
            start,
            limit: self.input.limit.saturating_add(1),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match self.fail_on_storage_error(event) {
            Ok(event) => event,
            Err(effects) => return effects,
        };

        match self.state {
            ListNotificationsState::List => self.handle_list(event),
            ListNotificationsState::Init
            | ListNotificationsState::Finish
            | ListNotificationsState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ListNotificationsState::Finish | ListNotificationsState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(ListNotificationsError::NotFinished)?
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
    use aruna_core::keyspaces::NOTIFICATION_INBOX_KEYSPACE;
    use aruna_core::structs::{NotificationClass, NotificationKind, RealmId};
    use aruna_storage::storage::{FjallStorage, StorageHandle};
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

    #[tokio::test]
    async fn list_returns_newest_first() {
        let (_tempdir, context) = context_with_storage();
        let recipient = user(1, 1);
        let records: Vec<_> = [10, 20, 30, 40, 50]
            .into_iter()
            .map(|ts| record(recipient, ts))
            .collect();
        seed(&context.storage_handle, &records).await;

        let output = drive(
            ListNotificationsOperation::new(ListNotificationsInput {
                recipient,
                cursor: None,
                limit: 10,
            }),
            &context,
        )
        .await
        .unwrap();

        let timestamps: Vec<u64> = output.records.iter().map(|r| r.created_at_ms).collect();
        assert_eq!(timestamps, vec![50, 40, 30, 20, 10]);
        assert_eq!(output.next_cursor, None);
    }

    #[tokio::test]
    async fn list_paginates_with_lookahead() {
        let (_tempdir, context) = context_with_storage();
        let recipient = user(1, 1);
        let records: Vec<_> = (1..=5).map(|ts| record(recipient, ts * 10)).collect();
        seed(&context.storage_handle, &records).await;

        let mut seen_ids = Vec::new();
        let mut page_sizes = Vec::new();
        let mut cursor = None;
        loop {
            let output = drive(
                ListNotificationsOperation::new(ListNotificationsInput {
                    recipient,
                    cursor: cursor.clone(),
                    limit: 2,
                }),
                &context,
            )
            .await
            .unwrap();
            page_sizes.push(output.records.len());
            seen_ids.extend(output.records.iter().map(|r| r.notification_id));
            match output.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }

        assert_eq!(page_sizes, vec![2, 2, 1]);
        let mut expected: Vec<Ulid> = records.iter().map(|r| r.notification_id).collect();
        expected.sort();
        seen_ids.sort();
        assert_eq!(seen_ids, expected);
    }

    #[tokio::test]
    async fn list_cursor_is_stable_under_inserts() {
        let (_tempdir, context) = context_with_storage();
        let recipient = user(1, 1);
        let records: Vec<_> = (1..=4).map(|ts| record(recipient, ts * 10)).collect();
        seed(&context.storage_handle, &records).await;

        let page1 = drive(
            ListNotificationsOperation::new(ListNotificationsInput {
                recipient,
                cursor: None,
                limit: 2,
            }),
            &context,
        )
        .await
        .unwrap();
        let cursor = page1.next_cursor.clone().expect("cursor for page 2");

        seed(&context.storage_handle, &[record(recipient, 1000)]).await;

        let page2 = drive(
            ListNotificationsOperation::new(ListNotificationsInput {
                recipient,
                cursor: Some(cursor),
                limit: 2,
            }),
            &context,
        )
        .await
        .unwrap();

        let page1_ids: Vec<Ulid> = page1.records.iter().map(|r| r.notification_id).collect();
        for record in &page2.records {
            assert!(!page1_ids.contains(&record.notification_id));
        }
    }

    #[tokio::test]
    async fn list_is_recipient_scoped_at_key_level() {
        let (_tempdir, context) = context_with_storage();
        let alice = user(1, 1);
        let bob = user(1, 2);
        seed(
            &context.storage_handle,
            &[record(alice, 10), record(bob, 10)],
        )
        .await;

        let output = drive(
            ListNotificationsOperation::new(ListNotificationsInput {
                recipient: alice,
                cursor: None,
                limit: 10,
            }),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(output.records.len(), 1);
        assert!(output.records.iter().all(|r| r.recipient == alice));

        let keys = match context
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: NOTIFICATION_INBOX_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 1024,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values,
            other => panic!("unexpected event: {other:?}"),
        };
        assert_eq!(keys.len(), 2);
        assert!(
            keys.iter()
                .any(|(k, _)| k.starts_with(alice.to_storage_key()))
        );
        assert!(
            keys.iter()
                .any(|(k, _)| k.starts_with(bob.to_storage_key()))
        );
    }

    #[tokio::test]
    async fn list_rejects_malformed_cursor() {
        let (_tempdir, context) = context_with_storage();
        let recipient = user(1, 1);
        let result = drive(
            ListNotificationsOperation::new(ListNotificationsInput {
                recipient,
                cursor: Some(vec![0u8; 23]),
                limit: 10,
            }),
            &context,
        )
        .await;
        assert_eq!(result, Err(ListNotificationsError::InvalidCursor));
    }

    #[test]
    fn list_clamps_limit() {
        let recipient = user(1, 1);
        let mut zero = ListNotificationsOperation::new(ListNotificationsInput {
            recipient,
            cursor: None,
            limit: 0,
        });
        match zero.start().first() {
            Some(Effect::Storage(StorageEffect::Iter { limit, .. })) => assert_eq!(*limit, 2),
            other => panic!("unexpected effect: {other:?}"),
        }

        let mut huge = ListNotificationsOperation::new(ListNotificationsInput {
            recipient,
            cursor: None,
            limit: 10_000,
        });
        match huge.start().first() {
            Some(Effect::Storage(StorageEffect::Iter { limit, .. })) => assert_eq!(*limit, 201),
            other => panic!("unexpected effect: {other:?}"),
        }
    }
}
