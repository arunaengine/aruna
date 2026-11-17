//! Runs the operation that pages a user's inbox records with a resume cursor.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::NOTIFICATION_INBOX_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::notification::{
    NotificationRecord, notification_inbox_cursor, notification_inbox_prefix, parse_inbox_key,
};
use aruna_core::types::{Key, Value};
use thiserror::Error;

use crate::notifications::unexpected;

pub const LIST_MAX_LIMIT: usize = 200;
/// The unread count a holder reports before it marks the count as capped.
pub(crate) const UNREAD_COUNT_CAP: usize = 100;
/// The inbox rows one unread count examines at most.
pub(crate) const SCAN_MAX_ROWS: usize = 2_000;

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

/// Reads one page of a recipient's inbox, newest first, and one row more to know whether
/// another page follows.
#[derive(Debug, PartialEq)]
pub struct ListNotificationsOperation {
    input: ListNotificationsInput,
}

#[derive(Debug, Error, PartialEq)]
pub enum ListNotificationsError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("invalid cursor")]
    InvalidCursor,
    #[error(transparent)]
    Io(IoError),
}

impl From<IoError> for ListNotificationsError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::StorageError(error),
            error => Self::Io(error),
        }
    }
}

impl ListNotificationsOperation {
    pub fn new(mut input: ListNotificationsInput) -> Self {
        input.limit = input.limit.clamp(1, LIST_MAX_LIMIT);
        Self { input }
    }

    fn collect(
        &self,
        rows: Vec<(Key, Value)>,
    ) -> Result<ListNotificationsOutput, ListNotificationsError> {
        let mut records = Vec::with_capacity(rows.len());
        for (key, value) in rows {
            let (recipient, created_at_ms, notification_id) = parse_inbox_key(&key)?;
            let record = NotificationRecord::from_bytes(&value)?;
            if recipient != self.input.recipient
                || (recipient, created_at_ms, notification_id)
                    != (
                        record.recipient,
                        record.created_at_ms,
                        record.notification_id,
                    )
            {
                return Err(ConversionError::FromStrError(
                    "notification inbox key does not match payload identity".to_string(),
                )
                .into());
            }
            records.push(record);
        }

        let next_cursor = if records.len() > self.input.limit {
            records.truncate(self.input.limit);
            records.last().map(|record| {
                notification_inbox_cursor(record.created_at_ms, record.notification_id)
            })
        } else {
            None
        };
        Ok(ListNotificationsOutput {
            records,
            next_cursor,
        })
    }
}

impl Operation for ListNotificationsOperation {
    type Output = ListNotificationsOutput;
    type Error = ListNotificationsError;

    fn first(&mut self) -> Result<Next<Self::Output>, Self::Error> {
        if matches!(&self.input.cursor, Some(cursor) if cursor.len() != 24) {
            return Err(ListNotificationsError::InvalidCursor);
        }
        let prefix = notification_inbox_prefix(self.input.recipient);
        let after = self.input.cursor.as_ref().map(|cursor| {
            let mut key = Vec::with_capacity(72);
            key.extend_from_slice(prefix.as_ref());
            key.extend_from_slice(cursor);
            Key::from(key)
        });
        Ok(Next::Call(IoCall::Page {
            space: NOTIFICATION_INBOX_KEYSPACE.to_string(),
            prefix,
            after,
            limit: self.input.limit.saturating_add(1),
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, Self::Error> {
        match result {
            IoResult::Page { rows, .. } => Ok(Next::Done(self.collect(rows)?)),
            other => Err(unexpected("IoResult::Page", &other).into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::drive;
    use crate::tests::notifications::{context_with_storage, seed, user};
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::structs::execution::notification::{NotificationClass, notification_inbox_key};
    use ulid::Ulid;

    fn record(recipient: UserId, created_at_ms: u64) -> NotificationRecord {
        crate::tests::notifications::record(recipient, NotificationClass::Direct, created_at_ms)
    }

    #[tokio::test]
    async fn newest_first() {
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
    async fn pagination_looks_ahead() {
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
    async fn cursor_stable_inserts() {
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
    async fn recipient_key_scoped() {
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
    async fn malformed_cursor_rejected() {
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

    #[tokio::test]
    async fn identity_mismatch_rejected() {
        let (_tempdir, context) = context_with_storage();
        let recipient = user(1, 1);
        let original = record(recipient, 10);
        let mut mismatched = original.clone();
        mismatched.notification_id = Ulid::generate();
        assert!(matches!(
            context
                .storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: NOTIFICATION_INBOX_KEYSPACE.to_string(),
                    key: notification_inbox_key(
                        recipient,
                        original.created_at_ms,
                        original.notification_id,
                    ),
                    value: mismatched.to_bytes().unwrap().into(),
                    txn_id: None,
                })
                .await,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));

        assert!(matches!(
            drive(
                ListNotificationsOperation::new(ListNotificationsInput {
                    recipient,
                    cursor: None,
                    limit: 10,
                }),
                &context,
            )
            .await,
            Err(ListNotificationsError::ConversionError(_))
        ));
    }

    #[test]
    fn list_clamps_limit() {
        let recipient = user(1, 1);
        for (limit, asked) in [(0, 2), (10_000, 201)] {
            let mut list = ListNotificationsOperation::new(ListNotificationsInput {
                recipient,
                cursor: None,
                limit,
            });
            match list.first() {
                Ok(Next::Call(IoCall::Page { limit, .. })) => assert_eq!(limit, asked),
                other => panic!("unexpected call: {other:?}"),
            }
        }
    }

    #[test]
    fn cursor_pages_after() {
        let recipient = user(1, 1);
        let cursor = vec![7u8; 24];
        let mut list = ListNotificationsOperation::new(ListNotificationsInput {
            recipient,
            cursor: Some(cursor.clone()),
            limit: 1,
        });
        let prefix = notification_inbox_prefix(recipient);
        let after = Key::from([prefix.as_ref(), cursor.as_slice()].concat());
        assert_eq!(
            list.first(),
            Ok(Next::Call(IoCall::Page {
                space: NOTIFICATION_INBOX_KEYSPACE.to_string(),
                prefix,
                after: Some(after),
                limit: 2,
            }))
        );
        let first = record(recipient, 20);
        let second = record(recipient, 10);
        let row = |record: &NotificationRecord| {
            let key = notification_inbox_key(
                record.recipient,
                record.created_at_ms,
                record.notification_id,
            );
            (key, Value::from(record.to_bytes().unwrap()))
        };
        let page = IoResult::Page {
            rows: vec![row(&first), row(&second)],
            next: None,
        };
        let cursor = notification_inbox_cursor(first.created_at_ms, first.notification_id);
        assert_eq!(
            list.next(page),
            Ok(Next::Done(ListNotificationsOutput {
                records: vec![first],
                next_cursor: Some(cursor),
            }))
        );
        assert!(matches!(
            list.next(IoResult::Done),
            Err(ListNotificationsError::Io(IoError::Unexpected { .. }))
        ));
    }
}
