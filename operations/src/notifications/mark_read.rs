//! Runs the operation that marks inbox records read by id or up to a timestamp.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::NOTIFICATION_INBOX_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::storage_entries::inbox_update_entry;
use aruna_core::structs::execution::notification::{
    NotificationRecord, invert_timestamp_ms, notification_inbox_prefix, parse_inbox_key,
};
use aruna_core::types::{Key, KeySpace, Value};
use thiserror::Error;
use ulid::Ulid;

use crate::notifications::unexpected;

const MARK_PAGE_SIZE: usize = 512;
pub const MARK_MAX_IDS: usize = 512;

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

/// Pages a recipient's inbox and marks the unread records it names by id or up to a time.
#[derive(Debug, PartialEq)]
pub struct MarkReadOperation {
    input: MarkReadInput,
    marked: usize,
    ids_remaining: usize,
    /// The key to page after once the pending write is done; `None` ends the scan.
    pending_next: Option<Key>,
    too_many_ids: bool,
}

#[derive(Debug, Error, PartialEq)]
pub enum MarkReadError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("mark read id count exceeds cap {max}")]
    TooManyIds { max: usize },
    #[error(transparent)]
    Io(IoError),
}

impl From<IoError> for MarkReadError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::StorageError(error),
            error => Self::Io(error),
        }
    }
}

type Turn = Result<Next<MarkReadOutput>, MarkReadError>;

impl MarkReadOperation {
    pub fn new(mut input: MarkReadInput) -> Self {
        let too_many_ids = input.ids.len() > MARK_MAX_IDS;
        if !too_many_ids {
            input.ids.sort_unstable();
            input.ids.dedup();
        }
        let ids_remaining = input.ids.len();
        Self {
            input,
            marked: 0,
            ids_remaining,
            pending_next: None,
            too_many_ids,
        }
    }

    fn scan(&self, after: Option<Key>) -> Turn {
        Ok(Next::Call(IoCall::Page {
            space: NOTIFICATION_INBOX_KEYSPACE.to_string(),
            prefix: notification_inbox_prefix(self.input.recipient),
            after,
            limit: MARK_PAGE_SIZE,
        }))
    }

    fn finish(&self) -> Turn {
        Ok(Next::Done(MarkReadOutput {
            marked: self.marked,
        }))
    }

    fn mark_page(&mut self, rows: Vec<(Key, Value)>, next: Option<Key>) -> Turn {
        let mut writes: Vec<(KeySpace, Key, Value)> = Vec::new();
        for (key, value) in rows {
            let (recipient, created_at_ms, notification_id) = parse_inbox_key(&key)?;
            let mut record = NotificationRecord::from_bytes(&value)?;
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
                writes.push(inbox_update_entry(&record)?);
                self.marked += 1;
            }
        }

        let all_ids_found = self.input.up_to_ms.is_none() && self.ids_remaining == 0;
        let next = next.filter(|_| !all_ids_found);
        if !writes.is_empty() {
            self.pending_next = next;
            return Ok(Next::Call(IoCall::Write(writes)));
        }
        match next {
            Some(next) => self.scan(Some(next)),
            None => self.finish(),
        }
    }
}

impl Operation for MarkReadOperation {
    type Output = MarkReadOutput;
    type Error = MarkReadError;

    fn first(&mut self) -> Turn {
        if self.too_many_ids {
            return Err(MarkReadError::TooManyIds { max: MARK_MAX_IDS });
        }
        if self.input.ids.is_empty() && self.input.up_to_ms.is_none() {
            return self.finish();
        }
        let after = match (self.input.up_to_ms, self.input.ids.is_empty()) {
            // No key lies between this one and itself with one more zero byte, so paging after
            // it starts at the first record created at or before `up_to_ms`.
            (Some(up_to_ms), true) => {
                let mut seek = Vec::with_capacity(71);
                seek.extend_from_slice(&self.input.recipient.to_storage_key());
                seek.extend_from_slice(&invert_timestamp_ms(up_to_ms).to_be_bytes());
                seek.extend_from_slice(&[0u8; 15]);
                Some(Key::from(seek))
            }
            _ => None,
        };
        self.scan(after)
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match result {
            IoResult::Page { rows, next } => self.mark_page(rows, next),
            IoResult::Done => match self.pending_next.take() {
                Some(next) => self.scan(Some(next)),
                None => self.finish(),
            },
            other => Err(unexpected("an inbox page or write", &other).into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::tests::notifications::{context_with_storage, seed, user};
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::PRUNE_INDEX_KEYSPACE;
    use aruna_core::structs::execution::notification::{NotificationClass, notification_inbox_key};
    use aruna_storage::storage::StorageHandle;

    fn record(recipient: UserId, created_at_ms: u64) -> NotificationRecord {
        crate::tests::notifications::record(recipient, NotificationClass::Direct, created_at_ms)
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
                key_space: PRUNE_INDEX_KEYSPACE.to_string(),
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
    async fn mark_ids_idempotent() {
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
    async fn mark_through_timestamp() {
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
    async fn mark_filters_combine() {
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
    async fn unknown_ids_noop() {
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
    fn rejects_id_limit() {
        let recipient = user(1, 1);
        let ids = (0..=MARK_MAX_IDS).map(|_| Ulid::generate()).collect();
        let mut operation = MarkReadOperation::new(MarkReadInput {
            recipient,
            ids,
            up_to_ms: None,
            now_ms: 1,
        });
        assert_eq!(
            operation.first(),
            Err(MarkReadError::TooManyIds { max: MARK_MAX_IDS })
        );
    }

    /// A page with marks writes them before it asks for the next page.
    #[test]
    fn writes_before_paging() {
        let recipient = user(1, 1);
        let records = [record(recipient, 20), record(recipient, 10)];
        let mut operation = MarkReadOperation::new(MarkReadInput {
            recipient,
            ids: Vec::new(),
            up_to_ms: Some(20),
            now_ms: 9,
        });
        let Ok(Next::Call(IoCall::Page {
            after: Some(seek), ..
        })) = operation.first()
        else {
            panic!("a page after the seek key");
        };
        assert_eq!(seek.len(), 71);
        let rows = records
            .iter()
            .map(|record| {
                let key = notification_inbox_key(
                    record.recipient,
                    record.created_at_ms,
                    record.notification_id,
                );
                (key, Value::from(record.to_bytes().unwrap()))
            })
            .collect();
        let last = Key::from(vec![9u8]);
        let page = IoResult::Page {
            rows,
            next: Some(last.clone()),
        };
        let Ok(Next::Call(IoCall::Write(writes))) = operation.next(page) else {
            panic!("a write of the marks");
        };
        assert_eq!(writes.len(), 2);
        assert!(matches!(
            operation.next(IoResult::Done),
            Ok(Next::Call(IoCall::Page { after: Some(after), .. })) if after == last
        ));
        let empty = IoResult::Page {
            rows: Vec::new(),
            next: None,
        };
        assert_eq!(
            operation.next(empty),
            Ok(Next::Done(MarkReadOutput { marked: 2 }))
        );
        assert!(operation.next(IoResult::Woke).is_err());
    }

    #[tokio::test]
    async fn recipient_scoped() {
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
    async fn identity_mismatch_rejected() {
        let (_tempdir, context) = context_with_storage();
        let recipient = user(1, 1);
        let original = record(recipient, 10);
        let mut mismatched = original.clone();
        mismatched.created_at_ms = 20;
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
                MarkReadOperation::new(MarkReadInput {
                    recipient,
                    ids: vec![original.notification_id],
                    up_to_ms: None,
                    now_ms: 30,
                }),
                &context,
            )
            .await,
            Err(MarkReadError::ConversionError(_))
        ));
    }

    #[tokio::test]
    async fn prune_index_untouched() {
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
