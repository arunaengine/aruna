//! Lists one event of each common kind for tests that check an operation refuses events.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::errors::StorageError;
use crate::events::{Event, StorageEvent, SubOperationEvent};
use crate::task::TaskEvent;
use ulid::Ulid;

/// One event of each common kind, for tests that check an operation refuses events.
pub fn sample_events() -> Vec<Event> {
    let txn_id = Ulid::from_parts(1, 1);
    let key = || Vec::<u8>::new().into();
    vec![
        Event::Storage(StorageEvent::TransactionStarted { txn_id }),
        Event::Storage(StorageEvent::TransactionCommitted { txn_id }),
        Event::Storage(StorageEvent::TransactionAborted { txn_id }),
        Event::Storage(StorageEvent::ReadResult {
            key: key(),
            value: None,
        }),
        Event::Storage(StorageEvent::BatchReadResult { values: Vec::new() }),
        Event::Storage(StorageEvent::WriteResult { key: key() }),
        Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }),
        Event::Storage(StorageEvent::DeleteResult { key: key() }),
        Event::Storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        }),
        Event::Storage(StorageEvent::SyncAllFinished),
        Event::Storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: None,
        }),
        Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        }),
        Event::Task(TaskEvent::Error {
            key: None,
            message: String::new(),
        }),
        Event::SubOperation(SubOperationEvent::NotificationsEmitted),
    ]
}
