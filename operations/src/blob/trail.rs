//! Drops the delete trail of a removed bucket or group, a page at a time.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::DELETE_TRAIL_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::storage::delete_audit::bucket_prefix;
use aruna_core::types::{Effects, Key, Value};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

const TRAIL_PAGE: usize = 500;

#[derive(Debug, Error, PartialEq)]
pub enum TrailError {
    #[error("could not drop trail rows: {0}")]
    Storage(#[from] StorageError),
    #[error("unexpected event while dropping trail rows: {0}")]
    UnexpectedEvent(String),
    #[error("the trail drop did not finish")]
    NotFinished,
}

#[derive(Debug, PartialEq)]
enum TrailState {
    Init,
    ReadPage,
    DeletePage { last: bool },
    Finish,
    Error,
}

/// Deletes the trail rows of one incarnation of `bucket`, or of the whole group without one.
/// A bucket created again under the name has its own rows, which stay. An interrupted pass
/// leaves the rest for the next one.
#[derive(Debug, PartialEq)]
pub(crate) struct DropTrailOperation {
    prefix: Key,
    state: TrailState,
    error: Option<TrailError>,
}

impl DropTrailOperation {
    pub(crate) fn new(group_id: Ulid, bucket: Option<&str>, incarnation: u128) -> Self {
        let prefix = match bucket {
            Some(bucket) => bucket_prefix(group_id, bucket, incarnation).into(),
            None => group_id.to_bytes().to_vec().into(),
        };
        Self {
            prefix,
            state: TrailState::Init,
            error: None,
        }
    }

    /// Always reads from the prefix start, since the previous page is gone by then.
    fn read_page(&mut self) -> Effects {
        self.state = TrailState::ReadPage;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: DELETE_TRAIL_KEYSPACE.to_string(),
            prefix: Some(self.prefix.clone()),
            start: None,
            limit: TRAIL_PAGE,
            txn_id: None,
        })]
    }

    fn page_read(&mut self, rows: Vec<(Key, Value)>, next: Option<Key>) -> Effects {
        if rows.is_empty() {
            return self.after_page(next.is_none());
        }
        let deletes = rows
            .into_iter()
            .map(|(key, _)| (DELETE_TRAIL_KEYSPACE.to_string(), key))
            .collect();
        self.state = TrailState::DeletePage {
            last: next.is_none(),
        };
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes,
            txn_id: None,
        })]
    }

    fn after_page(&mut self, last: bool) -> Effects {
        if last {
            self.state = TrailState::Finish;
            return smallvec![];
        }
        self.read_page()
    }

    fn fail(&mut self, error: TrailError) -> Effects {
        self.state = TrailState::Error;
        self.error = Some(error);
        smallvec![]
    }
}

impl Operation for DropTrailOperation {
    type Output = ();
    type Error = TrailError;

    fn start(&mut self) -> Effects {
        self.read_page()
    }

    fn step(&mut self, event: Event) -> Effects {
        match (&self.state, event) {
            (
                TrailState::ReadPage | TrailState::DeletePage { .. },
                Event::Storage(StorageEvent::Error { error }),
            ) => self.fail(error.into()),
            (
                TrailState::ReadPage,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => self.page_read(values, next_start_after),
            (
                &TrailState::DeletePage { last },
                Event::Storage(StorageEvent::BatchDeleteResult { .. }),
            ) => self.after_page(last),
            (_, event) => self.fail(TrailError::UnexpectedEvent(event.label().to_string())),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, TrailState::Finish | TrailState::Error)
    }

    fn finalize(self) -> Result<(), TrailError> {
        match (self.state, self.error) {
            (TrailState::Finish, _) => Ok(()),
            (_, Some(error)) => Err(error),
            _ => Err(TrailError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };

    fn group() -> Ulid {
        Ulid::from_parts(1, 1)
    }

    fn operation() -> DropTrailOperation {
        DropTrailOperation::new(group(), Some("b"), 1)
    }

    fn read() -> Effect {
        Effect::Storage(StorageEffect::Iter {
            key_space: DELETE_TRAIL_KEYSPACE.to_string(),
            prefix: Some(bucket_prefix(group(), "b", 1).into()),
            start: None,
            limit: TRAIL_PAGE,
            txn_id: None,
        })
    }

    fn page(keys: &[&[u8]], next: Option<&[u8]>) -> Event {
        Event::Storage(StorageEvent::IterResult {
            values: (keys.iter())
                .map(|key| (key.to_vec().into(), Vec::new().into()))
                .collect(),
            next_start_after: next.map(|key| key.to_vec().into()),
        })
    }

    fn delete(keys: &[&[u8]]) -> Effect {
        Effect::Storage(StorageEffect::BatchDelete {
            deletes: (keys.iter())
                .map(|key| (DELETE_TRAIL_KEYSPACE.to_string(), key.to_vec().into()))
                .collect(),
            txn_id: None,
        })
    }

    fn deleted() -> Event {
        Event::Storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        })
    }

    #[test]
    fn drops_every_page() {
        // Each page is read again from the start, since the previous one is gone.
        let mut operation = operation();
        let steps = vec![
            (page(&[b"a", b"b"], Some(b"b")), vec![delete(&[b"a", b"b"])]),
            (deleted(), vec![read()]),
            (page(&[b"c"], None), vec![delete(&[b"c"])]),
            (deleted(), vec![]),
        ];
        check_trace(&mut operation, &[read()], steps);
        assert!(operation.is_complete());
        assert_eq!(operation.finalize(), Ok(()));
    }

    #[test]
    fn empty_trail_finishes() {
        let mut operation = operation();
        check_trace(&mut operation, &[read()], vec![(page(&[], None), vec![])]);
        assert_eq!(operation.finalize(), Ok(()));
    }

    #[test]
    fn whole_group_prefix() {
        let mut operation = DropTrailOperation::new(group(), None, 0);
        let [Effect::Storage(StorageEffect::Iter { prefix, .. })] = operation.start().as_slice()
        else {
            panic!("expected one page read");
        };
        assert_eq!(prefix.as_deref(), Some(group().to_bytes().as_slice()));
    }

    #[test]
    fn storage_errors_fail() {
        let error = || {
            Event::Storage(StorageEvent::Error {
                error: StorageError::Timeout,
            })
        };
        let mut reading = operation();
        reading.start();
        assert!(reading.step(error()).is_empty());
        assert_eq!(
            reading.finalize(),
            Err(TrailError::Storage(StorageError::Timeout))
        );
        let mut deleting = operation();
        deleting.start();
        deleting.step(page(&[b"a"], None));
        assert!(deleting.step(error()).is_empty());
        assert_eq!(
            deleting.finalize(),
            Err(TrailError::Storage(StorageError::Timeout))
        );
    }

    #[test]
    fn rejects_unexpected_events() {
        let reading = || {
            let mut operation = operation();
            operation.start();
            operation
        };
        let refused = rejects_events(reading, sample_events, |event| {
            matches!(
                event,
                Event::Storage(StorageEvent::IterResult { .. } | StorageEvent::Error { .. })
            )
        });
        assert!(refused.iter().all(|(_, effects, _)| effects.is_empty()));
        let deleting = || {
            let mut operation = operation();
            operation.start();
            operation.step(page(&[b"a"], None));
            operation
        };
        let refused = rejects_events(deleting, sample_events, |event| {
            matches!(
                event,
                Event::Storage(StorageEvent::BatchDeleteResult { .. } | StorageEvent::Error { .. })
            )
        });
        assert!(!refused.is_empty());
        // Before start nothing is expected at all.
        assert_eq!(
            rejects_events(operation, sample_events, |_| false).len(),
            sample_events().len()
        );
    }

    #[test]
    fn abort_and_finalize() {
        // No transaction is held, so abort has nothing to clean up in any phase.
        let mut fresh = operation();
        check_abort(&mut fresh, &[]);
        rejects_finalize(fresh);
        let mut reading = operation();
        reading.start();
        check_abort(&mut reading, &[]);
        rejects_finalize(reading);
        let mut deleting = operation();
        deleting.start();
        deleting.step(page(&[b"a"], Some(b"a")));
        check_abort(&mut deleting, &[]);
        rejects_finalize(deleting);
    }
}
