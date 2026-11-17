//! Reads one bounded page of quarantine rows together with the usage counters.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::SyncQuarantineRecord;
use aruna_core::types::Effects;
use smallvec::smallvec;

use super::{
    QuarantineAdminError, QuarantinePage, QuarantinePageRequest, decode_usage, iter_effect,
    unexpected, usage_read,
};

#[derive(Debug, PartialEq)]
enum ListState {
    Ready(QuarantinePageRequest),
    Scanning,
    ReadingUsage {
        records: Vec<SyncQuarantineRecord>,
        next_start_after: Option<Vec<u8>>,
    },
    Done(Result<QuarantinePage, QuarantineAdminError>),
}

/// One page of quarantine evidence in key order, with the usage read after the page.
#[derive(Debug, PartialEq)]
pub struct ListQuarantineOperation {
    state: ListState,
}

impl ListQuarantineOperation {
    pub fn new(request: QuarantinePageRequest) -> Self {
        Self {
            state: ListState::Ready(request),
        }
    }

    fn fail(&mut self, error: QuarantineAdminError) -> Effects {
        self.state = ListState::Done(Err(error));
        Effects::new()
    }
}

impl Operation for ListQuarantineOperation {
    type Output = QuarantinePage;
    type Error = QuarantineAdminError;

    fn start(&mut self) -> Effects {
        let ListState::Ready(request) = &self.state else {
            return Effects::new();
        };
        let effect = iter_effect(request);
        self.state = ListState::Scanning;
        smallvec![effect]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (std::mem::replace(&mut self.state, ListState::Scanning), event) {
            (
                ListState::Scanning,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => {
                let records = values
                    .into_iter()
                    .map(|(_, value)| SyncQuarantineRecord::from_bytes(value.as_ref()))
                    .collect::<Result<Vec<_>, _>>();
                match records {
                    Ok(records) => {
                        self.state = ListState::ReadingUsage {
                            records,
                            next_start_after: next_start_after.map(|key| key.as_ref().to_vec()),
                        };
                        smallvec![usage_read(None)]
                    }
                    Err(error) => self.fail(error.into()),
                }
            }
            (
                ListState::ReadingUsage {
                    records,
                    next_start_after,
                },
                Event::Storage(StorageEvent::ReadResult { value, .. }),
            ) => {
                let page = decode_usage(value).map(|usage| QuarantinePage {
                    records,
                    next_start_after,
                    usage,
                });
                self.state = ListState::Done(page);
                Effects::new()
            }
            (ListState::ReadingUsage { .. }, event) => {
                self.fail(unexpected("storage read result", event))
            }
            (_, event) => self.fail(unexpected("storage iteration result", event)),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ListState::Done(_))
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            ListState::Done(result) => result,
            _ => Err(QuarantineAdminError::Unfinished),
        }
    }

    fn abort(&mut self) -> Effects {
        self.fail(QuarantineAdminError::Unfinished)
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{record, record_bytes, usage_bytes};
    use super::*;
    use aruna_core::effects::Effect;
    use aruna_core::errors::StorageError;
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use aruna_core::structs::SyncQuarantineUsage;

    fn request() -> QuarantinePageRequest {
        QuarantinePageRequest {
            start_after: Some(vec![1]),
            topic: Some(vec![2]),
            limit: Some(500),
        }
    }

    fn scanning() -> ListQuarantineOperation {
        let mut operation = ListQuarantineOperation::new(request());
        operation.start();
        operation
    }

    fn page(next: Option<Vec<u8>>) -> Event {
        Event::Storage(StorageEvent::IterResult {
            values: vec![(vec![7].into(), record_bytes(&record(0, false)))],
            next_start_after: next.map(Into::into),
        })
    }

    #[test]
    fn lists_page_usage() {
        let usage = SyncQuarantineUsage {
            records: 3,
            bytes: 90,
        };
        let mut operation = ListQuarantineOperation::new(request());
        check_trace(
            &mut operation,
            &[iter_effect(&request())],
            vec![
                (page(Some(vec![7])), vec![usage_read(None)]),
                (
                    Event::Storage(StorageEvent::ReadResult {
                        key: Vec::<u8>::new().into(),
                        value: Some(usage_bytes(usage)),
                    }),
                    vec![],
                ),
            ],
        );
        let page = operation.finalize().expect("page reads");
        assert_eq!(page.records, vec![record(0, false)]);
        assert_eq!(page.next_start_after, Some(vec![7]));
        assert_eq!(page.usage, usage);
        let Effect::Storage(aruna_core::effects::StorageEffect::Iter { limit, .. }) =
            iter_effect(&request())
        else {
            panic!("list starts with an iteration");
        };
        assert_eq!(limit, super::super::QUARANTINE_PAGE_MAX);
    }

    #[test]
    fn storage_errors_fail() {
        let mut operation = scanning();
        operation.step(Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        }));
        assert_eq!(
            operation.finalize(),
            Err(QuarantineAdminError::Storage(StorageError::Timeout))
        );
        let mut corrupt = scanning();
        corrupt.step(Event::Storage(StorageEvent::IterResult {
            values: vec![(vec![7].into(), vec![0xff].into())],
            next_start_after: None,
        }));
        assert!(matches!(
            corrupt.finalize(),
            Err(QuarantineAdminError::Conversion(_))
        ));
    }

    #[test]
    fn rejects_foreign_events() {
        let refusals = rejects_events(scanning, sample_events, |event| {
            matches!(
                event,
                Event::Storage(StorageEvent::IterResult { .. } | StorageEvent::Error { .. })
            )
        });
        for (label, effects, error) in refusals {
            assert!(effects.is_empty(), "{label} emitted effects");
            assert!(
                matches!(error, QuarantineAdminError::Unexpected { .. }),
                "{label}"
            );
        }
        let reading = || {
            let mut operation = scanning();
            operation.step(page(None));
            operation
        };
        let refusals = rejects_events(reading, sample_events, |event| {
            matches!(
                event,
                Event::Storage(StorageEvent::ReadResult { .. } | StorageEvent::Error { .. })
            )
        });
        assert!(!refusals.is_empty());
    }

    #[test]
    fn abort_and_finalize() {
        rejects_finalize(ListQuarantineOperation::new(request()));
        rejects_finalize(scanning());
        let mut operation = scanning();
        check_abort(&mut operation, &[]);
        assert_eq!(operation.finalize(), Err(QuarantineAdminError::Unfinished));
    }
}
