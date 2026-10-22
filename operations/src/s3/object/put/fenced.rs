//! Stores write intents in one transaction with a read of their import rule, so a removal,
//! which deletes the rule before it drains the intents, never misses one.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::BACKEND_IMPORT_KEYSPACE;
use aruna_core::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;

/// How a fenced write ended.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Fenced {
    Written,
    /// The rule is gone, so nothing was written.
    RuleGone,
    Failed(Option<StorageError>),
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Step {
    Start,
    Read,
    Write,
    Commit,
    Abort,
    Done,
}

/// The rule read and the writes of one transaction, driven by its host operation.
#[derive(Debug, PartialEq)]
pub(crate) struct FencedWrite {
    rule: Key,
    writes: Vec<(String, Key, Value)>,
    step: Step,
    txn_id: Option<TxnId>,
    outcome: Option<Fenced>,
}

impl FencedWrite {
    pub(crate) fn new(rule: Key, writes: Vec<(String, Key, Value)>) -> Self {
        Self {
            rule,
            writes,
            step: Step::Start,
            txn_id: None,
            outcome: None,
        }
    }

    pub(crate) fn start(&mut self) -> Effects {
        self.step = Step::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    /// Whether the transaction has not started yet.
    #[cfg(test)]
    pub(crate) fn starting(&self) -> bool {
        self.step == Step::Start
    }

    /// How the write ended, once it did.
    pub(crate) fn outcome(&self) -> Option<&Fenced> {
        self.outcome.as_ref().filter(|_| self.step == Step::Done)
    }

    pub(crate) fn step(&mut self, event: Event) -> Effects {
        match (self.step, event) {
            (Step::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                self.step = Step::Read;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: BACKEND_IMPORT_KEYSPACE.to_string(),
                    key: self.rule.clone(),
                    txn_id: Some(txn_id),
                })]
            }
            (Step::Read, Event::Storage(StorageEvent::ReadResult { value: Some(_), .. })) => {
                self.step = Step::Write;
                smallvec![Effect::Storage(StorageEffect::BatchWrite {
                    writes: std::mem::take(&mut self.writes),
                    txn_id: self.txn_id,
                })]
            }
            (Step::Read, Event::Storage(StorageEvent::ReadResult { value: None, .. })) => {
                self.abort(Fenced::RuleGone)
            }
            (Step::Write, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                let Some(txn_id) = self.txn_id else {
                    return self.finish(Fenced::Failed(None));
                };
                self.step = Step::Commit;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (Step::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn_id = None;
                self.finish(Fenced::Written)
            }
            (
                Step::Abort,
                Event::Storage(
                    StorageEvent::TransactionAborted { .. } | StorageEvent::Error { .. },
                ),
            ) => {
                self.txn_id = None;
                self.step = Step::Done;
                smallvec![]
            }
            // A failed commit leaves no transaction to abort.
            (Step::Commit, Event::Storage(StorageEvent::Error { error })) => {
                self.txn_id = None;
                self.finish(Fenced::Failed(Some(error)))
            }
            (_, Event::Storage(StorageEvent::Error { error })) => {
                self.abort(Fenced::Failed(Some(error)))
            }
            _ => self.abort(Fenced::Failed(None)),
        }
    }

    /// The abort of a transaction still open when the host operation is aborted.
    pub(crate) fn abort_open(&mut self) -> Option<Effect> {
        let txn_id = self.txn_id.take()?;
        self.step = Step::Done;
        Some(Effect::Storage(StorageEffect::AbortTransaction { txn_id }))
    }

    fn abort(&mut self, outcome: Fenced) -> Effects {
        self.outcome = Some(outcome);
        match self.txn_id {
            Some(txn_id) => {
                self.step = Step::Abort;
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            None => {
                self.step = Step::Done;
                smallvec![]
            }
        }
    }

    fn finish(&mut self, outcome: Fenced) -> Effects {
        self.outcome = Some(outcome);
        self.step = Step::Done;
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn storage(event: StorageEvent) -> Event {
        Event::Storage(event)
    }

    fn started(fence: &mut FencedWrite) -> Effects {
        fence.start();
        fence.step(storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::from(1),
        }))
    }

    #[test]
    fn writes_with_rule() {
        let row = (
            "intents".to_string(),
            Key::from(vec![2]),
            Value::from(vec![3]),
        );
        let mut fence = FencedWrite::new(Key::from(vec![1]), vec![row.clone()]);
        let effects = started(&mut fence);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read {
                txn_id: Some(_),
                ..
            })]
        ));
        let effects = fence.step(storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: Some(vec![1].into()),
        }));
        let [
            Effect::Storage(StorageEffect::BatchWrite {
                writes,
                txn_id: Some(_),
            }),
        ] = effects.as_slice()
        else {
            panic!("expected the writes in the transaction, got {effects:?}")
        };
        assert_eq!(writes, &vec![row]);
        assert!(fence.outcome().is_none());
        fence.step(storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        fence.step(storage(StorageEvent::TransactionCommitted {
            txn_id: TxnId::from(1),
        }));
        assert_eq!(fence.outcome(), Some(&Fenced::Written));
    }

    #[test]
    fn skips_removed_rule() {
        // A removed rule gets nothing written, and its transaction is aborted.
        let mut fence = FencedWrite::new(Key::from(vec![1]), Vec::new());
        started(&mut fence);
        let effects = fence.step(storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: None,
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        assert!(fence.outcome().is_none());
        fence.step(storage(StorageEvent::TransactionAborted {
            txn_id: TxnId::from(1),
        }));
        assert_eq!(fence.outcome(), Some(&Fenced::RuleGone));
        // A conflict at commit fails the write.
        let mut fence = FencedWrite::new(Key::from(vec![1]), Vec::new());
        started(&mut fence);
        fence.step(storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: Some(vec![1].into()),
        }));
        fence.step(storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        fence.step(storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        }));
        let conflict = Fenced::Failed(Some(StorageError::TransactionConflict));
        assert_eq!(fence.outcome(), Some(&conflict));
    }
}
