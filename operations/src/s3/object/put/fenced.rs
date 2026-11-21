//! Stores write intents in one transaction with a read of their import rule, so a removal,
//! which deletes the rule before it drains the intents, never misses one.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::errors::StorageError;
use aruna_core::keyspaces::BACKEND_IMPORT_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::read_row;
use aruna_core::types::{Key, Value};

/// How a fenced write ended.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Fenced {
    Written,
    /// The rule is gone, so nothing was written.
    RuleGone,
}

/// The rule read and the writes of one transaction, run as a child. A refused read, write or
/// commit fails the child with its storage error.
#[derive(Debug, PartialEq)]
pub(crate) struct FencedWrite {
    rule: Key,
    writes: Vec<(String, Key, Value)>,
    writing: bool,
}

impl FencedWrite {
    pub(crate) fn new(rule: Key, writes: Vec<(String, Key, Value)>) -> Self {
        Self {
            rule,
            writes,
            writing: false,
        }
    }
}

impl Operation for FencedWrite {
    type Output = Fenced;
    type Error = StorageError;

    fn first(&mut self) -> Result<Next<Fenced>, StorageError> {
        Ok(Next::Call(read_row(
            BACKEND_IMPORT_KEYSPACE,
            self.rule.clone(),
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Fenced>, StorageError> {
        if self.writing {
            result.done()?;
            return Ok(Next::Done(Fenced::Written));
        }
        if result.value()?.is_none() {
            return Ok(Next::Done(Fenced::RuleGone));
        }
        self.writing = true;
        Ok(Next::Call(IoCall::Write(std::mem::take(&mut self.writes))))
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_with_rule() {
        let row = (
            "intents".to_string(),
            Key::from(vec![2]),
            Value::from(vec![3]),
        );
        let mut fence = FencedWrite::new(Key::from(vec![1]), vec![row.clone()]);
        assert_eq!(fence.transaction(), TransactionMode::Write);
        let Ok(Next::Call(IoCall::Read(reads))) = fence.first() else {
            panic!("expected the rule read")
        };
        assert_eq!(reads[0].0, BACKEND_IMPORT_KEYSPACE);
        let turn = fence.next(IoResult::Values(vec![Some(vec![1].into())]));
        assert_eq!(turn, Ok(Next::Call(IoCall::Write(vec![row]))));
        assert_eq!(fence.next(IoResult::Done), Ok(Next::Done(Fenced::Written)));

        // A removed rule gets nothing written.
        let mut fence = FencedWrite::new(Key::from(vec![1]), Vec::new());
        fence.first().unwrap();
        let turn = fence.next(IoResult::Values(vec![None]));
        assert_eq!(turn, Ok(Next::Done(Fenced::RuleGone)));
    }
}
