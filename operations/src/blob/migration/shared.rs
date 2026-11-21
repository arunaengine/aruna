//! The error and the small reads the migration operations share.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::fmt::Display;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::BUCKET_ENCRYPTION_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::read_row;
use aruna_core::structs::storage::encryption::BucketEncryption;
use aruna_core::structs::storage::routing::{NodeRouting, RoutingSnapshot};
use aruna_core::types::Value;
use thiserror::Error;
use ulid::Ulid;

use super::Earlier;
use crate::blob::import_rows::{BucketRoutingOperation, ImportRowsError};
use crate::s3::imports::{ImportRuleError, ReadEarlierOperation};

#[derive(Debug, Error, PartialEq)]
pub enum MigrationError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("{0}")]
    Failed(String),
}

impl From<ImportRowsError> for MigrationError {
    fn from(error: ImportRowsError) -> Self {
        Self::failed(error)
    }
}

impl MigrationError {
    pub fn failed(error: impl Display) -> Self {
        Self::Failed(error.to_string())
    }
}

/// Reads the encryption settings of `bucket`; [`is_encrypted`] takes the answer.
pub(crate) fn encryption_read(bucket: &str) -> IoCall {
    read_row(BUCKET_ENCRYPTION_KEYSPACE, bucket.as_bytes().to_vec())
}

pub(crate) fn is_encrypted(value: Option<Value>) -> Result<bool, MigrationError> {
    Ok(BucketEncryption::from_row(value.as_deref())?.is_encrypted())
}

/// Resolves the routing a move of `bucket` uses; [`routing_result`] takes the answer.
pub(crate) fn routing_call(routing: NodeRouting, bucket: &str) -> IoCall {
    IoCall::child(BucketRoutingOperation::new(routing, bucket))
}

pub(crate) fn routing_result(result: IoResult) -> Result<Option<RoutingSnapshot>, MigrationError> {
    (result.child::<Option<RoutingSnapshot>, ImportRowsError>()?).map_err(MigrationError::failed)
}

/// Reads the earlier patterns of the rules a move resolves against.
#[derive(Debug, PartialEq)]
pub struct EarlierPatternsOperation {
    rules: VecDeque<Ulid>,
    current: Option<Ulid>,
    earlier: Earlier,
}

type EarlierTurn = Result<Next<Earlier>, MigrationError>;

impl EarlierPatternsOperation {
    pub fn new(routing: &RoutingSnapshot) -> Self {
        Self {
            rules: routing.imports.iter().map(|rule| rule.import_id).collect(),
            current: None,
            earlier: Earlier::new(),
        }
    }

    fn next_rule(&mut self) -> EarlierTurn {
        self.current = self.rules.pop_front();
        match self.current {
            Some(rule) => Ok(Next::Call(IoCall::child(ReadEarlierOperation::new(rule)))),
            None => Ok(Next::Done(std::mem::take(&mut self.earlier))),
        }
    }
}

impl Operation for EarlierPatternsOperation {
    type Output = Earlier;
    type Error = MigrationError;

    fn first(&mut self) -> EarlierTurn {
        self.next_rule()
    }

    fn next(&mut self, result: IoResult) -> EarlierTurn {
        let patterns =
            (result.child::<Vec<String>, ImportRuleError>()?).map_err(MigrationError::failed)?;
        if let Some(rule) = self.current.filter(|_| !patterns.is_empty()) {
            self.earlier.insert(rule, patterns);
        }
        self.next_rule()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;

    fn answer(patterns: Vec<String>) -> IoResult {
        let outcome: Result<Vec<String>, ImportRuleError> = Ok(patterns);
        IoResult::Child(ChildOutcome::new(outcome))
    }

    #[test]
    fn keeps_pattern_rules() {
        let (first, second) = (Ulid::from_bytes([1; 16]), Ulid::from_bytes([2; 16]));
        let mut operation = EarlierPatternsOperation {
            rules: VecDeque::from([first, second]),
            current: None,
            earlier: Earlier::new(),
        };
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let turn = operation.next(answer(vec!["old".to_string()]));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
        let expected = Earlier::from([(first, vec!["old".to_string()])]);
        assert_eq!(operation.next(answer(Vec::new())), Ok(Next::Done(expected)));
    }

    #[test]
    fn missing_settings_unencrypted() {
        assert_eq!(is_encrypted(None), Ok(false));
    }
}
