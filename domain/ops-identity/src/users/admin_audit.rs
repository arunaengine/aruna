//! Reads one bounded page of this node's administrative audit rows for the realm, a group or an
//! owner's secrets.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::ADMIN_AUDIT_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::admin_audit::{
    AdminAuditRecord, AuditIndex, audit_key, audit_prefix,
};
use thiserror::Error;
use ulid::Ulid;

pub const MAX_AUDIT_LIMIT: usize = 200;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuditPageInput {
    /// The realm rows hold every recorded change.
    pub index: AuditIndex,
    pub start_after: Option<Ulid>,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuditPage {
    pub records: Vec<AdminAuditRecord>,
    /// Set when more rows follow; pass it as `start_after` for the next page.
    pub next: Option<Ulid>,
}

#[derive(Debug, Error, PartialEq)]
pub enum AuditPageError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("audit page read did not finish")]
    NotFinished,
    #[error("unexpected event while reading audit rows: {0}")]
    UnexpectedEvent(String),
    #[error(transparent)]
    Io(#[from] IoError),
}

#[derive(Debug, PartialEq)]
pub struct AuditPageOperation {
    input: AuditPageInput,
}

impl AuditPageOperation {
    pub fn new(mut input: AuditPageInput) -> Self {
        input.limit = input.limit.clamp(1, MAX_AUDIT_LIMIT);
        Self { input }
    }

    fn page(
        &self,
        values: Vec<(aruna_core::types::Key, aruna_core::types::Value)>,
    ) -> Result<AuditPage, AuditPageError> {
        let more = values.len() > self.input.limit;
        let records = values
            .iter()
            .take(self.input.limit)
            .map(|(_, value)| AdminAuditRecord::from_bytes(value))
            .collect::<Result<Vec<_>, _>>()?;
        let next = match more {
            true => records.last().map(|record| record.audit_id),
            false => None,
        };
        Ok(AuditPage { records, next })
    }
}

impl Operation for AuditPageOperation {
    type Output = AuditPage;
    type Error = AuditPageError;

    fn first(&mut self) -> Result<Next<AuditPage>, AuditPageError> {
        let index = self.input.index;
        let after = (self.input.start_after).map(|audit_id| audit_key(index, audit_id).into());
        Ok(Next::Call(IoCall::Page {
            space: ADMIN_AUDIT_KEYSPACE.to_string(),
            prefix: audit_prefix(index).into(),
            after,
            limit: self.input.limit + 1,
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<AuditPage>, AuditPageError> {
        match result {
            IoResult::Page { rows, .. } => Ok(Next::Done(self.page(rows)?)),
            _ => Err(AuditPageError::UnexpectedEvent(
                "another result".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::types::{Key, Value};

    fn input(limit: usize) -> AuditPageInput {
        AuditPageInput {
            index: AuditIndex::Realm,
            start_after: Some(Ulid::from_bytes([2u8; 16])),
            limit,
        }
    }

    #[test]
    fn reads_one_page() {
        // One page after the cursor, one row more than asked to tell whether more follow.
        let mut operation = AuditPageOperation::new(input(5));
        let index = AuditIndex::Realm;
        let page = IoCall::Page {
            space: ADMIN_AUDIT_KEYSPACE.to_string(),
            prefix: audit_prefix(index).into(),
            after: Some(audit_key(index, Ulid::from_bytes([2u8; 16])).into()),
            limit: 6,
        };
        assert_eq!(operation.first(), Ok(Next::Call(page)));
        let empty = IoResult::Page {
            rows: Vec::new(),
            next: None,
        };
        let done = AuditPage {
            records: Vec::new(),
            next: None,
        };
        assert_eq!(operation.next(empty), Ok(Next::Done(done)));
    }

    #[test]
    fn page_failures_end() {
        // An undecodable row or an answer that is not a page ends the read.
        let mut operation = AuditPageOperation::new(input(5));
        operation.first().unwrap();
        let rows = vec![(Key::from(vec![1u8]), Value::from(vec![0xff; 3]))];
        let broken = IoResult::Page { rows, next: None };
        assert!(matches!(
            operation.next(broken),
            Err(AuditPageError::Conversion(_))
        ));
        let mut operation = AuditPageOperation::new(input(5));
        operation.first().unwrap();
        assert!(matches!(
            operation.next(IoResult::Done),
            Err(AuditPageError::UnexpectedEvent(_))
        ));
    }
}
