//! Reads one bounded page of this node's administrative audit rows for the realm or a group.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::ADMIN_AUDIT_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::admin_audit::{AdminAuditRecord, audit_key, audit_prefix};
use aruna_core::types::{Effects, GroupId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

pub const MAX_AUDIT_LIMIT: usize = 200;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuditPageInput {
    /// `None` reads the realm rows, which hold every recorded change.
    pub group_id: Option<GroupId>,
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
}

#[derive(Debug, PartialEq)]
pub struct AuditPageOperation {
    input: AuditPageInput,
    output: Option<Result<AuditPage, AuditPageError>>,
}

impl AuditPageOperation {
    pub fn new(mut input: AuditPageInput) -> Self {
        input.limit = input.limit.clamp(1, MAX_AUDIT_LIMIT);
        Self {
            input,
            output: None,
        }
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

    fn start(&mut self) -> Effects {
        let group_id = self.input.group_id;
        let start = self
            .input
            .start_after
            .map(|audit_id| IterStart::After(audit_key(group_id, audit_id).into()));
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: ADMIN_AUDIT_KEYSPACE.to_string(),
            prefix: Some(audit_prefix(group_id).into()),
            start,
            limit: self.input.limit + 1,
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if self.output.is_some() {
            self.output = Some(Err(AuditPageError::UnexpectedEvent(format!("{event:?}"))));
            return smallvec![];
        }
        self.output = Some(match event {
            Event::Storage(StorageEvent::IterResult { values, .. }) => self.page(values),
            Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
            other => Err(AuditPageError::UnexpectedEvent(format!("{other:?}"))),
        });
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(AuditPageError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
