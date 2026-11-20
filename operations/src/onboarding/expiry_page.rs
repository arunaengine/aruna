//! Lists onboarding secrets soonest expiry first, one page of the expiry index at a time.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::ONBOARDING_KEYSPACE;
use aruna_core::onboarding::OnboardingSecretRecord;
use aruna_core::operation::{Operation, TransactionMode};
use byteview::ByteView;
use ulid::Ulid;

use crate::onboarding::create_secret::{SECRET_EXPIRY_PREFIX, secret_record_key};
use crate::onboarding::list_secrets::{ListSecretsError, OnboardingSecretEntry, SecretPage};
use crate::onboarding::secret_state::{resolve_secret_state, secret_state_key};

/// One page of `limit` index rows after the index key `after`, read with their records and
/// states in one snapshot. `next` is the last index key of a page that may have more.
#[derive(Debug, PartialEq)]
pub struct ExpiryPageOperation {
    phase: Phase,
    after: Option<ByteView>,
    limit: usize,
    ids: Vec<Ulid>,
    next: Option<ByteView>,
}

/// The answer the operation waits for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Created,
    Index,
    Values,
    Done,
}

impl ExpiryPageOperation {
    pub fn new(after: Option<ByteView>, limit: usize) -> Self {
        Self {
            phase: Phase::Created,
            after,
            limit,
            ids: Vec::new(),
            next: None,
        }
    }

    /// Whether `key` is a key of the expiry index, so a caller's cursor can be checked.
    pub fn index_key(key: &[u8]) -> bool {
        key.starts_with(SECRET_EXPIRY_PREFIX.as_bytes())
    }

    /// The record and state reads of the page's enrollments.
    fn reads(&mut self, rows: Vec<(ByteView, ByteView)>) -> Result<IoCall, ListSecretsError> {
        let mut reads = Vec::with_capacity(rows.len() * 2);
        for (key, _) in rows {
            let id = (key.len().checked_sub(16)).and_then(|start| key.get(start..));
            let id = id.and_then(|bytes| <[u8; 16]>::try_from(bytes).ok());
            let id = Ulid::from_bytes(id.ok_or_else(|| {
                ConversionError::InvalidOperationConversion("short expiry index key".into())
            })?);
            reads.push((ONBOARDING_KEYSPACE.to_string(), secret_record_key(id)));
            reads.push((ONBOARDING_KEYSPACE.to_string(), secret_state_key(id)));
            self.ids.push(id);
        }
        Ok(IoCall::Read(reads))
    }
}

impl Operation for ExpiryPageOperation {
    type Output = SecretPage;
    type Error = ListSecretsError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::ReadSnapshot
    }

    fn first(&mut self) -> Result<Next<SecretPage>, ListSecretsError> {
        self.phase = Phase::Index;
        Ok(Next::Call(IoCall::Page {
            space: ONBOARDING_KEYSPACE.to_string(),
            prefix: ByteView::from(SECRET_EXPIRY_PREFIX.as_bytes()),
            after: self.after.take(),
            limit: self.limit,
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<SecretPage>, ListSecretsError> {
        let values = match (self.phase, result) {
            (Phase::Index, IoResult::Page { rows, next }) => {
                self.phase = Phase::Values;
                self.next = next;
                return Ok(Next::Call(self.reads(rows)?));
            }
            (Phase::Values, IoResult::Values(values)) => {
                self.phase = Phase::Done;
                values
            }
            (phase, other) => {
                return Err(ListSecretsError::UnexpectedEvent {
                    state: format!("{phase:?}"),
                    expected: "the index page, then the record and state batch",
                    got: other.label(),
                });
            }
        };
        if values.len() != self.ids.len() * 2 {
            return Err(ListSecretsError::UnexpectedEvent {
                state: "ExpiryPage".to_string(),
                expected: "a record and a state per index row",
                got: "a batch of another length",
            });
        }
        let mut entries = Vec::with_capacity(self.ids.len());
        for pair in values.chunks(2) {
            // The index commits with its record, so a row without one is skipped, not fatal.
            let Some(record) = &pair[0] else {
                continue;
            };
            let record: OnboardingSecretRecord =
                postcard::from_bytes(record).map_err(ConversionError::from)?;
            let state = resolve_secret_state(&record, pair[1].as_ref())?;
            entries.push(OnboardingSecretEntry { record, state });
        }
        let next = self.next.take();
        Ok(Next::Done(SecretPage { entries, next }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::onboarding::create_secret::secret_expiry_key;

    // Each answer is taken only in the phase that asked for it.
    #[test]
    fn wrong_order_refused() {
        let page = || IoResult::Page {
            rows: Vec::new(),
            next: None,
        };
        let mut early = ExpiryPageOperation::new(None, 2);
        early.first().unwrap();
        let refused = early.next(IoResult::Values(Vec::new()));
        assert!(matches!(refused, Err(ListSecretsError::UnexpectedEvent { .. })));
        let mut twice = ExpiryPageOperation::new(None, 2);
        twice.first().unwrap();
        twice.next(page()).unwrap();
        let refused = twice.next(page());
        assert!(matches!(refused, Err(ListSecretsError::UnexpectedEvent { .. })));
        let mut done = ExpiryPageOperation::new(None, 2);
        done.first().unwrap();
        done.next(page()).unwrap();
        done.next(IoResult::Values(Vec::new())).unwrap();
        let refused = done.next(IoResult::Values(Vec::new()));
        assert!(matches!(refused, Err(ListSecretsError::UnexpectedEvent { .. })));
    }

    #[test]
    fn pages_by_expiry() {
        // A page reads the index after its cursor, then each enrollment's two rows in order.
        let after = secret_expiry_key(5, Ulid::from_bytes([1; 16]));
        let mut operation = ExpiryPageOperation::new(Some(after.clone()), 2);
        assert_eq!(operation.transaction(), TransactionMode::ReadSnapshot);
        let Ok(Next::Call(IoCall::Page {
            after: asked,
            limit,
            ..
        })) = operation.first()
        else {
            panic!("expected an index page");
        };
        assert_eq!((asked, limit), (Some(after), 2));
        let (first, second) = (Ulid::from_bytes([9; 16]), Ulid::from_bytes([2; 16]));
        let rows = vec![
            (secret_expiry_key(6, first), ByteView::from(Vec::new())),
            (secret_expiry_key(7, second), ByteView::from(Vec::new())),
        ];
        let next = Some(secret_expiry_key(7, second));
        let Ok(Next::Call(IoCall::Read(keys))) = operation.next(IoResult::Page { rows, next })
        else {
            panic!("expected the record and state reads");
        };
        assert_eq!(keys[0].1, secret_record_key(first));
        assert_eq!(keys[2].1, secret_record_key(second));
        let missing = IoResult::Values(vec![None; 4]);
        let Ok(Next::Done(page)) = operation.next(missing) else {
            panic!("expected the page");
        };
        assert!(page.entries.is_empty());
        assert_eq!(page.next, Some(secret_expiry_key(7, second)));
        assert!(ExpiryPageOperation::index_key(&secret_expiry_key(1, first)));
        assert!(!ExpiryPageOperation::index_key(&secret_record_key(first)));
    }
}
