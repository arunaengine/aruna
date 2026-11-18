//! Spends the single-use nonce of a delegation, ticket or granted read once, durably until
//! it expires, and prunes the expired nonce rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::DELEGATION_NONCE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::{PrefixPages, done, read_row, values};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum NonceError {
    #[error("the nonce was already used")]
    Replayed,
    #[error("the nonce store is unavailable")]
    Unavailable,
}

/// A storage failure while spending: a conflict means another acceptance spent the nonce.
impl From<IoError> for NonceError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => NonceError::Replayed,
            _ => NonceError::Unavailable,
        }
    }
}

/// Expired nonce rows one spend deletes, so the keyspace never grows past live ones.
const PRUNE_PAGE: usize = 64;

/// The row of a spent nonce: expiry second first, so expired rows sort ahead.
fn nonce_key(expires: u64, nonce: [u8; 16]) -> Vec<u8> {
    let mut key = expires.to_be_bytes().to_vec();
    key.extend_from_slice(&nonce);
    key
}

/// Records a nonce as spent in one transaction, durably until it expires, so a restart never
/// accepts it again; after the commit it deletes one bounded page of nonce rows that expired
/// before `now`. A failed prune does not fail the spend.
#[derive(Debug, PartialEq)]
pub struct SpendNonceOperation {
    key: Vec<u8>,
    now: u64,
    written: bool,
}

impl SpendNonceOperation {
    pub fn new(nonce: [u8; 16], expires: u64, now: u64) -> Self {
        Self {
            key: nonce_key(expires, nonce),
            now,
            written: false,
        }
    }
}

impl Operation for SpendNonceOperation {
    type Output = ();
    type Error = NonceError;

    fn first(&mut self) -> Result<Next<()>, NonceError> {
        Ok(Next::Call(read_row(
            DELEGATION_NONCE_KEYSPACE,
            self.key.clone(),
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, NonceError> {
        if self.written {
            done(result)?;
            let prune = IoCall::child(PruneNoncesOperation::new(self.now));
            return Ok(Next::Then {
                output: (),
                calls: vec![prune],
            });
        }
        if values::<1>(result)?[0].is_some() {
            return Err(NonceError::Replayed);
        }
        self.written = true;
        let row = (
            DELEGATION_NONCE_KEYSPACE.to_string(),
            self.key.clone().into(),
            Vec::new().into(),
        );
        Ok(Next::Call(IoCall::Write(vec![row])))
    }

    fn transaction(&self) -> aruna_core::operation::TransactionMode {
        aruna_core::operation::TransactionMode::Write
    }

    /// A replay is final; a fresh run would read the same spent row.
    fn is_conflict(_error: &NonceError) -> bool {
        false
    }
}

/// Deletes the nonce rows of the first page that expired before `now`.
#[derive(Debug, PartialEq)]
struct PruneNoncesOperation {
    now: u64,
    pages: PrefixPages,
    deleting: bool,
}

impl PruneNoncesOperation {
    fn new(now: u64) -> Self {
        Self {
            now,
            pages: PrefixPages::new(DELEGATION_NONCE_KEYSPACE, Vec::new(), PRUNE_PAGE),
            deleting: false,
        }
    }
}

impl Operation for PruneNoncesOperation {
    type Output = ();
    type Error = NonceError;

    fn first(&mut self) -> Result<Next<()>, NonceError> {
        Ok(Next::Call(self.pages.call()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, NonceError> {
        if self.deleting {
            done(result)?;
            return Ok(Next::Done(()));
        }
        let expired = |key: &[u8]| {
            (key.get(..8))
                .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
                .is_some_and(|expires| u64::from_be_bytes(expires) < self.now)
        };
        let deletes: Vec<_> = (self.pages.take(result)?.into_iter())
            .filter(|(key, _)| expired(key))
            .map(|(key, _)| (DELEGATION_NONCE_KEYSPACE.to_string(), key))
            .collect();
        if deletes.is_empty() {
            return Ok(Next::Done(()));
        }
        self.deleting = true;
        Ok(Next::Call(IoCall::Delete(deletes)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::test_store::{context, get, open};
    use crate::driver::{DriverContext, drive};

    async fn spend(
        context: &DriverContext,
        nonce: u8,
        expires: u64,
        now: u64,
    ) -> Result<(), NonceError> {
        drive(SpendNonceOperation::new([nonce; 16], expires, now), context).await
    }

    #[tokio::test]
    async fn nonces_spend_once() {
        let (_dir, handle) = open();
        let context = context(&handle);
        assert_eq!(spend(&context, 1, 50, 0).await, Ok(()));
        assert_eq!(spend(&context, 1, 50, 0).await, Err(NonceError::Replayed));
        assert_eq!(spend(&context, 2, 50, 0).await, Ok(()));
    }

    #[tokio::test]
    async fn expired_nonces_pruned() {
        let (_dir, handle) = open();
        let context = context(&handle);
        spend(&context, 1, 10, 0).await.unwrap();
        spend(&context, 2, 90, 50).await.unwrap();
        let key = DELEGATION_NONCE_KEYSPACE;
        assert!(get(&handle, key, nonce_key(10, [1; 16])).await.is_none());
        assert!(get(&handle, key, nonce_key(90, [2; 16])).await.is_some());
    }

    // A fresh nonce is read and written in the transaction, then pruned after the commit.
    #[test]
    fn spends_then_prunes() {
        let key = nonce_key(50, [1; 16]);
        let mut spend = SpendNonceOperation::new([1; 16], 50, 40);
        assert_eq!(
            spend.transaction(),
            aruna_core::operation::TransactionMode::Write
        );
        let read = read_row(DELEGATION_NONCE_KEYSPACE, key.clone());
        assert_eq!(spend.first(), Ok(Next::Call(read)));
        let row = (
            DELEGATION_NONCE_KEYSPACE.to_string(),
            key.into(),
            Vec::new().into(),
        );
        let fresh = spend.next(IoResult::Values(vec![None]));
        assert_eq!(fresh, Ok(Next::Call(IoCall::Write(vec![row]))));
        let Ok(Next::Then { output: (), calls }) = spend.next(IoResult::Done) else {
            panic!("expected the prune after the commit");
        };
        assert!(matches!(calls.as_slice(), [IoCall::Child(_)]));
    }

    // A spent nonce is a replay; a conflict is one too, other failures leave it unspent.
    #[test]
    fn replays_and_failures() {
        let mut spend = SpendNonceOperation::new([1; 16], 50, 40);
        spend.first().unwrap();
        let spent = IoResult::Values(vec![Some(Vec::new().into())]);
        assert_eq!(spend.next(spent), Err(NonceError::Replayed));
        assert_eq!(NonceError::from(IoError::Conflict), NonceError::Replayed);
        let storage = IoError::Storage(aruna_core::errors::StorageError::Timeout);
        assert_eq!(NonceError::from(storage), NonceError::Unavailable);
    }

    // Only rows that expired before now are deleted.
    #[test]
    fn prunes_expired_rows() {
        let expired = nonce_key(10, [2; 16]);
        let live = nonce_key(50, [1; 16]);
        let mut prune = PruneNoncesOperation::new(40);
        prune.first().unwrap();
        let page = IoResult::Page {
            rows: vec![
                (expired.clone().into(), Vec::new().into()),
                (live.into(), Vec::new().into()),
            ],
            next: None,
        };
        let deletes = vec![(DELEGATION_NONCE_KEYSPACE.to_string(), expired.into())];
        assert_eq!(prune.next(page), Ok(Next::Call(IoCall::Delete(deletes))));
        assert_eq!(prune.next(IoResult::Done), Ok(Next::Done(())));
    }
}
