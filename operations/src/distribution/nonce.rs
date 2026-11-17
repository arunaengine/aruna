//! Spends the single-use nonce of a delegation, ticket or granted read once, durably until
//! it expires, and prunes the expired nonce rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect, abort_effects};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::DELEGATION_NONCE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::types::{Effects, TxnId};
use smallvec::smallvec;

use thiserror::Error;

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum NonceError {
    #[error("the nonce was already used")]
    Replayed,
    #[error("the nonce store is unavailable")]
    Unavailable,
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
/// accepts it again; then deletes one bounded page of nonce rows that expired before `now`.
/// A failed prune does not fail the spend.
#[derive(Debug, PartialEq)]
pub struct SpendNonceOperation {
    key: Vec<u8>,
    now: u64,
    state: SpendState,
    txn_id: Option<TxnId>,
    output: Option<Result<(), NonceError>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SpendState {
    Init,
    Start,
    Read,
    Write,
    Commit,
    Abort,
    Page,
    Prune,
    Finish,
    Error,
}

impl SpendNonceOperation {
    pub fn new(nonce: [u8; 16], expires: u64, now: u64) -> Self {
        Self {
            key: nonce_key(expires, nonce),
            now,
            state: SpendState::Init,
            txn_id: None,
            output: None,
        }
    }

    fn storage(&mut self, state: SpendState, effect: StorageEffect) -> Effects {
        self.state = state;
        smallvec![Effect::Storage(effect)]
    }

    /// Aborts the open transaction, if any, and ends with `error`.
    fn fail(&mut self, error: NonceError) -> Effects {
        self.output = Some(Err(error));
        match self.txn_id.take() {
            Some(txn_id) => self.storage(
                SpendState::Abort,
                StorageEffect::AbortTransaction { txn_id },
            ),
            None => {
                self.state = SpendState::Error;
                smallvec![]
            }
        }
    }

    fn done(&mut self) -> Effects {
        self.state = SpendState::Finish;
        smallvec![]
    }
}

/// A storage failure while spending: a conflict means another acceptance spent the nonce.
fn spend_error(error: StorageError) -> NonceError {
    match error {
        StorageError::TransactionConflict => NonceError::Replayed,
        _ => NonceError::Unavailable,
    }
}

impl Operation for SpendNonceOperation {
    type Output = ();
    type Error = NonceError;

    fn start(&mut self) -> Effects {
        self.storage(
            SpendState::Start,
            StorageEffect::StartTransaction { read: false },
        )
    }

    fn step(&mut self, event: Event) -> Effects {
        let Event::Storage(event) = event else {
            return self.fail(NonceError::Unavailable);
        };
        match (self.state, event) {
            (SpendState::Start, StorageEvent::TransactionStarted { txn_id }) => {
                self.txn_id = Some(txn_id);
                let read = StorageEffect::Read {
                    key_space: DELEGATION_NONCE_KEYSPACE.to_string(),
                    key: self.key.clone().into(),
                    txn_id: Some(txn_id),
                };
                self.storage(SpendState::Read, read)
            }
            (SpendState::Read, StorageEvent::ReadResult { value: Some(_), .. }) => {
                self.fail(NonceError::Replayed)
            }
            (SpendState::Read, StorageEvent::ReadResult { value: None, .. }) => {
                let write = StorageEffect::Write {
                    key_space: DELEGATION_NONCE_KEYSPACE.to_string(),
                    key: self.key.clone().into(),
                    value: Vec::new().into(),
                    txn_id: self.txn_id,
                };
                self.storage(SpendState::Write, write)
            }
            (SpendState::Write, StorageEvent::WriteResult { .. }) => match self.txn_id.take() {
                Some(txn_id) => self.storage(
                    SpendState::Commit,
                    StorageEffect::CommitTransaction { txn_id },
                ),
                None => self.fail(NonceError::Unavailable),
            },
            (SpendState::Commit, StorageEvent::TransactionCommitted { .. }) => {
                self.output = Some(Ok(()));
                let page = StorageEffect::Iter {
                    key_space: DELEGATION_NONCE_KEYSPACE.to_string(),
                    prefix: None,
                    start: None,
                    limit: PRUNE_PAGE,
                    txn_id: None,
                };
                self.storage(SpendState::Page, page)
            }
            (SpendState::Page, StorageEvent::IterResult { values, .. }) => {
                let deletes: Vec<_> = values
                    .into_iter()
                    .filter(|(key, _)| {
                        key.get(..8)
                            .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
                            .is_some_and(|expires| u64::from_be_bytes(expires) < self.now)
                    })
                    .map(|(key, _)| (DELEGATION_NONCE_KEYSPACE.to_string(), key))
                    .collect();
                if deletes.is_empty() {
                    return self.done();
                }
                let delete = StorageEffect::BatchDelete {
                    deletes,
                    txn_id: None,
                };
                self.storage(SpendState::Prune, delete)
            }
            (SpendState::Page | SpendState::Prune, StorageEvent::Error { .. })
            | (SpendState::Prune, StorageEvent::BatchDeleteResult { .. }) => self.done(),
            (SpendState::Abort, _) => {
                self.state = SpendState::Error;
                smallvec![]
            }
            (SpendState::Commit, StorageEvent::Error { error }) => {
                // A failed commit leaves no transaction to abort.
                self.txn_id = None;
                self.fail(spend_error(error))
            }
            (_, StorageEvent::Error { error }) => self.fail(spend_error(error)),
            (_, _) => self.fail(NonceError::Unavailable),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, SpendState::Finish | SpendState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            SpendState::Finish | SpendState::Error => {
                self.output.unwrap_or(Err(NonceError::Unavailable))
            }
            _ => Err(NonceError::Unavailable),
        }
    }

    fn abort(&mut self) -> Effects {
        abort_effects(&mut self.txn_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::test_store::{context, get, open};
    use crate::driver::{DriverContext, drive};
    use aruna_core::operation_check::{
        check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use ulid::Ulid;

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

    const TXN: TxnId = Ulid::from_bytes([9; 16]);

    fn stored(event: StorageEvent) -> Event {
        Event::Storage(event)
    }

    fn nonce_row(expires: u64) -> Effect {
        Effect::Storage(StorageEffect::Read {
            key_space: DELEGATION_NONCE_KEYSPACE.to_string(),
            key: nonce_key(expires, [1; 16]).into(),
            txn_id: Some(TXN),
        })
    }

    fn prune_page() -> Effect {
        Effect::Storage(StorageEffect::Iter {
            key_space: DELEGATION_NONCE_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: PRUNE_PAGE,
            txn_id: None,
        })
    }

    fn abort() -> Effect {
        Effect::Storage(StorageEffect::AbortTransaction { txn_id: TXN })
    }

    // A fresh nonce is written and committed, then expired rows are deleted.
    #[test]
    fn spends_and_prunes() {
        let key = nonce_key(50, [1; 16]);
        let expired = nonce_key(10, [2; 16]);
        let mut operation = SpendNonceOperation::new([1; 16], 50, 40);
        check_trace(
            &mut operation,
            &[Effect::Storage(StorageEffect::StartTransaction {
                read: false,
            })],
            vec![
                (
                    stored(StorageEvent::TransactionStarted { txn_id: TXN }),
                    vec![nonce_row(50)],
                ),
                (
                    stored(StorageEvent::ReadResult {
                        key: key.clone().into(),
                        value: None,
                    }),
                    vec![Effect::Storage(StorageEffect::Write {
                        key_space: DELEGATION_NONCE_KEYSPACE.to_string(),
                        key: key.clone().into(),
                        value: Vec::new().into(),
                        txn_id: Some(TXN),
                    })],
                ),
                (
                    stored(StorageEvent::WriteResult {
                        key: key.clone().into(),
                    }),
                    vec![Effect::Storage(StorageEffect::CommitTransaction {
                        txn_id: TXN,
                    })],
                ),
                (
                    stored(StorageEvent::TransactionCommitted { txn_id: TXN }),
                    vec![prune_page()],
                ),
                (
                    stored(StorageEvent::IterResult {
                        values: vec![
                            (expired.clone().into(), Vec::new().into()),
                            (key.into(), Vec::new().into()),
                        ],
                        next_start_after: None,
                    }),
                    vec![Effect::Storage(StorageEffect::BatchDelete {
                        deletes: vec![(DELEGATION_NONCE_KEYSPACE.to_string(), expired.into())],
                        txn_id: None,
                    })],
                ),
                (
                    stored(StorageEvent::BatchDeleteResult {
                        entries: Vec::new(),
                    }),
                    Vec::new(),
                ),
            ],
        );
        assert_eq!(operation.finalize(), Ok(()));
    }

    // A spent nonce aborts the transaction and reports a replay; a failed prune keeps the spend.
    #[test]
    fn replay_aborts() {
        let mut operation = SpendNonceOperation::new([1; 16], 50, 40);
        operation.start();
        operation.step(stored(StorageEvent::TransactionStarted { txn_id: TXN }));
        let read = StorageEvent::ReadResult {
            key: nonce_key(50, [1; 16]).into(),
            value: Some(Vec::new().into()),
        };
        assert_eq!(operation.step(stored(read)).as_slice(), [abort()]);
        assert!(!operation.is_complete());
        operation.step(stored(StorageEvent::TransactionAborted { txn_id: TXN }));
        assert_eq!(operation.finalize(), Err(NonceError::Replayed));

        let mut operation = SpendNonceOperation::new([1; 16], 50, 40);
        operation.start();
        operation.step(stored(StorageEvent::TransactionStarted { txn_id: TXN }));
        operation.step(stored(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: None,
        }));
        operation.step(stored(StorageEvent::WriteResult {
            key: Vec::new().into(),
        }));
        operation.step(stored(StorageEvent::TransactionCommitted { txn_id: TXN }));
        let error = StorageError::Timeout;
        assert!(
            operation
                .step(stored(StorageEvent::Error { error }))
                .is_empty()
        );
        assert_eq!(operation.finalize(), Ok(()));
    }

    // A conflict is a replay, other failures leave the nonce unspent; a failed commit has no
    // transaction left to abort.
    #[test]
    fn storage_errors_map() {
        let mut operation = SpendNonceOperation::new([1; 16], 50, 40);
        operation.start();
        let error = StorageError::TransactionConflict;
        assert!(
            operation
                .step(stored(StorageEvent::Error { error }))
                .is_empty()
        );
        assert_eq!(operation.finalize(), Err(NonceError::Replayed));

        let mut operation = SpendNonceOperation::new([1; 16], 50, 40);
        operation.start();
        operation.step(stored(StorageEvent::TransactionStarted { txn_id: TXN }));
        operation.step(stored(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: None,
        }));
        operation.step(stored(StorageEvent::WriteResult {
            key: Vec::new().into(),
        }));
        let error = StorageError::Timeout;
        assert!(
            operation
                .step(stored(StorageEvent::Error { error }))
                .is_empty()
        );
        assert_eq!(operation.finalize(), Err(NonceError::Unavailable));
    }

    // Events outside each phase are refused; abort ends an open transaction.
    #[test]
    fn rejects_unexpected() {
        let fresh = || SpendNonceOperation::new([1; 16], 50, 40);
        assert_eq!(
            rejects_events(fresh, sample_events, |_| false).len(),
            sample_events().len()
        );
        let reading = || {
            let mut operation = fresh();
            operation.start();
            operation.step(stored(StorageEvent::TransactionStarted { txn_id: TXN }));
            operation
        };
        let mut operation = reading();
        assert_eq!(operation.step(Event::Search()).as_slice(), [abort()]);
        let mut operation = reading();
        assert_eq!(operation.abort().as_slice(), [abort()]);
        assert!(fresh().abort().is_empty());
        rejects_finalize(fresh());
        rejects_finalize(reading());
    }
}
