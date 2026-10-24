//! Publishes the bytes a member keeps on use. A fill first claims its copy, so concurrent reads
//! fetch it once.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::FILL_CLAIM_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::storage::distribution::on_use::FillClaim;
use aruna_core::types::{Effects, TxnId};
use smallvec::smallvec;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum KeepError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: got {got}")]
    UnexpectedEvent { state: &'static str, got: String },
}

fn unexpected(state: &'static str, event: Event) -> KeepError {
    KeepError::UnexpectedEvent {
        state,
        got: format!("{event:?}"),
    }
}

/// Whether a fill may start.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Claimed {
    Claimed,
    /// Another fill of the same copy runs and is not stale yet.
    Busy,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClaimState {
    Init,
    Start,
    Read,
    Write,
    Commit,
    Abort,
    Done,
}

/// Claims one fill: absent or stale claims are taken, a fresh one of another fill is not.
#[derive(Debug, PartialEq)]
pub struct ClaimFillOperation {
    key: Vec<u8>,
    claim: FillClaim,
    state: ClaimState,
    txn_id: Option<TxnId>,
    output: Option<Result<Claimed, KeepError>>,
}

impl ClaimFillOperation {
    pub fn new(key: Vec<u8>, claim: FillClaim) -> Self {
        Self {
            key,
            claim,
            state: ClaimState::Init,
            txn_id: None,
            output: None,
        }
    }

    fn fail(&mut self, error: KeepError) -> Effects {
        self.output = Some(Err(error));
        self.state = ClaimState::Done;
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            ClaimState::Init => "init",
            ClaimState::Start => "start",
            ClaimState::Read => "read",
            ClaimState::Write => "write",
            ClaimState::Commit => "commit",
            ClaimState::Abort => "abort",
            ClaimState::Done => "done",
        }
    }
}

impl Operation for ClaimFillOperation {
    type Output = Claimed;
    type Error = KeepError;

    fn start(&mut self) -> Effects {
        self.state = ClaimState::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match (self.state, event) {
            (ClaimState::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                self.state = ClaimState::Read;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: FILL_CLAIM_KEYSPACE.to_string(),
                    key: self.key.clone().into(),
                    txn_id: Some(txn_id),
                })]
            }
            (ClaimState::Read, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                let held = match value.as_deref().map(FillClaim::from_bytes).transpose() {
                    Ok(held) => held,
                    Err(error) => return self.fail(error.into()),
                };
                let busy = held.is_some_and(|held| {
                    held.claim_id != self.claim.claim_id && !held.stale(self.claim.claimed_at_ms)
                });
                let Some(txn_id) = self.txn_id else {
                    return self.fail(KeepError::NotFinished);
                };
                if busy {
                    self.output = Some(Ok(Claimed::Busy));
                    self.state = ClaimState::Abort;
                    self.txn_id = None;
                    return smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })];
                }
                let value = match self.claim.to_bytes() {
                    Ok(value) => value,
                    Err(error) => return self.fail(error.into()),
                };
                self.state = ClaimState::Write;
                smallvec![Effect::Storage(StorageEffect::Write {
                    key_space: FILL_CLAIM_KEYSPACE.to_string(),
                    key: self.key.clone().into(),
                    value: value.into(),
                    txn_id: Some(txn_id),
                })]
            }
            (ClaimState::Write, Event::Storage(StorageEvent::WriteResult { .. })) => {
                let Some(txn_id) = self.txn_id.take() else {
                    return self.fail(KeepError::NotFinished);
                };
                self.state = ClaimState::Commit;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (ClaimState::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.output = Some(Ok(Claimed::Claimed));
                self.state = ClaimState::Done;
                smallvec![]
            }
            (ClaimState::Abort, Event::Storage(StorageEvent::TransactionAborted { .. })) => {
                self.state = ClaimState::Done;
                smallvec![]
            }
            (_, event) => {
                let error = unexpected(self.state_name(), event);
                self.fail(error)
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.state == ClaimState::Done
    }

    fn finalize(self) -> Result<Claimed, KeepError> {
        self.output.unwrap_or(Err(KeepError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        self.fail(KeepError::NotFinished)
    }
}

#[cfg(test)]
#[path = "keep_tests.rs"]
mod tests;
