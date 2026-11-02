//! Stores the realm's central assistant provider and charges each user's daily model calls.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{ASSISTANT_USAGE_KEYSPACE, CENTRAL_ASSISTANT_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    AssistantUsage, CENTRAL_PROVIDER_ID, CentralAssistant, QuotaReached, charge,
};
use aruna_core::types::{Effects, Key, TxnId, Value};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum CentralStoreError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("central assistant changed concurrently")]
    Stale,
    #[error("daily assistant limit reached")]
    QuotaReached(QuotaReached),
    #[error("central assistant operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

fn unexpected(
    state: &impl std::fmt::Debug,
    expected: &'static str,
    event: &Event,
) -> CentralStoreError {
    CentralStoreError::UnexpectedEvent {
        state: format!("{state:?}"),
        expected,
        got: format!("{event:?}"),
    }
}

fn central_key() -> Key {
    ByteView::from(CENTRAL_PROVIDER_ID.as_bytes())
}

fn usage_key(user_id: UserId) -> Key {
    ByteView::from(user_id.to_storage_key())
}

fn decode_central(value: Option<&Value>) -> Result<Option<CentralAssistant>, ConversionError> {
    value
        .map(|bytes| CentralAssistant::from_bytes(bytes.as_ref()))
        .transpose()
}

fn decode_usage(value: Option<&Value>) -> Result<Option<AssistantUsage>, ConversionError> {
    value
        .map(|bytes| AssistantUsage::from_bytes(bytes.as_ref()))
        .transpose()
}

fn abort_txn(txn_id: &mut Option<TxnId>) -> Effects {
    txn_id
        .take()
        .map_or_else(smallvec::SmallVec::new, |txn_id| {
            smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
        })
}

/// The central provider, if set up, and the caller's usage row.
pub type CentralRead = (Option<CentralAssistant>, Option<AssistantUsage>);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReadState {
    Init,
    Read,
    Finish,
}

/// Reads the central provider together with the caller's usage row.
#[derive(Debug, PartialEq)]
pub struct GetCentralOperation {
    user_id: UserId,
    state: ReadState,
    output: Option<Result<CentralRead, CentralStoreError>>,
}

impl GetCentralOperation {
    pub fn new(user_id: UserId) -> Self {
        Self {
            user_id,
            state: ReadState::Init,
            output: None,
        }
    }
}

impl Operation for GetCentralOperation {
    type Output = CentralRead;
    type Error = CentralStoreError;

    fn start(&mut self) -> Effects {
        self.state = ReadState::Read;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (CENTRAL_ASSISTANT_KEYSPACE.to_string(), central_key()),
                (
                    ASSISTANT_USAGE_KEYSPACE.to_string(),
                    usage_key(self.user_id)
                ),
            ],
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let result = match (&self.state, &event) {
            (ReadState::Read, Event::Storage(StorageEvent::BatchReadResult { values }))
                if values.len() == 2 =>
            {
                decode_central(values[0].1.as_ref())
                    .and_then(|central| Ok((central, decode_usage(values[1].1.as_ref())?)))
                    .map_err(CentralStoreError::from)
            }
            (_, Event::Storage(StorageEvent::Error { error })) => Err(error.clone().into()),
            _ => Err(unexpected(&self.state, "central read", &event)),
        };
        self.output = Some(result);
        self.state = ReadState::Finish;
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.state == ReadState::Finish
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(CentralStoreError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TxnState {
    Init,
    Start,
    Read,
    Write,
    Commit,
    Finish,
}

/// Replaces or removes the central provider when the stored row still equals `expected`.
#[derive(Debug, PartialEq)]
pub struct SetCentralOperation {
    next: Option<CentralAssistant>,
    expected: Option<CentralAssistant>,
    txn_id: Option<TxnId>,
    state: TxnState,
    output: Option<Result<(), CentralStoreError>>,
}

impl SetCentralOperation {
    /// `next` of `None` removes the provider.
    pub fn new(next: Option<CentralAssistant>, expected: Option<CentralAssistant>) -> Self {
        Self {
            next,
            expected,
            txn_id: None,
            state: TxnState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: CentralStoreError) -> Effects {
        let cleanup = self.abort();
        self.output = Some(Err(error));
        self.state = TxnState::Finish;
        cleanup
    }

    fn handle_read(&mut self, value: Option<&Value>, txn_id: TxnId) -> Effects {
        let current = match decode_central(value) {
            Ok(current) => current,
            Err(error) => return self.fail(error.into()),
        };
        if current != self.expected {
            return self.fail(CentralStoreError::Stale);
        }
        let effect = match &self.next {
            Some(next) => match next.to_bytes() {
                Ok(bytes) => StorageEffect::Write {
                    key_space: CENTRAL_ASSISTANT_KEYSPACE.to_string(),
                    key: central_key(),
                    value: bytes.into(),
                    txn_id: Some(txn_id),
                },
                Err(error) => return self.fail(error.into()),
            },
            None => StorageEffect::Delete {
                key_space: CENTRAL_ASSISTANT_KEYSPACE.to_string(),
                key: central_key(),
                txn_id: Some(txn_id),
            },
        };
        self.state = TxnState::Write;
        smallvec![Effect::Storage(effect)]
    }
}

impl Operation for SetCentralOperation {
    type Output = ();
    type Error = CentralStoreError;

    fn start(&mut self) -> Effects {
        self.state = TxnState::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match (self.state, &event, self.txn_id) {
            (TxnState::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id }), _) => {
                self.txn_id = Some(*txn_id);
                self.state = TxnState::Read;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: CENTRAL_ASSISTANT_KEYSPACE.to_string(),
                    key: central_key(),
                    txn_id: Some(*txn_id),
                })]
            }
            (TxnState::Read, Event::Storage(StorageEvent::ReadResult { value, .. }), Some(id)) => {
                self.handle_read(value.as_ref(), id)
            }
            (
                TxnState::Write,
                Event::Storage(
                    StorageEvent::WriteResult { .. } | StorageEvent::DeleteResult { .. },
                ),
                Some(txn_id),
            ) => {
                self.state = TxnState::Commit;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (TxnState::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. }), _) => {
                self.txn_id = None;
                self.output = Some(Ok(()));
                self.state = TxnState::Finish;
                smallvec![]
            }
            _ => {
                let error = unexpected(&self.state, "central write step", &event);
                self.fail(error)
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.state == TxnState::Finish
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(CentralStoreError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        abort_txn(&mut self.txn_id)
    }
}

/// Counts one central model call for a user on `today`; a refused call writes nothing.
#[derive(Debug, PartialEq)]
pub struct ChargeUsageOperation {
    user_id: UserId,
    today: u32,
    limit: Option<u32>,
    txn_id: Option<TxnId>,
    state: TxnState,
    output: Option<Result<AssistantUsage, CentralStoreError>>,
}

impl ChargeUsageOperation {
    pub fn new(user_id: UserId, today: u32, limit: Option<u32>) -> Self {
        Self {
            user_id,
            today,
            limit,
            txn_id: None,
            state: TxnState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: CentralStoreError) -> Effects {
        let cleanup = self.abort();
        self.output = Some(Err(error));
        self.state = TxnState::Finish;
        cleanup
    }

    fn handle_read(&mut self, value: Option<&Value>, txn_id: TxnId) -> Effects {
        let usage = match decode_usage(value) {
            Ok(usage) => usage,
            Err(error) => return self.fail(error.into()),
        };
        let next = match charge(usage, self.today, self.limit) {
            Ok(next) => next,
            Err(reached) => return self.fail(CentralStoreError::QuotaReached(reached)),
        };
        let bytes = match next.to_bytes() {
            Ok(bytes) => bytes,
            Err(error) => return self.fail(error.into()),
        };
        self.output = Some(Ok(next));
        self.state = TxnState::Write;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: ASSISTANT_USAGE_KEYSPACE.to_string(),
            key: usage_key(self.user_id),
            value: bytes.into(),
            txn_id: Some(txn_id),
        })]
    }
}

impl Operation for ChargeUsageOperation {
    type Output = AssistantUsage;
    type Error = CentralStoreError;

    fn start(&mut self) -> Effects {
        self.state = TxnState::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match (self.state, &event, self.txn_id) {
            (TxnState::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id }), _) => {
                self.txn_id = Some(*txn_id);
                self.state = TxnState::Read;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: ASSISTANT_USAGE_KEYSPACE.to_string(),
                    key: usage_key(self.user_id),
                    txn_id: Some(*txn_id),
                })]
            }
            (TxnState::Read, Event::Storage(StorageEvent::ReadResult { value, .. }), Some(id)) => {
                self.handle_read(value.as_ref(), id)
            }
            (TxnState::Write, Event::Storage(StorageEvent::WriteResult { .. }), Some(txn_id)) => {
                self.state = TxnState::Commit;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (TxnState::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. }), _) => {
                self.txn_id = None;
                self.state = TxnState::Finish;
                smallvec![]
            }
            _ => {
                let error = unexpected(&self.state, "usage charge step", &event);
                self.fail(error)
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.state == TxnState::Finish
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match (self.state, self.output) {
            (TxnState::Finish, Some(output)) => output,
            _ => Err(CentralStoreError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        abort_txn(&mut self.txn_id)
    }
}

#[cfg(test)]
#[path = "central_tests.rs"]
mod tests;
