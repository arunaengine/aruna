//! Admits one member on its own node: checks that the local bucket carries the member's
//! binding, opens the ranges whose records it holds and signs its admission statement, all in
//! one storage transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::effects::{Effect, NetEffect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, NetEvent, StatementSignEvent, StorageEvent};
use aruna_core::keyspaces::{BUCKET_CREATION_KEYSPACE, RANGE_ACCOUNT_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{BucketInfo, incarnation};
use aruna_core::structs::storage::distribution::accounting::account_key;
use aruna_core::structs::storage::distribution::{
    KeyRange, SignedStatement, Statement, StatementClaim,
};
use aruna_core::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use super::inventory::open_ranges;

#[derive(Debug, Clone, PartialEq)]
pub struct AdmitConfig {
    pub realm_id: RealmId,
    /// This node; the statement is signed with its key.
    pub node_id: NodeId,
    pub bucket: String,
    pub set_id: Ulid,
    pub member_id: Ulid,
    /// The create or join digest the admission answers.
    pub intent: [u8; 32],
    /// The ranges whose records the member holds; only those not opened yet are written.
    pub ranges: Vec<KeyRange>,
}

#[derive(Debug, Error, PartialEq)]
pub enum AdmitError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("bucket \"{0}\" is missing or does not carry this member's binding")]
    BindingChanged(String),
    #[error("statement signing unavailable: {0}")]
    SignUnavailable(String),
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: got {got}")]
    UnexpectedEvent { state: &'static str, got: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdmitState {
    Init,
    StartTransaction,
    Read,
    Write,
    Sign,
    Commit,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct AdmitOperation {
    config: AdmitConfig,
    state: AdmitState,
    txn_id: Option<TxnId>,
    /// The claim to sign once the range rows are staged.
    pending: Option<StatementClaim>,
    signed: Option<SignedStatement>,
    output: Option<Result<SignedStatement, AdmitError>>,
}

impl AdmitOperation {
    pub fn new(config: AdmitConfig) -> Self {
        Self {
            config,
            state: AdmitState::Init,
            txn_id: None,
            pending: None,
            signed: None,
            output: None,
        }
    }

    fn read(&mut self, txn_id: TxnId) -> Effects {
        self.txn_id = Some(txn_id);
        self.state = AdmitState::Read;
        let bucket: Key = self.config.bucket.as_bytes().into();
        let mut reads = vec![
            (S3_BUCKET_KEYSPACE.to_string(), bucket.clone()),
            (BUCKET_CREATION_KEYSPACE.to_string(), bucket),
        ];
        reads.extend(self.config.ranges.iter().map(|range| {
            let key = account_key(self.config.set_id, range.range_id);
            (RANGE_ACCOUNT_KEYSPACE.to_string(), Key::from(key))
        }));
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: Some(txn_id),
        })]
    }

    /// The bucket must carry this member's binding for its own incarnation; ranges without an
    /// account row are opened.
    fn plan(&mut self, values: Vec<Option<Value>>) -> Result<Effects, AdmitError> {
        let mut values = values.into_iter();
        let bucket = values.next().flatten();
        let creation = values.next().flatten();
        let changed = || AdmitError::BindingChanged(self.config.bucket.clone());
        let info = bucket
            .map(|value| BucketInfo::from_bytes(value.as_ref()))
            .transpose()?
            .ok_or_else(changed)?;
        let binding = info.distribution.ok_or_else(changed)?;
        let local = incarnation(creation.as_deref());
        if binding.set_id != self.config.set_id
            || binding.member_id != self.config.member_id
            || binding.incarnation != local
        {
            return Err(changed());
        }
        let missing: Vec<KeyRange> = self
            .config
            .ranges
            .iter()
            .zip(values)
            .filter(|(_, account)| account.is_none())
            .map(|(range, _)| range.clone())
            .collect();
        let statement = Statement::admit(
            self.config.realm_id,
            self.config.set_id,
            self.config.member_id,
            binding.incarnation,
            self.config.intent,
        );
        let claim = StatementClaim {
            signer: self.config.node_id,
            statement,
        };
        if missing.is_empty() {
            return Ok(self.sign(claim));
        }
        let txn_id = self.txn_id.ok_or(AdmitError::NotFinished)?;
        let writes = open_ranges(self.config.set_id, &missing)?;
        self.state = AdmitState::Write;
        self.pending = Some(claim);
        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }

    fn sign(&mut self, claim: StatementClaim) -> Effects {
        self.state = AdmitState::Sign;
        smallvec![Effect::Net(NetEffect::StatementSign(Box::new(claim)))]
    }

    fn commit(&mut self, signed: SignedStatement) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(AdmitError::NotFinished);
        };
        self.signed = Some(signed);
        self.state = AdmitState::Commit;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn fail(&mut self, error: AdmitError) -> Effects {
        let cleanup = self.abort();
        self.state = AdmitState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected(&mut self, state: &'static str, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        self.fail(AdmitError::UnexpectedEvent {
            state,
            got: format!("{event:?}"),
        })
    }
}

impl Operation for AdmitOperation {
    type Output = SignedStatement;
    type Error = AdmitError;

    fn start(&mut self) -> Effects {
        self.state = AdmitState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.state, event) {
            (
                AdmitState::StartTransaction,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => self.read(txn_id),
            (AdmitState::Read, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                let values = values.into_iter().map(|(_, value)| value).collect();
                self.plan(values).unwrap_or_else(|error| self.fail(error))
            }
            (AdmitState::Write, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                match self.pending.take() {
                    Some(claim) => self.sign(claim),
                    None => self.fail(AdmitError::NotFinished),
                }
            }
            (AdmitState::Sign, Event::Net(NetEvent::StatementSign(signed))) => match signed {
                StatementSignEvent::Signed(signed) => self.commit(*signed),
                StatementSignEvent::Unavailable(reason) => {
                    self.fail(AdmitError::SignUnavailable(reason))
                }
            },
            (AdmitState::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn_id = None;
                self.state = AdmitState::Finish;
                self.output = self.signed.take().map(Ok);
                smallvec![]
            }
            (AdmitState::Init | AdmitState::Finish | AdmitState::Error, _) => smallvec![],
            (state, event) => {
                let name = match state {
                    AdmitState::StartTransaction => "start transaction",
                    AdmitState::Read => "read",
                    AdmitState::Write => "write",
                    AdmitState::Sign => "sign",
                    AdmitState::Commit => "commit",
                    AdmitState::Init | AdmitState::Finish | AdmitState::Error => "done",
                };
                self.unexpected(name, event)
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, AdmitState::Finish | AdmitState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if !self.is_complete() {
            return Err(AdmitError::NotFinished);
        }
        self.output.unwrap_or(Err(AdmitError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

#[cfg(test)]
#[path = "admit_tests.rs"]
mod tests;
