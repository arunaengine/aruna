//! Stores named node secrets: the record in the node's secret keyspace, the value in its node
//! vault, and the audit row, all in one local transaction. Nothing replicates.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{AUTH_KEYSPACE, NODE_SECRETS_KEYSPACE};
use aruna_core::node_vault::{VaultEntry, VaultPurpose};
use aruna_core::operation::Operation;
use aruna_core::secrets::{SecretName, SecretRecord, SecretScope};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, TxnId};
use aruna_core::{NodeId, UserId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use super::{ChangeError, Managers, SecretChange, apply_change, widening_notices};
use crate::node_vault::{delete_secret, write_secret};
use crate::notifications::outbox::schedule_drain_effect;

/// One node secret change, with the time and write id it uses.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeChange {
    pub realm_id: RealmId,
    /// The node the secret lives on, named in the notice a widening change sends.
    pub node_id: NodeId,
    pub actor: UserId,
    pub change: SecretChange,
    pub now_ms: u64,
    pub write_id: Ulid,
}

#[derive(Debug, Error, PartialEq)]
pub enum NodeSecretError {
    #[error(transparent)]
    Change(#[from] ChangeError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("unexpected event in state {state}: {got}")]
    UnexpectedEvent { state: &'static str, got: String },
    #[error("operation did not finish")]
    NotFinished,
}

/// The result a queued write waits for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Pending {
    BatchWrite,
    BatchDelete,
    Write,
    Delete,
}

#[derive(Debug, PartialEq)]
enum State {
    Init,
    StartTransaction,
    ReadRecord,
    Writing(Pending),
    Commit,
    ScheduleNotices,
    Done,
}

/// Creates, replaces, rotates or deletes one node secret.
#[derive(Debug, PartialEq)]
pub struct NodeSecretOperation {
    input: Option<NodeChange>,
    state: State,
    txn_id: Option<TxnId>,
    queue: VecDeque<(Effect, Pending)>,
    notices: bool,
    result: Option<Option<SecretRecord>>,
    output: Option<Result<Option<SecretRecord>, NodeSecretError>>,
}

pub fn record_key(name: &SecretName) -> Vec<u8> {
    name.as_str().as_bytes().to_vec()
}

pub fn value_entry(value_id: Ulid) -> VaultEntry {
    VaultEntry::new(VaultPurpose::NamedSecret, value_id)
}

fn decode(bytes: &[u8]) -> Result<SecretRecord, ConversionError> {
    Ok(postcard::from_bytes(bytes)?)
}

impl NodeSecretOperation {
    pub fn new(input: NodeChange) -> Self {
        Self {
            input: Some(input),
            state: State::Init,
            txn_id: None,
            queue: VecDeque::new(),
            notices: false,
            result: None,
            output: None,
        }
    }

    fn fail(&mut self, error: NodeSecretError) -> Effects {
        self.output = Some(Err(error));
        self.state = State::Done;
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }

    fn unexpected(&mut self, state: &'static str, event: Event) -> Effects {
        let got = format!("{event:?}");
        self.fail(NodeSecretError::UnexpectedEvent { state, got })
    }

    /// Decides the change from the stored record, then queues every write of the transaction.
    fn plan(
        &mut self,
        stored: Option<&[u8]>,
        auth: Option<&[u8]>,
    ) -> Result<Effects, NodeSecretError> {
        let current = stored.map(decode).transpose()?;
        let input = self.input.take().ok_or(NodeSecretError::NotFinished)?;
        let key = record_key(input.change.name());
        let name = input.change.name().clone();
        let applied = apply_change(
            SecretScope::Node,
            current.as_ref(),
            input.change,
            input.actor,
            input.now_ms,
            input.write_id,
        )?;
        let mut audit = applied.audit.entries()?;
        if let (Some(info), Some(auth)) = (&applied.info, auth) {
            let old = current.as_ref().map_or(&[][..], |current| &current.uses);
            let notices = widening_notices(
                input.realm_id,
                &name,
                (old, &info.uses),
                Managers::Realm(auth),
                Some(input.node_id),
                input.actor,
                input.now_ms,
            )?;
            self.notices = !notices.is_empty();
            audit.extend(notices);
        }
        let keyspace = NODE_SECRETS_KEYSPACE.to_string();
        match &applied.info {
            Some(info) => {
                let row = postcard::to_allocvec(info).map_err(ConversionError::from)?;
                audit.push((keyspace, key.into(), row.into()));
            }
            None => {
                let deletes = vec![(keyspace, key.into())];
                let effect = StorageEffect::BatchDelete {
                    deletes,
                    txn_id: self.txn_id,
                };
                self.queue
                    .push_back((Effect::Storage(effect), Pending::BatchDelete));
            }
        }
        let writes = StorageEffect::BatchWrite {
            writes: audit,
            txn_id: self.txn_id,
        };
        self.queue
            .push_back((Effect::Storage(writes), Pending::BatchWrite));
        if let (Some(info), Some(fields)) = (&applied.info, &applied.value) {
            let sealed = write_secret(value_entry(info.value_id), fields.encode()?, self.txn_id);
            self.queue.push_back((sealed, Pending::Write));
        }
        if let Some(retired) = applied.retired {
            let removal = delete_secret(value_entry(retired), self.txn_id);
            self.queue.push_back((removal, Pending::Delete));
        }
        self.result = Some(applied.info);
        Ok(self.next())
    }

    fn next(&mut self) -> Effects {
        if let Some((effect, pending)) = self.queue.pop_front() {
            self.state = State::Writing(pending);
            return smallvec![effect];
        }
        let Some(txn_id) = self.txn_id else {
            return self.fail(NodeSecretError::NotFinished);
        };
        self.state = State::Commit;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }
}

impl From<aruna_core::secrets::SecretError> for NodeSecretError {
    fn from(error: aruna_core::secrets::SecretError) -> Self {
        Self::Change(ChangeError::Invalid(error))
    }
}

impl Operation for NodeSecretOperation {
    type Output = Option<SecretRecord>;
    type Error = NodeSecretError;

    fn start(&mut self) -> Effects {
        self.state = State::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            if matches!(self.state, State::Commit) {
                self.txn_id = None;
            }
            return self.fail(error.into());
        }
        match (&self.state, event) {
            (
                State::StartTransaction,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.txn_id = Some(txn_id);
                let Some(input) = &self.input else {
                    return self.fail(NodeSecretError::NotFinished);
                };
                self.state = State::ReadRecord;
                let auth_key = input.realm_id.as_bytes().to_vec();
                smallvec![Effect::Storage(StorageEffect::BatchRead {
                    reads: vec![
                        (
                            NODE_SECRETS_KEYSPACE.to_string(),
                            record_key(input.change.name()).into()
                        ),
                        (AUTH_KEYSPACE.to_string(), auth_key.into()),
                    ],
                    txn_id: Some(txn_id),
                })]
            }
            (State::ReadRecord, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                let [(_, stored), (_, auth)] = values.as_slice() else {
                    let got = format!("{} values", values.len());
                    return self.fail(NodeSecretError::UnexpectedEvent {
                        state: "ReadRecord",
                        got,
                    });
                };
                match self.plan(stored.as_deref(), auth.as_deref()) {
                    Ok(effects) => effects,
                    Err(error) => self.fail(error),
                }
            }
            (State::Writing(pending), event) => {
                let done = matches!(
                    (pending, &event),
                    (
                        Pending::BatchWrite,
                        Event::Storage(StorageEvent::BatchWriteResult { .. })
                    ) | (
                        Pending::BatchDelete,
                        Event::Storage(StorageEvent::BatchDeleteResult { .. })
                    ) | (
                        Pending::Write,
                        Event::Storage(StorageEvent::WriteResult { .. })
                    ) | (
                        Pending::Delete,
                        Event::Storage(StorageEvent::DeleteResult { .. })
                    )
                );
                match done {
                    true => self.next(),
                    false => self.unexpected("Writing", event),
                }
            }
            (State::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn_id = None;
                if self.notices {
                    self.state = State::ScheduleNotices;
                    return smallvec![schedule_drain_effect()];
                }
                self.state = State::Done;
                self.output = Some(Ok(self.result.take().flatten()));
                smallvec![]
            }
            (
                State::ScheduleNotices,
                Event::Task(TaskEvent::TimerScheduled { .. } | TaskEvent::Error { .. }),
            ) => {
                // The notice rows are committed; a missed timer leaves them to the next drain.
                self.state = State::Done;
                self.output = Some(Ok(self.result.take().flatten()));
                smallvec![]
            }
            (State::Init | State::Done, _) => smallvec![],
            (_, event) => self.unexpected("NodeSecret", event),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, State::Done)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(NodeSecretError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

/// Reads one page of node secret records by name, never values.
#[derive(Debug, PartialEq)]
pub struct NodeSecretsOperation {
    start: Option<IterStart>,
    limit: usize,
    output: Option<Result<(Vec<SecretRecord>, bool), NodeSecretError>>,
}

impl NodeSecretsOperation {
    /// The records after `after`; `limit` is clamped to 1..=200.
    pub fn new(after: Option<&SecretName>, limit: usize) -> Self {
        Self {
            start: after.map(|name| IterStart::After(record_key(name).into())),
            limit: limit.clamp(1, 200),
            output: None,
        }
    }

    /// The record named `name` first, if it exists.
    pub fn from_name(name: &SecretName) -> Self {
        Self {
            start: Some(IterStart::At(record_key(name).into())),
            limit: 1,
            output: None,
        }
    }
}

impl Operation for NodeSecretsOperation {
    /// The records and whether more follow.
    type Output = (Vec<SecretRecord>, bool);
    type Error = NodeSecretError;

    fn start(&mut self) -> Effects {
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: NODE_SECRETS_KEYSPACE.to_string(),
            prefix: None,
            start: self.start.clone(),
            limit: self.limit + 1,
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if self.output.is_some() {
            return smallvec![];
        }
        self.output = Some(match event {
            Event::Storage(StorageEvent::IterResult { values, .. }) => {
                let more = values.len() > self.limit;
                (values.iter().take(self.limit))
                    .map(|(_, value)| decode(value))
                    .collect::<Result<Vec<_>, _>>()
                    .map(|records| (records, more))
                    .map_err(NodeSecretError::from)
            }
            Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
            other => Err(NodeSecretError::UnexpectedEvent {
                state: "Iter",
                got: format!("{other:?}"),
            }),
        });
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(NodeSecretError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
#[path = "node_tests.rs"]
mod tests;
