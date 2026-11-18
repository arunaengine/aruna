//! Stores named node secrets: the record in the node's secret keyspace, the value in its node
//! vault, and the audit row, all in one local transaction. Nothing replicates.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{AUTH_KEYSPACE, NODE_SECRETS_KEYSPACE};
use aruna_core::node_vault::{VaultEntry, VaultPurpose};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows;
use aruna_core::secrets::{SecretName, SecretRecord, SecretScope};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::{NodeId, UserId};
use thiserror::Error;
use ulid::Ulid;

use super::{ChangeError, Managers, SecretChange, apply_change, widening_notices};
use crate::node_vault::{delete_secret, write_secret};
use crate::notifications::outbox_record::schedule_drain_effect;

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
}

impl From<IoError> for NodeSecretError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

/// Creates, replaces, rotates or deletes one node secret.
#[derive(Debug, PartialEq)]
pub struct NodeSecretOperation {
    input: Option<NodeChange>,
    /// The writes of the transaction still to send, once the change is decided.
    queue: VecDeque<IoCall>,
    notices: bool,
    result: Option<Option<SecretRecord>>,
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

/// The error for an event that does not answer an iteration.
pub(super) fn unexpected_iter(event: &Event) -> IoError {
    IoError::Unexpected {
        expected: "StorageEvent::IterResult",
        got: event.label(),
    }
}

/// Checks the answer to a queued write: a row write or delete, or a vault write or delete.
pub(super) fn written(result: IoResult) -> Result<(), IoError> {
    match result {
        IoResult::Event(event)
            if matches!(
                *event,
                Event::Storage(
                    StorageEvent::WriteResult { .. } | StorageEvent::DeleteResult { .. }
                )
            ) =>
        {
            Ok(())
        }
        other => rows::done(other),
    }
}

impl NodeSecretOperation {
    pub fn new(input: NodeChange) -> Self {
        Self {
            input: Some(input),
            queue: VecDeque::new(),
            notices: false,
            result: None,
        }
    }

    /// Decides the change from the stored record, then queues every write of the transaction.
    fn plan(
        &mut self,
        input: NodeChange,
        stored: Option<&[u8]>,
        auth: Option<&[u8]>,
    ) -> Result<(), NodeSecretError> {
        let current = stored.map(decode).transpose()?;
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
            None => self
                .queue
                .push_back(IoCall::Delete(vec![(keyspace, key.into())])),
        }
        self.queue.push_back(IoCall::Write(audit));
        if let (Some(info), Some(fields)) = (&applied.info, &applied.value) {
            let sealed = write_secret(value_entry(info.value_id), fields.encode()?, None);
            self.queue.push_back(IoCall::Effect(Box::new(sealed)));
        }
        if let Some(retired) = applied.retired {
            let removal = delete_secret(value_entry(retired), None);
            self.queue.push_back(IoCall::Effect(Box::new(removal)));
        }
        self.result = Some(applied.info);
        Ok(())
    }

    fn send(&mut self) -> Next<Option<SecretRecord>> {
        match self.queue.pop_front() {
            Some(call) => Next::Call(call),
            // The notice rows are committed; a missed timer leaves them to the next drain.
            None => Next::Then {
                output: self.result.take().flatten(),
                calls: (self.notices.then(schedule_drain_effect).into_iter())
                    .map(IoCall::from)
                    .collect(),
            },
        }
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

    fn first(&mut self) -> Result<Next<Self::Output>, NodeSecretError> {
        let input = self.input.as_ref().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::Read(vec![
            (
                NODE_SECRETS_KEYSPACE.to_string(),
                record_key(input.change.name()).into(),
            ),
            (
                AUTH_KEYSPACE.to_string(),
                input.realm_id.as_bytes().to_vec().into(),
            ),
        ])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, NodeSecretError> {
        match self.input.take() {
            Some(input) => {
                let [stored, auth] = rows::values::<2>(result)?;
                self.plan(input, stored.as_deref(), auth.as_deref())?;
            }
            None => written(result)?,
        }
        Ok(self.send())
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

/// Reads one page of node secret records by name, never values.
#[derive(Debug, PartialEq)]
pub struct NodeSecretsOperation {
    start: Option<IterStart>,
    limit: usize,
}

impl NodeSecretsOperation {
    /// The records after `after`; `limit` is clamped to 1..=200.
    pub fn new(after: Option<&SecretName>, limit: usize) -> Self {
        Self {
            start: after.map(|name| IterStart::After(record_key(name).into())),
            limit: limit.clamp(1, 200),
        }
    }

    /// The record named `name` first, if it exists.
    pub fn from_name(name: &SecretName) -> Self {
        Self {
            start: Some(IterStart::At(record_key(name).into())),
            limit: 1,
        }
    }
}

impl Operation for NodeSecretsOperation {
    /// The records and whether more follow.
    type Output = (Vec<SecretRecord>, bool);
    type Error = NodeSecretError;

    fn first(&mut self) -> Result<Next<Self::Output>, NodeSecretError> {
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Storage(
            StorageEffect::Iter {
                key_space: NODE_SECRETS_KEYSPACE.to_string(),
                prefix: None,
                start: self.start.clone(),
                limit: self.limit + 1,
                txn_id: None,
            },
        )))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, NodeSecretError> {
        let values = match rows::event(result)? {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values,
            other => return Err(unexpected_iter(&other).into()),
        };
        let more = values.len() > self.limit;
        let records = (values.iter().take(self.limit))
            .map(|(_, value)| decode(value))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Next::Done((records, more)))
    }
}

#[cfg(test)]
#[path = "node_tests.rs"]
mod tests;
