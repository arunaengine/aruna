//! Installs realm-signed deletion decisions before a joining node accepts any traffic.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::{DOCUMENT_STATE_KEYSPACE, GROUP_DELETE_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::reducer::{AdminDocumentState, decode_reducer_state};
use aruna_core::rows::{done, values};
use aruna_core::storage_entries::{group_deletion_entries, reducer_state_key};
use aruna_core::structs::identity::group_delete::{
    GroupDeletePhase, GroupDeleteRecord, GroupDeletionError, MembershipFence,
};
use aruna_core::types::{Key, Value};

#[derive(Debug, PartialEq)]
enum State {
    Read,
    Write,
    Delete,
}

#[derive(Debug, PartialEq)]
pub(super) struct InstallOperation {
    record: GroupDeleteRecord,
    state: State,
    deletes: Vec<(String, Key)>,
}

impl InstallOperation {
    pub(super) fn new(record: GroupDeleteRecord) -> Self {
        Self {
            record,
            state: State::Read,
            deletes: Vec::new(),
        }
    }

    fn inspect(&mut self, rows: [Option<Value>; 4]) -> Result<Next<()>, GroupDeletionError> {
        let [previous, state, cancelled, membership] = rows;
        if cancelled.is_some() {
            return Err(GroupDeletionError::Invalid(
                "onboarding deletion was cancelled".into(),
            ));
        }
        if let Some(bytes) = &previous {
            let previous = GroupDeleteRecord::from_bytes(bytes)?;
            if previous.blocks_writes() && previous.plan != self.record.plan {
                return Err(GroupDeletionError::Conflict(
                    "onboarding deletion conflicts with local decision".into(),
                ));
            }
        }
        let event = self
            .record
            .event
            .as_ref()
            .ok_or_else(|| GroupDeletionError::Invalid("deletion event is missing".into()))?;
        let mut state = match &state {
            Some(bytes) => decode_reducer_state(bytes)
                .map_err(|error| GroupDeletionError::Invalid(error.to_string()))?,
            None => AdminDocumentState::new(event.target.clone()),
        };
        state
            .apply(event)
            .map_err(|error| GroupDeletionError::Invalid(error.to_string()))?;
        let (deletes, mut writes) = group_deletion_entries(&self.record, &state)?;
        let mut membership = MembershipFence::from_value(membership.as_deref())?;
        if membership.pending.remove(&self.record.plan.group_id) {
            writes.push(membership.entry(self.record.plan.realm_id)?);
        }
        self.deletes = deletes;
        self.state = State::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

impl Operation for InstallOperation {
    type Output = ();
    type Error = GroupDeletionError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<()>, GroupDeletionError> {
        let valid = self.record.phase == GroupDeletePhase::Deleted
            && self.record.event.as_ref().is_some_and(|event| {
                event.target
                    == AdminDocumentTarget::Group {
                        group_id: self.record.plan.group_id,
                    }
                    && event.origin_node_id == self.record.plan.coordinator
                    && event.actor.node_id == self.record.plan.coordinator
                    && event.actor.realm_id == self.record.plan.realm_id
                    && event.actor.user_id == self.record.plan.requested_by
                    && self.record.deleted_by == Some(event.actor.user_id)
                    && matches!(&event.op, AdminDocumentOperation::GroupDeleted { certificate }
                        if certificate.plan == self.record.plan && certificate.verify())
            });
        if !valid {
            return Err(GroupDeletionError::Invalid(
                "invalid onboarding deletion".into(),
            ));
        }
        let group_id = self.record.plan.group_id;
        Ok(Next::Call(IoCall::Read(vec![
            (
                GROUP_DELETE_KEYSPACE.to_string(),
                group_id.to_bytes().into(),
            ),
            (
                DOCUMENT_STATE_KEYSPACE.to_string(),
                reducer_state_key(&AdminDocumentTarget::Group { group_id }),
            ),
            (
                GROUP_DELETE_KEYSPACE.to_string(),
                self.record.plan.cancelled_key().into(),
            ),
            (
                GROUP_DELETE_KEYSPACE.to_string(),
                MembershipFence::key(self.record.plan.realm_id),
            ),
        ])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, GroupDeletionError> {
        match self.state {
            State::Read => self.inspect(values::<4>(result)?),
            State::Write => {
                done(result)?;
                self.state = State::Delete;
                Ok(Next::Call(IoCall::Delete(std::mem::take(
                    &mut self.deletes,
                ))))
            }
            State::Delete => {
                done(result)?;
                Ok(Next::Done(()))
            }
        }
    }
}
