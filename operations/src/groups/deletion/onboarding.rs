//! Installs realm-signed deletion decisions before a joining node accepts any traffic.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{DOCUMENT_STATE_KEYSPACE, GROUP_DELETE_KEYSPACE};
use aruna_core::onboarding::OnboardingTicket;
use aruna_core::operation::Operation;
use aruna_core::reducer::{AdminDocumentState, decode_reducer_state};
use aruna_core::storage_entries::{group_deletion_entries, reducer_state_key};
use aruna_core::structs::identity::group_delete::{
    GroupDeletePhase, GroupDeleteRecord, GroupDeletionError, MembershipFence,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;

use crate::driver::{DriverContext, drive};

pub async fn install_onboarding(
    context: &DriverContext,
    ticket: &OnboardingTicket,
    realm_id: RealmId,
    node_id: NodeId,
    now: u64,
) -> Result<(), GroupDeletionError> {
    if ticket.payload.realm_id != realm_id.to_string() {
        return Err(GroupDeletionError::Invalid(
            "onboarding realm mismatch".into(),
        ));
    }
    ticket
        .verify(
            node_id,
            &DocumentTarget::RealmAuthorization { realm_id },
            now,
        )
        .map_err(|error| GroupDeletionError::Invalid(error.to_string()))?;
    for record in &ticket.payload.group_deletions {
        if record.plan.realm_id != realm_id {
            return Err(GroupDeletionError::Invalid(
                "deletion realm mismatch".into(),
            ));
        }
        drive(InstallOperation::new(record.clone()), context).await?;
    }
    super::sync_decision(context).await
}

#[derive(Debug, PartialEq)]
enum State {
    Init,
    Start,
    Read,
    Write,
    Delete,
    Commit,
    Finish,
}

#[derive(Debug, PartialEq)]
struct InstallOperation {
    record: GroupDeleteRecord,
    state: State,
    txn_id: Option<TxnId>,
    deletes: Vec<(String, Key)>,
    output: Option<Result<(), GroupDeletionError>>,
}

impl InstallOperation {
    fn new(record: GroupDeleteRecord) -> Self {
        Self {
            record,
            state: State::Init,
            txn_id: None,
            deletes: Vec::new(),
            output: None,
        }
    }

    fn fail(&mut self, error: GroupDeletionError) -> Effects {
        self.state = State::Finish;
        self.output = Some(Err(error));
        self.abort()
    }

    fn inspect(
        &mut self,
        values: Vec<(Key, Option<Value>)>,
    ) -> Result<Effects, GroupDeletionError> {
        let [(_, previous), (_, state), (_, cancelled), (_, membership)] = values.as_slice() else {
            return Err(GroupDeletionError::Unavailable(
                "invalid onboarding read".into(),
            ));
        };
        if cancelled.is_some() {
            return Err(GroupDeletionError::Invalid(
                "onboarding deletion was cancelled".into(),
            ));
        }
        if let Some(bytes) = previous {
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
        let mut state = match state {
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
        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: self.txn_id
        })])
    }
}

impl Operation for InstallOperation {
    type Output = ();
    type Error = GroupDeletionError;

    fn start(&mut self) -> Effects {
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
            return self.fail(GroupDeletionError::Invalid(
                "invalid onboarding deletion".into(),
            ));
        }
        self.state = State::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match (&self.state, event) {
            (State::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                self.state = State::Read;
                smallvec![Effect::Storage(StorageEffect::BatchRead {
                    reads: vec![
                        (
                            GROUP_DELETE_KEYSPACE.to_string(),
                            self.record.plan.group_id.to_bytes().into()
                        ),
                        (
                            DOCUMENT_STATE_KEYSPACE.to_string(),
                            reducer_state_key(&AdminDocumentTarget::Group {
                                group_id: self.record.plan.group_id
                            })
                        ),
                        (
                            GROUP_DELETE_KEYSPACE.to_string(),
                            self.record.plan.cancelled_key().into()
                        ),
                        (
                            GROUP_DELETE_KEYSPACE.to_string(),
                            MembershipFence::key(self.record.plan.realm_id)
                        ),
                    ],
                    txn_id: Some(txn_id),
                })]
            }
            (State::Read, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                match self.inspect(values) {
                    Ok(effects) => effects,
                    Err(error) => self.fail(error),
                }
            }
            (State::Write, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                self.state = State::Delete;
                smallvec![Effect::Storage(StorageEffect::BatchDelete {
                    deletes: std::mem::take(&mut self.deletes),
                    txn_id: self.txn_id
                })]
            }
            (State::Delete, Event::Storage(StorageEvent::BatchDeleteResult { .. })) => {
                let Some(txn_id) = self.txn_id else {
                    return self.fail(GroupDeletionError::Unavailable(
                        "onboarding transaction missing".into(),
                    ));
                };
                self.state = State::Commit;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (State::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn_id = None;
                self.state = State::Finish;
                self.output = Some(Ok(()));
                smallvec![]
            }
            _ => self.fail(GroupDeletionError::Unavailable(
                "unexpected onboarding event".into(),
            )),
        }
    }

    fn is_complete(&self) -> bool {
        self.state == State::Finish
    }

    fn finalize(self) -> Result<(), GroupDeletionError> {
        self.output.unwrap_or_else(|| {
            Err(GroupDeletionError::Unavailable(
                "onboarding did not finish".into(),
            ))
        })
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map(|txn_id| smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })])
            .unwrap_or_default()
    }
}
