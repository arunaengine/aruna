//! Freezes local group creation while checking emptiness, or records cancellation.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::SystemTime;

use aruna_core::NodeId;
use aruna_core::admin_documents::AdminDocumentTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{
    DOCUMENT_STATE_KEYSPACE, GROUP_DELETE_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE,
};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::reducer::decode_reducer_state;
use aruna_core::storage_entries::reducer_state_key;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::group::Group;
use aruna_core::structs::identity::group_delete::{
    GroupDeletePhase, GroupDeletePlan, GroupDeleteRecord, GroupDeletionError, MembershipFence,
};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::types::{Effects, TxnId, Value};
use smallvec::smallvec;

use super::resources::EmptyCheck;
use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};

#[derive(Debug, PartialEq)]
enum State {
    Init,
    Start,
    Read,
    GroupAuth,
    RealmAuth,
    Scan,
    Write,
    Commit,
    Persist,
    Finish,
}

#[derive(Debug, PartialEq)]
pub(super) struct PrepareOperation {
    plan: GroupDeletePlan,
    phase: GroupDeletePhase,
    node_id: NodeId,
    membership: MembershipFence,
    txn_id: Option<TxnId>,
    state: State,
    scan: EmptyCheck,
    replace: bool,
    output: Option<Result<(), GroupDeletionError>>,
}

impl PrepareOperation {
    pub fn new(
        plan: GroupDeletePlan,
        phase: GroupDeletePhase,
        node_id: NodeId,
        now: SystemTime,
    ) -> Self {
        let scan = EmptyCheck::new(plan.group_id, now);
        Self {
            plan,
            phase,
            node_id,
            membership: MembershipFence::default(),
            txn_id: None,
            state: State::Init,
            scan,
            replace: true,
            output: None,
        }
    }

    fn fail(&mut self, error: GroupDeletionError) -> Effects {
        self.output = Some(Err(error));
        self.state = State::Finish;
        self.abort()
    }

    fn inspect(
        &mut self,
        values: Vec<(aruna_core::types::Key, Option<Value>)>,
        txn_id: TxnId,
    ) -> Result<Effects, GroupDeletionError> {
        let [
            (_, current),
            (_, cancelled),
            (_, config),
            (_, group),
            (_, reducer),
            (_, membership),
        ] = values.as_slice()
        else {
            return Err(GroupDeletionError::Unavailable(
                "invalid deletion read".into(),
            ));
        };
        self.membership = MembershipFence::from_value(membership.as_deref())?;
        let config = config.as_deref().ok_or_else(|| {
            GroupDeletionError::Unavailable("realm configuration is missing".into())
        })?;
        let config = RealmConfigDocument::from_bytes(config)?;
        if self.phase == GroupDeletePhase::Preparing
            && (!self.plan.matches_config(&config) || !self.plan.nodes.contains(&self.node_id))
        {
            return Err(GroupDeletionError::Conflict(
                "realm membership changed; retry deletion".into(),
            ));
        }
        if let Some(group) = group.as_deref() {
            let group = Group::from_bytes(group)?;
            if group.group_id != self.plan.group_id
                || group.realm_id != self.plan.realm_id
                || group.owner != self.plan.owner
            {
                return Err(GroupDeletionError::Invalid(
                    "group identity does not match the plan".into(),
                ));
            }
        }
        if let Some(reducer) = reducer.as_deref() {
            let reducer = decode_reducer_state(reducer)
                .map_err(|error| GroupDeletionError::Unavailable(error.to_string()))?;
            if reducer.group_deleted() {
                return Err(GroupDeletionError::Conflict(
                    "group deletion has already committed".into(),
                ));
            }
        }
        let current = current
            .as_deref()
            .map(GroupDeleteRecord::from_bytes)
            .transpose()?;
        if current
            .as_ref()
            .is_some_and(|record| record.phase == GroupDeletePhase::Deleted)
        {
            return Err(GroupDeletionError::Conflict(
                "group deletion has already committed".into(),
            ));
        }
        if self.phase == GroupDeletePhase::Preparing {
            if cancelled.is_some() {
                return Err(GroupDeletionError::Conflict(
                    "deletion attempt was cancelled".into(),
                ));
            }
            if current.as_ref().is_some_and(|record| {
                record.blocks_writes()
                    && (record.plan != self.plan || record.phase != GroupDeletePhase::Preparing)
            }) {
                return Err(GroupDeletionError::Conflict(
                    "another deletion decision is pending".into(),
                ));
            }
            if current.as_ref().is_some_and(|record| {
                record.plan == self.plan && record.phase == GroupDeletePhase::Preparing
            }) {
                self.state = State::Scan;
                return Ok(self.scan.start(txn_id));
            }
            if group.is_none() {
                if config.nodes.iter().any(|node| {
                    node.node_id == self.node_id.to_string() && node.kind.is_sync_eligible()
                }) {
                    return Err(GroupDeletionError::Unavailable(
                        "group has not synchronized to this node".into(),
                    ));
                }
                self.state = State::Scan;
                return Ok(self.scan.start(txn_id));
            }
            self.state = State::GroupAuth;
            return Ok(self.authorize(txn_id, false));
        }
        self.replace = current
            .as_ref()
            .is_none_or(|record| record.plan == self.plan || !record.blocks_writes());
        self.write(txn_id)
    }

    fn authorize(&self, txn_id: TxnId, realm: bool) -> Effects {
        let path = if realm {
            format!("/{}/admin/groups", self.plan.realm_id)
        } else {
            format!("/{}/g/{}/admin", self.plan.realm_id, self.plan.group_id)
        };
        let auth_context = AuthContext {
            user_id: self.plan.requested_by,
            realm_id: self.plan.realm_id,
            path_restrictions: None,
            session: None,
        };
        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new_with_txn(
                CheckPermissionsConfig {
                    auth_context,
                    path,
                    required_permission: Permission::WRITE
                },
                txn_id
            ),
            |allowed| Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }),
        ))]
    }

    fn write(&mut self, txn_id: TxnId) -> Result<Effects, GroupDeletionError> {
        let mut writes = Vec::new();
        if self.replace {
            if self.phase == GroupDeletePhase::Preparing {
                self.membership.pending.insert(self.plan.group_id);
            } else {
                self.membership.pending.remove(&self.plan.group_id);
            }
            writes.push(self.membership.entry(self.plan.realm_id)?);
            let record = GroupDeleteRecord {
                plan: self.plan.clone(),
                phase: self.phase.clone(),
                deleted_by: None,
                event: None,
            };
            writes.push((
                GROUP_DELETE_KEYSPACE.to_string(),
                self.plan.group_id.to_bytes().into(),
                record.to_bytes()?.into(),
            ));
        }
        if matches!(
            self.phase,
            GroupDeletePhase::Aborting | GroupDeletePhase::Cancelled
        ) {
            writes.push((
                GROUP_DELETE_KEYSPACE.to_string(),
                self.plan.cancelled_key().into(),
                vec![1].into(),
            ));
        }
        self.state = State::Write;
        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id)
        })])
    }
}

impl Operation for PrepareOperation {
    type Output = ();
    type Error = GroupDeletionError;

    fn start(&mut self) -> Effects {
        self.state = State::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        let result = match (&self.state, event) {
            (State::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                self.state = State::Read;
                Ok(smallvec![Effect::Storage(StorageEffect::BatchRead {
                    reads: vec![
                        (
                            GROUP_DELETE_KEYSPACE.to_string(),
                            self.plan.group_id.to_bytes().into()
                        ),
                        (
                            GROUP_DELETE_KEYSPACE.to_string(),
                            self.plan.cancelled_key().into()
                        ),
                        (
                            REALM_CONFIG_KEYSPACE.to_string(),
                            self.plan.realm_id.as_bytes().to_vec().into()
                        ),
                        (
                            GROUP_KEYSPACE.to_string(),
                            self.plan.group_id.to_bytes().into()
                        ),
                        (
                            DOCUMENT_STATE_KEYSPACE.to_string(),
                            reducer_state_key(&AdminDocumentTarget::Group {
                                group_id: self.plan.group_id
                            })
                        ),
                        (
                            GROUP_DELETE_KEYSPACE.to_string(),
                            MembershipFence::key(self.plan.realm_id)
                        ),
                    ],
                    txn_id: Some(txn_id),
                })])
            }
            (State::Read, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                match self.txn_id {
                    Some(txn_id) => self.inspect(values, txn_id),
                    None => Err(GroupDeletionError::Unavailable(
                        "missing transaction".into(),
                    )),
                }
            }
            (
                State::GroupAuth | State::RealmAuth,
                Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }),
            ) => match self.txn_id {
                Some(txn_id) => match allowed {
                    Ok(true) => {
                        self.state = State::Scan;
                        Ok(self.scan.start(txn_id))
                    }
                    Ok(false) if self.state == State::GroupAuth => {
                        self.state = State::RealmAuth;
                        Ok(self.authorize(txn_id, true))
                    }
                    Ok(false) => Err(GroupDeletionError::Unauthorized),
                    Err(error) => Err(GroupDeletionError::Unavailable(error.to_string())),
                },
                None => Err(GroupDeletionError::Unavailable(
                    "missing transaction".into(),
                )),
            },
            (State::Scan, event) => match self.txn_id {
                Some(txn_id) => match self.scan.step(event, txn_id) {
                    Ok(Some(effects)) => Ok(effects),
                    Ok(None) => self.write(txn_id),
                    Err(error) => Err(error),
                },
                None => Err(GroupDeletionError::Unavailable(
                    "missing transaction".into(),
                )),
            },
            (State::Write, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                match self.txn_id {
                    Some(txn_id) => {
                        self.state = State::Commit;
                        Ok(smallvec![Effect::Storage(
                            StorageEffect::CommitTransaction { txn_id }
                        )])
                    }
                    None => Err(GroupDeletionError::Unavailable(
                        "missing transaction".into(),
                    )),
                }
            }
            (State::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn_id = None;
                self.state = State::Persist;
                Ok(smallvec![Effect::Storage(StorageEffect::SyncAll)])
            }
            (State::Persist, Event::Storage(StorageEvent::SyncAllFinished)) => {
                self.state = State::Finish;
                self.output = Some(Ok(()));
                Ok(smallvec![])
            }
            _ => Err(GroupDeletionError::Unavailable(
                "unexpected deletion event".into(),
            )),
        };
        result.unwrap_or_else(|error| self.fail(error))
    }

    fn is_complete(&self) -> bool {
        self.state == State::Finish
    }

    fn finalize(self) -> Result<(), GroupDeletionError> {
        self.output.unwrap_or_else(|| {
            Err(GroupDeletionError::Unavailable(
                "deletion did not finish".into(),
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
