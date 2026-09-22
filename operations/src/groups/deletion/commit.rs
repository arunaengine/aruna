//! Commits a certified group tombstone with its active rows, owner allocation and outbox.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::admin_documents::{
    AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget,
};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{
    DOCUMENT_STATE_KEYSPACE, GROUP_DELETE_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE,
};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::reducer::{AdminDocumentState, decode_reducer_state};
use aruna_core::storage_entries::{
    conflict_write_entries, group_deletion_entries, reducer_state_key, stale_conflict_deletes,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::group::Group;
use aruna_core::structs::identity::group_delete::{
    GroupDeleteCertificate, GroupDeletePhase, GroupDeleteRecord, GroupDeletionError,
    MembershipFence,
};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::placement::fence::WriteFence;
use crate::placement::target_placement_ref;
use crate::sync::document_outbox::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};

#[derive(Debug, PartialEq)]
enum State {
    Init,
    Start,
    Read,
    GroupAuth,
    RealmAuth,
    Write,
    Delete,
    Fence,
    Commit,
    Persist,
    Schedule,
    Finish,
}

#[derive(Debug, PartialEq)]
pub(super) struct CommitOperation {
    certificate: GroupDeleteCertificate,
    actor: Actor,
    auth: Option<AuthContext>,
    event: Option<AdminDocumentEvent>,
    config: Option<RealmConfigDocument>,
    reducer: Option<AdminDocumentState>,
    deletes: Vec<(String, Key)>,
    membership: MembershipFence,
    txn_id: Option<TxnId>,
    fence: WriteFence,
    state: State,
    output: Option<Result<AdminDocumentEvent, GroupDeletionError>>,
}

impl CommitOperation {
    pub fn new(
        certificate: GroupDeleteCertificate,
        actor: Actor,
        auth: Option<AuthContext>,
        event: Option<AdminDocumentEvent>,
    ) -> Self {
        Self {
            certificate,
            actor,
            auth,
            event,
            config: None,
            reducer: None,
            deletes: Vec::new(),
            membership: MembershipFence::default(),
            txn_id: None,
            fence: Default::default(),
            state: State::Init,
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
        values: Vec<(Key, Option<Value>)>,
        txn_id: TxnId,
    ) -> Result<Effects, GroupDeletionError> {
        let [
            (_, record),
            (_, cancelled),
            (_, group),
            (_, reducer),
            (_, config),
            (_, membership),
        ] = values.as_slice()
        else {
            return Err(GroupDeletionError::Unavailable(
                "invalid commit read".into(),
            ));
        };
        self.membership = MembershipFence::from_value(membership.as_deref())?;
        let record = record.as_deref().ok_or_else(|| {
            GroupDeletionError::Conflict("node has not confirmed emptiness".into())
        })?;
        let record = GroupDeleteRecord::from_bytes(record)?;
        if record.plan != self.certificate.plan {
            return Err(GroupDeletionError::Conflict(
                "deletion attempt changed".into(),
            ));
        }
        if record.phase == GroupDeletePhase::Deleted {
            let event = record.event.ok_or_else(|| {
                GroupDeletionError::Unavailable("committed deletion event is missing".into())
            })?;
            self.event = Some(*event);
            return Ok(self.commit(txn_id));
        }
        if cancelled.is_some() || record.phase != GroupDeletePhase::Preparing {
            return Err(GroupDeletionError::Conflict(
                "deletion attempt was cancelled".into(),
            ));
        }
        let config = config.as_deref().ok_or_else(|| {
            GroupDeletionError::Unavailable("realm configuration is missing".into())
        })?;
        let config = RealmConfigDocument::from_bytes(config)?;
        if self.auth.is_some()
            && (!self.certificate.plan.matches_config(&config)
                || !crate::forward::authorize::is_sync_eligible(&config, self.actor.node_id))
        {
            return Err(GroupDeletionError::Conflict(
                "realm membership changed; retry deletion".into(),
            ));
        }
        if let Some(group) = group.as_deref() {
            let group = Group::from_bytes(group)?;
            if group.group_id != record.plan.group_id
                || group.owner != record.plan.owner
                || group.realm_id != record.plan.realm_id
            {
                return Err(GroupDeletionError::Invalid("group identity changed".into()));
            }
        } else if self.auth.is_some() {
            return Err(GroupDeletionError::NotFound);
        }
        self.reducer = Some(match reducer.as_deref() {
            Some(bytes) => decode_reducer_state(bytes)
                .map_err(|error| GroupDeletionError::Unavailable(error.to_string()))?,
            None => AdminDocumentState::new(AdminDocumentTarget::Group {
                group_id: record.plan.group_id,
            }),
        });
        self.config = Some(config);
        if self.auth.is_some() {
            self.state = State::GroupAuth;
            Ok(self.authorize(txn_id, false))
        } else {
            self.write(txn_id)
        }
    }

    fn authorize(&self, txn_id: TxnId, realm: bool) -> Effects {
        let Some(auth_context) = self.auth.clone() else {
            return smallvec![];
        };
        let plan = &self.certificate.plan;
        let path = if realm {
            format!("/{}/admin/groups", plan.realm_id)
        } else {
            format!("/{}/g/{}/admin", plan.realm_id, plan.group_id)
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
        let Some(previous) = self.reducer.as_ref() else {
            return Err(GroupDeletionError::Unavailable(
                "group reducer is missing".into(),
            ));
        };
        let mut reducer = previous.clone();
        let event = match self.event.as_ref() {
            Some(event) => {
                reducer
                    .apply(event)
                    .map_err(|error| GroupDeletionError::Invalid(error.to_string()))?;
                event.clone()
            }
            None => reducer
                .apply_operation(
                    &self.actor,
                    AdminDocumentOperation::GroupDeleted {
                        certificate: Box::new(self.certificate.clone()),
                    },
                )
                .map_err(|error| GroupDeletionError::Invalid(error.to_string()))?,
        };
        let record = GroupDeleteRecord {
            plan: self.certificate.plan.clone(),
            phase: GroupDeletePhase::Deleted,
            deleted_by: Some(event.actor.user_id),
            event: Some(Box::new(event.clone())),
        };
        let (mut deletes, mut writes) = group_deletion_entries(&record, &reducer)?;
        self.membership.pending.remove(&record.plan.group_id);
        writes.push(self.membership.entry(record.plan.realm_id)?);
        deletes.extend(stale_conflict_deletes(Some(previous), Some(&reducer)));
        writes.extend(conflict_write_entries(&reducer)?);
        if self.auth.is_some() {
            let config = self.config.as_ref().ok_or_else(|| {
                GroupDeletionError::Unavailable("realm configuration is missing".into())
            })?;
            let target = DocumentTarget::GroupAuthorization {
                group_id: record.plan.group_id,
            };
            let placement = target_placement_ref(config, &target, Default::default());
            self.fence.add(record.plan.realm_id, config, [placement]);
            let outbox = new_identified_record(
                event.event_id,
                self.actor.node_id,
                target,
                Vec::new(),
                DocumentOutboxEvent::admin(event.clone()),
                placement,
                false,
            )
            .fenced_at(self.fence.generation(&record.plan.realm_id, &placement));
            writes.push(
                outbox_write_entry(&outbox)
                    .map_err(|error| GroupDeletionError::Unavailable(error.to_string()))?,
            );
        }
        self.event = Some(event);
        self.deletes = deletes;
        self.state = State::Write;
        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id)
        })])
    }

    fn commit(&mut self, txn_id: TxnId) -> Effects {
        self.state = State::Commit;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }
}

impl Operation for CommitOperation {
    type Output = AdminDocumentEvent;
    type Error = GroupDeletionError;

    fn start(&mut self) -> Effects {
        if self.actor.user_id != self.certificate.plan.requested_by {
            return self.fail(GroupDeletionError::Unauthorized);
        }
        if !self.certificate.verify() || self.actor.realm_id != self.certificate.plan.realm_id
            || self.actor.node_id != self.certificate.plan.coordinator
            || self.auth.as_ref().is_some_and(|auth| auth.user_id != self.actor.user_id || auth.realm_id != self.actor.realm_id || auth.path_restrictions.is_some())
            || self.event.as_ref().is_some_and(|event| !matches!(&event.op, AdminDocumentOperation::GroupDeleted { certificate } if certificate.as_ref() == &self.certificate) || event.actor != self.actor)
        {
            return self.fail(GroupDeletionError::Invalid("deletion certificate or actor mismatch".into()));
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
        let result = match (&self.state, event) {
            (State::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                self.state = State::Read;
                let plan = &self.certificate.plan;
                Ok(smallvec![Effect::Storage(StorageEffect::BatchRead {
                    reads: vec![
                        (
                            GROUP_DELETE_KEYSPACE.to_string(),
                            plan.group_id.to_bytes().into()
                        ),
                        (
                            GROUP_DELETE_KEYSPACE.to_string(),
                            plan.cancelled_key().into()
                        ),
                        (GROUP_KEYSPACE.to_string(), plan.group_id.to_bytes().into()),
                        (
                            DOCUMENT_STATE_KEYSPACE.to_string(),
                            reducer_state_key(&AdminDocumentTarget::Group {
                                group_id: plan.group_id
                            })
                        ),
                        (
                            REALM_CONFIG_KEYSPACE.to_string(),
                            plan.realm_id.as_bytes().to_vec().into()
                        ),
                        (
                            GROUP_DELETE_KEYSPACE.to_string(),
                            MembershipFence::key(plan.realm_id)
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
                    Ok(true) => self.write(txn_id),
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
            (State::Write, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                self.state = State::Delete;
                Ok(smallvec![Effect::Storage(StorageEffect::BatchDelete {
                    deletes: std::mem::take(&mut self.deletes),
                    txn_id: self.txn_id
                })])
            }
            (State::Delete, Event::Storage(StorageEvent::BatchDeleteResult { .. })) => {
                match self.txn_id {
                    Some(txn_id) if self.fence.is_empty() => Ok(self.commit(txn_id)),
                    Some(txn_id) => {
                        self.state = State::Fence;
                        Ok(smallvec![Effect::Storage(StorageEffect::BatchRead {
                            reads: self.fence.reads(),
                            txn_id: Some(txn_id)
                        })])
                    }
                    None => Err(GroupDeletionError::Unavailable(
                        "missing transaction".into(),
                    )),
                }
            }
            (State::Fence, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                if self.fence.admits(&values) {
                    match self.txn_id {
                        Some(txn_id) => Ok(self.commit(txn_id)),
                        None => Err(GroupDeletionError::Unavailable(
                            "missing transaction".into(),
                        )),
                    }
                } else {
                    Err(GroupDeletionError::Conflict(
                        "group placement changed".into(),
                    ))
                }
            }
            (State::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn_id = None;
                self.state = State::Persist;
                Ok(smallvec![Effect::Storage(StorageEffect::SyncAll)])
            }
            (State::Persist, Event::Storage(StorageEvent::SyncAllFinished)) => {
                if self.auth.is_some() {
                    self.state = State::Schedule;
                    Ok(smallvec![schedule_drain_effect()])
                } else {
                    self.state = State::Finish;
                    self.output = self.event.take().map(Ok);
                    Ok(smallvec![])
                }
            }
            (
                State::Schedule,
                Event::Task(TaskEvent::TimerScheduled { .. } | TaskEvent::Error { .. }),
            ) => {
                self.state = State::Finish;
                self.output = self.event.take().map(Ok);
                Ok(smallvec![])
            }
            _ => Err(GroupDeletionError::Unavailable(
                "unexpected group commit event".into(),
            )),
        };
        result.unwrap_or_else(|error| self.fail(error))
    }

    fn is_complete(&self) -> bool {
        self.state == State::Finish
    }

    fn finalize(self) -> Result<AdminDocumentEvent, GroupDeletionError> {
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
