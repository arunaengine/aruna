use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::document_sync_outbox::{
    new_outbox_record_with_id, outbox_write_entry, schedule_outbox_drain_effect,
};
use crate::placement::placement_ref_for_target;
use aruna_core::admin_document_reducer::{
    AdminDocumentReducerError, decode_admin_document_reducer_state,
};
use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::document::{DocumentSyncOutboxEvent, DocumentSyncTarget};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::join_request::{
    JoinDecision, JoinDecisionKind, JoinRequest, JoinRequestState, valid_message,
};
use aruna_core::keyspaces::{
    ADMIN_DOCUMENT_STATE_KEYSPACE, AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE,
};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::storage_entries::{
    admin_document_conflict_write_entries, admin_document_reducer_state_key,
    admin_document_reducer_state_write_entry, stale_admin_document_conflict_delete_entries,
};
use aruna_core::structs::{
    Actor, AuthContext, Group, GroupAuthorizationDocument, Permission, RealmConfigDocument,
};
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, Key, KeySpace, TxnId, Value};
use smallvec::smallvec;
use std::collections::BTreeSet;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, PartialEq)]
pub enum JoinAction {
    Request {
        message: Option<String>,
    },
    Decide {
        request_id: Ulid,
        approve: bool,
        role_ids: BTreeSet<Ulid>,
        reason: Option<String>,
    },
    Withdraw {
        request_id: Ulid,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct GroupJoinInput {
    pub actor: Actor,
    pub auth: AuthContext,
    pub group_id: Ulid,
    pub action: JoinAction,
    pub now_ms: u64,
}

#[derive(Debug, Error, PartialEq)]
pub enum GroupJoinError {
    #[error("not authorized to manage this membership request")]
    Unauthorized,
    #[error("group or membership request not found")]
    NotFound,
    #[error("membership request is already decided or user is already a member")]
    Conflict,
    #[error("membership request message is invalid")]
    InvalidMessage,
    #[error("approval must select existing roles or one unambiguous default user role")]
    InvalidRoles,
    #[error("group placement changed; retry the request")]
    PlacementFenced,
    #[error("unexpected event for group membership request")]
    UnexpectedEvent,
    #[error("group membership request did not finish")]
    NotFinished,
    #[error(transparent)]
    Authorization(#[from] AuthorizationError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Reducer(#[from] AdminDocumentReducerError),
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum State {
    Init,
    Auth,
    Begin,
    Read,
    Write,
    Delete,
    Fence,
    Commit,
    Schedule,
    Done,
    Failed,
}

#[derive(Debug, PartialEq)]
pub struct GroupJoinOperation {
    input: GroupJoinInput,
    state: State,
    txn_id: Option<TxnId>,
    fence: crate::placement::fence::WriteFence,
    deletes: Vec<(KeySpace, Key)>,
    changed: bool,
    output: Option<Result<JoinRequestState, GroupJoinError>>,
}

impl GroupJoinOperation {
    pub fn new(input: GroupJoinInput) -> Self {
        Self {
            input,
            state: State::Init,
            txn_id: None,
            fence: Default::default(),
            deletes: Vec::new(),
            changed: false,
            output: None,
        }
    }

    fn fail(&mut self, error: GroupJoinError) -> Effects {
        let effects = self.abort();
        self.output = Some(Err(error));
        self.state = State::Failed;
        effects
    }

    fn begin(&mut self) -> Effects {
        self.state = State::Begin;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn read(&mut self, txn_id: TxnId) -> Effects {
        self.txn_id = Some(txn_id);
        self.state = State::Read;
        let group_key: Key = self.input.group_id.to_bytes().to_vec().into();
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (GROUP_KEYSPACE.into(), group_key.clone()),
                (AUTH_KEYSPACE.into(), group_key),
                (
                    ADMIN_DOCUMENT_STATE_KEYSPACE.into(),
                    admin_document_reducer_state_key(&AdminDocumentTarget::Group {
                        group_id: self.input.group_id
                    })
                ),
                (
                    REALM_CONFIG_KEYSPACE.into(),
                    self.input.actor.realm_id.as_bytes().to_vec().into()
                ),
            ],
            txn_id: Some(txn_id),
        })]
    }

    fn mutate(&mut self, values: Vec<(Key, Option<Value>)>) -> Result<Effects, GroupJoinError> {
        let [(_, group), (_, auth), (_, reducer), (_, config)] = values.as_slice() else {
            return Err(GroupJoinError::UnexpectedEvent);
        };
        let group = Group::from_bytes(group.as_deref().ok_or(GroupJoinError::NotFound)?)?;
        let mut auth = GroupAuthorizationDocument::from_bytes(
            auth.as_deref().ok_or(GroupJoinError::NotFound)?,
        )?;
        if group.group_id != self.input.group_id
            || auth.group_id != self.input.group_id
            || group.realm_id != self.input.actor.realm_id
        {
            return Err(GroupJoinError::Unauthorized);
        }
        let previous = reducer
            .as_deref()
            .map(decode_admin_document_reducer_state)
            .transpose()
            .map_err(ConversionError::from)?;
        let mut state = previous.clone().ok_or(GroupJoinError::NotFound)?;
        if state.target
            != (AdminDocumentTarget::Group {
                group_id: self.input.group_id,
            })
        {
            return Err(GroupJoinError::Unauthorized);
        }
        let requests = state.join_requests();
        let op = match &self.input.action {
            JoinAction::Request { message } => {
                if auth
                    .roles
                    .values()
                    .any(|role| role.assigned_users.contains(&self.input.actor.user_id))
                {
                    return Err(GroupJoinError::Conflict);
                }
                if let Some(existing) = requests.iter().find(|entry| {
                    entry.request.user_id == self.input.actor.user_id && entry.decision.is_none()
                }) {
                    self.output = Some(Ok(existing.clone()));
                    return Ok(self.commit());
                }
                let message = normalized(message);
                if !valid_message(&message) {
                    return Err(GroupJoinError::InvalidMessage);
                }
                AdminDocumentOperation::GroupJoinRequested {
                    request: JoinRequest {
                        request_id: Ulid::generate(),
                        group_id: self.input.group_id,
                        user_id: self.input.actor.user_id,
                        message,
                        created_at: self.input.now_ms,
                    },
                }
            }
            JoinAction::Decide {
                request_id,
                approve,
                role_ids,
                reason,
            } => {
                let existing = requests
                    .iter()
                    .find(|entry| entry.request.request_id == *request_id)
                    .ok_or(GroupJoinError::NotFound)?;
                let kind = if *approve {
                    JoinDecisionKind::Approved
                } else {
                    JoinDecisionKind::Denied
                };
                if let Some(decision) = &existing.decision {
                    if decision.kind != kind {
                        return Err(GroupJoinError::Conflict);
                    }
                    self.output = Some(Ok(existing.clone()));
                    return Ok(self.commit());
                }
                let role_ids = if *approve {
                    let roles = if role_ids.is_empty() {
                        let roles: BTreeSet<_> = auth
                            .roles
                            .iter()
                            .filter_map(|(id, role)| (role.name == "user").then_some(*id))
                            .collect();
                        if roles.len() != 1 {
                            return Err(GroupJoinError::InvalidRoles);
                        }
                        roles
                    } else {
                        role_ids.clone()
                    };
                    if roles.iter().any(|id| !auth.roles.contains_key(id)) {
                        return Err(GroupJoinError::InvalidRoles);
                    }
                    roles
                } else {
                    BTreeSet::new()
                };
                let reason = if *approve { None } else { normalized(reason) };
                if !valid_message(&reason) {
                    return Err(GroupJoinError::InvalidMessage);
                }
                AdminDocumentOperation::GroupJoinDecided {
                    decision: JoinDecision {
                        request_id: *request_id,
                        user_id: existing.request.user_id,
                        kind,
                        decided_by: self.input.actor.user_id,
                        reason,
                        decided_at: self.input.now_ms,
                        role_ids,
                    },
                }
            }
            JoinAction::Withdraw { request_id } => {
                let existing = requests
                    .iter()
                    .find(|entry| entry.request.request_id == *request_id)
                    .ok_or(GroupJoinError::NotFound)?;
                if existing.request.user_id != self.input.actor.user_id {
                    return Err(GroupJoinError::Unauthorized);
                }
                if let Some(decision) = &existing.decision {
                    if decision.kind != JoinDecisionKind::Withdrawn {
                        return Err(GroupJoinError::Conflict);
                    }
                    self.output = Some(Ok(existing.clone()));
                    return Ok(self.commit());
                }
                AdminDocumentOperation::GroupJoinDecided {
                    decision: JoinDecision {
                        request_id: *request_id,
                        user_id: existing.request.user_id,
                        kind: JoinDecisionKind::Withdrawn,
                        decided_by: self.input.actor.user_id,
                        reason: None,
                        decided_at: self.input.now_ms,
                        role_ids: BTreeSet::new(),
                    },
                }
            }
        };
        let request_id = match &op {
            AdminDocumentOperation::GroupJoinRequested { request } => request.request_id,
            AdminDocumentOperation::GroupJoinDecided { decision } => {
                for role_id in &decision.role_ids {
                    auth.roles
                        .get_mut(role_id)
                        .ok_or(GroupJoinError::InvalidRoles)?
                        .assigned_users
                        .insert(decision.user_id);
                }
                decision.request_id
            }
            _ => return Err(GroupJoinError::UnexpectedEvent),
        };
        let event = state.apply_operation(&self.input.actor, op)?;
        let config =
            RealmConfigDocument::from_bytes(config.as_deref().ok_or(GroupJoinError::NotFound)?)?;
        if config.realm_id != group.realm_id {
            return Err(GroupJoinError::Unauthorized);
        }
        let target = DocumentSyncTarget::GroupAuthorization {
            group_id: self.input.group_id,
        };
        let placement = placement_ref_for_target(&config, &target, Default::default());
        self.fence.add(group.realm_id, &config, [placement]);
        let record = new_outbox_record_with_id(
            event.event_id,
            self.input.actor.node_id,
            target,
            Vec::new(),
            DocumentSyncOutboxEvent::admin(event),
            placement,
            false,
        )
        .fenced_at(self.fence.generation(&group.realm_id, &placement));
        self.deletes =
            stale_admin_document_conflict_delete_entries(previous.as_ref(), Some(&state));
        self.output = Some(Ok(state
            .join_requests()
            .into_iter()
            .find(|entry| entry.request.request_id == request_id)
            .ok_or(GroupJoinError::NotFound)?));
        let mut writes = vec![
            (
                AUTH_KEYSPACE.into(),
                self.input.group_id.to_bytes().to_vec().into(),
                auth.to_bytes(&self.input.actor)?.into(),
            ),
            admin_document_reducer_state_write_entry(&state)?,
            outbox_write_entry(&record).map_err(ConversionError::from)?,
        ];
        writes.extend(admin_document_conflict_write_entries(&state)?);
        self.changed = true;
        self.state = State::Write;
        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: self.txn_id
        })])
    }

    fn fence(&mut self) -> Effects {
        if self.fence.is_empty() {
            return self.commit();
        }
        self.state = State::Fence;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: self.fence.reads(),
            txn_id: self.txn_id
        })]
    }

    fn commit(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(GroupJoinError::UnexpectedEvent);
        };
        self.state = State::Commit;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }
}

fn normalized(value: &Option<String>) -> Option<String> {
    value
        .as_ref()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

impl Operation for GroupJoinOperation {
    type Output = JoinRequestState;
    type Error = GroupJoinError;

    fn start(&mut self) -> Effects {
        if self.state != State::Init {
            return self.fail(GroupJoinError::UnexpectedEvent);
        }
        if self.input.auth.path_restrictions.is_some()
            || self.input.actor.user_id.is_nil()
            || self.input.auth.user_id != self.input.actor.user_id
            || self.input.auth.realm_id != self.input.actor.realm_id
            || self.input.actor.user_id.realm_id != self.input.actor.realm_id
        {
            return self.fail(GroupJoinError::Unauthorized);
        }
        if matches!(self.input.action, JoinAction::Decide { .. }) {
            self.state = State::Auth;
            return smallvec![Effect::SubOperation(boxed_suboperation(
                CheckPermissionsOperation::new(CheckPermissionsConfig {
                    auth_context: self.input.auth.clone(),
                    path: format!(
                        "/{}/g/{}/admin/users/**",
                        self.input.actor.realm_id, self.input.group_id
                    ),
                    required_permission: Permission::WRITE,
                }),
                |allowed| Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed })
            ))];
        }
        self.begin()
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match (self.state, event) {
            (
                State::Auth,
                Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }),
            ) => match allowed {
                Ok(true) => self.begin(),
                Ok(false) => self.fail(GroupJoinError::Unauthorized),
                Err(error) => self.fail(error.into()),
            },
            (State::Begin, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.read(txn_id)
            }
            (State::Read, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                match self.mutate(values) {
                    Ok(effects) => effects,
                    Err(error) => self.fail(error),
                }
            }
            (State::Write, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                if self.deletes.is_empty() {
                    return self.fence();
                }
                self.state = State::Delete;
                smallvec![Effect::Storage(StorageEffect::BatchDelete {
                    deletes: std::mem::take(&mut self.deletes),
                    txn_id: self.txn_id
                })]
            }
            (State::Delete, Event::Storage(StorageEvent::BatchDeleteResult { .. })) => self.fence(),
            (State::Fence, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                if !self.fence.admits(&values) {
                    return self.fail(GroupJoinError::PlacementFenced);
                }
                self.commit()
            }
            (State::Commit, Event::Storage(StorageEvent::TransactionCommitted { txn_id }))
                if Some(txn_id) == self.txn_id =>
            {
                self.txn_id = None;
                if self.changed {
                    self.state = State::Schedule;
                    smallvec![schedule_outbox_drain_effect()]
                } else {
                    self.state = State::Done;
                    smallvec![]
                }
            }
            (
                State::Schedule,
                Event::Task(TaskEvent::TimerScheduled { .. } | TaskEvent::Error { .. }),
            ) => {
                self.state = State::Done;
                smallvec![]
            }
            _ => self.fail(GroupJoinError::UnexpectedEvent),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, State::Done | State::Failed)
    }
    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if !self.is_complete() {
            return Err(GroupJoinError::NotFinished);
        }
        self.output.ok_or(GroupJoinError::NotFinished)?
    }
    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}
