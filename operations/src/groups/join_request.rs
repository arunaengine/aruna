//! Runs the group join flow: asking to join, withdrawing, and the owner's decision.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::auth::epoch_due::DueMarks;
use crate::notifications::routing::{RoutingContext, route_resource_event};
use crate::sync::outbox_record::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};
use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::Event;
use aruna_core::join_request::{
    JoinDecision, JoinDecisionKind, JoinRequest, JoinRequestState, valid_message,
};
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, DOCUMENT_STATE_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode, boxed_retry};
use aruna_core::placement::fence::WriteFence;
use aruna_core::placement::target_placement_ref;
use aruna_core::reducer::{AdminDocumentError, decode_reducer_state};
use aruna_core::rows::done;
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, reducer_state_key, stale_conflict_deletes,
};
use aruna_core::structs::execution::notification::{NotificationOutboxRecord, ResourceEvent};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::types::{Key, KeySpace, Value};
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
    Reducer(#[from] AdminDocumentError),
}

impl From<IoError> for GroupJoinError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::Storage(error),
            _ => Self::UnexpectedEvent,
        }
    }
}

/// Checks a deciding owner, then applies the join step in a write transaction that reruns after a
/// lost conflict, since background writes touch the same rows.
#[derive(Debug, PartialEq)]
pub struct GroupJoinOperation {
    input: GroupJoinInput,
    authorized: bool,
}

impl GroupJoinOperation {
    pub fn new(input: GroupJoinInput) -> Self {
        Self {
            input,
            authorized: false,
        }
    }

    fn change(&mut self) -> Next<JoinRequestState> {
        self.authorized = true;
        let input = self.input.clone();
        let seed = self.input.actor.user_id.to_bytes();
        let build = move || JoinWrite::new(input.clone());
        let child = boxed_retry(build, &seed, |result| {
            Event::Child(ChildOutcome::new(result))
        });
        Next::Call(IoCall::Child(child))
    }
}

impl Operation for GroupJoinOperation {
    type Output = JoinRequestState;
    type Error = GroupJoinError;

    fn first(&mut self) -> Result<Next<JoinRequestState>, GroupJoinError> {
        if self.input.auth.path_restrictions.is_some()
            || self.input.actor.user_id.is_nil()
            || self.input.auth.user_id != self.input.actor.user_id
            || self.input.auth.realm_id != self.input.actor.realm_id
        {
            return Err(GroupJoinError::Unauthorized);
        }
        if !matches!(self.input.action, JoinAction::Decide { .. }) {
            return Ok(self.change());
        }
        Ok(Next::Call(IoCall::child(CheckPermissionsOperation::new(
            CheckPermissionsConfig {
                auth_context: self.input.auth.clone(),
                path: format!(
                    "/{}/g/{}/admin/users/**",
                    self.input.actor.realm_id, self.input.group_id
                ),
                required_permission: Permission::WRITE,
            },
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<JoinRequestState>, GroupJoinError> {
        if self.authorized {
            return result
                .child::<JoinRequestState, GroupJoinError>()?
                .map(Next::Done);
        }
        if !result.child::<bool, AuthorizationError>()?? {
            return Err(GroupJoinError::Unauthorized);
        }
        Ok(self.change())
    }
}

/// What the join transaction waits for.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Stage {
    Read,
    Write,
    Delete,
    Fence,
    Marks,
}

/// Reads the group's documents and applies the join step in one write transaction, with the
/// bucket fence and, when a DENY role is newly assigned, the group's epoch marks.
#[derive(Debug, PartialEq)]
struct JoinWrite {
    input: GroupJoinInput,
    stage: Stage,
    fence: WriteFence,
    deletes: Vec<(KeySpace, Key)>,
    notifications: bool,
    deny: bool,
    result: Option<JoinRequestState>,
    marks: DueMarks,
}

impl JoinWrite {
    fn new(input: GroupJoinInput) -> Self {
        let marks = DueMarks::new(Some(input.group_id), input.now_ms);
        Self {
            input,
            stage: Stage::Read,
            fence: Default::default(),
            deletes: Vec::new(),
            notifications: false,
            deny: false,
            result: None,
            marks,
        }
    }

    fn mutate(
        &mut self,
        values: Vec<Option<Value>>,
    ) -> Result<Next<JoinRequestState>, GroupJoinError> {
        let [group, auth, reducer, config] =
            <[_; 4]>::try_from(values).map_err(|_| GroupJoinError::UnexpectedEvent)?;
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
            .map(decode_reducer_state)
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
                    return Ok(Next::Done(existing.clone()));
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
                    return Ok(Next::Done(existing.clone()));
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
                    return Ok(Next::Done(existing.clone()));
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
                    let role = auth
                        .roles
                        .get_mut(role_id)
                        .ok_or(GroupJoinError::InvalidRoles)?;
                    let added = role.assigned_users.insert(decision.user_id);
                    // A newly assigned DENY rule can narrow the member's existing READ scopes.
                    self.deny |= added && role.permissions.values().any(|p| *p == Permission::DENY);
                }
                decision.request_id
            }
            _ => return Err(GroupJoinError::UnexpectedEvent),
        };
        let notifications = if matches!(op, AdminDocumentOperation::GroupJoinRequested { .. }) {
            route_resource_event(
                &ResourceEvent::GroupJoinRequested {
                    group_id: self.input.group_id,
                    request_id,
                    actor_user_id: self.input.actor.user_id,
                },
                RoutingContext {
                    group_auth: Some(&auth),
                    realm_auth: None,
                    realm_id: self.input.actor.realm_id,
                },
                self.input.now_ms,
            )
        } else {
            Vec::new()
        };
        let event = state.apply_operation(&self.input.actor, op)?;
        let config =
            RealmConfigDocument::from_bytes(config.as_deref().ok_or(GroupJoinError::NotFound)?)?;
        if config.realm_id != group.realm_id {
            return Err(GroupJoinError::Unauthorized);
        }
        let target = DocumentTarget::GroupAuthorization {
            group_id: self.input.group_id,
        };
        let placement = target_placement_ref(&config, &target, Default::default());
        self.fence.add(group.realm_id, &config, [placement]);
        let record = new_identified_record(
            event.event_id,
            self.input.actor.node_id,
            target,
            Vec::new(),
            DocumentOutboxEvent::admin(event),
            placement,
            false,
        )
        .fenced_at(self.fence.generation(&group.realm_id, &placement));
        self.deletes = stale_conflict_deletes(previous.as_ref(), Some(&state));
        self.result = Some(
            state
                .join_requests()
                .into_iter()
                .find(|entry| entry.request.request_id == request_id)
                .ok_or(GroupJoinError::NotFound)?,
        );
        let mut writes = vec![
            (
                AUTH_KEYSPACE.into(),
                self.input.group_id.to_bytes().to_vec().into(),
                auth.to_bytes(&self.input.actor)?.into(),
            ),
            reducer_state_entry(&state)?,
            outbox_write_entry(&record).map_err(ConversionError::from)?,
        ];
        writes.extend(conflict_write_entries(&state)?);
        self.notifications = !notifications.is_empty();
        for record in notifications {
            writes.push(aruna_core::storage_entries::outbox_write_entry(
                &NotificationOutboxRecord {
                    outbox_id: record.notification_id,
                    record,
                },
            )?);
        }
        self.stage = Stage::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    fn after_writes(&mut self) -> Result<Next<JoinRequestState>, GroupJoinError> {
        if self.fence.is_empty() {
            return self.after_fence();
        }
        self.stage = Stage::Fence;
        Ok(Next::Call(IoCall::Read(self.fence.reads())))
    }

    fn after_fence(&mut self) -> Result<Next<JoinRequestState>, GroupJoinError> {
        if self.deny {
            self.stage = Stage::Marks;
            return Ok(Next::Call(self.marks.first()));
        }
        self.finish()
    }

    /// Wakes the document outbox drain after the commit, and the notification drain when a
    /// request notified the owners; a failed timer leaves the durable outbox retryable.
    fn finish(&mut self) -> Result<Next<JoinRequestState>, GroupJoinError> {
        let output = self.result.take().ok_or(GroupJoinError::NotFinished)?;
        let mut calls = vec![IoCall::Effect(Box::new(schedule_drain_effect()))];
        if self.notifications {
            let notices = crate::notifications::outbox_record::schedule_drain_effect();
            calls.push(IoCall::Effect(Box::new(notices)));
        }
        Ok(Next::Then { output, calls })
    }
}

fn normalized(value: &Option<String>) -> Option<String> {
    value
        .as_ref()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

impl Operation for JoinWrite {
    type Output = JoinRequestState;
    type Error = GroupJoinError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn conflict_retries() -> u32 {
        u32::from(super::CONFLICT_RETRIES)
    }

    fn first(&mut self) -> Result<Next<JoinRequestState>, GroupJoinError> {
        let group_key: Key = self.input.group_id.to_bytes().to_vec().into();
        let target = AdminDocumentTarget::Group {
            group_id: self.input.group_id,
        };
        Ok(Next::Call(IoCall::Read(vec![
            (GROUP_KEYSPACE.into(), group_key.clone()),
            (AUTH_KEYSPACE.into(), group_key),
            (DOCUMENT_STATE_KEYSPACE.into(), reducer_state_key(&target)),
            (
                REALM_CONFIG_KEYSPACE.into(),
                self.input.actor.realm_id.as_bytes().to_vec().into(),
            ),
        ])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<JoinRequestState>, GroupJoinError> {
        match self.stage {
            Stage::Read => match result {
                IoResult::Values(values) => self.mutate(values),
                _ => Err(GroupJoinError::UnexpectedEvent),
            },
            Stage::Write if !self.deletes.is_empty() => {
                done(result)?;
                self.stage = Stage::Delete;
                Ok(Next::Call(IoCall::Delete(std::mem::take(
                    &mut self.deletes,
                ))))
            }
            Stage::Write | Stage::Delete => {
                done(result)?;
                self.after_writes()
            }
            Stage::Fence => {
                let IoResult::Values(values) = result else {
                    return Err(GroupJoinError::UnexpectedEvent);
                };
                let keys = self.fence.reads().into_iter().map(|(_, key)| key);
                let rows: Vec<_> = keys.zip(values).collect();
                if !self.fence.admits(&rows) {
                    return Err(GroupJoinError::PlacementFenced);
                }
                self.after_fence()
            }
            Stage::Marks => match self.marks.next(result, true) {
                Ok(Some(call)) => Ok(Next::Call(call)),
                Ok(None) => self.finish(),
                Err(error) => Err(StorageError::WriteError(error.to_string()).into()),
            },
        }
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::admin_documents::AdminRoleDefinition;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::document::DocumentOutboxRecord;
    use aruna_core::keyspaces::SYNC_OUTBOX_KEYSPACE;
    use aruna_core::reducer::AdminDocumentState;
    use aruna_core::structs::identity::realm::RealmId;

    #[test]
    fn retries_commit_conflict() {
        // A lost commit reruns the whole join step, rereading every row, up to the retry bound.
        let conflict = GroupJoinError::from(IoError::Conflict);
        assert!(JoinWrite::is_conflict(&conflict));
        assert_eq!(
            JoinWrite::conflict_retries(),
            u32::from(crate::groups::CONFLICT_RETRIES)
        );
        let realm_id = RealmId::from_bytes([8; 32]);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
            user_id: UserId::local(Ulid::from_bytes([4; 16]), realm_id),
            realm_id,
        };
        let mut restricted = GroupJoinOperation::new(GroupJoinInput {
            auth: AuthContext {
                user_id: actor.user_id,
                realm_id,
                path_restrictions: Some(Vec::new()),
                session: None,
            },
            actor,
            group_id: Ulid::from_bytes([5; 16]),
            action: JoinAction::Request { message: None },
            now_ms: 3,
        });
        assert_eq!(restricted.first(), Err(GroupJoinError::Unauthorized));
    }

    #[test]
    fn membership_is_atomic() {
        let realm_id = RealmId::from_bytes([7; 32]);
        let group_id = Ulid::from_bytes([3; 16]);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[1; 32]).public(),
            user_id: UserId::local(Ulid::from_bytes([1; 16]), realm_id),
            realm_id,
        };
        // A federated user of realm 9 asks to join and is approved.
        let member = Actor {
            user_id: UserId::new(Ulid::from_bytes([2; 16]), RealmId::from_bytes([9; 32])),
            ..actor.clone()
        };
        let auth_doc =
            GroupAuthorizationDocument::default_group_doc(actor.user_id, realm_id, group_id);
        let user_role = auth_doc
            .roles
            .values()
            .find(|role| role.name == "user")
            .unwrap()
            .role_id;
        let group = Group {
            group_id,
            realm_id,
            display_name: "Group".into(),
            owner: actor.user_id,
            roles: auth_doc.roles.keys().copied().collect(),
        };
        let mut reducer = AdminDocumentState::new(AdminDocumentTarget::Group { group_id });
        reducer
            .apply_operation(
                &actor,
                AdminDocumentOperation::GroupCreated {
                    realm_id,
                    display_name: "Group".into(),
                    owner: actor.user_id,
                },
            )
            .unwrap();
        for role in auth_doc.roles.values() {
            reducer
                .apply_operation(
                    &actor,
                    AdminDocumentOperation::GroupRoleCreated {
                        role: AdminRoleDefinition::from(role),
                    },
                )
                .unwrap();
        }
        let request_id = Ulid::from_bytes([5; 16]);
        reducer
            .apply_operation(
                &member,
                AdminDocumentOperation::GroupJoinRequested {
                    request: JoinRequest {
                        request_id,
                        group_id,
                        user_id: member.user_id,
                        message: None,
                        created_at: 1,
                    },
                },
            )
            .unwrap();
        let config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        let mut checked = GroupJoinOperation::new(GroupJoinInput {
            actor: actor.clone(),
            auth: AuthContext {
                user_id: actor.user_id,
                realm_id,
                path_restrictions: None,
                session: None,
            },
            group_id,
            action: JoinAction::Decide {
                request_id,
                approve: true,
                role_ids: BTreeSet::new(),
                reason: None,
            },
            now_ms: 2,
        });
        assert!(matches!(checked.first(), Ok(Next::Call(IoCall::Child(_)))));
        let allowed = ChildOutcome::new(Ok::<bool, AuthorizationError>(true));
        assert!(matches!(
            checked.next(IoResult::Child(allowed)),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let mut operation = JoinWrite::new(checked.input.clone());
        let values: Vec<Option<Value>> = vec![
            Some(group.to_bytes(&actor).unwrap().into()),
            Some(auth_doc.to_bytes(&actor).unwrap().into()),
            Some(reducer_state_entry(&reducer).unwrap().2),
            Some(config.to_bytes(&actor).unwrap().into()),
        ];
        let requestor = Actor {
            user_id: UserId::local(Ulid::from_bytes([8; 16]), realm_id),
            ..actor.clone()
        };
        let input = GroupJoinInput {
            actor: requestor.clone(),
            auth: AuthContext {
                user_id: requestor.user_id,
                realm_id,
                path_restrictions: None,
                session: None,
            },
            group_id,
            action: JoinAction::Request { message: None },
            now_ms: 3,
        };
        let mut request_operation = JoinWrite::new(input.clone());
        assert_eq!(request_operation.transaction(), TransactionMode::Write);
        request_operation.first().unwrap();
        let request_turn = request_operation.next(IoResult::Values(values.clone()));
        let Ok(Next::Call(IoCall::Write(writes))) = request_turn else {
            panic!("request must write one atomic batch");
        };
        let notifications: Vec<_> = writes
            .iter()
            .filter(|(space, _, _)| space == aruna_core::keyspaces::NOTIFICATION_OUTBOX_KEYSPACE)
            .collect();
        assert_eq!(notifications.len(), 1);
        let notification = NotificationOutboxRecord::from_bytes(&notifications[0].2).unwrap();
        assert_eq!(notification.record.recipient, actor.user_id);
        assert!(
            matches!(notification.record.kind, aruna_core::structs::execution::notification::NotificationKind::GroupJoinRequested { group_id: notified_group, actor_user_id, .. } if notified_group == group_id && actor_user_id == requestor.user_id)
        );
        let mut duplicate_values = values.clone();
        duplicate_values[2] = Some(
            writes
                .iter()
                .find(|(space, _, _)| space == DOCUMENT_STATE_KEYSPACE)
                .unwrap()
                .2
                .clone(),
        );
        // A repeated request finds its open request and writes nothing.
        let mut duplicate = JoinWrite::new(input);
        duplicate.first().unwrap();
        assert!(matches!(
            duplicate.next(IoResult::Values(duplicate_values)),
            Ok(Next::Done(_))
        ));
        operation.first().unwrap();
        let turn = operation.next(IoResult::Values(values));
        let Ok(Next::Call(IoCall::Write(writes))) = turn else {
            panic!("expected one atomic batch: {turn:?}");
        };
        let value = |space: &str| {
            &writes
                .iter()
                .find(|(keyspace, _, _)| keyspace == space)
                .unwrap()
                .2
        };
        let auth_doc = GroupAuthorizationDocument::from_bytes(value(AUTH_KEYSPACE)).unwrap();
        assert!(
            auth_doc.roles[&user_role]
                .assigned_users
                .contains(&member.user_id)
        );
        let reducer = decode_reducer_state(value(DOCUMENT_STATE_KEYSPACE)).unwrap();
        assert_eq!(
            reducer.join_requests()[0].decision.as_ref().unwrap().kind,
            JoinDecisionKind::Approved
        );
        let outbox: DocumentOutboxRecord =
            postcard::from_bytes(value(SYNC_OUTBOX_KEYSPACE)).unwrap();
        assert!(
            matches!(outbox.event, DocumentOutboxEvent::AdminOperation { event, .. }
        if matches!(event.op, AdminDocumentOperation::GroupJoinDecided { .. }))
        );
    }

    #[test]
    fn deny_approval_marks() {
        let realm_id = RealmId::from_bytes([17; 32]);
        let group_id = Ulid::from_bytes([13; 16]);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[11; 32]).public(),
            user_id: UserId::local(Ulid::from_bytes([11; 16]), realm_id),
            realm_id,
        };
        let member = Actor {
            user_id: UserId::local(Ulid::from_bytes([12; 16]), realm_id),
            ..actor.clone()
        };
        let mut auth_doc =
            GroupAuthorizationDocument::default_group_doc(actor.user_id, realm_id, group_id);
        let role = auth_doc
            .roles
            .values_mut()
            .find(|role| role.name == "user")
            .unwrap();
        role.permissions.insert("/blocked".into(), Permission::DENY);
        let group = Group {
            group_id,
            realm_id,
            display_name: "Group".into(),
            owner: actor.user_id,
            roles: auth_doc.roles.keys().copied().collect(),
        };
        let mut reducer = AdminDocumentState::new(AdminDocumentTarget::Group { group_id });
        for role in auth_doc.roles.values() {
            let role = AdminRoleDefinition::from(role);
            let op = AdminDocumentOperation::GroupRoleCreated { role };
            reducer.apply_operation(&actor, op).unwrap();
        }
        let request_id = Ulid::from_bytes([15; 16]);
        let request = JoinRequest {
            request_id,
            group_id,
            user_id: member.user_id,
            message: None,
            created_at: 1,
        };
        let op = AdminDocumentOperation::GroupJoinRequested { request };
        reducer.apply_operation(&member, op).unwrap();
        let config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        let input = GroupJoinInput {
            actor: actor.clone(),
            auth: AuthContext {
                user_id: actor.user_id,
                realm_id,
                path_restrictions: None,
                session: None,
            },
            group_id,
            action: JoinAction::Decide {
                request_id,
                approve: true,
                role_ids: BTreeSet::new(),
                reason: None,
            },
            now_ms: 2,
        };
        let mut values: Vec<Option<Value>> = vec![
            Some(group.to_bytes(&actor).unwrap().into()),
            Some(auth_doc.to_bytes(&actor).unwrap().into()),
            Some(reducer_state_entry(&reducer).unwrap().2),
            Some(config.to_bytes(&actor).unwrap().into()),
        ];
        let decide = |values: Vec<Option<Value>>| {
            let mut operation = JoinWrite::new(input.clone());
            operation.first().unwrap();
            let turn = operation.next(IoResult::Values(values));
            (operation, turn)
        };
        let (mut operation, turn) = decide(values.clone());
        let Ok(Next::Call(IoCall::Write(writes))) = turn else {
            panic!("expected one atomic batch: {turn:?}");
        };
        let mut turn = operation.next(IoResult::Done);
        if let Ok(Next::Call(IoCall::Delete(_))) = turn {
            turn = operation.next(IoResult::Done);
        }

        // The newly assigned DENY role marks epochs due inside the approval's transaction.
        assert!(matches!(turn, Ok(Next::Call(IoCall::Page { .. }))));

        // A duplicate decision writes nothing.
        for (slot, space) in [(1, AUTH_KEYSPACE), (2, DOCUMENT_STATE_KEYSPACE)] {
            let written = writes.iter().find(|(keyspace, _, _)| keyspace == space);
            values[slot] = Some(written.unwrap().2.clone());
        }
        let (_, turn) = decide(values);
        assert!(matches!(turn, Ok(Next::Done(_))));
    }
}
