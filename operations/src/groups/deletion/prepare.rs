//! Freezes local group creation while checking emptiness, or records cancellation.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::SystemTime;

use aruna_core::NodeId;
use aruna_core::admin_documents::AdminDocumentTarget;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::errors::AuthorizationError;
use aruna_core::keyspaces::{
    DOCUMENT_STATE_KEYSPACE, GROUP_DELETE_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::reducer::decode_reducer_state;
use aruna_core::rows::done;
use aruna_core::storage_entries::reducer_state_key;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::group::Group;
use aruna_core::structs::identity::group_delete::{
    GroupDeletePhase, GroupDeletePlan, GroupDeleteRecord, GroupDeletionError, MembershipFence,
};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::types::Value;

use super::resources::EmptyCheck;
use crate::auth::permission_rules::{read_rules, rule_reads};

/// Records this node's deletion decision, then syncs it to disk before it answers, so the
/// coordinator never counts a confirmation a crash could lose.
#[derive(Debug, PartialEq)]
pub(super) struct PrepareOperation {
    write: Option<PrepareWrite>,
}

impl PrepareOperation {
    pub fn new(
        plan: GroupDeletePlan,
        phase: GroupDeletePhase,
        node_id: NodeId,
        now: SystemTime,
    ) -> Self {
        let scan = EmptyCheck::new(plan.group_id, now);
        let write = PrepareWrite {
            plan,
            phase,
            node_id,
            membership: MembershipFence::default(),
            stage: Stage::Read,
            scan,
            replace: true,
        };
        Self { write: Some(write) }
    }
}

impl Operation for PrepareOperation {
    type Output = ();
    type Error = GroupDeletionError;

    fn first(&mut self) -> Result<Next<()>, GroupDeletionError> {
        let write = self.write.take().ok_or_else(|| {
            GroupDeletionError::Unavailable("deletion prepare started twice".into())
        })?;
        Ok(Next::Call(IoCall::child(write)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, GroupDeletionError> {
        if let IoResult::Child(_) = result {
            result.child::<(), GroupDeletionError>()??;
            return Ok(Next::Call(IoCall::Sync));
        }
        done(result)?;
        Ok(Next::Done(()))
    }
}

/// What the prepare transaction waits for.
#[derive(Debug, PartialEq)]
enum Stage {
    Read,
    GroupAuth,
    RealmAuth,
    Scan,
    Write,
}

/// Checks the plan, the caller and emptiness, and writes the decision, in one write transaction.
#[derive(Debug, PartialEq)]
struct PrepareWrite {
    plan: GroupDeletePlan,
    phase: GroupDeletePhase,
    node_id: NodeId,
    membership: MembershipFence,
    stage: Stage,
    scan: EmptyCheck,
    replace: bool,
}

fn unavailable(error: AuthorizationError) -> GroupDeletionError {
    GroupDeletionError::Unavailable(error.to_string())
}

impl PrepareWrite {
    fn inspect(&mut self, values: Vec<Option<Value>>) -> Result<Next<()>, GroupDeletionError> {
        let [current, cancelled, config, group, reducer, membership] =
            <[_; 6]>::try_from(values)
                .map_err(|_| GroupDeletionError::Unavailable("invalid deletion read".into()))?;
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
        if self.phase != GroupDeletePhase::Preparing {
            self.replace = current
                .as_ref()
                .is_none_or(|record| record.plan == self.plan || !record.blocks_writes());
            return self.write();
        }
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
            return Ok(self.scan());
        }
        if group.is_none() {
            if config.nodes.iter().any(|node| {
                node.node_id == self.node_id.to_string() && node.kind.is_sync_eligible()
            }) {
                return Err(GroupDeletionError::Unavailable(
                    "group has not synchronized to this node".into(),
                ));
            }
            return Ok(self.scan());
        }
        self.authorize(false)
    }

    fn scan(&mut self) -> Next<()> {
        self.stage = Stage::Scan;
        Next::Call(self.scan.call())
    }

    fn auth_path(&self, realm: bool) -> String {
        match realm {
            true => format!("/{}/admin/groups", self.plan.realm_id),
            false => format!("/{}/g/{}/admin", self.plan.realm_id, self.plan.group_id),
        }
    }

    /// Reads the caller's rules in this transaction: the group's administrators first, then the
    /// realm's group administrators.
    fn authorize(&mut self, realm: bool) -> Result<Next<()>, GroupDeletionError> {
        self.stage = match realm {
            true => Stage::RealmAuth,
            false => Stage::GroupAuth,
        };
        let reads = rule_reads(&self.auth_path(realm)).map_err(unavailable)?;
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn authorized(&mut self, values: Vec<Option<Value>>) -> Result<Next<()>, GroupDeletionError> {
        let realm = self.stage == Stage::RealmAuth;
        let auth_context = AuthContext {
            user_id: self.plan.requested_by,
            realm_id: self.plan.realm_id,
            path_restrictions: None,
            session: None,
        };
        let rules = read_rules(&auth_context, values).map_err(unavailable)?;
        if rules.allows(&self.auth_path(realm), &Permission::WRITE) {
            return Ok(self.scan());
        }
        if !realm {
            return self.authorize(true);
        }
        Err(GroupDeletionError::Unauthorized)
    }

    fn write(&mut self) -> Result<Next<()>, GroupDeletionError> {
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
        self.stage = Stage::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

impl Operation for PrepareWrite {
    type Output = ();
    type Error = GroupDeletionError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<()>, GroupDeletionError> {
        let plan = &self.plan;
        let reducer = AdminDocumentTarget::Group {
            group_id: plan.group_id,
        };
        Ok(Next::Call(IoCall::Read(vec![
            (
                GROUP_DELETE_KEYSPACE.to_string(),
                plan.group_id.to_bytes().into(),
            ),
            (
                GROUP_DELETE_KEYSPACE.to_string(),
                plan.cancelled_key().into(),
            ),
            (
                REALM_CONFIG_KEYSPACE.to_string(),
                plan.realm_id.as_bytes().to_vec().into(),
            ),
            (GROUP_KEYSPACE.to_string(), plan.group_id.to_bytes().into()),
            (
                DOCUMENT_STATE_KEYSPACE.to_string(),
                reducer_state_key(&reducer),
            ),
            (
                GROUP_DELETE_KEYSPACE.to_string(),
                MembershipFence::key(plan.realm_id),
            ),
        ])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, GroupDeletionError> {
        match (&self.stage, result) {
            (Stage::Read, IoResult::Values(values)) => self.inspect(values),
            (Stage::GroupAuth | Stage::RealmAuth, IoResult::Values(values)) => {
                self.authorized(values)
            }
            (Stage::Scan, result) => match self.scan.take(result)? {
                Some(call) => Ok(Next::Call(call)),
                None => self.write(),
            },
            (Stage::Write, result) => {
                done(result)?;
                Ok(Next::Done(()))
            }
            _ => Err(GroupDeletionError::Unavailable(
                "unexpected deletion event".into(),
            )),
        }
    }
}
