//! Commits a certified group tombstone with its active rows, owner allocation and outbox.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::admin_documents::{
    AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget,
};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::errors::AuthorizationError;
use aruna_core::events::Event;
use aruna_core::keyspaces::{
    BACKEND_INDEX_KEYSPACE, DOCUMENT_STATE_KEYSPACE, GROUP_DELETE_KEYSPACE, GROUP_KEYSPACE,
    NODE_VAULT_KEYSPACE, REALM_CONFIG_KEYSPACE, STORAGE_BACKEND_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::reducer::{AdminDocumentState, decode_reducer_state};
use aruna_core::rows::{PrefixPages, done};
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
use aruna_core::structs::storage::group_backend::{GroupStorage, GroupStorageSecret};
use aruna_core::task::TaskEvent;
use aruna_core::types::{Key, Value};

use crate::auth::permission_rules::{read_rules, rule_reads};
use crate::groups::backends::{backend_key, index_prefix};
use crate::sync::outbox_record::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};
use aruna_core::placement::fence::WriteFence;
use aruna_core::placement::target_placement_ref;

/// What the outer commit waits for: the transaction, the sync, then the drain timer.
#[derive(Debug, PartialEq)]
enum Step {
    Write,
    Persist,
    Schedule,
}

/// Commits the tombstone in its own transaction, syncs it to disk, then wakes the outbox drain
/// when it wrote an outbox record. The event is answered only once it is durable.
#[derive(Debug, PartialEq)]
pub(super) struct CommitOperation {
    write: Option<CommitWrite>,
    scheduled: bool,
    event: Option<AdminDocumentEvent>,
    step: Step,
}

impl CommitOperation {
    pub fn new(
        certificate: GroupDeleteCertificate,
        actor: Actor,
        auth: Option<AuthContext>,
        event: Option<AdminDocumentEvent>,
    ) -> Self {
        let scheduled = auth.is_some();
        let backends = PrefixPages::new(
            BACKEND_INDEX_KEYSPACE,
            index_prefix(certificate.plan.group_id),
            128,
        );
        let write = CommitWrite {
            certificate,
            actor,
            auth,
            event,
            config: None,
            reducer: None,
            deletes: Vec::new(),
            membership: MembershipFence::default(),
            fence: Default::default(),
            stage: Stage::Read,
            backends,
        };
        Self {
            write: Some(write),
            scheduled,
            event: None,
            step: Step::Write,
        }
    }
}

impl Operation for CommitOperation {
    type Output = AdminDocumentEvent;
    type Error = GroupDeletionError;

    fn first(&mut self) -> Result<Next<AdminDocumentEvent>, GroupDeletionError> {
        let write = self.write.take().ok_or_else(|| {
            GroupDeletionError::Unavailable("deletion commit started twice".into())
        })?;
        write.admit()?;
        Ok(Next::Call(IoCall::child(write)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<AdminDocumentEvent>, GroupDeletionError> {
        match self.step {
            Step::Write => {
                self.event = Some(result.child::<AdminDocumentEvent, GroupDeletionError>()??);
                self.step = Step::Persist;
                Ok(Next::Call(IoCall::Sync))
            }
            Step::Persist => {
                done(result)?;
                if !self.scheduled {
                    return self.finish();
                }
                self.step = Step::Schedule;
                Ok(Next::Call(IoCall::Effect(
                    Box::new(schedule_drain_effect()),
                )))
            }
            // A failed drain timer only delays the drain; the tombstone is already durable.
            Step::Schedule => match result {
                IoResult::Event(event)
                    if matches!(
                        *event,
                        Event::Task(TaskEvent::TimerScheduled { .. } | TaskEvent::Error { .. })
                    ) =>
                {
                    self.finish()
                }
                _ => Err(GroupDeletionError::Unavailable(
                    "unexpected group commit event".into(),
                )),
            },
        }
    }
}

impl CommitOperation {
    fn finish(&mut self) -> Result<Next<AdminDocumentEvent>, GroupDeletionError> {
        self.event
            .take()
            .map(Next::Done)
            .ok_or_else(|| GroupDeletionError::Unavailable("deletion did not finish".into()))
    }
}

/// What the commit transaction waits for.
#[derive(Debug, PartialEq)]
enum Stage {
    Read,
    GroupAuth,
    RealmAuth,
    Write,
    Backends,
    Delete,
    Fence,
}

/// Checks the certificate against the stored state and writes the tombstone, in one write
/// transaction.
#[derive(Debug, PartialEq)]
struct CommitWrite {
    certificate: GroupDeleteCertificate,
    actor: Actor,
    auth: Option<AuthContext>,
    event: Option<AdminDocumentEvent>,
    config: Option<RealmConfigDocument>,
    reducer: Option<AdminDocumentState>,
    deletes: Vec<(String, Key)>,
    membership: MembershipFence,
    fence: WriteFence,
    stage: Stage,
    backends: PrefixPages,
}

fn unavailable(error: AuthorizationError) -> GroupDeletionError {
    GroupDeletionError::Unavailable(error.to_string())
}

impl CommitWrite {
    /// Refuses a certificate or actor that cannot commit, before any read.
    fn admit(&self) -> Result<(), GroupDeletionError> {
        if self.actor.user_id != self.certificate.plan.requested_by {
            return Err(GroupDeletionError::Unauthorized);
        }
        if !self.certificate.verify() || self.actor.realm_id != self.certificate.plan.realm_id
            || self.actor.node_id != self.certificate.plan.coordinator
            || self.auth.as_ref().is_some_and(|auth| auth.user_id != self.actor.user_id || auth.realm_id != self.actor.realm_id || auth.path_restrictions.is_some())
            || self.event.as_ref().is_some_and(|event| !matches!(&event.op, AdminDocumentOperation::GroupDeleted { certificate } if certificate.as_ref() == &self.certificate) || event.actor != self.actor)
        {
            return Err(GroupDeletionError::Invalid("deletion certificate or actor mismatch".into()));
        }
        Ok(())
    }

    fn inspect(
        &mut self,
        values: Vec<Option<Value>>,
    ) -> Result<Next<AdminDocumentEvent>, GroupDeletionError> {
        let [record, cancelled, group, reducer, config, membership] = <[_; 6]>::try_from(values)
            .map_err(|_| GroupDeletionError::Unavailable("invalid commit read".into()))?;
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
            self.event = Some(*event.clone());
            return Ok(Next::Done(*event));
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
                || !crate::realm::peer_trust::is_sync_eligible(&config, self.actor.node_id))
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
            self.authorize(false)
        } else {
            self.write()
        }
    }

    fn auth_path(&self, realm: bool) -> String {
        let plan = &self.certificate.plan;
        match realm {
            true => format!("/{}/admin/groups", plan.realm_id),
            false => format!("/{}/g/{}/admin", plan.realm_id, plan.group_id),
        }
    }

    /// Reads the caller's rules in this transaction: the group's administrators first, then the
    /// realm's group administrators.
    fn authorize(&mut self, realm: bool) -> Result<Next<AdminDocumentEvent>, GroupDeletionError> {
        self.stage = match realm {
            true => Stage::RealmAuth,
            false => Stage::GroupAuth,
        };
        let reads = rule_reads(&self.auth_path(realm)).map_err(unavailable)?;
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn authorized(
        &mut self,
        values: Vec<Option<Value>>,
    ) -> Result<Next<AdminDocumentEvent>, GroupDeletionError> {
        let realm = self.stage == Stage::RealmAuth;
        let Some(auth_context) = &self.auth else {
            return Err(GroupDeletionError::Unauthorized);
        };
        let rules = read_rules(auth_context, values).map_err(unavailable)?;
        if rules.allows(&self.auth_path(realm), &Permission::WRITE) {
            return self.write();
        }
        if !realm {
            return self.authorize(true);
        }
        Err(GroupDeletionError::Unauthorized)
    }

    fn write(&mut self) -> Result<Next<AdminDocumentEvent>, GroupDeletionError> {
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
        writes.push(trail_drop(record.plan.group_id)?);
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
        self.stage = Stage::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    /// Drops this node's backend records of the group in one page; prepare left only retained ones.
    fn drop_backends(
        &mut self,
        result: IoResult,
    ) -> Result<Next<AdminDocumentEvent>, GroupDeletionError> {
        for (key, value) in self.backends.take(result)? {
            let record = GroupStorage::from_bytes(&value)?;
            if record.group_id != self.certificate.plan.group_id {
                return Err(GroupDeletionError::Invalid("backend index mismatch".into()));
            }
            let entry = GroupStorageSecret::vault_entry(record.backend_id);
            self.deletes.extend([
                (
                    STORAGE_BACKEND_KEYSPACE.to_string(),
                    backend_key(record.backend_id),
                ),
                (BACKEND_INDEX_KEYSPACE.to_string(), key),
                (NODE_VAULT_KEYSPACE.to_string(), entry.key().into()),
            ]);
        }
        if !self.backends.done() {
            return Ok(Next::Call(self.backends.call()));
        }
        self.stage = Stage::Delete;
        Ok(Next::Call(IoCall::Delete(std::mem::take(
            &mut self.deletes,
        ))))
    }

    fn finish(&mut self) -> Result<Next<AdminDocumentEvent>, GroupDeletionError> {
        self.event
            .clone()
            .map(Next::Done)
            .ok_or_else(|| GroupDeletionError::Unavailable("deletion did not finish".into()))
    }
}

/// The cleanup queue drops the group's delete trail, a page at a time; the audit stays.
fn trail_drop(group_id: ulid::Ulid) -> Result<(String, Key, Value), GroupDeletionError> {
    let row = ulid::Ulid::generate();
    let work = aruna_core::structs::storage::blob::BlobCleanupWork::DropTrail {
        group_id,
        bucket: None,
        incarnation: 0,
    };
    let value = work
        .to_bytes()
        .map_err(|error| GroupDeletionError::Invalid(error.to_string()))?;
    let space = aruna_core::keyspaces::BLOB_CLEANUP_KEYSPACE.to_string();
    Ok((space, row.to_bytes().to_vec().into(), value.into()))
}

impl Operation for CommitWrite {
    type Output = AdminDocumentEvent;
    type Error = GroupDeletionError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<AdminDocumentEvent>, GroupDeletionError> {
        let plan = &self.certificate.plan;
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
            (GROUP_KEYSPACE.to_string(), plan.group_id.to_bytes().into()),
            (
                DOCUMENT_STATE_KEYSPACE.to_string(),
                reducer_state_key(&reducer),
            ),
            (
                REALM_CONFIG_KEYSPACE.to_string(),
                plan.realm_id.as_bytes().to_vec().into(),
            ),
            (
                GROUP_DELETE_KEYSPACE.to_string(),
                MembershipFence::key(plan.realm_id),
            ),
        ])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<AdminDocumentEvent>, GroupDeletionError> {
        match (&self.stage, result) {
            (Stage::Read, IoResult::Values(values)) => self.inspect(values),
            (Stage::GroupAuth | Stage::RealmAuth, IoResult::Values(values)) => {
                self.authorized(values)
            }
            (Stage::Write, result) => {
                done(result)?;
                self.stage = Stage::Backends;
                Ok(Next::Call(self.backends.call()))
            }
            (Stage::Backends, result) => self.drop_backends(result),
            (Stage::Delete, result) => {
                done(result)?;
                if self.fence.is_empty() {
                    return self.finish();
                }
                self.stage = Stage::Fence;
                Ok(Next::Call(IoCall::Read(self.fence.reads())))
            }
            (Stage::Fence, IoResult::Values(values)) => {
                let keys = self.fence.reads().into_iter().map(|(_, key)| key);
                let rows: Vec<_> = keys.zip(values).collect();
                if !self.fence.admits(&rows) {
                    return Err(GroupDeletionError::Conflict(
                        "group placement changed".into(),
                    ));
                }
                self.finish()
            }
            _ => Err(GroupDeletionError::Unavailable(
                "unexpected group commit event".into(),
            )),
        }
    }
}
