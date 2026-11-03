//! Applies merge changes to admin documents and collects the rows of one transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::MergeError;
use aruna_core::admin_documents::{
    AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget,
};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::errors::ConversionError;
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState, decode_reducer_state};
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, stale_conflict_deletes,
};
use aruna_core::structs::identity::auth::{Actor, Role};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::types::{Key, KeySpace, RoleId, Value};
use aruna_core::{NodeId, UserId};
use byteview::ByteView;
use std::collections::HashMap;

use crate::placement::fence::WriteFence;
use crate::sync::document_outbox::{new_identified_record, outbox_write_entry};

/// One document's reducer before and after the merge, with the events it emitted.
#[derive(Debug)]
pub(super) struct Change {
    previous: Option<AdminDocumentState>,
    pub(super) state: AdminDocumentState,
    events: Vec<AdminDocumentEvent>,
}

impl Change {
    pub(super) fn new(
        value: Option<&ByteView>,
        target: AdminDocumentTarget,
    ) -> Result<Self, MergeError> {
        let previous = value
            .map(|value| decode_reducer_state(value).map_err(ConversionError::from))
            .transpose()?;
        if previous
            .as_ref()
            .is_some_and(|state| state.target != target)
        {
            return Err(AdminDocumentError::TargetMismatch.into());
        }
        let state = previous
            .clone()
            .unwrap_or_else(|| AdminDocumentState::new(target));
        Ok(Self {
            previous,
            state,
            events: Vec::new(),
        })
    }

    pub(super) fn apply(
        &mut self,
        actor: &Actor,
        op: AdminDocumentOperation,
    ) -> Result<(), MergeError> {
        self.events.push(self.state.apply_operation(actor, op)?);
        Ok(())
    }

    /// Grants `survivor` the `handed` roles and removes `merged` from `held`, then sets the
    /// stored assignments of those roles from the reducer, for a group or the realm.
    pub(super) fn move_roles(
        &mut self,
        actor: &Actor,
        roles: &mut HashMap<RoleId, Role>,
        (handed, held): (&[RoleId], &[RoleId]),
        (survivor, merged): (UserId, UserId),
    ) -> Result<(), MergeError> {
        let group = matches!(self.state.target, AdminDocumentTarget::Group { .. });
        for (role_id, user_id, added) in handed
            .iter()
            .map(|role_id| (*role_id, survivor, true))
            .chain(held.iter().map(|role_id| (*role_id, merged, false)))
        {
            if self.unseeded(role_id, group) {
                self.apply(actor, seed_op(role_id, group))?;
            }
            self.apply(actor, assignment_op(role_id, user_id, added, group))?;
        }
        let assignments = if group {
            self.state.materialized_group_assignments()
        } else {
            self.state.materialized_realm_assignments()
        };
        for role_id in held {
            let Some(role) = roles.get_mut(role_id) else {
                continue;
            };
            for user_id in [survivor, merged] {
                if assignments
                    .get(role_id)
                    .is_some_and(|users| users.contains(&user_id))
                {
                    role.assigned_users.insert(user_id);
                } else {
                    role.assigned_users.remove(&user_id);
                }
            }
        }
        Ok(())
    }

    fn unseeded(&self, role_id: RoleId, group: bool) -> bool {
        let (roles, prefix) = if group {
            (self.state.materialized_group_roles(), "group")
        } else {
            (self.state.materialized_realm_roles(), "realm")
        };
        !roles.contains(&role_id)
            && !self
                .state
                .conflicts
                .contains_key(&format!("{prefix}.roles.{role_id}"))
    }
}

fn seed_op(role_id: RoleId, group: bool) -> AdminDocumentOperation {
    if group {
        AdminDocumentOperation::GroupRoleAdded { role_id }
    } else {
        AdminDocumentOperation::RealmRoleAdded { role_id }
    }
}

fn assignment_op(
    role_id: RoleId,
    user_id: UserId,
    added: bool,
    group: bool,
) -> AdminDocumentOperation {
    match (group, added) {
        (true, true) => AdminDocumentOperation::GroupAssignmentAdded { role_id, user_id },
        (true, false) => AdminDocumentOperation::GroupAssignmentRemoved { role_id, user_id },
        (false, true) => AdminDocumentOperation::RealmAssignmentAdded { role_id, user_id },
        (false, false) => AdminDocumentOperation::RealmAssignmentRemoved { role_id, user_id },
    }
}

/// The rows one merge transaction writes and deletes, and the buckets it publishes onto.
#[derive(Debug, Default, PartialEq)]
pub(super) struct Batch {
    pub(super) writes: Vec<(KeySpace, Key, Value)>,
    pub(super) deletes: Vec<(KeySpace, Key)>,
    pub(super) fence: WriteFence,
}

impl Batch {
    /// Stages the reducer state, its conflicts and one outbox record per event of `change`.
    pub(super) fn stage(
        &mut self,
        node_id: NodeId,
        realm_id: RealmId,
        (document, placement): (DocumentTarget, PlacementRef),
        change: &Change,
    ) -> Result<(), MergeError> {
        self.writes.push(reducer_state_entry(&change.state)?);
        self.writes.extend(conflict_write_entries(&change.state)?);
        self.deletes.extend(stale_conflict_deletes(
            change.previous.as_ref(),
            Some(&change.state),
        ));
        let generation = self.fence.generation(&realm_id, &placement);
        for event in &change.events {
            let record = new_identified_record(
                event.event_id,
                node_id,
                document.clone(),
                Vec::new(),
                DocumentOutboxEvent::admin(event.clone()),
                placement,
                false,
            )
            .fenced_at(generation);
            self.writes
                .push(outbox_write_entry(&record).map_err(ConversionError::from)?);
        }
        Ok(())
    }
}
