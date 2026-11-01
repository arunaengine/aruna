//! Stages each document change of a merge: users, subject claims, roles and devices.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::stage::Change;
use super::{Account, Documents, MergeError, MergeOutcome, MergeState, MergeUsersOperation, plan};
use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::document::{DocumentChange, DocumentTarget};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::{SUBJECT_CLAIMS_KEYSPACE, SUBJECT_INDEX_KEYSPACE};
use aruna_core::storage_entries::{subject_index_key, subject_index_value, sync_revision_entry};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
use aruna_core::structs::identity::user::User;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{Effects, GroupId, RoleId, TxnId, Value};
use aruna_core::user::validation::{
    DEACTIVATED_ATTRIBUTE, MERGED_INTO_ATTRIBUTE, MERGED_PREFIX, counted_attributes,
    validate_attribute_count,
};
use byteview::ByteView;
use smallvec::smallvec;
use std::collections::{BTreeSet, HashMap};
use std::str::FromStr;

use crate::placement::target_placement_ref;
use crate::realm::ensure_config::overlay_reducer_state;
use crate::users::update_user::local_sync_change;

impl MergeUsersOperation {
    /// Stages every change of the merge; the batch read answered group states, then claims.
    pub(super) fn stage_all(
        &mut self,
        txn_id: TxnId,
        values: Vec<(ByteView, Option<Value>)>,
    ) -> Result<Effects, MergeError> {
        let secondary_ids = self.secondary_ids();
        let mut documents = self.documents.take().ok_or(MergeError::NotFinished)?;
        let (group_states, rest) = values.split_at(self.groups.len().min(values.len()));
        let (secondaries, claims) = rest.split_at((secondary_ids.len() * 3).min(rest.len()));
        for (user_id, chunk) in secondary_ids.iter().zip(secondaries.chunks(3)) {
            let [(_, user), (_, state), (_, revision)] = chunk else {
                return Err(MergeError::NotFinished);
            };
            let user = User::from_bytes(user.as_ref().ok_or(MergeError::NotFound)?)?;
            if user.user_id != *user_id {
                return Err(MergeError::NotFound);
            }
            documents.secondaries.push(Account {
                user,
                state: state.clone(),
                revision: revision.clone(),
            });
        }
        let mut outcome = self.empty_outcome(false);
        // Realm and group roles are decided together, since a realm deny also limits group paths.
        let none = HashMap::new();
        let realm_roles = documents
            .realm_auth
            .as_ref()
            .map_or(&none, |auth| &auth.roles);
        let all_roles: Vec<_> = std::iter::once(realm_roles)
            .chain(self.groups.iter().map(|group| &group.roles))
            .collect();
        let others: Vec<_> = self.survivor_groups.iter().collect();
        let mut plans = plan::hand_over(&all_roles, &others, self.survivor_id(), self.merged_id())?;
        let realm_plan = plans.remove(0);
        let subjects = documents.merged.subject_ids.clone();
        self.stage_users(&mut documents, &mut outcome)?;
        self.stage_claims(&subjects, claims)?;
        self.stage_realm(&mut documents, &mut outcome, realm_plan)?;
        self.stage_groups(group_states, &documents.config, &mut outcome, &plans)?;
        self.stage_devices(&mut documents, &mut outcome)?;
        self.outcome = Some(outcome);
        self.state = MergeState::Write { txn_id };
        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes: std::mem::take(&mut self.batch.writes),
            txn_id: Some(txn_id),
        })])
    }

    fn placement(
        &mut self,
        config: &RealmConfigDocument,
        document: &DocumentTarget,
    ) -> PlacementRef {
        let placement = target_placement_ref(config, document, Default::default());
        self.batch
            .fence
            .add(self.config.actor.realm_id, config, [placement]);
        placement
    }

    /// Moves the subjects and copies missing attributes to the survivor, and marks the merged
    /// account deactivated and merged into the survivor. Each account writes its own document.
    fn stage_users(
        &mut self,
        documents: &mut Documents,
        outcome: &mut MergeOutcome,
    ) -> Result<(), MergeError> {
        let survivor = self.survivor_id();
        let mut survivor_ops = Vec::new();
        for subject_id in &documents.merged.subject_ids {
            if !documents.survivor.subject_ids.contains(subject_id) {
                documents.survivor.subject_ids.push(subject_id.clone());
                survivor_ops.push(AdminDocumentOperation::SubjectIdAdded {
                    subject_id: subject_id.clone(),
                });
            }
        }
        let (copied, skipped) = plan::copied_attributes(&documents.survivor, &documents.merged);
        outcome.attributes = copied.len();
        outcome.skipped_attributes = skipped;
        for (key, value) in copied {
            documents
                .survivor
                .attributes
                .insert(key.clone(), value.clone());
            survivor_ops.push(AdminDocumentOperation::UserAttributeSet { key, value });
        }
        // The pointer goes first: a peer that applies a removal after it hands the subject's
        // claim to the survivor, even before the survivor's own addition arrives.
        let mut merged_ops = point_at(&mut documents.merged, survivor, true);
        let removals: Vec<_> = documents
            .merged
            .subject_ids
            .drain(..)
            .map(|subject_id| AdminDocumentOperation::SubjectIdRemoved { subject_id })
            .collect();
        outcome.subjects = removals.len();
        merged_ops.extend(removals);
        // The survivor stays the one primary: the merged account and its secondaries point at it.
        let merged = documents.merged.user_id;
        let moved: Vec<UserId> = std::iter::once(merged)
            .chain(
                documents
                    .secondaries
                    .iter()
                    .map(|account| account.user.user_id),
            )
            .collect();
        outcome.secondaries = moved.len() - 1;
        for user_id in &moved {
            let key = format!("{MERGED_PREFIX}{}", user_id.user_ulid);
            documents
                .survivor
                .attributes
                .insert(key.clone(), user_id.to_string());
            survivor_ops.push(AdminDocumentOperation::UserAttributeSet {
                key,
                value: user_id.to_string(),
            });
        }
        let mut stale: Vec<String> = documents
            .merged
            .attributes
            .keys()
            .filter(|key| key.starts_with(MERGED_PREFIX))
            .cloned()
            .collect();
        stale.sort();
        for key in stale {
            documents.merged.attributes.remove(&key);
            merged_ops.push(AdminDocumentOperation::UserAttributeRemoved { key });
        }
        let config = documents.config.clone();
        // Each account authors its own document, which peers accept as self-service.
        self.stage_user(
            &config,
            &documents.survivor,
            (&documents.survivor_state, &documents.survivor_revision),
            survivor_ops,
        )?;
        self.stage_user(
            &config,
            &documents.merged,
            (&documents.merged_state, &documents.merged_revision),
            merged_ops,
        )?;
        for account in &mut documents.secondaries {
            let ops = point_at(&mut account.user, survivor, false);
            self.stage_user(
                &config,
                &account.user,
                (&account.state, &account.revision),
                ops,
            )?;
        }
        Ok(())
    }

    fn stage_user(
        &mut self,
        config: &RealmConfigDocument,
        user: &User,
        (state, revision): (&Option<Value>, &Option<Value>),
        ops: Vec<AdminDocumentOperation>,
    ) -> Result<(), MergeError> {
        // A merge never leaves an account that its next update would refuse.
        validate_attribute_count(counted_attributes(user.attributes.keys()))?;
        let actor = self.acting(user.user_id);
        let user_id = user.user_id;
        let mut change = Change::new(state.as_ref(), AdminDocumentTarget::User { user_id })?;
        for op in ops {
            change.apply(&actor, op)?;
        }
        let document = DocumentTarget::User { user_id };
        let placement = self.placement(config, &document);
        let previous = revision
            .as_ref()
            .map(|bytes| postcard::from_bytes::<DocumentChange>(bytes.as_ref()))
            .transpose()
            .map_err(ConversionError::from)?;
        let revision = local_sync_change(previous.as_ref(), &actor, placement);
        self.batch.writes.push((
            document.storage_keyspace().to_string(),
            document.storage_key(),
            user.to_bytes(&actor)?.into(),
        ));
        self.batch
            .writes
            .push(sync_revision_entry(&document, &revision)?);
        self.batch.stage(
            actor.node_id,
            actor.realm_id,
            (document, placement),
            &change,
        )
    }

    /// Points each moved subject at its new owner, read as claims or a lone index entry.
    fn stage_claims(
        &mut self,
        subjects: &[String],
        values: &[(ByteView, Option<Value>)],
    ) -> Result<(), MergeError> {
        let (survivor, merged) = (self.survivor_id(), self.merged_id());
        for (subject_id, pair) in subjects.iter().zip(values.chunks(2)) {
            let [(_, claims), (_, index)] = pair else {
                return Err(MergeError::NotFinished);
            };
            let claims = match (claims, index) {
                (Some(bytes), _) => postcard::from_bytes::<BTreeSet<UserId>>(bytes)
                    .map_err(ConversionError::from)?,
                (None, Some(bytes)) => BTreeSet::from([UserId::from_storage_key(bytes)?]),
                (None, None) => BTreeSet::new(),
            };
            let claims = plan::moved_claims(claims, survivor, merged);
            let key = subject_index_key(subject_id);
            let owner = claims.first().copied().unwrap_or(survivor);
            self.batch.writes.push((
                SUBJECT_CLAIMS_KEYSPACE.to_string(),
                key.clone(),
                postcard::to_allocvec(&claims)
                    .map_err(ConversionError::from)?
                    .into(),
            ));
            self.batch.writes.push((
                SUBJECT_INDEX_KEYSPACE.to_string(),
                key,
                subject_index_value(owner),
            ));
        }
        Ok(())
    }

    fn stage_realm(
        &mut self,
        documents: &mut Documents,
        outcome: &mut MergeOutcome,
        (handed, held): (Vec<RoleId>, Vec<RoleId>),
    ) -> Result<(), MergeError> {
        let Some(realm_auth) = documents.realm_auth.as_mut() else {
            return Ok(());
        };
        let (survivor, merged) = (self.survivor_id(), self.merged_id());
        if held.is_empty() {
            return Ok(());
        }
        outcome.realm_roles = handed.len();
        let realm_id = realm_auth.realm_id;
        let actor = self.acting(merged);
        let mut change = Change::new(
            documents.realm_state.as_ref(),
            AdminDocumentTarget::Realm { realm_id },
        )?;
        change.move_roles(
            &actor,
            &mut realm_auth.roles,
            (&handed, &held),
            (survivor, merged),
        )?;
        let document = DocumentTarget::RealmAuthorization { realm_id };
        self.batch.writes.push((
            document.storage_keyspace().to_string(),
            document.storage_key(),
            realm_auth.to_bytes(&actor)?.into(),
        ));
        self.batch.stage(
            actor.node_id,
            realm_id,
            (document, PlacementRef::NIL),
            &change,
        )
    }

    fn stage_groups(
        &mut self,
        states: &[(ByteView, Option<Value>)],
        config: &RealmConfigDocument,
        outcome: &mut MergeOutcome,
        plans: &[(Vec<RoleId>, Vec<RoleId>)],
    ) -> Result<(), MergeError> {
        let (survivor, merged) = (self.survivor_id(), self.merged_id());
        let actor = self.acting(merged);
        let mut groups = std::mem::take(&mut self.groups);
        outcome.groups = groups.len();
        for ((group, (_, state)), (handed, held)) in groups.iter_mut().zip(states).zip(plans) {
            let group_id: GroupId = group.group_id;
            let mut change = Change::new(state.as_ref(), AdminDocumentTarget::Group { group_id })?;
            change.move_roles(&actor, &mut group.roles, (handed, held), (survivor, merged))?;
            let document = DocumentTarget::GroupAuthorization { group_id };
            let placement = self.placement(config, &document);
            self.batch.writes.push((
                document.storage_keyspace().to_string(),
                document.storage_key(),
                group.to_bytes(&actor)?.into(),
            ));
            self.batch.stage(
                actor.node_id,
                actor.realm_id,
                (document, placement),
                &change,
            )?;
        }
        self.groups = groups;
        Ok(())
    }

    /// Re-owns the merged account's devices; each must sign in again as the survivor.
    fn stage_devices(
        &mut self,
        documents: &mut Documents,
        outcome: &mut MergeOutcome,
    ) -> Result<(), MergeError> {
        let (survivor, merged) = (self.survivor_id(), self.merged_id());
        let devices = documents
            .config
            .nodes
            .iter()
            .filter(|node| node.kind.owner() == Some(merged))
            .map(|node| NodeId::from_str(&node.node_id))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| ConversionError::FromStrError(error.to_string()))?;
        outcome.devices = devices.len();
        if devices.is_empty() {
            return Ok(());
        }
        let realm_id = self.config.actor.realm_id;
        let actor = self.acting(merged);
        let mut change = Change::new(
            documents.config_state.as_ref(),
            AdminDocumentTarget::RealmConfig { realm_id },
        )?;
        for node_id in devices {
            change.apply(
                &actor,
                AdminDocumentOperation::ConfigNodeEnsured {
                    node_id,
                    kind: RealmNodeKind::User { owner: survivor },
                },
            )?;
        }
        overlay_reducer_state(
            &mut documents.config,
            &change.state,
            unix_timestamp_millis(),
        );
        let document = DocumentTarget::RealmConfig { realm_id };
        let placement = target_placement_ref(&documents.config, &document, Default::default());
        self.batch.writes.push((
            document.storage_keyspace().to_string(),
            document.storage_key(),
            documents.config.to_bytes(&actor)?.into(),
        ));
        self.batch
            .stage(actor.node_id, realm_id, (document, placement), &change)
    }
}

/// Marks `user` merged into `primary`; a newly merged account is also deactivated.
fn point_at(user: &mut User, primary: UserId, deactivate: bool) -> Vec<AdminDocumentOperation> {
    let mut pairs = vec![(MERGED_INTO_ATTRIBUTE, primary.to_string())];
    if deactivate {
        pairs.insert(0, (DEACTIVATED_ATTRIBUTE, "true".to_string()));
    }
    pairs
        .into_iter()
        .map(|(key, value)| {
            user.attributes.insert(key.to_string(), value.clone());
            AdminDocumentOperation::UserAttributeSet {
                key: key.to_string(),
                value,
            }
        })
        .collect()
}
