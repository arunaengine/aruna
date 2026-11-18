//! Applies replicated admin operations to users, groups, realms and the realm config.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use byteview::ByteView;
use ulid::Ulid;

use super::registry::Row;
use super::{ApplyError, Step, invalid};
use crate::UserId;
use crate::admin_checks::{
    coalescible_config_op, config_mutation_allowed, validate_config_actor, validate_config_target,
    validate_revocation_snapshot,
};
use crate::admin_documents::{
    AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget, roles_narrowed,
};
use crate::document::DocumentTarget;
use crate::keyspaces::{
    BUCKET_ENCRYPTION_KEYSPACE, DOCUMENT_STATE_KEYSPACE, FEDERATION_KEYSPACE,
    GROUP_DELETE_KEYSPACE, GROUP_ENCRYPTED_KEYSPACE, GROUP_KEYSPACE, OWNER_INDEX_KEYSPACE,
    SUBJECT_CLAIMS_KEYSPACE, SUBJECT_INDEX_KEYSPACE, USER_KEYSPACE,
};
use crate::reducer::{
    AdminApplyStatus, AdminDocumentState, RevocationIndex, decode_reducer_state,
    materialize_group_authorization, materialize_realm_authorization, materialize_user_operation,
    materialized_group, materialized_realm_config, needs_revocation_index, overlay_group_state,
    overlay_realm_config,
};
use crate::row_plan::{PlanRows, Planned, RowPlan};
use crate::storage_entries::{
    conflict_write_entries, group_deletion_entries, reducer_state_entry, reducer_state_key,
    stale_conflict_deletes, subject_index_key, subject_index_value,
};
use crate::structs::identity::group::{Group, GroupAuthorizationDocument, owner_group_key};
use crate::structs::identity::group_delete::{GroupDeleteRecord, MembershipFence};
use crate::structs::identity::realm::{RealmAuthorizationDocument, RealmConfigDocument, RealmId};
use crate::structs::identity::user::User;
use crate::structs::storage::abe_access::{due_rows, indexed_buckets};
use crate::types::{GroupId, Key, KeySpace};

/// What the caller still does after an admin operation committed.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct AdminFollowUp {
    /// The change must be durable before the event counts as applied.
    pub sync: bool,
    /// A membership change may give parked snapshots of this realm their publisher.
    pub retry_parked: Option<RealmId>,
}

type Deletes = Vec<(KeySpace, Key)>;

/// A transient conflict must never wedge the topic: an attempt rereads, so a concurrent local
/// change is kept, and the bound is only a livelock safety valve.
const CONFLICT_RETRIES: u32 = 63;

fn wrong(reason: impl Into<String>) -> ApplyError {
    ApplyError::Invalid(reason.into())
}

/// Applies one admin operation to the document it targets.
#[derive(Clone, Debug, PartialEq)]
pub struct AdminOperationPlan {
    pub target: DocumentTarget,
    pub event: AdminDocumentEvent,
}

impl RowPlan for AdminOperationPlan {
    type Output = AdminFollowUp;
    type Error = ApplyError;

    fn plan(&self, rows: &mut PlanRows) -> Step<Planned<AdminFollowUp>> {
        let (target, event) = (&self.target, &self.event);
        match (target, &event.target) {
            (DocumentTarget::User { .. }, AdminDocumentTarget::User { .. }) => {
                apply_user(rows, target, event)
            }
            (DocumentTarget::GroupAuthorization { .. }, AdminDocumentTarget::Group { .. }) => {
                apply_group(rows, target, event)
            }
            (DocumentTarget::RealmAuthorization { .. }, AdminDocumentTarget::Realm { .. }) => {
                apply_realm_authorization(rows, target, event)
            }
            (DocumentTarget::RealmConfig { .. }, AdminDocumentTarget::RealmConfig { .. }) => {
                apply_realm_config(rows, target, event)
            }
            _ => Err(
                wrong("admin document operation target does not match document sync target").into(),
            ),
        }
    }

    fn conflict_retries() -> u32 {
        CONFLICT_RETRIES
    }
}

/// Applies a run of coalescible realm-config events at once. A transition replicates hundreds of
/// values, so paying for state, document, materialization and commit once avoids quadratic
/// reducer rewrites.
#[derive(Clone, Debug, PartialEq)]
pub struct ConfigRunPlan {
    pub target: DocumentTarget,
    pub events: Vec<AdminDocumentEvent>,
}

impl RowPlan for ConfigRunPlan {
    type Output = ();
    type Error = ApplyError;

    fn plan(&self, rows: &mut PlanRows) -> Step<Planned<()>> {
        let DocumentTarget::RealmConfig { realm_id } = self.target else {
            return Err(wrong(
                "realm config admin operation sync only supports realm config targets",
            )
            .into());
        };
        for event in &self.events {
            check_run_event(event, realm_id)?;
        }
        let Some(actor) = self.events.last().map(|event| event.actor.clone()) else {
            return Ok(Planned::output(()));
        };
        let state_target = AdminDocumentTarget::RealmConfig { realm_id };
        let previous_state = stored_state(rows, &state_target)?;
        let previous_config = stored_config(rows, &self.target)?;
        let now_ms = rows.now_ms();
        let raw_now = now_ms / 1000;
        let effective_now = previous_state
            .as_ref()
            .map_or(raw_now, |state| state.revocation_floor.max(raw_now));
        let mut reducer_state = previous_state
            .clone()
            .unwrap_or_else(|| AdminDocumentState::new(state_target));
        for event in &self.events {
            reducer_state.apply(event).map_err(invalid)?;
        }
        reducer_state.advance_revocation_floor(effective_now);
        let needs_index = needs_revocation_index(
            false,
            previous_config.is_some(),
            &reducer_state,
            effective_now,
        );
        let mut index = needs_index.then(|| reducer_state.revocation_index(effective_now));
        if let Some(index) = index.as_mut() {
            index.compact(&mut reducer_state);
        }
        let (config, changed) = plan_realm_change(
            previous_config,
            realm_id,
            &reducer_state,
            effective_now,
            now_ms,
            index.as_ref(),
        )?;
        if previous_state.as_ref() == Some(&reducer_state) && !changed {
            return Ok(Planned::output(()));
        }
        let mut writes = Vec::new();
        if changed && let Some(config) = config {
            writes.push(target_row(
                &self.target,
                config.to_bytes(&actor).map_err(invalid)?,
            ));
        }
        writes.push(reducer_state_entry(&reducer_state).map_err(invalid)?);
        if previous_state
            .as_ref()
            .is_none_or(|previous| previous.conflicts != reducer_state.conflicts)
        {
            writes.extend(conflict_write_entries(&reducer_state).map_err(invalid)?);
        }
        let deletes = stale_conflict_deletes(previous_state.as_ref(), Some(&reducer_state));
        Ok(Planned::changes((), deletes, writes))
    }

    fn conflict_retries() -> u32 {
        CONFLICT_RETRIES
    }
}

fn check_run_event(event: &AdminDocumentEvent, realm_id: RealmId) -> Result<(), ApplyError> {
    let AdminDocumentTarget::RealmConfig {
        realm_id: event_realm_id,
    } = event.target
    else {
        return Err(wrong(
            "admin document operation payload target is not a realm config",
        ));
    };
    if event_realm_id != realm_id {
        return Err(wrong(format!(
            "replicated realm config admin operation target {realm_id} does not match payload realm id {event_realm_id}"
        )));
    }
    if !coalescible_config_op(&event.op) {
        return Err(wrong(
            "realm config event run only supports transition updates",
        ));
    }
    if event.origin_node_id != event.actor.node_id
        || event.actor.realm_id != realm_id
        || event.actor.user_id.realm_id != realm_id
    {
        return Err(wrong(
            "realm config event actor and origin do not match the target realm",
        ));
    }
    Ok(())
}

fn target_row(target: &DocumentTarget, bytes: Vec<u8>) -> Row {
    (
        target.storage_keyspace().to_string(),
        target.storage_key(),
        bytes.into(),
    )
}

fn stored_state(
    rows: &mut PlanRows,
    target: &AdminDocumentTarget,
) -> Step<Option<AdminDocumentState>> {
    let value = rows.read(DOCUMENT_STATE_KEYSPACE, reducer_state_key(target))?;
    Ok(value
        .map(|bytes| decode_reducer_state(&bytes))
        .transpose()
        .map_err(invalid)?)
}

fn stored_config(
    rows: &mut PlanRows,
    target: &DocumentTarget,
) -> Step<Option<RealmConfigDocument>> {
    let value = rows.read(target.storage_keyspace(), target.storage_key())?;
    Ok(value
        .map(|bytes| RealmConfigDocument::from_bytes(&bytes))
        .transpose()
        .map_err(invalid)?)
}

/// Epoch due rows for this node's encrypted buckets of a group, or of all groups without one,
/// when `event` or the `narrowed` materialization can narrow a READ scope.
fn due_writes(
    rows: &mut PlanRows,
    event: &AdminDocumentEvent,
    narrowed: bool,
    group_id: Option<GroupId>,
) -> Step<Vec<Row>> {
    if !narrowed && !event.op.narrows_reads() {
        return Ok(Vec::new());
    }
    let prefix = group_id.map(|group| Key::from(group.to_bytes().to_vec()));
    let indexed = rows.scan(GROUP_ENCRYPTED_KEYSPACE, prefix)?;
    let buckets = indexed_buckets(&indexed);
    if buckets.is_empty() {
        return Ok(Vec::new());
    }
    for bucket in &buckets {
        rows.want(BUCKET_ENCRYPTION_KEYSPACE, bucket.as_bytes().to_vec());
    }
    let mut settings = Vec::with_capacity(buckets.len());
    for bucket in &buckets {
        let key = Key::from(bucket.as_bytes().to_vec());
        let value = rows.read(BUCKET_ENCRYPTION_KEYSPACE, key.clone())?;
        settings.push((key, value));
    }
    Ok(due_rows(&buckets, settings, rows.now_ms()).0)
}

fn apply_user(
    rows: &mut PlanRows,
    target: &DocumentTarget,
    event: &AdminDocumentEvent,
) -> Step<Planned<AdminFollowUp>> {
    let DocumentTarget::User { user_id } = *target else {
        return Err(wrong("admin document operation sync only supports user targets").into());
    };
    let AdminDocumentTarget::User {
        user_id: event_user_id,
    } = event.target.clone()
    else {
        return Err(wrong("admin document operation payload target is not a user").into());
    };
    if event_user_id != user_id {
        return Err(wrong(format!(
            "replicated user admin operation target {user_id} does not match payload user id {event_user_id}"
        ))
        .into());
    }
    if !matches!(
        &event.op,
        AdminDocumentOperation::UserNameSet { .. }
            | AdminDocumentOperation::SubjectIdAdded { .. }
            | AdminDocumentOperation::SubjectIdRemoved { .. }
            | AdminDocumentOperation::UserAttributeSet { .. }
            | AdminDocumentOperation::UserAttributeRemoved { .. }
            | AdminDocumentOperation::UserAliasAdded { .. }
            | AdminDocumentOperation::UserAliasRemoved { .. }
    ) {
        return Err(wrong(
            "admin document operation sync only supports user name, subject, and attribute updates",
        )
        .into());
    }
    let changed_subject = match &event.op {
        AdminDocumentOperation::SubjectIdAdded { subject_id }
        | AdminDocumentOperation::SubjectIdRemoved { subject_id } => Some(subject_id.as_str()),
        _ => None,
    };
    let changed_alias = match &event.op {
        AdminDocumentOperation::UserAliasAdded { alias }
        | AdminDocumentOperation::UserAliasRemoved { alias } => Some(*alias),
        _ => None,
    };
    let (deletes, writes) =
        user_batch(rows, target, event, changed_subject, changed_alias)?.unwrap_or_default();
    Ok(Planned::changes(AdminFollowUp::default(), deletes, writes))
}

/// The deletes and writes of a user event: its reduction and materialization and the claims.
/// `None` for a duplicate event.
pub fn user_batch(
    rows: &mut PlanRows,
    target: &DocumentTarget,
    event: &AdminDocumentEvent,
    changed_subject: Option<&str>,
    changed_alias: Option<UserId>,
) -> Step<Option<(Deletes, Vec<Row>)>> {
    let DocumentTarget::User { user_id } = *target else {
        return Err(wrong("user batch needs a user target").into());
    };
    let previous_state = stored_state(rows, &event.target)?;
    let mut reducer_state = previous_state
        .clone()
        .unwrap_or_else(|| AdminDocumentState::new(event.target.clone()));
    match reducer_state.apply(event).map_err(invalid)? {
        AdminApplyStatus::Applied => {}
        AdminApplyStatus::Duplicate => return Ok(None),
        AdminApplyStatus::Redundant | AdminApplyStatus::StaleOriginSequence => {
            let entry = reducer_state_entry(&reducer_state).map_err(invalid)?;
            return Ok(Some((Vec::new(), vec![entry])));
        }
    }
    let previous_user = rows
        .read(target.storage_keyspace(), target.storage_key())?
        .map(|bytes| User::from_bytes(&bytes))
        .transpose()
        .map_err(invalid)?;
    let user = materialize_user_operation(user_id, previous_user.as_ref(), &reducer_state, event);
    // Turning inactive ends READ; a replay or repeat leaves the status unchanged.
    let deactivating =
        !previous_user.as_ref().is_some_and(User::is_deactivated) && user.is_deactivated();
    let mut writes = vec![
        target_row(target, user.to_bytes(&event.actor).map_err(invalid)?),
        reducer_state_entry(&reducer_state).map_err(invalid)?,
    ];
    writes.extend(conflict_write_entries(&reducer_state).map_err(invalid)?);
    let mut deletes = stale_conflict_deletes(previous_state.as_ref(), Some(&reducer_state));
    let subject_ids = match (changed_subject, changed_alias) {
        (Some(subject_id), _) => vec![subject_id.to_string()],
        (None, Some(_)) => Vec::new(),
        (None, None) => user.subject_ids.clone(),
    };
    // Markers come from this attempt so a bucket encrypted meanwhile is included.
    if deactivating {
        writes.extend(due_writes(rows, event, true, None)?);
    }
    for subject_id in &subject_ids {
        let subject_key = subject_index_key(subject_id);
        let claims = rows.read(SUBJECT_CLAIMS_KEYSPACE, subject_key.clone())?;
        let mut claims = match claims {
            Some(bytes) => postcard::from_bytes::<BTreeSet<UserId>>(&bytes).map_err(invalid)?,
            None => {
                let mut claims = BTreeSet::new();
                if let Some(bytes) = rows.read(SUBJECT_INDEX_KEYSPACE, subject_key.clone())? {
                    claims.insert(UserId::from_storage_key(&bytes).map_err(invalid)?);
                }
                claims
            }
        };
        if user.subject_ids.contains(subject_id) {
            claims.insert(user_id);
            // Claims an earlier hand-over left with accounts merged into this one end here.
            for other in claims.clone() {
                if other != user_id
                    && let Some(record) = read_user(rows, other)?
                    && merge_root(rows, &record, &user)? == Some(user_id)
                {
                    claims.remove(&other);
                }
            }
        } else {
            // A merged account hands the subject to the current root of its merges, so the
            // subject never lacks an owner while the root's own addition is on its way. Until
            // that root is known and lists the account, the account's claim stays.
            match merge_root(rows, &user, &user)? {
                Some(primary) => {
                    claims.remove(&user_id);
                    claims.insert(primary);
                }
                None if user.merged_into().is_some() => {}
                None => {
                    claims.remove(&user_id);
                }
            }
        }
        if let Some(canonical) = claims.first().copied() {
            writes.push((
                SUBJECT_CLAIMS_KEYSPACE.to_string(),
                subject_key.clone(),
                postcard::to_allocvec(&claims).map_err(invalid)?.into(),
            ));
            writes.push((
                SUBJECT_INDEX_KEYSPACE.to_string(),
                subject_key,
                subject_index_value(canonical),
            ));
        } else {
            deletes.push((SUBJECT_CLAIMS_KEYSPACE.to_string(), subject_key.clone()));
            deletes.push((SUBJECT_INDEX_KEYSPACE.to_string(), subject_key));
        }
    }
    // Claims of the linked login; more than one claim leaves it unusable until resolved.
    if let Some(alias) = changed_alias {
        let key = ByteView::from(crate::link::alias_claims_key(&alias));
        let mut claims = match rows.read(FEDERATION_KEYSPACE, key.clone())? {
            Some(bytes) => postcard::from_bytes::<BTreeSet<UserId>>(&bytes).map_err(invalid)?,
            None => BTreeSet::new(),
        };
        if user.alias_user_ids.contains(&alias) {
            claims.insert(user_id);
        } else {
            claims.remove(&user_id);
        }
        if claims.is_empty() {
            deletes.push((FEDERATION_KEYSPACE.to_string(), key));
        } else {
            let value = postcard::to_allocvec(&claims).map_err(invalid)?;
            writes.push((FEDERATION_KEYSPACE.to_string(), key, value.into()));
        }
    }
    Ok(Some((deletes, writes)))
}

fn group_entries(
    rows: &mut PlanRows,
    group_id: Ulid,
    reducer_state: &AdminDocumentState,
) -> Step<Vec<Row>> {
    let target = DocumentTarget::Group { group_id };
    let group = match rows.read(GROUP_KEYSPACE, target.storage_key())? {
        Some(bytes) => {
            let mut group = Group::from_bytes(&bytes).map_err(invalid)?;
            if group.group_id != group_id {
                return Err(wrong(format!(
                    "stored group document id {group_id} does not match payload group id {}",
                    group.group_id
                ))
                .into());
            }
            overlay_group_state(&mut group, reducer_state);
            group
        }
        None => {
            let Some(group) = materialized_group(group_id, reducer_state) else {
                return Ok(Vec::new());
            };
            group
        }
    };
    Ok(vec![
        target_row(&target, postcard::to_allocvec(&group).map_err(invalid)?),
        (
            OWNER_INDEX_KEYSPACE.to_string(),
            owner_group_key(group.owner, group.group_id).into(),
            ByteView::from(Vec::new()),
        ),
    ])
}

fn apply_group(
    rows: &mut PlanRows,
    target: &DocumentTarget,
    event: &AdminDocumentEvent,
) -> Step<Planned<AdminFollowUp>> {
    let DocumentTarget::GroupAuthorization { group_id } = *target else {
        return Err(
            wrong("group admin operation sync only supports group authorization targets").into(),
        );
    };
    let AdminDocumentTarget::Group {
        group_id: event_group_id,
    } = event.target.clone()
    else {
        return Err(wrong("admin document operation payload target is not a group").into());
    };
    if event_group_id != group_id {
        return Err(wrong(format!(
            "replicated group admin operation target {group_id} does not match payload group id {event_group_id}"
        ))
        .into());
    }
    if !matches!(
        &event.op,
        AdminDocumentOperation::GroupCreated { .. }
            | AdminDocumentOperation::GroupDeleted { .. }
            | AdminDocumentOperation::GroupRoleAdded { .. }
            | AdminDocumentOperation::GroupRoleCreated { .. }
            | AdminDocumentOperation::GroupRoleRemoved { .. }
            | AdminDocumentOperation::GroupAssignmentAdded { .. }
            | AdminDocumentOperation::GroupAssignmentRemoved { .. }
            | AdminDocumentOperation::GroupPoliciesSet { .. }
            | AdminDocumentOperation::GroupJoinRequested { .. }
            | AdminDocumentOperation::GroupJoinDecided { .. }
            | AdminDocumentOperation::DisplayNameSet { .. }
            | AdminDocumentOperation::GroupLocationSet { .. }
    ) {
        return Err(wrong(
            "group admin operation sync only supports group creation, renames, role seeds, role creation/removal, role user assignment updates, and policy updates",
        )
        .into());
    }
    let previous_state = stored_state(rows, &event.target)?;
    let mut reducer_state = previous_state
        .clone()
        .unwrap_or_else(|| AdminDocumentState::new(event.target.clone()));
    let status = reducer_state.apply(event).map_err(invalid)?;
    if reducer_state.group_deleted() {
        return group_deletion(
            rows,
            group_id,
            event,
            previous_state.as_ref(),
            &reducer_state,
        );
    }
    // A duplicate leaves the reducer state as stored; rewriting it would only conflict a local
    // group change that read the row.
    if status == AdminApplyStatus::Duplicate {
        return Ok(Planned::output(AdminFollowUp::default()));
    }
    if status != AdminApplyStatus::Applied {
        let writes = vec![reducer_state_entry(&reducer_state).map_err(invalid)?];
        return Ok(Planned::changes(
            AdminFollowUp::default(),
            Vec::new(),
            writes,
        ));
    }
    let previous_doc = rows
        .read(target.storage_keyspace(), target.storage_key())?
        .map(|bytes| GroupAuthorizationDocument::from_bytes(&bytes))
        .transpose()
        .map_err(invalid)?;
    let mut auth_doc = previous_doc.unwrap_or_else(|| GroupAuthorizationDocument {
        group_id,
        roles: Default::default(),
        policies: Default::default(),
    });
    let before = auth_doc.roles.clone();
    materialize_group_authorization(&mut auth_doc, &reducer_state, event);
    let narrowed = roles_narrowed(&before, &auth_doc.roles);
    let group_writes = group_entries(rows, group_id, &reducer_state)?;
    let mut writes = vec![
        target_row(target, auth_doc.to_bytes(&event.actor).map_err(invalid)?),
        reducer_state_entry(&reducer_state).map_err(invalid)?,
    ];
    writes.extend(group_writes);
    writes.extend(due_writes(rows, event, narrowed, Some(group_id))?);
    writes.extend(conflict_write_entries(&reducer_state).map_err(invalid)?);
    let deletes = stale_conflict_deletes(previous_state.as_ref(), Some(&reducer_state));
    Ok(Planned::changes(AdminFollowUp::default(), deletes, writes))
}

/// The tombstone of a deleted group, written durably; a cancelled attempt never lands.
fn group_deletion(
    rows: &mut PlanRows,
    group_id: Ulid,
    event: &AdminDocumentEvent,
    previous_state: Option<&AdminDocumentState>,
    reducer_state: &AdminDocumentState,
) -> Step<Planned<AdminFollowUp>> {
    let mut record = reducer_state
        .group_deletion()
        .ok_or_else(|| wrong("group tombstone is invalid"))?;
    if rows
        .read(
            GROUP_DELETE_KEYSPACE,
            ByteView::from(record.plan.cancelled_key()),
        )?
        .is_some()
    {
        return Err(wrong("group deletion attempt was cancelled").into());
    }
    let previous = rows.read(GROUP_DELETE_KEYSPACE, group_id.to_bytes().to_vec())?;
    record.event = previous
        .as_deref()
        .map(GroupDeleteRecord::from_bytes)
        .transpose()
        .map_err(invalid)?
        .filter(|previous| previous.plan == record.plan)
        .and_then(|previous| previous.event);
    if matches!(&event.op, AdminDocumentOperation::GroupDeleted { certificate } if certificate.plan == record.plan)
    {
        record.event = Some(Box::new(event.clone()));
    }
    let (mut deletes, mut writes) =
        group_deletion_entries(&record, reducer_state).map_err(invalid)?;
    let fence = rows.read(
        GROUP_DELETE_KEYSPACE,
        MembershipFence::key(record.plan.realm_id),
    )?;
    let mut membership = MembershipFence::from_value(fence.as_deref()).map_err(invalid)?;
    if membership.pending.remove(&group_id) {
        writes.push(membership.entry(record.plan.realm_id).map_err(invalid)?);
    }
    deletes.extend(stale_conflict_deletes(previous_state, Some(reducer_state)));
    writes.extend(conflict_write_entries(reducer_state).map_err(invalid)?);
    let follow_up = AdminFollowUp {
        sync: true,
        retry_parked: None,
    };
    Ok(Planned::changes(follow_up, deletes, writes))
}

fn apply_realm_authorization(
    rows: &mut PlanRows,
    target: &DocumentTarget,
    event: &AdminDocumentEvent,
) -> Step<Planned<AdminFollowUp>> {
    let DocumentTarget::RealmAuthorization { realm_id } = *target else {
        return Err(
            wrong("realm admin operation sync only supports realm authorization targets").into(),
        );
    };
    let AdminDocumentTarget::Realm {
        realm_id: event_realm_id,
    } = event.target.clone()
    else {
        return Err(wrong("admin document operation payload target is not a realm").into());
    };
    if event_realm_id != realm_id {
        return Err(wrong(format!(
            "replicated realm admin operation target {realm_id} does not match payload realm id {event_realm_id}"
        ))
        .into());
    }
    if !matches!(
        &event.op,
        AdminDocumentOperation::RealmRoleAdded { .. }
            | AdminDocumentOperation::RealmRoleCreated { .. }
            | AdminDocumentOperation::RealmAssignmentAdded { .. }
            | AdminDocumentOperation::RealmAssignmentRemoved { .. }
    ) {
        return Err(wrong(
            "realm admin operation sync only supports role seeds, role creation, and role user assignment updates",
        )
        .into());
    }
    let previous_state = stored_state(rows, &event.target)?;
    let mut reducer_state = previous_state
        .clone()
        .unwrap_or_else(|| AdminDocumentState::new(event.target.clone()));
    match reducer_state.apply(event).map_err(invalid)? {
        AdminApplyStatus::Applied => {}
        AdminApplyStatus::Duplicate => return Ok(Planned::output(AdminFollowUp::default())),
        AdminApplyStatus::Redundant | AdminApplyStatus::StaleOriginSequence => {
            let writes = vec![reducer_state_entry(&reducer_state).map_err(invalid)?];
            return Ok(Planned::changes(
                AdminFollowUp::default(),
                Vec::new(),
                writes,
            ));
        }
    }
    let mut writes = vec![reducer_state_entry(&reducer_state).map_err(invalid)?];
    writes.extend(conflict_write_entries(&reducer_state).map_err(invalid)?);
    let deletes = stale_conflict_deletes(previous_state.as_ref(), Some(&reducer_state));
    let mut auth_doc = rows
        .read(target.storage_keyspace(), target.storage_key())?
        .map(|bytes| RealmAuthorizationDocument::from_bytes(&bytes))
        .transpose()
        .map_err(invalid)?
        .unwrap_or_else(|| RealmAuthorizationDocument {
            realm_id,
            roles: Default::default(),
            operation_restrictions: Default::default(),
        });
    let before = auth_doc.roles.clone();
    materialize_realm_authorization(&mut auth_doc, &reducer_state, event);
    let narrowed = roles_narrowed(&before, &auth_doc.roles);
    writes.push(target_row(
        target,
        auth_doc.to_bytes(&event.actor).map_err(invalid)?,
    ));
    // The roles and markers come from this attempt, so a bucket encrypted meanwhile is included.
    writes.extend(due_writes(rows, event, narrowed, None)?);
    Ok(Planned::changes(AdminFollowUp::default(), deletes, writes))
}

/// The document a reduced realm config implies: the stored document is overlaid, or a new one
/// materialized, and the bool reports an observable change.
fn plan_realm_change(
    previous_config: Option<RealmConfigDocument>,
    realm_id: RealmId,
    reducer_state: &AdminDocumentState,
    effective_now: u64,
    now_ms: u64,
    revocation_index: Option<&RevocationIndex>,
) -> Result<(Option<RealmConfigDocument>, bool), ApplyError> {
    match previous_config {
        Some(mut config) => {
            if config.realm_id != realm_id {
                return Err(wrong(format!(
                    "stored realm config document id {realm_id} does not match payload realm id {}",
                    config.realm_id
                )));
            }
            let before = config.clone();
            overlay_realm_config(
                &mut config,
                reducer_state,
                effective_now,
                now_ms,
                revocation_index,
            );
            let changed = config != before;
            Ok((Some(config), changed))
        }
        None => {
            let config = materialized_realm_config(
                realm_id,
                reducer_state,
                effective_now,
                now_ms,
                revocation_index,
            );
            let changed = config.is_some();
            Ok((config, changed))
        }
    }
}

/// The end of `user`'s merge pointers once that account exists in the realm and lists `user` as
/// merged; `None` until then, when unmerged, or when pointers loop. `applied`, the account the
/// event being applied changes, is read as just materialized, since its row is not written yet.
fn merge_root(rows: &mut PlanRows, user: &User, applied: &User) -> Step<Option<UserId>> {
    let mut seen = BTreeSet::from([user.user_id]);
    let mut current = user.clone();
    while let Some(next) = current.merged_into() {
        if next.realm_id != user.user_id.realm_id || !seen.insert(next) {
            return Ok(None);
        }
        if next == applied.user_id {
            current = applied.clone();
            continue;
        }
        match read_user(rows, next)? {
            Some(record) => current = record,
            None => return Ok(None),
        }
    }
    let root = current.user_id;
    Ok((root != user.user_id && current.merged_user_ids().contains(&user.user_id)).then_some(root))
}

fn read_user(rows: &mut PlanRows, user_id: UserId) -> Step<Option<User>> {
    let user = rows
        .read(USER_KEYSPACE, ByteView::from(user_id.to_bytes()))?
        .map(|bytes| User::from_bytes(&bytes))
        .transpose()
        .map_err(invalid)?;
    Ok(user.filter(|user| user.user_id == user_id))
}

fn apply_realm_config(
    rows: &mut PlanRows,
    target: &DocumentTarget,
    event: &AdminDocumentEvent,
) -> Step<Planned<AdminFollowUp>> {
    let realm_id = validate_config_target(target, event).map_err(ApplyError::Invalid)?;
    if !config_mutation_allowed(&event.op) {
        return Err(wrong(
            "realm config admin operation sync only supports node membership updates, OIDC provider updates, settings updates, description updates, quota updates, placement updates, transition updates, policy updates, compute updates, federation updates, portal updates, and token revocations",
        )
        .into());
    }
    validate_config_actor(realm_id, event).map_err(ApplyError::Invalid)?;
    let follow_up = || AdminFollowUp {
        sync: false,
        retry_parked: Some(realm_id),
    };
    let is_revocation = matches!(&event.op, AdminDocumentOperation::ConfigTokenRevoked { .. });
    let now_ms = rows.now_ms();
    let raw_now = now_ms / 1000;
    let previous_state = stored_state(rows, &event.target)?;
    let previous_config = stored_config(rows, target)?;
    if is_revocation {
        validate_revocation_snapshot(
            previous_config.as_ref(),
            previous_state.as_ref(),
            event,
            realm_id,
            raw_now,
        )
        .map_err(ApplyError::Invalid)?;
    }
    let effective_now = previous_state
        .as_ref()
        .map_or(raw_now, |state| state.revocation_floor.max(raw_now));
    let mut reducer_state = previous_state
        .clone()
        .unwrap_or_else(|| AdminDocumentState::new(event.target.clone()));
    let needs_index = needs_revocation_index(
        is_revocation,
        previous_config.is_some(),
        &reducer_state,
        effective_now,
    );
    let mut index = needs_index.then(|| reducer_state.revocation_index(effective_now));
    let mut applied = false;
    if is_revocation {
        let index = index
            .as_mut()
            .ok_or_else(|| wrong("revocation index was not admitted"))?;
        reducer_state
            .apply_revocation_event(event, index)
            .map_err(invalid)?;
    } else {
        applied = reducer_state.apply(event).map_err(invalid)? == AdminApplyStatus::Applied;
    }
    reducer_state.advance_revocation_floor(effective_now);
    if let Some(index) = index.as_mut() {
        index.compact(&mut reducer_state);
    }
    let cutoff_owner = match &event.op {
        AdminDocumentOperation::ConfigTokenRevoked { token_owner, .. } => Some(*token_owner),
        _ => None,
    };
    let cutoff_before = cutoff_owner.and_then(|owner| {
        previous_config
            .as_ref()
            .and_then(|config| config.user_cutoff(&owner, effective_now))
    });
    let (config, changed) = plan_realm_change(
        previous_config,
        realm_id,
        &reducer_state,
        effective_now,
        now_ms,
        index.as_ref(),
    )?;
    if previous_state.as_ref() == Some(&reducer_state) && !changed {
        return Ok(Planned::output(follow_up()));
    }
    // A new or later user cutoff ends READ for a deactivated account; a replay leaves it equal.
    let cutoff_raised = cutoff_owner.is_some_and(|owner| {
        config
            .as_ref()
            .and_then(|config| config.user_cutoff(&owner, effective_now))
            > cutoff_before
    });
    let mut writes = Vec::new();
    if changed && let Some(config) = config {
        writes.push(target_row(
            target,
            config.to_bytes(&event.actor).map_err(invalid)?,
        ));
    }
    writes.push(reducer_state_entry(&reducer_state).map_err(invalid)?);
    // A replayed policy event is a duplicate and writes no new due marker.
    if applied || cutoff_raised {
        writes.extend(due_writes(rows, event, cutoff_raised, None)?);
    }
    if previous_state
        .as_ref()
        .is_none_or(|previous| previous.conflicts != reducer_state.conflicts)
    {
        writes.extend(conflict_write_entries(&reducer_state).map_err(invalid)?);
    }
    let deletes = stale_conflict_deletes(previous_state.as_ref(), Some(&reducer_state));
    Ok(Planned::changes(follow_up(), deletes, writes))
}
