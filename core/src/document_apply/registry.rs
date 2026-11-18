//! Stores metadata registry records, fencing out stale rows and deleted documents.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use byteview::ByteView;
use ulid::Ulid;

use super::reads::realm_config;
use super::{ApplyError, PlacementOutcome, Step, invalid};
use crate::MetaResourceId;
use crate::deferred_topics::DocumentSyncDependency;
use crate::document::{DocumentChange, DocumentTarget};
use crate::keyspaces::{
    CREATE_ACCEPTANCE_KEYSPACE, DOCUMENT_LIFECYCLE_KEYSPACE, EVENT_LOG_KEYSPACE,
    GRAPH_LIFECYCLE_KEYSPACE, GROUP_DELETE_KEYSPACE, ID_MAPPING_KEYSPACE, SYNC_REVISION_KEYSPACE,
};
use crate::metadata::{
    GraphLifecycleRecord, GraphPruneRecord, MetadataDeleteRecord, MetadataEventRecord,
    MetadataLifecycleRecord,
};
use crate::row_plan::{PlanRows, Planned, RowPlan};
use crate::storage_entries::{
    commit_message_entry, create_acceptance_entry, create_acceptance_key,
    create_projection_entries, document_lifecycle_entry, document_lifecycle_key, event_log_key,
    graph_lifecycle_entry, graph_lifecycle_key, graph_prune_entry, registry_delete_entries,
    registry_write_entries, shard_manifest_entry, sync_revision_entry, sync_revision_key,
};
use crate::structs::identity::group_delete::{GroupWriteError, check_group_write};
use crate::structs::identity::realm::RealmId;
use crate::structs::placement::binding_directory::BindingError;
use crate::structs::placement::policy::document::{
    PlacementPolicyDocument, placement_policy_change, placement_policy_target,
};
use crate::structs::placement::record::{DocumentClass, PlacementRef, PlacementScope};
use crate::structs::storage::metadata_registry::MetadataRegistryRecord;
use crate::structs::{
    PersistentIdMapping, persistent_id_change, persistent_id_key, persistent_id_target,
    secondary_index_deletes, secondary_index_entries,
};
use crate::types::{Key, KeySpace, Value};

/// A row to write: key space, key and value.
pub type Row = (KeySpace, Key, Value);

/// Refuses a change to a group whose deletion is pending or done.
pub fn group_fence(rows: &mut PlanRows, group_id: Ulid) -> Step<()> {
    let value = rows.read(GROUP_DELETE_KEYSPACE, group_id.to_bytes().to_vec())?;
    match check_group_write(value.as_deref()) {
        Ok(()) => Ok(()),
        Err(GroupWriteError::Frozen) => {
            Err(ApplyError::Deferred("group deletion is pending".to_string()).into())
        }
        Err(GroupWriteError::Deleted) => {
            Err(ApplyError::Invalid("group has been deleted".to_string()).into())
        }
        Err(error) => Err(ApplyError::Storage(crate::errors::StorageError::ReadError(
            error.to_string(),
        ))
        .into()),
    }
}

/// Stores one replicated registry record, or repairs its sidecars when the stored row wins.
#[derive(Debug, PartialEq)]
pub struct RegistryUpsertPlan {
    pub record: MetadataRegistryRecord,
    pub primary_bytes: Vec<u8>,
}

impl RowPlan for RegistryUpsertPlan {
    type Output = PlacementOutcome<()>;
    type Error = ApplyError;

    fn plan(&self, rows: &mut PlanRows) -> Step<Planned<PlacementOutcome<()>>> {
        let record = &self.record;
        if !record.identity_valid() {
            return Ok(Planned::output(PlacementOutcome::Rejected));
        }
        let mut entries = registry_write_entries(record).map_err(invalid)?;
        if let Some((_, _, value)) = entries.first_mut() {
            *value = self.primary_bytes.clone().into();
        }
        let target = DocumentTarget::MetadataRegistry {
            group_id: record.group_id,
            document_id: record.document_id,
        };
        let accepted = PlacementOutcome::Accepted(());
        let delete_present = delete_record(rows, record.document_id)?.is_some();
        if record_fenced(rows, record)? {
            // The stale local row carries the timestamp half of its index key; the fenced
            // incoming record may be stamped differently.
            let stale = rows
                .read(target.storage_keyspace(), target.storage_key())?
                .and_then(|value| postcard::from_bytes::<MetadataRegistryRecord>(&value).ok());
            let deletes = registry_delete_entries(stale.as_ref().unwrap_or(record));
            return Ok(Planned::changes(accepted, deletes, Vec::new()));
        }
        match placement_fence(rows, record)? {
            PlacementOutcome::Accepted(()) => {}
            PlacementOutcome::Deferred(dependency) => {
                return Ok(Planned::output(PlacementOutcome::Deferred(dependency)));
            }
            PlacementOutcome::Rejected => return Ok(Planned::output(PlacementOutcome::Rejected)),
        }
        let existing = rows
            .read(target.storage_keyspace(), target.storage_key())?
            .map(|bytes| postcard::from_bytes::<MetadataRegistryRecord>(&bytes))
            .transpose()
            .map_err(invalid)?;
        if existing
            .as_ref()
            .is_some_and(|existing| !existing.same_identity(record))
        {
            return Ok(Planned::output(PlacementOutcome::Rejected));
        }
        if let Some(existing) = existing
            .as_ref()
            .filter(|existing| delete_present || record.freshness() <= existing.freshness())
        {
            let repairs = sidecar_repairs(rows, existing)?;
            return Ok(Planned::changes(accepted, Vec::new(), repairs));
        }
        Ok(Planned::changes(accepted, Vec::new(), entries))
    }
}

/// Stores a graph tombstone that a stored document delete covers; the output tells if it landed.
#[derive(Debug, PartialEq)]
pub struct GraphLifecyclePlan {
    pub record: GraphLifecycleRecord,
    pub primary_bytes: Vec<u8>,
}

impl RowPlan for GraphLifecyclePlan {
    type Output = bool;
    type Error = ApplyError;

    fn plan(&self, rows: &mut PlanRows) -> Step<Planned<bool>> {
        let record = &self.record;
        if !record.is_deleted() {
            return Ok(Planned::output(false));
        }
        let (space, key, _) = graph_lifecycle_entry(record).map_err(invalid)?;
        let Some(delete) = delete_record(rows, record.document_id)? else {
            return Ok(Planned::output(false));
        };
        let (live, registry) = registry_live(rows, record.group_id, record.document_id, &delete)?;
        if !delete.covers_graph(record) || live {
            return Ok(Planned::output(false));
        }
        let deletes = registry
            .as_ref()
            .map(registry_delete_entries)
            .unwrap_or_default();
        let writes = vec![(space, key, self.primary_bytes.clone().into())];
        Ok(Planned::changes(true, deletes, writes))
    }
}

/// Stores one replicated document lifecycle record; the output tells if anything changed.
#[derive(Debug, PartialEq)]
pub struct DocumentLifecyclePlan {
    pub record: MetadataLifecycleRecord,
    pub change: DocumentChange,
}

impl RowPlan for DocumentLifecyclePlan {
    type Output = bool;
    type Error = ApplyError;

    fn plan(&self, rows: &mut PlanRows) -> Step<Planned<bool>> {
        let record = &self.record;
        let writes = lifecycle_entries(rows, record, &self.change)?;
        let cleanup = if let MetadataLifecycleRecord::Delete { event } = record
            && event.tombstone.is_deleted()
        {
            let current = delete_record(rows, record.document_id())?;
            if writes.is_some() {
                Some(event.clone())
            } else {
                current
            }
        } else {
            None
        };
        let deletes = match cleanup.as_ref() {
            Some(delete) => registry_cleanup(
                rows,
                delete.tombstone.group_id,
                delete.tombstone.document_id,
                delete,
            )?,
            None => Vec::new(),
        };
        if writes.is_none() && deletes.is_empty() {
            return Ok(Planned::output(false));
        }
        Ok(Planned::changes(true, deletes, writes.unwrap_or_default()))
    }
}

/// Folds a replicated PID mapping, its sidecar and manifest entry into one monotonic change that
/// never lets an Active row overwrite a Withdrawn tombstone and fences the stamped placement.
#[derive(Debug, PartialEq)]
pub struct PidMappingPlan {
    pub realm_id: RealmId,
    pub incoming: PersistentIdMapping,
    pub placement: PlacementRef,
}

impl RowPlan for PidMappingPlan {
    type Output = PlacementOutcome<bool>;
    type Error = ApplyError;

    fn plan(&self, rows: &mut PlanRows) -> Step<Planned<PlacementOutcome<bool>>> {
        let incoming = &self.incoming;
        let placement = self.placement;
        match derive_placement(rows, self.realm_id, None, incoming.target, placement)? {
            PlacementOutcome::Accepted(_) => {}
            PlacementOutcome::Deferred(dependency) => {
                return Ok(Planned::output(PlacementOutcome::Deferred(dependency)));
            }
            PlacementOutcome::Rejected => return Ok(Planned::output(PlacementOutcome::Rejected)),
        }
        let Some(merged) = pid_merge(rows, incoming)? else {
            return Ok(Planned::output(PlacementOutcome::Accepted(false)));
        };
        let target = persistent_id_target(merged.target);
        let change = persistent_id_change(&merged, placement);
        let mut writes = vec![(
            ID_MAPPING_KEYSPACE.to_string(),
            ByteView::from(persistent_id_key(merged.target)),
            Value::from(merged.to_bytes().map_err(invalid)?),
        )];
        writes.extend(secondary_index_entries(&merged));
        writes.extend(revision_entries(&target, &change)?);
        let deletes = secondary_index_deletes(&merged);
        Ok(Planned::changes(
            PlacementOutcome::Accepted(true),
            deletes,
            writes,
        ))
    }
}

/// Stores one replicated policy document. The bucket is derived again from the policy id, and a
/// known id whose definition differs is rejected: one id resolves to exactly one rule.
#[derive(Debug, PartialEq)]
pub struct PolicyDocumentPlan {
    pub realm_id: RealmId,
    pub incoming: PlacementPolicyDocument,
    pub placement: PlacementRef,
}

impl RowPlan for PolicyDocumentPlan {
    type Output = PlacementOutcome<bool>;
    type Error = ApplyError;

    fn plan(&self, rows: &mut PlanRows) -> Step<Planned<PlacementOutcome<bool>>> {
        let incoming = &self.incoming;
        let placement = self.placement;
        match policy_bucket(rows, self.realm_id, incoming.policy_id(), placement)? {
            PlacementOutcome::Accepted(_) => {}
            PlacementOutcome::Deferred(dependency) => {
                return Ok(Planned::output(PlacementOutcome::Deferred(dependency)));
            }
            PlacementOutcome::Rejected => return Ok(Planned::output(PlacementOutcome::Rejected)),
        }
        let merged = match policy_merge(rows, incoming)? {
            Ok(Some(merged)) => merged,
            Ok(None) => return Ok(Planned::output(PlacementOutcome::Accepted(false))),
            Err(()) => return Ok(Planned::output(PlacementOutcome::Rejected)),
        };
        let target = placement_policy_target(merged.policy_id());
        let change = placement_policy_change(&merged, placement);
        let mut writes = vec![(
            target.storage_keyspace().to_string(),
            target.storage_key(),
            Value::from(merged.to_bytes().map_err(invalid)?),
        )];
        writes.extend(revision_entries(&target, &change)?);
        Ok(Planned::changes(
            PlacementOutcome::Accepted(true),
            Vec::new(),
            writes,
        ))
    }
}

/// Removes the registry row of a document whose stored delete covers it.
#[derive(Debug, PartialEq)]
pub struct DeleteRegistryPlan {
    pub group_id: Ulid,
    pub document_id: Ulid,
}

impl RowPlan for DeleteRegistryPlan {
    type Output = ();
    type Error = ApplyError;

    fn plan(&self, rows: &mut PlanRows) -> Step<Planned<()>> {
        let Some(delete) = delete_record(rows, self.document_id)? else {
            return Ok(Planned::output(()));
        };
        let deletes = registry_cleanup(rows, self.group_id, self.document_id, &delete)?;
        Ok(Planned::changes((), deletes, Vec::new()))
    }
}

/// The revision row of a change and the shard manifest row it implies.
pub fn revision_entries(target: &DocumentTarget, change: &DocumentChange) -> Step<Vec<Row>> {
    let mut entries = vec![sync_revision_entry(target, change).map_err(invalid)?];
    if let Some(entry) = shard_manifest_entry(target, change).map_err(invalid)? {
        entries.push(entry);
    }
    Ok(entries)
}

/// The bucket a policy id must ride, derived from the realm config and compared to its stamp.
pub fn policy_bucket(
    rows: &mut PlanRows,
    realm_id: RealmId,
    policy_id: Ulid,
    placement: PlacementRef,
) -> Step<PlacementOutcome<PlacementRef>> {
    if placement == PlacementRef::NIL || placement.strategy_id.is_nil() {
        return Ok(PlacementOutcome::Rejected);
    }
    let Some(config) = realm_config(rows, realm_id)? else {
        return Ok(PlacementOutcome::Deferred(
            DocumentSyncDependency::RealmConfig(realm_id),
        ));
    };
    if config.realm_id != realm_id {
        return Ok(PlacementOutcome::Rejected);
    }
    Ok(match config.policy_placement(policy_id) {
        Some(derived) if derived == placement => PlacementOutcome::Accepted(derived),
        Some(_) => PlacementOutcome::Rejected,
        None => PlacementOutcome::Deferred(DocumentSyncDependency::PlacementStrategy {
            realm_id,
            strategy_id: placement.strategy_id,
        }),
    })
}

/// `Ok(None)` when the local row absorbs the incoming one, `Err(())` when the id is reused with
/// a different definition.
fn policy_merge(
    rows: &mut PlanRows,
    incoming: &PlacementPolicyDocument,
) -> Step<Result<Option<PlacementPolicyDocument>, ()>> {
    if let Some(group_id) = incoming.policy.owner_group_id {
        group_fence(rows, group_id)?;
    }
    let target = placement_policy_target(incoming.policy_id());
    let Some(local) = rows.read(target.storage_keyspace(), target.storage_key())? else {
        return Ok(Ok(Some(incoming.clone())));
    };
    let mut local = PlacementPolicyDocument::from_bytes(&local).map_err(invalid)?;
    Ok(match local.merge(incoming) {
        Ok(true) => Ok(Some(local)),
        Ok(false) => Ok(None),
        Err(_) => Err(()),
    })
}

/// `None` when the local row absorbs the incoming one.
fn pid_merge(
    rows: &mut PlanRows,
    incoming: &PersistentIdMapping,
) -> Step<Option<PersistentIdMapping>> {
    let key = ByteView::from(persistent_id_key(incoming.target));
    let Some(local) = rows.read(ID_MAPPING_KEYSPACE, key)? else {
        return Ok(Some(incoming.clone()));
    };
    let mut local = PersistentIdMapping::from_bytes(&local).map_err(invalid)?;
    Ok(local.merge(incoming).then_some(local))
}

fn lifecycle_entries(
    rows: &mut PlanRows,
    record: &MetadataLifecycleRecord,
    change: &DocumentChange,
) -> Step<Option<Vec<Row>>> {
    let target = DocumentTarget::MetadataDocumentLifecycle {
        document_id: record.document_id(),
    };
    // One lifecycle target carries every event: an older concurrent upsert still belongs in
    // the event log, but never moves the lifecycle revision back.
    let stale = lifecycle_stale(rows, &target, change)?;
    if stale && record.upsert().is_none() {
        return Ok(None);
    }
    let mut acceptance = None;
    if let Some(event) = record.upsert() {
        validate_metadata_event(event)?;
        if create_fence(rows, event)? {
            return Ok(None);
        }
        let accepted = accepted_create(rows, event.record.document_id)?;
        if let Some(accepted) = accepted.as_ref() {
            validate_metadata_event(accepted)?;
        }
        if event.is_create() {
            if accepted
                .as_ref()
                .is_some_and(|accepted| !same_create_event(accepted, event))
            {
                return Ok(None);
            }
            if accepted.is_none() {
                acceptance = Some(event);
            }
        } else if accepted.as_ref().is_none_or(|accepted| {
            !accepted.is_create() || !accepted.record.same_identity(&event.record)
        }) {
            return Ok(None);
        }
    }
    if let Some(event) = record.upsert()
        && stale
        && event_logged(rows, event)?
    {
        return Ok(None);
    }
    let mut entries = match record {
        MetadataLifecycleRecord::Upsert { event } => {
            create_projection_entries(event).map_err(invalid)?
        }
        MetadataLifecycleRecord::UpsertWithMessage { event, message } => {
            let mut entries = create_projection_entries(event).map_err(invalid)?;
            entries.push(commit_message_entry(event, message).map_err(invalid)?);
            entries
        }
        MetadataLifecycleRecord::Delete { event } => document_delete_entries(event, rows.now_ms())?,
    };
    if let Some(event) = acceptance {
        entries.push(create_acceptance_entry(event).map_err(invalid)?);
    }
    if !stale {
        entries.extend(revision_entries(&target, change)?);
    }
    Ok(Some(entries))
}

/// The accepted create event of a document, if any.
pub fn accepted_create(
    rows: &mut PlanRows,
    document_id: Ulid,
) -> Step<Option<MetadataEventRecord>> {
    let value = rows.read(
        CREATE_ACCEPTANCE_KEYSPACE,
        create_acceptance_key(document_id),
    )?;
    Ok(value
        .map(|value| postcard::from_bytes::<MetadataEventRecord>(&value))
        .transpose()
        .map_err(invalid)?)
}

/// Whether the event log already holds this event.
pub fn event_logged(rows: &mut PlanRows, event: &MetadataEventRecord) -> Step<bool> {
    let key = event_log_key(event.record.document_id, event.event_id);
    Ok(rows.read(EVENT_LOG_KEYSPACE, key)?.is_some())
}

/// Whether the stored revision of `target` is at least as new as `incoming`.
pub fn lifecycle_stale(
    rows: &mut PlanRows,
    target: &DocumentTarget,
    incoming: &DocumentChange,
) -> Step<bool> {
    let Some(value) = rows.read(SYNC_REVISION_KEYSPACE, sync_revision_key(target))? else {
        return Ok(false);
    };
    let local: DocumentChange = postcard::from_bytes(&value).map_err(invalid)?;
    Ok(incoming.current <= local.current)
}

pub fn validate_metadata_event(event: &MetadataEventRecord) -> Result<(), ApplyError> {
    if !event.record.identity_valid()
        || event.record.last_event_id != event.event_id
        || event.is_create() && event.record.establishing_event_id != event.event_id
    {
        return Err(ApplyError::Invalid(
            "replicated metadata event has inconsistent event identity".to_string(),
        ));
    }
    Ok(())
}

pub fn same_create_event(accepted: &MetadataEventRecord, incoming: &MetadataEventRecord) -> bool {
    accepted.event_id == incoming.event_id
        && accepted.record.same_identity(&incoming.record)
        && accepted.record.public == incoming.record.public
        && accepted.record.updated_at_ms == incoming.record.updated_at_ms
        && accepted.record.last_event_id == incoming.record.last_event_id
        && accepted.user_id == incoming.user_id
        && accepted.node_id == incoming.node_id
        && accepted.payload == incoming.payload
        && accepted.occurred_at_ms == incoming.occurred_at_ms
}

fn sidecar_repairs(rows: &mut PlanRows, record: &MetadataRegistryRecord) -> Step<Vec<Row>> {
    let entries = registry_write_entries(record).map_err(invalid)?;
    let mut repairs = Vec::new();
    for (space, key, value) in entries.into_iter().skip(1) {
        if rows.read(&space, key.clone())?.as_ref() != Some(&value) {
            repairs.push((space, key, value));
        }
    }
    Ok(repairs)
}

fn graph_record(rows: &mut PlanRows, graph_iri: &str) -> Step<Option<GraphLifecycleRecord>> {
    let Some(value) = rows.read(GRAPH_LIFECYCLE_KEYSPACE, graph_lifecycle_key(graph_iri))? else {
        return Ok(None);
    };
    Ok(Some(postcard::from_bytes(&value).map_err(invalid)?))
}

fn delete_record(rows: &mut PlanRows, document_id: Ulid) -> Step<Option<MetadataDeleteRecord>> {
    let key = document_lifecycle_key(document_id);
    let Some(value) = rows.read(DOCUMENT_LIFECYCLE_KEYSPACE, key)? else {
        return Ok(None);
    };
    let record: MetadataLifecycleRecord = postcard::from_bytes(&value).map_err(invalid)?;
    Ok(match record {
        MetadataLifecycleRecord::Delete { event } => Some(event),
        MetadataLifecycleRecord::Upsert { .. }
        | MetadataLifecycleRecord::UpsertWithMessage { .. } => None,
    })
}

/// Whether a stored delete or graph tombstone fences this event out.
pub fn create_fence(rows: &mut PlanRows, event: &MetadataEventRecord) -> Step<bool> {
    let fenced = match delete_record(rows, event.record.document_id)? {
        Some(delete) => event.event_id <= delete.deleted_after_id,
        None => {
            graph_record(rows, &event.record.graph_iri)?.is_some_and(|record| record.is_deleted())
        }
    };
    if !fenced {
        group_fence(rows, event.record.group_id)?;
    }
    Ok(fenced)
}

/// Whether a stored delete or graph tombstone fences this registry record out.
pub fn record_fenced(rows: &mut PlanRows, record: &MetadataRegistryRecord) -> Step<bool> {
    if let Some(delete) = delete_record(rows, record.document_id)? {
        return Ok(!registry_live(rows, record.group_id, record.document_id, &delete)?.0);
    }
    Ok(graph_record(rows, &record.graph_iri)?.is_some_and(|record| record.is_deleted()))
}

fn registry_cleanup(
    rows: &mut PlanRows,
    group_id: Ulid,
    document_id: Ulid,
    delete: &MetadataDeleteRecord,
) -> Step<Vec<(KeySpace, Key)>> {
    if !delete.deletes_document(group_id, document_id) {
        return Ok(Vec::new());
    }
    let target = DocumentTarget::MetadataRegistry {
        group_id,
        document_id,
    };
    // A missing registry row means cleanup already ran; an equal delete replay stays a no-op.
    let Some(value) = rows.read(target.storage_keyspace(), target.storage_key())? else {
        return Ok(Vec::new());
    };
    let record: MetadataRegistryRecord = postcard::from_bytes(&value).map_err(invalid)?;
    if record.updated_at_ms > delete.tombstone.updated_at_ms
        || record.last_event_id > delete.deleted_after_id
    {
        return Ok(Vec::new());
    }
    Ok(registry_delete_entries(&record))
}

/// Fences a registry record by its group and the placement of its document.
pub fn placement_fence(
    rows: &mut PlanRows,
    record: &MetadataRegistryRecord,
) -> Step<PlacementOutcome<()>> {
    group_fence(rows, record.group_id)?;
    Ok(
        match derive_placement(
            rows,
            record.realm_id,
            Some(record.group_id),
            record.document_id,
            record.placement,
        )? {
            PlacementOutcome::Accepted(_) => PlacementOutcome::Accepted(()),
            PlacementOutcome::Deferred(dependency) => PlacementOutcome::Deferred(dependency),
            PlacementOutcome::Rejected => PlacementOutcome::Rejected,
        },
    )
}

/// The placement a structured metadata id must ride, derived from the realm config and compared
/// with the publisher's stamp. `group_id` is compared only when known.
pub fn derive_placement(
    rows: &mut PlanRows,
    realm_id: RealmId,
    group_id: Option<Ulid>,
    document_id: Ulid,
    placement: PlacementRef,
) -> Step<PlacementOutcome<PlacementRef>> {
    let dependency = DocumentSyncDependency::PlacementStrategy {
        realm_id,
        strategy_id: placement.strategy_id,
    };
    if placement == PlacementRef::NIL || placement.strategy_id.is_nil() {
        return Ok(PlacementOutcome::Rejected);
    }
    let Some(config) = realm_config(rows, realm_id)? else {
        return Ok(PlacementOutcome::Deferred(
            DocumentSyncDependency::RealmConfig(realm_id),
        ));
    };
    if config.realm_id != realm_id {
        return Ok(PlacementOutcome::Rejected);
    }
    let Ok(id) = MetaResourceId::from_bytes(document_id.to_bytes()) else {
        return Ok(PlacementOutcome::Rejected);
    };
    let resolved = match config.binding_directory().resolve_id(&id, |strategy_id| {
        config
            .strategy(&strategy_id)
            .and_then(|strategy| u16::try_from(strategy.shard_count).ok())
    }) {
        Ok(resolved) => resolved,
        Err(BindingError::UnknownStrategy(_)) => {
            return Ok(PlacementOutcome::Deferred(dependency));
        }
        Err(BindingError::Unknown(_)) => {
            return Ok(PlacementOutcome::Deferred(
                DocumentSyncDependency::RealmConfig(realm_id),
            ));
        }
        Err(BindingError::Conflicted(_) | BindingError::OutOfRange(_)) => {
            return Ok(PlacementOutcome::Rejected);
        }
    };
    let scope_matches = match resolved.scope {
        PlacementScope::Realm(scope_realm) => scope_realm == realm_id,
        PlacementScope::Group(scope_group) => group_id.is_none_or(|group| scope_group == group),
    };
    let derived = PlacementRef {
        strategy_id: resolved.strategy_id,
        shard: u32::from(resolved.bucket.get()),
    };
    if resolved.document_class != DocumentClass::Metadata || !scope_matches || derived != placement
    {
        return Ok(PlacementOutcome::Rejected);
    }
    Ok(PlacementOutcome::Accepted(derived))
}

/// The rows a document delete writes: its lifecycle, the graph tombstone and the prune job.
pub fn document_delete_entries(
    record: &MetadataDeleteRecord,
    now_ms: u64,
) -> Result<Vec<Row>, ApplyError> {
    let lifecycle = MetadataLifecycleRecord::Delete {
        event: record.clone(),
    };
    let mut entries = vec![
        document_lifecycle_entry(&lifecycle).map_err(invalid)?,
        graph_lifecycle_entry(&record.tombstone).map_err(invalid)?,
    ];
    if record.tombstone.is_deleted() {
        let job = GraphPruneRecord::new(record.tombstone.graph_iri.clone(), now_ms);
        entries.push(graph_prune_entry(&job).map_err(invalid)?);
    }
    Ok(entries)
}

fn registry_live(
    rows: &mut PlanRows,
    group_id: Ulid,
    document_id: Ulid,
    delete: &MetadataDeleteRecord,
) -> Step<(bool, Option<MetadataRegistryRecord>)> {
    let target = DocumentTarget::MetadataRegistry {
        group_id,
        document_id,
    };
    let Some(value) = rows.read(target.storage_keyspace(), target.storage_key())? else {
        return Ok((false, None));
    };
    let record: MetadataRegistryRecord = postcard::from_bytes(&value).map_err(invalid)?;
    let live = record.updated_at_ms > delete.tombstone.updated_at_ms
        || record.last_event_id > delete.deleted_after_id;
    Ok((live, Some(record)))
}
