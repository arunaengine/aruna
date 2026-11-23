//! Runs every document apply plan over rows held in memory and checks what it decides.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use ulid::Ulid;

use super::admin::{AdminOperationPlan, ConfigRunPlan};
use super::create::{CreateBatchOutcome, CreateBatchPlan, CreateEventPlan, PendingCreateApply};
use super::distribution::{DistributionPlan, ReplayForkPlan, RetryParkedPlan};
use super::events::{ApplyEventPlan, ApplyOutcome, PolicyChangePlan, WatchChangePlan};
use super::fixtures::{
    actor, check_plan, config_target, config_topic, node, realm, signature, single_event,
    snapshot_event,
};
use super::followups::{FollowUpNode, QueuedApply, followup_id, followup_row};
use super::links::LinkPlan;
use super::quarantine::CursorEvidencePlan;
use super::reads::{dependency_available, relay_allowed};
use super::registry::{
    DeleteRegistryPlan, DocumentLifecyclePlan, GraphLifecyclePlan, PidMappingPlan,
    PolicyDocumentPlan, RegistryUpsertPlan, Row,
};
use super::{ApplyError, PlacementOutcome};
use crate::calls::{IoCall, IoResult, Next};
use crate::config_authority::AdminEventValidation;
use crate::deferred_topics::DocumentSyncDependency;
use crate::document::{
    AppliedStep, AppliedSync, DocumentChange, DocumentChangeKind, DocumentEvent,
    DocumentReconcileResult, DocumentSyncRevision, DocumentTarget, order_counter_key,
    order_entry_key,
};
use crate::effects::{Effect, StorageEffect};
use crate::events::{Event, StorageEvent};
use crate::keyspaces::{
    APPLIED_ORDER_KEYSPACE, APPLIED_SYNC_KEYSPACE, DOCUMENT_LIFECYCLE_KEYSPACE,
    SYNC_REVISION_KEYSPACE,
};
use crate::metadata::{
    GraphLifecycleRecord, MetadataDeleteRecord, MetadataEventPayload, MetadataEventRecord,
    MetadataLifecycleRecord,
};
use crate::operation::{Operation, TransactionMode};
use crate::row_plan::{PlanOperation, PlanRows, RowPlan};
use crate::storage_entries::{
    document_lifecycle_key, registry_delete_entries, registry_write_entries, sync_revision_key,
};
use crate::structs::execution::job::identity::JobId;
use crate::structs::identity::realm::RealmConfigDocument;
use crate::structs::persistent_id::{PersistentIdMapping, PersistentIdRevision};
use crate::structs::placement::policy::document::{
    PlacementPolicyDocument, PolicyPublicationClaim,
};
use crate::structs::placement::policy::{PlacementPolicy, PlacementSelector, VerifiedPolicy};
use crate::structs::placement::record::{LabelMatch, METADATA_HANDLE, PlacementRef};
use crate::structs::storage::metadata_registry::MetadataRegistryRecord;
use crate::structs::sync_quarantine::{
    QUARANTINE_MAX_RECORDS, SyncQuarantineIdentity, SyncQuarantineUsage, SyncRejection,
    quarantine_usage_entry,
};
use crate::structured_id::{BucketId, MetaResourceId, PlacementHandle, StructuredId};
use crate::types::{Key, KeySpace, Value};
use crate::user::id::UserId;

type Stored = BTreeMap<(KeySpace, Key), Value>;

const NOW_MS: u64 = 5_000;

/// What a plan decided over the stored rows.
#[derive(Debug)]
struct Ran<T> {
    output: Result<T, ApplyError>,
    deletes: Vec<(KeySpace, Key)>,
    writes: Vec<Row>,
}

impl<T> Ran<T> {
    fn output(self) -> T
    where
        T: std::fmt::Debug,
    {
        self.output.expect("the plan succeeds")
    }

    fn unchanged(&self) -> bool {
        self.deletes.is_empty() && self.writes.is_empty()
    }
}

fn scan_answer(effect: Effect, stored: &Stored) -> IoResult {
    let Effect::Storage(StorageEffect::Iter {
        key_space,
        prefix,
        limit,
        ..
    }) = effect
    else {
        panic!("a plan asked for other I/O: {effect:?}");
    };
    let values = stored
        .iter()
        .filter(|((space, key), _)| {
            *space == key_space
                && prefix
                    .as_ref()
                    .is_none_or(|prefix| key.starts_with(prefix.as_ref()))
        })
        .take(limit)
        .map(|((_, key), value)| (key.clone(), value.clone()))
        .collect();
    IoResult::Event(Box::new(Event::Storage(StorageEvent::IterResult {
        values,
        next_start_after: None,
    })))
}

fn run<P>(plan: P, stored: &Stored) -> Ran<P::Output>
where
    P: RowPlan<Error = ApplyError>,
    P::Output: PartialEq,
{
    let mut operation = PlanOperation::new(plan);
    operation.stamp(NOW_MS);
    let (mut deletes, mut writes) = (Vec::new(), Vec::new());
    let mut next = operation.first();
    loop {
        let result = match next {
            Err(error) => {
                return Ran {
                    output: Err(error),
                    deletes,
                    writes,
                };
            }
            Ok(Next::Done(output)) => {
                return Ran {
                    output: Ok(output),
                    deletes,
                    writes,
                };
            }
            Ok(Next::Call(IoCall::Read(keys))) => {
                IoResult::Values(keys.iter().map(|key| stored.get(key).cloned()).collect())
            }
            Ok(Next::Call(IoCall::Delete(rows))) => {
                deletes = rows;
                IoResult::Done
            }
            Ok(Next::Call(IoCall::Write(rows))) => {
                writes = rows;
                IoResult::Done
            }
            Ok(Next::Call(IoCall::Effect(effect))) => scan_answer(*effect, stored),
            Ok(other) => panic!("a plan asked for something else: {other:?}"),
        };
        next = operation.next(result);
    }
}

fn nothing() -> Stored {
    Stored::new()
}

fn placed() -> PlacementRef {
    PlacementRef {
        strategy_id: Ulid::from_parts(4, 4),
        shard: 0,
    }
}

fn config_missing<T>() -> PlacementOutcome<T> {
    PlacementOutcome::Deferred(DocumentSyncDependency::RealmConfig(realm()))
}

fn group_id() -> Ulid {
    Ulid::from_parts(5, 5)
}

fn document_id() -> Ulid {
    Ulid::from_parts(6, 6)
}

fn registry_record(placement: PlacementRef) -> MetadataRegistryRecord {
    let path = "datasets/registry";
    MetadataRegistryRecord {
        realm_id: realm(),
        group_id: group_id(),
        document_id: document_id(),
        document_path: path.to_string(),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id()),
        public: true,
        permission_path: MetadataRegistryRecord::permission_path_for(
            &realm(),
            group_id(),
            path,
            document_id(),
        ),
        placement,
        holder_node_ids: vec![node()],
        created_at_ms: 1,
        updated_at_ms: 2,
        establishing_event_id: Ulid::from_parts(7, 7),
        last_event_id: Ulid::from_parts(7, 7),
    }
}

fn create_event() -> MetadataEventRecord {
    MetadataEventRecord {
        event_id: Ulid::from_parts(7, 7),
        record: registry_record(placed()),
        user_id: UserId::nil(realm()),
        node_id: node(),
        payload: MetadataEventPayload::RoCrate {
            jsonld: "{}".to_string(),
        },
        occurred_at_ms: 2,
    }
}

fn tombstone(updated_at_ms: u64) -> GraphLifecycleRecord {
    GraphLifecycleRecord::deleted(
        MetadataRegistryRecord::graph_iri_for(document_id()),
        realm(),
        group_id(),
        document_id(),
        updated_at_ms,
    )
}

fn delete_record() -> MetadataDeleteRecord {
    MetadataDeleteRecord {
        event_id: Ulid::from_parts(8, 8),
        tombstone: tombstone(10),
        deleted_after_id: Ulid::from_parts(7, 7),
    }
}

/// Rows that hold the delete of the test document.
fn deleted_document() -> Stored {
    let record = MetadataLifecycleRecord::Delete {
        event: delete_record(),
    };
    Stored::from([(
        (
            DOCUMENT_LIFECYCLE_KEYSPACE.to_string(),
            document_lifecycle_key(document_id()),
        ),
        Value::from(postcard::to_allocvec(&record).unwrap()),
    )])
}

fn with_row(mut stored: Stored, row: Row) -> Stored {
    stored.insert((row.0, row.1), row.2);
    stored
}

fn registry_row() -> Row {
    registry_write_entries(&registry_record(placed()))
        .unwrap()
        .remove(0)
}

fn change(kind: DocumentChangeKind, placement: PlacementRef) -> DocumentChange {
    DocumentChange {
        base: None,
        current: DocumentSyncRevision {
            generation: 1,
            event_id: Ulid::from_parts(9, 9),
            actor: node(),
            updated_at_ms: 1,
        },
        kind,
        placement,
    }
}

fn revision_row(target: &DocumentTarget, change: &DocumentChange) -> Row {
    (
        SYNC_REVISION_KEYSPACE.to_string(),
        sync_revision_key(target),
        Value::from(postcard::to_allocvec(change).unwrap()),
    )
}

fn pid_mapping() -> PersistentIdMapping {
    PersistentIdMapping::requested(
        document_id(),
        false,
        UserId::nil(realm()),
        JobId::from_bytes([4; 16]),
        true,
        "/documents/test".to_string(),
        PersistentIdRevision {
            event_id: Ulid::from_parts(9, 9),
            actor: node(),
            occurred_at_ms: 1,
        },
    )
}

fn policy_document() -> PlacementPolicyDocument {
    let policy = PlacementPolicy::new(
        Ulid::from_parts(3, 3),
        "eu-only".to_string(),
        vec![PlacementSelector {
            node_id: None,
            location: Some("eu-west".to_string()),
            labels: vec![LabelMatch {
                key: "tier".to_string(),
                value: "hot".to_string(),
            }],
            executor_kind: None,
        }],
    )
    .unwrap();
    let policy = VerifiedPolicy::verify(policy).unwrap();
    let secret = iroh::SecretKey::from_bytes(&[2; 32]);
    let publication = PolicyPublicationClaim::new(
        realm(),
        &policy,
        node(),
        UserId::nil(realm()),
        Ulid::from_parts(2, 2),
        1,
        [7; 32],
    )
    .sign(&secret);
    PlacementPolicyDocument::new(realm(), &policy, publication)
}

fn identity() -> SyncQuarantineIdentity {
    SyncQuarantineIdentity {
        topic: config_topic(),
        actor: actor(),
        actor_seq: 1,
    }
}

fn rejection() -> SyncRejection {
    let event = DocumentEvent::Delete {
        event_id: Ulid::from_parts(9, 9),
        target: config_target(),
        change: change(DocumentChangeKind::Delete, PlacementRef::NIL),
    };
    SyncRejection::new(identity(), event, "refused")
}

fn cursor_row() -> Row {
    (
        "cursors".to_string(),
        Key::from(vec![1]),
        Value::from(vec![2]),
    )
}

fn invalid<T: std::fmt::Debug>(ran: Ran<T>) -> bool {
    matches!(ran.output, Err(ApplyError::Invalid(_)))
}

#[test]
fn upsert_checks_identity() {
    let mut record = registry_record(placed());
    record.graph_iri = "other".to_string();
    let plan = RegistryUpsertPlan {
        record,
        primary_bytes: vec![1],
    };
    let ran = run(plan, &nothing());
    assert!(ran.unchanged());
    assert_eq!(ran.output(), PlacementOutcome::Rejected);
}

#[test]
fn upsert_checks_placement() {
    let nil = RegistryUpsertPlan {
        record: registry_record(PlacementRef::NIL),
        primary_bytes: vec![1],
    };
    assert_eq!(run(nil, &nothing()).output(), PlacementOutcome::Rejected);
    let waits = RegistryUpsertPlan {
        record: registry_record(placed()),
        primary_bytes: vec![1],
    };
    assert_eq!(run(waits, &nothing()).output(), config_missing());
}

#[test]
fn upsert_clears_fenced() {
    let record = registry_record(placed());
    let plan = RegistryUpsertPlan {
        record: record.clone(),
        primary_bytes: vec![1],
    };
    let ran = run(plan, &deleted_document());
    assert_eq!(ran.deletes, registry_delete_entries(&record));
    assert!(ran.writes.is_empty());
    assert_eq!(ran.output(), PlacementOutcome::Accepted(()));
}

#[test]
fn graph_needs_delete() {
    let plan = |record| GraphLifecyclePlan {
        record,
        primary_bytes: vec![1],
    };
    assert!(!run(plan(tombstone(5)), &nothing()).output());
    assert!(!run(plan(tombstone(11)), &deleted_document()).output());

    let ran = run(plan(tombstone(5)), &deleted_document());
    assert_eq!(ran.writes.len(), 1);
    assert_eq!(ran.writes[0].2, Value::from(vec![1]));
    assert!(ran.output());
}

#[test]
fn lifecycle_stores_delete() {
    let target = DocumentTarget::MetadataDocumentLifecycle {
        document_id: document_id(),
    };
    let revision = change(DocumentChangeKind::Upsert, placed());
    let plan = DocumentLifecyclePlan {
        record: MetadataLifecycleRecord::Delete {
            event: delete_record(),
        },
        change: revision,
    };
    let ran = run(plan.clone(), &nothing());
    assert!(!ran.writes.is_empty());
    assert!(ran.output());

    let stale = with_row(nothing(), revision_row(&target, &revision));
    let ran = run(plan, &stale);
    assert!(ran.unchanged());
    assert!(!ran.output());
}

// A pulled delete stores its follow-up row in its own transaction, so a replay that finds the
// delete applied and skips it leaves the queued graph prune in place.
#[test]
fn delete_queues_followups() {
    let queued = QueuedApply {
        plan: lifecycle_delete(),
        node: followup_node(),
        identity: identity(),
    };
    let ran = run(queued.clone(), &nothing());
    let row = followup_row(&followup_node(), &[identity()], deleted_result()).unwrap();
    assert!(ran.writes.contains(&row));
    let stored: Stored = (ran.writes.iter())
        .map(|(space, key, value)| ((space.clone(), key.clone()), value.clone()))
        .collect();
    let replayed = run(queued, &stored);
    assert!(replayed.unchanged());
    assert_eq!(replayed.output(), ApplyOutcome::Skipped);
}

/// The pulled delete of the document, as its lifecycle record.
fn lifecycle_delete() -> ApplyEventPlan {
    let record = MetadataLifecycleRecord::Delete {
        event: delete_record(),
    };
    ApplyEventPlan {
        topic_id: config_topic(),
        actor_id: actor(),
        realm_id: realm(),
        identity: identity(),
        event: DocumentEvent::Upsert {
            event_id: Ulid::from_parts(8, 8),
            target: lifecycle_target(),
            bytes: postcard::to_allocvec(&record).unwrap(),
            change: change(DocumentChangeKind::Upsert, placed()),
        },
    }
}

fn lifecycle_target() -> DocumentTarget {
    DocumentTarget::MetadataDocumentLifecycle {
        document_id: document_id(),
    }
}

/// What the drain has to do after the delete: clean up its graph.
fn deleted_result() -> DocumentReconcileResult {
    DocumentReconcileResult {
        targets: vec![lifecycle_target()],
        metadata_graph_tombstones: vec![tombstone(10)],
        ..DocumentReconcileResult::default()
    }
}

// An inbound or outbox sync runs the refreshes itself and queues only the metadata follow-up,
// in the apply's own transaction: a graph cleanup refused later, and a refused write after the
// cursor moved, leave the obligation stored with the delete it came with.
#[test]
fn metadata_followup_stored() {
    let node = FollowUpNode {
        from: AppliedStep::Metadata,
        ..followup_node()
    };
    let queued = QueuedApply {
        plan: lifecycle_delete(),
        node,
        identity: identity(),
    };
    let ran = run(queued, &nothing());
    let row = followup_row(&node, &[identity()], deleted_result()).unwrap();
    assert!(ran.writes.contains(&row));
    let stored = AppliedSync::from_bytes(&row.2).unwrap();
    assert_eq!(stored.next, AppliedStep::Metadata);
}

#[test]
fn lifecycle_refuses_mismatch() {
    let mut event = create_event();
    event.event_id = Ulid::from_parts(1, 1);
    let plan = DocumentLifecyclePlan {
        record: MetadataLifecycleRecord::Upsert {
            event: Box::new(event),
        },
        change: change(DocumentChangeKind::Upsert, placed()),
    };
    assert!(invalid(run(plan, &nothing())));
}

#[test]
fn removes_deleted_registry() {
    let plan = DeleteRegistryPlan {
        group_id: group_id(),
        document_id: document_id(),
    };
    assert!(run(plan.clone(), &nothing()).unchanged());
    let stored = with_row(deleted_document(), registry_row());
    let ran = run(plan, &stored);
    assert_eq!(
        ran.deletes,
        registry_delete_entries(&registry_record(placed()))
    );
}

#[test]
fn mapping_checks_placement() {
    let plan = |placement| PidMappingPlan {
        realm_id: realm(),
        incoming: pid_mapping(),
        placement,
    };
    assert_eq!(
        run(plan(PlacementRef::NIL), &nothing()).output(),
        PlacementOutcome::Rejected
    );
    assert_eq!(run(plan(placed()), &nothing()).output(), config_missing());
}

#[test]
fn policy_checks_placement() {
    let plan = |placement| PolicyDocumentPlan {
        realm_id: realm(),
        incoming: policy_document(),
        placement,
    };
    assert_eq!(
        run(plan(PlacementRef::NIL), &nothing()).output(),
        PlacementOutcome::Rejected
    );
    assert_eq!(run(plan(placed()), &nothing()).output(), config_missing());
}

#[test]
fn policy_change_waits() {
    let plan = PolicyChangePlan {
        realm_id: realm(),
        document: policy_document(),
        placement: placed(),
    };
    assert_eq!(run(plan, &nothing()).output(), config_missing());
}

#[test]
fn link_checks_target() {
    let change = change(DocumentChangeKind::Upsert, placed());
    let wrong = LinkPlan {
        realm_id: realm(),
        target: config_target(),
        bytes: None,
        change,
    };
    assert_eq!(run(wrong, &nothing()).output(), PlacementOutcome::Rejected);
}

#[test]
fn watch_applies_once() {
    let target = DocumentTarget::WatchSubscription {
        owner: UserId::nil(realm()),
        watch_id: Ulid::from_parts(1, 2),
    };
    let upsert = change(DocumentChangeKind::Upsert, PlacementRef::NIL);
    let plan = WatchChangePlan {
        target: target.clone(),
        bytes: Some(vec![1]),
        change: upsert,
    };
    let ran = run(plan.clone(), &nothing());
    assert_eq!(ran.writes.len(), 3);
    assert!(ran.output());

    let stored = with_row(nothing(), revision_row(&target, &upsert));
    let ran = run(plan, &stored);
    assert!(ran.unchanged());
    assert!(!ran.output());

    let delete = WatchChangePlan {
        target: target.clone(),
        bytes: None,
        change: change(DocumentChangeKind::Delete, PlacementRef::NIL),
    };
    let ran = run(delete, &stored);
    let row = (target.storage_keyspace().to_string(), target.storage_key());
    assert_eq!(ran.deletes, vec![row]);
}

#[test]
fn create_respects_fence() {
    let target = DocumentTarget::MetadataCreateEvent {
        document_id: document_id(),
        event_id: Ulid::from_parts(7, 7),
    };
    let plan = CreateEventPlan {
        event: create_event(),
        target: target.clone(),
        bytes: vec![1],
    };
    let row = (
        target.storage_keyspace().to_string(),
        target.storage_key(),
        Value::from(vec![1]),
    );
    assert_eq!(run(plan.clone(), &nothing()).writes, vec![row]);
    assert!(run(plan, &deleted_document()).unchanged());
}

#[test]
fn batch_moves_cursors() {
    let empty = CreateBatchPlan::new(Vec::new(), Vec::new(), Vec::new()).unwrap();
    let ran = run(empty, &nothing());
    assert!(ran.unchanged());
    assert_eq!(ran.output(), CreateBatchOutcome::default());

    let cursor = vec![(config_topic(), cursor_row())];
    let plan = CreateBatchPlan::new(Vec::new(), cursor, Vec::new()).unwrap();
    assert!(run(plan, &nothing()).writes.contains(&cursor_row()));
}

/// A create whose structured id rides the seeded metadata binding, and the stored realm config
/// that accepts its placement.
fn accepted_create() -> (PendingCreateApply, Stored) {
    let mut config = RealmConfigDocument::default_for_realm(realm(), Vec::new());
    config.seed_default_placement();
    let strategy_id = config.default_strategy_id.expect("seeded default strategy");
    let handle = PlacementHandle::new(METADATA_HANDLE).unwrap();
    let id = MetaResourceId::from_parts(7, handle, BucketId::new(0).unwrap(), 9).unwrap();
    let document_id = id.as_ulid();
    let placement = PlacementRef {
        strategy_id,
        shard: 0,
    };
    let path = "datasets/accepted";
    let mut record = create_event();
    record.record = MetadataRegistryRecord {
        document_id,
        document_path: path.to_string(),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        permission_path: MetadataRegistryRecord::permission_path_for(
            &realm(),
            group_id(),
            path,
            document_id,
        ),
        ..registry_record(placement)
    };
    let target = DocumentTarget::MetadataCreateEvent {
        document_id,
        event_id: record.event_id,
    };
    let pending = PendingCreateApply {
        identity: identity(),
        event: DocumentEvent::Upsert {
            event_id: record.event_id,
            target: target.clone(),
            bytes: vec![1],
            change: change(DocumentChangeKind::Upsert, placement),
        },
        target,
        record,
        bytes: vec![1],
        lifecycle_revision: None,
        message: None,
    };
    let config_target = DocumentTarget::RealmConfig { realm_id: realm() };
    let row = (
        config_target.storage_keyspace().to_string(),
        config_target.storage_key(),
    );
    let stored = Stored::from([(row, Value::from(postcard::to_allocvec(&config).unwrap()))]);
    (pending, stored)
}

fn followup_node() -> FollowUpNode {
    FollowUpNode {
        node_id: node(),
        realm_id: realm(),
        from: AppliedStep::Usage,
    }
}

// An accepted create writes its follow-up row, keyed by its sync identity, in the create
// transaction; moving cursors alone queues none.
#[test]
fn batch_queues_accepted() {
    let (pending, stored) = accepted_create();
    let accepted = (pending.target.clone(), pending.record.clone());
    let plan = CreateBatchPlan::new(vec![pending], Vec::new(), Vec::new()).unwrap();
    let ran = run(plan.queuing(Some(followup_node())), &stored);
    let result = DocumentReconcileResult {
        targets: vec![accepted.0.clone()],
        metadata_create_events: vec![accepted.1.clone()],
        ..DocumentReconcileResult::default()
    };
    let row = followup_row(&followup_node(), &[identity()], result).unwrap();
    assert!(ran.writes.contains(&row));
    assert_eq!(ran.output().accepted, vec![accepted]);

    let cursor = vec![(config_topic(), cursor_row())];
    let plan = CreateBatchPlan::new(Vec::new(), cursor, Vec::new()).unwrap();
    let ran = run(plan.queuing(Some(followup_node())), &nothing());
    assert_eq!(ran.writes, vec![cursor_row()]);
}

// The events one create transaction accepts share one follow-up row in acceptance order, so the
// drain never projects the update before its create, even where their own rows sorted reversed.
#[test]
fn batch_keeps_order() {
    let (create, stored) = accepted_create();
    let alone = |identity: SyncQuarantineIdentity| {
        let empty = DocumentReconcileResult::default();
        let row = followup_row(&followup_node(), &[identity], empty).unwrap();
        row.1.to_vec()
    };
    let first = alone(create.identity);
    let sorts_first = |actor_seq| {
        let mut identity = create.identity;
        identity.actor_seq = actor_seq;
        alone(identity) < first
    };
    let actor_seq = (2..64)
        .find(|seq| sorts_first(*seq))
        .expect("an identity whose own row sorts first");
    let update = accepted_update(&create, actor_seq);
    let order = [create.clone(), update.clone()];
    let plan = CreateBatchPlan::new(order.to_vec(), Vec::new(), Vec::new()).unwrap();
    let ran = run(plan.queuing(Some(followup_node())), &stored);
    let result = DocumentReconcileResult {
        targets: vec![create.target.clone(), update.target.clone()],
        metadata_create_events: vec![create.record.clone(), update.record.clone()],
        ..DocumentReconcileResult::default()
    };
    let identities = [create.identity, update.identity];
    let row = followup_row(&followup_node(), &identities, result).unwrap();
    let queued: Vec<_> = (ran.writes.iter())
        .filter(|(space, ..)| space == APPLIED_SYNC_KEYSPACE)
        .collect();
    assert_eq!(queued, vec![&row]);
}

/// An update of the document `create` makes, sent under the actor's `actor_seq`.
fn accepted_update(create: &PendingCreateApply, actor_seq: u64) -> PendingCreateApply {
    let mut update = create.clone();
    update.identity.actor_seq = actor_seq;
    update.record.event_id = Ulid::from_parts(8, 8);
    update.record.record.last_event_id = update.record.event_id;
    update.record.payload = MetadataEventPayload::UpsertContextualEntity {
        jsonld: r##"{"@id":"#person","@type":"Person"}"##.to_string(),
    };
    update.target = DocumentTarget::MetadataCreateEvent {
        document_id: update.record.record.document_id,
        event_id: update.record.event_id,
    };
    update
}

// Batches of one document that separate transactions accept are queued in that order under
// their own ids, so the drain projects the update after its create whatever the ids sort as.
#[test]
fn transactions_keep_order() {
    let (create, mut stored) = accepted_create();
    let document_id = create.record.record.document_id;
    let first = CreateBatchPlan::new(vec![create.clone()], Vec::new(), Vec::new()).unwrap();
    let ran = run(first.queuing(Some(followup_node())), &stored);
    let entry = |sequence: u64, identity: SyncQuarantineIdentity| {
        let id = Value::from(followup_id(&[identity]).to_bytes().to_vec());
        let key = order_entry_key(document_id, sequence);
        (APPLIED_ORDER_KEYSPACE.to_string(), key, id)
    };
    let counter = |next: u64| {
        let key = order_counter_key(document_id);
        let next = Value::from(next.to_be_bytes().to_vec());
        (APPLIED_ORDER_KEYSPACE.to_string(), key, next)
    };
    assert!(ran.writes.contains(&entry(0, create.identity)));
    assert!(ran.writes.contains(&counter(1)));
    let written: Stored = (ran.writes.into_iter())
        .map(|(space, key, value)| ((space, key), value))
        .collect();
    stored.extend(written);

    let update = accepted_update(&create, 2);
    let second = CreateBatchPlan::new(vec![update.clone()], Vec::new(), Vec::new()).unwrap();
    let ran = run(second.queuing(Some(followup_node())), &stored);
    assert!(ran.writes.contains(&entry(1, update.identity)));
    assert!(ran.writes.contains(&counter(2)));
}

#[test]
fn batch_rejects_invalid() {
    let mut record = create_event();
    record.event_id = Ulid::from_parts(1, 1);
    let target = DocumentTarget::MetadataCreateEvent {
        document_id: document_id(),
        event_id: record.event_id,
    };
    let pending = PendingCreateApply {
        identity: identity(),
        event: DocumentEvent::Upsert {
            event_id: record.event_id,
            target: target.clone(),
            bytes: vec![1],
            change: change(DocumentChangeKind::Upsert, placed()),
        },
        target,
        record,
        bytes: vec![1],
        lifecycle_revision: None,
        message: None,
    };
    let plan = CreateBatchPlan::new(vec![pending], Vec::new(), Vec::new()).unwrap();
    let ran = run(plan, &nothing());
    assert!(!ran.writes.is_empty());
    let outcome = ran.output();
    assert_eq!(outcome.notes.len(), 1);
    assert!(outcome.accepted.is_empty());
}

#[test]
fn evidence_moves_cursor() {
    let plan = CursorEvidencePlan {
        rejections: vec![rejection()],
        cursor: cursor_row(),
    };
    let ran = run(plan, &nothing());
    assert_eq!(ran.writes.len(), 3);
    assert_eq!(ran.writes.last(), Some(&cursor_row()));
    assert!(ran.output());
}

#[test]
fn full_holds_cursor() {
    let usage = SyncQuarantineUsage {
        records: QUARANTINE_MAX_RECORDS,
        bytes: 0,
    };
    let stored = with_row(nothing(), quarantine_usage_entry(usage).unwrap());
    let plan = CursorEvidencePlan {
        rejections: vec![rejection()],
        cursor: cursor_row(),
    };
    let ran = run(plan, &stored);
    assert!(ran.unchanged());
    assert!(!ran.output());
}

#[test]
fn admin_checks_target() {
    let plan = AdminOperationPlan {
        target: DocumentTarget::RealmAuthorization { realm_id: realm() },
        event: single_event(1),
    };
    assert_eq!(AdminOperationPlan::conflict_retries(), 63);
    assert!(invalid(run(plan, &nothing())));
}

#[test]
fn run_checks_target() {
    let wrong = ConfigRunPlan {
        target: DocumentTarget::RealmAuthorization { realm_id: realm() },
        events: vec![single_event(1)],
    };
    assert!(invalid(run(wrong, &nothing())));
    let empty = ConfigRunPlan {
        target: config_target(),
        events: Vec::new(),
    };
    assert!(run(empty, &nothing()).unchanged());
}

#[test]
fn distribution_checks_event() {
    let plan = |target, event| DistributionPlan { target, event };
    let wrong_target = DocumentTarget::RealmAuthorization { realm_id: realm() };
    assert!(invalid(run(
        plan(wrong_target, snapshot_event(1, 1)),
        &nothing()
    )));
    assert!(invalid(run(
        plan(config_target(), single_event(1)),
        &nothing()
    )));
    assert!(invalid(run(
        plan(config_target(), snapshot_event(1, 1)),
        &nothing()
    )));
}

#[test]
fn parked_retry_empty() {
    let plan = RetryParkedPlan { realm_id: realm() };
    assert!(run(plan, &nothing()).unchanged());
}

#[test]
fn fork_needs_snapshots() {
    let plan = ReplayForkPlan {
        earlier: snapshot_event(1, 1),
        event: snapshot_event(2, 1),
    };
    let ran = run(plan, &nothing());
    assert!(ran.unchanged());
    assert!(!ran.output());
}

#[test]
fn check_refuses_topic() {
    let mut plan = check_plan(single_event(1));
    plan.topic_id = irokle::TopicId::from_bytes([9; 32]);
    let (validation, _) = run(plan, &nothing()).output();
    assert!(matches!(validation, AdminEventValidation::Rejected(_)));
}

#[test]
fn check_refuses_signature() {
    let mut plan = check_plan(single_event(1));
    plan.signature = signature();
    let (validation, _) = run(plan, &nothing()).output();
    assert_eq!(
        validation,
        AdminEventValidation::Rejected("admin event is not signed by its origin node".to_string())
    );
}

#[test]
fn event_rejects_document() {
    let target = DocumentTarget::NodeInfo {
        realm_id: realm(),
        node_id: node(),
    };
    let plan = ApplyEventPlan {
        topic_id: config_topic(),
        actor_id: actor(),
        realm_id: realm(),
        identity: identity(),
        event: DocumentEvent::Upsert {
            event_id: Ulid::from_parts(9, 9),
            target,
            bytes: vec![0xff],
            change: change(DocumentChangeKind::Upsert, PlacementRef::NIL),
        },
    };
    assert_eq!(plan.transaction(), TransactionMode::None);
    let ran = run(plan, &nothing());
    assert!(ran.unchanged());
    assert!(matches!(ran.output(), ApplyOutcome::Rejected(_)));
}

#[test]
fn reads_missing_documents() {
    let target = config_target();
    let row = (target.storage_keyspace().to_string(), target.storage_key());
    let mut rows = PlanRows::with_rows([(row, None)], NOW_MS);
    let dependency = DocumentSyncDependency::RealmConfig(realm());
    assert_eq!(dependency_available(&mut rows, dependency), Ok(false));
    assert_eq!(
        relay_allowed(&mut rows, config_topic(), actor(), realm()),
        Ok(false)
    );
}
