
use std::collections::BTreeMap;

use aruna_core::UserId;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::events::StorageEvent;
use aruna_core::metadata::{MetadataCreateEventPayload, MetadataCreateEventRecord};
use aruna_core::storage_entries::{create_projection_entries, registry_write_entries};
use aruna_core::structs::{
    AffinityEffect, AffinityRule, DEFAULT_NODE_WEIGHT, DEFAULT_SHARD_COUNT, DocumentClass,
    FIRST_GRANTABLE_HANDLE, HandleRange, LabelMatch, MetadataRegistryRecord, PlacementBinding,
    PlacementRef, PlacementScope, RealmId, RealmNodeKind,
};
use aruna_core::structured_id::PlacementHandle;
use aruna_core::task::{TaskEffect, TaskKey};
use tempfile::tempdir;

use super::*;
use crate::driver::{DriverContext, drive};
use crate::placement::transition::{TransitionRequest, plan_transition};
use crate::realm::get_config::GetRealmConfigOperation;
use aruna_core::structs::{PlacementTransition, ProofClaim, TransitionLimits};

fn node(seed: u8) -> aruna_core::NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn node_secret(node_id: &aruna_core::NodeId) -> iroh::SecretKey {
    (1..=8u8)
        .map(|seed| iroh::SecretKey::from_bytes(&[seed; 32]))
        .find(|secret| secret.public() == *node_id)
        .expect("fixture node keys are seeded")
}

fn actor(realm_id: RealmId) -> Actor {
    Actor {
        node_id: node(1),
        user_id: UserId::local(Ulid::from_bytes([1; 16]), realm_id),
        realm_id,
    }
}

fn context(root: &str) -> DriverContext {
    DriverContext {
        storage_handle: aruna_storage::FjallStorage::open(root).unwrap(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    }
}

async fn seed_config(context: &DriverContext, actor: &Actor) -> RealmConfigDocument {
    let mut document = RealmConfigDocument::new(actor.realm_id, Vec::new(), 3);
    document.seed_default_placement();
    document.ensure_node(actor.node_id, RealmNodeKind::Management);
    document.placement_handle_ranges.push(HandleRange {
        range_id: Ulid::from_bytes([9; 16]),
        owner: actor.node_id,
        start: FIRST_GRANTABLE_HANDLE,
        end: FIRST_GRANTABLE_HANDLE + 1024,
    });
    let target = DocumentSyncTarget::RealmConfig {
        realm_id: actor.realm_id,
    };
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key(),
            value: document.to_bytes(actor).unwrap().into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
    document
}

async fn mutate(
    context: &DriverContext,
    actor: &Actor,
    mutation: RealmPlacementMutation,
) -> Result<RealmConfigDocument, MutateRealmPlacementError> {
    drive(
        MutateRealmPlacementOperation::new(MutateRealmPlacementConfig {
            actor: actor.clone(),
            mutation,
        }),
        context,
    )
    .await
}

#[test]
fn conflict_is_expected() {
    assert!(MutateRealmPlacementOperation::expected_error(
        &MutateRealmPlacementError::StorageError(StorageError::TransactionConflict)
    ));
    assert!(!MutateRealmPlacementOperation::expected_error(
        &MutateRealmPlacementError::RealmConfigNotFound
    ));
}

#[tokio::test]
async fn concurrent_mutations_land() {
    // Writers racing on one document conflict at commit; each re-drives
    // its own mutation, so all of them land instead of one failing.
    let root = tempdir().unwrap();
    let context = context(root.path().to_str().unwrap());
    let actor = actor(RealmId::from_bytes([21; 32]));
    seed_config(&context, &actor).await;
    let ids: Vec<Ulid> = (1..=6u8).map(|seed| Ulid::from_bytes([seed; 16])).collect();
    let mut tasks = Vec::new();
    for strategy_id in ids.clone() {
        let context = context.clone();
        let actor = actor.clone();
        tasks.push(tokio::spawn(async move {
            drive_placement_mutation(
                MutateRealmPlacementConfig {
                    actor,
                    mutation: RealmPlacementMutation::UpsertStrategy(strategy(strategy_id)),
                },
                None,
                &context,
            )
            .await
        }));
    }
    for task in tasks {
        task.await.unwrap().expect("every mutation lands");
    }
    let config = drive(GetRealmConfigOperation::new(actor.realm_id), &context)
        .await
        .unwrap();
    for strategy_id in ids {
        assert!(config.strategy(&strategy_id).is_some());
    }
}

fn strategy(strategy_id: Ulid) -> PlacementStrategy {
    PlacementStrategy {
        strategy_id,
        name: "hot".to_string(),
        replica_count: Some(2),
        distinct_locations: true,
        affinity: Vec::new(),
        shard_count: 64,
    }
}

fn create_event(actor: &Actor, strategy_id: Ulid, document_seed: u8) -> MetadataCreateEventRecord {
    let document_id = Ulid::from_bytes([document_seed; 16]);
    let event_id = Ulid::from_bytes([document_seed.wrapping_add(1); 16]);
    MetadataCreateEventRecord {
        event_id,
        record: MetadataRegistryRecord {
            realm_id: actor.realm_id,
            group_id: Ulid::from_bytes([document_seed.wrapping_add(2); 16]),
            document_id,
            document_path: "datasets/referenced".to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: true,
            permission_path: "/referenced".to_string(),
            placement: PlacementRef {
                strategy_id,
                shard: 1,
            },
            holder_node_ids: vec![actor.node_id],
            created_at_ms: 1,
            updated_at_ms: 1,
            establishing_event_id: event_id,
            last_event_id: event_id,
        },
        user_id: actor.user_id,
        node_id: actor.node_id,
        payload: MetadataCreateEventPayload::Scaffold {
            name: "Referenced".to_string(),
            description: "Strategy reference".to_string(),
            date_published: "2026-01-01".to_string(),
            license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
        },
        occurred_at_ms: 1,
    }
}

async fn write_entries(context: &DriverContext, writes: Vec<(String, Key, Value)>) {
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));
}

#[tokio::test]
async fn strategy_binding_lifecycle() {
    let directory = tempdir().unwrap();
    let context = context(directory.path().to_str().unwrap());
    let realm_id = RealmId::from_bytes([3; 32]);
    let actor = actor(realm_id);
    let initial = seed_config(&context, &actor).await;
    let initial_default = initial.default_strategy_id.unwrap();
    let strategy_id = Ulid::from_bytes([8; 16]);
    let scope = BindingScope::Class(DocumentClass::Metadata);
    let subject = vec![0xab, 0xcd];

    mutate(
        &context,
        &actor,
        RealmPlacementMutation::UpsertStrategy(strategy(strategy_id)),
    )
    .await
    .unwrap();
    assert!(matches!(
        mutate(
            &context,
            &actor,
            RealmPlacementMutation::SetDefaultStrategy(strategy_id),
        )
        .await,
        Err(MutateRealmPlacementError::InvalidInput(reason))
            if reason.contains("has no binding")
    ));
    let range_id = initial.placement_handle_ranges[0].range_id;
    mutate(
        &context,
        &actor,
        RealmPlacementMutation::AppendPlacementBinding(PlacementBinding {
            handle: PlacementHandle::new(FIRST_GRANTABLE_HANDLE).unwrap(),
            scope: PlacementScope::Realm(realm_id),
            document_class: DocumentClass::Metadata,
            strategy_id,
            allocator_range_id: Some(range_id),
            allocated_by: Some(actor.node_id),
            allocated_at_ms: Some(1),
        }),
    )
    .await
    .unwrap();
    mutate(
        &context,
        &actor,
        RealmPlacementMutation::SetDefaultStrategy(strategy_id),
    )
    .await
    .unwrap();
    mutate(
        &context,
        &actor,
        RealmPlacementMutation::SetBinding(StrategyBinding {
            scope: scope.clone(),
            strategy_id,
        }),
    )
    .await
    .unwrap();
    let stored = mutate(
        &context,
        &actor,
        RealmPlacementMutation::SetOverride(PlacementOverride {
            subject: subject.clone(),
            pinned: vec![node(2)],
            excluded: vec![node(3)],
            strategy_id: Some(strategy_id),
        }),
    )
    .await
    .unwrap();

    assert_eq!(stored.default_strategy_id, Some(strategy_id));
    assert!(stored.strategy(&strategy_id).is_some());
    assert!(
        stored
            .strategy_bindings
            .iter()
            .any(|binding| { binding.scope == scope && binding.strategy_id == strategy_id })
    );
    assert!(
        stored
            .placement_overrides
            .iter()
            .any(|record| { record.subject == subject && record.strategy_id == Some(strategy_id) })
    );

    mutate(
        &context,
        &actor,
        RealmPlacementMutation::RemoveOverride(subject),
    )
    .await
    .unwrap();
    mutate(
        &context,
        &actor,
        RealmPlacementMutation::RemoveBinding(scope.clone()),
    )
    .await
    .unwrap();
    mutate(
        &context,
        &actor,
        RealmPlacementMutation::SetDefaultStrategy(initial_default),
    )
    .await
    .unwrap();
    assert!(matches!(
        mutate(
        &context,
        &actor,
        RealmPlacementMutation::RemoveStrategy(strategy_id),
    )
        .await,
        Err(MutateRealmPlacementError::StrategyReferenced {
            strategy_id: referenced
        }) if referenced == strategy_id
    ));

    let stored = drive(GetRealmConfigOperation::new(realm_id), &context)
        .await
        .unwrap();
    assert!(stored.strategy(&strategy_id).is_some());
    assert_eq!(stored.default_strategy_id, Some(initial_default));
    assert!(
        !stored
            .strategy_bindings
            .iter()
            .any(|binding| binding.scope == scope)
    );
    assert!(
        !stored
            .placement_overrides
            .iter()
            .any(|record| record.subject == vec![0xab, 0xcd])
    );
}

#[tokio::test]
async fn node_placement_lifecycle() {
    let directory = tempdir().unwrap();
    let context = context(directory.path().to_str().unwrap());
    let realm_id = RealmId::from_bytes([12; 32]);
    let actor = actor(realm_id);
    seed_config(&context, &actor).await;
    let entry = NodePlacementEntry {
        node_id: node(2),
        location: "eu-west".to_string(),
        weight: 250,
        full: false,
        draining: false,
        labels: BTreeMap::new(),
    };

    let stored = mutate(
        &context,
        &actor,
        RealmPlacementMutation::UpsertNode(entry.clone()),
    )
    .await
    .unwrap();
    assert_eq!(stored.placement_entry(entry.node_id), Some(&entry));

    let stored = mutate(
        &context,
        &actor,
        RealmPlacementMutation::RemoveNode(entry.node_id),
    )
    .await
    .unwrap();
    assert!(stored.placement_entry(entry.node_id).is_none());
}

#[tokio::test]
async fn rejects_reserved_label() {
    let directory = tempdir().unwrap();
    let context = context(directory.path().to_str().unwrap());
    let realm_id = RealmId::from_bytes([13; 32]);
    let actor = actor(realm_id);
    seed_config(&context, &actor).await;
    let entry = NodePlacementEntry {
        node_id: node(2),
        location: String::new(),
        weight: DEFAULT_NODE_WEIGHT,
        full: false,
        draining: false,
        labels: BTreeMap::from([(
            aruna_core::structs::KIND_LABEL_KEY.to_string(),
            "Server".to_string(),
        )]),
    };

    assert!(matches!(
        mutate(&context, &actor, RealmPlacementMutation::UpsertNode(entry)).await,
        Err(MutateRealmPlacementError::AdminDocumentReducerError(
            AdminDocumentReducerError::ReservedPlacementLabel(_)
        ))
    ));
}

#[tokio::test]
async fn rejects_dangling_refs() {
    let directory = tempdir().unwrap();
    let context = context(directory.path().to_str().unwrap());
    let realm_id = RealmId::from_bytes([4; 32]);
    let actor = actor(realm_id);
    seed_config(&context, &actor).await;
    let missing = Ulid::from_bytes([9; 16]);

    let mut zero = strategy(missing);
    zero.replica_count = Some(0);
    assert!(matches!(
        mutate(
            &context,
            &actor,
            RealmPlacementMutation::UpsertStrategy(zero)
        )
        .await,
        Err(MutateRealmPlacementError::InvalidInput(reason)) if reason.contains("zero")
    ));

    for mutation in [
        RealmPlacementMutation::SetDefaultStrategy(missing),
        RealmPlacementMutation::SetBinding(StrategyBinding {
            scope: BindingScope::Realm,
            strategy_id: missing,
        }),
        RealmPlacementMutation::SetOverride(PlacementOverride {
            subject: vec![1],
            pinned: Vec::new(),
            excluded: Vec::new(),
            strategy_id: Some(missing),
        }),
    ] {
        assert!(matches!(
            mutate(&context, &actor, mutation).await,
            Err(MutateRealmPlacementError::InvalidInput(reason)) if reason.contains("missing strategy")
        ));
    }
}

#[test]
fn shard_count_frozen() {
    // A bucket-space reshape would orphan per-shard activations.
    let realm_id = RealmId::from_bytes([15; 32]);
    let mut document = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    document.seed_default_placement();
    document.snapshot_candidate_map();
    let strategy_id = document.default_strategy_id.unwrap();
    let mut reshaped = document.strategy(&strategy_id).unwrap().clone();
    reshaped.shard_count *= 2;

    assert!(matches!(
        RealmPlacementMutation::UpsertStrategy(reshaped).validate(&document),
        Err(MutateRealmPlacementError::InvalidInput(reason)) if reason.contains("shard_count")
    ));

    // Selector edits without a shard_count change stay allowed.
    let mut edited = document.strategy(&strategy_id).unwrap().clone();
    edited.replica_count = Some(1);
    assert_eq!(
        RealmPlacementMutation::UpsertStrategy(edited).validate(&document),
        Ok(())
    );
}

#[test]
fn family_strategy_frozen() {
    // Removing or reshaping the family strategy would re-route every
    // retained v1 family record, so both are refused outright.
    let realm_id = RealmId::from_bytes([16; 32]);
    let mut document = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    document.seed_default_placement();
    let strategy_id = document.job_family_strategy_id;
    let mut reshaped = document.strategy(&strategy_id).unwrap().clone();
    reshaped.shard_count *= 2;

    assert_eq!(
        RealmPlacementMutation::UpsertStrategy(reshaped).validate(&document),
        Err(MutateRealmPlacementError::JobFamilyImmutable { strategy_id })
    );
    assert_eq!(
        RealmPlacementMutation::RemoveStrategy(strategy_id).validate(&document),
        Err(MutateRealmPlacementError::JobFamilyImmutable { strategy_id })
    );

    // Holder movement under the same strategy stays allowed.
    let mut edited = document.strategy(&strategy_id).unwrap().clone();
    edited.replica_count = Some(1);
    assert_eq!(
        RealmPlacementMutation::UpsertStrategy(edited).validate(&document),
        Ok(())
    );
}

#[test]
fn group_reuses_realm() {
    let realm_id = RealmId::from_bytes([14; 32]);
    let mut document = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    document.seed_default_placement();
    let strategy_id = document.default_strategy_id.unwrap();
    let mutation = RealmPlacementMutation::SetBinding(StrategyBinding {
        scope: BindingScope::Group(Ulid::generate()),
        strategy_id,
    });

    assert_eq!(mutation.validate(&document), Ok(()));
}

#[tokio::test]
async fn referenced_strategy_conflicts() {
    let directory = tempdir().unwrap();
    let context = context(directory.path().to_str().unwrap());
    let realm_id = RealmId::from_bytes([5; 32]);
    let actor = actor(realm_id);
    let document = seed_config(&context, &actor).await;
    let strategy_id = document.default_strategy_id.unwrap();

    assert_eq!(
        mutate(
            &context,
            &actor,
            RealmPlacementMutation::RemoveStrategy(strategy_id)
        )
        .await,
        Err(MutateRealmPlacementError::StrategyReferenced { strategy_id })
    );
}

#[tokio::test]
async fn materialized_reference_blocks() {
    let directory = tempdir().unwrap();
    let context = context(directory.path().to_str().unwrap());
    let realm_id = RealmId::from_bytes([14; 32]);
    let actor = actor(realm_id);
    seed_config(&context, &actor).await;
    let strategy_id = Ulid::from_bytes([14; 16]);
    mutate(
        &context,
        &actor,
        RealmPlacementMutation::UpsertStrategy(strategy(strategy_id)),
    )
    .await
    .unwrap();
    let event = create_event(&actor, strategy_id, 31);
    write_entries(&context, registry_write_entries(&event.record).unwrap()).await;

    assert_eq!(
        mutate(
            &context,
            &actor,
            RealmPlacementMutation::RemoveStrategy(strategy_id)
        )
        .await,
        Err(MutateRealmPlacementError::StrategyReferenced { strategy_id })
    );
}

#[tokio::test]
async fn pending_reference_blocks() {
    let directory = tempdir().unwrap();
    let context = context(directory.path().to_str().unwrap());
    let realm_id = RealmId::from_bytes([15; 32]);
    let actor = actor(realm_id);
    seed_config(&context, &actor).await;
    let strategy_id = Ulid::from_bytes([15; 16]);
    mutate(
        &context,
        &actor,
        RealmPlacementMutation::UpsertStrategy(strategy(strategy_id)),
    )
    .await
    .unwrap();
    let event = create_event(&actor, strategy_id, 41);
    write_entries(&context, create_projection_entries(&event).unwrap()).await;

    assert_eq!(
        mutate(
            &context,
            &actor,
            RealmPlacementMutation::RemoveStrategy(strategy_id)
        )
        .await,
        Err(MutateRealmPlacementError::StrategyReferenced { strategy_id })
    );
}

#[tokio::test]
async fn missing_config_absent() {
    let directory = tempdir().unwrap();
    let context = context(directory.path().to_str().unwrap());
    let realm_id = RealmId::from_bytes([11; 32]);
    let actor = actor(realm_id);

    assert_eq!(
        mutate(
            &context,
            &actor,
            RealmPlacementMutation::RemoveOverride(Vec::new())
        )
        .await,
        Err(MutateRealmPlacementError::RealmConfigNotFound)
    );
}

#[test]
fn mutation_schedules_revalidation() {
    let realm_id = RealmId::from_bytes([6; 32]);
    let actor = actor(realm_id);
    let mut operation = MutateRealmPlacementOperation::new(MutateRealmPlacementConfig {
        actor: actor.clone(),
        mutation: RealmPlacementMutation::RemoveOverride(Vec::new()),
    });
    operation.state = MutateRealmPlacementState::ScheduleDocumentSyncOutboxDrain;

    let effects = operation.step(Event::Task(TaskEvent::TimerScheduled {
        key: TaskKey::DrainDocumentSyncOutbox,
        after: std::time::Duration::ZERO,
    }));

    assert!(matches!(
        effects.as_slice(),
        [Effect::Task(TaskEffect::ResetTimer {
            key: TaskKey::SyncPlacements {
                realm_id: scheduled_realm,
                node_id,
            },
            after,
        })] if *scheduled_realm == realm_id && *node_id == actor.node_id && after.is_zero()
    ));
}

async fn seed_placement_config(
    context: &DriverContext,
    actor: &Actor,
    nodes: &[aruna_core::NodeId],
    replica: Option<u32>,
) -> RealmConfigDocument {
    let mut document = RealmConfigDocument::new(actor.realm_id, Vec::new(), 3);
    document.seed_default_placement();
    let default_id = document.default_strategy_id.unwrap();
    for strategy in document.strategies.iter_mut() {
        if strategy.strategy_id == default_id {
            strategy.replica_count = replica;
        }
    }
    for node_id in nodes {
        document.ensure_node(*node_id, RealmNodeKind::Server);
    }
    // The issuing actor must be Management, or admission rejects it.
    document.ensure_node(actor.node_id, RealmNodeKind::Management);
    let target = DocumentSyncTarget::RealmConfig {
        realm_id: actor.realm_id,
    };
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key(),
            value: document.to_bytes(actor).unwrap().into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
    document
}

fn draining_entry(node_id: aruna_core::NodeId) -> NodePlacementEntry {
    NodePlacementEntry {
        node_id,
        location: String::new(),
        weight: DEFAULT_NODE_WEIGHT,
        full: false,
        draining: true,
        labels: BTreeMap::new(),
    }
}

#[test]
fn draining_change_rejected() {
    let node_id = node(1);
    // Selection inputs stay frozen on transition and later draining upserts.
    for already_draining in [false, true] {
        let mut document = RealmConfigDocument::new(RealmId::from_bytes([24; 32]), Vec::new(), 3);
        let mut current = draining_entry(node_id);
        current.draining = already_draining;
        document.placement_map.push(current);
        let mut changed = draining_entry(node_id);
        changed.weight = 0;

        assert!(matches!(
            RealmPlacementMutation::UpsertNode(changed).validate(&document),
            Err(MutateRealmPlacementError::InvalidInput(reason))
                if reason.contains("draining freezes")
        ));
    }
}

fn placed_entry(node_id: aruna_core::NodeId) -> NodePlacementEntry {
    NodePlacementEntry {
        node_id,
        location: "eu-west".to_string(),
        weight: 42,
        full: false,
        draining: false,
        labels: BTreeMap::from([("tier".to_string(), "hot".to_string())]),
    }
}

fn placed_document(entry: NodePlacementEntry) -> RealmConfigDocument {
    let mut document = RealmConfigDocument::new(RealmId::from_bytes([26; 32]), Vec::new(), 3);
    document.placement_map.push(entry);
    document
}

fn set_attributes(
    node_id: aruna_core::NodeId,
    location: Option<&str>,
    labels: Option<BTreeMap<String, String>>,
) -> RealmPlacementMutation {
    RealmPlacementMutation::SetNodeAttributes {
        node_id,
        location: location.map(str::to_string),
        labels,
    }
}

#[test]
fn merges_node_attributes() {
    // An absent field keeps its stored value; a present one replaces it.
    let document = placed_document(placed_entry(node(1)));
    let mutation = set_attributes(node(1), Some(" us-east "), None);
    mutation.validate(&document).unwrap();

    let Ok(AdminDocumentOperation::RealmConfigNodePlacementSet { entry }) =
        mutation.admin_operation(&document)
    else {
        panic!("attribute edit reduces to a placement entry write");
    };
    assert_eq!(entry.location, "us-east");
    assert_eq!(entry.labels, placed_entry(node(1)).labels);
    assert_eq!(entry.weight, placed_entry(node(1)).weight);
}

#[test]
fn advances_subject() {
    // A changed attribute advances the node's storage subject generation,
    // which is what makes it revalidate; the stored values leave it alone.
    let current = placed_entry(node(1));
    let document = placed_document(current.clone());
    let record = aruna_core::structs::NodeSubjectRecord::seed(
        aruna_core::structs::storage_subject(&current, 1),
    )
    .unwrap();

    let entry_of = |mutation: RealmPlacementMutation| match mutation.admin_operation(&document) {
        Ok(AdminDocumentOperation::RealmConfigNodePlacementSet { entry }) => entry,
        other => panic!("unexpected reduction: {other:?}"),
    };
    let unchanged = entry_of(set_attributes(
        node(1),
        Some("eu-west"),
        Some(current.labels.clone()),
    ));
    assert_eq!(
        record
            .advance(aruna_core::structs::storage_subject(&unchanged, 1))
            .unwrap(),
        None
    );

    let moved = entry_of(set_attributes(node(1), Some("us-east"), None));
    let advanced = record
        .advance(aruna_core::structs::storage_subject(&moved, 1))
        .unwrap()
        .expect("a moved node advertises a new subject");
    assert_eq!(advanced.subject.generation, 2);
    assert!(advanced.serving_blocked);
}

#[test]
fn rejects_unknown_node() {
    let document = placed_document(placed_entry(node(1)));
    assert!(matches!(
        set_attributes(node(2), Some("us-east"), None).validate(&document),
        Err(MutateRealmPlacementError::InvalidInput(reason))
            if reason.contains("no placement entry")
    ));
}

#[test]
fn rejects_derived_label() {
    // Derived labels are stamped from the entry itself and never set here.
    let document = placed_document(placed_entry(node(1)));
    let labels = BTreeMap::from([(
        "aruna-engine.org/location".to_string(),
        "forged".to_string(),
    )]);
    assert!(matches!(
        set_attributes(node(1), None, Some(labels)).validate(&document),
        Err(MutateRealmPlacementError::InvalidInput(reason))
            if reason.contains("derived")
    ));
}

fn label_map(count: usize) -> BTreeMap<String, String> {
    (0..count)
        .map(|index| (format!("tier-{index}"), "hot".to_string()))
        .collect()
}

#[test]
fn rejects_label_count() {
    // The derived location label counts too, so the cap is reached one
    // operator label early on a node that declares a location.
    let document = placed_document(placed_entry(node(1)));
    assert!(matches!(
        set_attributes(node(1), None, Some(label_map(33))).validate(&document),
        Err(MutateRealmPlacementError::InvalidInput(_))
    ));
    assert!(
        set_attributes(node(1), None, Some(label_map(31)))
            .validate(&document)
            .is_ok()
    );
}

#[test]
fn rejects_blank_label() {
    let document = placed_document(placed_entry(node(1)));
    let labels = BTreeMap::from([("   ".to_string(), "hot".to_string())]);
    assert!(matches!(
        set_attributes(node(1), None, Some(labels)).validate(&document),
        Err(MutateRealmPlacementError::InvalidInput(_))
    ));
}

#[test]
fn rejects_colliding_labels() {
    // Two keys that trim to one are ambiguous rather than merged.
    let document = placed_document(placed_entry(node(1)));
    let labels = BTreeMap::from([
        ("tier".to_string(), "hot".to_string()),
        (" tier".to_string(), "cold".to_string()),
    ]);
    assert!(matches!(
        set_attributes(node(1), None, Some(labels)).validate(&document),
        Err(MutateRealmPlacementError::InvalidInput(_))
    ));
}

#[test]
fn accepts_valid_change() {
    let document = placed_document(placed_entry(node(1)));
    assert!(
        set_attributes(node(1), Some("us-east"), None)
            .validate(&document)
            .is_ok()
    );
    assert!(
        set_attributes(node(1), None, Some(label_map(2)))
            .validate(&document)
            .is_ok()
    );
}

#[test]
fn upsert_rejects_labels() {
    // Onboarding writes the joiner's entry, so its labels are bounded here
    // as well; the reducer only refuses derived keys.
    let document = placed_document(placed_entry(node(1)));
    let mut entry = placed_entry(node(2));
    entry.labels = label_map(33);
    assert!(matches!(
        RealmPlacementMutation::UpsertNode(entry).validate(&document),
        Err(MutateRealmPlacementError::InvalidInput(_))
    ));
}

#[test]
fn rejects_long_location() {
    let document = placed_document(placed_entry(node(1)));
    assert!(matches!(
        set_attributes(node(1), Some(&"x".repeat(65)), None).validate(&document),
        Err(MutateRealmPlacementError::InvalidInput(_))
    ));
}

#[test]
fn rejects_draining_edit() {
    let mut entry = placed_entry(node(1));
    entry.draining = true;
    let document = placed_document(entry);
    assert!(matches!(
        set_attributes(node(1), Some("us-east"), None).validate(&document),
        Err(MutateRealmPlacementError::InvalidInput(reason))
            if reason.contains("draining freezes")
    ));
}

#[test]
fn unmapped_drain_allowed() {
    let document = RealmConfigDocument::new(RealmId::from_bytes([25; 32]), Vec::new(), 3);
    // Resolver defaults make a draining-only upsert valid for an unmapped node.
    assert!(
        RealmPlacementMutation::UpsertNode(draining_entry(node(1)))
            .validate(&document)
            .is_ok()
    );
}

#[tokio::test]
async fn overlapping_transition_allowed() {
    let directory = tempdir().unwrap();
    let context = context(directory.path().to_str().unwrap());
    let realm_id = RealmId::from_bytes([22; 32]);
    let actor = actor(realm_id);
    // Replica two with two nodes: every shard holds both, so draining one
    // leaves the other as an overlapping holder.
    seed_placement_config(&context, &actor, &[node(1), node(2)], Some(2)).await;

    let result = mutate(
        &context,
        &actor,
        RealmPlacementMutation::UpsertNode(draining_entry(node(1))),
    )
    .await;
    assert!(
        result.is_ok(),
        "overlap-preserving change rejected: {result:?}"
    );
}

#[tokio::test]
async fn rejects_empty_holders() {
    let directory = tempdir().unwrap();
    let context = context(directory.path().to_str().unwrap());
    let realm_id = RealmId::from_bytes([23; 32]);
    let actor = actor(realm_id);
    let document = seed_placement_config(&context, &actor, &[node(1), node(2)], Some(2)).await;
    let default_id = document.default_strategy_id.unwrap();
    // Refilter the referenced default strategy onto a label no node carries:
    // its shards resolve to zero holders while both nodes stay usable.
    let filtered = PlacementStrategy {
        strategy_id: default_id,
        name: "default".to_string(),
        replica_count: Some(2),
        distinct_locations: false,
        affinity: vec![AffinityRule {
            matcher: LabelMatch {
                key: "tier".to_string(),
                value: "hot".to_string(),
            },
            effect: AffinityEffect::Filter,
        }],
        shard_count: DEFAULT_SHARD_COUNT,
    };

    assert!(matches!(
        mutate(
            &context,
            &actor,
            RealmPlacementMutation::UpsertStrategy(filtered)
        )
        .await,
        Err(MutateRealmPlacementError::EmptyShardHolders { .. })
    ));
}

#[test]
fn accepts_override_only() {
    let realm_id = RealmId::from_bytes([7; 32]);
    let document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    assert!(
        RealmPlacementMutation::SetOverride(PlacementOverride {
            subject: vec![1],
            pinned: Vec::new(),
            excluded: Vec::new(),
            strategy_id: None,
        })
        .validate(&document)
        .is_ok()
    );
}

#[test]
fn preserves_affinity_data() {
    let strategy = PlacementStrategy {
        strategy_id: Ulid::from_bytes([10; 16]),
        name: "affinity".to_string(),
        replica_count: None,
        distinct_locations: false,
        shard_count: 64,
        affinity: vec![aruna_core::structs::AffinityRule {
            matcher: aruna_core::structs::LabelMatch {
                key: "tier".to_string(),
                value: "hot".to_string(),
            },
            effect: aruna_core::structs::AffinityEffect::Multiply { permille: 1500 },
        }],
    };
    let mutation = RealmPlacementMutation::UpsertStrategy(strategy.clone());
    let document = RealmConfigDocument::new(RealmId::from_bytes([7; 32]), Vec::new(), 3);
    assert!(matches!(
        mutation.admin_operation(&document),
        Ok(AdminDocumentOperation::RealmConfigPlacementStrategyUpserted { strategy: stored })
            if stored == strategy
    ));
}

fn placement_binding(realm_id: RealmId, handle: u32, strategy_id: Ulid) -> PlacementBinding {
    PlacementBinding {
        handle: PlacementHandle::new(handle).unwrap(),
        scope: PlacementScope::Realm(realm_id),
        document_class: DocumentClass::MetadataRegistry,
        strategy_id,
        allocator_range_id: Some(Ulid::from_bytes([44; 16])),
        allocated_by: None,
        allocated_at_ms: None,
    }
}

// Removing a strategy still named by an immutable placement binding is a
// StrategyReferenced conflict, like the other reference kinds above.
#[test]
fn binding_blocks_removal() {
    let realm_id = RealmId::from_bytes([14; 32]);
    let strategy_id = Ulid::from_bytes([14; 16]);
    let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    document.strategies.push(strategy(strategy_id));
    document
        .placement_bindings
        .push(placement_binding(realm_id, 1, strategy_id));

    assert_eq!(
        RealmPlacementMutation::RemoveStrategy(strategy_id).validate(&document),
        Err(MutateRealmPlacementError::StrategyReferenced { strategy_id })
    );
}

#[test]
fn binding_requires_strategy() {
    let realm_id = RealmId::from_bytes([31; 32]);
    let document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    let binding = placement_binding(realm_id, 5, Ulid::from_bytes([9; 16]));
    assert!(matches!(
        RealmPlacementMutation::AppendPlacementBinding(binding).validate(&document),
        Err(MutateRealmPlacementError::InvalidInput(reason)) if reason.contains("missing strategy")
    ));
}

#[test]
fn rejects_divergent_rebind() {
    let realm_id = RealmId::from_bytes([32; 32]);
    let strategy_a = Ulid::from_bytes([1; 16]);
    let strategy_b = Ulid::from_bytes([2; 16]);
    let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    document.strategies.push(strategy(strategy_a));
    document.strategies.push(strategy(strategy_b));
    let handle = FIRST_GRANTABLE_HANDLE;
    let mut existing = placement_binding(realm_id, handle, strategy_a);
    existing.allocated_by = Some(node(4));
    existing.allocated_at_ms = Some(1);
    document.placement_handle_ranges.push(HandleRange {
        range_id: Ulid::from_bytes([44; 16]),
        owner: node(4),
        start: handle,
        end: handle + 1024,
    });
    document.placement_bindings.push(existing.clone());

    assert!(
        RealmPlacementMutation::AppendPlacementBinding(existing)
            .validate(&document)
            .is_ok()
    );
    let mut same = placement_binding(realm_id, handle, strategy_a);
    same.allocated_by = Some(node(4));
    same.allocated_at_ms = Some(1);
    same.allocator_range_id = Some(Ulid::from_bytes([77; 16]));
    assert!(matches!(
        RealmPlacementMutation::AppendPlacementBinding(same).validate(&document),
        Err(MutateRealmPlacementError::InvalidInput(reason)) if reason.contains("provenance")
    ));

    let mut divergent = placement_binding(realm_id, handle, strategy_b);
    divergent.allocated_by = Some(node(4));
    divergent.allocated_at_ms = Some(1);
    assert!(matches!(
        RealmPlacementMutation::AppendPlacementBinding(divergent).validate(&document),
        Err(MutateRealmPlacementError::InvalidInput(reason)) if reason.contains("different tuple")
    ));

    let foreign = placement_binding(RealmId::from_bytes([33; 32]), 6, strategy_a);
    assert!(matches!(
        RealmPlacementMutation::AppendPlacementBinding(foreign).validate(&document),
        Err(MutateRealmPlacementError::InvalidInput(reason)) if reason.contains("does not match")
    ));
}

fn transition_document() -> (RealmConfigDocument, Ulid) {
    // Four nodes, one replica: every bucket moves to a disjoint holder when
    // the newest map adds a node, which is what a transition is for.
    let realm_id = RealmId::from_bytes([41; 32]);
    let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 1);
    let strategy_id = Ulid::from_bytes([42; 16]);
    document.strategies.push(PlacementStrategy {
        strategy_id,
        name: "moved".to_string(),
        replica_count: Some(1),
        distinct_locations: false,
        affinity: Vec::new(),
        shard_count: 4,
    });
    document.default_strategy_id = Some(strategy_id);
    for seed in 1..=3u8 {
        document.ensure_node(node(seed), RealmNodeKind::Server);
    }
    document.snapshot_candidate_map();
    document.ensure_node(node(4), RealmNodeKind::Server);
    document.snapshot_candidate_map();
    (document, strategy_id)
}

fn transition_request(strategy_id: Ulid) -> TransitionRequest {
    TransitionRequest {
        transition_id: Ulid::from_bytes([43; 16]),
        strategy_id,
        buckets: Vec::new(),
        target_map_epoch: 2,
        limits: TransitionLimits::default(),
        created_by: node(1),
        created_at_ms: 1,
    }
}

#[test]
fn rejects_plan_mismatch() {
    let (document, strategy_id) = transition_document();
    let plan = plan_transition(&document, transition_request(strategy_id)).unwrap();
    assert_eq!(plan.buckets.len(), 4);
    assert!(
        RealmPlacementMutation::StartTransition(plan.clone())
            .validate(&document)
            .is_ok()
    );

    // A plan naming a holder set this node does not derive never enters.
    let mut forged = plan.clone();
    forged.buckets[0].target_holders = vec![node(1)];
    assert!(matches!(
        RealmPlacementMutation::StartTransition(forged).validate(&document),
        Err(MutateRealmPlacementError::InvalidInput(reason))
            if reason.contains("does not match the resolved holder sets")
    ));

    // One transition per strategy at a time.
    let mut in_flight = document.clone();
    in_flight
        .placement_transitions
        .push(PlacementTransition::new(plan.clone()));
    let mut successor = plan.clone();
    successor.transition_id = Ulid::from_bytes([44; 16]);
    assert!(matches!(
        RealmPlacementMutation::StartTransition(successor).validate(&in_flight),
        Err(MutateRealmPlacementError::TransitionInFlight { transition_id })
            if transition_id == plan.transition_id
    ));
}

fn issuer(realm_id: RealmId, node_id: aruna_core::NodeId) -> Actor {
    Actor {
        node_id,
        user_id: UserId::nil(realm_id),
        realm_id,
    }
}

#[test]
fn authority_needs_management() {
    // Server, User, and unknown issuers must be rejected before any
    // reducer state is touched; Management keeps working.
    let (mut document, strategy_id) = transition_document();
    let plan = plan_transition(&document, transition_request(strategy_id)).unwrap();
    document
        .placement_transitions
        .push(PlacementTransition::new(plan.clone()));
    let realm_id = document.realm_id;
    document.ensure_node(
        node(5),
        RealmNodeKind::User {
            owner: UserId::nil(realm_id),
        },
    );
    document.ensure_node(node(6), RealmNodeKind::Management);

    for mutation in [
        RealmPlacementMutation::PublishCandidateMap(document.freeze_map(3)),
        RealmPlacementMutation::InitializeActivations {
            strategy_id,
            candidate_map_epoch: 2,
        },
        RealmPlacementMutation::StartTransition(plan.clone()),
        RealmPlacementMutation::AbortTransition(plan.transition_id),
        RealmPlacementMutation::ForceFinalizeBucket {
            transition_id: plan.transition_id,
            bucket: plan.buckets[0].bucket,
            at_risk_report: "old holders lost".to_string(),
        },
        RealmPlacementMutation::UpsertStrategy(strategy(Ulid::from_bytes([77; 16]))),
        RealmPlacementMutation::RemoveNode(node(3)),
    ] {
        for rejected in [node(1), node(5), node(9)] {
            assert!(
                matches!(
                    mutation.authorize(&document, &issuer(realm_id, rejected)),
                    Err(MutateRealmPlacementError::Unauthorized { node_id })
                        if node_id == rejected
                ),
                "{rejected} must not originate an authority-moving mutation"
            );
        }
        assert_eq!(
            mutation.authorize(&document, &issuer(realm_id, node(6))),
            Ok(())
        );
    }
}

#[test]
fn reports_require_roles() {
    // A non-Management participant may only report the role its bucket
    // plan names it for, and only for itself.
    let (mut document, strategy_id) = transition_document();
    let plan = plan_transition(&document, transition_request(strategy_id)).unwrap();
    let bucket = plan
        .buckets
        .iter()
        .find(|bucket| bucket.old_holders != bucket.target_holders)
        .expect("the fixture moves at least one bucket")
        .clone();
    document
        .placement_transitions
        .push(PlacementTransition::new(plan.clone()));
    let realm_id = document.realm_id;
    let old = bucket.old_holders[0];
    let target = bucket.target_holders[0];
    let transition_id = plan.transition_id;

    let barrier = |reported_by| RealmPlacementMutation::ReportBarrier {
        transition_id,
        bucket: bucket.bucket,
        reported_by,
        frontier: vec![1],
    };
    assert_eq!(
        barrier(old).authorize(&document, &issuer(realm_id, old)),
        Ok(())
    );
    assert!(
        barrier(target)
            .authorize(&document, &issuer(realm_id, target))
            .is_err()
    );
    // Reporting on another node's behalf is never a self-report.
    assert!(
        barrier(old)
            .authorize(&document, &issuer(realm_id, target))
            .is_err()
    );
    document.ensure_node(node(6), RealmNodeKind::Management);
    assert!(
        barrier(old)
            .authorize(&document, &issuer(realm_id, node(6)))
            .is_err()
    );

    let claim = ProofClaim {
        realm_id,
        transition_id,
        strategy_id,
        bucket: bucket.bucket,
        old_activation_epoch: 1,
        target_map_epoch: 2,
        barrier_digest: [0; 32],
        checkpoint_root: [1; 32],
        holder: target,
    };
    let completion = RealmPlacementMutation::SubmitCompletion {
        transition_id,
        strategy_id,
        proof: claim.sign(&node_secret(&target)),
    };
    assert_eq!(
        completion.authorize(&document, &issuer(realm_id, target)),
        Ok(())
    );
    assert!(
        completion
            .authorize(&document, &issuer(realm_id, old))
            .is_err()
    );

    let drained = |reported_by| RealmPlacementMutation::ReportDrained {
        transition_id,
        bucket: bucket.bucket,
        reported_by,
    };
    assert_eq!(
        drained(old).authorize(&document, &issuer(realm_id, old)),
        Ok(())
    );
    assert!(
        drained(target)
            .authorize(&document, &issuer(realm_id, target))
            .is_err()
    );

    let stall = |reported_by| RealmPlacementMutation::ReportStall {
        transition_id,
        bucket: bucket.bucket,
        reported_by,
        reason: "no source".to_string(),
    };
    for participant in [old, target] {
        assert_eq!(
            stall(participant).authorize(&document, &issuer(realm_id, participant)),
            Ok(())
        );
    }
    let outsider = (1..=9u8)
        .map(node)
        .find(|candidate| {
            !bucket.old_holders.contains(candidate)
                && !bucket.target_holders.contains(candidate)
                && node_kind(&document, *candidate).is_some()
        })
        .expect("the fixture has an uninvolved node");
    assert!(
        stall(outsider)
            .authorize(&document, &issuer(realm_id, outsider))
            .is_err()
    );
}

#[tokio::test]
async fn unauthorized_writes_nothing() {
    let directory = tempdir().unwrap();
    let context = context(directory.path().to_str().unwrap());
    let realm_id = RealmId::from_bytes([46; 32]);
    let management = actor(realm_id);
    let seeded = seed_placement_config(&context, &management, &[node(2)], Some(2)).await;

    let result = mutate(
        &context,
        &issuer(realm_id, node(2)),
        RealmPlacementMutation::PublishCandidateMap(seeded.freeze_map(1)),
    )
    .await;

    assert!(matches!(
        result,
        Err(MutateRealmPlacementError::Unauthorized { node_id }) if node_id == node(2)
    ));
    let stored = drive(GetRealmConfigOperation::new(realm_id), &context)
        .await
        .expect("the realm config survives a rejected mutation");
    assert!(stored.candidate_maps.is_empty());
    assert!(
        crate::sync::document_outbox::read_outbox_tails(&context.storage_handle)
            .await
            .expect("outbox scan")
            .is_empty()
    );
}

#[test]
fn force_requires_proof() {
    let (mut document, strategy_id) = transition_document();
    let plan = plan_transition(&document, transition_request(strategy_id)).unwrap();
    let bucket = plan.buckets[0].bucket;
    let holder = plan.buckets[0].target_holders[0];
    document
        .placement_transitions
        .push(PlacementTransition::new(plan.clone()));
    let force = RealmPlacementMutation::ForceFinalizeBucket {
        transition_id: plan.transition_id,
        bucket,
        at_risk_report: "old holders lost".to_string(),
    };

    assert!(matches!(
        force.validate(&document),
        Err(MutateRealmPlacementError::ForceWithoutProof { bucket: forced, .. })
            if forced == bucket
    ));

    let secret = node_secret(&holder);
    document.placement_transitions[0].proofs.push(
        ProofClaim {
            realm_id: document.realm_id,
            transition_id: plan.transition_id,
            strategy_id,
            bucket,
            old_activation_epoch: 1,
            target_map_epoch: 2,
            barrier_digest: [0; 32],
            checkpoint_root: [1; 32],
            holder,
        }
        .sign(&secret),
    );
    assert_eq!(force.validate(&document), Ok(()));
}

#[test]
fn completion_must_verify() {
    let (mut document, strategy_id) = transition_document();
    let plan = plan_transition(&document, transition_request(strategy_id)).unwrap();
    let bucket = plan.buckets[0].bucket;
    let holder = plan.buckets[0].target_holders[0];
    document
        .placement_transitions
        .push(PlacementTransition::new(plan.clone()));
    let claim = ProofClaim {
        realm_id: document.realm_id,
        transition_id: plan.transition_id,
        strategy_id,
        bucket,
        old_activation_epoch: 1,
        target_map_epoch: 2,
        barrier_digest: [0; 32],
        checkpoint_root: [1; 32],
        holder,
    };
    let submit = |proof| RealmPlacementMutation::SubmitCompletion {
        transition_id: plan.transition_id,
        strategy_id,
        proof,
    };

    assert_eq!(
        submit(claim.sign(&node_secret(&holder))).validate(&document),
        Ok(())
    );

    // Signed by the wrong key, and aimed at a bucket the plan does not cover.
    let other = (1..=4u8)
        .map(node)
        .find(|candidate| *candidate != holder)
        .expect("the fixture has more than one node");
    assert!(matches!(
        submit(claim.sign(&node_secret(&other))).validate(&document),
        Err(MutateRealmPlacementError::InvalidInput(reason))
            if reason.contains("does not verify")
    ));
    let mut off_plan = claim;
    off_plan.bucket = 9;
    assert!(matches!(
        submit(off_plan.sign(&node_secret(&holder))).validate(&document),
        Err(MutateRealmPlacementError::InvalidInput(reason))
            if reason.contains("does not cover bucket")
    ));
}

fn auth(actor: &Actor) -> AuthContext {
    AuthContext {
        user_id: actor.user_id,
        realm_id: actor.realm_id,
        path_restrictions: None,
        session: None,
    }
}

fn placement_config(actor: &Actor) -> MutateRealmPlacementConfig {
    MutateRealmPlacementConfig {
        actor: actor.clone(),
        mutation: RealmPlacementMutation::SetDefaultStrategy(Ulid::from_bytes([2; 16])),
    }
}

#[test]
fn authorized_checks_permission() {
    let realm_id = RealmId::from_bytes([1; 32]);
    let actor = actor(realm_id);
    let mut operation =
        MutateRealmPlacementOperation::authorized(placement_config(&actor), auth(&actor));
    let effects = operation.start();
    assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
    let emitted = format!("{effects:?}");
    assert!(emitted.contains(&policy_admin_path(realm_id)));
    assert!(emitted.contains("WRITE"));
}

#[test]
fn denied_is_terminal() {
    let realm_id = RealmId::from_bytes([1; 32]);
    let actor = actor(realm_id);
    let mut operation =
        MutateRealmPlacementOperation::authorized(placement_config(&actor), auth(&actor));
    operation.start();
    let effects = operation.step(Event::SubOperation(
        SubOperationEvent::AuthorizationResult { allowed: Ok(false) },
    ));
    assert!(effects.is_empty());
    assert!(operation.is_complete());
    assert_eq!(
        operation.finalize(),
        Err(MutateRealmPlacementError::Unauthorized {
            node_id: actor.node_id,
        })
    );
}

#[test]
fn capacity_not_denied() {
    // Storage exhaustion inside the check is infrastructure, not a verdict.
    let realm_id = RealmId::from_bytes([1; 32]);
    let actor = actor(realm_id);
    let mut operation =
        MutateRealmPlacementOperation::authorized(placement_config(&actor), auth(&actor));
    operation.start();
    operation.step(Event::SubOperation(
        SubOperationEvent::AuthorizationResult {
            allowed: Err(AuthorizationError::StorageError(
                StorageError::CleanupCapacity,
            )),
        },
    ));
    assert!(operation.is_complete());
    assert_eq!(
        operation.finalize(),
        Err(MutateRealmPlacementError::StorageError(
            StorageError::CleanupCapacity
        ))
    );
}

#[test]
fn internal_skips_authorization() {
    let realm_id = RealmId::from_bytes([1; 32]);
    let actor = actor(realm_id);
    let mut operation = MutateRealmPlacementOperation::new(placement_config(&actor));
    assert_eq!(
        operation.start().as_slice(),
        &[Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    );
}

#[test]
fn refuses_server_node() {
    // An authorized caller still may not mutate placement on a server node.
    let realm_id = RealmId::from_bytes([1; 32]);
    let actor = actor(realm_id);
    let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    document.ensure_node(actor.node_id, RealmNodeKind::Server);
    let mut operation =
        MutateRealmPlacementOperation::authorized(placement_config(&actor), auth(&actor));
    operation.start();
    operation.step(Event::SubOperation(
        SubOperationEvent::AuthorizationResult { allowed: Ok(true) },
    ));
    operation.step(Event::Storage(StorageEvent::TransactionStarted {
        txn_id: Ulid::from_bytes([7; 16]),
    }));
    operation.step(Event::Storage(StorageEvent::BatchReadResult {
        values: vec![
            (
                Key::from(vec![0u8]),
                Some(document.to_bytes(&actor).unwrap().into()),
            ),
            (Key::from(vec![1u8]), None),
        ],
    }));
    assert_eq!(
        operation.finalize(),
        Err(MutateRealmPlacementError::Unauthorized {
            node_id: actor.node_id,
        })
    );
}
