use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use aruna_core::UserId;
use aruna_core::document::{
    DocumentSyncChange, DocumentSyncChangeKind, DocumentSyncPublish, DocumentSyncRevision,
    DocumentSyncTarget, shard_topic_id,
};
use aruna_core::effects::{Effect, NetEffect, StorageEffect};
use aruna_core::events::{Event, NetEvent, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    METADATA_EVENT_LOG_KEYSPACE, METADATA_HOLDERS_KEYSPACE, METADATA_INDEX_KEYSPACE,
    REALM_CONFIG_KEYSPACE,
};
use aruna_core::metadata::{
    MetadataCreateEventPayload, MetadataCreateEventRecord, MetadataDocumentDeleteRecord,
    MetadataDocumentLifecycleRecord, MetadataEffect, MetadataEvent, MetadataGraphLifecycleRecord,
};
use aruna_core::storage_entries::{
    metadata_create_event_write_entry, metadata_document_lifecycle_revision_change,
    metadata_event_log_key, metadata_registry_key,
};
use aruna_core::structs::{
    Actor, MetadataRegistryRecord, NodePlacementEntry, PlacementRef, RealmConfigDocument, RealmId,
    RealmNodeKind, shard_for_subject,
};
use aruna_core::util::unix_timestamp_millis;
use aruna_core::{DocumentSyncEffect, DocumentSyncNetEvent};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::announce_realm_presence::{
    AnnounceRealmPresenceConfig, AnnounceRealmPresenceOperation,
};
use aruna_operations::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentOperation, CreateMetadataDocumentPayload,
};
use aruna_operations::delete_metadata_document::DeleteMetadataDocumentOperation;
use aruna_operations::document_sync_outbox::read_outbox_records;
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::get_metadata_document::GetMetadataDocumentOperation;
use aruna_operations::get_realm_config::GetRealmConfigOperation;
use aruna_operations::get_realm_nodes::GetRealmNodesOperation;
use aruna_operations::incoming::initialize_net_incoming;
use aruna_operations::metadata::MetadataHandle;
use aruna_operations::metadata::projector::{
    project_metadata_create_events, replay_metadata_event_log,
};
use aruna_operations::mutate_realm_placement::{
    MutateRealmPlacementConfig, MutateRealmPlacementOperation, RealmPlacementMutation,
};
use aruna_operations::placement::{
    PlacementResolutionContext, choose_origin_bucket, held_buckets, resolve_shard_holders,
    strategy_for_target, subject_bytes,
};
use aruna_operations::sync_placement::sort_node_ids;
use aruna_operations::task_incoming::{drive_document_sync_outbox_drain, initialize_task_incoming};
use aruna_operations::update_metadata_document::{
    UpdateMetadataDocumentConfig, UpdateMetadataDocumentMutation, UpdateMetadataDocumentOperation,
};
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use tempfile::TempDir;
use tokio::time::{Instant, sleep};
use ulid::Ulid;

// Every wait below polls to a condition; the ceiling only bounds a genuine
// hang. Post-replan convergence measures ~1s (registry row) to ~10s (event
// log behind a holder-transition graph sync) under tenfold contention, but a
// loaded CI runner can stall consecutive peer syncs for the full 30s peer-sync
// timeout each, so the backstop is 2-3x that timeout, not the expected latency.
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(120);

struct TestNode {
    _temp_dir: TempDir,
    net: NetHandle,
    context: Arc<DriverContext>,
}

#[tokio::test]
async fn metadata_creation_replicates_to_all_three_holders()
-> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([41u8; 32]);
    let (nodes, _config) = build_realm_nodes(&realm_id, 3).await?;
    let group_id = Ulid::r#gen();
    let document_id = Ulid::r#gen();

    let visible_nodes = drive(
        GetRealmNodesOperation::new(realm_id),
        nodes[0].context.as_ref(),
    )
    .await?;
    assert_eq!(visible_nodes.len(), 3);

    let created = drive(
        CreateMetadataDocumentOperation::new(CreateMetadataDocumentConfig {
            actor: Actor {
                node_id: nodes[0].net.node_id(),
                user_id: UserId::local(Ulid::r#gen(), realm_id),
                realm_id,
            },
            group_id,
            document_id,
            document_path: "datasets/bootstrap".to_string(),
            public: true,
            payload: CreateMetadataDocumentPayload::Scaffold {
                name: "Bootstrap Dataset".to_string(),
                description: "Replicated metadata".to_string(),
                date_published: "2026-01-01".to_string(),
                license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
            },
        }),
        nodes[0].context.as_ref(),
    )
    .await?
    .record;

    assert_eq!(created.holder_node_ids, vec![nodes[0].net.node_id()]);
    // Replay is idempotent: it projects the logged create event unless the
    // async drain (and the holders' expansion round trip) already did, so the
    // stable invariant is the logged event itself, not the projection count.
    assert!(replay_metadata_event_log(nodes[0].context.as_ref()).await? <= 1);
    assert!(
        read_metadata_event_log_value(&nodes[0], document_id, created.last_event_id)
            .await?
            .is_some()
    );

    wait_for_metadata_convergence(&nodes, group_id, document_id, &created.graph_iri).await?;
    shutdown_nodes(nodes).await;
    Ok(())
}

#[tokio::test]
async fn replan_reaches_replacement() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([46u8; 32]);
    let (nodes, _config) = build_realm_nodes(&realm_id, 4).await?;
    let group_id = Ulid::r#gen();
    let document_id = Ulid::r#gen();
    let document_path = "datasets/replan-holder-refresh";

    let created = drive(
        CreateMetadataDocumentOperation::new(CreateMetadataDocumentConfig {
            actor: Actor {
                node_id: nodes[0].net.node_id(),
                user_id: UserId::local(Ulid::r#gen(), realm_id),
                realm_id,
            },
            group_id,
            document_id,
            document_path: document_path.to_string(),
            public: true,
            payload: CreateMetadataDocumentPayload::Scaffold {
                name: "Replan Holder Refresh".to_string(),
                description: "Replacement holder index convergence".to_string(),
                date_published: "2026-01-01".to_string(),
                license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
            },
        }),
        nodes[0].context.as_ref(),
    )
    .await?
    .record;
    // See metadata_creation_replicates_to_all_three_holders: the projection
    // count races the async drain, the logged event is the stable invariant.
    assert!(replay_metadata_event_log(nodes[0].context.as_ref()).await? <= 1);
    assert!(
        read_metadata_event_log_value(&nodes[0], document_id, created.last_event_id)
            .await?
            .is_some()
    );

    let initial_config = drive(
        GetRealmConfigOperation::new(realm_id),
        nodes[0].context.as_ref(),
    )
    .await?;
    // The bucket was chosen by the origin at create; holders derive from it.
    let placement = created.placement;
    let mut initial_holders = resolve_shard_holders(&initial_config, &placement);
    sort_node_ids(&mut initial_holders);
    assert!(initial_holders.contains(&nodes[0].net.node_id()));
    wait_for_persisted_holder_set(
        &nodes,
        &initial_holders,
        group_id,
        document_id,
        &initial_holders,
    )
    .await?;

    drive(
        UpdateMetadataDocumentOperation::new(UpdateMetadataDocumentConfig {
            actor: Actor {
                node_id: nodes[0].net.node_id(),
                user_id: UserId::local(Ulid::r#gen(), realm_id),
                realm_id,
            },
            group_id,
            document_id,
            public: true,
            mutation: UpdateMetadataDocumentMutation::UpsertDataEntity {
                jsonld: r#"{"@id":"./latest.txt","@type":"File","name":"latest.txt"}"#.to_string(),
            },
        }),
        nodes[0].context.as_ref(),
    )
    .await?;
    let (latest_registry, _) = read_persisted_holder_set(&nodes[0], group_id, document_id)
        .await?
        .expect("origin persisted latest metadata update");
    let latest_update_id = latest_registry.last_event_id;

    let obsolete = initial_holders[0];
    let updated_config = drive(
        MutateRealmPlacementOperation::new(MutateRealmPlacementConfig {
            actor: Actor {
                node_id: nodes[0].net.node_id(),
                user_id: UserId::nil(realm_id),
                realm_id,
            },
            mutation: RealmPlacementMutation::UpsertNode(NodePlacementEntry {
                node_id: obsolete,
                location: String::new(),
                weight: 100,
                full: false,
                draining: true,
                labels: Default::default(),
            }),
        }),
        nodes[0].context.as_ref(),
    )
    .await?;
    let mut final_holders = resolve_shard_holders(&updated_config, &placement);
    sort_node_ids(&mut final_holders);
    let replacement = final_holders
        .iter()
        .copied()
        .find(|node_id| !initial_holders.contains(node_id))
        .expect("replan selects a replacement holder");

    let live_holders = resolve_shard_holders(&updated_config, &placement);
    assert!(live_holders.contains(&replacement));
    assert!(!live_holders.contains(&obsolete));

    let replacement_node = nodes
        .iter()
        .find(|node| node.net.node_id() == replacement)
        .expect("replacement fixture node exists");
    // holder_node_ids is an event-time stamp; freshness is the shard guarantee (#395).
    let registry =
        wait_for_persisted_update(replacement_node, group_id, document_id, latest_update_id)
            .await?;
    assert_eq!(registry.last_event_id, latest_update_id);
    // The registry row rides the everywhere-bound registry class, so it can land
    // on the replacement before the document's own bucket topic delivers the
    // event: the row is a routing pointer, not evidence the content arrived.
    let refreshed_event =
        wait_for_event_log_value(replacement_node, document_id, latest_update_id).await?;
    let refreshed_event: MetadataCreateEventRecord = postcard::from_bytes(&refreshed_event)?;
    assert!(matches!(
        refreshed_event.payload,
        MetadataCreateEventPayload::UpsertDataEntity { .. }
    ));

    shutdown_nodes(nodes).await;
    Ok(())
}

// Black-hole ordering, pinned deterministically: the origin's update drain runs
// only AFTER the replan marks the origin draining, so the drain classifies a
// draining former-holder. It must still flush its accepted-before-drain records;
// none is left undeliverable and the update reaches the replacement holder.
#[tokio::test]
async fn flush_after_drain() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([47u8; 32]);
    let (nodes, _config, refreshers) =
        build_pinned_realm(&realm_id, &[11, 12, 13, 14], &[false, true, true, true]).await?;
    let group_id = Ulid::r#gen();
    let document_id = Ulid::r#gen();
    let (placement, initial_holders, update_event_id) = seed_and_update(
        &nodes,
        realm_id,
        group_id,
        document_id,
        "datasets/flush-after-drain",
    )
    .await?;

    // The update's two records sit undrained on the hand-driven origin.
    let pending = read_outbox_records(&nodes[0].context.storage_handle, &[], None, 16).await?;
    assert_eq!(
        pending.records.len(),
        2,
        "update enqueues two outbox records"
    );

    // Drain the origin first, then release its outbox drain into that config. An
    // undeliverable (black-holed) record is retained forever, so a fully emptied
    // outbox is the deterministic proof that none was classified undeliverable.
    let replacement = drain_origin(&nodes, realm_id, &placement, &initial_holders).await?;
    drain_until_empty(&nodes[0]).await?;

    assert_update_reaches(
        &nodes,
        realm_id,
        replacement,
        group_id,
        document_id,
        update_event_id,
        &placement,
    )
    .await?;
    for refresher in refreshers {
        refresher.abort();
    }
    shutdown_nodes(nodes).await;
    Ok(())
}

// Opposite ordering: the origin publishes the update while still a holder, THEN
// it is drained. The replacement pulls the canonical topic history, update
// included, and adopts the original genesis rather than forking one.
#[tokio::test]
async fn publish_before_drain() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([48u8; 32]);
    let (nodes, _config, refreshers) =
        build_pinned_realm(&realm_id, &[21, 22, 23, 24], &[false, true, true, true]).await?;
    let group_id = Ulid::r#gen();
    let document_id = Ulid::r#gen();
    let (placement, initial_holders, update_event_id) = seed_and_update(
        &nodes,
        realm_id,
        group_id,
        document_id,
        "datasets/publish-before-drain",
    )
    .await?;

    // Publish the update while the origin is still a holder.
    drain_until_empty(&nodes[0]).await?;

    // Now drain the origin and flush the config change out.
    let replacement = drain_origin(&nodes, realm_id, &placement, &initial_holders).await?;
    drain_until_empty(&nodes[0]).await?;

    assert_update_reaches(
        &nodes,
        realm_id,
        replacement,
        group_id,
        document_id,
        update_event_id,
        &placement,
    )
    .await?;
    for refresher in refreshers {
        refresher.abort();
    }
    shutdown_nodes(nodes).await;
    Ok(())
}

// Drives the hand-driven node's outbox drain until it is empty. The drain
// deletes a record only once its holders acknowledge the sync, so a one-shot
// push that loses a race under load is retried instead of stranding the record;
// an undeliverable record is retained forever and trips the deadline.
async fn drain_until_empty(node: &TestNode) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    loop {
        drive_document_sync_outbox_drain(node.context.clone()).await;
        let remaining = read_outbox_records(&node.context.storage_handle, &[], None, 16).await?;
        if remaining.records.is_empty() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "outbox never drained: {} records remain",
                remaining.records.len()
            )
            .into());
        }
        sleep(Duration::from_millis(50)).await;
    }
}

// Creates a document on the hand-driven origin (index 0), replicates the create
// to its holders, then commits an update. Returns the bucket placement, the
// initial holder set, and the update's revision id (taken from the returned
// record, which the revision-id fix makes authoritative).
async fn seed_and_update(
    nodes: &[TestNode],
    realm_id: RealmId,
    group_id: Ulid,
    document_id: Ulid,
    document_path: &str,
) -> Result<(PlacementRef, Vec<aruna_core::NodeId>, Ulid), Box<dyn std::error::Error>> {
    let created = drive(
        CreateMetadataDocumentOperation::new(CreateMetadataDocumentConfig {
            actor: Actor {
                node_id: nodes[0].net.node_id(),
                user_id: UserId::local(Ulid::r#gen(), realm_id),
                realm_id,
            },
            group_id,
            document_id,
            document_path: document_path.to_string(),
            public: true,
            payload: CreateMetadataDocumentPayload::Scaffold {
                name: "Draining Flush".to_string(),
                description: "Draining flush regression".to_string(),
                date_published: "2026-01-01".to_string(),
                license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
            },
        }),
        nodes[0].context.as_ref(),
    )
    .await?
    .record;
    // Origin runs no auto loop: project the create into the local index (so the
    // update can read it) and publish it to the holders, both by hand.
    replay_metadata_event_log(nodes[0].context.as_ref()).await?;
    drain_until_empty(&nodes[0]).await?;

    let config = drive(
        GetRealmConfigOperation::new(realm_id),
        nodes[0].context.as_ref(),
    )
    .await?;
    let placement = created.placement;
    let mut initial_holders = resolve_shard_holders(&config, &placement);
    sort_node_ids(&mut initial_holders);
    assert!(initial_holders.contains(&nodes[0].net.node_id()));
    wait_for_persisted_holder_set(
        nodes,
        &initial_holders,
        group_id,
        document_id,
        &initial_holders,
    )
    .await?;
    assert!(
        nodes[0]
            .net
            .document_sync_topic_exists(shard_topic_id(realm_id, &placement))
            .unwrap_or(false),
        "origin holds the shard genesis"
    );

    let updated = drive(
        UpdateMetadataDocumentOperation::new(UpdateMetadataDocumentConfig {
            actor: Actor {
                node_id: nodes[0].net.node_id(),
                user_id: UserId::local(Ulid::r#gen(), realm_id),
                realm_id,
            },
            group_id,
            document_id,
            public: true,
            mutation: UpdateMetadataDocumentMutation::UpsertDataEntity {
                jsonld: r#"{"@id":"./latest.txt","@type":"File","name":"latest.txt"}"#.to_string(),
            },
        }),
        nodes[0].context.as_ref(),
    )
    .await?;
    Ok((placement, initial_holders, updated.last_event_id))
}

// Marks the origin draining and returns the replacement holder the replan pulls
// into the bucket's holder set.
async fn drain_origin(
    nodes: &[TestNode],
    realm_id: RealmId,
    placement: &PlacementRef,
    initial_holders: &[aruna_core::NodeId],
) -> Result<aruna_core::NodeId, Box<dyn std::error::Error>> {
    let obsolete = nodes[0].net.node_id();
    let updated_config = drive(
        MutateRealmPlacementOperation::new(MutateRealmPlacementConfig {
            actor: Actor {
                node_id: nodes[0].net.node_id(),
                user_id: UserId::nil(realm_id),
                realm_id,
            },
            mutation: RealmPlacementMutation::UpsertNode(NodePlacementEntry {
                node_id: obsolete,
                location: String::new(),
                weight: 100,
                full: false,
                draining: true,
                labels: Default::default(),
            }),
        }),
        nodes[0].context.as_ref(),
    )
    .await?;
    let mut final_holders = resolve_shard_holders(&updated_config, placement);
    sort_node_ids(&mut final_holders);
    assert!(
        !final_holders.contains(&obsolete),
        "drained origin left holders"
    );
    final_holders
        .into_iter()
        .find(|node_id| !initial_holders.contains(node_id))
        .ok_or_else(|| "replan selects a replacement holder".into())
}

// Waits for the update to converge on the replacement: the everywhere-bound
// registry pointer, then the bucket's own event content, and confirms the
// replacement adopted the original shard genesis instead of forking one.
async fn assert_update_reaches(
    nodes: &[TestNode],
    realm_id: RealmId,
    replacement: aruna_core::NodeId,
    group_id: Ulid,
    document_id: Ulid,
    update_event_id: Ulid,
    placement: &PlacementRef,
) -> Result<(), Box<dyn std::error::Error>> {
    let replacement_node = nodes
        .iter()
        .find(|node| node.net.node_id() == replacement)
        .ok_or("replacement fixture node exists")?;
    let registry =
        wait_for_persisted_update(replacement_node, group_id, document_id, update_event_id).await?;
    assert_eq!(registry.last_event_id, update_event_id);
    let event = wait_for_event_log_value(replacement_node, document_id, update_event_id).await?;
    let event: MetadataCreateEventRecord = postcard::from_bytes(&event)?;
    assert!(matches!(
        event.payload,
        MetadataCreateEventPayload::UpsertDataEntity { .. }
    ));
    assert!(
        replacement_node
            .net
            .document_sync_topic_exists(shard_topic_id(realm_id, placement))
            .unwrap_or(false),
        "replacement adopted the original genesis, not a fork"
    );
    Ok(())
}

// The document id hashes into a bucket the origin does not hold: before the
// bucket was chosen at create, the origin could never join that bucket's topic
// and the create never replicated.
#[tokio::test]
async fn origin_off_hash_converges() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([47u8; 32]);
    let (nodes, config) = build_realm_nodes(&realm_id, 4).await?;
    let group_id = Ulid::r#gen();
    let document_path = "datasets/off-hash-origin";
    let origin = nodes[0].net.node_id();

    let context = PlacementResolutionContext {
        group_id: Some(group_id),
        metadata_path: Some(document_path),
    };
    let sample = DocumentSyncTarget::MetadataDocumentLifecycle {
        document_id: Ulid::r#gen(),
    };
    let (strategy, _) =
        strategy_for_target(&config, &sample, context).expect("metadata strategy resolves");
    let held = held_buckets(&config, strategy, origin);
    assert!(!held.is_empty() && held.len() < strategy.shard_count as usize);

    let hashed_shard =
        |document_id: Ulid| shard_for_subject(&document_id.to_bytes(), strategy.shard_count);
    let mut document_id = Ulid::r#gen();
    while held.contains(&hashed_shard(document_id)) {
        document_id = Ulid::r#gen();
    }

    let created = drive(
        CreateMetadataDocumentOperation::new(CreateMetadataDocumentConfig {
            actor: Actor {
                node_id: origin,
                user_id: UserId::local(Ulid::r#gen(), realm_id),
                realm_id,
            },
            group_id,
            document_id,
            document_path: document_path.to_string(),
            public: true,
            payload: CreateMetadataDocumentPayload::Scaffold {
                name: "Off Hash Origin".to_string(),
                description: "Created on a node outside the hashed bucket".to_string(),
                date_published: "2026-01-01".to_string(),
                license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
            },
        }),
        nodes[0].context.as_ref(),
    )
    .await?
    .record;

    assert!(!held.contains(&hashed_shard(document_id)));
    assert!(held.contains(&created.placement.shard));

    let mut holders = resolve_shard_holders(&config, &created.placement);
    sort_node_ids(&mut holders);
    assert!(holders.contains(&origin));
    wait_for_persisted_holder_set(&nodes, &holders, group_id, document_id, &holders).await?;

    shutdown_nodes(nodes).await;
    Ok(())
}

#[tokio::test]
async fn metadata_updates_and_deletes_apply_to_local_holder()
-> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([42u8; 32]);
    let (nodes, _config) = build_realm_nodes(&realm_id, 3).await?;
    let group_id = Ulid::r#gen();
    let document_id = Ulid::r#gen();

    let created = drive(
        CreateMetadataDocumentOperation::new(CreateMetadataDocumentConfig {
            actor: Actor {
                node_id: nodes[0].net.node_id(),
                user_id: UserId::local(Ulid::r#gen(), realm_id),
                realm_id,
            },
            group_id,
            document_id,
            document_path: "datasets/propagation".to_string(),
            public: false,
            payload: CreateMetadataDocumentPayload::Scaffold {
                name: "Initial Dataset".to_string(),
                description: "Initial description".to_string(),
                date_published: "2026-01-01".to_string(),
                license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
            },
        }),
        nodes[0].context.as_ref(),
    )
    .await?;

    // See metadata_creation_replicates_to_all_three_holders: the projection
    // count races the async drain, the logged event is the stable invariant.
    assert!(replay_metadata_event_log(nodes[0].context.as_ref()).await? <= 1);
    assert!(
        read_metadata_event_log_value(&nodes[0], document_id, created.record.last_event_id)
            .await?
            .is_some()
    );

    wait_for_metadata_state(
        &nodes,
        group_id,
        document_id,
        &created.record.graph_iri,
        3,
        "Initial Dataset",
    )
    .await?;

    let updated_jsonld = format!(
        r#"{{
  "@context": "https://w3id.org/ro/crate/1.2/context",
  "@graph": [
    {{
      "@id": "ro-crate-metadata.json",
      "@type": "CreativeWork",
      "conformsTo": {{"@id": "https://w3id.org/ro/crate/1.2"}},
      "about": {{"@id": "https://w3id.org/aruna/{document_id}"}}
    }},
    {{
      "@id": "https://w3id.org/aruna/{document_id}",
      "@type": "Dataset",
      "name": "Updated Dataset",
      "description": "Replicated update",
      "datePublished": "2026-02-01",
      "license": {{"@id": "https://creativecommons.org/licenses/by/4.0/"}}
    }}
  ]
}}"#
    );

    let updated = drive(
        UpdateMetadataDocumentOperation::new(UpdateMetadataDocumentConfig {
            actor: Actor {
                node_id: nodes[0].net.node_id(),
                user_id: UserId::local(Ulid::r#gen(), realm_id),
                realm_id,
            },
            group_id,
            document_id,
            public: true,
            mutation: UpdateMetadataDocumentMutation::ReplaceRoCrate {
                jsonld: updated_jsonld,
            },
        }),
        nodes[0].context.as_ref(),
    )
    .await?;
    assert!(updated.public);

    wait_for_metadata_state(
        &nodes,
        group_id,
        document_id,
        &created.record.graph_iri,
        3,
        "Updated Dataset",
    )
    .await?;

    drive(
        DeleteMetadataDocumentOperation::new(
            Actor {
                node_id: nodes[0].net.node_id(),
                user_id: UserId::local(Ulid::r#gen(), realm_id),
                realm_id,
            },
            group_id,
            document_id,
        ),
        nodes[0].context.as_ref(),
    )
    .await?;

    wait_for_metadata_absence(&nodes, group_id, document_id, &created.record.graph_iri).await?;
    shutdown_nodes(nodes).await;
    Ok(())
}

#[tokio::test]
async fn batched_metadata_create_projection_materializes_many_documents()
-> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([43u8; 32]);
    let nodes = vec![spawn_node(realm_id).await?];
    let config = install_realm_config(&nodes, &realm_id).await?;
    let node = &nodes[0];
    let group_id = Ulid::r#gen();
    let mut events = Vec::new();

    for index in 0..8u8 {
        let document_id = Ulid::r#gen();
        let now = unix_timestamp_millis().saturating_add(index.into());
        let document_path = format!("datasets/batch-{index}");
        let graph_iri = MetadataRegistryRecord::graph_iri_for(document_id);
        let target = DocumentSyncTarget::MetadataDocumentLifecycle { document_id };
        let (strategy, _) = strategy_for_target(
            &config,
            &target,
            PlacementResolutionContext {
                group_id: Some(group_id),
                metadata_path: Some(document_path.as_str()),
            },
        )
        .expect("metadata strategy resolves");
        let placement = choose_origin_bucket(
            &config,
            strategy,
            node.net.node_id(),
            &subject_bytes(&target),
        )
        .expect("origin holds a bucket");
        let record = MetadataRegistryRecord {
            realm_id,
            group_id,
            document_id,
            document_path: document_path.clone(),
            graph_iri,
            public: true,
            permission_path: MetadataRegistryRecord::permission_path_for(
                &realm_id,
                group_id,
                &document_path,
                document_id,
            ),
            placement,
            holder_node_ids: vec![node.net.node_id()],
            created_at_ms: now,
            updated_at_ms: now,
            last_event_id: Ulid::nil(),
        };
        events.push(MetadataCreateEventRecord {
            event_id: Ulid::r#gen(),
            record,
            user_id: UserId::local(Ulid::r#gen(), realm_id),
            node_id: node.net.node_id(),
            payload: MetadataCreateEventPayload::Scaffold {
                name: format!("Batch Dataset {index}"),
                description: "Projected from one metadata batch".to_string(),
                date_published: "2026-01-01".to_string(),
                license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
            },
            occurred_at_ms: now,
        });
    }

    let writes = events
        .iter()
        .map(metadata_create_event_write_entry)
        .collect::<Result<Vec<_>, _>>()?;
    match node
        .context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { entries }) => {
            assert_eq!(entries.len(), events.len())
        }
        other => return Err(format!("unexpected metadata event batch write: {other:?}").into()),
    }

    let projected = project_metadata_create_events(
        node.context.as_ref(),
        events.clone(),
        Some(node.net.node_id()),
    )
    .await?;
    assert_eq!(projected, events.len());

    wait_for_batched_metadata_projection(node, group_id, &events).await?;
    shutdown_nodes(nodes).await;
    Ok(())
}

#[tokio::test]
async fn metadata_delete_wins_when_stale_create_arrives_after_tombstone()
-> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([44u8; 32]);
    let (nodes, realm_config) = build_realm_nodes(&realm_id, 2).await?;
    let group_id = Ulid::r#gen();
    let document_id = Ulid::r#gen();
    let document_path = "datasets/reordered-delete";
    let event_id = Ulid::r#gen();
    let graph_iri = MetadataRegistryRecord::graph_iri_for(document_id);
    let lifecycle_target = DocumentSyncTarget::MetadataDocumentLifecycle { document_id };
    let placement = aruna_operations::placement::placement_ref_for_target(
        &realm_config,
        &lifecycle_target,
        Default::default(),
    );
    let record = MetadataRegistryRecord {
        realm_id,
        group_id,
        document_id,
        document_path: document_path.to_string(),
        graph_iri: graph_iri.clone(),
        public: true,
        permission_path: MetadataRegistryRecord::permission_path_for(
            &realm_id,
            group_id,
            document_path,
            document_id,
        ),
        placement,
        holder_node_ids: vec![nodes[0].net.node_id(), nodes[1].net.node_id()],
        created_at_ms: 1,
        updated_at_ms: 1,
        last_event_id: event_id,
    };
    let create_event = MetadataCreateEventRecord {
        event_id,
        record: record.clone(),
        user_id: UserId::local(Ulid::r#gen(), realm_id),
        node_id: nodes[0].net.node_id(),
        payload: MetadataCreateEventPayload::Scaffold {
            name: "Reordered Delete".to_string(),
            description: "Stale create follows tombstone".to_string(),
            date_published: "2026-01-01".to_string(),
            license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
        },
        occurred_at_ms: 1,
    };
    let tombstone =
        MetadataGraphLifecycleRecord::deleted(graph_iri, realm_id, group_id, document_id, 2);
    let delete_event_id = Ulid::r#gen();
    let lifecycle = MetadataDocumentLifecycleRecord::Delete {
        event: MetadataDocumentDeleteRecord {
            event_id: delete_event_id,
            tombstone,
            deleted_after_event_id: event_id,
        },
    };
    // All records of this document ride the bucket stamped on the record, so
    // one shard topic. Its genesis exists on both nodes after install with no
    // manual bootstrap: rank-0 created it, and the other holder pulled it in
    // its own placement pass. The publisher below only joins it, never creates.
    assert_ne!(placement, PlacementRef::NIL);
    let shard_topic_of = |target: &DocumentSyncTarget| target.sync_topic_id(realm_id, &placement);
    assert!(
        nodes[0]
            .net
            .document_sync_topic_exists(shard_topic_of(&lifecycle_target))?,
        "shard topic genesis unavailable on both nodes after install"
    );
    publish_document_to_peer(
        &nodes[0],
        delete_event_id,
        lifecycle_target.clone(),
        postcard::to_allocvec(&lifecycle)?,
        nodes[1].net.node_id(),
        placement,
    )
    .await?;
    nodes[1]
        .net
        .sync_document_topic_with_peers(
            shard_topic_of(&lifecycle_target),
            vec![nodes[0].net.node_id()],
        )
        .await?;
    let delete_result = nodes[1]
        .net
        .reconcile_document_sync_topics(vec![shard_topic_of(&lifecycle_target)])
        .await?;
    assert_eq!(delete_result.metadata_create_events.len(), 0);

    let stale_target = DocumentSyncTarget::MetadataCreateEvent {
        document_id,
        event_id,
    };
    publish_document_to_peer(
        &nodes[0],
        Ulid::r#gen(),
        stale_target.clone(),
        postcard::to_allocvec(&create_event)?,
        nodes[1].net.node_id(),
        placement,
    )
    .await?;
    nodes[1]
        .net
        .sync_document_topic_with_peers(shard_topic_of(&stale_target), vec![nodes[0].net.node_id()])
        .await?;
    let stale_result = nodes[1]
        .net
        .reconcile_document_sync_topics(vec![shard_topic_of(&stale_target)])
        .await?;

    assert!(stale_result.metadata_create_events.is_empty());
    assert!(
        read_metadata_event_log_value(&nodes[1], document_id, event_id)
            .await?
            .is_none()
    );
    let fetched = drive(
        GetMetadataDocumentOperation::new(group_id, document_id),
        nodes[1].context.as_ref(),
    )
    .await;
    assert!(fetched.is_err());
    shutdown_nodes(nodes).await;
    Ok(())
}

async fn build_realm_nodes(
    realm_id: &RealmId,
    count: usize,
) -> Result<(Vec<TestNode>, RealmConfigDocument), Box<dyn std::error::Error>> {
    let mut nodes = Vec::with_capacity(count);
    for _ in 0..count {
        nodes.push(spawn_node(*realm_id).await?);
    }
    let config = finish_realm_setup(&nodes, realm_id).await?;
    Ok((nodes, config))
}

// Fixed identities and per-node task-handler control: pinning node ids makes
// holder roles a chosen schedule, and withholding the auto task loop from a node
// lets a test drive that node's outbox drain by hand at a precise boundary.
async fn build_pinned_realm(
    realm_id: &RealmId,
    secret_seeds: &[u8],
    start_tasks: &[bool],
) -> Result<
    (
        Vec<TestNode>,
        RealmConfigDocument,
        Vec<tokio::task::JoinHandle<()>>,
    ),
    Box<dyn std::error::Error>,
> {
    let mut nodes = Vec::with_capacity(secret_seeds.len());
    for (index, seed) in secret_seeds.iter().enumerate() {
        let secret = iroh::SecretKey::from_bytes(&[*seed; 32]);
        nodes.push(spawn_node_configured(*realm_id, Some(secret), start_tasks[index]).await?);
    }
    // A node with no auto loop cannot refresh its own realm presence, which
    // expires after REALM_PRESENCE_TTL; keep it alive with a background
    // re-announce so setup convergence never stalls on an expired presence.
    let mut refreshers = Vec::new();
    for (index, node) in nodes.iter().enumerate() {
        if start_tasks[index] {
            continue;
        }
        let context = node.context.clone();
        let node_id = node.net.node_id();
        let realm_id = *realm_id;
        refreshers.push(tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(5)).await;
                let _ = drive(
                    AnnounceRealmPresenceOperation::new(AnnounceRealmPresenceConfig {
                        realm_id,
                        node_id,
                        schedule_refresh: false,
                    }),
                    context.as_ref(),
                )
                .await;
            }
        }));
    }
    let config = finish_realm_setup(&nodes, realm_id).await?;
    Ok((nodes, config, refreshers))
}

async fn finish_realm_setup(
    nodes: &[TestNode],
    realm_id: &RealmId,
) -> Result<RealmConfigDocument, Box<dyn std::error::Error>> {
    for i in 0..nodes.len() {
        for j in (i + 1)..nodes.len() {
            nodes[i]
                .net
                .add_peer_addr(nodes[j].net.endpoint_addr())
                .await;
            nodes[j]
                .net
                .add_peer_addr(nodes[i].net.endpoint_addr())
                .await;
        }
    }

    for node in nodes {
        drive(
            AnnounceRealmPresenceOperation::new(AnnounceRealmPresenceConfig {
                realm_id: *realm_id,
                node_id: node.net.node_id(),
                schedule_refresh: true,
            }),
            node.context.as_ref(),
        )
        .await?;
    }

    wait_for_realm_node_convergence(nodes, realm_id).await?;
    install_realm_config(nodes, realm_id).await
}

async fn spawn_node(realm_id: RealmId) -> Result<TestNode, Box<dyn std::error::Error>> {
    spawn_node_configured(realm_id, None, true).await
}

async fn spawn_node_configured(
    realm_id: RealmId,
    secret_key: Option<iroh::SecretKey>,
    start_tasks: bool,
) -> Result<TestNode, Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let storage = FjallStorage::open(temp_dir.path().to_str().ok_or("invalid temp path")?)?;
    let net = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().expect("valid bind addr"),
            realm_id,
            secret_key,
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        storage.clone(),
    )
    .await?;
    let task_handle = TaskHandle::new();
    let metadata_handle = MetadataHandle::new(
        temp_dir.path().join("metadata"),
        net.node_id(),
        storage.clone(),
        Some(net.clone()),
        Some(net.document_sync_node()),
        Some(net.document_sync_database()),
    )?;

    let context = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: Some(net.clone()),
        blob_handle: None,
        metadata_handle: Some(metadata_handle),
        task_handle: Some(task_handle.clone()),
    });

    initialize_net_incoming(context.clone());
    // A node without the auto task loop leaves its outbox drain (and every other
    // timer) for the test to drive by hand.
    if start_tasks {
        initialize_task_incoming(context.clone(), task_handle).await;
    }

    Ok(TestNode {
        _temp_dir: temp_dir,
        net,
        context,
    })
}

async fn install_realm_config(
    nodes: &[TestNode],
    realm_id: &RealmId,
) -> Result<RealmConfigDocument, Box<dyn std::error::Error>> {
    let mut config = RealmConfigDocument::default_for_realm(*realm_id, Vec::new());
    config.seed_default_placement();
    for node in nodes {
        config.ensure_node(node.net.node_id(), RealmNodeKind::Management);
    }

    for node in nodes {
        let actor = Actor {
            node_id: node.net.node_id(),
            user_id: UserId::nil(*realm_id),
            realm_id: *realm_id,
        };
        let bytes = config.to_bytes(&actor)?;
        match node
            .context
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Write {
                key_space: REALM_CONFIG_KEYSPACE.to_string(),
                key: (*realm_id.as_bytes()).into(),
                value: bytes.into(),
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => return Err(format!("unexpected realm config write event: {other:?}").into()),
        }
        node.net.refresh_realm_peers_from_document(&config).await?;
    }
    // Startup hook, exactly as the binary runs it after loading the config: it
    // joins the shared realm topics (RealmConfig among them, so a later placement
    // change actually reaches the other nodes) and reconciles the held shard
    // topics. A node whose rank-0 co-holder has not created a genesis yet leaves
    // it for the next pass, so run until quiescent instead of waiting out the
    // production retry timer.
    for _ in 0..3 {
        for node in nodes {
            aruna_operations::startup::restore_shard_subscriptions(
                &node.context,
                node.net.node_id(),
                *realm_id,
            )
            .await;
        }
        let mut retry = false;
        for node in nodes {
            retry |= aruna_operations::process_placements::process_shard_placements(
                &node.context,
                *realm_id,
                node.net.node_id(),
            )
            .await
            .retry_scheduled;
        }
        if !retry {
            break;
        }
    }

    Ok(config)
}

async fn publish_document_to_peer(
    node: &TestNode,
    event_id: Ulid,
    target: DocumentSyncTarget,
    bytes: Vec<u8>,
    peer: aruna_core::NodeId,
    placement: aruna_core::structs::PlacementRef,
) -> Result<(), Box<dyn std::error::Error>> {
    match node
        .net
        .send_effect(Effect::Net(NetEffect::DocumentSync(
            DocumentSyncEffect::PublishDocuments {
                documents: vec![DocumentSyncPublish::Upsert {
                    event_id,
                    target: target.clone(),
                    change: document_change_for_publish(
                        node.net.node_id(),
                        &target,
                        &bytes,
                        placement,
                    )?,
                    bytes,
                    allow_genesis: true,
                }],
                peers: vec![peer],
            },
        )))
        .await
    {
        Event::Net(NetEvent::DocumentSync(DocumentSyncNetEvent::DocumentsPublished {
            targets,
        })) => {
            assert_eq!(targets, vec![target]);
            Ok(())
        }
        other => Err(format!("unexpected publish event: {other:?}").into()),
    }
}

fn document_change_for_publish(
    node_id: aruna_core::NodeId,
    target: &DocumentSyncTarget,
    bytes: &[u8],
    placement: aruna_core::structs::PlacementRef,
) -> Result<DocumentSyncChange, Box<dyn std::error::Error>> {
    match target {
        DocumentSyncTarget::MetadataDocumentLifecycle { document_id } => {
            let lifecycle: MetadataDocumentLifecycleRecord = postcard::from_bytes(bytes)?;
            if lifecycle.document_id() != *document_id {
                return Err("metadata document lifecycle target mismatch".into());
            }
            Ok(metadata_document_lifecycle_revision_change(
                &lifecycle, node_id, placement,
            ))
        }
        DocumentSyncTarget::MetadataCreateEvent {
            document_id,
            event_id,
        } => {
            let record: MetadataCreateEventRecord = postcard::from_bytes(bytes)?;
            if record.record.document_id != *document_id || record.event_id != *event_id {
                return Err("metadata create-event target mismatch".into());
            }
            Ok(DocumentSyncChange {
                base: None,
                current: DocumentSyncRevision {
                    generation: record.record.updated_at_ms,
                    event_id: record.event_id,
                    actor: record.node_id,
                    updated_at_ms: record.occurred_at_ms,
                },
                kind: DocumentSyncChangeKind::Upsert,
                placement,
            })
        }
        _ => Err("unsupported test publish target".into()),
    }
}

async fn read_metadata_event_log_value(
    node: &TestNode,
    document_id: Ulid,
    event_id: Ulid,
) -> Result<Option<aruna_core::types::Value>, Box<dyn std::error::Error>> {
    match node
        .context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: METADATA_EVENT_LOG_KEYSPACE.to_string(),
            key: metadata_event_log_key(document_id, event_id),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(format!("unexpected event log read event: {other:?}").into()),
    }
}

async fn read_persisted_holder_set(
    node: &TestNode,
    group_id: Ulid,
    document_id: Ulid,
) -> Result<Option<(MetadataRegistryRecord, Vec<aruna_core::NodeId>)>, Box<dyn std::error::Error>> {
    let key = metadata_registry_key(group_id, document_id);
    match node
        .context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (METADATA_INDEX_KEYSPACE.to_string(), key.clone()),
                (METADATA_HOLDERS_KEYSPACE.to_string(), key),
            ],
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::BatchReadResult { values }) => {
            let [(_, registry), (_, holders)] = values.as_slice() else {
                return Err(
                    format!("unexpected metadata index read count: {}", values.len()).into(),
                );
            };
            match (registry, holders) {
                (Some(registry), Some(holders)) => Ok(Some((
                    postcard::from_bytes(registry)?,
                    postcard::from_bytes(holders)?,
                ))),
                (None, None) => Ok(None),
                _ => Err("metadata registry and holder indexes diverged".into()),
            }
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(format!("unexpected metadata index read event: {other:?}").into()),
    }
}

async fn wait_for_event_log_value(
    node: &TestNode,
    document_id: Ulid,
    event_id: Ulid,
) -> Result<aruna_core::types::Value, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    loop {
        if let Some(value) = read_metadata_event_log_value(node, document_id, event_id).await? {
            return Ok(value);
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "node={} never persisted metadata event {event_id} of document {document_id}",
                node.net.node_id()
            )
            .into());
        }
        sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_persisted_update(
    node: &TestNode,
    group_id: Ulid,
    document_id: Ulid,
    expected_event_id: Ulid,
) -> Result<MetadataRegistryRecord, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    loop {
        let last = match read_persisted_holder_set(node, group_id, document_id).await? {
            Some((registry, _)) if registry.last_event_id == expected_event_id => {
                return Ok(registry);
            }
            Some((registry, _)) => format!("last_event_id={}", registry.last_event_id),
            None => "missing".to_string(),
        };
        if Instant::now() >= deadline {
            return Err(format!(
                "replacement did not converge to update {expected_event_id}: {last}"
            )
            .into());
        }
        sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_persisted_holder_set(
    nodes: &[TestNode],
    node_ids: &[aruna_core::NodeId],
    group_id: Ulid,
    document_id: Ulid,
    expected_holders: &[aruna_core::NodeId],
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    let mut last = Vec::new();
    loop {
        let mut converged = true;
        last.clear();
        for node_id in node_ids {
            let node = nodes
                .iter()
                .find(|node| node.net.node_id() == *node_id)
                .ok_or("holder fixture node missing")?;
            match read_persisted_holder_set(node, group_id, document_id).await? {
                Some((registry, holders))
                    if same_holder_set(&registry.holder_node_ids, expected_holders)
                        && same_holder_set(&holders, expected_holders) =>
                {
                    last.push(format!("{node_id}=converged"));
                }
                Some((registry, holders)) => {
                    last.push(format!(
                        "{node_id}=registry:{:?},holders:{holders:?}",
                        registry.holder_node_ids
                    ));
                    converged = false;
                }
                None => {
                    last.push(format!("{node_id}=missing"));
                    converged = false;
                }
            }
        }
        if converged {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "metadata holder indexes did not converge to {expected_holders:?}: {last:?}"
            )
            .into());
        }
        sleep(Duration::from_millis(50)).await;
    }
}

fn same_holder_set(left: &[aruna_core::NodeId], right: &[aruna_core::NodeId]) -> bool {
    left.len() == right.len()
        && left.iter().copied().collect::<HashSet<_>>()
            == right.iter().copied().collect::<HashSet<_>>()
}

async fn wait_for_realm_node_convergence(
    nodes: &[TestNode],
    realm_id: &RealmId,
) -> Result<(), Box<dyn std::error::Error>> {
    let expected: HashSet<_> = nodes.iter().map(|node| node.net.node_id()).collect();
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;

    loop {
        let mut converged = true;
        for node in nodes {
            match drive(
                GetRealmNodesOperation::new(*realm_id),
                node.context.as_ref(),
            )
            .await
            {
                Ok(realm_nodes) if realm_nodes == expected => {}
                _ => {
                    converged = false;
                    break;
                }
            }
        }

        if converged {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("realm nodes did not converge".into());
        }
        sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_metadata_convergence(
    nodes: &[TestNode],
    group_id: Ulid,
    document_id: Ulid,
    graph_iri: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    wait_for_metadata_state(
        nodes,
        group_id,
        document_id,
        graph_iri,
        nodes.len(),
        "Bootstrap Dataset",
    )
    .await
}

async fn wait_for_metadata_state(
    nodes: &[TestNode],
    group_id: Ulid,
    document_id: Ulid,
    graph_iri: &str,
    expected_holder_count: usize,
    expected_text: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    let expected_holders = nodes
        .iter()
        .map(|node| node.net.node_id())
        .collect::<HashSet<_>>();
    let mut last_states = Vec::new();

    loop {
        let mut converged = true;
        last_states.clear();

        for node in nodes {
            if !converged {
                break;
            }
            match drive(
                GetMetadataDocumentOperation::new(group_id, document_id),
                node.context.as_ref(),
            )
            .await
            {
                Ok(document)
                    if document.record.graph_iri == graph_iri
                        && document.record.holder_node_ids.len() == expected_holder_count
                        && document
                            .record
                            .holder_node_ids
                            .iter()
                            .copied()
                            .collect::<HashSet<_>>()
                            == expected_holders
                        && document.jsonld.contains(expected_text) =>
                {
                    last_states.push(format!("node={} converged", node.net.node_id()));
                }
                Ok(document) => {
                    last_states.push(format!(
                        "node={} graph={} holders={} jsonld_contains={}",
                        node.net.node_id(),
                        document.record.graph_iri,
                        document.record.holder_node_ids.len(),
                        document.jsonld.contains(expected_text)
                    ));
                    converged = false;
                    break;
                }
                Err(error) => {
                    let graph_state = match node
                        .context
                        .metadata_handle
                        .as_ref()
                        .unwrap()
                        .send_effect(Effect::Metadata(MetadataEffect::ExportRoCrate {
                            graph_iri: graph_iri.to_string(),
                        }))
                        .await
                    {
                        Event::Metadata(MetadataEvent::RoCrateExportResult { .. }) => {
                            "graph-present"
                        }
                        Event::Metadata(MetadataEvent::Error { .. }) => "graph-missing",
                        _ => "graph-unknown",
                    };
                    last_states.push(format!(
                        "node={} error={error:?} {graph_state}",
                        node.net.node_id()
                    ));
                    converged = false;
                    break;
                }
            }
        }

        if converged {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("metadata state did not converge: {last_states:?}").into());
        }
        sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_metadata_absence(
    nodes: &[TestNode],
    group_id: Ulid,
    document_id: Ulid,
    graph_iri: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    let mut last_states = Vec::new();

    loop {
        let mut converged = true;
        last_states.clear();

        for node in nodes {
            if !converged {
                break;
            }
            match drive(
                GetMetadataDocumentOperation::new(group_id, document_id),
                node.context.as_ref(),
            )
            .await
            {
                Err(error) => {
                    let graph_state = match node
                        .context
                        .metadata_handle
                        .as_ref()
                        .unwrap()
                        .send_effect(Effect::Metadata(MetadataEffect::ExportRoCrate {
                            graph_iri: graph_iri.to_string(),
                        }))
                        .await
                    {
                        Event::Metadata(MetadataEvent::Error { .. }) => "graph-missing",
                        _ => "graph-present",
                    };
                    if graph_state != "graph-missing" {
                        last_states.push(format!(
                            "node={} error={error:?} graph still present",
                            node.net.node_id()
                        ));
                        converged = false;
                        break;
                    }
                    last_states.push(format!("node={} absent", node.net.node_id()));
                }
                Ok(_) => {
                    last_states.push(format!(
                        "node={} document still present",
                        node.net.node_id()
                    ));
                    converged = false;
                    break;
                }
            }
        }

        if converged {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("metadata deletion did not converge: {last_states:?}").into());
        }
        sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_batched_metadata_projection(
    node: &TestNode,
    group_id: Ulid,
    events: &[MetadataCreateEventRecord],
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    let mut last_states = Vec::new();

    loop {
        let mut converged = true;
        last_states.clear();

        for event in events {
            let document_id = event.record.document_id;
            let expected_name = match &event.payload {
                MetadataCreateEventPayload::Scaffold { name, .. } => name.as_str(),
                _ => "",
            };
            match drive(
                GetMetadataDocumentOperation::new(group_id, document_id),
                node.context.as_ref(),
            )
            .await
            {
                Ok(document)
                    if document.record.graph_iri == event.record.graph_iri
                        && document.record.holder_node_ids == vec![node.net.node_id()]
                        && document.jsonld.contains(expected_name) => {}
                Ok(document) => {
                    last_states.push(format!(
                        "document={} holders={} jsonld_contains={}",
                        document_id,
                        document.record.holder_node_ids.len(),
                        document.jsonld.contains(expected_name)
                    ));
                    converged = false;
                    break;
                }
                Err(error) => {
                    last_states.push(format!("document={document_id} error={error:?}"));
                    converged = false;
                    break;
                }
            }
        }

        if converged {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "batched metadata projection did not materialize: {last_states:?}"
            )
            .into());
        }
        sleep(Duration::from_millis(50)).await;
    }
}

async fn shutdown_nodes(nodes: Vec<TestNode>) {
    for node in nodes {
        node.net.shutdown().await;
    }
}
