use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use aruna_core::admin_document_reducer::AdminDocumentReducerState;
use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::document::{DocumentSyncPublish, DocumentSyncTarget, PendingDocumentPlacement};
use aruna_core::effects::{Effect, NetEffect, StorageEffect};
use aruna_core::events::{Event, NetEvent, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::metadata::{MetadataCreateEventPayload, MetadataCreateEventRecord};
use aruna_core::shutdown::Shutdown;
use aruna_core::structs::{
    Actor, AffinityEffect, AffinityRule, LabelMatch, MetadataRegistryRecord, NodePlacementEntry,
    NodeUrls, RealmConfigDocument, RealmId, RealmNodeKind,
};
use aruna_core::{DocumentSyncEffect, DocumentSyncNetEvent};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::get_realm_config::GetRealmConfigOperation;
use aruna_operations::incoming::initialize_net_incoming;
use aruna_operations::mutate_realm_placement::{
    MutateRealmPlacementConfig, MutateRealmPlacementOperation, RealmPlacementMutation,
};
use aruna_operations::node_info::{read_node_info_document, seed_node_info_document};
use aruna_operations::placement::{
    PlacementResolutionContext, build_view, plan_target_placement, resolve_holders,
};
use aruna_operations::replicate_documents::{
    ReplicateDocumentsConfig, ReplicateDocumentsOperation,
};
use aruna_operations::sync_placement::{decode_placement, placement_key};
use aruna_operations::task_incoming::initialize_task_incoming;
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use tempfile::TempDir;
use tokio::time::{Instant, sleep};
use ulid::Ulid;

const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(60);

struct TestNode {
    _temp_dir: TempDir,
    net: NetHandle,
    context: Arc<DriverContext>,
}

// #261 Definition of Done: placement policy converges over the real admin topic,
// every node derives the same weighted multi-location holder sets, and a durable
// completed inventory is revalidated when that policy changes.
#[tokio::test]
async fn placement_policy_converges_and_replans_completed_inventory()
-> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([61u8; 32]);
    let nodes = build_realm_nodes(&realm_id, 4).await?;
    let actor = test_actor(&nodes[0], realm_id);
    let group_id = Ulid::from_bytes([71; 16]);
    let document_id = Ulid::from_bytes([72; 16]);
    let event_id = Ulid::from_bytes([73; 16]);
    let target = DocumentSyncTarget::MetadataCreateEvent {
        document_id,
        event_id,
    };
    write_metadata_create_event(
        &nodes[0],
        realm_id,
        group_id,
        document_id,
        event_id,
        &target,
    )
    .await?;

    drive(
        ReplicateDocumentsOperation::new(ReplicateDocumentsConfig {
            realm_id,
            local_node_id: nodes[0].net.node_id(),
            excluded_peers: Vec::new(),
            documents: vec![target.clone()],
            allow_genesis: true,
        }),
        nodes[0].context.as_ref(),
    )
    .await?;

    let initial_config = drive(
        GetRealmConfigOperation::new(realm_id),
        nodes[0].context.as_ref(),
    )
    .await?;
    assert_weighted_distinct_resolution(&nodes, &initial_config);
    let mut expected_initial = plan_target_placement(
        &initial_config,
        &target,
        PlacementResolutionContext::default(),
    )
    .expect("default placement strategy should plan the document")
    .holders;
    sort_node_ids(&mut expected_initial);
    let initial = wait_for_placement(&nodes[0], realm_id, &target, |record| {
        record.desired_holder_count == 2 && record.selected_holders == expected_initial
    })
    .await?;

    let obsolete = initial.selected_holders[0];
    let old_entry = initial_config
        .placement_entry(obsolete)
        .expect("selected holder should have a placement entry");
    drive(
        MutateRealmPlacementOperation::new(MutateRealmPlacementConfig {
            actor: actor.clone(),
            mutation: RealmPlacementMutation::UpsertNode(NodePlacementEntry {
                draining: true,
                ..old_entry.clone()
            }),
        }),
        nodes[0].context.as_ref(),
    )
    .await?;

    let mut expanded_strategy = initial_config
        .strategy(
            &initial_config
                .default_strategy_id
                .expect("default strategy should be configured"),
        )
        .expect("default strategy should resolve")
        .clone();
    expanded_strategy.replica_count = Some(3);
    drive(
        MutateRealmPlacementOperation::new(MutateRealmPlacementConfig {
            actor,
            mutation: RealmPlacementMutation::UpsertStrategy(expanded_strategy.clone()),
        }),
        nodes[0].context.as_ref(),
    )
    .await?;

    let configs = wait_for_policy_convergence(&nodes, realm_id, obsolete, 3).await?;
    assert_placement_state_identical(&configs);
    assert_resolve_holders_identical(&configs);
    let mut expected_final =
        plan_target_placement(&configs[0], &target, PlacementResolutionContext::default())
            .expect("updated placement strategy should plan the document")
            .holders;
    sort_node_ids(&mut expected_final);
    assert_eq!(expected_final.len(), 3);
    assert!(!expected_final.contains(&obsolete));

    let final_record = wait_for_placement(&nodes[0], realm_id, &target, |record| {
        record.desired_holder_count == 3 && record.selected_holders == expected_final
    })
    .await?;
    assert_eq!(
        final_record.placement.strategy_id,
        expanded_strategy.strategy_id
    );
    assert!(!final_record.selected_holders.contains(&obsolete));
    assert!(
        final_record
            .selected_holders
            .iter()
            .any(|holder| !initial.selected_holders.contains(holder)),
        "policy change should add replacement/top-up holders"
    );
    for holder in &final_record.selected_holders {
        let node = nodes
            .iter()
            .find(|node| node.net.node_id() == *holder)
            .expect("final holder should be a fixture node");
        wait_for_document(node, &target).await?;
    }

    shutdown_nodes(nodes).await;
    Ok(())
}

#[tokio::test]
async fn shared_node_info_topic_propagates_placement_authoritative_document()
-> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([67u8; 32]);
    let nodes = build_realm_nodes(&realm_id, 2).await?;
    let publisher = &nodes[0];
    let peer = &nodes[1];
    let publisher_id = publisher.net.node_id();
    let urls = NodeUrls {
        api: Some("https://api.node-a.example.test".to_string()),
        s3: Some("https://s3.node-a.example.test".to_string()),
    };

    seed_node_info_document(
        publisher.context.as_ref(),
        publisher_id,
        realm_id,
        urls.clone(),
    )
    .await
    .map_err(std::io::Error::other)?;
    drive(
        ReplicateDocumentsOperation::new(ReplicateDocumentsConfig {
            realm_id,
            local_node_id: publisher_id,
            excluded_peers: Vec::new(),
            documents: vec![DocumentSyncTarget::NodeInfo {
                realm_id,
                node_id: publisher_id,
            }],
            allow_genesis: true,
        }),
        publisher.context.as_ref(),
    )
    .await?;

    let expected_labels = build_view(
        &drive(
            GetRealmConfigOperation::new(realm_id),
            publisher.context.as_ref(),
        )
        .await?,
    )
    .nodes
    .into_iter()
    .find(|node| node.node_id == publisher_id)
    .expect("publisher should be present in placement view")
    .labels;
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    let received = loop {
        if let Some(document) = read_node_info_document(&peer.context.storage_handle, publisher_id)
            .await
            .map_err(std::io::Error::other)?
        {
            break document;
        }
        if Instant::now() >= deadline {
            shutdown_nodes(nodes).await;
            return Err("node info document did not propagate over the shared topic".into());
        }
        sleep(Duration::from_millis(50)).await;
    };

    assert_eq!(received.node_id, publisher_id);
    assert_eq!(received.labels, expected_labels);
    assert_eq!(received.labels.get("tier").map(String::as_str), Some("hot"));
    assert_eq!(received.urls, urls);
    assert_eq!(received.utilization.storage_bytes_used, 0);
    assert_eq!(received.utilization.documents_held, None);
    assert_eq!(received.utilization.load_permille, None);
    assert!(received.utilization.heartbeat_at_ms > 0);
    assert!(received.updated_at_ms > 0);

    shutdown_nodes(nodes).await;
    Ok(())
}

fn test_actor(node: &TestNode, realm_id: RealmId) -> Actor {
    Actor {
        node_id: node.net.node_id(),
        user_id: aruna_core::UserId::nil(realm_id),
        realm_id,
    }
}

async fn write_metadata_create_event(
    node: &TestNode,
    realm_id: RealmId,
    group_id: Ulid,
    document_id: Ulid,
    event_id: Ulid,
    target: &DocumentSyncTarget,
) -> Result<(), Box<dyn std::error::Error>> {
    let document_path = "datasets/issue-261";
    let record = MetadataCreateEventRecord {
        event_id,
        record: MetadataRegistryRecord {
            realm_id,
            group_id,
            document_id,
            document_path: document_path.to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: false,
            permission_path: MetadataRegistryRecord::permission_path_for(
                &realm_id,
                group_id,
                document_path,
                document_id,
            ),
            holder_node_ids: vec![node.net.node_id()],
            created_at_ms: 1,
            updated_at_ms: 1,
            last_event_id: event_id,
        },
        user_id: aruna_core::UserId::local(Ulid::from_bytes([74; 16]), realm_id),
        node_id: node.net.node_id(),
        payload: MetadataCreateEventPayload::Scaffold {
            name: "Issue 261 placement".to_string(),
            description: "Integration placement fixture".to_string(),
            date_published: "2026-07-10".to_string(),
            license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
        },
        occurred_at_ms: 1,
    };
    match node
        .context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key(),
            value: postcard::to_allocvec(&record)?.into(),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        other => Err(format!("unexpected metadata create-event write: {other:?}").into()),
    }
}

async fn read_placement(
    node: &TestNode,
    realm_id: RealmId,
    target: &DocumentSyncTarget,
) -> Result<Option<PendingDocumentPlacement>, Box<dyn std::error::Error>> {
    match node
        .context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: aruna_core::keyspaces::SYNC_PLACEMENT_KEYSPACE.to_string(),
            key: placement_key(realm_id, target),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value
            .map(|value| decode_placement(value.as_ref()))
            .transpose()?),
        other => Err(format!("unexpected placement read event: {other:?}").into()),
    }
}

async fn wait_for_placement(
    node: &TestNode,
    realm_id: RealmId,
    target: &DocumentSyncTarget,
    predicate: impl Fn(&PendingDocumentPlacement) -> bool,
) -> Result<PendingDocumentPlacement, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    let mut last_record = None;
    loop {
        if let Some(record) = read_placement(node, realm_id, target).await? {
            if predicate(&record) {
                return Ok(record);
            }
            last_record = Some(record);
        }
        if Instant::now() >= deadline {
            let config = drive(
                GetRealmConfigOperation::new(realm_id),
                node.context.as_ref(),
            )
            .await?;
            let active_strategy = config
                .default_strategy_id
                .and_then(|strategy_id| config.strategy(&strategy_id));
            return Err(format!(
                "placement inventory did not reach the expected state; active strategy: {active_strategy:?}; last record: {last_record:?}"
            )
            .into());
        }
        sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_document(
    node: &TestNode,
    target: &DocumentSyncTarget,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    loop {
        match node
            .context
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Read {
                key_space: target.storage_keyspace().to_string(),
                key: target.storage_key(),
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value: Some(_), .. }) => return Ok(()),
            Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {}
            other => return Err(format!("unexpected document read event: {other:?}").into()),
        }
        if Instant::now() >= deadline {
            return Err(
                format!("document did not reach final holder {}", node.net.node_id()).into(),
            );
        }
        sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_policy_convergence(
    nodes: &[TestNode],
    realm_id: RealmId,
    draining_node: aruna_core::NodeId,
    replica_count: u32,
) -> Result<Vec<RealmConfigDocument>, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    loop {
        let mut configs = Vec::with_capacity(nodes.len());
        let mut converged = true;
        for node in nodes {
            let config = drive(
                GetRealmConfigOperation::new(realm_id),
                node.context.as_ref(),
            )
            .await?;
            let strategy_matches = config
                .default_strategy_id
                .and_then(|strategy_id| config.strategy(&strategy_id))
                .is_some_and(|strategy| strategy.replica_count == Some(replica_count));
            let node_matches = config
                .placement_entry(draining_node)
                .is_some_and(|entry| entry.draining);
            converged &= strategy_matches && node_matches;
            configs.push(config);
        }
        if converged {
            return Ok(configs);
        }
        if Instant::now() >= deadline {
            return Err("placement policy did not converge on all nodes".into());
        }
        sleep(Duration::from_millis(100)).await;
    }
}

fn assert_weighted_distinct_resolution(nodes: &[TestNode], config: &RealmConfigDocument) {
    let strategy = config
        .default_strategy_id
        .and_then(|strategy_id| config.strategy(&strategy_id))
        .expect("default placement strategy should resolve");
    assert_eq!(strategy.replica_count, Some(2));
    assert!(strategy.distinct_locations);
    assert!(matches!(
        strategy.affinity.as_slice(),
        [AffinityRule {
            effect: AffinityEffect::Multiply { permille: 4_000 },
            ..
        }]
    ));

    let view = build_view(config);
    let hot_node = nodes[0].net.node_id();
    let mut baseline = strategy.clone();
    baseline.affinity.clear();
    let mut hot_wins = 0usize;
    let mut baseline_hot_wins = 0usize;
    for counter in 0u64..2_000 {
        let subject = blake3::hash(&counter.to_le_bytes());
        let holders = resolve_holders(&view, strategy, subject.as_bytes(), 0, None);
        let locations: std::collections::HashSet<_> = holders
            .iter()
            .map(|holder| {
                view.nodes
                    .iter()
                    .find(|node| node.node_id == *holder)
                    .expect("holder should be in placement view")
                    .location
                    .as_str()
            })
            .collect();
        assert_eq!(holders.len(), 2);
        assert_eq!(locations.len(), 2);
        hot_wins += usize::from(holders[0] == hot_node);
        baseline_hot_wins += usize::from(
            resolve_holders(&view, &baseline, subject.as_bytes(), 0, None)[0] == hot_node,
        );
    }
    assert!(
        hot_wins > baseline_hot_wins + 400,
        "weighted location affinity should materially increase first-choice share: weighted={hot_wins} baseline={baseline_hot_wins}"
    );
}

fn sort_node_ids(nodes: &mut [aruna_core::NodeId]) {
    nodes.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
}

fn assert_placement_state_identical(configs: &[RealmConfigDocument]) {
    let first = &configs[0];
    let mut first_map = first.placement_map.clone();
    first_map.sort_by(|a, b| a.node_id.as_bytes().cmp(b.node_id.as_bytes()));
    for config in &configs[1..] {
        let mut map = config.placement_map.clone();
        map.sort_by(|a, b| a.node_id.as_bytes().cmp(b.node_id.as_bytes()));
        assert_eq!(map, first_map, "placement_map diverged across nodes");
        assert_eq!(
            config.strategies, first.strategies,
            "strategies diverged across nodes"
        );
        assert_eq!(
            config.default_strategy_id, first.default_strategy_id,
            "default strategy diverged across nodes"
        );
        assert_eq!(
            config.strategy_bindings, first.strategy_bindings,
            "strategy bindings diverged across nodes"
        );
    }
}

fn assert_resolve_holders_identical(configs: &[RealmConfigDocument]) {
    let strategy = configs[0]
        .default_strategy_id
        .and_then(|id| configs[0].strategy(&id))
        .expect("realm config has a default strategy")
        .clone();

    let views: Vec<_> = configs.iter().map(build_view).collect();

    for counter in 0u64..100 {
        let subject = *blake3::hash(&counter.to_le_bytes()).as_bytes();
        let reference = resolve_holders(&views[0], &strategy, &subject, 0, None);
        assert!(
            !reference.is_empty(),
            "empty holder set for subject {counter}"
        );
        for view in &views[1..] {
            let holders = resolve_holders(view, &strategy, &subject, 0, None);
            assert_eq!(
                holders, reference,
                "holder set diverged for subject {counter}"
            );
        }
    }
}

async fn build_realm_nodes(
    realm_id: &RealmId,
    count: usize,
) -> Result<Vec<TestNode>, Box<dyn std::error::Error>> {
    let mut nodes = Vec::with_capacity(count);
    for _ in 0..count {
        nodes.push(spawn_node(*realm_id).await?);
    }
    mesh_nodes(&nodes).await;
    install_realm_config(&nodes, *realm_id).await?;
    Ok(nodes)
}

async fn mesh_nodes(nodes: &[TestNode]) {
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
}

async fn spawn_node(realm_id: RealmId) -> Result<TestNode, Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let storage = FjallStorage::open(temp_dir.path().to_str().ok_or("invalid temp path")?)?;
    let net = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().expect("valid bind addr"),
            realm_id,
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        storage.clone(),
    )
    .await?;
    let task_handle = TaskHandle::new();

    let context = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: Some(net.clone()),
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(task_handle.clone()),
    });

    initialize_net_incoming(context.clone(), &Shutdown::new());
    initialize_task_incoming(context.clone(), task_handle, &Shutdown::new()).await;

    Ok(TestNode {
        _temp_dir: temp_dir,
        net,
        context,
    })
}

// Four independently weighted locations; the hot label multiplies both the
// location and node rendezvous weight for node zero.
fn base_config(nodes: &[TestNode], realm_id: RealmId) -> RealmConfigDocument {
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.seed_default_placement();
    let default_id = config.default_strategy_id.expect("seeded default strategy");
    if let Some(strategy) = config
        .strategies
        .iter_mut()
        .find(|strategy| strategy.strategy_id == default_id)
    {
        strategy.replica_count = Some(2);
        strategy.distinct_locations = true;
        strategy.affinity = vec![AffinityRule {
            matcher: LabelMatch {
                key: "tier".to_string(),
                value: "hot".to_string(),
            },
            effect: AffinityEffect::Multiply { permille: 4_000 },
        }];
    }

    let locations = ["eu", "us", "ap", "sa"];
    let weights = [100u32, 200, 150, 75];
    for (index, node) in nodes.iter().enumerate() {
        let node_id = node.net.node_id();
        config.ensure_node(node_id, RealmNodeKind::Management);
        config.placement_map.push(NodePlacementEntry {
            node_id,
            location: locations[index % locations.len()].to_string(),
            weight: weights[index % weights.len()],
            full: false,
            draining: false,
            labels: BTreeMap::from([(
                "tier".to_string(),
                if index == 0 { "hot" } else { "cold" }.to_string(),
            )]),
        });
    }
    config
}

async fn install_realm_config(
    nodes: &[TestNode],
    realm_id: RealmId,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = base_config(nodes, realm_id);
    for node in nodes {
        let actor = Actor {
            node_id: node.net.node_id(),
            user_id: aruna_core::UserId::nil(realm_id),
            realm_id,
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
    seed_realm_config_sync_topic(nodes, realm_id, &config).await?;
    Ok(())
}

async fn seed_realm_config_sync_topic(
    nodes: &[TestNode],
    realm_id: RealmId,
    config: &RealmConfigDocument,
) -> Result<(), Box<dyn std::error::Error>> {
    let target = DocumentSyncTarget::RealmConfig { realm_id };
    let actor = Actor {
        node_id: nodes[1].net.node_id(),
        user_id: aruna_core::UserId::nil(realm_id),
        realm_id,
    };
    let mut reducer_state =
        AdminDocumentReducerState::new(AdminDocumentTarget::RealmConfig { realm_id });
    let event = reducer_state.apply_operation(
        &actor,
        AdminDocumentOperation::RealmConfigNodePlacementSet {
            entry: config
                .placement_map
                .first()
                .ok_or("realm config has no placement entry")?
                .clone(),
        },
    )?;

    match nodes[1]
        .net
        .send_effect(Effect::Net(NetEffect::DocumentSync(
            DocumentSyncEffect::PublishDocuments {
                documents: vec![DocumentSyncPublish::AdminOperation {
                    target: target.clone(),
                    event: Box::new(event),
                    allow_genesis: true,
                }],
                peers: Vec::new(),
            },
        )))
        .await
    {
        Event::Net(NetEvent::DocumentSync(DocumentSyncNetEvent::DocumentsPublished { .. })) => {}
        other => {
            return Err(format!("unexpected realm config seed publish event: {other:?}").into());
        }
    }

    match nodes[0]
        .net
        .send_effect(Effect::Net(NetEffect::DocumentSync(
            DocumentSyncEffect::SyncDocuments {
                targets: vec![target],
                peers: Vec::new(),
            },
        )))
        .await
    {
        Event::Net(NetEvent::DocumentSync(DocumentSyncNetEvent::DocumentsReconciled {
            ..
        })) => Ok(()),
        other => Err(format!("unexpected realm config seed sync event: {other:?}").into()),
    }
}

async fn shutdown_nodes(nodes: Vec<TestNode>) {
    for node in nodes {
        node.net.shutdown().await;
    }
}
