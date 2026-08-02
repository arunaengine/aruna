// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "256"]
//! Holder and non-holder coverage on a realm sized above the replication factor.
//!
//! Every other multi-node fixture in this workspace runs at `node_count <= RF`,
//! where every node holds every bucket and non-holder behaviour is unobservable.
//! These tests run five Management nodes at RF three plus a User-kind node, and
//! prove holdership against the bucket a create actually stamps before exercising
//! the path, so a regression to universal holdership fails the fixture instead of
//! quietly voiding the test.

mod topology;

use aruna_core::StructuredId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::metadata::MetadataError;
use aruna_core::storage_entries::metadata_registry_delete_entries;
use aruna_core::structs::{
    ComputeResources, DocumentClass, ExecutionSpec, JOBCONTROL_HANDLE, PlacementRef,
    PlacementScope, WorkspaceMode,
};
use aruna_operations::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentOperation, CreateMetadataDocumentPayload,
    mint_forward_document, mint_local_document,
};
use aruna_operations::driver::drive;
use aruna_operations::get_metadata_document::{
    GetMetadataDocumentError, GetMetadataDocumentOperation, load_metadata_record_by_document,
};
use aruna_operations::jobs::runtime::JobsRuntime;
use aruna_operations::jobs::service::{
    RoutedCancelOutcome, cancel_job_routed, read_job_routed, read_owned_job, submit_execution_job,
};
use aruna_operations::metadata::forward::{
    create_metadata_document_routed, delete_metadata_document_routed, origin_holds_document,
    update_metadata_document_routed,
};
use aruna_operations::metadata::projector::replay_metadata_event_log;
use aruna_operations::sync_placement::sort_node_ids;
use aruna_operations::update_metadata_document::UpdateMetadataDocumentMutation;
use ulid::Ulid;

use topology::{TestNode, TestResult, Topology, wait_until};

const MANAGEMENT_NODES: usize = 5;
const USER_NODES: usize = 1;
const REPLICATION_FACTOR: u32 = 3;

#[tokio::test]
async fn job_read_routes() -> TestResult<()> {
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let owner = realm.node(0);
    let submitted = submit_execution_job(
        owner.context.as_ref(),
        ExecutionSpec {
            group_id: Ulid::from_bytes([41; 16]),
            name: None,
            description: None,
            tags: Default::default(),
            image: "alpine:3".to_string(),
            entrypoint: None,
            command: Vec::new(),
            workdir: None,
            env: Default::default(),
            resources: ComputeResources::default(),
            executor_constraint: None,
            inputs: Vec::new(),
            file_outputs: Vec::new(),
            workspace_outputs: Vec::new(),
            output_prefixes: Vec::new(),
        },
        realm.user_id,
        owner.node_id(),
        None,
        WorkspaceMode::None,
        None,
        aruna_operations::jobs::JOB_RETENTION_MS,
    )
    .await?;

    let binding = realm
        .config
        .binding_directory()
        .resolve(submitted.job_id.as_routable()?.placement_handle())?;
    let routable = submitted.job_id.as_routable()?;
    assert_eq!(routable.placement_handle().get(), JOBCONTROL_HANDLE);
    assert_eq!(binding.document_class, DocumentClass::JobControl);
    assert_eq!(binding.scope, PlacementScope::Realm(realm.realm_id));
    let placement = PlacementRef {
        strategy_id: binding.strategy_id,
        epoch: 0,
        shard: u32::from(routable.bucket().get()),
    };
    realm.assert_holder(owner.node_id(), &placement);
    let reader = realm.user_node();
    realm.assert_not_holder(reader.node_id(), &placement);
    let stored = read_owned_job(owner.context.as_ref(), realm.user_id, submitted.job_id)
        .await?
        .expect("holder stores the submitted job");
    assert_eq!(stored.owner_node_id, owner.node_id());

    let routed = read_job_routed(
        reader.context.as_ref(),
        realm.user_id,
        submitted.job_id,
        Some(realm.bearer_token()),
    )
    .await?;
    assert_eq!(routed.job.job_id, submitted.job_id);

    let caller_runtime = JobsRuntime::new_paused();
    let cancelled = cancel_job_routed(
        reader.context.as_ref(),
        caller_runtime.as_ref(),
        realm.user_id,
        submitted.job_id,
        Some(realm.bearer_token()),
    )
    .await?;
    assert!(matches!(cancelled, RoutedCancelOutcome::Requested(_)));
    let stored = read_owned_job(owner.context.as_ref(), realm.user_id, submitted.job_id)
        .await?
        .expect("holder retains the submitted job");
    assert!(stored.cancel_requested);

    realm.shutdown().await;
    Ok(())
}

// The fixture itself is the deliverable: without this proof every test below
// degrades silently into the all-nodes-hold-everything case.
#[tokio::test]
async fn fixture_proves_nonholders() -> TestResult<()> {
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;

    let group_id = Ulid::from_bytes([11; 16]);
    let document_id = Ulid::from_bytes([12; 16]);
    let path = "datasets/proof";
    let origin = realm.node(0);

    let placement = realm
        .origin_placement(origin, group_id, document_id, path)
        .expect("a Management node holds buckets of the default strategy");
    let holders = realm.assert_holder(origin.node_id(), &placement);
    assert_eq!(holders.len(), REPLICATION_FACTOR as usize);

    let non_holders = realm.non_holder_ids(&placement);
    assert_eq!(
        non_holders.len(),
        MANAGEMENT_NODES - REPLICATION_FACTOR as usize
    );
    for node_id in &non_holders {
        realm.assert_not_holder(*node_id, &placement);
    }

    // Holders are a pure function of the replicated config and the stamped bucket,
    // so the proof is exact rather than probabilistic: every node derives the same
    // set, in the same rank order.
    for view in realm.holder_views(&placement).await? {
        assert_eq!(view, holders, "holder set diverged across nodes");
    }

    // A User-kind node is never sync-eligible, so it holds no bucket to stamp: the
    // one origin from which a create must be forwarded (D10).
    assert_eq!(
        realm.origin_placement(realm.user_node(), group_id, document_id, path),
        None
    );

    realm.shutdown().await;
    Ok(())
}

// D3: a create stamps the best-ranked bucket its origin already holds, so the
// origin is always a holder of what it creates and can always publish it. A
// blind document hash would place it anywhere, including on buckets the origin
// does not hold - the state that makes an offline node's writes undeliverable.
#[tokio::test]
async fn create_stamps_origin() -> TestResult<()> {
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;

    let group_id = Ulid::from_bytes([21; 16]);
    let path = "datasets/stamped-by-origin";
    let origin = realm.node(0);
    let document_id =
        mint_local_document(&realm.config, &realm.actor(origin), group_id, path)?.as_ulid();
    let expected = realm
        .origin_placement(origin, group_id, document_id, path)
        .expect("a Management node holds buckets");
    let holders = realm.assert_holder(origin.node_id(), &expected);

    let created = create_document(&realm, origin, group_id, document_id, path).await?;
    assert_eq!(
        created, expected,
        "create stamped a bucket it does not hold"
    );

    for holder in &holders {
        let node = realm.find(*holder);
        wait_until("document reaches holder", node.node_id(), || {
            document_present(node, group_id, document_id)
        })
        .await?;
    }

    let view = drive(
        GetMetadataDocumentOperation::new(group_id, document_id),
        origin.context.as_ref(),
    )
    .await?;
    assert_eq!(view.record.placement, expected);
    let mut recorded = view.record.holder_node_ids.clone();
    let mut planned = holders.clone();
    sort_node_ids(&mut recorded);
    sort_node_ids(&mut planned);
    assert_eq!(
        recorded, planned,
        "origin recorded a holder set that is not the stamped bucket's"
    );

    realm.shutdown().await;
    Ok(())
}

// A non-holder carries the document's registry row - the row rides the
// everywhere-bound registry class, which is what lets it route reads and writes -
// but it holds no bucket of the document, so it has no graph. The miss is
// therefore the graph's, not the record's.
#[tokio::test]
async fn read_misses_nonholder() -> TestResult<()> {
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;

    let group_id = Ulid::from_bytes([31; 16]);
    let path = "datasets/read-off-holders";
    let origin = realm.node(0);
    let document_id =
        mint_local_document(&realm.config, &realm.actor(origin), group_id, path)?.as_ulid();
    let placement = create_document(&realm, origin, group_id, document_id, path).await?;
    let holders = realm.assert_holder(origin.node_id(), &placement);

    for holder in &holders {
        let node = realm.find(*holder);
        wait_until("document reaches holder", node.node_id(), || {
            document_present(node, group_id, document_id)
        })
        .await?;
    }

    let bystander = realm.non_holder(&placement);
    wait_until(
        "registry row reaches non-holder",
        bystander.node_id(),
        || registry_row_present(bystander, document_id),
    )
    .await?;

    let result = drive(
        GetMetadataDocumentOperation::new(group_id, document_id),
        bystander.context.as_ref(),
    )
    .await;
    assert_eq!(
        result.unwrap_err(),
        GetMetadataDocumentError::MetadataError(MetadataError::GraphNotFound),
        "non-holder served a graph it never received"
    );

    // The User node is not sync-eligible, so it holds not even the registry row:
    // its miss is the record's.
    let user = realm.user_node();
    let result = drive(
        GetMetadataDocumentOperation::new(group_id, document_id),
        user.context.as_ref(),
    )
    .await;
    assert_eq!(
        result.unwrap_err(),
        GetMetadataDocumentError::DocumentNotFound
    );

    realm.shutdown().await;
    Ok(())
}

// D10/D11: a write arriving at a node that holds no bucket of the document is
// forwarded to a holder. The bystander never joins the bucket's topic to publish
// it itself, so the mutation can only reach the holders through the forward -
// and the bystander still has no graph afterwards.
#[tokio::test]
async fn bystander_writes_forward() -> TestResult<()> {
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let group_id = realm.seed_group().await?;

    let path = "datasets/bystander-writes";
    let origin = realm.node(0);
    let document_id =
        mint_local_document(&realm.config, &realm.actor(origin), group_id, path)?.as_ulid();
    let placement = create_document(&realm, origin, group_id, document_id, path).await?;
    let holders = realm.assert_holder(origin.node_id(), &placement);
    for holder in &holders {
        let node = realm.find(*holder);
        wait_until("document reaches holder", node.node_id(), || {
            document_present(node, group_id, document_id)
        })
        .await?;
    }

    let bystander = realm.non_holder(&placement);
    wait_until(
        "registry row reaches non-holder",
        bystander.node_id(),
        || registry_row_present(bystander, document_id),
    )
    .await?;
    let record = load_metadata_record_by_document(bystander.context.as_ref(), document_id)
        .await
        .map_err(|error| format!("registry read failed: {error:?}"))?
        .ok_or("the non-holder must carry the registry row")?;
    assert_eq!(record.placement, placement);
    assert!(
        !origin_holds_document(
            &bystander.context,
            realm.realm_id,
            bystander.node_id(),
            document_id,
        )
        .await?
    );

    update_metadata_document_routed(
        &bystander.context,
        realm.actor(bystander),
        None,
        document_id,
        None,
        UpdateMetadataDocumentMutation::UpsertDataEntity {
            jsonld: r#"{"@id":"./off-holder.txt","@type":"File","name":"off-holder.txt"}"#
                .to_string(),
        },
        Some(realm.bearer_token()),
    )
    .await?;

    for holder in &holders {
        let node = realm.find(*holder);
        wait_until("update reaches holder", node.node_id(), || async {
            drive(
                GetMetadataDocumentOperation::new(group_id, document_id),
                node.context.as_ref(),
            )
            .await
            .is_ok_and(|view| view.jsonld.contains("off-holder.txt"))
        })
        .await?;
    }
    assert_eq!(
        drive(
            GetMetadataDocumentOperation::new(group_id, document_id),
            bystander.context.as_ref(),
        )
        .await
        .unwrap_err(),
        GetMetadataDocumentError::MetadataError(MetadataError::GraphNotFound),
        "the forwarder must not have applied the write onto a bucket it does not hold"
    );

    let stale = realm.find(holders[0]);
    let deleted = stale
        .context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchDelete {
            deletes: metadata_registry_delete_entries(group_id, document_id),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        deleted,
        Event::Storage(StorageEvent::BatchDeleteResult { .. })
    ));
    assert!(!registry_row_present(stale, document_id).await);

    update_metadata_document_routed(
        &stale.context,
        realm.actor(stale),
        None,
        document_id,
        None,
        UpdateMetadataDocumentMutation::UpsertDataEntity {
            jsonld: r#"{"@id":"./stale-holder.txt","@type":"File","name":"stale-holder.txt"}"#
                .to_string(),
        },
        Some(realm.bearer_token()),
    )
    .await?;
    let healthy = realm.find(holders[1]);
    wait_until("stale holder forwards", healthy.node_id(), || async {
        drive(
            GetMetadataDocumentOperation::new(group_id, document_id),
            healthy.context.as_ref(),
        )
        .await
        .is_ok_and(|view| view.jsonld.contains("stale-holder.txt"))
    })
    .await?;

    delete_metadata_document_routed(
        &bystander.context,
        realm.actor(bystander),
        None,
        document_id,
        Some(realm.bearer_token()),
    )
    .await?;

    for holder in &holders {
        let node = realm.find(*holder);
        wait_until("delete reaches holder", node.node_id(), || async {
            matches!(
                drive(
                    GetMetadataDocumentOperation::new(group_id, document_id),
                    node.context.as_ref(),
                )
                .await,
                Err(GetMetadataDocumentError::DocumentNotFound)
            )
        })
        .await?;
    }

    realm.shutdown().await;
    Ok(())
}

// The create a bucket-holding node can never reach: a User-kind node holds no
// bucket, so it can stamp none and must forward the create to a holder (D10).
// The holder stamps the document's blind-hashed bucket, and the forwarder is
// never added to it (D11).
#[tokio::test]
async fn user_create_forwards() -> TestResult<()> {
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let group_id = realm.seed_group().await?;

    let path = "datasets/forwarded-by-user";
    let user = realm.user_node();
    let document_id =
        mint_forward_document(&realm.config, &realm.actor(user), group_id, path)?.as_ulid();
    assert_eq!(
        realm.origin_placement(user, group_id, document_id, path),
        None
    );

    let created = create_metadata_document_routed(
        CreateMetadataDocumentOperation::new(document_config(
            &realm,
            user,
            group_id,
            document_id,
            path,
        )),
        user.context.clone(),
        Some(realm.bearer_token()),
    )
    .await?
    .record;

    let holders = realm.assert_not_holder(user.node_id(), &created.placement);
    assert_eq!(holders.len(), REPLICATION_FACTOR as usize);

    for holder in &holders {
        let node = realm.find(*holder);
        wait_until("forwarded create reaches holder", node.node_id(), || {
            document_present(node, group_id, document_id)
        })
        .await?;
    }

    // The forwarder holds nothing, not even the everywhere-bound registry row: it
    // is not a sync target at all.
    assert_eq!(
        drive(
            GetMetadataDocumentOperation::new(group_id, document_id),
            user.context.as_ref(),
        )
        .await
        .unwrap_err(),
        GetMetadataDocumentError::DocumentNotFound
    );

    realm.shutdown().await;
    Ok(())
}

fn document_config(
    realm: &Topology,
    node: &TestNode,
    group_id: Ulid,
    document_id: Ulid,
    document_path: &str,
) -> CreateMetadataDocumentConfig {
    CreateMetadataDocumentConfig {
        actor: realm.actor(node),
        group_id,
        document_id,
        document_path: document_path.to_string(),
        public: true,
        payload: CreateMetadataDocumentPayload::Scaffold {
            name: "Topology Dataset".to_string(),
            description: "Written on a realm above the replication factor".to_string(),
            date_published: "2026-01-01".to_string(),
            license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
        },
    }
}

/// Creates locally on a bucket-holding node and returns the bucket it stamped.
async fn create_document(
    realm: &Topology,
    node: &TestNode,
    group_id: Ulid,
    document_id: Ulid,
    document_path: &str,
) -> TestResult<aruna_core::structs::PlacementRef> {
    let created = drive(
        CreateMetadataDocumentOperation::new(document_config(
            realm,
            node,
            group_id,
            document_id,
            document_path,
        )),
        node.context.as_ref(),
    )
    .await?;
    replay_metadata_event_log(node.context.as_ref()).await?;
    Ok(created.record.placement)
}

async fn document_present(node: &TestNode, group_id: Ulid, document_id: Ulid) -> bool {
    drive(
        GetMetadataDocumentOperation::new(group_id, document_id),
        node.context.as_ref(),
    )
    .await
    .is_ok()
}

async fn registry_row_present(node: &TestNode, document_id: Ulid) -> bool {
    load_metadata_record_by_document(node.context.as_ref(), document_id)
        .await
        .is_ok_and(|record| record.is_some())
}
