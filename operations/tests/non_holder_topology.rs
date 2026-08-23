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
use aruna_core::metadata::MetadataQueryResults;
use aruna_core::storage_entries::metadata_registry_delete_entries;
use aruna_core::structs::{
    AuthContext, ComputeResources, ExecutionSpec, ImportMetadataTarget, ImportRoCrateSource,
    ImportRoCrateSpec, ImportRoCrateTarget, JobState, RoCrateLimits, WorkspaceMode, band_start,
    user_dedup_key,
};
use aruna_core::structured_id::{BucketId, PlacementHandle};
use aruna_operations::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentOperation, CreateMetadataDocumentPayload,
    mint_forward_document, mint_local_document,
};
use aruna_operations::driver::drive;
use aruna_operations::get_metadata_document::{
    GetMetadataDocumentError, GetMetadataDocumentOperation, load_metadata_record_by_document,
};
use aruna_operations::jobs::JobRouteError;
use aruna_operations::jobs::drain::{JobClassBudget, process_job_queue_batch};
use aruna_operations::jobs::runtime::JobsRuntime;
use aruna_operations::jobs::service::{
    RoutedCancelOutcome, cancel_job_routed, read_job_routed, read_owned_job, read_record_routed,
    submit_execution_job, submit_rocrate_import,
};
use aruna_operations::jobs::store::find_dedup_job;
use aruna_operations::jobs::submit::{SubmitJobError, mint_job_id};
use aruna_operations::metadata::MetadataAuthToken;
use aruna_operations::metadata::api::{
    ExportMetadataRoCrateRequest, ExportMetadataRoCrateResult, MetadataApiError,
    MetadataApiQueryMode, MetadataDocumentQueryRequest, MetadataRoCrateExportView,
    query_metadata_document,
};
use aruna_operations::metadata::forward::{
    MetadataWriteError, create_metadata_document_routed, delete_metadata_document_routed,
    export_rocrate_routed, origin_holds_document, update_metadata_document_routed,
};
use aruna_operations::metadata::projector::replay_metadata_event_log;
use aruna_operations::sync_placement::sort_node_ids;
use aruna_operations::update_metadata_document::UpdateMetadataDocumentMutation;
use std::cell::RefCell;
use ulid::Ulid;

use topology::{TestNode, TestResult, Topology, wait_for_convergence, wait_until};

const MANAGEMENT_NODES: usize = 5;
const USER_NODES: usize = 1;
const REPLICATION_FACTOR: u32 = 3;

fn execution_spec(seed: u8) -> ExecutionSpec {
    ExecutionSpec {
        group_id: Ulid::from_bytes([seed; 16]),
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
        collision_policy: Default::default(),
    }
}

const JOB_BUDGET: JobClassBudget = JobClassBudget {
    in_process: 1,
    external: 1,
};

/// Owner derivation as every node performs it: pure, from the replicated config.
fn derived_owner(
    realm: &Topology,
    job_id: aruna_core::structs::JobId,
) -> TestResult<aruna_core::NodeId> {
    Ok(realm.config.job_owner(job_id)?)
}

#[tokio::test]
async fn owner_read_routes() -> TestResult<()> {
    // The accepting node is the immutable owner and keeps the only record;
    // every other node derives the owner from the JobId and reads through it.
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let ingress = realm.node(0);
    let submitted = submit_execution_job(
        ingress.context.as_ref(),
        execution_spec(41),
        realm.user_id,
        ingress.node_id(),
        None,
        WorkspaceMode::None,
        None,
        aruna_operations::jobs::JOB_RETENTION_MS,
    )
    .await?;

    assert_eq!(derived_owner(&realm, submitted.job_id)?, ingress.node_id());
    let stored = read_owned_job(ingress.context.as_ref(), realm.user_id, submitted.job_id)
        .await?
        .expect("the accepting node owns the record");
    assert_eq!(stored.owner_node_id, ingress.node_id());
    assert_eq!(stored.state, JobState::Queued);

    for node in realm
        .nodes
        .iter()
        .filter(|node| node.node_id() != ingress.node_id())
    {
        assert!(
            read_owned_job(node.context.as_ref(), realm.user_id, submitted.job_id)
                .await?
                .is_none(),
            "no other node may carry any record"
        );
    }

    let bystander = realm.node(1);
    let routed = read_job_routed(
        bystander.context.as_ref(),
        &realm.auth_context(),
        submitted.job_id,
        Some(realm.bearer_token()),
    )
    .await?;
    assert_eq!(routed.job.job_id, submitted.job_id);

    realm.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn record_routes_owner() -> TestResult<()> {
    // TES/staging fetch the full owner record off-owner: a non-owner routes to
    // the owner and, when the owner is down, reports 503 not a false 404.
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let ingress = realm.node(0);
    let owner_id = ingress.node_id();
    let submitted = submit_execution_job(
        ingress.context.as_ref(),
        execution_spec(47),
        realm.user_id,
        owner_id,
        None,
        WorkspaceMode::None,
        None,
        aruna_operations::jobs::JOB_RETENTION_MS,
    )
    .await?;
    let probe = realm.node(1);

    let routed = read_record_routed(
        probe.context.as_ref(),
        realm.user_id,
        submitted.job_id,
        Some(realm.bearer_token()),
    )
    .await?
    .expect("the owner returns the full record");
    assert_eq!(routed.job_id, submitted.job_id);
    assert_eq!(routed.owner_node_id, owner_id);

    realm.find(owner_id).net.shutdown().await;
    let down = read_record_routed(
        probe.context.as_ref(),
        realm.user_id,
        submitted.job_id,
        Some(realm.bearer_token()),
    )
    .await;
    assert!(
        matches!(down, Err(JobRouteError::Unavailable(_))),
        "owner-down record fetch must be unavailable, never a false 404: {down:?}"
    );

    realm.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn origin_scoped_dedup() -> TestResult<()> {
    // Idempotency is per-origin by contract: a retry through the same origin
    // dedups in its local transaction; a different origin creates its own job.
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let first_ingress = realm.node(0);
    let second_ingress = realm.node(1);
    let key = Some("origin-scoped-retry".to_string());
    let first = submit_execution_job(
        first_ingress.context.as_ref(),
        execution_spec(42),
        realm.user_id,
        first_ingress.node_id(),
        key.clone(),
        WorkspaceMode::None,
        None,
        aruna_operations::jobs::JOB_RETENTION_MS,
    )
    .await?;
    let retried = submit_execution_job(
        first_ingress.context.as_ref(),
        execution_spec(42),
        realm.user_id,
        first_ingress.node_id(),
        key.clone(),
        WorkspaceMode::None,
        None,
        aruna_operations::jobs::JOB_RETENTION_MS,
    )
    .await?;
    assert!(first.created);
    assert!(!retried.created);
    assert_eq!(retried.job_id, first.job_id);

    let elsewhere = submit_execution_job(
        second_ingress.context.as_ref(),
        execution_spec(42),
        realm.user_id,
        second_ingress.node_id(),
        key,
        WorkspaceMode::None,
        None,
        aruna_operations::jobs::JOB_RETENTION_MS,
    )
    .await?;
    assert!(elsewhere.created, "another origin owns its own job");
    assert_ne!(elsewhere.job_id, first.job_id);
    assert_eq!(
        derived_owner(&realm, elsewhere.job_id)?,
        second_ingress.node_id()
    );

    // Submission is one local transaction: dedup rows and records exist only
    // on their origin, nothing was reserved on any other node.
    let dedup_key = user_dedup_key(realm.user_id, "origin-scoped-retry");
    for (index, node) in realm.nodes.iter().enumerate() {
        let row = find_dedup_job(
            &node.context.storage_handle,
            realm.user_id,
            &dedup_key,
            None,
        )
        .await?;
        match index {
            0 => assert_eq!(row, Some(first.job_id)),
            1 => assert_eq!(row, Some(elsewhere.job_id)),
            _ => assert_eq!(row, None, "no cross-node reservation may exist"),
        }
    }

    realm.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn cap_stays_local() -> TestResult<()> {
    // Active-job caps are per-origin: a full origin rejects while another
    // origin still accepts, and cap rows never leave their origin.
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let import_spec = |seed: u8| ImportRoCrateSpec {
        auth_context: AuthContext {
            user_id: realm.user_id,
            realm_id: realm.realm_id,
            path_restrictions: None,
        },
        source: ImportRoCrateSource::Upload {
            upload_id: Ulid::from_bytes([seed; 16]),
        },
        target: ImportRoCrateTarget {
            bucket: "target".to_string(),
            prefix: String::new(),
        },
        metadata: ImportMetadataTarget {
            group_id: Ulid::from_bytes([90; 16]),
            path: format!("crate-{seed}"),
            public: false,
        },
        limits: RoCrateLimits {
            max_active_jobs: 1,
            ..RoCrateLimits::default()
        },
        document_id: Ulid::from_bytes([seed; 16]),
    };
    let ingress = realm.node(0);
    let first = submit_rocrate_import(
        ingress.context.as_ref(),
        import_spec(1),
        ingress.node_id(),
        None,
    )
    .await?;
    assert!(first.created);
    let capped = submit_rocrate_import(
        ingress.context.as_ref(),
        import_spec(2),
        ingress.node_id(),
        None,
    )
    .await;
    assert!(matches!(
        capped,
        Err(SubmitJobError::ActiveJobLimit { limit: 1 })
    ));

    let other = realm.node(1);
    let accepted = submit_rocrate_import(
        other.context.as_ref(),
        import_spec(3),
        other.node_id(),
        None,
    )
    .await?;
    assert!(accepted.created, "another origin counts its own cap");

    realm.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn owner_claims() -> TestResult<()> {
    // Only the immutable owner's drain sees or claims the job.
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let ingress = realm.node(0);
    let submitted = submit_execution_job(
        ingress.context.as_ref(),
        execution_spec(43),
        realm.user_id,
        ingress.node_id(),
        None,
        WorkspaceMode::None,
        None,
        aruna_operations::jobs::JOB_RETENTION_MS,
    )
    .await?;
    let passive = realm.node(1);

    let passive_result = process_job_queue_batch(
        &passive.context.storage_handle,
        passive.node_id(),
        JOB_BUDGET,
        None,
    )
    .await?;
    assert!(passive_result.claimed.is_empty());
    assert!(
        read_owned_job(passive.context.as_ref(), realm.user_id, submitted.job_id)
            .await?
            .is_none(),
        "a non-owner has nothing to claim"
    );

    let owner_result = process_job_queue_batch(
        &ingress.context.storage_handle,
        ingress.node_id(),
        JOB_BUDGET,
        None,
    )
    .await?;
    assert_eq!(owner_result.claimed.len(), 1);
    assert_eq!(owner_result.claimed[0].job_id, submitted.job_id);
    assert_eq!(owner_result.claimed[0].owner_node_id, ingress.node_id());
    assert_eq!(
        owner_result.claimed[0]
            .claim
            .as_ref()
            .map(|claim| claim.holder_node_id),
        Some(ingress.node_id())
    );

    realm.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn swap_keeps_owner() -> TestResult<()> {
    // An arbitrary placement rebalance never re-homes a job: the owner is
    // derived from the JobId, so it keeps executing and control ops still
    // route to it even after it stops being selectable for any placement.
    let mut realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let owner_id = realm.node(0).node_id();
    let submitted = submit_execution_job(
        realm.node(0).context.as_ref(),
        execution_spec(44),
        realm.user_id,
        owner_id,
        None,
        WorkspaceMode::None,
        None,
        aruna_operations::jobs::JOB_RETENTION_MS,
    )
    .await?;
    assert_eq!(derived_owner(&realm, submitted.job_id)?, owner_id);

    // Data holders move: the owner becomes unselectable everywhere and an
    // override excludes it explicitly. The derived owner does not move.
    let mut config = realm.config.clone();
    for entry in config.placement_map.iter_mut() {
        if entry.node_id == owner_id {
            entry.weight = 0;
            entry.draining = true;
        }
    }
    config
        .placement_overrides
        .push(aruna_core::structs::PlacementOverride {
            subject: b"any-shard-subject".to_vec(),
            pinned: Vec::new(),
            excluded: vec![owner_id],
            strategy_id: None,
        });
    realm.apply_config(config).await?;
    assert_eq!(
        derived_owner(&realm, submitted.job_id)?,
        owner_id,
        "the derived owner survives an arbitrary placement change"
    );

    for node in realm
        .nodes
        .iter()
        .filter(|node| node.is_sync_eligible() && node.node_id() != owner_id)
    {
        let result = process_job_queue_batch(
            &node.context.storage_handle,
            node.node_id(),
            JOB_BUDGET,
            None,
        )
        .await?;
        assert!(result.claimed.is_empty(), "a non-owner must never claim");
        assert!(
            read_owned_job(node.context.as_ref(), realm.user_id, submitted.job_id)
                .await?
                .is_none(),
            "no runnable copy may appear anywhere else"
        );
    }

    let owner = realm.find(owner_id);
    let result =
        process_job_queue_batch(&owner.context.storage_handle, owner_id, JOB_BUDGET, None).await?;
    assert_eq!(
        result.claimed.len(),
        1,
        "the rebalance must not strand the owner"
    );
    assert_eq!(result.claimed[0].job_id, submitted.job_id);

    // Cancellation from a bystander reaches only the owner.
    let bystander = realm.node(1);
    let runtime = JobsRuntime::new_paused();
    let outcome = cancel_job_routed(
        bystander.context.as_ref(),
        &runtime,
        realm.user_id,
        submitted.job_id,
        Some(realm.bearer_token()),
    )
    .await?;
    assert!(matches!(outcome, RoutedCancelOutcome::Requested(_)));
    let record = read_owned_job(owner.context.as_ref(), realm.user_id, submitted.job_id)
        .await?
        .expect("the owner keeps its record");
    assert!(record.cancel_requested, "cancel landed on the owner");

    realm.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn owner_down_unavailable() -> TestResult<()> {
    // An unreachable owner yields Unavailable, never a false 404 and never a
    // takeover: no surviving node claims the job or fabricates an answer.
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let ingress = realm.node(0);
    let owner_id = ingress.node_id();
    let submitted = submit_execution_job(
        ingress.context.as_ref(),
        execution_spec(45),
        realm.user_id,
        owner_id,
        None,
        WorkspaceMode::None,
        None,
        aruna_operations::jobs::JOB_RETENTION_MS,
    )
    .await?;
    let probe = realm.node(1);

    realm.find(owner_id).net.shutdown().await;

    let status = read_job_routed(
        probe.context.as_ref(),
        &realm.auth_context(),
        submitted.job_id,
        Some(realm.bearer_token()),
    )
    .await;
    assert!(
        matches!(status, Err(JobRouteError::Unavailable(_))),
        "owner-down status must be unavailable, never absence: {status:?}"
    );
    let runtime = JobsRuntime::new_paused();
    let cancel = cancel_job_routed(
        probe.context.as_ref(),
        &runtime,
        realm.user_id,
        submitted.job_id,
        Some(realm.bearer_token()),
    )
    .await;
    assert!(
        matches!(cancel, Err(JobRouteError::Unavailable(_))),
        "owner-down cancel must be unavailable, not a passive terminalization"
    );

    for node in realm
        .nodes
        .iter()
        .filter(|node| node.is_sync_eligible() && node.node_id() != owner_id)
    {
        let result = process_job_queue_batch(
            &node.context.storage_handle,
            node.node_id(),
            JOB_BUDGET,
            None,
        )
        .await?;
        assert!(result.claimed.is_empty(), "no survivor may adopt the job");
        assert!(
            read_owned_job(node.context.as_ref(), realm.user_id, submitted.job_id)
                .await?
                .is_none(),
            "no survivor may hold a runnable copy"
        );
    }

    realm.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn owner_answers_absence() -> TestResult<()> {
    // Only the resolved owner returns an authoritative 404; a handle without
    // a synced binding degrades to Unavailable, never a false absence.
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let ingress = realm.node(0);
    let bystander = realm.node(1);
    let handle = realm
        .config
        .job_control_handle(&ingress.node_id())
        .expect("fixture binds every node");
    let missing = mint_job_id(handle, BucketId::new(0)?)?;

    let routed = read_job_routed(
        bystander.context.as_ref(),
        &realm.auth_context(),
        missing,
        Some(realm.bearer_token()),
    )
    .await;
    assert!(
        matches!(routed, Err(JobRouteError::NotFound)),
        "the owner is the sole 404 authority: {routed:?}"
    );

    let unbound = PlacementHandle::new(band_start(500))?;
    let unresolved = mint_job_id(unbound, BucketId::new(0)?)?;
    let routed = read_job_routed(
        bystander.context.as_ref(),
        &realm.auth_context(),
        unresolved,
        Some(realm.bearer_token()),
    )
    .await;
    assert!(
        matches!(routed, Err(JobRouteError::Unavailable(_))),
        "an unsynced binding must degrade to 503, never 404: {routed:?}"
    );

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
    let origin = realm.leading_node(group_id, path);
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
    let origin = realm.leading_node(group_id, path);
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
    let origin = realm.leading_node(group_id, path);
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

    update_routed(
        bystander,
        &realm,
        document_id,
        r#"{"@id":"./off-holder.txt","@type":"File","name":"off-holder.txt"}"#,
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
    let stale_record = load_metadata_record_by_document(stale.context.as_ref(), document_id)
        .await
        .map_err(|error| format!("registry read failed: {error:?}"))?
        .ok_or("the holder must carry the registry row")?;
    let deleted = stale
        .context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchDelete {
            deletes: metadata_registry_delete_entries(&stale_record),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        deleted,
        Event::Storage(StorageEvent::BatchDeleteResult { .. })
    ));
    assert!(!registry_row_present(stale, document_id).await);

    update_routed(
        stale,
        &realm,
        document_id,
        r#"{"@id":"./stale-holder.txt","@type":"File","name":"stale-holder.txt"}"#,
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

    delete_routed(bystander, &realm, document_id).await?;

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

// A per-document SPARQL query arriving at a non-holder must route to a holder
// rather than fail on a graph the bystander never materialized.
#[tokio::test]
async fn document_sparql_routes() -> TestResult<()> {
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;

    // Distributed queries bulk-load group policies, which fails closed unless
    // the group document exists on the queried holders.
    let group_id = realm.seed_group().await?;
    let path = "datasets/sparql-routes";
    let origin = realm.leading_node(group_id, path);
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

    let execution = query_metadata_document(
        bystander.context.as_ref(),
        realm.realm_id,
        bystander.node_id(),
        MetadataDocumentQueryRequest {
            document_id,
            auth: None,
            bearer_token: Some(realm.bearer_string()),
            query: "ASK { ?s ?p ?o }".to_string(),
            mode: Some(MetadataApiQueryMode::Distributed),
            allow_partial: false,
        },
    )
    .await?;
    assert!(
        matches!(execution.results, MetadataQueryResults::Boolean(true)),
        "non-holder query did not route to a holder graph"
    );

    realm.shutdown().await;
    Ok(())
}

// An RO-Crate export arriving at a non-holder must route to a document holder,
// under either a bearer token (sync) or a peer-attested principal (queued job).
#[tokio::test]
async fn document_export_routes() -> TestResult<()> {
    let realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;

    // Routed export loads group policies, which fails closed unless the group
    // document exists on the holders.
    let group_id = realm.seed_group().await?;
    let path = "datasets/export-routes";
    let origin = realm.leading_node(group_id, path);
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

    let request = |auth| ExportMetadataRoCrateRequest {
        document_id,
        auth,
        view: MetadataRoCrateExportView::Full,
        limit: None,
        offset: None,
        after: None,
    };
    let bearer = export_routed(&realm, bystander, request(None), realm.bearer_token()).await?;
    assert!(matches!(bearer, ExportMetadataRoCrateResult::Full { .. }));

    let principal = AuthContext {
        user_id: realm.user_id,
        realm_id: realm.realm_id,
        path_restrictions: None,
    };
    let internal = export_routed(
        &realm,
        bystander,
        request(Some(principal.clone())),
        MetadataAuthToken::internal(principal),
    )
    .await?;
    assert!(matches!(internal, ExportMetadataRoCrateResult::Full { .. }));

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

/// A routed export, re-run while the fan-out reports it unavailable: the first
/// request pays a cold metadata dial inside the fan-out's per-peer slot, which a
/// starved machine can spend before the holder is reached at all.
async fn export_routed(
    realm: &Topology,
    bystander: &TestNode,
    request: ExportMetadataRoCrateRequest,
    token: MetadataAuthToken,
) -> TestResult<ExportMetadataRoCrateResult> {
    let export = RefCell::new(None);
    wait_for_convergence::<_, _, Box<dyn std::error::Error>>(
        "no routed export reached a holder",
        || async {
            match export_rocrate_routed(
                &bystander.context,
                realm.realm_id,
                request.clone(),
                Some(token.clone()),
                RoCrateLimits::default().metadata_bytes,
            )
            .await
            {
                Err(MetadataApiError::ServiceUnavailable) => Ok(1),
                other => {
                    *export.borrow_mut() = Some(other?);
                    Ok(0)
                }
            }
        },
    )
    .await?;
    Ok(export
        .into_inner()
        .ok_or("the routed export produced no result")?)
}

/// A routed update, re-run while every holder reports its placement view
/// unavailable: forwarded writes fail closed while config replication or a
/// pending registry projection lags, which a starved machine stretches past
/// one attempt.
async fn update_routed(
    node: &TestNode,
    realm: &Topology,
    document_id: Ulid,
    jsonld: &str,
) -> TestResult<()> {
    wait_for_convergence::<_, _, Box<dyn std::error::Error>>(
        "no routed update reached a holder",
        || async {
            match update_metadata_document_routed(
                &node.context,
                realm.actor(node),
                None,
                document_id,
                None,
                UpdateMetadataDocumentMutation::UpsertDataEntity {
                    jsonld: jsonld.to_string(),
                },
                Some(realm.bearer_token()),
            )
            .await
            {
                Err(MetadataWriteError::Undeliverable(reason))
                    if reason.contains("placement view is unavailable") =>
                {
                    Ok(1)
                }
                other => {
                    other?;
                    Ok(0)
                }
            }
        },
    )
    .await
}

/// The delete twin of [`update_routed`]; deletes additionally require the pid
/// authority's converged view, so the window is wider.
async fn delete_routed(node: &TestNode, realm: &Topology, document_id: Ulid) -> TestResult<()> {
    wait_for_convergence::<_, _, Box<dyn std::error::Error>>(
        "no routed delete reached a holder",
        || async {
            match delete_metadata_document_routed(
                &node.context,
                realm.actor(node),
                None,
                document_id,
                Some(realm.bearer_token()),
            )
            .await
            {
                Err(MetadataWriteError::Undeliverable(reason))
                    if reason.contains("placement view is unavailable") =>
                {
                    Ok(1)
                }
                other => {
                    other?;
                    Ok(0)
                }
            }
        },
    )
    .await
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
