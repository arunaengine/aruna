//! Tests that a finished local run reaches the realm only when its owner publishes it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::topology;

use std::time::{Duration, SystemTime};

use crate::job_calls::insert_job;
use crate::job_calls::prune_job_batch;
use crate::job_calls::read_job_record;
use crate::job_calls::{put_crate_status, read_crate_status, read_run_link};
use aruna_core::document::DocumentTarget;
use aruna_core::effects::StorageEffect;
use aruna_core::jobs::{JobRunOutcome, ProgressReporter};
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::execution::job::identity::{JobId, JobState};
use aruna_core::structs::execution::job::input::{ExecutionSpec, JobPayload};
use aruna_core::structs::execution::job::local::{
    JobRecord, JobResultPayload, OutputObject, RunCrateStatus,
};
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::routing::RoutingSnapshot;
use aruna_core::structs::synced_folder::{RemoteBinding, SyncRefusal};
use aruna_core::types::GroupId;
use aruna_ops_data::s3::bucket::create::CreateBucketOperation;
use aruna_ops_data::s3::object::get::{GetObjectInput, GetObjectOperation};
use aruna_ops_data::s3::object::put::{PutObjectConfig, PutObjectInput, PutObjectOperation};
use aruna_ops_identity::device_node::realm_documents::fetch_realm_documents;
use aruna_ops_work::jobs::workflow::publish_run::{
    PublishRunError, PublishRunInput, local_run_crate, publish_run,
};
use aruna_ops_work::jobs::workflow::run_crate::{WriteCrateOperation, crate_outcome};
use aruna_runner::driver::{DriverContext, drive};
use aruna_runner::jobs::context::JobContext;
use futures_util::StreamExt;
use tokio_util::sync::CancellationToken;
use topology::{TestNode, TestResult, Topology, read_group_auth, wait_until};
use ulid::Ulid;

const REMOTE_BUCKET: &str = "lab-data";

async fn write_run_crate(ctx: &JobContext, for_job: JobId) -> JobRunOutcome {
    let write = WriteCrateOperation::new(for_job, ctx.driver.node_and_realm());
    crate_outcome(drive(write, &ctx.driver).await)
}
const LOCAL_BUCKET: &str = "run-out";
const OUTPUT: &[u8] = b">contig1\nACGT\n";
/// Only ends a hung exchange; never a performance assertion.
const FETCH_BUDGET: Duration = Duration::from_secs(30);

async fn create_bucket(
    context: &DriverContext,
    name: &str,
    group_id: GroupId,
    realm: &Topology,
) -> TestResult<()> {
    drive(
        CreateBucketOperation::new(
            name.to_string(),
            BucketInfo {
                group_id,
                created_at: SystemTime::now(),
                created_by: realm.user_id,
                cors_configuration: None,
                storage_routing: Vec::new(),
                placement_policies: Vec::new(),
                placement_policy_generation: 0,
                compression: Compression::Off,
                distribution: None,
            },
        ),
        context,
    )
    .await?;
    Ok(())
}

/// The output a local run wrote into a device bucket.
async fn write_output(realm: &Topology, group_id: GroupId) -> TestResult<Ulid> {
    let device = realm.user_node();
    let result = drive(
        PutObjectOperation::new(PutObjectConfig {
            user_id: realm.user_id,
            group_id,
            realm_id: realm.realm_id,
            node_id: device.node_id(),
            request: PutObjectInput {
                bucket: LOCAL_BUCKET.to_string(),
                key: "contigs.fasta".to_string(),
                content_length: Some(OUTPUT.len() as u64),
                body: Some(
                    BackendStream::new(tokio_util::io::ReaderStream::new(OUTPUT))
                        as BackendStream<Result<bytes::Bytes, StreamError>>,
                ),
            },
            expected_checksums: Vec::new(),
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: None,
            quota_ceiling: None,
            routing: RoutingSnapshot::single(group_id),
        }),
        device.context.as_ref(),
    )
    .await?;
    Ok(result.version_id)
}

/// A succeeded local run whose crate waits for the owner, as the device records it.
/// The output a local run captured at `version_id` in the device bucket.
fn output(realm: &Topology, version_id: Ulid) -> OutputObject {
    OutputObject {
        node_id: realm.user_node().node_id(),
        bucket: LOCAL_BUCKET.to_string(),
        key: "contigs.fasta".to_string(),
        version_id,
        execution_id: Ulid::from(9u128),
        container_path: "/out/contigs.fasta".to_string(),
        size: OUTPUT.len() as u64,
        digest: Some(blake3::hash(OUTPUT).to_hex().to_string()),
    }
}

async fn finished_run(
    (realm, device): (&Topology, &TestNode),
    group_id: GroupId,
    outputs: Vec<OutputObject>,
    status: RunCrateStatus,
) -> TestResult<JobId> {
    let job_id = JobId::from_bytes([0xC3; 16]);
    let spec = ExecutionSpec {
        group_id,
        name: None,
        description: None,
        tags: Default::default(),
        image: "alpine".to_string(),
        entrypoint: None,
        command: Vec::new(),
        workdir: None,
        env: Default::default(),
        resources: Default::default(),
        executor_constraint: None,
        backend: None,
        inputs: Vec::new(),
        file_outputs: Vec::new(),
        workspace_outputs: Vec::new(),
        output_prefixes: Vec::new(),
        collision_policy: Default::default(),
    };
    let mut record = JobRecord::new(
        job_id,
        JobPayload::Execution(spec),
        realm.user_id,
        device.node_id(),
        1,
        1,
        None,
    );
    record.state = JobState::Succeeded;
    // Inside its retention, so the node's job pruner leaves it alone.
    record.finished_at_ms = Some(aruna_clock::unix_timestamp_millis());
    record.result = Some(JobResultPayload::Execution {
        exit_code: Some(0),
        workspace_bucket: None,
        outputs,
        stdout: String::new(),
        stderr: String::new(),
        output_digest: None,
    });
    insert_job(&device.context, &record).await?;
    put_crate_status(&device.context, job_id, &status).await?;
    Ok(job_id)
}

async fn read_remote(realm: &Topology, group_id: GroupId, key: &str) -> TestResult<Vec<u8>> {
    let server = realm.node(0);
    let result = drive(
        GetObjectOperation::new(GetObjectInput {
            bucket: REMOTE_BUCKET.to_string(),
            key: key.to_string(),
            version_id: None,
            range: None,
            group_id,
            user_identity: realm.user_id,
            realm_id: realm.realm_id,
            node_id: server.node_id(),
        }),
        server.context.as_ref(),
    )
    .await?;
    let mut bytes = Vec::new();
    let mut blob = result.blob.0;
    while let Some(chunk) = blob.next().await {
        bytes.extend_from_slice(&chunk?);
    }
    Ok(bytes)
}

/// Publishing copies the exact output version under the prefix and writes the
/// crate once; a repeated publish replays the same copy.
#[tokio::test]
async fn publishes_local_run() -> TestResult<()> {
    let realm = Topology::spawn(2, 1, 1).await?;
    let group_id = realm.seed_group().await?;
    let device = realm.user_node();
    let server = realm.node(0);
    // A node lists a new group a little later, so a fetch can miss it; the device must hold it.
    wait_until("the group on the device", device.node_id(), || async {
        drive(
            fetch_realm_documents(device.context.node_and_realm(), FETCH_BUDGET),
            &device.context,
        )
        .await
        .unwrap_or(false)
            && matches!(read_group_auth(device, group_id).await, Ok(Some(_)))
    })
    .await?;
    create_bucket(&server.context, REMOTE_BUCKET, group_id, &realm).await?;
    create_bucket(&device.context, LOCAL_BUCKET, group_id, &realm).await?;
    let version_id = write_output(&realm, group_id).await?;
    let job_id = finished_run(
        (&realm, realm.user_node()),
        group_id,
        vec![output(&realm, version_id)],
        RunCrateStatus::Local,
    )
    .await?;
    let input = || PublishRunInput {
        job_id,
        owner: realm.user_id,
        node_id: device.node_id(),
        target: RemoteBinding {
            node_id: server.node_id(),
            bucket: REMOTE_BUCKET.to_string(),
            prefix: "runs/assembly".to_string(),
        },
    };

    let published = drive(
        publish_run(device.context.node_and_realm(), input()),
        &device.context,
    )
    .await?;
    assert_eq!(published.outputs.len(), 1);
    assert_eq!(
        published.outputs[0].key,
        "runs/assembly/run-out/contigs.fasta"
    );
    assert_eq!(published.outputs[0].node_id, server.node_id());
    assert!(
        matches!(published.run_crate, RunCrateStatus::Written { .. }),
        "expected written crate, got {:?}",
        published.run_crate
    );
    assert_eq!(
        read_remote(&realm, group_id, "runs/assembly/run-out/contigs.fasta").await?,
        OUTPUT
    );

    let replayed = drive(
        publish_run(device.context.node_and_realm(), input()),
        &device.context,
    )
    .await?;
    assert_eq!(
        replayed.outputs[0].version_id,
        published.outputs[0].version_id
    );
    assert_eq!(replayed.run_crate, published.run_crate);
    assert_eq!(
        read_crate_status(&device.context, job_id).await?,
        Some(published.run_crate)
    );

    // The first target is frozen: another prefix would leave the crate naming the first copies.
    let mut moved = input();
    moved.target.prefix = "runs/other".to_string();
    assert!(matches!(
        drive(
            publish_run(device.context.node_and_realm(), moved),
            &device.context
        )
        .await,
        Err(PublishRunError::TargetChanged)
    ));
    Ok(())
}

/// The crate job itself keeps a device run local, and waits while the realm configuration
/// cannot tell what this node is instead of forwarding anything.
#[tokio::test]
async fn crate_stays_local() -> TestResult<()> {
    let realm = Topology::spawn(2, 1, 1).await?;
    let group_id = realm.seed_group().await?;
    let device = realm.user_node();
    let storage = &device.context.storage_handle;
    let job_id = finished_run(
        (&realm, device),
        group_id,
        Vec::new(),
        RunCrateStatus::Pending,
    )
    .await?;
    let record = read_job_record(&device.context, job_id, None)
        .await?
        .ok_or("the run is stored")?;
    let context = || JobContext {
        driver: device.context.clone(),
        job_id,
        owner_node_id: device.node_id(),
        claim_token: Ulid::from(1u128),
        final_attempt: false,
        cancel: CancellationToken::new(),
        shutdown: CancellationToken::new(),
        progress: ProgressReporter::from_progress(&record.progress),
        range_turns: Default::default(),
    };

    assert!(matches!(
        write_run_crate(&context(), job_id).await,
        JobRunOutcome::Succeeded(JobResultPayload::RunCrate { ref resource }) if resource == "local"
    ));
    assert_eq!(
        read_crate_status(&device.context, job_id).await?,
        Some(RunCrateStatus::Local)
    );
    // The device keeps the run's crate itself, rooted at the crate folder.
    let local = drive(local_run_crate(job_id, record.created_by), &device.context).await?;
    let root = local["@graph"]
        .as_array()
        .ok_or("the crate has a graph")?
        .iter()
        .find(|entity| entity["conformsTo"]["@id"] == "https://w3id.org/ro/wfrun/process/0.5")
        .ok_or("the crate has a root")?;
    assert_eq!(root["@id"], "./");
    let stranger = aruna_core::user::id::UserId::local(Ulid::from(9u128), realm.realm_id);
    assert!(
        drive(local_run_crate(job_id, stranger), &device.context)
            .await
            .is_err()
    );

    put_crate_status(&device.context, job_id, &RunCrateStatus::Pending).await?;
    let target = DocumentTarget::RealmConfig {
        realm_id: realm.realm_id,
    };
    storage
        .send_storage_effect(StorageEffect::Delete {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        write_run_crate(&context(), job_id).await,
        JobRunOutcome::Deferred(_)
    ));
    assert_eq!(
        read_crate_status(&device.context, job_id).await?,
        Some(RunCrateStatus::Pending)
    );
    // Pruning the job record leaves the local crate readable by its owner.
    storage
        .send_storage_effect(StorageEffect::Delete {
            key_space: aruna_core::keyspaces::JOB_KEYSPACE.to_string(),
            key: aruna_core::structs::execution::job::local::job_record_key(job_id),
            txn_id: None,
        })
        .await;
    assert!(
        read_job_record(&device.context, job_id, None)
            .await?
            .is_none()
    );
    assert!(
        drive(local_run_crate(job_id, record.created_by), &device.context)
            .await
            .is_ok()
    );
    Ok(())
}

/// The target bucket is checked at the realm before anything else, so a run without outputs
/// cannot report a publish into a bucket that does not exist.
#[tokio::test]
async fn checks_empty_target() -> TestResult<()> {
    let realm = Topology::spawn(2, 1, 1).await?;
    let group_id = realm.seed_group().await?;
    let device = realm.user_node();
    let server = realm.node(0);
    assert!(
        drive(
            fetch_realm_documents(device.context.node_and_realm(), FETCH_BUDGET),
            &device.context
        )
        .await
        .unwrap_or(false)
    );
    let job_id = finished_run(
        (&realm, device),
        group_id,
        Vec::new(),
        RunCrateStatus::Local,
    )
    .await?;

    let refused = drive(
        publish_run(
            device.context.node_and_realm(),
            PublishRunInput {
                job_id,
                owner: realm.user_id,
                node_id: device.node_id(),
                target: RemoteBinding {
                    node_id: server.node_id(),
                    bucket: "missing-bucket".to_string(),
                    prefix: String::new(),
                },
            },
        ),
        &device.context,
    )
    .await;
    assert!(matches!(
        refused,
        Err(PublishRunError::Refused {
            refusal: SyncRefusal::NotFound,
            ..
        })
    ));
    assert_eq!(
        read_crate_status(&device.context, job_id).await?,
        Some(RunCrateStatus::Local)
    );
    Ok(())
}

/// A crate job stopped after the document exists but before its outcome batch leaves neither
/// `Written` nor the run link; the re-driven job writes both for the same document.
#[tokio::test]
async fn crate_redrive_links() -> TestResult<()> {
    use aruna_core::keyspaces::RUN_LINK_KEYSPACE;
    use aruna_core::structs::execution::job::local::run_crate_key;
    let realm = Topology::spawn(2, 1, 1).await?;
    let group_id = realm.seed_group().await?;
    let server = realm.node(0);
    let storage = &server.context.storage_handle;
    let job_id = finished_run(
        (&realm, server),
        group_id,
        Vec::new(),
        RunCrateStatus::Pending,
    )
    .await?;
    let record = read_job_record(&server.context, job_id, None)
        .await?
        .ok_or("the run is stored")?;
    let context = || JobContext {
        driver: server.context.clone(),
        job_id,
        owner_node_id: server.node_id(),
        claim_token: Ulid::from(1u128),
        final_attempt: false,
        cancel: CancellationToken::new(),
        shutdown: CancellationToken::new(),
        progress: ProgressReporter::from_progress(&record.progress),
        range_turns: Default::default(),
    };
    let JobRunOutcome::Succeeded(JobResultPayload::RunCrate { resource }) =
        write_run_crate(&context(), job_id).await
    else {
        return Err("the crate is written".into());
    };
    let document_id = Ulid::from_string(&resource)?;
    let link = read_run_link(&server.context, job_id)
        .await?
        .ok_or("the run is linked")?;
    assert_eq!(link.run_crate, Some(document_id));
    assert_eq!((link.group_id, link.created_by), (group_id, realm.user_id));

    // The node prunes on its own schedule; a run inside its retention survives a pass.
    prune_job_batch(server.context.as_ref()).await?;
    put_crate_status(
        &server.context,
        job_id,
        &RunCrateStatus::Minted { document_id },
    )
    .await?;
    storage
        .send_storage_effect(StorageEffect::Delete {
            key_space: RUN_LINK_KEYSPACE.to_string(),
            key: run_crate_key(job_id),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        write_run_crate(&context(), job_id).await,
        JobRunOutcome::Succeeded(JobResultPayload::RunCrate { resource: ref again }) if *again == resource
    ));
    assert_eq!(
        read_crate_status(&server.context, job_id).await?,
        Some(RunCrateStatus::Written { resource })
    );
    let link = read_run_link(&server.context, job_id)
        .await?
        .ok_or("the run is linked again")?;
    assert_eq!(link.run_crate, Some(document_id));
    Ok(())
}

/// Publishing a finished local workflow run copies its outputs to the realm bucket, writes its
/// workflow RO-Crate once, replays idempotently, and refuses a target change.
#[tokio::test]
async fn publishes_local_workflow() -> TestResult<()> {
    use aruna_core::keyspaces::WORKFLOW_RUN_KEYSPACE;
    use aruna_core::structs::execution::workflow_run::{
        CacheScope, RunFile, RunOutput, RunState, WorkflowRunRecord, run_key,
    };
    use aruna_ops_work::workflows::runs::provenance::{WriteProvenanceOperation, status};
    use aruna_ops_work::workflows::runs::publish::{
        DevicePublishInput, local_device_crate, publish_device_run,
    };

    let realm = Topology::spawn(2, 1, 1).await?;
    let group_id = realm.seed_group().await?;
    let device = realm.user_node();
    let server = realm.node(0);
    wait_until("the group on the device", device.node_id(), || async {
        drive(
            fetch_realm_documents(device.context.node_and_realm(), FETCH_BUDGET),
            &device.context,
        )
        .await
        .unwrap_or(false)
            && matches!(read_group_auth(device, group_id).await, Ok(Some(_)))
    })
    .await?;
    create_bucket(&server.context, REMOTE_BUCKET, group_id, &realm).await?;
    create_bucket(&device.context, LOCAL_BUCKET, group_id, &realm).await?;
    let version_id = write_output(&realm, group_id).await?;

    let run_id = Ulid::generate();
    let now_ms = aruna_clock::unix_timestamp_millis();
    let record = WorkflowRunRecord {
        run_id,
        realm_id: realm.realm_id,
        group_id,
        workflow_id: Ulid::generate(),
        workflow_name: "assembly".to_owned(),
        version: 1,
        lineage: run_id,
        state: RunState::Succeeded,
        node_id: device.node_id(),
        created_by: realm.user_id,
        created_at_ms: now_ms,
        finished_at_ms: Some(now_ms),
        error: None,
        values_json: "{}".to_owned(),
        inputs_json: None,
        output: Some(RunOutput {
            bucket: LOCAL_BUCKET.to_owned(),
            prefix: String::new(),
            node_id: None,
            dataset_json: None,
        }),
        outputs: vec![RunFile {
            name: "contigs.fasta".to_owned(),
            bucket: Some(LOCAL_BUCKET.to_owned()),
            key: Some("contigs.fasta".to_owned()),
            version_id: Some(version_id.to_string()),
            node_id: Some(device.node_id()),
            size: Some(OUTPUT.len() as u64),
            digest: Some(blake3::hash(OUTPUT).to_hex().to_string()),
        }],
        provenance_crate: true,
        provenance_document_id: None,
        provenance_status: None,
        last_event_seq: 1,
        local_only: true,
        origin: None,
        parent_job_id: None,
        claim_token_digest: None,
        attempts: 1,
        steps: Vec::new(),
        resumes_run_id: None,
        repeats_run_id: None,
        cache_scope: CacheScope::Group,
        placement_site: None,
        protected: false,
        started_at_ms: Some(now_ms),
        secrets: Default::default(),
    };
    device
        .context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: WORKFLOW_RUN_KEYSPACE.to_owned(),
            key: run_key(run_id),
            value: record.to_bytes()?.into(),
            txn_id: None,
        })
        .await;

    let written = drive(
        WriteProvenanceOperation::new(run_id, (device.node_id(), realm.realm_id)),
        &device.context,
    )
    .await?;
    assert_eq!(written.provenance_status.as_deref(), Some(status::LOCAL));
    let local = drive(local_device_crate(run_id, realm.user_id), &device.context).await?;
    assert!(local["@graph"].is_array());

    let input = || DevicePublishInput {
        id: run_id,
        owner: realm.user_id,
        node_id: device.node_id(),
        target: RemoteBinding {
            node_id: server.node_id(),
            bucket: REMOTE_BUCKET.to_owned(),
            prefix: "runs/workflow".to_owned(),
        },
    };
    let published = drive(
        publish_device_run(device.context.node_and_realm(), input()),
        &device.context,
    )
    .await?;
    assert_eq!(published.outputs.len(), 1);
    assert_eq!(
        published.outputs[0].key,
        "runs/workflow/run-out/contigs.fasta"
    );
    assert_eq!(published.outputs[0].node_id, server.node_id());
    assert!(
        matches!(published.run_crate, RunCrateStatus::Written { .. }),
        "expected written crate, got {:?}",
        published.run_crate
    );
    assert_eq!(
        read_remote(&realm, group_id, "runs/workflow/run-out/contigs.fasta").await?,
        OUTPUT
    );

    let replayed = drive(
        publish_device_run(device.context.node_and_realm(), input()),
        &device.context,
    )
    .await?;
    assert_eq!(
        replayed.outputs[0].version_id,
        published.outputs[0].version_id
    );
    assert_eq!(replayed.run_crate, published.run_crate);

    let mut moved = input();
    moved.target.prefix = "runs/other".to_owned();
    assert!(matches!(
        drive(
            publish_device_run(device.context.node_and_realm(), moved),
            &device.context
        )
        .await,
        Err(PublishRunError::TargetChanged)
    ));
    Ok(())
}
