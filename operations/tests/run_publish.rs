//! Tests that a finished local run reaches the realm only when its owner publishes it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::topology;

use std::time::{Duration, SystemTime};

use aruna_core::document::DocumentTarget;
use aruna_core::effects::StorageEffect;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::execution::job::{
    ExecutionSpec, JobId, JobPayload, JobRecord, JobResultPayload, JobState, OutputObject,
    RunCrateStatus,
};
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::routing::RoutingSnapshot;
use aruna_core::structs::{RemoteBinding, SyncRefusal};
use aruna_core::types::GroupId;
use aruna_operations::device::publish_run::{PublishRunError, PublishRunInput, publish_run};
use aruna_operations::device::realm_documents::fetch_realm_documents;
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::jobs::executor::{JobContext, JobRunOutcome, ProgressReporter};
use aruna_operations::jobs::store::{
    insert_job, put_crate_status, read_crate_status, read_job_record,
};
use aruna_operations::jobs::workflow::run_crate::write_run_crate;
use aruna_operations::s3::bucket::create::CreateBucketOperation;
use aruna_operations::s3::object::get::{GetObjectInput, GetObjectOperation};
use aruna_operations::s3::object::put::{PutObjectConfig, PutObjectInput, PutObjectOperation};
use futures_util::StreamExt;
use tokio_util::sync::CancellationToken;
use topology::{TestResult, Topology, read_group_auth, wait_until};
use ulid::Ulid;

const REMOTE_BUCKET: &str = "lab-data";
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
    realm: &Topology,
    group_id: GroupId,
    outputs: Vec<OutputObject>,
    status: RunCrateStatus,
) -> TestResult<JobId> {
    let device = realm.user_node();
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
    record.finished_at_ms = Some(2);
    record.result = Some(JobResultPayload::Execution {
        exit_code: Some(0),
        workspace_bucket: None,
        outputs,
        stdout: String::new(),
        stderr: String::new(),
        output_digest: None,
    });
    let storage = &device.context.storage_handle;
    insert_job(storage, &record).await?;
    put_crate_status(storage, job_id, &status).await?;
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
        fetch_realm_documents(&device.context, FETCH_BUDGET).await
            && matches!(read_group_auth(device, group_id).await, Ok(Some(_)))
    })
    .await?;
    create_bucket(&server.context, REMOTE_BUCKET, group_id, &realm).await?;
    create_bucket(&device.context, LOCAL_BUCKET, group_id, &realm).await?;
    let version_id = write_output(&realm, group_id).await?;
    let job_id = finished_run(
        &realm,
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

    let published = publish_run(&device.context, input()).await?;
    assert_eq!(published.outputs.len(), 1);
    assert_eq!(
        published.outputs[0].key,
        "runs/assembly/run-out/contigs.fasta"
    );
    assert_eq!(published.outputs[0].node_id, server.node_id());
    assert!(matches!(
        published.run_crate,
        RunCrateStatus::Written { .. }
    ));
    assert_eq!(
        read_remote(&realm, group_id, "runs/assembly/run-out/contigs.fasta").await?,
        OUTPUT
    );

    let replayed = publish_run(&device.context, input()).await?;
    assert_eq!(
        replayed.outputs[0].version_id,
        published.outputs[0].version_id
    );
    assert_eq!(replayed.run_crate, published.run_crate);
    assert_eq!(
        read_crate_status(&device.context.storage_handle, job_id).await?,
        Some(published.run_crate)
    );

    // The first target is frozen: another prefix would leave the crate naming the first copies.
    let mut moved = input();
    moved.target.prefix = "runs/other".to_string();
    assert!(matches!(
        publish_run(&device.context, moved).await,
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
    let job_id = finished_run(&realm, group_id, Vec::new(), RunCrateStatus::Pending).await?;
    let record = read_job_record(storage, job_id, None)
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
    };

    assert!(matches!(
        write_run_crate(&context(), job_id).await,
        JobRunOutcome::Succeeded(JobResultPayload::RunCrate { ref resource }) if resource == "local"
    ));
    assert_eq!(
        read_crate_status(storage, job_id).await?,
        Some(RunCrateStatus::Local)
    );

    put_crate_status(storage, job_id, &RunCrateStatus::Pending).await?;
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
        read_crate_status(storage, job_id).await?,
        Some(RunCrateStatus::Pending)
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
    assert!(fetch_realm_documents(&device.context, FETCH_BUDGET).await);
    let job_id = finished_run(&realm, group_id, Vec::new(), RunCrateStatus::Local).await?;

    let refused = publish_run(
        &device.context,
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
        read_crate_status(&device.context.storage_handle, job_id).await?,
        Some(RunCrateStatus::Local)
    );
    Ok(())
}
