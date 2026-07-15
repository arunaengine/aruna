//! End-to-end compute-layer tests against a real Docker daemon and a real S3
//! endpoint. Each test skips with a written message when no daemon is reachable,
//! so a CI box without Docker records the skip rather than failing.

mod shared;

use std::sync::Arc;
use std::time::{Duration, Instant};

use aruna_compute::ExecutorRegistry;
use aruna_compute::backend::ExecutorBackend;
use aruna_compute::docker::{DockerBackend, DockerConfig};
use aruna_core::structs::{
    ComputeResources, ExecutionSpec, InputMode, InputSelection, InputSource, JobId, JobPayload,
    JobRecord, JobState, RunCrateStatus,
};
use aruna_operations::driver::DriverContext;
use aruna_operations::jobs::reconcile::ExternalReconciler;
use aruna_operations::jobs::store::{
    ClaimOutcome, JobMutation, claim_job, insert_job, mutate_job, read_job_record,
    read_run_crate_status,
};
use aruna_operations::jobs::workflow::reconcile::ComputeReconciler;
use aruna_operations::jobs::workflow::run_execution_job;
use aws_sdk_s3::primitives::ByteStream;
use shared::{
    S3Credentials, TestResult, create_bearer_token, create_group_via_http,
    create_s3_credentials_via_http, s3_client, spawn_full_seed_node, wait_for_group_via_http,
};
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

/// A reachable, healthy Docker daemon, or `None` (test skips).
async fn docker_or_skip() -> Option<DockerBackend> {
    let config = DockerConfig {
        // Host networking lets the container reach the loopback S3 endpoint.
        network_mode: Some("host".to_string()),
        keep_failed: std::env::var("ARUNA_KEEP_FAILED").is_ok(),
        ..DockerConfig::default()
    };
    match DockerBackend::with_config(config) {
        Ok(backend) => match backend.health().await {
            Ok(()) => Some(backend),
            Err(error) => {
                eprintln!("skipping compute test: docker daemon unhealthy: {error}");
                None
            }
        },
        Err(error) => {
            eprintln!("skipping compute test: no reachable docker daemon: {error}");
            None
        }
    }
}

struct Fixture {
    seed: shared::SeedNode,
    compute_ctx: Arc<DriverContext>,
    group_id: Ulid,
    s3: S3Credentials,
    endpoint: shared::S3Endpoint,
    source_bucket: String,
}

/// Full node + Docker-backed compute context + a group with a source object staged.
async fn setup(backend: DockerBackend) -> TestResult<Fixture> {
    let seed = spawn_full_seed_node().await?;
    let endpoint = seed.s3.clone().expect("full node exposes S3");

    let bearer = create_bearer_token(
        &seed.context,
        seed.user_id,
        seed.realm_id,
        seed.capabilities.clone(),
    )
    .await?;
    let group = create_group_via_http(&seed.base_url, &bearer, "compute-e2e").await?;
    wait_for_group_via_http(&seed.base_url, &bearer, &group.group_id).await?;
    let group_id = Ulid::from_string(&group.group_id)?;
    let creds = create_s3_credentials_via_http(&seed.base_url, &bearer, &group.group_id).await?;

    // A source object the workflow snapshots into the workspace.
    let source_bucket = format!("src-{}", Ulid::r#gen().to_string().to_lowercase());
    let client = s3_client(&endpoint, &creds);
    client.create_bucket().bucket(&source_bucket).send().await?;
    client
        .put_object()
        .bucket(&source_bucket)
        .key("data.txt")
        .body(ByteStream::from_static(b"payload-from-source"))
        .send()
        .await?;

    // Compute-enabled context sharing the node's storage/net/blob/metadata. The
    // container reaches the loopback S3 server over host networking; force IPv4 so
    // `localhost`->`::1` cannot miss the IPv4-bound endpoint.
    let container_endpoint = endpoint.endpoint_url.replace("localhost", "127.0.0.1");
    let registry = ExecutorRegistry::new()
        .with_backend(Arc::new(backend))
        .with_workspace_endpoint(Some(container_endpoint), "eu-central-1".to_string());
    let mut ctx = (*seed.context).clone();
    ctx.compute_handle = Some(Arc::new(registry));
    let compute_ctx = Arc::new(ctx);

    Ok(Fixture {
        seed,
        compute_ctx,
        group_id,
        s3: creds,
        endpoint,
        source_bucket,
    })
}

fn execution_spec(
    fixture: &Fixture,
    image: &str,
    entrypoint: Option<Vec<String>>,
    command: Vec<String>,
) -> ExecutionSpec {
    ExecutionSpec {
        group_id: fixture.group_id,
        image: image.to_string(),
        entrypoint,
        command,
        env: Default::default(),
        resources: ComputeResources {
            cpu_cores: Some(1),
            ram_bytes: Some(512 * 1024 * 1024),
            max_walltime_ms: Some(120_000),
        },
        executor_constraint: Some("docker".to_string()),
        inputs: vec![InputSelection {
            source: InputSource::S3 {
                bucket: fixture.source_bucket.clone(),
                key: "data.txt".to_string(),
                version_id: None,
            },
            dest_key: "inputs/data.txt".to_string(),
            mode: InputMode::Snapshot,
        }],
        output_prefixes: vec!["outputs/".to_string()],
    }
}

/// Insert a queued execution job and claim it, returning the claimed record. The
/// job is inserted already-claimed so the seed node's background drain (which has
/// no compute backend) never races us for it.
async fn claim_execution(fixture: &Fixture, spec: ExecutionSpec) -> (JobId, JobRecord) {
    let ctx = fixture.compute_ctx.as_ref();
    let node_id = ctx.net_handle.as_ref().unwrap().node_id();
    let job_id = JobId::new();
    let record = JobRecord::new(
        job_id,
        JobPayload::Execution(spec),
        fixture.seed.user_id,
        node_id,
        now_ms(),
        now_ms(),
        None,
    );
    insert_job(&ctx.storage_handle, &record).await.unwrap();
    let ClaimOutcome::Claimed(claimed) = claim_job(&ctx.storage_handle, job_id, node_id, now_ms())
        .await
        .unwrap()
    else {
        panic!("claim failed");
    };
    (job_id, claimed)
}

fn now_ms() -> u64 {
    aruna_core::util::unix_timestamp_millis()
}

async fn wait_state(
    ctx: &DriverContext,
    job_id: JobId,
    want: JobState,
    timeout: Duration,
) -> JobState {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(Some(record)) = read_job_record(&ctx.storage_handle, job_id, None).await
            && (record.state == want || (record.state.is_terminal() && want.is_terminal()))
        {
            return record.state;
        }
        if Instant::now() >= deadline {
            let state = read_job_record(&ctx.storage_handle, job_id, None)
                .await
                .ok()
                .flatten()
                .map(|r| r.state);
            panic!("timed out waiting for {want:?}, last state {state:?}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn count_containers(job_id: JobId) -> usize {
    let output = tokio::process::Command::new("docker")
        .args([
            "ps",
            "-a",
            "--filter",
            &format!("name=aruna-{}", job_id.to_string().to_lowercase()),
            "-q",
        ])
        .output()
        .await
        .expect("docker ps");
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|line| !line.trim().is_empty())
        .count()
}

async fn wait_run_crate(
    ctx: &DriverContext,
    job_id: JobId,
    timeout: Duration,
) -> Option<RunCrateStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(Some(status)) = read_run_crate_status(&ctx.storage_handle, job_id).await {
            return Some(status);
        }
        if Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

// Full happy path: workspace bucket, staged input, a container that reads the
// input and writes an output via workspace S3 creds, terminal success, durable
// output, and a run crate.
#[tokio::test]
async fn execution_end_to_end() -> TestResult<()> {
    let Some(backend) = docker_or_skip().await else {
        return Ok(());
    };
    let fixture = setup(backend).await?;

    // aws-cli reads the staged input and writes an output back, both via the
    // injected workspace credentials against the node's S3 endpoint.
    // The security baseline drops all caps, so only /tmp is writable; keep aws
    // config and HOME there and force path-style addressing.
    let script = "\
set -e
export HOME=/tmp
export AWS_CONFIG_FILE=/tmp/awscfg
export AWS_EC2_METADATA_DISABLED=true
printf '[default]\\ns3 =\\n    addressing_style = path\\n' > /tmp/awscfg
aws --endpoint-url \"$AWS_ENDPOINT_URL\" s3 cp \"s3://$ARUNA_WORKSPACE_BUCKET/inputs/data.txt\" /tmp/in
tr a-z A-Z < /tmp/in > /tmp/out
aws --endpoint-url \"$AWS_ENDPOINT_URL\" s3 cp /tmp/out \"s3://$ARUNA_WORKSPACE_BUCKET/outputs/result.txt\"
";
    let spec = execution_spec(
        &fixture,
        "amazon/aws-cli:latest",
        Some(vec!["/bin/sh".to_string(), "-c".to_string()]),
        vec![script.to_string()],
    );
    let (job_id, record) = claim_execution(&fixture, spec).await;

    run_execution_job(
        fixture.compute_ctx.clone(),
        record,
        CancellationToken::new(),
    )
    .await;

    let state = wait_state(
        &fixture.compute_ctx,
        job_id,
        JobState::Succeeded,
        Duration::from_secs(180),
    )
    .await;
    if state != JobState::Succeeded {
        let logs = tokio::process::Command::new("docker")
            .args([
                "logs",
                &format!("aruna-{}-a0", job_id.to_string().to_lowercase()),
            ])
            .output()
            .await
            .map(|o| {
                format!(
                    "STDOUT:\n{}\nSTDERR:\n{}",
                    String::from_utf8_lossy(&o.stdout),
                    String::from_utf8_lossy(&o.stderr)
                )
            })
            .unwrap_or_default();
        eprintln!("=== container logs ===\n{logs}");
    }
    assert_eq!(state, JobState::Succeeded, "container job must succeed");

    // The output the container wrote is durable in the run bucket.
    let bucket = JobRecord::workspace_bucket_name(job_id);
    let client = s3_client(&fixture.endpoint, &fixture.s3);
    let output = client
        .get_object()
        .bucket(&bucket)
        .key("outputs/result.txt")
        .send()
        .await
        .expect("output object durable in workspace");
    let body = output.body.collect().await.unwrap().into_bytes();
    assert_eq!(&body[..], b"PAYLOAD-FROM-SOURCE");

    // The staged input is durable too.
    client
        .get_object()
        .bucket(&bucket)
        .key("inputs/data.txt")
        .send()
        .await
        .expect("staged input durable in workspace");

    // The run crate obligation ran and wrote a crate at runs/{JobId}.
    let crate_status = wait_run_crate(&fixture.compute_ctx, job_id, Duration::from_secs(30)).await;
    eprintln!("run crate status: {crate_status:?}");
    assert!(
        matches!(crate_status, Some(RunCrateStatus::Written { .. })),
        "run crate must be written, got {crate_status:?}"
    );

    fixture.seed.shutdown().await;
    Ok(())
}

// A cancel mid-run terminalizes as Cancelled (evidence-correlated).
#[tokio::test]
async fn execution_cancel_terminalizes() -> TestResult<()> {
    let Some(backend) = docker_or_skip().await else {
        return Ok(());
    };
    let fixture = setup(backend).await?;
    let spec = execution_spec(
        &fixture,
        "busybox:latest",
        None,
        vec!["sh".to_string(), "-c".to_string(), "sleep 120".to_string()],
    );
    let (job_id, record) = claim_execution(&fixture, spec).await;

    let cancel = CancellationToken::new();
    let ctx = fixture.compute_ctx.clone();
    let handle = tokio::spawn(async move {
        run_execution_job(ctx, record, cancel).await;
    });

    // Wait until the container is running, then cancel.
    wait_state(
        &fixture.compute_ctx,
        job_id,
        JobState::Running,
        Duration::from_secs(90),
    )
    .await;
    aruna_operations::jobs::store::set_cancel_requested(
        &fixture.compute_ctx.storage_handle,
        job_id,
        now_ms(),
    )
    .await
    .unwrap();

    let state = wait_state(
        &fixture.compute_ctx,
        job_id,
        JobState::Cancelled,
        Duration::from_secs(60),
    )
    .await;
    assert_eq!(
        state,
        JobState::Cancelled,
        "cancel must terminalize as Cancelled"
    );
    let _ = handle.await;

    fixture.seed.shutdown().await;
    Ok(())
}

// A node restart mid-run: the reconciler adopts the running container and does
// NOT start a second one.
#[tokio::test]
async fn execution_restart_adopts() -> TestResult<()> {
    let Some(backend) = docker_or_skip().await else {
        return Ok(());
    };
    let fixture = setup(backend).await?;
    let spec = execution_spec(
        &fixture,
        "busybox:latest",
        None,
        vec!["sh".to_string(), "-c".to_string(), "sleep 20".to_string()],
    );
    let (job_id, record) = claim_execution(&fixture, spec).await;

    // Supervise, then simulate a crash by dropping the supervisor task.
    let ctx = fixture.compute_ctx.clone();
    let crashed = tokio::spawn(async move {
        run_execution_job(ctx, record, CancellationToken::new()).await;
    });
    wait_state(
        &fixture.compute_ctx,
        job_id,
        JobState::Running,
        Duration::from_secs(90),
    )
    .await;
    crashed.abort();
    let _ = crashed.await;
    assert_eq!(
        count_containers(job_id).await,
        1,
        "exactly one container before adopt"
    );

    // In production the reconciler is only handed a job after the lease sweep
    // observes its expired lease; a live lease means the holder is still alive and
    // must not be adopted. Simulate the sweep by expiring the crashed holder's lease.
    mutate_job(&fixture.compute_ctx.storage_handle, job_id, |record| {
        if let Some(claim) = record.claim.as_mut() {
            claim.lease_expires_at_ms = 1;
        }
        Ok(JobMutation::Persist)
    })
    .await
    .expect("expire crashed lease");

    // The reconciler adopts the still-running container.
    let lost = read_job_record(&fixture.compute_ctx.storage_handle, job_id, None)
        .await
        .unwrap()
        .unwrap();
    let reconciler = ComputeReconciler::new(fixture.compute_ctx.clone());
    reconciler
        .reconcile_lost_attempt(&fixture.compute_ctx.storage_handle, lost)
        .await;

    // Still exactly one container: no double-run.
    assert_eq!(
        count_containers(job_id).await,
        1,
        "adoption must not double-run"
    );

    // The adopted supervision drives it to a terminal success.
    let state = wait_state(
        &fixture.compute_ctx,
        job_id,
        JobState::Succeeded,
        Duration::from_secs(90),
    )
    .await;
    assert_eq!(state, JobState::Succeeded);

    fixture.seed.shutdown().await;
    Ok(())
}
