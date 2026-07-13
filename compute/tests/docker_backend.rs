//! Integration tests against a real Docker daemon. Each test skips with a
//! clear message when no daemon is reachable, but is written to run for real.
#![cfg(feature = "docker")]

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aruna_compute::backend::ExecutorBackend;
use aruna_compute::docker::{DockerBackend, DockerConfig};
use aruna_compute::logs::{LogSink, LogStream};
use aruna_compute::spec::{AttemptRef, LogLimits, TaskSpec};
use aruna_compute::status::{AttemptPhase, CancelEvidence, ReconcileOutcome};
use tokio_util::sync::CancellationToken;

const IMAGE: &str = "alpine:3.24";

/// Connect + health-check, or `None` when no daemon is available. Uses network
/// mode `none`: these containers need no egress and it keeps the tests hermetic.
async fn daemon() -> Option<DockerBackend> {
    let config = DockerConfig {
        network_mode: Some("none".to_string()),
        ..DockerConfig::default()
    };
    let backend = DockerBackend::with_config(config).ok()?;
    match backend.health().await {
        Ok(()) => Some(backend),
        Err(e) => {
            eprintln!("skipping docker test: daemon unhealthy: {e}");
            None
        }
    }
}

macro_rules! backend_or_skip {
    () => {
        match daemon().await {
            Some(b) => b,
            None => {
                eprintln!("skipping docker test: no reachable docker daemon");
                return;
            }
        }
    };
}

fn unique(prefix: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}-{t}-{n}")
}

fn sh(job: &str, script: &str) -> TaskSpec {
    let mut spec = TaskSpec::new(AttemptRef::new(job, 0), IMAGE);
    spec.command = vec!["sh".into(), "-c".into(), script.into()];
    spec
}

async fn wait_running(backend: &DockerBackend, attempt: &AttemptRef) {
    for _ in 0..200 {
        let status = backend.status(attempt).await.unwrap();
        if matches!(status.phase, AttemptPhase::Running) || status.is_terminal() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("container never reached running/terminal");
}

#[tokio::test]
async fn idempotent_submit() {
    // Submitting the same external name twice adopts the one container.
    let backend = backend_or_skip!();
    let job = unique("idem");
    let spec = sh(&job, "sleep 60");
    let attempt = spec.attempt.clone();

    let first = backend.submit(&spec).await.unwrap();
    let second = backend.submit(&spec).await.unwrap();
    assert_eq!(
        first.backend_ref, second.backend_ref,
        "second submit must adopt the same container id, not start a new run"
    );

    // The daemon can only hold one container for a name; confirm exactly one.
    let listed = list_by_name(&attempt.external_name()).await;
    assert_eq!(listed, 1, "exactly one container must exist for the name");

    let _ = backend.cleanup(&attempt).await;
}

#[tokio::test]
async fn exit_code_capture() {
    // Distinct exit codes surface as distinct terminal evidence.
    let backend = backend_or_skip!();
    let cancel = CancellationToken::new();

    let ok = sh(&unique("exit0"), "exit 0");
    let ok_attempt = ok.attempt.clone();
    backend.submit(&ok).await.unwrap();
    let ok_status = backend.wait(&ok_attempt, &cancel).await.unwrap();
    assert_eq!(ok_status.phase, AttemptPhase::Exited { code: 0 });

    let bad = sh(&unique("exit42"), "exit 42");
    let bad_attempt = bad.attempt.clone();
    backend.submit(&bad).await.unwrap();
    let bad_status = backend.wait(&bad_attempt, &cancel).await.unwrap();
    assert_eq!(bad_status.phase, AttemptPhase::Exited { code: 42 });

    let _ = backend.cleanup(&ok_attempt).await;
    let _ = backend.cleanup(&bad_attempt).await;
}

#[tokio::test]
async fn cancel_stops() {
    // A long-running container is stopped with definitive evidence.
    let backend = backend_or_skip!();
    let spec = sh(&unique("cancel"), "sleep 300");
    let attempt = spec.attempt.clone();

    backend.submit(&spec).await.unwrap();
    wait_running(&backend, &attempt).await;

    let evidence = backend.cancel(&attempt).await.unwrap();
    match evidence {
        CancelEvidence::Stopped(status) => {
            assert_eq!(status.phase, AttemptPhase::Cancelled);
        }
        other => panic!("expected Stopped evidence, got {other:?}"),
    }
    // No longer running.
    let status = backend.status(&attempt).await.unwrap();
    assert!(status.is_terminal(), "cancelled container must be terminal");

    let _ = backend.cleanup(&attempt).await;
}

#[tokio::test]
async fn reconcile_adopts() {
    // A fresh backend (simulating restart) re-adopts a running container by name.
    let submitter = backend_or_skip!();
    let spec = sh(&unique("adopt"), "sleep 120");
    let attempt = spec.attempt.clone();
    submitter.submit(&spec).await.unwrap();
    wait_running(&submitter, &attempt).await;

    // Drop the submitting handle; a brand-new backend has no in-memory state.
    drop(submitter);
    let recovered = backend_or_skip!();
    match recovered.reconcile(&attempt).await {
        ReconcileOutcome::Found(status) => {
            assert_eq!(status.phase, AttemptPhase::Running);
        }
        other => panic!("expected Found(Running) on adopt, got {other:?}"),
    }

    // A name that was never submitted reconciles to NotFound.
    let missing = AttemptRef::new(unique("ghost"), 0);
    assert!(matches!(
        recovered.reconcile(&missing).await,
        ReconcileOutcome::NotFound
    ));

    let _ = recovered.cleanup(&attempt).await;
}

#[derive(Default)]
struct CountingSink {
    stdout: std::sync::atomic::AtomicU64,
}

impl LogSink for CountingSink {
    fn write(&self, stream: LogStream, chunk: &[u8]) {
        if stream == LogStream::Stdout {
            self.stdout.fetch_add(chunk.len() as u64, Ordering::Relaxed);
        }
    }
}

#[tokio::test]
async fn log_bounds() {
    // A chatty container's captured tail is bounded; totals show truncation.
    let backend = backend_or_skip!();
    let cancel = CancellationToken::new();
    let spec = sh(
        &unique("logs"),
        "i=0; while [ $i -lt 5000 ]; do echo aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa; i=$((i+1)); done",
    );
    let mut spec = spec;
    spec.log_limits = LogLimits {
        max_bytes_per_stream: 4096,
        inline_tail_bytes: 1024,
    };
    let attempt = spec.attempt.clone();

    backend.submit(&spec).await.unwrap();
    backend.wait(&attempt, &cancel).await.unwrap();

    let sink = CountingSink::default();
    let tails = backend
        .fetch_logs(&attempt, &spec.log_limits, &sink)
        .await
        .unwrap();

    assert!(
        tails.stdout.len() <= 4096,
        "captured tail must respect the cap, got {}",
        tails.stdout.len()
    );
    assert!(
        tails.stdout_truncated,
        "a chatty stream must report truncation"
    );
    assert!(
        tails.stdout_total > 4096,
        "true total must exceed the cap, got {}",
        tails.stdout_total
    );
    // The sink saw the full stream, not just the bounded tail.
    assert!(sink.stdout.load(Ordering::Relaxed) > 4096);

    let _ = backend.cleanup(&attempt).await;
}

#[tokio::test]
async fn resource_limits() {
    // Memory / CPU / pids ceilings are actually applied to the container config.
    let backend = backend_or_skip!();
    let mut spec = sh(&unique("limits"), "sleep 30");
    spec.resources.ram_bytes = Some(64 * 1024 * 1024);
    spec.resources.cpu_cores = Some(1);
    let attempt = spec.attempt.clone();

    backend.submit(&spec).await.unwrap();
    let inspect = backend.inspect(&attempt).await.unwrap();
    let host = inspect.host_config.expect("host_config present");
    assert_eq!(host.memory, Some(64 * 1024 * 1024));
    assert_eq!(host.nano_cpus, Some(1_000_000_000));
    assert_eq!(host.pids_limit, Some(backend.config().pids_limit));
    assert_eq!(host.cap_drop, Some(vec!["ALL".to_string()]));

    let _ = backend.cleanup(&attempt).await;
}

#[tokio::test]
async fn foreign_not_adopted() {
    // A same-named container created outside Aruna (no aruna.io/* labels) is
    // neither adopted as evidence nor removed by cleanup.
    let backend = backend_or_skip!();
    // Warm the image so the bare create below cannot 404.
    let warmup = sh(&unique("warmup"), "true");
    backend.submit(&warmup).await.unwrap();
    backend
        .wait(&warmup.attempt, &CancellationToken::new())
        .await
        .unwrap();
    backend.cleanup(&warmup.attempt).await.unwrap();

    use bollard::Docker;
    use bollard::models::ContainerCreateBody;
    use bollard::query_parameters::{CreateContainerOptionsBuilder, RemoveContainerOptionsBuilder};
    let spec = sh(&unique("foreign"), "sleep 30");
    let attempt = spec.attempt.clone();
    let docker = Docker::connect_with_defaults().unwrap();
    let opts = CreateContainerOptionsBuilder::new()
        .name(&attempt.external_name())
        .build();
    docker
        .create_container(
            Some(opts),
            ContainerCreateBody {
                image: Some(IMAGE.to_string()),
                cmd: Some(vec!["sleep".to_string(), "30".to_string()]),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    match backend.reconcile(&attempt).await {
        ReconcileOutcome::Unavailable(_) => {}
        other => panic!("foreign container must not reconcile, got {other:?}"),
    }
    assert!(
        backend.submit(&spec).await.is_err(),
        "submit must not adopt a foreign name collision"
    );
    assert!(
        backend.cleanup(&attempt).await.is_err(),
        "cleanup must not remove a foreign container"
    );

    docker
        .remove_container(
            &attempt.external_name(),
            Some(RemoveContainerOptionsBuilder::new().force(true).build()),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn walltime_enforced() {
    // A run past its walltime ceiling is stopped and surfaces a backend failure
    // instead of running forever.
    let backend = backend_or_skip!();
    let mut spec = sh(&unique("wall"), "sleep 300");
    spec.resources.max_walltime = Some(Duration::from_secs(1));
    let attempt = spec.attempt.clone();

    backend.submit(&spec).await.unwrap();
    let status = tokio::time::timeout(
        Duration::from_secs(60),
        backend.wait(&attempt, &CancellationToken::new()),
    )
    .await
    .expect("wait must terminalize an over-walltime run")
    .unwrap();
    assert!(
        matches!(status.phase, AttemptPhase::Failed { .. }),
        "expected walltime failure, got {:?}",
        status.phase
    );

    let _ = backend.cleanup(&attempt).await;
}

/// Count containers (any state) carrying the given name, via an independent client.
async fn list_by_name(name: &str) -> usize {
    use bollard::Docker;
    use bollard::query_parameters::ListContainersOptionsBuilder;
    use std::collections::HashMap;

    let docker = Docker::connect_with_defaults().unwrap();
    let mut filters: HashMap<String, Vec<String>> = HashMap::new();
    filters.insert("name".to_string(), vec![name.to_string()]);
    let opts = ListContainersOptionsBuilder::new()
        .all(true)
        .filters(&filters)
        .build();
    let list = docker.list_containers(Some(opts)).await.unwrap();
    // Docker name filter is a substring match; count exact matches on the
    // leading-slash canonical name.
    list.iter()
        .filter(|c| {
            c.names
                .as_ref()
                .map(|ns| ns.iter().any(|n| n == &format!("/{name}")))
                .unwrap_or(false)
        })
        .count()
}
