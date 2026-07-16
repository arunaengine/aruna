//! Integration tests against a real Docker daemon. Each test skips with a
//! clear message when no daemon is reachable, but is written to run for real.
#![cfg(feature = "docker")]

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aruna_compute::DockerConfig;
use aruna_compute::executor::docker::DockerBackend;
use aruna_compute::executor::logs::LogSink;
use aruna_core::compute::{
    AttemptPhase, AttemptRef, AttemptStatus, BackendError, CancelEvidence, FenceContext, LogLimits,
    LogStream, LogTails, ReconcileEvidence, TaskInput, TaskOutput, TaskSpec,
};
use bytes::Bytes;
use futures_util::StreamExt;
use tokio_util::sync::CancellationToken;

fn fence(attempt: &AttemptRef) -> FenceContext {
    FenceContext {
        attempt: attempt.clone(),
        attempt_epoch: 1,
        controller_generation: 1,
    }
}

#[async_trait::async_trait]
trait DockerTestExt {
    async fn submit(
        &self,
        spec: &TaskSpec,
        cancel: &CancellationToken,
    ) -> Result<AttemptStatus, BackendError>;
    async fn status(&self, attempt: &AttemptRef) -> Result<AttemptStatus, BackendError>;
    async fn wait(
        &self,
        attempt: &AttemptRef,
        cancel: &CancellationToken,
    ) -> Result<AttemptStatus, BackendError>;
    async fn cancel(&self, attempt: &AttemptRef) -> Result<CancelEvidence, BackendError>;
    async fn fetch_logs(
        &self,
        attempt: &AttemptRef,
        limits: &LogLimits,
        sink: &dyn LogSink,
    ) -> Result<LogTails, BackendError>;
    async fn fetch_output(
        &self,
        attempt: &AttemptRef,
        path: &str,
    ) -> Result<TaskOutput, BackendError>;
    async fn reconcile(&self, attempt: &AttemptRef) -> ReconcileEvidence;
    async fn cleanup(&self, attempt: &AttemptRef) -> Result<(), BackendError>;
}

#[async_trait::async_trait]
impl DockerTestExt for DockerBackend {
    async fn submit(
        &self,
        spec: &TaskSpec,
        cancel: &CancellationToken,
    ) -> Result<AttemptStatus, BackendError> {
        aruna_compute::ExecutorBackend::submit(self, &fence(&spec.attempt), spec, cancel).await
    }

    async fn status(&self, attempt: &AttemptRef) -> Result<AttemptStatus, BackendError> {
        aruna_compute::ExecutorBackend::status(self, &fence(attempt)).await
    }

    async fn wait(
        &self,
        attempt: &AttemptRef,
        cancel: &CancellationToken,
    ) -> Result<AttemptStatus, BackendError> {
        aruna_compute::ExecutorBackend::wait(self, &fence(attempt), cancel).await
    }

    async fn cancel(&self, attempt: &AttemptRef) -> Result<CancelEvidence, BackendError> {
        aruna_compute::ExecutorBackend::cancel(self, &fence(attempt)).await
    }

    async fn fetch_logs(
        &self,
        attempt: &AttemptRef,
        limits: &LogLimits,
        sink: &dyn LogSink,
    ) -> Result<LogTails, BackendError> {
        aruna_compute::ExecutorBackend::fetch_logs(self, &fence(attempt), limits, sink).await
    }

    async fn fetch_output(
        &self,
        attempt: &AttemptRef,
        path: &str,
    ) -> Result<TaskOutput, BackendError> {
        aruna_compute::ExecutorBackend::fetch_output(self, &fence(attempt), path).await
    }

    async fn reconcile(&self, attempt: &AttemptRef) -> ReconcileEvidence {
        aruna_compute::ExecutorBackend::reconcile(self, &fence(attempt)).await
    }

    async fn cleanup(&self, attempt: &AttemptRef) -> Result<(), BackendError> {
        aruna_compute::ExecutorBackend::cleanup(self, &fence(attempt)).await
    }
}

/// Fetch one output and collect its streamed chunks.
async fn read_output(backend: &DockerBackend, attempt: &AttemptRef, path: &str) -> Vec<u8> {
    let output = backend.fetch_output(attempt, path).await.unwrap();
    let mut chunks = output.chunks;
    let mut bytes = Vec::new();
    while let Some(chunk) = chunks.next().await {
        bytes.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(bytes.len() as u64, output.size, "declared size must match");
    bytes
}

const IMAGE: &str = "alpine:3.24";

/// Connect + health-check, or `None` when no daemon is available. Uses network
/// mode `none`: these containers need no egress and it keeps the tests hermetic.
async fn daemon() -> Option<DockerBackend> {
    let config = DockerConfig {
        state_root: std::env::temp_dir()
            .join("aruna-compute-tests")
            .join(unique("state")),
        network_mode: Some("none".to_string()),
        ..DockerConfig::default()
    };
    let backend = DockerBackend::with_config(config).ok()?;
    match aruna_compute::ExecutorBackend::health(&backend).await {
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

    let first = backend
        .submit(&spec, &CancellationToken::new())
        .await
        .unwrap();
    let second = backend
        .submit(&spec, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        first.backend_ref, second.backend_ref,
        "second submit must adopt the same container id, not start a new run"
    );

    // The daemon can only hold one container for a name; confirm exactly one.
    let listed = list_by_name(&attempt.external_name()).await;
    assert_eq!(listed, 1, "exactly one container must exist for the name");

    backend.cleanup(&attempt).await.unwrap();
    backend.cleanup(&attempt).await.unwrap();
}

#[tokio::test]
async fn exit_code_capture() {
    // Distinct exit codes surface as distinct terminal evidence.
    let backend = backend_or_skip!();
    let cancel = CancellationToken::new();

    let ok = sh(&unique("exit0"), "exit 0");
    let ok_attempt = ok.attempt.clone();
    backend.submit(&ok, &cancel).await.unwrap();
    let ok_status = backend.wait(&ok_attempt, &cancel).await.unwrap();
    assert_eq!(ok_status.phase, AttemptPhase::Exited { code: 0 });

    let bad = sh(&unique("exit42"), "exit 42");
    let bad_attempt = bad.attempt.clone();
    backend.submit(&bad, &cancel).await.unwrap();
    let bad_status = backend.wait(&bad_attempt, &cancel).await.unwrap();
    assert_eq!(bad_status.phase, AttemptPhase::Exited { code: 42 });

    let _ = backend.cleanup(&ok_attempt).await;
    let _ = backend.cleanup(&bad_attempt).await;
}

#[tokio::test]
async fn file_transfer() {
    let backend = backend_or_skip!();
    let cancel = CancellationToken::new();
    let mut spec = sh(
        &unique("files"),
        "test \"$(stat -c %a /tmp)\" = 1777 && cat /tmp/aruna-input.txt > /tmp/aruna-output.txt",
    );
    spec.inputs.push(TaskInput::from_bytes(
        "/tmp/aruna-input.txt",
        b"hello from aruna".to_vec(),
    ));
    spec.output_paths.push("/tmp/aruna-output.txt".to_string());
    let attempt = spec.attempt.clone();

    backend.submit(&spec, &cancel).await.unwrap();
    let status = backend.wait(&attempt, &cancel).await.unwrap();
    assert_eq!(status.phase, AttemptPhase::Exited { code: 0 });
    assert_eq!(
        read_output(&backend, &attempt, "/tmp/aruna-output.txt").await,
        b"hello from aruna"
    );

    let _ = backend.cleanup(&attempt).await;
}

#[tokio::test]
async fn chunked_transfer() {
    // A multi-chunk input stream round-trips through the container and comes
    // back as a multi-chunk output stream, byte for byte.
    let backend = backend_or_skip!();
    let cancel = CancellationToken::new();
    let mut spec = sh(
        &unique("chunks"),
        "cat /tmp/aruna-input.bin > /tmp/aruna-output.bin",
    );
    let chunk_count = 16usize;
    let chunk_len = 64 * 1024usize;
    let mut payload = Vec::with_capacity(chunk_count * chunk_len);
    let chunks: Vec<std::io::Result<Bytes>> = (0..chunk_count)
        .map(|index| {
            let chunk = vec![b'a' + index as u8; chunk_len];
            payload.extend_from_slice(&chunk);
            Ok(Bytes::from(chunk))
        })
        .collect();
    spec.inputs.push(TaskInput::from_stream(
        "/tmp/aruna-input.bin",
        payload.len() as u64,
        Box::pin(futures_util::stream::iter(chunks)),
    ));
    spec.output_paths.push("/tmp/aruna-output.bin".to_string());
    let attempt = spec.attempt.clone();

    backend.submit(&spec, &cancel).await.unwrap();
    let status = backend.wait(&attempt, &cancel).await.unwrap();
    assert_eq!(status.phase, AttemptPhase::Exited { code: 0 });
    assert_eq!(
        read_output(&backend, &attempt, "/tmp/aruna-output.bin").await,
        payload
    );

    let _ = backend.cleanup(&attempt).await;
}

#[tokio::test]
async fn cancelled_submit() {
    let backend = backend_or_skip!();
    let spec = sh(&unique("cancelled-submit"), "sleep 300");
    let cancel = CancellationToken::new();
    cancel.cancel();

    assert!(matches!(
        backend.submit(&spec, &cancel).await,
        Err(BackendError::Cancelled)
    ));
    assert!(matches!(
        backend.reconcile(&spec.attempt).await,
        ReconcileEvidence::Absent
    ));
}

#[tokio::test]
async fn cancel_stops() {
    // A long-running container is stopped with definitive evidence.
    let backend = backend_or_skip!();
    let spec = sh(&unique("cancel"), "sleep 300");
    let attempt = spec.attempt.clone();

    backend
        .submit(&spec, &CancellationToken::new())
        .await
        .unwrap();
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
async fn created_cancel_removes() {
    let warm_backend = backend_or_skip!();
    let warmup = sh(&unique("created-warmup"), "true");
    warm_backend
        .submit(&warmup, &CancellationToken::new())
        .await
        .unwrap();
    warm_backend
        .wait(&warmup.attempt, &CancellationToken::new())
        .await
        .unwrap();
    warm_backend.cleanup(&warmup.attempt).await.unwrap();

    use bollard::Docker;
    use bollard::models::ContainerCreateBody;
    use bollard::query_parameters::CreateContainerOptionsBuilder;
    let spec = sh(&unique("created-cancel"), "sleep 300");
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
                cmd: Some(vec!["sleep".to_string(), "300".to_string()]),
                labels: Some(
                    attempt
                        .labels()
                        .into_iter()
                        .chain([
                            (
                                "aruna-engine.org/attempt-epoch".to_string(),
                                "1".to_string(),
                            ),
                            (
                                "aruna-engine.org/controller-generation".to_string(),
                                "1".to_string(),
                            ),
                        ])
                        .collect(),
                ),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let backend = DockerBackend::with_config(DockerConfig {
        state_root: std::env::temp_dir()
            .join("aruna-compute-tests")
            .join(unique("state")),
        keep_failed: true,
        network_mode: Some("none".to_string()),
        ..DockerConfig::default()
    })
    .unwrap();
    assert!(matches!(
        backend.status(&attempt).await.unwrap().phase,
        AttemptPhase::Submitted
    ));
    assert!(matches!(
        backend.cancel(&attempt).await.unwrap(),
        CancelEvidence::AlreadyGone
    ));
    assert!(matches!(
        backend.reconcile(&attempt).await,
        ReconcileEvidence::Absent
    ));
}

#[tokio::test]
async fn reconcile_adopts() {
    // A fresh backend (simulating restart) re-adopts a running container by name.
    let submitter = backend_or_skip!();
    let spec = sh(&unique("adopt"), "sleep 120");
    let attempt = spec.attempt.clone();
    submitter
        .submit(&spec, &CancellationToken::new())
        .await
        .unwrap();
    wait_running(&submitter, &attempt).await;

    // Drop the submitting handle; a brand-new backend has no in-memory state.
    drop(submitter);
    let recovered = backend_or_skip!();
    match recovered.reconcile(&attempt).await {
        ReconcileEvidence::Adoptable(evidence) => {
            let status = evidence.status;
            assert_eq!(status.phase, AttemptPhase::Running);
        }
        other => panic!("expected Found(Running) on adopt, got {other:?}"),
    }

    // A name that was never submitted reconciles to NotFound.
    let missing = AttemptRef::new(unique("ghost"), 0);
    assert!(matches!(
        recovered.reconcile(&missing).await,
        ReconcileEvidence::Absent
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

    backend.submit(&spec, &cancel).await.unwrap();
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

    backend
        .submit(&spec, &CancellationToken::new())
        .await
        .unwrap();
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
    // A same-named container created outside Aruna (no aruna-engine.org/* labels) is
    // neither adopted as evidence nor removed by cleanup.
    let backend = backend_or_skip!();
    // Warm the image so the bare create below cannot 404.
    let warmup = sh(&unique("warmup"), "true");
    backend
        .submit(&warmup, &CancellationToken::new())
        .await
        .unwrap();
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
        ReconcileEvidence::Unadoptable(_) => {}
        other => panic!("foreign container must not reconcile, got {other:?}"),
    }
    assert!(
        backend
            .submit(&spec, &CancellationToken::new())
            .await
            .is_err(),
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

    backend
        .submit(&spec, &CancellationToken::new())
        .await
        .unwrap();
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
