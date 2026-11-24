//! Starts coordinator attempts: a sandboxed container on an executor backend, or a local binary
//! for development and tests. Each attempt is one byte channel that ends with its guard.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::path::PathBuf;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use aruna_compute::executor::ExecutorBackend;
use aruna_core::compute::runtimes::SESSION_SOCKET_PATH;
use aruna_core::compute::{
    AttemptRef, BackendError, FenceContext, NetworkAccess, ResourceRequest, SecurityContext,
    TaskSpec,
};
use aruna_core::workflow::WorkflowError;
use async_trait::async_trait;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, BufReader};
use tokio::sync::OnceCell;
use tokio_util::sync::CancellationToken;

use crate::settings::ContainerLimits;

/// One running coordinator: its channel, and a guard that ends it when dropped.
pub struct Attempt {
    pub reader: Pin<Box<dyn AsyncRead + Send>>,
    pub writer: Pin<Box<dyn AsyncWrite + Send>>,
    pub guard: Box<dyn Send>,
}

/// Starts coordinator attempts; tests give a scripted one.
#[async_trait]
pub trait CoordinatorTransport: Send + Sync {
    async fn start(&self) -> Result<Attempt, WorkflowError>;
}

/// A coordinator binary run as a child process with an empty environment. It has no sandbox,
/// so the node uses it only when the operator names it for development or tests.
pub struct LocalBinary {
    path: PathBuf,
}

impl LocalBinary {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

#[async_trait]
impl CoordinatorTransport for LocalBinary {
    async fn start(&self) -> Result<Attempt, WorkflowError> {
        let mut child = tokio::process::Command::new(&self.path)
            .env_clear()
            .env("TMPDIR", std::env::temp_dir())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|error| crashed(format!("cannot start {}: {error}", self.path.display())))?;
        let (Some(writer), Some(reader), Some(errors)) =
            (child.stdin.take(), child.stdout.take(), child.stderr.take())
        else {
            return Err(crashed("the coordinator process has no pipes".into()));
        };
        tokio::spawn(async move {
            let mut lines = BufReader::new(errors).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                tracing::debug!(target: "aruna_workflow::coordinator", "{line}");
            }
        });
        Ok(Attempt {
            reader: Box::pin(reader),
            writer: Box::pin(writer),
            guard: Box::new(child),
        })
    }
}

/// The coordinator image as a session task on an executor backend: no network, a read-only
/// root, no token and no storage, bounded by the operator's limits.
pub struct ContainerTransport {
    backend: Arc<dyn ExecutorBackend>,
    image: String,
    limits: ContainerLimits,
    pinned: OnceCell<String>,
}

impl ContainerTransport {
    pub fn new(backend: Arc<dyn ExecutorBackend>, image: String, limits: ContainerLimits) -> Self {
        Self {
            backend,
            image,
            limits,
            pinned: OnceCell::new(),
        }
    }

    /// The task of one attempt; the session flag keeps it running until the node ends it.
    pub fn task(&self, attempt: AttemptRef, image: String) -> TaskSpec {
        let mut spec = TaskSpec::new(attempt, image);
        spec.session = true;
        spec.workdir = Some("/tmp".to_string());
        spec.env.insert(
            "ARUNA_SESSION_SOCKET".to_string(),
            format!("/tmp/{SESSION_SOCKET_PATH}"),
        );
        spec.security = SecurityContext {
            network: NetworkAccess::Isolated,
            read_only_rootfs: true,
            pids_limit: Some(self.limits.pids),
            ..SecurityContext::default()
        };
        spec.resources = ResourceRequest {
            cpu_cores: Some(self.limits.cpus),
            ram_bytes: Some(self.limits.memory_bytes),
            disk_bytes: Some(self.limits.disk_bytes),
            max_walltime: Some(self.limits.walltime),
            ..ResourceRequest::default()
        };
        spec
    }
}

/// Stops and removes the container of an attempt once its channel is dropped.
struct ContainerGuard {
    backend: Arc<dyn ExecutorBackend>,
    fence: FenceContext,
}

impl Drop for ContainerGuard {
    fn drop(&mut self) {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            tracing::warn!(attempt = %self.fence.attempt.external_name(), "no runtime ends the coordinator");
            return;
        };
        let (backend, fence) = (self.backend.clone(), self.fence.clone());
        runtime.spawn(async move {
            if let Err(error) = backend.cancel(&fence).await {
                tracing::debug!(%error, "the coordinator was not running any more");
            }
            if let Err(error) = backend.cleanup(&fence).await {
                tracing::warn!(%error, "the coordinator container was not removed");
            }
        });
    }
}

#[async_trait]
impl CoordinatorTransport for ContainerTransport {
    async fn start(&self) -> Result<Attempt, WorkflowError> {
        let cancel = CancellationToken::new();
        let image = self
            .pinned
            .get_or_try_init(|| self.backend.resolve_image(&self.image, &cancel))
            .await
            .map_err(|error| crashed(format!("the coordinator image: {error}")))?
            .clone();
        let job = format!("wf-{}", ulid::Ulid::generate().to_string().to_lowercase());
        let fence = FenceContext {
            attempt: AttemptRef::new(job, 1),
            attempt_epoch: 1,
            controller_generation: 1,
        };
        let spec = self.task(fence.attempt.clone(), image);
        let guard = ContainerGuard {
            backend: self.backend.clone(),
            fence: fence.clone(),
        };
        self.backend
            .submit(&fence, &spec, &cancel)
            .await
            .map_err(|error| crashed(format!("the coordinator did not start: {error}")))?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let channel = loop {
            match self.backend.open_session(&fence).await {
                Ok(channel) => break channel,
                Err(BackendError::Conflict(_)) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(error) => {
                    return Err(crashed(format!("no channel to the coordinator: {error}")));
                }
            }
        };
        Ok(Attempt {
            reader: channel.output,
            writer: channel.input,
            guard: Box::new(guard),
        })
    }
}

pub(crate) fn crashed(message: String) -> WorkflowError {
    WorkflowError::Crashed(message)
}
