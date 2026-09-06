//! Contract of the session channel: a backend that advertises it hands the
//! node a byte stream, and the session manager speaks the helper protocol over
//! it without interpreting a cell.

use aruna_compute::executor::{BackendCaps, ExecutorBackend, SessionChannel};
use aruna_compute::session::events::EventKind;
use aruna_compute::session::{EndReason, SessionConfig, SessionRegistry};
use aruna_core::compute::{
    AttemptRef, AttemptStatus, BackendError, CancelEvidence, ExecutorKind, FenceContext, LogLimits,
    LogTails, NOBODY, ReconcileEvidence, TaskOutput, TaskSpec, UserSpec,
};
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

/// A backend whose channel is an in-memory pipe. The far end stands in for the
/// helper inside a container.
struct FakeBackend {
    helper: Mutex<Option<DuplexStream>>,
    session: bool,
}

impl FakeBackend {
    fn new(session: bool) -> (Arc<Self>, DuplexStream) {
        let (node, helper) = tokio::io::duplex(64 * 1024);
        let backend = Arc::new(Self {
            helper: Mutex::new(Some(helper)),
            session,
        });
        (backend, node)
    }
}

#[async_trait]
impl ExecutorBackend for FakeBackend {
    fn kind(&self) -> ExecutorKind {
        ExecutorKind::Docker
    }

    fn capabilities(&self) -> BackendCaps {
        BackendCaps {
            session: self.session,
            ..BackendCaps::default()
        }
    }

    fn run_identity(&self) -> UserSpec {
        NOBODY
    }

    async fn health(&self) -> Result<(), BackendError> {
        Ok(())
    }

    async fn resolve_image(
        &self,
        image: &str,
        _cancel: &CancellationToken,
    ) -> Result<String, BackendError> {
        Ok(image.to_string())
    }

    async fn fence(&self, _context: &FenceContext) -> Result<(), BackendError> {
        Ok(())
    }

    async fn submit(
        &self,
        _context: &FenceContext,
        _spec: &TaskSpec,
        _cancel: &CancellationToken,
    ) -> Result<AttemptStatus, BackendError> {
        Err(BackendError::InvalidSpec("not used".to_string()))
    }

    async fn status(&self, _context: &FenceContext) -> Result<AttemptStatus, BackendError> {
        Err(BackendError::InvalidSpec("not used".to_string()))
    }

    async fn cancel(&self, _context: &FenceContext) -> Result<CancelEvidence, BackendError> {
        Ok(CancelEvidence::AlreadyGone)
    }

    async fn fetch_logs(
        &self,
        _context: &FenceContext,
        _limits: &LogLimits,
    ) -> Result<LogTails, BackendError> {
        Ok(LogTails::default())
    }

    async fn fetch_output(
        &self,
        _context: &FenceContext,
        _path: &str,
    ) -> Result<TaskOutput, BackendError> {
        Err(BackendError::InvalidSpec("not used".to_string()))
    }

    async fn open_session(&self, _context: &FenceContext) -> Result<SessionChannel, BackendError> {
        if !self.session {
            return Err(BackendError::InvalidSpec(
                "backend does not support session channels".to_string(),
            ));
        }
        let stream = self
            .helper
            .lock()
            .await
            .take()
            .ok_or_else(|| BackendError::Conflict("channel already open".to_string()))?;
        let (output, input) = tokio::io::split(stream);
        Ok(SessionChannel {
            input: Box::pin(input),
            output: Box::pin(output),
        })
    }

    async fn reconcile(&self, _context: &FenceContext) -> ReconcileEvidence {
        ReconcileEvidence::Absent
    }

    async fn cleanup(&self, _context: &FenceContext) -> Result<(), BackendError> {
        Ok(())
    }
}

fn fence() -> FenceContext {
    FenceContext {
        attempt: AttemptRef::new("01jjrstvwxyz0123456789abcd", 1),
        attempt_epoch: 1,
        controller_generation: 1,
    }
}

fn config() -> SessionConfig {
    SessionConfig {
        job_id: "01JJRSTVWXYZ0123456789ABCD".to_string(),
        runtime: "python-notebook".to_string(),
        workspace_bucket: "lab-data".to_string(),
        executor_node_id: "node-1".to_string(),
        idle_after_ms: 600_000,
        credential_expires_at_ms: 0,
    }
}

#[tokio::test]
async fn backend_without_session_refuses() {
    let (backend, _node) = FakeBackend::new(false);
    let opened = ExecutorBackend::open_session(backend.as_ref(), &fence()).await;
    assert!(
        matches!(opened, Err(BackendError::InvalidSpec(_))),
        "a backend without the flag opens no channel"
    );
    assert!(!backend.capabilities().session);
}

#[tokio::test]
async fn cell_reaches_the_helper() {
    // The node forwards the code unchanged and never interprets it.
    let (backend, node) = FakeBackend::new(true);
    let registry = Arc::new(SessionRegistry::new());
    let session = registry.open(config(), backend, fence());
    let mut helper = BufReader::new(node);

    let mut line = String::new();
    // The helper reports itself ready before a cell is accepted.
    helper
        .get_mut()
        .write_all(b"{\"kind\":\"kernel\",\"state\":\"idle\"}\n")
        .await
        .expect("helper announces readiness");
    wait_for_ready(&session).await;

    session.submit_cell("c1", "print(1)").expect("submit");
    helper.read_line(&mut line).await.expect("request arrives");
    let request: serde_json::Value = serde_json::from_str(&line).expect("request parses");
    assert_eq!(request["op"], "execute");
    assert_eq!(request["cell_id"], "c1");
    assert_eq!(request["code"], "print(1)");

    helper
        .get_mut()
        .write_all(
            b"{\"kind\":\"output\",\"cell_id\":\"c1\",\"output\":{\"output_type\":\"stream\",\"name\":\"stdout\",\"text\":\"1\\n\"}}\n",
        )
        .await
        .expect("helper answers");
    let output = wait_for_output(&session).await;
    assert!(output.contains("\\\"text\\\":\\\"1"), "{output}");
}

#[tokio::test]
async fn channel_loss_ends_the_session() {
    let (backend, node) = FakeBackend::new(true);
    let registry = Arc::new(SessionRegistry::new());
    let session = registry.open(config(), backend, fence());
    drop(node);
    let reason = tokio::time::timeout(Duration::from_secs(5), session.finished())
        .await
        .expect("a dead channel ends the session");
    assert_eq!(reason, EndReason::KernelExit);
    // The registry drops an ended session from its own task, so wait for it.
    for _ in 0..64 {
        if registry.get(session.job_id()).is_none() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("an ended session stayed registered");
}

/// Waits for the manager to observe the helper's readiness. The pump runs in
/// its own task, so the test waits for the state instead of sleeping.
async fn wait_for_ready(session: &Arc<aruna_compute::Session>) {
    let (_, mut receiver) = session.subscribe_all();
    for _ in 0..64 {
        if session.snapshot().state != aruna_compute::session::SessionPhase::Starting {
            return;
        }
        let _ = tokio::time::timeout(Duration::from_secs(5), receiver.recv()).await;
    }
    panic!("the session never became ready");
}

async fn wait_for_output(session: &Arc<aruna_compute::Session>) -> String {
    let (_, mut receiver) = session.subscribe_all();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(5), receiver.recv())
            .await
            .expect("an output arrives")
            .expect("the stream stays open");
        if event.kind == EventKind::Output {
            return serde_json::to_string(&event.data).unwrap_or_default();
        }
    }
}
