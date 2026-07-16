use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use aruna_core::compute::ExecutorKind;

use crate::executor::ExecutorBackend;

/// Container-facing S3 endpoint the workspace credential targets. Injected into
/// the attempt env so unconfigured tooling reaches the node's S3 plane.
#[derive(Clone, Debug)]
pub struct WorkspaceEndpoint {
    pub endpoint: Option<String>,
    pub region: String,
}

impl Default for WorkspaceEndpoint {
    fn default() -> Self {
        Self {
            endpoint: None,
            region: "eu-central-1".to_string(),
        }
    }
}

/// Enabled executor backends keyed by their wire kind. The driver selects a
/// backend per execution job; advertisement (Stage 3) reads `kinds()`.
#[derive(Default)]
pub struct ExecutorRegistry {
    backends: BTreeMap<String, Arc<dyn ExecutorBackend>>,
    workspace: WorkspaceEndpoint,
}

impl ExecutorRegistry {
    pub fn new() -> Self {
        Self {
            backends: BTreeMap::new(),
            workspace: WorkspaceEndpoint::default(),
        }
    }

    pub fn with_backend(mut self, backend: Arc<dyn ExecutorBackend>) -> Self {
        self.register(backend);
        self
    }

    pub fn with_workspace_endpoint(mut self, endpoint: Option<String>, region: String) -> Self {
        self.workspace = WorkspaceEndpoint { endpoint, region };
        self
    }

    pub fn workspace_endpoint(&self) -> &WorkspaceEndpoint {
        &self.workspace
    }

    pub fn register(&mut self, backend: Arc<dyn ExecutorBackend>) {
        self.backends.insert(backend.kind().as_wire(), backend);
    }

    pub fn get(&self, kind: &ExecutorKind) -> Option<&Arc<dyn ExecutorBackend>> {
        self.backends.get(&kind.as_wire())
    }

    /// Pick a backend satisfying the constraint, or the first enabled one when
    /// unconstrained. `None` means no enabled backend can run the job.
    pub fn select(&self, constraint: Option<&ExecutorKind>) -> Option<&Arc<dyn ExecutorBackend>> {
        match constraint {
            Some(kind) => self.get(kind),
            None => self.backends.values().next(),
        }
    }

    /// Advertised wire kinds for the Node Descriptor.
    pub fn kinds(&self) -> BTreeSet<String> {
        self.backends.keys().cloned().collect()
    }

    pub fn is_empty(&self) -> bool {
        self.backends.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::ExecutorBackend;
    use crate::executor::logs::LogSink;
    use aruna_core::compute::{
        AttemptRef, AttemptStatus, BackendError, CancelEvidence, ExecutorKind, LogLimits, LogTails,
        ReconcileOutcome, TaskOutput, TaskSpec,
    };
    use async_trait::async_trait;
    use tokio_util::sync::CancellationToken;

    struct StubBackend(ExecutorKind);

    #[async_trait]
    impl ExecutorBackend for StubBackend {
        fn kind(&self) -> ExecutorKind {
            self.0.clone()
        }
        async fn health(&self) -> Result<(), BackendError> {
            Ok(())
        }
        async fn submit(
            &self,
            _spec: &TaskSpec,
            _cancel: &CancellationToken,
        ) -> Result<AttemptStatus, BackendError> {
            unimplemented!()
        }
        async fn status(&self, _attempt: &AttemptRef) -> Result<AttemptStatus, BackendError> {
            unimplemented!()
        }
        async fn cancel(&self, _attempt: &AttemptRef) -> Result<CancelEvidence, BackendError> {
            unimplemented!()
        }
        async fn fetch_logs(
            &self,
            _attempt: &AttemptRef,
            _limits: &LogLimits,
            _sink: &dyn LogSink,
        ) -> Result<LogTails, BackendError> {
            unimplemented!()
        }
        async fn fetch_output(
            &self,
            _attempt: &AttemptRef,
            _path: &str,
        ) -> Result<TaskOutput, BackendError> {
            unimplemented!()
        }
        async fn reconcile(&self, _attempt: &AttemptRef) -> ReconcileOutcome {
            ReconcileOutcome::NotFound
        }
        async fn cleanup(&self, _attempt: &AttemptRef) -> Result<(), BackendError> {
            Ok(())
        }
    }

    #[test]
    fn select_by_kind() {
        let registry =
            ExecutorRegistry::new().with_backend(Arc::new(StubBackend(ExecutorKind::Docker)));
        assert!(registry.get(&ExecutorKind::Docker).is_some());
        assert!(registry.get(&ExecutorKind::Slurm).is_none());
        // Unconstrained selects the only enabled backend.
        assert!(registry.select(None).is_some());
        // A constraint the node cannot satisfy selects nothing.
        assert!(registry.select(Some(&ExecutorKind::Slurm)).is_none());
        assert_eq!(registry.kinds().len(), 1);
    }
}
