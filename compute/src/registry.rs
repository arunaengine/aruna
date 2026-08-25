use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use aruna_core::compute::{BackendError, ExecutorCapability, ExecutorKind};
use aruna_core::structs::{PlacementPolicyError, PlacementSubject};

use crate::executor::{BackendCaps, ExecutorBackend};

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

    /// The endpoint a Direct-S3 attempt binding would hand its container. No
    /// attempt path builds one yet, so nothing reads this today; it stays
    /// because the operator configuration that fills it is documented.
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

    /// Advertisement of every enabled backend at the site it actually runs on.
    /// `subject` is the node's current placement subject; each backend pins its
    /// own executor kind and execution site into its copy.
    pub fn capabilities(
        &self,
        subject: &PlacementSubject,
        policy_draining: bool,
    ) -> Result<Vec<ExecutorCapability>, PlacementPolicyError> {
        self.backends
            .values()
            .map(|backend| {
                let caps = backend.capabilities();
                let mut capability =
                    ExecutorCapability::new(backend.kind().as_wire(), site(subject, &caps))?;
                capability.file_staging = caps.file_staging;
                capability.direct_s3 = caps.direct_s3;
                capability.s3_mount = caps.s3_mount;
                capability.network_policy = caps.network_policy;
                capability.limits = caps.limits;
                capability.policy_draining = policy_draining;
                Ok(capability)
            })
            .collect()
    }

    /// The backend for `kind`, but only while this node still advertises the
    /// exact site the launch was receipted under. Subject drift refuses the
    /// start instead of running accepted work at a site nobody authorized.
    pub fn fenced(
        &self,
        kind: &ExecutorKind,
        subject: &PlacementSubject,
        sealed_generation: u64,
        sealed_digest: &[u8; 32],
    ) -> Result<&Arc<dyn ExecutorBackend>, BackendError> {
        let backend = self
            .get(kind)
            .ok_or_else(|| BackendError::Unavailable(format!("no {} backend", kind.as_wire())))?;
        let current =
            ExecutorCapability::new(kind.as_wire(), site(subject, &backend.capabilities()))
                .map_err(|error| BackendError::InvalidSpec(error.to_string()))?;
        match current.subject.generation == sealed_generation
            && &current.subject_digest == sealed_digest
        {
            true => Ok(backend),
            false => Err(BackendError::Fenced),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.backends.is_empty()
    }
}

/// The execution site one backend advertises. A backend whose workers are
/// neither the controller nor a declared worker site cannot prove placement, so
/// it advertises no location and no labels rather than the controller's.
fn site(subject: &PlacementSubject, caps: &BackendCaps) -> PlacementSubject {
    match (caps.local_site, &caps.worker_site) {
        (true, _) => PlacementSubject {
            local_to_controller: true,
            ..subject.clone()
        },
        (false, Some(site)) => {
            let mut labels = site.labels.clone();
            aruna_core::structs::stamp_location(&mut labels, &site.location);
            PlacementSubject {
                local_to_controller: false,
                location: site.location.clone(),
                labels,
                ..subject.clone()
            }
        }
        (false, None) => PlacementSubject {
            local_to_controller: false,
            location: String::new(),
            labels: BTreeMap::new(),
            ..subject.clone()
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::logs::LogSink;
    use crate::executor::{BackendCaps, ExecutorBackend, WorkerSite};
    use aruna_core::compute::{
        AttemptStatus, BackendError, CancelEvidence, ExecutorKind, FenceContext, LogLimits,
        LogTails, NOBODY, ReconcileEvidence, TaskOutput, TaskSpec, TombstoneEvidence,
        TombstoneSpec, UserSpec,
    };
    use aruna_core::structs::LOCATION_LABEL_KEY;
    use async_trait::async_trait;
    use tokio_util::sync::CancellationToken;

    struct StubBackend(ExecutorKind, BackendCaps);

    impl StubBackend {
        fn local(kind: ExecutorKind) -> Self {
            Self(
                kind,
                BackendCaps {
                    file_staging: true,
                    local_site: true,
                    ..BackendCaps::default()
                },
            )
        }
    }

    #[async_trait]
    impl ExecutorBackend for StubBackend {
        fn kind(&self) -> ExecutorKind {
            self.0.clone()
        }
        fn capabilities(&self) -> BackendCaps {
            self.1.clone()
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
            unimplemented!()
        }
        async fn stage(
            &self,
            _context: &FenceContext,
            _spec: &TaskSpec,
            _cancel: &CancellationToken,
        ) -> Result<(), BackendError> {
            unimplemented!()
        }
        async fn unsuspend(
            &self,
            _context: &FenceContext,
            _cancel: &CancellationToken,
        ) -> Result<AttemptStatus, BackendError> {
            unimplemented!()
        }
        async fn status(&self, _context: &FenceContext) -> Result<AttemptStatus, BackendError> {
            unimplemented!()
        }
        async fn cancel(&self, _context: &FenceContext) -> Result<CancelEvidence, BackendError> {
            unimplemented!()
        }
        async fn fetch_logs(
            &self,
            _context: &FenceContext,
            _limits: &LogLimits,
            _sink: &dyn LogSink,
        ) -> Result<LogTails, BackendError> {
            unimplemented!()
        }
        async fn fetch_output(
            &self,
            _context: &FenceContext,
            _path: &str,
        ) -> Result<TaskOutput, BackendError> {
            unimplemented!()
        }
        async fn reconcile(&self, _context: &FenceContext) -> ReconcileEvidence {
            ReconcileEvidence::Absent
        }
        async fn tombstone(
            &self,
            _context: &FenceContext,
            _spec: &TombstoneSpec,
        ) -> Result<TombstoneEvidence, BackendError> {
            unimplemented!()
        }
        async fn cleanup(&self, _context: &FenceContext) -> Result<(), BackendError> {
            Ok(())
        }
        async fn sweep_orphans(&self, _grace: std::time::Duration) -> Result<(), BackendError> {
            Ok(())
        }
    }

    fn subject() -> PlacementSubject {
        PlacementSubject {
            node_id: iroh::SecretKey::from_bytes(&[9u8; 32]).public(),
            generation: 4,
            location: "eu-west".to_string(),
            labels: std::collections::BTreeMap::new(),
            executor_kind: None,
            local_to_controller: false,
        }
    }

    #[test]
    fn select_by_kind() {
        let registry = ExecutorRegistry::new()
            .with_backend(Arc::new(StubBackend::local(ExecutorKind::Docker)));
        assert!(registry.get(&ExecutorKind::Docker).is_some());
        assert!(registry.get(&ExecutorKind::Slurm).is_none());
        // Unconstrained selects the only enabled backend.
        assert!(registry.select(None).is_some());
        // A constraint the node cannot satisfy selects nothing.
        assert!(registry.select(Some(&ExecutorKind::Slurm)).is_none());
        assert_eq!(registry.kinds().len(), 1);
    }

    fn remote(kind: ExecutorKind, worker_site: Option<WorkerSite>) -> StubBackend {
        StubBackend(
            kind,
            BackendCaps {
                file_staging: true,
                local_site: false,
                worker_site,
                ..BackendCaps::default()
            },
        )
    }

    #[test]
    fn advertises_worker_site() {
        // Workers that run elsewhere must be advertised at their own location
        // and labels, never at the controller's.
        let site = WorkerSite {
            location: "dc-b".to_string(),
            labels: BTreeMap::from([("gpu".to_string(), "a100".to_string())]),
        };
        let registry = ExecutorRegistry::new().with_backend(Arc::new(remote(
            ExecutorKind::Kubernetes,
            Some(site.clone()),
        )));
        let subject = subject();

        let capability = registry
            .capabilities(&subject, false)
            .expect("subject is valid")
            .remove(0);

        assert_eq!(capability.subject.location, "dc-b");
        let mut expected = site.labels.clone();
        expected.insert(LOCATION_LABEL_KEY.to_string(), "dc-b".to_string());
        assert_eq!(capability.subject.labels, expected);
        assert!(!capability.subject.local_to_controller);
        assert!(capability.validate(subject.node_id).is_ok());
    }

    #[test]
    fn hides_unproven_site() {
        // A backend that cannot prove worker placement advertises no site facts,
        // so no location or label rule can match it.
        let mut subject = subject();
        subject.location = "controller-site".to_string();
        subject
            .labels
            .insert("tier".to_string(), "secure".to_string());
        let registry =
            ExecutorRegistry::new().with_backend(Arc::new(remote(ExecutorKind::Kubernetes, None)));

        let capability = registry
            .capabilities(&subject, false)
            .expect("subject is valid")
            .remove(0);

        assert!(capability.subject.location.is_empty());
        assert!(capability.subject.labels.is_empty());
        assert!(!capability.subject.local_to_controller);
    }

    #[test]
    fn fence_refuses_drift() {
        // Work receipted under one site may not start after the site changed.
        let registry = ExecutorRegistry::new()
            .with_backend(Arc::new(StubBackend::local(ExecutorKind::Docker)));
        let subject = subject();
        let sealed = registry
            .capabilities(&subject, false)
            .expect("subject is valid")
            .remove(0);

        assert!(
            registry
                .fenced(
                    &ExecutorKind::Docker,
                    &subject,
                    sealed.subject.generation,
                    &sealed.subject_digest
                )
                .is_ok()
        );

        let mut moved = subject.clone();
        moved.location = "elsewhere".to_string();
        assert_eq!(
            registry
                .fenced(
                    &ExecutorKind::Docker,
                    &moved,
                    sealed.subject.generation,
                    &sealed.subject_digest
                )
                .err(),
            Some(BackendError::Fenced)
        );

        let mut aged = subject.clone();
        aged.generation += 1;
        assert_eq!(
            registry
                .fenced(
                    &ExecutorKind::Docker,
                    &aged,
                    sealed.subject.generation,
                    &sealed.subject_digest
                )
                .err(),
            Some(BackendError::Fenced)
        );
        assert!(
            registry
                .fenced(
                    &ExecutorKind::Slurm,
                    &subject,
                    sealed.subject.generation,
                    &sealed.subject_digest
                )
                .is_err()
        );
    }

    #[test]
    fn advertises_backend_site() {
        // Each backend advertises its own kind, site locality, and sealed digest.
        let registry = ExecutorRegistry::new()
            .with_backend(Arc::new(StubBackend::local(ExecutorKind::Docker)));
        let subject = subject();
        let capabilities = registry
            .capabilities(&subject, true)
            .expect("subject is valid");

        assert_eq!(capabilities.len(), 1);
        let capability = &capabilities[0];
        assert_eq!(capability.kind, "docker");
        assert!(capability.file_staging && !capability.direct_s3);
        assert!(capability.policy_draining);
        assert!(capability.subject.local_to_controller);
        assert_eq!(capability.subject.generation, subject.generation);
        assert!(capability.validate(subject.node_id).is_ok());
    }
}
