//! Holds the enabled executor backends and picks one for each execution job.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use aruna_core::compute::backend::{DeclaredCapabilities, WorkloadSuitability};
use aruna_core::compute::identity::home_digest;
use aruna_core::compute::offer::{
    BackendCaps, BackendLimit, BackendLimits, ExecutorOffer, config_digest,
};
use aruna_core::compute::{BackendError, ExecutorCapability, ExecutorKind};
use aruna_core::structs::placement::policy::{PlacementPolicyError, PlacementSubject};
use aruna_core::workflow::WorkflowHost;

use crate::executor::ExecutorBackend;
use crate::session::SessionRegistry;

/// Container-facing S3 endpoint the workspace credential targets. Injected into
/// the attempt env so unconfigured tooling reaches the node's S3 plane.
#[derive(Clone, Debug)]
pub struct WorkspaceEndpoint {
    pub endpoint: Option<String>,
    pub region: String,
    /// Endpoint an interactive session reaches instead: the bridge gateway of
    /// the session network, which has no external route.
    pub session_endpoint: Option<String>,
}

impl Default for WorkspaceEndpoint {
    fn default() -> Self {
        Self {
            endpoint: None,
            region: "eu-central-1".to_string(),
            session_endpoint: None,
        }
    }
}

/// What the operator wrote for one backend in the backends file. These values replace the
/// adapter's defaults wherever the registry reads the backend's capabilities.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OperatorSettings {
    pub max_walltime_ms: Option<u64>,
    pub declared: DeclaredCapabilities,
    pub suitability: Option<WorkloadSuitability>,
    pub groups: Vec<aruna_core::types::GroupId>,
}

/// One enabled backend with the S3 endpoints its containers reach.
struct Registered {
    backend: Arc<dyn ExecutorBackend>,
    workspace: WorkspaceEndpoint,
    settings: Option<OperatorSettings>,
}

impl Registered {
    /// The adapter's capabilities with the operator's settings, when written, in place.
    fn caps(&self) -> BackendCaps {
        let caps = self.backend.capabilities();
        match &self.settings {
            Some(settings) => BackendCaps {
                max_walltime_ms: settings.max_walltime_ms,
                declared: settings.declared.clone(),
                suitability: settings.suitability,
                groups: settings.groups.clone(),
                ..caps
            },
            None => caps,
        }
    }
}

/// Enabled executor backends keyed by their node-local name. The driver selects
/// a backend per execution job; advertisement reads `capabilities()`.
#[derive(Default)]
pub struct ExecutorRegistry {
    backends: BTreeMap<String, Registered>,
    sessions: Arc<SessionRegistry>,
    /// The workflow coordinators of this node, which run on these backends or as a local binary.
    workflow: Option<Arc<dyn WorkflowHost>>,
    run_wakes: RunWakes,
}

/// Wakes the event streams of workflow runs on this node when a run stored new events. A wake
/// carries the run id; a missed one only delays a stream until its next read.
pub struct RunWakes(tokio::sync::broadcast::Sender<String>);

impl Default for RunWakes {
    fn default() -> Self {
        Self(tokio::sync::broadcast::channel(RUN_WAKES).0)
    }
}

/// Wakes one stream may fall behind by before it rereads from its last event.
const RUN_WAKES: usize = 256;

impl ExecutorRegistry {
    pub fn new() -> Self {
        Self {
            backends: BTreeMap::new(),
            sessions: Arc::new(SessionRegistry::new()),
            workflow: None,
            run_wakes: RunWakes::default(),
        }
    }

    /// Tells the event streams of run `run_id` that it stored new events.
    pub fn wake_run(&self, run_id: String) {
        let _ = self.run_wakes.0.send(run_id);
    }

    /// The wakes of every run on this node, from now on.
    pub fn run_wakes(&self) -> tokio::sync::broadcast::Receiver<String> {
        self.run_wakes.0.subscribe()
    }

    /// Attaches the workflow runtime that answers `Effect::Workflow` on this node.
    pub fn with_workflow(mut self, workflow: Arc<dyn WorkflowHost>) -> Self {
        self.workflow = Some(workflow);
        self
    }

    /// The workflow runtime of this node, when one is attached.
    pub fn workflow(&self) -> Option<&Arc<dyn WorkflowHost>> {
        self.workflow.as_ref()
    }

    /// Adds `backend` under `name` without a container-facing S3 endpoint.
    pub fn with_backend(self, name: impl Into<String>, backend: Arc<dyn ExecutorBackend>) -> Self {
        self.with_workspace(name, backend, WorkspaceEndpoint::default())
    }

    /// Adds `backend` under `name`, whose containers reach S3 at `workspace`.
    pub fn with_workspace(
        mut self,
        name: impl Into<String>,
        backend: Arc<dyn ExecutorBackend>,
        workspace: WorkspaceEndpoint,
    ) -> Self {
        self.register(name, backend, workspace);
        self
    }

    /// The S3 endpoints of the backend `placed` finds, which sessions and workspace
    /// access hand to its containers.
    pub fn workspace(
        &self,
        name: Option<&str>,
        constraint: Option<&ExecutorKind>,
    ) -> Option<&WorkspaceEndpoint> {
        self.placement(name, constraint)
            .map(|(_, entry)| &entry.workspace)
    }

    /// The interactive sessions this node runs. In memory only: a restart ends
    /// every session and reconcile resolves the jobs behind them.
    pub fn sessions(&self) -> &Arc<SessionRegistry> {
        &self.sessions
    }

    pub fn register(
        &mut self,
        name: impl Into<String>,
        backend: Arc<dyn ExecutorBackend>,
        workspace: WorkspaceEndpoint,
    ) {
        let settings = None;
        self.backends.insert(
            name.into(),
            Registered {
                backend,
                workspace,
                settings,
            },
        );
    }

    /// Gives the backend under `name` the operator's settings from the backends file.
    pub fn with_settings(mut self, name: &str, settings: OperatorSettings) -> Self {
        if let Some(entry) = self.backends.get_mut(name) {
            entry.settings = Some(settings);
        }
        self
    }

    /// The capabilities backend `name` is filtered and advertised by, operator settings included.
    pub fn caps(&self, name: &str) -> Option<BackendCaps> {
        self.backends.get(name).map(Registered::caps)
    }

    /// The backend registered under `name`.
    pub fn named(&self, name: &str) -> Option<&Arc<dyn ExecutorBackend>> {
        self.backends.get(name).map(|entry| &entry.backend)
    }

    /// The first backend of `kind` in name order.
    pub fn get(&self, kind: &ExecutorKind) -> Option<&Arc<dyn ExecutorBackend>> {
        self.selected(Some(kind)).map(|entry| &entry.backend)
    }

    /// Pick a backend satisfying the constraint, or the first enabled one when
    /// unconstrained. `None` means no enabled backend can run the job.
    pub fn select(&self, constraint: Option<&ExecutorKind>) -> Option<&Arc<dyn ExecutorBackend>> {
        self.selected(constraint).map(|entry| &entry.backend)
    }

    /// The backend a run is placed on with its name: the stored name, which must
    /// also match the kind constraint, else the first backend `select` picks.
    pub fn placed(
        &self,
        name: Option<&str>,
        constraint: Option<&ExecutorKind>,
    ) -> Option<(&str, &Arc<dyn ExecutorBackend>)> {
        self.placement(name, constraint)
            .map(|(name, entry)| (name.as_str(), &entry.backend))
    }

    fn placement(
        &self,
        name: Option<&str>,
        constraint: Option<&ExecutorKind>,
    ) -> Option<(&String, &Registered)> {
        let kind_matches =
            |entry: &Registered| constraint.is_none_or(|kind| &entry.backend.kind() == kind);
        match name {
            Some(name) => self
                .backends
                .get_key_value(name)
                .filter(|(_, entry)| kind_matches(entry)),
            None => self.backends.iter().find(|(_, entry)| kind_matches(entry)),
        }
    }

    fn selected(&self, constraint: Option<&ExecutorKind>) -> Option<&Registered> {
        self.backends
            .values()
            .find(|entry| constraint.is_none_or(|kind| &entry.backend.kind() == kind))
    }

    /// Where the attempts of backend `name` live: its kind and runtime destination.
    pub fn home_digest(&self, name: &str) -> Option<[u8; 32]> {
        let backend = self.named(name)?;
        Some(home_digest(
            &backend.kind().as_wire(),
            backend.destination().as_ref(),
        ))
    }

    /// The home digest and everything placement and admission read of backend `name`.
    pub fn config_digest(&self, name: &str) -> Option<[u8; 32]> {
        let home = self.home_digest(name)?;
        let backend = self.named(name)?;
        let caps = self.caps(name)?;
        Some(config_digest(
            &home,
            &caps,
            suitability(backend.as_ref(), &caps),
        ))
    }

    /// Advertised wire kinds for the Node Descriptor.
    pub fn kinds(&self) -> BTreeSet<String> {
        self.backends
            .values()
            .map(|entry| entry.backend.kind().as_wire())
            .collect()
    }

    /// What each registered backend advertises, by its name, with its wire kind and config.
    pub fn limits(&self) -> BackendLimits {
        (self.backends.iter())
            .map(|(name, entry)| {
                let limit = BackendLimit {
                    kind: entry.backend.kind().as_wire(),
                    limits: entry.caps().limits,
                    config_digest: self.config_digest(name).unwrap_or_default(),
                };
                (name.clone(), limit)
            })
            .collect()
    }

    /// Advertisement of every enabled backend at the site it actually runs on.
    /// `subject` is the node's current placement subject; each backend pins its
    /// own executor kind and execution site into its copy.
    pub fn capabilities(
        &self,
        subject: &PlacementSubject,
        policy_draining: bool,
    ) -> Result<Vec<ExecutorCapability>, PlacementPolicyError> {
        (self.offers().iter())
            .map(|offer| offer.capability(subject, policy_draining))
            .collect()
    }

    /// What every enabled backend offers, before a placement subject is known.
    pub fn offers(&self) -> Vec<ExecutorOffer> {
        (self.backends.iter())
            .map(|(name, entry)| {
                let backend = &entry.backend;
                let mut caps = entry.caps();
                caps.suitability = Some(suitability(backend.as_ref(), &caps));
                let kind = backend.kind().as_wire();
                let home = home_digest(&kind, backend.destination().as_ref());
                ExecutorOffer::new((name.clone(), kind), home, caps)
            })
            .collect()
    }

    /// The backend for `kind`, but only while this node still advertises the
    /// exact site the launch was receipted under. Subject drift refuses the
    /// start instead of running accepted work at a site nobody authorized.
    pub fn fenced(
        &self,
        name: &str,
        subject: &PlacementSubject,
        stored_generation: u64,
        stored_digest: &[u8; 32],
    ) -> Result<&Arc<dyn ExecutorBackend>, BackendError> {
        let backend = self
            .named(name)
            .ok_or_else(|| BackendError::Unavailable(format!("no backend named {name}")))?;
        let site = backend.capabilities().site(subject);
        let current = ExecutorCapability::new(name.to_string(), backend.kind().as_wire(), site)
            .map_err(|error| BackendError::InvalidSpec(error.to_string()))?;
        match current.subject.generation == stored_generation
            && &current.subject_digest == stored_digest
        {
            true => Ok(backend),
            false => Err(BackendError::Fenced),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.backends.is_empty()
    }
}

/// The written suitability of a backend, else the defaults of its kind.
fn suitability(backend: &dyn ExecutorBackend, caps: &BackendCaps) -> WorkloadSuitability {
    caps.suitability
        .unwrap_or_else(|| WorkloadSuitability::of_kind(&backend.kind()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::compute::offer::WorkerSite;
    use aruna_core::compute::{
        AttemptStatus, BackendError, CancelEvidence, ExecutorKind, FenceContext, LogLimits,
        LogTails, NOBODY, ReconcileEvidence, TaskOutput, TaskSpec, TombstoneEvidence,
        TombstoneSpec, UserSpec,
    };
    use aruna_core::structs::storage::node_info::{LOCATION_LABEL_KEY, NODE_LABEL_KEY};
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
    fn names_two_backends() {
        // Two backends of one kind stay apart under their names.
        let registry = ExecutorRegistry::new()
            .with_backend("lab", Arc::new(StubBackend::local(ExecutorKind::Docker)))
            .with_workspace(
                "edge",
                Arc::new(remote(ExecutorKind::Docker, None)),
                WorkspaceEndpoint {
                    endpoint: Some("http://edge.test".to_string()),
                    ..WorkspaceEndpoint::default()
                },
            );

        assert!(
            registry
                .named("lab")
                .is_some_and(|b| b.capabilities().local_site)
        );
        assert!(
            registry
                .named("edge")
                .is_some_and(|b| !b.capabilities().local_site)
        );
        assert!(registry.named("default").is_none());
        // Unnamed selection takes the first name in order.
        assert!(!registry.select(None).unwrap().capabilities().local_site);
        assert_eq!(
            registry
                .workspace(None, None)
                .and_then(|w| w.endpoint.as_deref()),
            Some("http://edge.test")
        );
        // A stored name pins its backend, and only while the kind still matches.
        let docker = Some(&ExecutorKind::Docker);
        assert_eq!(
            registry.placed(Some("lab"), docker).map(|(name, _)| name),
            Some("lab")
        );
        assert!(
            registry
                .placed(Some("lab"), Some(&ExecutorKind::Slurm))
                .is_none()
        );
        assert!(registry.placed(Some("gone"), None).is_none());
        let advertised = registry.capabilities(&subject(), false).unwrap();
        assert_eq!(advertised.len(), 2);
        assert!(
            advertised
                .iter()
                .all(|capability| capability.kind == "docker")
        );
    }

    #[test]
    fn select_by_kind() {
        let registry = ExecutorRegistry::new().with_backend(
            "default",
            Arc::new(StubBackend::local(ExecutorKind::Docker)),
        );
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
        let registry = ExecutorRegistry::new().with_backend(
            "default",
            Arc::new(remote(ExecutorKind::Kubernetes, Some(site.clone()))),
        );
        let subject = subject();

        let capability = registry
            .capabilities(&subject, false)
            .expect("subject is valid")
            .remove(0);

        assert_eq!(capability.subject.location, "dc-b");
        let mut expected = site.labels.clone();
        expected.insert(LOCATION_LABEL_KEY.to_string(), "dc-b".to_string());
        expected.insert(NODE_LABEL_KEY.to_string(), subject.node_id.to_string());
        assert_eq!(capability.subject.labels, expected);
        assert!(!capability.subject.local_to_controller);
        assert!(capability.validate(subject.node_id).is_ok());
    }

    #[test]
    fn hides_unproven_site() {
        // A backend that cannot prove worker placement advertises no site details,
        // so only a pin to the node itself can match it.
        let mut subject = subject();
        subject.location = "controller-site".to_string();
        subject
            .labels
            .insert("tier".to_string(), "secure".to_string());
        let registry = ExecutorRegistry::new()
            .with_backend("default", Arc::new(remote(ExecutorKind::Kubernetes, None)));

        let capability = registry
            .capabilities(&subject, false)
            .expect("subject is valid")
            .remove(0);

        assert!(capability.subject.location.is_empty());
        let node = BTreeMap::from([(NODE_LABEL_KEY.to_string(), subject.node_id.to_string())]);
        assert_eq!(capability.subject.labels, node);
        assert!(!capability.subject.local_to_controller);
    }

    #[test]
    fn fence_refuses_drift() {
        // Work receipted under one site may not start after the site changed.
        let registry = ExecutorRegistry::new().with_backend(
            "default",
            Arc::new(StubBackend::local(ExecutorKind::Docker)),
        );
        let subject = subject();
        let stored = registry
            .capabilities(&subject, false)
            .expect("subject is valid")
            .remove(0);

        assert!(
            registry
                .fenced(
                    "default",
                    &subject,
                    stored.subject.generation,
                    &stored.subject_digest
                )
                .is_ok()
        );

        let mut moved = subject.clone();
        moved.location = "elsewhere".to_string();
        assert_eq!(
            registry
                .fenced(
                    "default",
                    &moved,
                    stored.subject.generation,
                    &stored.subject_digest
                )
                .err(),
            Some(BackendError::Fenced)
        );

        let mut aged = subject.clone();
        aged.generation += 1;
        assert_eq!(
            registry
                .fenced(
                    "default",
                    &aged,
                    stored.subject.generation,
                    &stored.subject_digest
                )
                .err(),
            Some(BackendError::Fenced)
        );
        assert!(
            registry
                .fenced(
                    "missing",
                    &subject,
                    stored.subject.generation,
                    &stored.subject_digest
                )
                .is_err()
        );
    }

    #[test]
    fn advertises_kind_defaults() {
        // Without written suitability a backend advertises its kind's defaults; written values
        // and the walltime maximum are advertised as they are, and both move the config digest.
        use aruna_core::compute::backend::{Suitability, WorkloadSuitability};
        let registry = ExecutorRegistry::new()
            .with_backend("a", Arc::new(StubBackend::local(ExecutorKind::Apptainer)))
            .with_backend("d", Arc::new(StubBackend::local(ExecutorKind::Docker)));
        let advertised = registry.capabilities(&subject(), false).unwrap();
        assert_eq!(advertised[0].suitability.sessions, Suitability::Refused);
        assert_eq!(advertised[1].suitability.sessions, Suitability::Good);
        assert_eq!(advertised[1].max_walltime_ms, None);

        let mut caps = StubBackend::local(ExecutorKind::Docker).1;
        caps.max_walltime_ms = Some(3_600_000);
        let (first, second) = (
            aruna_core::types::GroupId::from(1u128),
            aruna_core::types::GroupId::from(2u128),
        );
        caps.groups = vec![second, first, second];
        caps.suitability = Some(WorkloadSuitability {
            sessions: Suitability::Refused,
            ..WorkloadSuitability::of_kind(&ExecutorKind::Docker)
        });
        let written = ExecutorRegistry::new()
            .with_backend("d", Arc::new(StubBackend(ExecutorKind::Docker, caps)));
        let capability = &written.capabilities(&subject(), false).unwrap()[0];
        assert_eq!(capability.suitability.sessions, Suitability::Refused);
        assert_eq!(capability.max_walltime_ms, Some(3_600_000));
        assert_eq!(capability.groups, [first, second]);
        assert_ne!(capability.config_digest, advertised[1].config_digest);
    }

    #[test]
    fn applies_operator_settings() {
        // A backend's file entry replaces the adapter defaults it is advertised, digested and
        // filtered by; a backend without one keeps them.
        use aruna_core::compute::backend::{Suitability, WorkloadSuitability};
        let plain = ExecutorRegistry::new()
            .with_backend("lab", Arc::new(StubBackend::local(ExecutorKind::Docker)));
        let group = aruna_core::types::GroupId::from(5u128);
        let settings = OperatorSettings {
            max_walltime_ms: Some(60_000),
            suitability: Some(WorkloadSuitability {
                sessions: Suitability::Refused,
                ..WorkloadSuitability::of_kind(&ExecutorKind::Docker)
            }),
            groups: vec![group],
            ..OperatorSettings::default()
        };
        let written = ExecutorRegistry::new()
            .with_backend("lab", Arc::new(StubBackend::local(ExecutorKind::Docker)))
            .with_settings("lab", settings);

        let caps = written.caps("lab").unwrap();
        assert_eq!(
            (caps.max_walltime_ms, caps.groups.clone()),
            (Some(60_000), vec![group])
        );
        assert!(caps.file_staging, "adapter capabilities stay");
        assert_eq!(
            written.offers()[0].suitability().sessions,
            Suitability::Refused
        );
        let advertised = &written.capabilities(&subject(), false).unwrap()[0];
        assert_eq!(advertised.groups, [group]);
        assert_eq!(advertised.max_walltime_ms, Some(60_000));
        assert_ne!(written.config_digest("lab"), plain.config_digest("lab"));
        assert_eq!(plain.caps("lab").unwrap().max_walltime_ms, None);
    }

    #[test]
    fn advertises_backend_site() {
        // Each backend advertises its own kind, site locality, and stored digest.
        let registry = ExecutorRegistry::new().with_backend(
            "default",
            Arc::new(StubBackend::local(ExecutorKind::Docker)),
        );
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
