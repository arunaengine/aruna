use std::fmt::Debug;
use std::future::Future;
use std::io::{self, Read, Write};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use aruna_core::compute::{
    AdoptableEvidence, ArtifactEvidence, AttemptPhase, AttemptStatus, BackendError, CancelEvidence,
    ExecutorKind, FenceContext, LogLimits, LogStream, LogTails, MAX_OUTPUT_MATCHES,
    MAX_TRANSFER_BYTES, NOBODY, OutputMatcher, ReconcileEvidence, ResumePoint, StagingMode,
    TaskOutput, TaskSpec, TombstoneEvidence, TombstoneSpec, UserSpec, literal_prefix,
    normalize_container_path,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use json_patch::Patch as JsonPatch;
use k8s_openapi::api::authorization::v1::SelfSubjectAccessReview;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{
    ConfigMap, ContainerState, Namespace, PersistentVolume, PersistentVolumeClaim, Pod, Secret,
    ServiceAccount,
};
use k8s_openapi::api::networking::v1::NetworkPolicy;
use k8s_openapi::api::storage::v1::{CSIDriver, StorageClass};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use k8s_openapi::jiff::Timestamp;
use kube::api::{
    Api, AttachParams, DeleteParams, ListParams, LogParams, Patch, PatchParams, PostParams,
    Preconditions,
};
use kube::{Client, ResourceExt};
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio_util::io::{StreamReader, SyncIoBridge};
use tokio_util::sync::CancellationToken;

use super::config::{KubernetesConfig, MAX_NODE_SELECTOR_ENTRIES};
use super::logs::{BoundedTail, LogSink};
use super::staging::{StageLayout, StagePlan};
use super::{BackendCaps, ExecutorBackend, digest_pinned};

mod manifest;

use manifest::{
    HELPER_PATH, StageMarker, WORKSPACE_PATH, helper_pod, job_manifest, marker_manifest,
    marker_name, mount_buckets, mount_name, mount_pv_manifest, mount_pvc_manifest, needs_workspace,
    network_policies, pvc_manifest, secret_manifest, secret_name, workspace_name,
};

pub const EPOCH_ANNOTATION: &str = "aruna-engine.org/attempt-epoch";
pub const GENERATION_ANNOTATION: &str = "aruna-engine.org/controller-generation";
pub const STATE_ANNOTATION: &str = "aruna-engine.org/state";
pub const ROLE_LABEL: &str = "aruna-engine.org/role";
const DEFAULT_WORKSPACE_BYTES: u64 = 10 * 1024 * 1024 * 1024;
/// Bounds one helper listing response; the helper enforces the same bound.
const MAX_LISTING_BYTES: u64 = 16 * 1024 * 1024;
/// Bounds the wait for an exec status so a stalled helper cannot hang a capture.
const EXEC_JOIN_TIMEOUT: Duration = Duration::from_secs(120);
/// Attach buffer for the helper standard streams, several times the archive read
/// size. kube-rs otherwise defaults to 1 KiB, which throttles a transfer to a trickle
/// and, on the read side, blocks the frame loop that drives its keepalive ping.
const EXEC_STREAM_BUF_BYTES: usize = 512 * 1024;

#[derive(Clone)]
pub struct KubernetesBackend {
    client: Client,
    config: KubernetesConfig,
}

impl KubernetesBackend {
    pub async fn with_config(config: KubernetesConfig) -> Result<Self, BackendError> {
        let client = Client::try_default().await.map_err(kube_error)?;
        Self::from_client(client, config)
    }

    pub fn from_client(client: Client, config: KubernetesConfig) -> Result<Self, BackendError> {
        validate_config(&config)?;
        Ok(Self { client, config })
    }

    pub fn config(&self) -> &KubernetesConfig {
        &self.config
    }

    fn jobs(&self) -> Api<Job> {
        Api::namespaced(self.client.clone(), &self.config.namespace)
    }

    fn pods(&self) -> Api<Pod> {
        Api::namespaced(self.client.clone(), &self.config.namespace)
    }

    fn pvcs(&self) -> Api<PersistentVolumeClaim> {
        Api::namespaced(self.client.clone(), &self.config.namespace)
    }

    fn pvs(&self) -> Api<PersistentVolume> {
        Api::all(self.client.clone())
    }

    fn markers(&self) -> Api<ConfigMap> {
        Api::namespaced(self.client.clone(), &self.config.namespace)
    }

    fn secrets(&self) -> Api<Secret> {
        Api::namespaced(self.client.clone(), &self.config.namespace)
    }

    async fn get_job(&self, context: &FenceContext) -> Result<Job, BackendError> {
        let job = self
            .jobs()
            .get(&context.attempt.external_name())
            .await
            .map_err(kube_error)?;
        validate_job(&job, context)?;
        Ok(job)
    }

    async fn ensure_job(
        &self,
        context: &FenceContext,
        spec: &TaskSpec,
        layout: &StageLayout,
    ) -> Result<Job, BackendError> {
        let manifest = job_manifest(context, spec, &self.config, layout)?;
        match self.jobs().create(&PostParams::default(), &manifest).await {
            Ok(job) => Ok(job),
            Err(error) if api_code(&error) == Some(409) => self.get_job(context).await,
            Err(error) => Err(kube_error(error)),
        }
    }

    async fn ensure_pvc(&self, context: &FenceContext, bytes: u64) -> Result<(), BackendError> {
        let pvc = pvc_manifest(context, &self.config, bytes)?;
        match self.pvcs().create(&PostParams::default(), &pvc).await {
            Ok(_) => Ok(()),
            Err(error) if api_code(&error) == Some(409) => {
                let existing = self
                    .pvcs()
                    .get(&workspace_name(&context.attempt.external_name()))
                    .await
                    .map_err(kube_error)?;
                let spec = existing.spec.ok_or_else(|| {
                    BackendError::Conflict("workspace PVC has no spec".to_string())
                })?;
                if spec.access_modes.as_deref() != Some(&["ReadWriteOncePod".to_string()])
                    || spec.storage_class_name.as_deref() != Some(&self.config.storage_class)
                {
                    return Err(BackendError::Conflict(
                        "workspace PVC does not satisfy the RWOP contract".to_string(),
                    ));
                }
                Ok(())
            }
            Err(error) => Err(kube_error(error)),
        }
    }

    async fn ensure_secret(
        &self,
        context: &FenceContext,
        spec: &TaskSpec,
    ) -> Result<(), BackendError> {
        let secret = secret_manifest(context, &self.config, spec)?;
        match self.secrets().create(&PostParams::default(), &secret).await {
            Ok(_) => Ok(()),
            Err(error) if api_code(&error) == Some(409) => {
                let existing = self
                    .secrets()
                    .get(&secret_name(&context.attempt.external_name()))
                    .await
                    .map_err(kube_error)?;
                let epoch = existing
                    .metadata
                    .annotations
                    .as_ref()
                    .and_then(|annotations| annotations.get(EPOCH_ANNOTATION))
                    .and_then(|epoch| epoch.parse::<u64>().ok());
                if epoch == Some(context.attempt_epoch) {
                    Ok(())
                } else {
                    Err(BackendError::Conflict(
                        "DirectS3 credential Secret belongs to another epoch".to_string(),
                    ))
                }
            }
            Err(error) => Err(kube_error(error)),
        }
    }

    async fn ensure_mounts(
        &self,
        context: &FenceContext,
        layout: &StageLayout,
    ) -> Result<(), BackendError> {
        let driver = self.config.s3_mount_driver.as_deref().ok_or_else(|| {
            BackendError::InvalidSpec("S3 mounts are disabled on this backend".to_string())
        })?;
        for (index, bucket) in mount_buckets(layout).iter().enumerate() {
            let name = mount_name(&context.attempt.external_name(), index);
            let pv = mount_pv_manifest(context, &self.config, index, bucket)?;
            match self.pvs().create(&PostParams::default(), &pv).await {
                Ok(_) => {}
                Err(error) if api_code(&error) == Some(409) => {
                    let existing = self.pvs().get(&name).await.map_err(kube_error)?;
                    let valid = existing
                        .metadata
                        .annotations
                        .as_ref()
                        .and_then(|annotations| annotations.get(EPOCH_ANNOTATION))
                        .and_then(|epoch| epoch.parse::<u64>().ok())
                        == Some(context.attempt_epoch)
                        && existing
                            .spec
                            .as_ref()
                            .and_then(|spec| spec.csi.as_ref())
                            .is_some_and(|csi| {
                                csi.driver == driver
                                    && csi.volume_handle == name
                                    && csi
                                        .volume_attributes
                                        .as_ref()
                                        .and_then(|attributes| attributes.get("bucketName"))
                                        == Some(bucket)
                            });
                    if !valid {
                        return Err(BackendError::Conflict(format!(
                            "S3 PersistentVolume `{name}` belongs to another attempt"
                        )));
                    }
                }
                Err(error) => return Err(kube_error(error)),
            }
            let pvc = mount_pvc_manifest(context, &self.config, index)?;
            match self.pvcs().create(&PostParams::default(), &pvc).await {
                Ok(_) => {}
                Err(error) if api_code(&error) == Some(409) => {
                    let existing = self.pvcs().get(&name).await.map_err(kube_error)?;
                    let valid = existing
                        .metadata
                        .annotations
                        .as_ref()
                        .and_then(|annotations| annotations.get(EPOCH_ANNOTATION))
                        .and_then(|epoch| epoch.parse::<u64>().ok())
                        == Some(context.attempt_epoch)
                        && existing
                            .spec
                            .as_ref()
                            .and_then(|spec| spec.volume_name.as_deref())
                            == Some(name.as_str());
                    if !valid {
                        return Err(BackendError::Conflict(format!(
                            "S3 PersistentVolumeClaim `{name}` belongs to another attempt"
                        )));
                    }
                }
                Err(error) => return Err(kube_error(error)),
            }
        }
        Ok(())
    }

    async fn remove_mounts(&self, context: &FenceContext) -> Result<(), BackendError> {
        let selector = format!("{},{}=s3-mount", attempt_selector(context), ROLE_LABEL);
        for pvc in self
            .pvcs()
            .list(&ListParams::default().labels(&selector))
            .await
            .map_err(kube_error)?
        {
            delete_named(self.pvcs(), &pvc.name_any()).await?;
        }
        for pv in self
            .pvs()
            .list(&ListParams::default().labels(&selector))
            .await
            .map_err(kube_error)?
        {
            delete_named(self.pvs(), &pv.name_any()).await?;
        }
        Ok(())
    }

    async fn apply_network(&self) -> Result<(), BackendError> {
        let policies: Api<NetworkPolicy> =
            Api::namespaced(self.client.clone(), &self.config.namespace);
        let params = PatchParams::apply("aruna-compute");
        for policy in network_policies(&self.config)? {
            policies
                .patch(&policy.name_any(), &params, &Patch::Apply(&policy))
                .await
                .map_err(kube_error)?;
        }
        Ok(())
    }

    async fn remove_helpers(&self, context: &FenceContext) -> Result<(), BackendError> {
        let selector = attempt_selector(context);
        let pods = self
            .pods()
            .list(&ListParams::default().labels(&selector))
            .await
            .map_err(kube_error)?;
        for pod in pods {
            let role = pod
                .metadata
                .labels
                .as_ref()
                .and_then(|labels| labels.get(ROLE_LABEL))
                .map(String::as_str);
            if matches!(role, Some("stage" | "fetch")) {
                self.delete_pod(&pod).await?;
            }
        }
        Ok(())
    }

    async fn lower_helpers(&self, context: &FenceContext) -> Result<Vec<Pod>, BackendError> {
        let selector = attempt_selector(context);
        let pods = self
            .pods()
            .list(&ListParams::default().labels(&selector))
            .await
            .map_err(kube_error)?;
        Ok(pods
            .into_iter()
            .filter(|pod| {
                let role = pod
                    .metadata
                    .labels
                    .as_ref()
                    .and_then(|labels| labels.get(ROLE_LABEL))
                    .map(String::as_str);
                let generation = pod
                    .metadata
                    .annotations
                    .as_ref()
                    .and_then(|annotations| annotations.get(GENERATION_ANNOTATION))
                    .and_then(|generation| generation.parse::<u64>().ok());
                matches!(role, Some("stage" | "fetch"))
                    && generation
                        .is_some_and(|generation| generation < context.controller_generation)
            })
            .collect())
    }

    async fn delete_pod(&self, pod: &Pod) -> Result<(), BackendError> {
        let name = pod.name_any();
        let uid = pod
            .metadata
            .uid
            .clone()
            .ok_or_else(|| BackendError::Api(format!("Pod `{name}` has no UID")))?;
        let params = DeleteParams {
            grace_period_seconds: Some(30),
            preconditions: Some(Preconditions {
                uid: Some(uid),
                resource_version: None,
            }),
            ..DeleteParams::default()
        };
        match self.pods().delete(&name, &params).await {
            Ok(_) => self.wait_pod_gone(&name).await,
            Err(error) if api_code(&error) == Some(404) => Ok(()),
            Err(error) => Err(kube_error(error)),
        }
    }

    async fn wait_pod_gone(&self, name: &str) -> Result<(), BackendError> {
        let deadline = Instant::now() + self.config.pull_deadline;
        loop {
            match self.pods().get_opt(name).await.map_err(kube_error)? {
                None => return Ok(()),
                Some(_) if Instant::now() >= deadline => {
                    return Err(BackendError::Timeout(format!(
                        "Pod `{name}` did not terminate gracefully"
                    )));
                }
                Some(_) => tokio::time::sleep(Duration::from_millis(250)).await,
            }
        }
    }

    async fn wait_pod(&self, name: &str, cancel: &CancellationToken) -> Result<Pod, BackendError> {
        let deadline = Instant::now() + self.config.pull_deadline;
        loop {
            if cancel.is_cancelled() {
                return Err(BackendError::Cancelled);
            }
            let pod = self.pods().get(name).await.map_err(kube_error)?;
            match pod
                .status
                .as_ref()
                .and_then(|status| status.phase.as_deref())
            {
                Some("Running") => return Ok(pod),
                Some("Failed" | "Succeeded") => {
                    return Err(BackendError::Api(format!(
                        "helper Pod `{name}` terminated before use"
                    )));
                }
                _ if Instant::now() >= deadline => {
                    return Err(BackendError::Timeout(format!(
                        "helper Pod `{name}` did not mount its RWOP workspace"
                    )));
                }
                _ => tokio::time::sleep(Duration::from_millis(250)).await,
            }
        }
    }

    async fn create_helper(
        &self,
        context: &FenceContext,
        role: &str,
        cancel: &CancellationToken,
    ) -> Result<Pod, BackendError> {
        let pod = helper_pod(context, &self.config, role)?;
        let name = pod.name_any();
        match self.pods().create(&PostParams::default(), &pod).await {
            Ok(_) => self.wait_pod(&name, cancel).await,
            Err(error) if api_code(&error) == Some(409) => {
                let existing = self.pods().get(&name).await.map_err(kube_error)?;
                self.delete_pod(&existing).await?;
                self.pods()
                    .create(&PostParams::default(), &pod)
                    .await
                    .map_err(kube_error)?;
                self.wait_pod(&name, cancel).await
            }
            Err(error) => Err(kube_error(error)),
        }
    }

    async fn remove_marker(&self, context: &FenceContext) -> Result<(), BackendError> {
        let name = marker_name(&context.attempt.external_name());
        match self.markers().get_opt(&name).await.map_err(kube_error)? {
            Some(marker) => {
                let uid = marker
                    .metadata
                    .uid
                    .ok_or_else(|| BackendError::Api("stage marker has no UID".to_string()))?;
                let params = DeleteParams {
                    preconditions: Some(Preconditions {
                        uid: Some(uid),
                        resource_version: None,
                    }),
                    ..DeleteParams::default()
                };
                self.markers()
                    .delete(&name, &params)
                    .await
                    .map_err(kube_error)?;
                Ok(())
            }
            None => Ok(()),
        }
    }

    async fn write_marker(
        &self,
        context: &FenceContext,
        marker: &StageMarker,
    ) -> Result<(), BackendError> {
        let manifest = marker_manifest(context, &self.config, marker)?;
        self.markers()
            .create(&PostParams::default(), &manifest)
            .await
            .map(|_| ())
            .map_err(kube_error)
    }

    async fn stage_archive(
        &self,
        pod: &Pod,
        plan: StagePlan,
        marker: &StageMarker,
    ) -> Result<(), BackendError> {
        let marker = serde_json::to_string(marker)
            .map_err(|error| BackendError::Api(format!("serialize stage marker: {error}")))?;
        let params = AttachParams::default()
            .container("helper")
            .stdin(true)
            .stdout(false)
            .stderr(false)
            .max_stdin_buf_size(EXEC_STREAM_BUF_BYTES);
        let mut attached = self
            .pods()
            .exec(
                &pod.name_any(),
                [
                    HELPER_PATH,
                    "stage",
                    "--workspace",
                    "/workspace",
                    "--sentinel",
                    "/workspace/.aruna-stage",
                    "--marker",
                    &marker,
                ],
                &params,
            )
            .await
            .map_err(kube_error)?;
        let mut stdin = attached.stdin().ok_or_else(|| {
            BackendError::Api("stage exec did not expose standard input".to_string())
        })?;
        let (tx, mut rx) = mpsc::channel(4);
        let builder = tokio::task::spawn_blocking(move || build_archive(tx, plan));
        while let Some(chunk) = rx.recv().await {
            stdin.write_all(&chunk?).await.map_err(io_error)?;
        }
        stdin.shutdown().await.map_err(io_error)?;
        drop(stdin);
        let built = builder
            .await
            .map_err(|error| BackendError::Api(format!("stage archive task failed: {error}")))?;
        built?;
        join_exec(attached).await
    }

    async fn cas_generation(&self, context: &FenceContext, job: Job) -> Result<(), BackendError> {
        let observed = job_generation(&job)?;
        if observed > context.controller_generation {
            return Err(BackendError::Fenced);
        }
        if observed == context.controller_generation {
            return Ok(());
        }
        let patch = cas_patch(
            &job,
            vec![(
                format!(
                    "/metadata/annotations/{}",
                    escape_pointer(GENERATION_ANNOTATION)
                ),
                json!(context.controller_generation.to_string()),
            )],
        )?;
        self.jobs()
            .patch(
                &context.attempt.external_name(),
                &PatchParams::default(),
                &Patch::Json::<()>(patch),
            )
            .await
            .map(|_| ())
            .map_err(kube_error)
    }

    async fn patch_state(
        &self,
        context: &FenceContext,
        state: &str,
        suspend: bool,
    ) -> Result<Job, BackendError> {
        let job = self.get_job(context).await?;
        let patch = cas_patch(
            &job,
            vec![
                (
                    format!("/metadata/annotations/{}", escape_pointer(STATE_ANNOTATION)),
                    json!(state),
                ),
                ("/spec/suspend".to_string(), json!(suspend)),
            ],
        )?;
        self.jobs()
            .patch(
                &context.attempt.external_name(),
                &PatchParams::default(),
                &Patch::Json::<()>(patch),
            )
            .await
            .map_err(kube_error)
    }

    async fn strip_finalizer(&self, context: &FenceContext, job: &Job) -> Result<(), BackendError> {
        if !job.metadata.finalizers.as_ref().is_some_and(|finalizers| {
            finalizers
                .iter()
                .any(|finalizer| finalizer == "aruna-engine.org/attempt-protection")
        }) {
            return Ok(());
        }
        let patch = cas_patch(job, vec![("/metadata/finalizers".to_string(), json!([]))])?;
        self.jobs()
            .patch(
                &context.attempt.external_name(),
                &PatchParams::default(),
                &Patch::Json::<()>(patch),
            )
            .await
            .map(|_| ())
            .map_err(kube_error)
    }

    async fn task_pods(&self, context: &FenceContext) -> Result<Vec<Pod>, BackendError> {
        let selector = format!("{},{}=task", attempt_selector(context), ROLE_LABEL);
        self.pods()
            .list(&ListParams::default().labels(&selector))
            .await
            .map(|pods| pods.items)
            .map_err(kube_error)
    }

    async fn observed_status(
        &self,
        context: &FenceContext,
        job: &Job,
    ) -> Result<AttemptStatus, BackendError> {
        let mut status = job_status(job);
        if status.is_terminal() {
            let pods = self.task_pods(context).await?;
            if let Some(terminated) = pods
                .iter()
                .find_map(|pod| task_state(pod)?.terminated.as_ref())
            {
                status.phase = AttemptPhase::Exited {
                    code: terminated.exit_code,
                };
                // A failed Job carries no completionTime, so the container is the
                // only terminal timing evidence.
                status.started_at_ms = status
                    .started_at_ms
                    .or_else(|| time_ms(terminated.started_at.as_ref()));
                status.finished_at_ms = status
                    .finished_at_ms
                    .or_else(|| time_ms(terminated.finished_at.as_ref()));
            }
            return Ok(status);
        }
        let pending = job_pending(job);
        if !pending && status.started_at_ms.is_some() {
            return Ok(status);
        }
        // Costs one extra Pod list per poll until a start time is observed: the
        // Job reports startTime only once its controller has processed the Job.
        let pods = self.task_pods(context).await?;
        if pending {
            let now = Timestamp::now();
            if let Some(reason) = pods
                .iter()
                .find_map(|pod| pod_stuck_reason(pod, self.config.pull_deadline, now))
            {
                tracing::warn!(
                    attempt = %context.attempt.external_name(),
                    %reason,
                    "failing attempt whose container cannot start"
                );
                status.phase = AttemptPhase::SystemError { reason };
                return Ok(status);
            }
        }
        status.started_at_ms = status
            .started_at_ms
            .or_else(|| pods.iter().find_map(pod_started));
        Ok(status)
    }

    async fn save_logs(&self, context: &FenceContext) -> Result<(), BackendError> {
        let name = logs_name(&context.attempt.external_name());
        if self
            .markers()
            .get_opt(&name)
            .await
            .map_err(kube_error)?
            .is_some()
        {
            return Ok(());
        }
        let pods = self.task_pods(context).await?;
        let Some(pod) = pods.first() else {
            return Ok(());
        };
        let logs = self
            .pods()
            .logs(
                &pod.name_any(),
                &LogParams {
                    container: Some("task".to_string()),
                    limit_bytes: Some(512 * 1024),
                    ..LogParams::default()
                },
            )
            .await
            .map_err(kube_error)?;
        let marker: ConfigMap = serde_json::from_value(json!({
            "apiVersion":"v1","kind":"ConfigMap",
            "metadata":{"name":name,"namespace":self.config.namespace},
            "data":{"stdout":logs}
        }))
        .map_err(|error| BackendError::Api(format!("build log record: {error}")))?;
        self.markers()
            .create(&PostParams::default(), &marker)
            .await
            .map(|_| ())
            .or_else(|error| {
                if api_code(&error) == Some(409) {
                    Ok(())
                } else {
                    Err(error)
                }
            })
            .map_err(kube_error)
    }

    async fn delete_tasks(&self, context: &FenceContext) -> Result<(), BackendError> {
        for pod in self.task_pods(context).await? {
            self.delete_pod(&pod).await?;
        }
        Ok(())
    }

    /// Enumerate the workspace below `prefix` through a helper Pod and keep the
    /// paths the pattern selects. The helper emits NUL-terminated paths closed
    /// by an empty record and the decimal path count.
    async fn list_archive(
        &self,
        context: &FenceContext,
        prefix: &str,
        glob: &OutputMatcher,
    ) -> Result<Vec<String>, BackendError> {
        let pod = self
            .create_helper(context, "fetch", &CancellationToken::new())
            .await?;
        let params = AttachParams::default()
            .container("helper")
            .stdin(false)
            .stdout(true)
            .stderr(false)
            .max_stdout_buf_size(EXEC_STREAM_BUF_BYTES);
        let listed = async {
            let mut attached = self
                .pods()
                .exec(
                    &pod.name_any(),
                    [
                        HELPER_PATH,
                        "list",
                        "--workspace",
                        WORKSPACE_PATH,
                        "--prefix",
                        prefix,
                    ],
                    &params,
                )
                .await
                .map_err(kube_error)?;
            let stdout = attached.stdout().ok_or_else(|| {
                BackendError::Api("list exec did not expose standard output".to_string())
            })?;
            let mut listing = Vec::new();
            // A helper wedged mid-listing must not stall the capture forever.
            let read = tokio::time::timeout(
                EXEC_JOIN_TIMEOUT,
                stdout.take(MAX_LISTING_BYTES).read_to_end(&mut listing),
            )
            .await
            .map_err(|_| BackendError::Timeout("output listing read timed out".to_string()))?;
            if let Err(error) = read {
                return Err(joined_error(join_exec(attached), io_error(error)).await);
            }
            join_exec(attached).await?;
            select_listed(&listing, glob)
        }
        .await;
        let _ = self.delete_pod(&pod).await;
        listed
    }

    async fn fetch_archive(
        &self,
        context: &FenceContext,
        path: &str,
    ) -> Result<TaskOutput, BackendError> {
        let pod = self
            .create_helper(context, "fetch", &CancellationToken::new())
            .await?;
        let params = AttachParams::default()
            .container("helper")
            .stdin(false)
            .stdout(true)
            .stderr(false)
            .max_stdout_buf_size(EXEC_STREAM_BUF_BYTES);
        let mut attached = self
            .pods()
            .exec(
                &pod.name_any(),
                [
                    HELPER_PATH,
                    "fetch",
                    "--workspace",
                    "/workspace",
                    "--path",
                    path,
                ],
                &params,
            )
            .await
            .map_err(kube_error)?;
        let mut stdout = attached.stdout().ok_or_else(|| {
            BackendError::Api("fetch exec did not expose standard output".to_string())
        })?;
        // The helper Pod still has to go on either failure.
        let (size, join) = match read_header(&mut stdout, path, join_exec(attached)).await {
            Ok(header) => header,
            Err(error) => {
                let _ = self.delete_pod(&pod).await;
                return Err(error);
            }
        };
        let (tx, rx) = mpsc::channel(4);
        let backend = self.clone();
        tokio::spawn(async move {
            stream_output(stdout, join, size, tx).await;
            let _ = backend.delete_pod(&pod).await;
        });
        Ok(TaskOutput {
            size,
            chunks: Box::pin(ChannelStream(rx)),
        })
    }
}

#[async_trait]
impl ExecutorBackend for KubernetesBackend {
    fn kind(&self) -> ExecutorKind {
        ExecutorKind::Kubernetes
    }

    /// Workers do not run on the controller: the advertised site is the
    /// configured worker placement, and network isolation only counts as proven
    /// when those pods are actually pinned to it.
    fn capabilities(&self) -> BackendCaps {
        let worker_site = self.config.worker_site();
        BackendCaps {
            file_staging: true,
            direct_s3: !self.config.s3_cidrs.is_empty(),
            s3_mount: self.config.s3_mount_driver.is_some(),
            network_policy: worker_site.is_some(),
            local_site: false,
            worker_site,
            limits: self.config.envelope,
        }
    }

    /// The pod securityContext switches users at launch, so the identity is pinned.
    fn run_identity(&self) -> UserSpec {
        NOBODY
    }

    async fn health(&self) -> Result<(), BackendError> {
        let namespaces: Api<Namespace> = Api::all(self.client.clone());
        namespaces
            .get(&self.config.namespace)
            .await
            .map_err(kube_error)?;
        let classes: Api<StorageClass> = Api::all(self.client.clone());
        classes
            .get(&self.config.storage_class)
            .await
            .map_err(kube_error)?;
        if let Some(driver) = &self.config.s3_mount_driver {
            let drivers: Api<CSIDriver> = Api::all(self.client.clone());
            drivers.get(driver).await.map_err(kube_error)?;
        }
        let accounts: Api<ServiceAccount> =
            Api::namespaced(self.client.clone(), &self.config.namespace);
        accounts
            .get(&self.config.service_account)
            .await
            .map_err(kube_error)?;
        for (group, resource, subresource, verb) in
            required_access(self.config.s3_mount_driver.is_some())
        {
            check_access(
                self.client.clone(),
                &self.config.namespace,
                group,
                resource,
                subresource,
                verb,
            )
            .await?;
        }
        Ok(())
    }

    async fn resolve_image(
        &self,
        image: &str,
        _cancel: &CancellationToken,
    ) -> Result<String, BackendError> {
        Ok(image.to_string())
    }

    async fn fence(&self, context: &FenceContext) -> Result<(), BackendError> {
        validate_name(&context.attempt.external_name())?;
        match self.jobs().get_opt(&context.attempt.external_name()).await {
            Ok(Some(job)) => {
                validate_epoch(&job, context)?;
                self.cas_generation(context, job).await
            }
            Ok(None) => Ok(()),
            Err(error) => Err(kube_error(error)),
        }
    }

    async fn submit(
        &self,
        context: &FenceContext,
        spec: &TaskSpec,
        cancel: &CancellationToken,
    ) -> Result<AttemptStatus, BackendError> {
        validate_spec(context, spec, &self.config)?;
        self.apply_network().await?;
        let layout = StageLayout::from_spec(spec)?;
        let job = self.ensure_job(context, spec, &layout).await?;
        validate_job(&job, context)?;
        if job_state(&job) == Some("tombstone") {
            return Err(BackendError::Conflict("attempt is tombstoned".to_string()));
        }
        let status = job_status(&job);
        if status.is_terminal()
            || job
                .spec
                .as_ref()
                .is_some_and(|spec| spec.suspend == Some(false))
        {
            return Ok(status);
        }
        self.remove_helpers(context).await?;
        self.remove_marker(context).await?;
        match spec.staging_mode {
            StagingMode::Files => {
                self.ensure_pvc(
                    context,
                    spec.resources.disk_bytes.unwrap_or(DEFAULT_WORKSPACE_BYTES),
                )
                .await?;
                self.stage(context, spec, cancel).await?;
            }
            StagingMode::DirectS3 => self.ensure_secret(context, spec).await?,
            StagingMode::S3Mount => {
                if !spec.s3_mounts.is_empty() {
                    self.ensure_secret(context, spec).await?;
                    self.ensure_mounts(context, &layout).await?;
                }
                if needs_workspace(spec) {
                    self.ensure_pvc(
                        context,
                        spec.resources.disk_bytes.unwrap_or(DEFAULT_WORKSPACE_BYTES),
                    )
                    .await?;
                    self.stage(context, spec, cancel).await?;
                }
            }
        }
        self.unsuspend(context, cancel).await
    }

    async fn stage(
        &self,
        context: &FenceContext,
        spec: &TaskSpec,
        cancel: &CancellationToken,
    ) -> Result<(), BackendError> {
        if !needs_workspace(spec) {
            return Ok(());
        }
        let job = self.get_job(context).await?;
        if job
            .spec
            .as_ref()
            .is_some_and(|spec| spec.suspend == Some(false))
        {
            return Err(BackendError::Conflict(
                "cannot stage an unsuspended Job".to_string(),
            ));
        }
        self.remove_marker(context).await?;
        let plan = StagePlan::from_spec(spec)?;
        let marker = StageMarker {
            job_uid: job_uid(&job)?,
            attempt_epoch: context.attempt_epoch,
            controller_generation: context.controller_generation,
            layout_digest: plan.layout.digest(),
        };
        let pod = self.create_helper(context, "stage", cancel).await?;
        let staged = self.stage_archive(&pod, plan, &marker).await;
        self.delete_pod(&pod).await?;
        staged?;
        self.write_marker(context, &marker).await
    }

    async fn unsuspend(
        &self,
        context: &FenceContext,
        _cancel: &CancellationToken,
    ) -> Result<AttemptStatus, BackendError> {
        let job = self.get_job(context).await?;
        if job_state(&job) == Some("tombstone") {
            return Err(BackendError::Conflict("attempt is tombstoned".to_string()));
        }
        if job
            .spec
            .as_ref()
            .is_some_and(|spec| spec.suspend == Some(false))
        {
            return Ok(job_status(&job));
        }
        let staging = job
            .metadata
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.get("aruna-engine.org/staging-mode"))
            .map(String::as_str);
        if matches!(staging, Some("files" | "s3-mount")) && job_workspace(&job) {
            let marker = self
                .markers()
                .get(&marker_name(&context.attempt.external_name()))
                .await
                .map_err(kube_error)?;
            validate_marker(&marker, &job, context)?;
        }
        let lower = self.lower_helpers(context).await?;
        if !lower.is_empty() {
            for pod in lower {
                self.delete_pod(&pod).await?;
            }
            return Err(BackendError::Conflict(
                "lower-generation helper appeared; restaging is required".to_string(),
            ));
        }
        let patch = cas_patch(&job, vec![("/spec/suspend".to_string(), json!(false))])?;
        let job = self
            .jobs()
            .patch(
                &context.attempt.external_name(),
                &PatchParams::default(),
                &Patch::Json::<()>(patch),
            )
            .await
            .map_err(kube_error)?;
        Ok(job_status(&job))
    }

    async fn status(&self, context: &FenceContext) -> Result<AttemptStatus, BackendError> {
        let job = self.get_job(context).await?;
        self.observed_status(context, &job).await
    }

    async fn cancel(&self, context: &FenceContext) -> Result<CancelEvidence, BackendError> {
        let job = match self.get_job(context).await {
            Ok(job) => job,
            Err(BackendError::NotFound(_)) => return Ok(CancelEvidence::AlreadyGone),
            Err(error) => return Err(error),
        };
        let status = job_status(&job);
        if status.is_terminal() {
            return Ok(CancelEvidence::Stopped(status));
        }
        self.remove_helpers(context).await?;
        // Logs from a stuck pod may be unavailable; that must not block cancel.
        if let Err(error) = self.save_logs(context).await {
            tracing::warn!(%error, "log capture failed during cancel");
        }
        for pod in self.task_pods(context).await? {
            self.delete_pod(&pod).await?;
        }
        let job = self.patch_state(context, "cancelled", true).await?;
        Ok(CancelEvidence::Stopped(AttemptStatus {
            phase: AttemptPhase::Cancelled,
            ..job_status(&job)
        }))
    }

    async fn fetch_logs(
        &self,
        context: &FenceContext,
        limits: &LogLimits,
        sink: &dyn LogSink,
    ) -> Result<LogTails, BackendError> {
        let stored = self
            .markers()
            .get_opt(&logs_name(&context.attempt.external_name()))
            .await
            .map_err(kube_error)?
            .and_then(|marker| marker.data)
            .and_then(|mut data| data.remove("stdout"));
        let bytes = if let Some(stored) = stored {
            stored.into_bytes()
        } else {
            let pods = self.task_pods(context).await?;
            let Some(pod) = pods.first() else {
                return Ok(LogTails::default());
            };
            // A container that never started rejects log reads, which must not
            // block the terminal handling that reaps the attempt.
            if !task_started(pod) {
                return Ok(LogTails::default());
            }
            self.pods()
                .logs(
                    &pod.name_any(),
                    &LogParams {
                        container: Some("task".to_string()),
                        limit_bytes: i64::try_from(limits.max_bytes_per_stream).ok(),
                        ..LogParams::default()
                    },
                )
                .await
                .map_err(kube_error)?
                .into_bytes()
        };
        sink.write(LogStream::Stdout, &bytes);
        let mut tail = BoundedTail::new(limits.max_bytes_per_stream);
        tail.push(&bytes);
        Ok(LogTails {
            stdout_total: tail.total(),
            stdout_truncated: tail.truncated(),
            stdout: tail.into_bytes(),
            ..LogTails::default()
        })
    }

    async fn fetch_output(
        &self,
        context: &FenceContext,
        path: &str,
    ) -> Result<TaskOutput, BackendError> {
        let job = self.get_job(context).await?;
        if !job_status(&job).is_terminal() {
            return Err(BackendError::InvalidSpec(
                "attempt is not terminal".to_string(),
            ));
        }
        validate_output(&job, path)?;
        self.save_logs(context).await?;
        self.delete_tasks(context).await?;
        Box::pin(self.fetch_archive(context, path)).await
    }

    async fn list_outputs(
        &self,
        context: &FenceContext,
        pattern: &str,
    ) -> Result<Vec<String>, BackendError> {
        let job = self.get_job(context).await?;
        if !job_status(&job).is_terminal() {
            return Err(BackendError::InvalidSpec(
                "attempt is not terminal".to_string(),
            ));
        }
        validate_output(&job, pattern)?;
        let glob = OutputMatcher::new([pattern]).map_err(BackendError::InvalidSpec)?;
        let prefix = literal_prefix(pattern).map_err(BackendError::InvalidSpec)?;
        // The listing helper needs the workspace claim, which the task pods hold
        // until they are reaped.
        self.save_logs(context).await?;
        self.delete_tasks(context).await?;
        Box::pin(self.list_archive(context, &prefix.to_string_lossy(), &glob)).await
    }

    async fn reconcile(&self, context: &FenceContext) -> ReconcileEvidence {
        match self.jobs().get_opt(&context.attempt.external_name()).await {
            Ok(Some(job)) => {
                if validate_epoch(&job, context).is_err() {
                    return ReconcileEvidence::Unadoptable(ArtifactEvidence {
                        artifact_kind: "kubernetes-job".to_string(),
                        backend_ref: job.metadata.uid.clone(),
                        observed_epoch: job_epoch(&job).ok(),
                        observed_generation: job_generation(&job).ok(),
                        exact_identity: false,
                        multiple: false,
                        foreign: true,
                    });
                }
                if job_state(&job) == Some("tombstone") {
                    return ReconcileEvidence::Tombstoned(TombstoneEvidence {
                        backend_ref: job.metadata.uid.clone().unwrap_or_else(|| job.name_any()),
                        attempt_epoch: context.attempt_epoch,
                    });
                }
                let status = match self.observed_status(context, &job).await {
                    Ok(status) => status,
                    Err(error) => return ReconcileEvidence::Unavailable(error),
                };
                let resume = if job
                    .spec
                    .as_ref()
                    .is_some_and(|spec| spec.suspend == Some(true))
                {
                    if !job_workspace(&job) {
                        ResumePoint::Unsuspend
                    } else {
                        let marker = self
                            .markers()
                            .get_opt(&marker_name(&context.attempt.external_name()))
                            .await;
                        if marker.is_ok_and(|marker| {
                            marker.is_some_and(|marker| {
                                validate_marker(&marker, &job, context).is_ok()
                            })
                        }) {
                            ResumePoint::Unsuspend
                        } else {
                            ResumePoint::Stage
                        }
                    }
                } else {
                    ResumePoint::Observe
                };
                ReconcileEvidence::Adoptable(AdoptableEvidence { status, resume })
            }
            Ok(None) => match self.orphan_evidence(context).await {
                Ok(Some(evidence)) => ReconcileEvidence::Unadoptable(evidence),
                Ok(None) => ReconcileEvidence::Absent,
                Err(error) => ReconcileEvidence::Unavailable(error),
            },
            Err(error) => ReconcileEvidence::Unavailable(kube_error(error)),
        }
    }

    async fn tombstone(
        &self,
        context: &FenceContext,
        _spec: &TombstoneSpec,
    ) -> Result<TombstoneEvidence, BackendError> {
        // A cancel racing submit leaves no Job; the absence itself is terminal.
        let mut job = match self.get_job(context).await {
            Ok(job) => job,
            Err(BackendError::NotFound(_)) => {
                return Ok(TombstoneEvidence {
                    backend_ref: context.attempt.external_name(),
                    attempt_epoch: context.attempt_epoch,
                });
            }
            Err(error) => return Err(error),
        };
        if job_state(&job) != Some("tombstone") {
            job = self.patch_state(context, "tombstone", true).await?;
        }
        self.strip_finalizer(context, &job).await?;
        Ok(TombstoneEvidence {
            backend_ref: job_uid(&job)?,
            attempt_epoch: context.attempt_epoch,
        })
    }

    async fn cleanup(&self, context: &FenceContext) -> Result<(), BackendError> {
        let mut job = self.get_job(context).await?;
        if job_state(&job) != Some("tombstone") {
            job = self.patch_state(context, "tombstone", true).await?;
        }
        self.strip_finalizer(context, &job).await?;
        self.remove_helpers(context).await?;
        self.delete_tasks(context).await?;
        self.remove_mounts(context).await?;
        delete_named(
            self.pvcs(),
            &workspace_name(&context.attempt.external_name()),
        )
        .await?;
        delete_named(
            self.secrets(),
            &secret_name(&context.attempt.external_name()),
        )
        .await?;
        delete_named(self.markers(), &logs_name(&context.attempt.external_name())).await?;
        delete_named(
            self.markers(),
            &marker_name(&context.attempt.external_name()),
        )
        .await?;
        // The Job goes last: deleting it earlier makes a cleanup retry observe
        // NotFound, which discharges the obligation and leaks the PVC and Secret.
        delete_named(self.jobs(), &context.attempt.external_name()).await
    }

    async fn sweep_orphans(&self, _grace: Duration) -> Result<(), BackendError> {
        Ok(())
    }
}

impl KubernetesBackend {
    async fn orphan_evidence(
        &self,
        context: &FenceContext,
    ) -> Result<Option<ArtifactEvidence>, BackendError> {
        let pods = self
            .pods()
            .list(&ListParams::default().labels(&attempt_selector(context)))
            .await
            .map_err(kube_error)?;
        let pvc = self
            .pvcs()
            .get_opt(&workspace_name(&context.attempt.external_name()))
            .await
            .map_err(kube_error)?;
        let marker = self
            .markers()
            .get_opt(&marker_name(&context.attempt.external_name()))
            .await
            .map_err(kube_error)?;
        let mount_selector = format!("{},{}=s3-mount", attempt_selector(context), ROLE_LABEL);
        let mounts = self
            .pvs()
            .list(&ListParams::default().labels(&mount_selector))
            .await
            .map_err(kube_error)?;
        if pods.items.is_empty() && pvc.is_none() && marker.is_none() && mounts.items.is_empty() {
            return Ok(None);
        }
        let reference = pods
            .items
            .first()
            .and_then(|pod| pod.metadata.uid.clone())
            .or_else(|| pvc.and_then(|pvc| pvc.metadata.uid))
            .or_else(|| marker.and_then(|marker| marker.metadata.uid))
            .or_else(|| {
                mounts
                    .items
                    .first()
                    .and_then(|mount| mount.metadata.uid.clone())
            });
        Ok(Some(ArtifactEvidence {
            artifact_kind: "kubernetes-accessory".to_string(),
            backend_ref: reference,
            observed_epoch: Some(context.attempt_epoch),
            observed_generation: None,
            exact_identity: true,
            multiple: pods.items.len() + mounts.items.len() > 1,
            foreign: false,
        }))
    }
}

fn validate_config(config: &KubernetesConfig) -> Result<(), BackendError> {
    if config.namespace.is_empty() || config.storage_class.is_empty() {
        return Err(BackendError::InvalidSpec(
            "Kubernetes namespace and StorageClass are required".to_string(),
        ));
    }
    if !digest_pinned(&config.helper_image) {
        return Err(BackendError::InvalidSpec(
            "Kubernetes helper image must be explicitly configured by digest".to_string(),
        ));
    }
    if config.pull_deadline.is_zero() || config.s3_port == 0 {
        return Err(BackendError::InvalidSpec(
            "Kubernetes deadlines and ports must be nonzero".to_string(),
        ));
    }
    if config.service_account.trim().is_empty() {
        return Err(BackendError::InvalidSpec(
            "Kubernetes workload service account is required".to_string(),
        ));
    }
    let bounded = |entries: &std::collections::BTreeMap<String, String>| {
        entries.len() <= MAX_NODE_SELECTOR_ENTRIES
            && entries.keys().all(|key| !key.trim().is_empty())
    };
    if !bounded(&config.node_selector) || !bounded(&config.execution_labels) {
        return Err(BackendError::InvalidSpec(format!(
            "Kubernetes selectors and worker labels are at most \
             {MAX_NODE_SELECTOR_ENTRIES} named entries"
        )));
    }
    Ok(())
}

fn validate_spec(
    context: &FenceContext,
    spec: &TaskSpec,
    config: &KubernetesConfig,
) -> Result<(), BackendError> {
    if context.attempt != spec.attempt {
        return Err(BackendError::InvalidSpec(
            "fence and task attempt differ".to_string(),
        ));
    }
    validate_name(&context.attempt.external_name())?;
    StageLayout::from_spec(spec)?;
    if spec.security.run_as != NOBODY {
        return Err(BackendError::InvalidSpec(
            "Kubernetes tasks must run as 65534:65534".to_string(),
        ));
    }
    if spec.staging_mode == StagingMode::S3Mount && config.s3_mount_driver.is_none() {
        return Err(BackendError::InvalidSpec(
            "S3 mounts are disabled on this backend".to_string(),
        ));
    }
    match (spec.staging_mode, spec.security.network) {
        (
            StagingMode::Files,
            aruna_core::compute::NetworkAccess::Isolated | aruna_core::compute::NetworkAccess::Open,
        ) => {}
        (StagingMode::DirectS3, aruna_core::compute::NetworkAccess::S3Only)
            if !config.s3_cidrs.is_empty() => {}
        (StagingMode::DirectS3, _) => {
            return Err(BackendError::InvalidSpec(
                "DirectS3 requires S3Only networking and configured S3 CIDRs".to_string(),
            ));
        }
        (StagingMode::Files, _) => {
            return Err(BackendError::InvalidSpec(
                "Files staging does not support S3-only networking".to_string(),
            ));
        }
        (
            StagingMode::S3Mount,
            aruna_core::compute::NetworkAccess::Isolated | aruna_core::compute::NetworkAccess::Open,
        ) => {}
        (StagingMode::S3Mount, _) => {
            return Err(BackendError::InvalidSpec(
                "S3Mount does not expose S3 networking to the task".to_string(),
            ));
        }
    }
    if spec.staging_mode == StagingMode::S3Mount
        && !spec.s3_mounts.is_empty()
        && !["access_key_id", "secret_access_key"]
            .iter()
            .all(|key| spec.secret_env.contains_key(*key))
    {
        return Err(BackendError::InvalidSpec(
            "S3Mount credentials are incomplete".to_string(),
        ));
    }
    if let Some(extension) = spec.resources.backend_extensions.keys().next() {
        return Err(BackendError::InvalidSpec(format!(
            "backend extension `{extension}` is not supported by Kubernetes"
        )));
    }
    Ok(())
}

fn validate_name(name: &str) -> Result<(), BackendError> {
    if name.len() > 54
        || name.is_empty()
        || name.starts_with('-')
        || name.ends_with('-')
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err(BackendError::InvalidSpec(
            "attempt name is not a Kubernetes DNS label with room for helpers".to_string(),
        ));
    }
    Ok(())
}

fn validate_job(job: &Job, context: &FenceContext) -> Result<(), BackendError> {
    validate_epoch(job, context)?;
    let generation = job_generation(job)?;
    if generation > context.controller_generation {
        return Err(BackendError::Fenced);
    }
    if generation < context.controller_generation {
        return Err(BackendError::Conflict(
            "Job generation has not been fenced".to_string(),
        ));
    }
    Ok(())
}

fn validate_epoch(job: &Job, context: &FenceContext) -> Result<(), BackendError> {
    if job_epoch(job)? != context.attempt_epoch {
        return Err(BackendError::Conflict(
            "Job attempt epoch mismatch".to_string(),
        ));
    }
    Ok(())
}

fn validate_marker(
    marker: &ConfigMap,
    job: &Job,
    context: &FenceContext,
) -> Result<StageMarker, BackendError> {
    let value = marker
        .data
        .as_ref()
        .and_then(|data| data.get("marker"))
        .ok_or_else(|| BackendError::Conflict("stage marker data is missing".to_string()))?;
    let marker: StageMarker = serde_json::from_str(value)
        .map_err(|error| BackendError::Conflict(format!("invalid stage marker: {error}")))?;
    let digest = job
        .metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get("aruna-engine.org/layout-digest"))
        .ok_or_else(|| BackendError::Conflict("Job layout digest is missing".to_string()))?;
    if marker.job_uid != job_uid(job)?
        || marker.attempt_epoch != context.attempt_epoch
        || marker.controller_generation != context.controller_generation
        || &marker.layout_digest != digest
    {
        return Err(BackendError::Conflict(
            "stage marker does not match the fenced Job".to_string(),
        ));
    }
    Ok(marker)
}

/// A path is valid when it was declared literally or matches a declared pattern.
fn validate_output(job: &Job, path: &str) -> Result<(), BackendError> {
    normalize_container_path(path).map_err(BackendError::InvalidSpec)?;
    let outputs = job
        .metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get("aruna-engine.org/output-paths"))
        .ok_or_else(|| BackendError::Conflict("Job output declarations are missing".to_string()))?;
    let outputs: Vec<String> = serde_json::from_str(outputs)
        .map_err(|error| BackendError::Conflict(format!("invalid Job outputs: {error}")))?;
    let declared = OutputMatcher::new(outputs.iter().map(String::as_str))
        .map_err(BackendError::InvalidSpec)?;
    if !declared.is_match(path) {
        return Err(BackendError::InvalidSpec(
            "output path was not declared".to_string(),
        ));
    }
    Ok(())
}

fn job_status(job: &Job) -> AttemptStatus {
    let phase = if job_state(job) == Some("cancelled") {
        AttemptPhase::Cancelled
    } else if job
        .status
        .as_ref()
        .and_then(|status| status.conditions.as_ref())
        .is_some_and(|conditions| {
            conditions
                .iter()
                .any(|condition| condition.type_ == "Complete" && condition.status == "True")
        })
    {
        AttemptPhase::Exited { code: 0 }
    } else if let Some(condition) = job
        .status
        .as_ref()
        .and_then(|status| status.conditions.as_ref())
        .and_then(|conditions| {
            conditions
                .iter()
                .find(|condition| condition.type_ == "Failed" && condition.status == "True")
        })
    {
        // A Job-level failure with no terminated container carries no evidence
        // about the payload itself; a terminated one overrides this phase.
        AttemptPhase::SystemError {
            reason: condition
                .message
                .clone()
                .or_else(|| condition.reason.clone())
                .unwrap_or_else(|| "Kubernetes Job failed".to_string()),
        }
    } else if job
        .status
        .as_ref()
        .and_then(|status| status.active)
        .unwrap_or(0)
        > 0
    {
        AttemptPhase::Running
    } else {
        AttemptPhase::Submitted
    };
    let times = job.status.as_ref();
    AttemptStatus {
        phase,
        backend_ref: job.metadata.uid.clone().unwrap_or_else(|| job.name_any()),
        started_at_ms: time_ms(times.and_then(|status| status.start_time.as_ref())),
        finished_at_ms: time_ms(times.and_then(|status| status.completion_time.as_ref())),
    }
}

fn time_ms(time: Option<&Time>) -> Option<u64> {
    u64::try_from(time?.0.as_millisecond()).ok()
}

/// Active but unready Pods are the only ones that can hold a stuck container.
fn job_pending(job: &Job) -> bool {
    job.status
        .as_ref()
        .is_some_and(|status| status.active.unwrap_or(0) > 0 && status.ready.unwrap_or(0) == 0)
}

/// Reasons the kubelet reports only after repeated failed attempts. A bare
/// `ErrImagePull` is a single try, so it never proves a stuck container.
fn repeated_wait(reason: &str) -> bool {
    matches!(
        reason,
        "ImagePullBackOff" | "CreateContainerConfigError" | "CreateContainerError"
    )
}

/// Kubernetes retries a failing image pull forever, so the Job counts such a
/// Pod as active and never reports a terminal condition. A malformed reference
/// fails at once; a repeated failure fails once it outlives `deadline`, which
/// is anchored at the Pod start and so also covers scheduling.
fn pod_stuck_reason(pod: &Pod, deadline: Duration, now: Timestamp) -> Option<String> {
    let status = pod.status.as_ref()?;
    let waited = status
        .start_time
        .as_ref()
        .or(pod.metadata.creation_timestamp.as_ref())
        .map(|since| Duration::try_from(now.duration_since(since.0)).unwrap_or_default())
        .unwrap_or_default();
    status
        .init_container_statuses
        .iter()
        .chain(status.container_statuses.iter())
        .flatten()
        .find_map(|container| {
            let waiting = container.state.as_ref()?.waiting.as_ref()?;
            let reason = waiting.reason.as_deref()?;
            if reason != "InvalidImageName" && !(repeated_wait(reason) && waited > deadline) {
                return None;
            }
            let detail = waiting.message.as_deref().unwrap_or("no detail reported");
            Some(format!(
                "container `{}` cannot start ({reason}): {detail}",
                container.name
            ))
        })
}

/// Log reads only succeed once the task container has left its waiting state.
fn task_started(pod: &Pod) -> bool {
    task_state(pod).is_some_and(|state| state.running.is_some() || state.terminated.is_some())
}

fn task_state(pod: &Pod) -> Option<&ContainerState> {
    pod.status
        .as_ref()?
        .container_statuses
        .as_ref()?
        .iter()
        .find(|status| status.name == "task")?
        .state
        .as_ref()
}

/// The container start survives its exit, so both states carry the evidence.
fn pod_started(pod: &Pod) -> Option<u64> {
    let state = task_state(pod)?;
    match (state.running.as_ref(), state.terminated.as_ref()) {
        (Some(running), _) => time_ms(running.started_at.as_ref()),
        (None, Some(terminated)) => time_ms(terminated.started_at.as_ref()),
        (None, None) => None,
    }
}

fn job_epoch(job: &Job) -> Result<u64, BackendError> {
    annotation_u64(job, EPOCH_ANNOTATION)
}

fn job_generation(job: &Job) -> Result<u64, BackendError> {
    annotation_u64(job, GENERATION_ANNOTATION)
}

fn annotation_u64(job: &Job, key: &str) -> Result<u64, BackendError> {
    job.metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get(key))
        .ok_or_else(|| BackendError::Conflict(format!("Job annotation `{key}` is missing")))?
        .parse()
        .map_err(|error| BackendError::Conflict(format!("invalid Job annotation `{key}`: {error}")))
}

fn job_uid(job: &Job) -> Result<String, BackendError> {
    job.metadata
        .uid
        .clone()
        .ok_or_else(|| BackendError::Api("Job has no UID".to_string()))
}

fn job_state(job: &Job) -> Option<&str> {
    job.metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get(STATE_ANNOTATION))
        .map(String::as_str)
}

fn job_workspace(job: &Job) -> bool {
    job.metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get("aruna-engine.org/workspace"))
        .is_some_and(|value| value == "true")
}

fn cas_patch(job: &Job, replacements: Vec<(String, Value)>) -> Result<JsonPatch, BackendError> {
    let uid = job_uid(job)?;
    let version = job
        .metadata
        .resource_version
        .clone()
        .ok_or_else(|| BackendError::Api("Job has no resourceVersion".to_string()))?;
    let mut operations = vec![
        json!({"op":"test","path":"/metadata/uid","value":uid}),
        json!({"op":"test","path":"/metadata/resourceVersion","value":version}),
    ];
    operations.extend(
        replacements
            .into_iter()
            .map(|(path, value)| json!({"op":"replace","path":path,"value":value})),
    );
    serde_json::from_value(Value::Array(operations))
        .map_err(|error| BackendError::Api(format!("build Kubernetes CAS patch: {error}")))
}

fn escape_pointer(value: &str) -> String {
    value.replace('~', "~0").replace('/', "~1")
}

fn attempt_selector(context: &FenceContext) -> String {
    format!(
        "aruna-engine.org/job-id={},aruna-engine.org/attempt={}",
        context.attempt.job_id, context.attempt.attempt
    )
}

fn logs_name(name: &str) -> String {
    format!("{name}-logs")
}

fn required_access(
    s3_mount: bool,
) -> Vec<(
    &'static str,
    &'static str,
    Option<&'static str>,
    &'static str,
)> {
    let mut access = vec![
        ("batch", "jobs", None, "create"),
        ("batch", "jobs", None, "get"),
        ("batch", "jobs", None, "list"),
        ("batch", "jobs", None, "watch"),
        ("batch", "jobs", None, "patch"),
        ("batch", "jobs", None, "delete"),
        ("", "pods", None, "create"),
        ("", "pods", None, "get"),
        ("", "pods", None, "list"),
        ("", "pods", None, "watch"),
        ("", "pods", None, "delete"),
        ("", "pods", Some("exec"), "create"),
        ("", "pods", Some("exec"), "get"),
        ("", "pods", Some("log"), "get"),
        ("", "persistentvolumes", None, "create"),
        ("", "persistentvolumes", None, "get"),
        ("", "persistentvolumes", None, "list"),
        ("", "persistentvolumes", None, "delete"),
        ("", "persistentvolumeclaims", None, "create"),
        ("", "persistentvolumeclaims", None, "get"),
        ("", "persistentvolumeclaims", None, "list"),
        ("", "persistentvolumeclaims", None, "watch"),
        ("", "persistentvolumeclaims", None, "delete"),
        ("", "secrets", None, "create"),
        ("", "secrets", None, "get"),
        ("", "secrets", None, "delete"),
        ("", "configmaps", None, "create"),
        ("", "configmaps", None, "get"),
        ("", "configmaps", None, "patch"),
        ("", "configmaps", None, "delete"),
        ("", "serviceaccounts", None, "get"),
        ("networking.k8s.io", "networkpolicies", None, "create"),
        ("networking.k8s.io", "networkpolicies", None, "get"),
        ("networking.k8s.io", "networkpolicies", None, "patch"),
        ("storage.k8s.io", "storageclasses", None, "get"),
    ];
    if s3_mount {
        access.push(("storage.k8s.io", "csidrivers", None, "get"));
    }
    access
}

async fn check_access(
    client: Client,
    namespace: &str,
    group: &str,
    resource: &str,
    subresource: Option<&str>,
    verb: &str,
) -> Result<(), BackendError> {
    let reviews: Api<SelfSubjectAccessReview> = Api::all(client);
    let review: SelfSubjectAccessReview = serde_json::from_value(json!({
        "apiVersion":"authorization.k8s.io/v1",
        "kind":"SelfSubjectAccessReview",
        "spec":{"resourceAttributes":{
            "namespace":if matches!(resource,"storageclasses" | "csidrivers" | "persistentvolumes") { None } else { Some(namespace) },
            "group":group,
            "resource":resource,
            "subresource":subresource,
            "verb":verb,
            "name":if matches!(resource,"storageclasses" | "csidrivers") { Some("") } else { None }
        }}
    }))
    .map_err(|error| BackendError::Api(format!("build access review: {error}")))?;
    let result = reviews
        .create(&PostParams::default(), &review)
        .await
        .map_err(kube_error)?;
    if result.status.is_some_and(|status| status.allowed) {
        Ok(())
    } else {
        Err(BackendError::Unavailable(format!(
            "Kubernetes access denied for {verb} {group}/{resource}"
        )))
    }
}

async fn delete_named<K>(api: Api<K>, name: &str) -> Result<(), BackendError>
where
    K: Clone + Debug + serde::de::DeserializeOwned + kube::Resource<DynamicType = ()> + Send + Sync,
{
    match api.delete(name, &DeleteParams::default()).await {
        Ok(_) => Ok(()),
        Err(error) if api_code(&error) == Some(404) => Ok(()),
        Err(error) => Err(kube_error(error)),
    }
}

fn build_archive(
    tx: mpsc::Sender<Result<Bytes, BackendError>>,
    plan: StagePlan,
) -> Result<(), BackendError> {
    let total = plan
        .entries
        .iter()
        .try_fold(0u64, |total, entry| total.checked_add(entry.size))
        .ok_or_else(|| BackendError::InvalidSpec("staged input size overflows".to_string()))?;
    if total > MAX_TRANSFER_BYTES {
        return Err(BackendError::InvalidSpec(
            "staged inputs exceed the transfer limit".to_string(),
        ));
    }
    let writer = ArchiveWriter { tx };
    let mut builder = tar::Builder::new(writer);
    for output in &plan.layout.output_parents {
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Directory);
        header.set_mode(0o770);
        header.set_size(0);
        header.set_cksum();
        builder
            .append_data(
                &mut header,
                output.strip_prefix("/").unwrap_or(output),
                io::empty(),
            )
            .map_err(io_error)?;
    }
    let handle = Handle::current();
    for entry in plan.entries {
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Regular);
        header.set_mode(0o440);
        header.set_size(entry.size);
        header.set_cksum();
        let stream = entry
            .stream
            .map(|chunk| chunk.map_err(|error| io::Error::other(error.to_string())));
        let reader = SyncIoBridge::new_with_handle(StreamReader::new(stream), handle.clone());
        let mut reader = ExactReader {
            inner: reader,
            remaining: entry.size,
        };
        builder
            .append_data(
                &mut header,
                entry.path.strip_prefix("/").unwrap_or(&entry.path),
                &mut reader,
            )
            .map_err(io_error)?;
        if reader.remaining != 0 {
            return Err(BackendError::InvalidSpec(format!(
                "input `{}` ended before its declared size",
                entry.path.display()
            )));
        }
        let mut extra = [0u8; 1];
        if reader.inner.read(&mut extra).map_err(io_error)? != 0 {
            return Err(BackendError::InvalidSpec(format!(
                "input `{}` exceeds its declared size",
                entry.path.display()
            )));
        }
    }
    builder.finish().map_err(io_error)
}

struct ExactReader<R> {
    inner: R,
    remaining: u64,
}

impl<R: Read> Read for ExactReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.remaining == 0 {
            return Ok(0);
        }
        let limit = usize::try_from(self.remaining)
            .unwrap_or(usize::MAX)
            .min(buffer.len());
        let read = self.inner.read(&mut buffer[..limit])?;
        self.remaining = self.remaining.saturating_sub(read as u64);
        Ok(read)
    }
}

struct ArchiveWriter {
    tx: mpsc::Sender<Result<Bytes, BackendError>>,
}

impl Write for ArchiveWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.tx
            .blocking_send(Ok(Bytes::copy_from_slice(buffer)))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "stage receiver closed"))?;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Split a helper listing into its path records, rejecting a stream whose
/// terminator is missing or whose count disagrees: a short read must fail the
/// capture instead of silently dropping matches.
fn listed_paths(listing: &[u8]) -> Result<Vec<&[u8]>, BackendError> {
    let truncated = || BackendError::InvalidSpec("output listing is truncated".to_string());
    let body = listing.strip_suffix(&[0]).ok_or_else(truncated)?;
    let mut records: Vec<&[u8]> = body.split(|byte| *byte == 0).collect();
    let count = records.pop().ok_or_else(truncated)?;
    if records.pop() != Some(&b""[..]) {
        return Err(truncated());
    }
    let count = std::str::from_utf8(count)
        .ok()
        .and_then(|count| count.parse::<usize>().ok())
        .ok_or_else(truncated)?;
    if count != records.len() || records.iter().any(|path| path.is_empty()) {
        return Err(BackendError::InvalidSpec(
            "output listing is inconsistent".to_string(),
        ));
    }
    Ok(records)
}

/// Listed paths the pattern selects. A path that is not UTF-8 cannot be
/// addressed as an output, so it is skipped instead of failing the capture.
fn select_listed(listing: &[u8], glob: &OutputMatcher) -> Result<Vec<String>, BackendError> {
    let mut matched = Vec::new();
    for path in listed_paths(listing)? {
        let Ok(path) = std::str::from_utf8(path) else {
            tracing::warn!(
                path = %String::from_utf8_lossy(path),
                "skipping output path that is not UTF-8"
            );
            continue;
        };
        if !glob.is_match(path) {
            continue;
        }
        if matched.len() >= MAX_OUTPUT_MATCHES {
            return Err(BackendError::InvalidSpec(
                "pattern matches too many files".to_string(),
            ));
        }
        matched.push(path.to_string());
    }
    matched.sort();
    Ok(matched)
}

/// Read the leading tar header the fetch helper emits, returning the unpolled `join`
/// beside the declared body size. A torn read reports the exec status, which holds
/// its real cause; an intact but invalid header keeps its own error and drops `join`.
async fn read_header<R, F>(stdout: &mut R, path: &str, join: F) -> Result<(u64, F), BackendError>
where
    R: AsyncRead + Unpin,
    F: Future<Output = Result<(), BackendError>>,
{
    let mut header_bytes = [0u8; 512];
    if let Err(error) = stdout.read_exact(&mut header_bytes).await {
        return Err(joined_error(join, io_error(error)).await);
    }
    Ok((parse_header(&header_bytes, path)?, join))
}

/// Check the header describes the requested file within the transfer bound.
fn parse_header(header_bytes: &[u8; 512], path: &str) -> Result<u64, BackendError> {
    let header = tar::Header::from_byte_slice(header_bytes);
    let size = header
        .size()
        .map_err(|error| BackendError::Api(format!("invalid fetch archive size: {error}")))?;
    if size > MAX_TRANSFER_BYTES || !header.entry_type().is_file() {
        return Err(BackendError::InvalidSpec(
            "fetch archive is not a bounded regular file".to_string(),
        ));
    }
    let archive_path = header
        .path()
        .map_err(|error| BackendError::Api(format!("invalid fetch archive path: {error}")))?;
    let expected = normalize_container_path(path).map_err(BackendError::InvalidSpec)?;
    if archive_path.as_ref() != expected.strip_prefix("/").unwrap_or(&expected) {
        return Err(BackendError::Conflict(
            "fetch helper returned a different output path".to_string(),
        ));
    }
    Ok(size)
}

async fn stream_output<R: AsyncRead + Unpin>(
    mut stdout: R,
    join: impl Future<Output = Result<(), BackendError>>,
    size: u64,
    tx: mpsc::Sender<Result<Bytes, BackendError>>,
) {
    let mut remaining = size;
    let mut buffer = vec![0u8; 64 * 1024];
    while remaining > 0 {
        let limit = usize::try_from(remaining)
            .unwrap_or(usize::MAX)
            .min(buffer.len());
        match stdout.read(&mut buffer[..limit]).await {
            Ok(0) => {
                let transferred = size - remaining;
                let short = BackendError::Api(format!(
                    "fetch archive ended after {transferred} of {size} bytes"
                ));
                let _ = tx.send(Err(joined_error(join, short).await)).await;
                return;
            }
            Ok(read) => {
                remaining -= read as u64;
                if tx
                    .send(Ok(Bytes::copy_from_slice(&buffer[..read])))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            Err(error) => {
                let _ = tx
                    .send(Err(joined_error(join, io_error(error)).await))
                    .await;
                return;
            }
        }
    }
    let _ = tokio::io::copy(&mut stdout, &mut tokio::io::sink()).await;
    if let Err(error) = join.await {
        let _ = tx.send(Err(error)).await;
    }
}

/// kube-rs reports a websocket failure or an unexpected close as a plain EOF or IO
/// error, so the real cause survives only in the exec status. `fallback` is reported
/// when the status itself is clean.
async fn joined_error(
    join: impl Future<Output = Result<(), BackendError>>,
    fallback: BackendError,
) -> BackendError {
    join.await.err().unwrap_or(fallback)
}

async fn join_exec(mut attached: kube::api::AttachedProcess) -> Result<(), BackendError> {
    let status = attached.take_status().ok_or_else(|| {
        BackendError::Api("Kubernetes exec status stream is unavailable".to_string())
    })?;
    let joined = async {
        attached.join().await.map_err(remote_error)?;
        status
            .await
            .ok_or_else(|| BackendError::Api("Kubernetes exec returned no status".to_string()))
    };
    // A helper that stops writing must not hold the capture open forever.
    let status = tokio::time::timeout(EXEC_JOIN_TIMEOUT, joined)
        .await
        .map_err(|_| BackendError::Timeout("Kubernetes exec did not finish".to_string()))??;
    if status.status.as_deref() == Some("Success") {
        return Ok(());
    }
    Err(BackendError::Api(
        status
            .message
            .or(status.reason)
            .unwrap_or_else(|| "Kubernetes exec failed".to_string()),
    ))
}

struct ChannelStream(mpsc::Receiver<Result<Bytes, BackendError>>);

impl Stream for ChannelStream {
    type Item = Result<Bytes, BackendError>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(context)
    }
}

fn api_code(error: &kube::Error) -> Option<u16> {
    match error {
        kube::Error::Api(response) => Some(response.code),
        _ => None,
    }
}

fn kube_error(error: kube::Error) -> BackendError {
    match &error {
        kube::Error::Api(response) if response.code == 404 => {
            BackendError::NotFound(response.message.clone())
        }
        kube::Error::Api(response) if response.code == 409 || response.code == 422 => {
            BackendError::Conflict(response.message.clone())
        }
        kube::Error::Api(response) if response.code == 401 || response.code == 403 => {
            BackendError::Unavailable(response.message.clone())
        }
        _ => BackendError::Api(format!("Kubernetes API: {error}")),
    }
}

fn io_error(error: io::Error) -> BackendError {
    BackendError::Api(format!("Kubernetes stream: {error}"))
}

fn remote_error(error: impl std::fmt::Display) -> BackendError {
    BackendError::Api(format!("Kubernetes exec: {error}"))
}

#[cfg(test)]
mod tests {
    use aruna_core::compute::AttemptRef;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    use super::*;
    use crate::executor::logs::NullSink;

    fn context() -> FenceContext {
        FenceContext {
            attempt: AttemptRef::new("job", 1),
            attempt_epoch: 7,
            controller_generation: 3,
        }
    }

    fn test_config() -> KubernetesConfig {
        KubernetesConfig {
            namespace: "compute".to_string(),
            storage_class: "csi".to_string(),
            helper_image: "helper:latest".to_string(),
            pull_deadline: Duration::from_secs(30),
            s3_cidrs: Vec::new(),
            s3_port: 443,
            s3_mount_driver: None,
            ..Default::default()
        }
    }

    fn fake_client<F>(handler: F) -> Client
    where
        F: Fn(&str, &str) -> (u16, Value) + Send + Sync + 'static,
    {
        let handler = std::sync::Arc::new(handler);
        let service = tower::service_fn(move |request: http::Request<kube::client::Body>| {
            let handler = handler.clone();
            async move {
                let (status, body) = handler(request.method().as_str(), request.uri().path());
                Ok::<_, std::convert::Infallible>(
                    http::Response::builder()
                        .status(status)
                        .header("content-type", "application/json")
                        .body(kube::client::Body::from(
                            serde_json::to_vec(&body).expect("serialize fake response"),
                        ))
                        .expect("build fake response"),
                )
            }
        });
        Client::new(service, "compute")
    }

    fn status_json(code: u16) -> Value {
        let reason = if code == 404 {
            "NotFound"
        } else {
            "BadRequest"
        };
        json!({
            "kind": "Status", "apiVersion": "v1", "status": "Failure",
            "message": "fake error", "reason": reason, "code": code
        })
    }

    fn job_json(state: &str) -> Value {
        json!({
            "apiVersion": "batch/v1", "kind": "Job",
            "metadata": {
                "name": "aruna-job-a1", "namespace": "compute",
                "uid": "job-uid", "resourceVersion": "1",
                "annotations": {
                    EPOCH_ANNOTATION: "7",
                    GENERATION_ANNOTATION: "3",
                    STATE_ANNOTATION: state,
                }
            },
            "spec": {"suspend": false},
            "status": {"active": 1}
        })
    }

    fn core_object(kind: &str, name: &str) -> Value {
        json!({
            "apiVersion": "v1", "kind": kind,
            "metadata": {"name": name, "namespace": "compute", "uid": "acc-uid"}
        })
    }

    fn pod_list() -> Value {
        json!({
            "apiVersion": "v1", "kind": "PodList", "metadata": {},
            "items": [{
                "apiVersion": "v1", "kind": "Pod",
                "metadata": {
                    "name": "task-pod", "namespace": "compute", "uid": "pod-uid",
                    "labels": {ROLE_LABEL: "task"}
                }
            }]
        })
    }

    #[test]
    fn accepts_pattern_output() {
        // A declared wildcard output authorizes every path it selects.
        let job: Job = serde_json::from_value(json!({
            "apiVersion": "batch/v1", "kind": "Job",
            "metadata": {
                "name": "aruna-job-a1", "namespace": "compute",
                "annotations": {"aruna-engine.org/output-paths": r#"["/out/*.txt","/log/run.txt"]"#}
            }
        }))
        .expect("build job");

        assert!(validate_output(&job, "/out/a.txt").is_ok());
        assert!(validate_output(&job, "/out/*.txt").is_ok());
        assert!(validate_output(&job, "/log/run.txt").is_ok());
        assert!(validate_output(&job, "/out/sub/a.txt").is_err());
        assert!(validate_output(&job, "/out/a.csv").is_err());
    }

    fn listing(paths: &[&[u8]]) -> Vec<u8> {
        let mut listing = Vec::new();
        for path in paths {
            listing.extend_from_slice(path);
            listing.push(0);
        }
        listing.push(0);
        listing.extend_from_slice(paths.len().to_string().as_bytes());
        listing.push(0);
        listing
    }

    #[test]
    fn rejects_cut_listing() {
        // Without the terminator a capped read would upload a partial listing
        // while the job still reported success.
        let glob = OutputMatcher::new(["/out/*.txt"]).expect("build matcher");
        let full = listing(&[b"/out/a.txt", b"/out/b.txt"]);
        assert_eq!(
            select_listed(&full, &glob).expect("accept full listing"),
            vec!["/out/a.txt".to_string(), "/out/b.txt".to_string()]
        );

        for cut in [full.len() - 1, full.len() - 4, 11, 0] {
            assert!(
                matches!(
                    select_listed(&full[..cut], &glob),
                    Err(BackendError::InvalidSpec(_))
                ),
                "listing cut at {cut} was accepted"
            );
        }
        // A terminator that disagrees with the records is equally unusable.
        let mut miscounted = listing(&[b"/out/a.txt"]);
        miscounted.pop();
        miscounted.pop();
        miscounted.extend_from_slice(b"2\0");
        assert!(matches!(
            select_listed(&miscounted, &glob),
            Err(BackendError::InvalidSpec(_))
        ));
        assert!(
            select_listed(&listing(&[]), &glob)
                .expect("accept empty listing")
                .is_empty()
        );
    }

    #[test]
    fn skips_invalid_listing() {
        // A file whose name is not UTF-8 cannot be addressed, but it must not
        // fail the capture of the files that can be.
        let glob = OutputMatcher::new(["/out/*.txt"]).expect("build matcher");
        let listing = listing(&[b"/out/a.txt", b"/out/\xff\xfe.txt", b"/out/b.txt"]);

        assert_eq!(
            select_listed(&listing, &glob).expect("accept mixed listing"),
            vec!["/out/a.txt".to_string(), "/out/b.txt".to_string()]
        );
    }

    #[tokio::test]
    async fn tombstones_absent_job() {
        // Cancel between ready and submit leaves no Job; terminal cleanup
        // must still converge instead of retrying forever.
        let backend = KubernetesBackend {
            client: fake_client(|_, _| (404, status_json(404))),
            config: test_config(),
        };

        let evidence = backend
            .tombstone(&context(), &TombstoneSpec { terminal_ref: None })
            .await
            .unwrap();

        assert_eq!(evidence.backend_ref, "aruna-job-a1");
        assert_eq!(evidence.attempt_epoch, 7);
    }

    #[tokio::test]
    async fn deletes_job_last() {
        // Deleting the Job before the accessories would make a cleanup retry
        // observe NotFound, discharging the obligation and leaking the PVC.
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorder = seen.clone();
        let client = fake_client(move |method, path| {
            recorder
                .lock()
                .expect("record request")
                .push(format!("{method} {path}"));
            match (method, path) {
                ("GET", "/apis/batch/v1/namespaces/compute/jobs/aruna-job-a1") => {
                    (200, job_json("tombstone"))
                }
                ("GET", "/api/v1/namespaces/compute/pods") => (200, pod_list()),
                ("GET", "/api/v1/namespaces/compute/persistentvolumeclaims") => (
                    200,
                    json!({"apiVersion":"v1","kind":"PersistentVolumeClaimList","items":[]}),
                ),
                ("GET", "/api/v1/persistentvolumes") => (
                    200,
                    json!({"apiVersion":"v1","kind":"PersistentVolumeList","items":[]}),
                ),
                ("DELETE", "/api/v1/namespaces/compute/pods/task-pod") => {
                    (200, core_object("Pod", "task-pod"))
                }
                ("DELETE", "/api/v1/namespaces/compute/persistentvolumeclaims/aruna-job-a1-ws") => {
                    (200, core_object("PersistentVolumeClaim", "aruna-job-a1-ws"))
                }
                ("DELETE", "/api/v1/namespaces/compute/secrets/aruna-job-a1-env") => {
                    (200, core_object("Secret", "aruna-job-a1-env"))
                }
                ("DELETE", "/api/v1/namespaces/compute/configmaps/aruna-job-a1-logs")
                | ("DELETE", "/api/v1/namespaces/compute/configmaps/aruna-job-a1-staged") => {
                    (200, core_object("ConfigMap", "marker"))
                }
                ("DELETE", "/apis/batch/v1/namespaces/compute/jobs/aruna-job-a1") => {
                    (200, job_json("tombstone"))
                }
                _ => (404, status_json(404)),
            }
        });
        let backend = KubernetesBackend {
            client,
            config: test_config(),
        };

        backend.cleanup(&context()).await.unwrap();

        let seen = seen.lock().expect("read requests").clone();
        for accessory in [
            "DELETE /api/v1/namespaces/compute/persistentvolumeclaims/aruna-job-a1-ws",
            "DELETE /api/v1/namespaces/compute/secrets/aruna-job-a1-env",
            "DELETE /api/v1/namespaces/compute/configmaps/aruna-job-a1-logs",
            "DELETE /api/v1/namespaces/compute/configmaps/aruna-job-a1-staged",
        ] {
            assert!(seen.iter().any(|entry| entry == accessory), "{accessory}");
        }
        assert_eq!(
            seen.last().map(String::as_str),
            Some("DELETE /apis/batch/v1/namespaces/compute/jobs/aruna-job-a1")
        );
    }

    #[tokio::test]
    async fn cancels_without_logs() {
        // A stuck pod rejects log reads with 400; cancel must still complete.
        let client = fake_client(|method, path| match (method, path) {
            ("GET", "/apis/batch/v1/namespaces/compute/jobs/aruna-job-a1") => {
                (200, job_json("running"))
            }
            ("PATCH", "/apis/batch/v1/namespaces/compute/jobs/aruna-job-a1") => {
                (200, job_json("cancelled"))
            }
            ("GET", "/api/v1/namespaces/compute/pods") => (200, pod_list()),
            ("GET", "/api/v1/namespaces/compute/pods/task-pod/log") => (400, status_json(400)),
            ("DELETE", "/api/v1/namespaces/compute/pods/task-pod") => (
                200,
                json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":"task-pod","uid":"pod-uid"}}),
            ),
            _ => (404, status_json(404)),
        });
        let backend = KubernetesBackend {
            client,
            config: test_config(),
        };

        let evidence = backend.cancel(&context()).await.unwrap();

        match evidence {
            CancelEvidence::Stopped(status) => assert_eq!(status.phase, AttemptPhase::Cancelled),
            other => panic!("unexpected cancel evidence: {other:?}"),
        }
    }

    #[test]
    fn delete_is_graceful() {
        let params = DeleteParams {
            grace_period_seconds: Some(30),
            preconditions: Some(Preconditions {
                uid: Some("uid".to_string()),
                resource_version: None,
            }),
            ..DeleteParams::default()
        };
        assert_ne!(params.grace_period_seconds, Some(0));
        assert_eq!(params.preconditions.unwrap().uid.as_deref(), Some("uid"));
    }

    #[test]
    fn patch_tests_uid() {
        let job = Job {
            metadata: ObjectMeta {
                uid: Some("uid".to_string()),
                resource_version: Some("9".to_string()),
                ..ObjectMeta::default()
            },
            ..Job::default()
        };
        let patch = cas_patch(&job, vec![("/spec/suspend".to_string(), json!(false))]).unwrap();
        let value = serde_json::to_value(patch).unwrap();
        assert_eq!(value[0]["op"], "test");
        assert_eq!(value[0]["path"], "/metadata/uid");
    }

    #[test]
    fn validates_helper_digest() {
        let config = KubernetesConfig {
            namespace: "compute".to_string(),
            storage_class: "csi".to_string(),
            helper_image: "helper:latest".to_string(),
            pull_deadline: Duration::from_secs(30),
            s3_cidrs: Vec::new(),
            s3_port: 443,
            s3_mount_driver: None,
            ..Default::default()
        };
        assert!(validate_config(&config).is_err());
    }

    #[tokio::test]
    async fn accepts_task_images() {
        let backend = KubernetesBackend {
            client: fake_client(|_, _| (404, status_json(404))),
            config: test_config(),
        };
        let cancel = CancellationToken::new();

        for image in [
            "registry.example/task:latest",
            "registry.example/task@sha256:0000000000000000000000000000000000000000000000000000000000000000",
        ] {
            let resolved = backend.resolve_image(image, &cancel).await.unwrap();
            let context = context();
            let spec = TaskSpec::new(context.attempt.clone(), resolved.clone());

            assert_eq!(resolved, image);
            assert!(validate_spec(&context, &spec, &backend.config).is_ok());
        }
    }

    #[test]
    fn selector_is_stable() {
        assert_eq!(
            attempt_selector(&context()),
            "aruna-engine.org/job-id=job,aruna-engine.org/attempt=1"
        );
    }

    #[test]
    fn rejects_disabled_mount() {
        // A disabled backend must refuse S3Mount specs before submission.
        let context = context();
        let mut spec = TaskSpec::new(context.attempt.clone(), "registry.example/task:latest");
        spec.staging_mode = StagingMode::S3Mount;
        assert!(matches!(
            validate_spec(&context, &spec, &test_config()),
            Err(BackendError::InvalidSpec(_))
        ));
    }

    #[tokio::test]
    async fn gates_driver_check() {
        // Health probes the CSIDriver only when the S3-mount driver is set.
        fn probe_client(seen: std::sync::Arc<std::sync::Mutex<Vec<String>>>) -> Client {
            fake_client(move |method, path| {
                seen.lock().expect("record request").push(path.to_string());
                if method == "POST" {
                    return (
                        200,
                        json!({
                            "apiVersion":"authorization.k8s.io/v1",
                            "kind":"SelfSubjectAccessReview",
                            "status":{"allowed":true}
                        }),
                    );
                }
                if path.contains("storageclasses") {
                    return (
                        200,
                        json!({
                            "apiVersion":"storage.k8s.io/v1","kind":"StorageClass",
                            "metadata":{"name":"csi"},"provisioner":"driver"
                        }),
                    );
                }
                if path.contains("csidrivers") {
                    return (
                        200,
                        json!({
                            "apiVersion":"storage.k8s.io/v1","kind":"CSIDriver",
                            "metadata":{"name":"driver"},"spec":{}
                        }),
                    );
                }
                if path.contains("serviceaccounts") {
                    return (
                        200,
                        json!({
                            "apiVersion":"v1","kind":"ServiceAccount",
                            "metadata":{"name":crate::executor::config::DEFAULT_WORKLOAD_SA}
                        }),
                    );
                }
                (
                    200,
                    json!({
                        "apiVersion":"v1","kind":"Namespace","metadata":{"name":"compute"}
                    }),
                )
            })
        }

        let disabled = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let backend = KubernetesBackend {
            client: probe_client(disabled.clone()),
            config: test_config(),
        };
        backend.health().await.unwrap();
        assert!(
            !disabled
                .lock()
                .expect("read requests")
                .iter()
                .any(|path| path.contains("csidrivers"))
        );

        let enabled = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut config = test_config();
        config.s3_mount_driver = Some("s3.csi.scality.com".to_string());
        let backend = KubernetesBackend {
            client: probe_client(enabled.clone()),
            config,
        };
        backend.health().await.unwrap();
        assert!(
            enabled
                .lock()
                .expect("read requests")
                .iter()
                .any(|path| path.contains("csidrivers"))
        );
    }

    const POD_START: &str = "2027-01-01T00:00:00Z";

    fn probe_now(after: i64) -> Timestamp {
        let start: Timestamp = POD_START.parse().expect("parse pod start");
        Timestamp::from_second(start.as_second() + after).expect("build probe time")
    }

    fn task_pod(state: Value, started: &str) -> Pod {
        serde_json::from_value(json!({
            "apiVersion":"v1","kind":"Pod",
            "metadata":{
                "name":"task-pod","namespace":"compute","uid":"pod-uid",
                "labels":{ROLE_LABEL:"task"}
            },
            "status":{
                "phase":"Pending","startTime":started,
                "containerStatuses":[{
                    "name":"task","image":"registry.example/task:1","imageID":"",
                    "ready":false,"restartCount":0,"state":state
                }]
            }
        }))
        .expect("build task pod")
    }

    fn waiting_pod(reason: &str, started: &str) -> Pod {
        task_pod(
            json!({"waiting":{"reason":reason,"message":"pull access failed"}}),
            started,
        )
    }

    #[test]
    fn fails_invalid_image() {
        // A malformed reference is permanent, so the deadline must not apply.
        let pod = waiting_pod("InvalidImageName", POD_START);
        let reason = pod_stuck_reason(&pod, Duration::from_secs(300), probe_now(1))
            .expect("reject invalid image");
        assert!(reason.contains("InvalidImageName"), "{reason}");
    }

    #[test]
    fn waits_pull_backoff() {
        // A registry blip inside the deadline must stay Running.
        let pod = waiting_pod("ImagePullBackOff", POD_START);
        assert_eq!(
            pod_stuck_reason(&pod, Duration::from_secs(30), probe_now(5)),
            None
        );
    }

    #[test]
    fn fails_stalled_pull() {
        // Only a reason implying repeated failures may end the attempt, and
        // then only past the deadline.
        for reason in [
            "ImagePullBackOff",
            "CreateContainerConfigError",
            "CreateContainerError",
        ] {
            let pod = waiting_pod(reason, POD_START);
            let stuck = pod_stuck_reason(&pod, Duration::from_secs(30), probe_now(120))
                .expect("reject stalled pull");
            assert!(stuck.contains(reason), "{stuck}");
            assert!(stuck.contains("pull access failed"), "{stuck}");
        }
    }

    #[test]
    fn waits_single_pull() {
        // The deadline starts at the Pod, so scheduling and volume attach time
        // count against it; one observed ErrImagePull is not repeated failure.
        let pod = waiting_pod("ErrImagePull", POD_START);
        assert_eq!(
            pod_stuck_reason(&pod, Duration::from_secs(30), probe_now(9_000)),
            None
        );
    }

    #[test]
    fn ignores_running_pod() {
        let pod = task_pod(json!({"running":{"startedAt":POD_START}}), POD_START);
        assert_eq!(
            pod_stuck_reason(&pod, Duration::from_secs(30), probe_now(9_000)),
            None
        );
        assert!(task_started(&pod));
    }

    #[test]
    fn maps_job_times() {
        // Without these the task log has no duration and the walltime cap has
        // no anchor.
        let job: Job = serde_json::from_value(json!({
            "apiVersion":"batch/v1","kind":"Job",
            "metadata":{"name":"aruna-job-a1","namespace":"compute","uid":"job-uid"},
            "status":{
                "startTime":POD_START,
                "completionTime":"2027-01-01T00:01:00Z",
                "conditions":[{"type":"Complete","status":"True"}]
            }
        }))
        .expect("build job");

        let status = job_status(&job);

        assert_eq!(status.phase, AttemptPhase::Exited { code: 0 });
        assert_eq!(
            status.started_at_ms,
            u64::try_from(probe_now(0).as_millisecond()).ok()
        );
        assert_eq!(
            status.finished_at_ms,
            u64::try_from(probe_now(60).as_millisecond()).ok()
        );
    }

    #[tokio::test]
    async fn status_uses_pod_start() {
        // The Job publishes startTime only once its controller processed it, so
        // the running container is the earlier evidence.
        let client = fake_client(|method, path| match (method, path) {
            ("GET", "/apis/batch/v1/namespaces/compute/jobs/aruna-job-a1") => {
                let mut job = job_json("running");
                job["status"] = json!({"active": 1, "ready": 1});
                (200, job)
            }
            ("GET", "/api/v1/namespaces/compute/pods") => {
                let pod = task_pod(json!({"running":{"startedAt":POD_START}}), POD_START);
                (
                    200,
                    json!({
                        "apiVersion":"v1","kind":"PodList","metadata":{},
                        "items":[serde_json::to_value(pod).expect("encode running pod")]
                    }),
                )
            }
            _ => (404, status_json(404)),
        });
        let backend = KubernetesBackend {
            client,
            config: test_config(),
        };

        let status = backend.status(&context()).await.unwrap();

        assert_eq!(status.phase, AttemptPhase::Running);
        assert_eq!(
            status.started_at_ms,
            u64::try_from(probe_now(0).as_millisecond()).ok()
        );
    }

    fn stuck_list() -> Value {
        let pod = waiting_pod("ImagePullBackOff", "2020-01-01T00:00:00Z");
        json!({
            "apiVersion":"v1","kind":"PodList","metadata":{},
            "items":[serde_json::to_value(pod).expect("encode stuck pod")]
        })
    }

    fn stuck_client() -> Client {
        fake_client(|method, path| match (method, path) {
            ("GET", "/apis/batch/v1/namespaces/compute/jobs/aruna-job-a1") => {
                (200, job_json("running"))
            }
            ("GET", "/api/v1/namespaces/compute/pods") => (200, stuck_list()),
            _ => (404, status_json(404)),
        })
    }

    #[tokio::test]
    async fn status_fails_stuck() {
        // The Job stays active forever, so the stuck Pod must drive the phase.
        let backend = KubernetesBackend {
            client: stuck_client(),
            config: test_config(),
        };

        let status = backend.status(&context()).await.unwrap();

        match status.phase {
            AttemptPhase::SystemError { reason } => {
                assert!(reason.contains("ImagePullBackOff"), "{reason}");
            }
            other => panic!("unexpected phase: {other:?}"),
        }
    }

    #[tokio::test]
    async fn skips_unstarted_logs() {
        // Terminal handling parks the job when log capture errors, so a Pod
        // that never started must report empty logs instead of failing.
        let backend = KubernetesBackend {
            client: stuck_client(),
            config: test_config(),
        };

        let tails = backend
            .fetch_logs(&context(), &LogLimits::default(), &NullSink)
            .await
            .unwrap();

        assert!(tails.stdout.is_empty());
        assert_eq!(tails.stdout_total, 0);
    }

    async fn drain_stream(
        mut rx: mpsc::Receiver<Result<Bytes, BackendError>>,
    ) -> Vec<Result<Bytes, BackendError>> {
        let mut items = Vec::new();
        while let Some(item) = rx.recv().await {
            items.push(item);
        }
        items
    }

    // A dead websocket reaches the reader as a plain EOF, so the exec status must win
    // over the truncation the reader sees.
    #[tokio::test]
    async fn truncation_prefers_status() {
        let (mut writer, reader) = tokio::io::duplex(64);
        writer.write_all(b"abc").await.unwrap();
        drop(writer);
        let (tx, rx) = mpsc::channel(4);

        let join = std::future::ready(Err(BackendError::Api("websocket reset".to_string())));
        stream_output(reader, join, 8, tx).await;

        let items = drain_stream(rx).await;
        assert_eq!(items.len(), 2);
        assert!(items[0].is_ok());
        match items[1].as_ref().expect_err("the stream must fail") {
            BackendError::Api(message) => assert_eq!(message, "websocket reset"),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    // A clean exec status leaves the shortfall as the only evidence.
    #[tokio::test]
    async fn truncation_reports_shortfall() {
        let (mut writer, reader) = tokio::io::duplex(64);
        writer.write_all(b"abc").await.unwrap();
        drop(writer);
        let (tx, rx) = mpsc::channel(4);

        stream_output(reader, std::future::ready(Ok(())), 8, tx).await;

        let items = drain_stream(rx).await;
        match items[1].as_ref().expect_err("the stream must fail") {
            BackendError::Api(message) => {
                assert_eq!(message, "fetch archive ended after 3 of 8 bytes")
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    // A complete body still fails when the helper exits non-zero afterwards.
    #[tokio::test]
    async fn complete_reports_status() {
        let (mut writer, reader) = tokio::io::duplex(64);
        writer.write_all(b"abcdefgh").await.unwrap();
        drop(writer);
        let (tx, rx) = mpsc::channel(4);

        let join = std::future::ready(Err(BackendError::Api("helper exited 1".to_string())));
        stream_output(reader, join, 8, tx).await;

        let items = drain_stream(rx).await;
        assert!(items[..items.len() - 1].iter().all(Result::is_ok));
        assert!(
            items.last().unwrap().is_err(),
            "the exec status must surface"
        );
    }

    fn header_bytes(path: &str, size: u64) -> [u8; 512] {
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Regular);
        header.set_size(size);
        header.set_path(path).expect("header path");
        header.set_cksum();
        *header.as_bytes()
    }

    // A deterministic protocol mismatch must keep its own error: joining an undrained
    // exec would block until the timeout and reclassify it as retryable.
    #[tokio::test(start_paused = true)]
    async fn invalid_header_survives() {
        let bytes = header_bytes("workspace/other.txt", 4);
        let join = std::future::pending::<Result<(), BackendError>>();
        let read = tokio::time::timeout(
            EXEC_JOIN_TIMEOUT * 4,
            read_header(&mut &bytes[..], "/workspace/data.txt", join),
        )
        .await
        .expect("validation must not wait on the exec");

        let Err(error) = read else {
            panic!("a mismatched archive path must fail");
        };
        assert!(matches!(error, BackendError::Conflict(_)));
    }

    // A header that never arrives in full is a transport failure: the exec status
    // holds the real cause.
    #[tokio::test]
    async fn torn_header_joins() {
        let bytes = header_bytes("workspace/data.txt", 4);
        let join = std::future::ready(Err(BackendError::Api("websocket closed".to_string())));

        let Err(error) = read_header(&mut &bytes[..16], "/workspace/data.txt", join).await else {
            panic!("a short header must fail");
        };
        assert!(matches!(&error, BackendError::Api(message) if message == "websocket closed"));
    }
}
