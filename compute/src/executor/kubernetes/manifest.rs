use std::collections::BTreeMap;
use std::path::Path;

use aruna_core::compute::{
    BackendError, FenceContext, StagingMode, TaskSpec, normalize_container_path,
};
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{ConfigMap, PersistentVolumeClaim, Pod, Secret};
use k8s_openapi::api::networking::v1::NetworkPolicy;
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::{EPOCH_ANNOTATION, GENERATION_ANNOTATION, ROLE_LABEL, STATE_ANNOTATION};
use crate::executor::config::KubernetesConfig;
use crate::executor::staging::StageLayout;

pub const WORKLOAD_SA: &str = "aruna-workload";
pub const WORKSPACE_PATH: &str = "/workspace";
pub const MARKER_PATH: &str = "/aruna-marker/marker";
pub const SENTINEL_PATH: &str = "/workspace/.aruna-stage";
pub const TASK_SENTINEL: &str = "/aruna-workspace/.aruna-stage";
pub const HELPER_PATH: &str = "/aruna-compute-helper";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageMarker {
    pub job_uid: String,
    pub attempt_epoch: u64,
    pub controller_generation: u64,
    pub layout_digest: String,
}

pub fn job_manifest(
    context: &FenceContext,
    spec: &TaskSpec,
    config: &KubernetesConfig,
    layout: &StageLayout,
) -> Result<Job, BackendError> {
    let name = context.attempt.external_name();
    let mut labels = labels(context, "task");
    labels.insert(
        "aruna-engine.org/network".to_string(),
        match spec.staging_mode {
            StagingMode::Files => "none",
            StagingMode::DirectS3 => "s3",
        }
        .to_string(),
    );
    let annotations = annotations(context, "active", layout, spec)?;
    let mut volumes = Vec::new();
    let mut mounts = Vec::new();
    let mut init = Vec::new();
    let mut env_from = Vec::new();
    if spec.staging_mode == StagingMode::Files {
        volumes.extend([
            json!({"name":"workspace","persistentVolumeClaim":{"claimName":workspace_name(&name)}}),
            json!({"name":"marker","configMap":{"name":marker_name(&name)}}),
            json!({"name":"tools","emptyDir":{}}),
        ]);
        for input in &layout.files {
            mounts.push(json!({
                "name":"workspace",
                "mountPath":input.path,
                "subPath":relative_path(&input.path)?,
                "readOnly":true
            }));
        }
        for output in &layout.output_parents {
            mounts.push(json!({
                "name":"workspace",
                "mountPath":output,
                "subPath":relative_path(output)?,
                "readOnly":false
            }));
        }
        mounts.extend([
            json!({"name":"workspace","mountPath":"/aruna-workspace","readOnly":true}),
            json!({"name":"marker","mountPath":"/aruna-marker","readOnly":true}),
            json!({"name":"tools","mountPath":"/aruna-tools","readOnly":true}),
        ]);
        init.push(json!({
            "name":"stage-gate",
            "image":config.helper_image,
            "imagePullPolicy":"IfNotPresent",
            "command":[HELPER_PATH],
            "args":["probe","--install","/tools/probe","--marker",MARKER_PATH,"--sentinel",SENTINEL_PATH],
            "securityContext":container_security(true),
            "volumeMounts":[
                {"name":"workspace","mountPath":WORKSPACE_PATH,"readOnly":true},
                {"name":"marker","mountPath":"/aruna-marker","readOnly":true},
                {"name":"tools","mountPath":"/tools"}
            ]
        }));
    } else {
        env_from.push(json!({"secretRef":{"name":secret_name(&name)}}));
    }
    let mut env = spec
        .env
        .iter()
        .map(|(name, value)| json!({"name":name,"value":value}))
        .collect::<Vec<_>>();
    env.push(json!({"name":"ARUNA_JOB_ID","value":spec.attempt.job_id}));
    if let Some(workspace) = &spec.workspace {
        env.extend([
            json!({"name":"AWS_ENDPOINT_URL","value":workspace.s3_endpoint}),
            json!({"name":"AWS_REGION","value":workspace.region}),
            json!({"name":"ARUNA_WORKSPACE_BUCKET","value":workspace.bucket_name}),
        ]);
    }
    let startup_probe = (spec.staging_mode == StagingMode::Files).then(|| {
        json!({
            "exec":{"command":["/aruna-tools/probe","probe","--marker",MARKER_PATH,"--sentinel",TASK_SENTINEL]},
            "failureThreshold":3,
            "periodSeconds":2,
            "timeoutSeconds":2
        })
    });
    let container = json!({
        "name":"task",
        "image":spec.image,
        "imagePullPolicy":"IfNotPresent",
        "command":spec.entrypoint,
        "args":if spec.command.is_empty() { None::<Vec<String>> } else { Some(spec.command.clone()) },
        "workingDir":spec.workdir,
        "env":env,
        "envFrom":env_from,
        "resources":resource_limits(spec),
        "securityContext":container_security(spec.security.read_only_rootfs),
        "startupProbe":startup_probe,
        "volumeMounts":mounts
    });
    let deadline = spec
        .resources
        .max_walltime
        .map(|duration| i64::try_from(duration.as_secs().max(1)).unwrap_or(i64::MAX));
    serde_json::from_value(json!({
        "apiVersion":"batch/v1",
        "kind":"Job",
        "metadata":{
            "name":name,
            "namespace":config.namespace,
            "labels":labels,
            "annotations":annotations,
            "finalizers":["aruna-engine.org/attempt-protection"]
        },
        "spec":{
            "suspend":true,
            "backoffLimit":0,
            "completions":1,
            "parallelism":1,
            "activeDeadlineSeconds":deadline,
            "template":{
                "metadata":{"labels":labels},
                "spec":{
                    "restartPolicy":"Never",
                    "serviceAccountName":WORKLOAD_SA,
                    "automountServiceAccountToken":false,
                    "securityContext":pod_security(),
                    "initContainers":init,
                    "containers":[container],
                    "volumes":volumes
                }
            }
        }
    }))
    .map_err(manifest_error)
}

pub fn pvc_manifest(
    context: &FenceContext,
    config: &KubernetesConfig,
    bytes: u64,
) -> Result<PersistentVolumeClaim, BackendError> {
    serde_json::from_value(json!({
        "apiVersion":"v1",
        "kind":"PersistentVolumeClaim",
        "metadata":{
            "name":workspace_name(&context.attempt.external_name()),
            "namespace":config.namespace,
            "labels":labels(context,"workspace"),
            "annotations":annotations_base(context,"active")
        },
        "spec":{
            "accessModes":["ReadWriteOncePod"],
            "storageClassName":config.storage_class,
            "resources":{"requests":{"storage":bytes.to_string()}}
        }
    }))
    .map_err(manifest_error)
}

pub fn helper_pod(
    context: &FenceContext,
    config: &KubernetesConfig,
    role: &str,
) -> Result<Pod, BackendError> {
    let name = helper_name(&context.attempt.external_name(), role);
    serde_json::from_value(json!({
        "apiVersion":"v1",
        "kind":"Pod",
        "metadata":{
            "name":name,
            "namespace":config.namespace,
            "labels":labels(context,role),
            "annotations":annotations_base(context,"active")
        },
        "spec":{
            "restartPolicy":"Never",
            "serviceAccountName":WORKLOAD_SA,
            "automountServiceAccountToken":false,
            "securityContext":pod_security(),
            "containers":[{
                "name":"helper",
                "image":config.helper_image,
                "imagePullPolicy":"IfNotPresent",
                "command":[HELPER_PATH],
                "args":["probe","--hold"],
                "securityContext":container_security(true),
                "volumeMounts":[{"name":"workspace","mountPath":WORKSPACE_PATH}]
            }],
            "volumes":[{
                "name":"workspace",
                "persistentVolumeClaim":{"claimName":workspace_name(&context.attempt.external_name())}
            }]
        }
    }))
    .map_err(manifest_error)
}

pub fn marker_manifest(
    context: &FenceContext,
    config: &KubernetesConfig,
    marker: &StageMarker,
) -> Result<ConfigMap, BackendError> {
    let value = serde_json::to_string(marker)
        .map_err(|error| BackendError::Api(format!("serialize stage marker: {error}")))?;
    serde_json::from_value(json!({
        "apiVersion":"v1",
        "kind":"ConfigMap",
        "metadata":{
            "name":marker_name(&context.attempt.external_name()),
            "namespace":config.namespace,
            "labels":labels(context,"marker"),
            "annotations":annotations_base(context,"active")
        },
        "data":{"marker":value}
    }))
    .map_err(manifest_error)
}

pub fn secret_manifest(
    context: &FenceContext,
    config: &KubernetesConfig,
    spec: &TaskSpec,
) -> Result<Secret, BackendError> {
    let values = spec
        .secret_env
        .iter()
        .map(|(key, value)| (key.clone(), value.expose().to_string()))
        .collect::<BTreeMap<_, _>>();
    serde_json::from_value(json!({
        "apiVersion":"v1",
        "kind":"Secret",
        "metadata":{
            "name":secret_name(&context.attempt.external_name()),
            "namespace":config.namespace,
            "labels":labels(context,"credentials"),
            "annotations":annotations_base(context,"active")
        },
        "type":"Opaque",
        "stringData":values
    }))
    .map_err(manifest_error)
}

pub fn network_policies(config: &KubernetesConfig) -> Result<Vec<NetworkPolicy>, BackendError> {
    let deny = serde_json::from_value(json!({
        "apiVersion":"networking.k8s.io/v1",
        "kind":"NetworkPolicy",
        "metadata":{"name":"aruna-compute-deny","namespace":config.namespace},
        "spec":{
            "podSelector":{"matchLabels":{"aruna-engine.org/network":"none"}},
            "policyTypes":["Ingress","Egress"],
            "ingress":[],
            "egress":[]
        }
    }))
    .map_err(manifest_error)?;
    let cidrs = config
        .s3_cidrs
        .iter()
        .map(|cidr| json!({"ipBlock":{"cidr":cidr}}))
        .collect::<Vec<_>>();
    if config.s3_cidrs.is_empty() {
        return Ok(vec![deny]);
    }
    let s3 = serde_json::from_value(json!({
        "apiVersion":"networking.k8s.io/v1",
        "kind":"NetworkPolicy",
        "metadata":{"name":"aruna-compute-s3","namespace":config.namespace},
        "spec":{
            "podSelector":{"matchLabels":{"aruna-engine.org/network":"s3"}},
            "policyTypes":["Ingress","Egress"],
            "ingress":[],
            "egress":[
                {"to":cidrs,"ports":[{"protocol":"TCP","port":config.s3_port}]},
                {"to":[{"namespaceSelector":{"matchLabels":{
                    "kubernetes.io/metadata.name":"kube-system"
                }}}],"ports":[
                    {"protocol":"UDP","port":53},{"protocol":"TCP","port":53}
                ]}
            ]
        }
    }))
    .map_err(manifest_error)?;
    Ok(vec![deny, s3])
}

pub fn marker_name(name: &str) -> String {
    format!("{name}-staged")
}

pub fn workspace_name(name: &str) -> String {
    format!("{name}-ws")
}

pub fn secret_name(name: &str) -> String {
    format!("{name}-env")
}

pub fn helper_name(name: &str, role: &str) -> String {
    format!("{name}-{role}")
}

fn labels(context: &FenceContext, role: &str) -> BTreeMap<String, String> {
    let network = if role == "task" { "task" } else { "none" };
    BTreeMap::from([
        (
            "aruna-engine.org/job-id".to_string(),
            context.attempt.job_id.clone(),
        ),
        (
            "aruna-engine.org/attempt".to_string(),
            context.attempt.attempt.to_string(),
        ),
        (ROLE_LABEL.to_string(), role.to_string()),
        (
            "aruna-engine.org/generation".to_string(),
            context.controller_generation.to_string(),
        ),
        ("aruna-engine.org/network".to_string(), network.to_string()),
    ])
}

fn annotations(
    context: &FenceContext,
    state: &str,
    layout: &StageLayout,
    spec: &TaskSpec,
) -> Result<BTreeMap<String, String>, BackendError> {
    let mut annotations = annotations_base(context, state);
    annotations.insert(
        "aruna-engine.org/layout-digest".to_string(),
        layout.digest(),
    );
    annotations.insert(
        "aruna-engine.org/staging-mode".to_string(),
        match spec.staging_mode {
            StagingMode::Files => "files",
            StagingMode::DirectS3 => "direct-s3",
        }
        .to_string(),
    );
    annotations.insert(
        "aruna-engine.org/output-paths".to_string(),
        serde_json::to_string(&spec.output_paths)
            .map_err(|error| BackendError::Api(format!("serialize output paths: {error}")))?,
    );
    Ok(annotations)
}

fn annotations_base(context: &FenceContext, state: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            EPOCH_ANNOTATION.to_string(),
            context.attempt_epoch.to_string(),
        ),
        (
            GENERATION_ANNOTATION.to_string(),
            context.controller_generation.to_string(),
        ),
        (STATE_ANNOTATION.to_string(), state.to_string()),
    ])
}

fn relative_path(path: &Path) -> Result<String, BackendError> {
    let normalized = normalize_container_path(
        path.to_str()
            .ok_or_else(|| BackendError::InvalidSpec("container path is not UTF-8".to_string()))?,
    )
    .map_err(BackendError::InvalidSpec)?;
    normalized
        .strip_prefix("/")
        .map(|path| path.display().to_string())
        .map_err(|_| BackendError::InvalidSpec("container path is not absolute".to_string()))
}

fn pod_security() -> serde_json::Value {
    json!({
        "runAsNonRoot":true,
        "runAsUser":65534,
        "runAsGroup":65534,
        "fsGroup":65534,
        "fsGroupChangePolicy":"OnRootMismatch",
        "seccompProfile":{"type":"RuntimeDefault"}
    })
}

fn container_security(read_only: bool) -> serde_json::Value {
    json!({
        "allowPrivilegeEscalation":false,
        "capabilities":{"drop":["ALL"]},
        "readOnlyRootFilesystem":read_only,
        "runAsNonRoot":true,
        "runAsUser":65534,
        "runAsGroup":65534,
        "seccompProfile":{"type":"RuntimeDefault"}
    })
}

fn resource_limits(spec: &TaskSpec) -> serde_json::Value {
    let mut limits = BTreeMap::new();
    if let Some(cpu) = spec.resources.cpu_cores {
        limits.insert("cpu", cpu.to_string());
    }
    if let Some(memory) = spec.resources.ram_bytes {
        limits.insert("memory", memory.to_string());
    }
    if let Some(disk) = spec.resources.disk_bytes {
        limits.insert("ephemeral-storage", disk.to_string());
    }
    json!({"limits":limits,"requests":limits})
}

fn manifest_error(error: serde_json::Error) -> BackendError {
    BackendError::Api(format!("build Kubernetes manifest: {error}"))
}

#[cfg(test)]
mod tests {
    use aruna_core::compute::{AttemptRef, TaskInput};

    use super::*;

    fn context() -> FenceContext {
        FenceContext {
            attempt: AttemptRef::new("job", 1),
            attempt_epoch: 7,
            controller_generation: 3,
        }
    }

    fn config() -> KubernetesConfig {
        KubernetesConfig {
            namespace: "compute".to_string(),
            storage_class: "csi".to_string(),
            helper_image:
                "helper@sha256:0000000000000000000000000000000000000000000000000000000000000000"
                    .to_string(),
            pull_deadline: std::time::Duration::from_secs(30),
            s3_cidrs: Vec::new(),
            s3_port: 443,
        }
    }

    #[test]
    fn uses_rwop_only() {
        let pvc = pvc_manifest(&context(), &config(), 1024).unwrap();
        assert_eq!(
            pvc.spec.unwrap().access_modes.unwrap(),
            ["ReadWriteOncePod"]
        );
    }

    #[test]
    fn gates_task_start() {
        let mut spec = TaskSpec::new(
            context().attempt,
            "task@sha256:0000000000000000000000000000000000000000000000000000000000000000",
        );
        spec.inputs.push(TaskInput::from_bytes("/input/a", "a"));
        spec.output_paths.push("/output/a".to_string());
        let layout = StageLayout::from_spec(&spec).unwrap();
        let job = job_manifest(&context(), &spec, &config(), &layout).unwrap();
        let pod = job.spec.unwrap().template.spec.unwrap();
        assert_eq!(pod.init_containers.unwrap().len(), 1);
        assert!(pod.containers[0].startup_probe.is_some());
    }

    #[test]
    fn restricts_dns_egress() {
        let mut config = config();
        config.s3_cidrs.push("10.0.0.0/8".to_string());

        let policies = network_policies(&config).unwrap();
        let policy = serde_json::to_value(&policies[1]).unwrap();
        let peer = &policy["spec"]["egress"][1]["to"][0];

        assert_eq!(
            peer["namespaceSelector"]["matchLabels"]["kubernetes.io/metadata.name"],
            "kube-system"
        );
        assert!(peer.get("ipBlock").is_none());
    }
}
