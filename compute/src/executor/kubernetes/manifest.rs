//! Builds the Kubernetes objects an attempt needs, from Job to volumes and policies.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use aruna_core::compute::{
    BackendError, CACHE_MANIFEST_KEY, FenceContext, GrantedResources, NetworkAccess, SessionMount,
    StagingMode, TaskSpec, normalize_container_path,
};
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{ConfigMap, PersistentVolume, PersistentVolumeClaim, Pod, Secret};
use k8s_openapi::api::networking::v1::NetworkPolicy;
use kube::api::DynamicObject;
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::{EPOCH_ANNOTATION, GENERATION_ANNOTATION, ROLE_LABEL, STATE_ANNOTATION};
use crate::executor::config::{KubedalCache, KubernetesConfig};
use crate::executor::staging::StageLayout;
use crate::executor::{digest_pinned, enforced_limit};

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
        match spec.security.network {
            NetworkAccess::Isolated if spec.reaches_s3() => "s3",
            NetworkAccess::Isolated => "none",
            NetworkAccess::S3Only => "s3",
            NetworkAccess::Open => "task",
        }
        .to_string(),
    );
    let annotations = annotations(context, "active", layout, spec)?;
    let mut volumes = Vec::new();
    let mut mounts = Vec::new();
    let mut init = Vec::new();
    let mut env_from = Vec::new();
    if needs_workspace(spec) {
        volumes.extend([
            json!({"name":"workspace","persistentVolumeClaim":{"claimName":workspace_name(&name)}}),
            json!({"name":"marker","configMap":{"name":marker_name(&name)}}),
            json!({"name":"tools","emptyDir":{}}),
        ]);
        // Copied inputs come from the workspace, also next to mounted ones.
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
    }
    if spec.staging_mode == StagingMode::S3Mount {
        let buckets = mount_buckets(layout);
        for (index, bucket) in buckets.iter().enumerate() {
            let inputs = layout.mounts.iter().filter(|input| &input.bucket == bucket);
            let (cached, streamed): (Vec<_>, Vec<_>) =
                inputs.partition(|input| input.cached && cache_of(config).is_some());
            // Cached inputs come from a KubeDAL file list that the worker keeps between runs.
            if let (Some(kubedal), false) = (&config.kubedal, cached.is_empty()) {
                let volume = cache_name(&name, index);
                volumes.push(json!({"name":volume,"csi":{
                    "driver":kubedal.driver,
                    "readOnly":true,
                    "volumeAttributes":{"dataNode":mount_name(&name,index),"manifest":CACHE_MANIFEST_KEY}
                }}));
                for input in &cached {
                    mounts.push(json!({
                        "name":volume,
                        "mountPath":input.path,
                        "subPath":input.key,
                        "readOnly":true
                    }));
                }
            }
            if streamed.is_empty() {
                continue;
            }
            // KubeDAL reads its endpoint and credential from the run's own DataNode.
            let source = match &config.kubedal {
                Some(kubedal) => json!({"csi":{
                    "driver":kubedal.driver,
                    "readOnly":true,
                    "volumeAttributes":{"dataNode":mount_name(&name,index),"mount":"fuse-read-only"}
                }}),
                None => json!({"persistentVolumeClaim":{"claimName":mount_name(&name,index)}}),
            };
            let mut volume = json!({"name":mount_name(&name,index)});
            if let (Some(volume), Some(source)) = (volume.as_object_mut(), source.as_object()) {
                volume.extend(source.clone());
            }
            volumes.push(volume);
            for input in streamed {
                mounts.push(json!({
                    "name":mount_name(&name,index),
                    "mountPath":input.path,
                    "subPath":input.key,
                    "readOnly":true
                }));
            }
        }
    }
    if spec.staging_mode == StagingMode::DirectS3 || spec.reaches_s3() {
        env_from.push(json!({"secretRef":{"name":secret_name(&name)}}));
    }
    // Tools write caches relative to the working directory, so the non-root
    // task gets a writable one that read-only inputs nest beneath.
    if let Some(workdir) = spec.workdir.as_deref() {
        let scratch = normalize_container_path(workdir).map_err(BackendError::InvalidSpec)?;
        let taken = layout.mounts.iter().any(|input| input.path == scratch)
            || layout.output_parents.contains(&scratch);
        if !taken {
            let empty_dir = match spec.resources.disk_bytes {
                Some(bytes) => json!({"sizeLimit":bytes.to_string()}),
                None => json!({}),
            };
            volumes.push(json!({"name":"scratch","emptyDir":empty_dir}));
            mounts.push(json!({"name":"scratch","mountPath":scratch}));
        }
    }
    // The data mount nests inside the scratch directory, so it comes after it.
    if let Some(mount) = session_mount(spec, config) {
        let path = normalize_container_path(&mount.path).map_err(BackendError::InvalidSpec)?;
        let volume = data_name(&name);
        // KubeDAL writes through the session's own DataNode, so the credential stays on this realm.
        volumes.push(match &config.kubedal {
            Some(kubedal) => {
                let mut attributes = json!({"dataNode":volume,"mount":"fuse-read-write"});
                if !mount.prefix.is_empty() {
                    attributes["path"] = json!(mount.prefix);
                }
                json!({"name":volume,"csi":{
                    "driver":kubedal.driver,
                    "readOnly":false,
                    "volumeAttributes":attributes
                }})
            }
            None => json!({"name":volume,"persistentVolumeClaim":{"claimName":volume}}),
        });
        mounts.push(json!({
            "name":volume,
            "mountPath":path,
            "readOnly":false
        }));
    }
    // The credential Secret rides `envFrom`, which any inline entry of the same
    // name would shadow, so the stored secret always wins here too.
    let mut env = spec
        .env
        .iter()
        .filter(|(name, _)| !spec.secret_env.contains_key(*name))
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
    let startup_probe = needs_workspace(spec).then(|| {
        json!({
            "exec":{"command":["/aruna-tools/probe","probe","--marker",MARKER_PATH,"--sentinel",TASK_SENTINEL]},
            "failureThreshold":3,
            "periodSeconds":2,
            "timeoutSeconds":2
        })
    });
    // Tags can move between attempts, so Kubernetes must pull them every time.
    let image_pull_policy = if digest_pinned(&spec.image) {
        "IfNotPresent"
    } else {
        "Always"
    };
    let container = json!({
        "name":"task",
        "image":spec.image,
        "imagePullPolicy":image_pull_policy,
        "command":spec.entrypoint,
        "args":if spec.command.is_empty() { None::<Vec<String>> } else { Some(spec.command.clone()) },
        "workingDir":spec.workdir,
        "env":env,
        "envFrom":env_from,
        "resources":resource_limits(spec, config)?,
        "securityContext":container_security(spec.security.read_only_rootfs),
        "startupProbe":startup_probe,
        // When the container writes no termination log, Kubernetes reports its
        // last log lines as the terminated message of a non-zero exit.
        "terminationMessagePolicy":"FallbackToLogsOnError",
        "volumeMounts":mounts
    });
    let deadline = deadline_secs(spec);
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
                    "serviceAccountName":config.service_account,
                    "automountServiceAccountToken":false,
                    "nodeSelector":node_selector(config),
                    "affinity":cache_affinity(config, layout),
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

pub fn mount_pv_manifest(
    context: &FenceContext,
    config: &KubernetesConfig,
    index: usize,
    bucket: &str,
) -> Result<PersistentVolume, BackendError> {
    let name = mount_name(&context.attempt.external_name(), index);
    let options = ["file-mode=0444", "dir-mode=0555"];
    s3_pv_manifest(context, config, &name, bucket, &options, true)
}

/// The KubeDAL DataNode one mounted bucket reads through: this node's gateway, the bucket
/// and the run's own secret, read only. The run's labels and epoch let cleanup find it.
pub fn data_node_manifest(
    context: &FenceContext,
    config: &KubernetesConfig,
    index: usize,
    bucket: &str,
) -> Result<serde_json::Value, BackendError> {
    let name = mount_name(&context.attempt.external_name(), index);
    kubedal_node(context, config, &name, bucket, true)
}

/// The session's writable DataNode for its workspace bucket, named like its data volume.
pub fn session_node_manifest(
    context: &FenceContext,
    config: &KubernetesConfig,
    bucket: &str,
) -> Result<serde_json::Value, BackendError> {
    let name = data_name(&context.attempt.external_name());
    kubedal_node(context, config, &name, bucket, false)
}

fn kubedal_node(
    context: &FenceContext,
    config: &KubernetesConfig,
    node: &str,
    bucket: &str,
    read_only: bool,
) -> Result<serde_json::Value, BackendError> {
    let kubedal = config.kubedal.as_ref().ok_or_else(|| {
        BackendError::InvalidSpec("KubeDAL mounts are disabled on this backend".to_string())
    })?;
    let name = context.attempt.external_name();
    let mut spec = json!({
        "backend":"S3",
        "read_only":read_only,
        "secretRef":{"name":secret_name(&name)},
        "config":{
            "bucket":bucket,
            "endpoint":kubedal.endpoint,
            "region":kubedal.region
        }
    });
    // Runs share cached content by its digest; a session's writable node never caches.
    if let Some(cache) = kubedal.cache.as_ref().filter(|_| read_only) {
        spec["cacheScope"] = json!(cache.scope);
    }
    Ok(json!({
        "apiVersion":"kubedal.arunaengine.org/v1alpha1",
        "kind":"DataNode",
        "metadata":{
            "name":node,
            "namespace":config.namespace,
            "labels":labels(context,"s3-mount"),
            "annotations":annotations_base(context,"active")
        },
        "spec":spec
    }))
}

pub fn mount_pvc_manifest(
    context: &FenceContext,
    config: &KubernetesConfig,
    index: usize,
) -> Result<PersistentVolumeClaim, BackendError> {
    s3_pvc_manifest(
        context,
        config,
        &mount_name(&context.attempt.external_name(), index),
    )
}

/// The session's writable view of the workspace bucket below `prefix`; an
/// empty prefix serves the whole bucket.
pub fn data_pv_manifest(
    context: &FenceContext,
    config: &KubernetesConfig,
    bucket: &str,
    prefix: &str,
) -> Result<PersistentVolume, BackendError> {
    let name = data_name(&context.attempt.external_name());
    let prefix = format!("prefix={prefix}");
    let mut options = vec![
        "allow-delete",
        "allow-overwrite",
        "file-mode=0644",
        "dir-mode=0755",
    ];
    if prefix != "prefix=" {
        options.insert(0, prefix.as_str());
    }
    s3_pv_manifest(context, config, &name, bucket, &options, false)
}

pub fn data_pvc_manifest(
    context: &FenceContext,
    config: &KubernetesConfig,
) -> Result<PersistentVolumeClaim, BackendError> {
    s3_pvc_manifest(
        context,
        config,
        &data_name(&context.attempt.external_name()),
    )
}

fn s3_pv_manifest(
    context: &FenceContext,
    config: &KubernetesConfig,
    name: &str,
    bucket: &str,
    options: &[&str],
    read_only: bool,
) -> Result<PersistentVolume, BackendError> {
    let driver = config.s3_mount_driver.as_deref().ok_or_else(|| {
        BackendError::InvalidSpec("S3 mounts are disabled on this backend".to_string())
    })?;
    let mut mount_options = vec!["uid=65534", "gid=65534", "allow-other"];
    mount_options.extend_from_slice(options);
    serde_json::from_value(json!({
        "apiVersion":"v1",
        "kind":"PersistentVolume",
        "metadata":{
            "name":name,
            "labels":labels(context,"s3-mount"),
            "annotations":annotations_base(context,"active")
        },
        "spec":{
            "capacity":{"storage":"1Gi"},
            "accessModes":["ReadWriteMany"],
            "persistentVolumeReclaimPolicy":"Retain",
            "storageClassName":"",
            "claimRef":{"namespace":config.namespace,"name":name},
            "mountOptions":mount_options,
            "csi":{
                "driver":driver,
                "volumeHandle":name,
                "readOnly":read_only,
                "volumeAttributes":{
                    "bucketName":bucket,
                    "authenticationSource":"secret"
                },
                "nodePublishSecretRef":{
                    "name":secret_name(&context.attempt.external_name()),
                    "namespace":config.namespace
                }
            }
        }
    }))
    .map_err(manifest_error)
}

fn s3_pvc_manifest(
    context: &FenceContext,
    config: &KubernetesConfig,
    name: &str,
) -> Result<PersistentVolumeClaim, BackendError> {
    serde_json::from_value(json!({
        "apiVersion":"v1",
        "kind":"PersistentVolumeClaim",
        "metadata":{
            "name":name,
            "namespace":config.namespace,
            "labels":labels(context,"s3-mount"),
            "annotations":annotations_base(context,"active")
        },
        "spec":{
            "accessModes":["ReadWriteMany"],
            "storageClassName":"",
            "volumeName":name,
            "resources":{"requests":{"storage":"1Gi"}}
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
            "serviceAccountName":config.service_account,
            "automountServiceAccountToken":false,
            "nodeSelector":node_selector(config),
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
    let mut values = spec
        .secret_env
        .iter()
        .map(|(key, value)| (key.clone(), value.expose().to_string()))
        .collect::<BTreeMap<_, _>>();
    // The CSI driver reads the data mount credential under its own key names.
    if session_mount(spec, config).is_some() {
        for (env, key) in [
            ("AWS_ACCESS_KEY_ID", "access_key_id"),
            ("AWS_SECRET_ACCESS_KEY", "secret_access_key"),
        ] {
            if let Some(value) = values.get(env).cloned() {
                values.entry(key.to_string()).or_insert(value);
            }
        }
    }
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
                {"ports":[{"protocol":"UDP","port":53},{"protocol":"TCP","port":53}]}
            ]
        }
    }))
    .map_err(manifest_error)?;
    Ok(vec![deny, s3])
}

/// An operator manifest with the file it was read from, so a later discovery
/// or apply failure can name that file.
#[derive(Clone, Debug)]
pub struct PolicyManifest {
    pub source: PathBuf,
    pub object: DynamicObject,
}

/// Reads the configured manifest files, including multi-document files. The
/// node applies these objects itself, so a broken document is refused here.
pub fn policy_manifests(config: &KubernetesConfig) -> Result<Vec<PolicyManifest>, BackendError> {
    let mut manifests = Vec::new();
    for path in &config.policy_manifests {
        let text = std::fs::read_to_string(path).map_err(|error| {
            BackendError::InvalidSpec(format!(
                "policy manifest `{}` is unreadable: {error}",
                path.display()
            ))
        })?;
        let documents: Vec<serde_json::Value> = serde_saphyr::from_multiple(&text)
            .map_err(|error| policy_error(path, 0, &error.to_string()))?;
        for (index, value) in documents.into_iter().enumerate() {
            if value.is_null() {
                continue;
            }
            manifests.push(PolicyManifest {
                source: path.clone(),
                object: policy_object(config, path, index, value)?,
            });
        }
    }
    Ok(manifests)
}

/// Namespaced objects only: the node applies them in its compute namespace.
fn policy_object(
    config: &KubernetesConfig,
    path: &Path,
    index: usize,
    mut value: serde_json::Value,
) -> Result<DynamicObject, BackendError> {
    for field in ["apiVersion", "kind"] {
        if named_field(&value, field).is_none() {
            return Err(policy_error(path, index, &format!("has no {field}")));
        }
    }
    if named_field(&value["metadata"], "name").is_none() {
        return Err(policy_error(path, index, "has no metadata.name"));
    }
    match value["metadata"].get("namespace") {
        None | Some(serde_json::Value::Null) => {
            value["metadata"]["namespace"] = json!(config.namespace);
        }
        Some(namespace) if namespace.as_str() == Some(config.namespace.as_str()) => {}
        Some(_) => {
            return Err(policy_error(
                path,
                index,
                &format!("is not in namespace `{}`", config.namespace),
            ));
        }
    }
    serde_json::from_value(value).map_err(|error| policy_error(path, index, &error.to_string()))
}

fn named_field<'a>(value: &'a serde_json::Value, field: &str) -> Option<&'a str> {
    value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .filter(|text| !text.trim().is_empty())
}

fn policy_error(path: &Path, index: usize, detail: &str) -> BackendError {
    BackendError::InvalidSpec(format!(
        "policy manifest `{}` document {index} {detail}",
        path.display()
    ))
}

pub fn marker_name(name: &str) -> String {
    format!("{name}-staged")
}

pub fn workspace_name(name: &str) -> String {
    format!("{name}-ws")
}

pub fn mount_name(name: &str, index: usize) -> String {
    format!("{name}-s3-{index}")
}

pub fn cache_name(name: &str, index: usize) -> String {
    format!("{name}-cache-{index}")
}

pub fn data_name(name: &str) -> String {
    format!("{name}-data")
}

/// The bucket slice a session sees as a folder, when this backend can mount it.
pub fn session_mount<'a>(
    spec: &'a TaskSpec,
    config: &KubernetesConfig,
) -> Option<&'a SessionMount> {
    if !spec.session || spec.workspace.is_none() || !config.mounts_inputs() {
        return None;
    }
    spec.session_mount.as_ref()
}

pub fn mount_buckets(layout: &StageLayout) -> Vec<String> {
    layout
        .mounts
        .iter()
        .map(|mount| mount.bucket.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

pub fn needs_workspace(spec: &TaskSpec) -> bool {
    spec.staging_mode == StagingMode::Files
        || (spec.staging_mode == StagingMode::S3Mount
            && (!spec.output_paths.is_empty() || !spec.inputs.is_empty()))
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
            StagingMode::S3Mount => "s3-mount",
        }
        .to_string(),
    );
    annotations.insert(
        "aruna-engine.org/workspace".to_string(),
        needs_workspace(spec).to_string(),
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

/// Requests and limits of the task container. CPU and memory always carry a
/// bound: an attempt the stored spec and the backend both leave open would run
/// against the whole node.
fn resource_limits(
    spec: &TaskSpec,
    config: &KubernetesConfig,
) -> Result<serde_json::Value, BackendError> {
    let mut limits = BTreeMap::new();
    limits.insert("cpu", cpu_cores(spec, config)?.to_string());
    limits.insert(
        "memory",
        enforced_limit(spec.resources.ram_bytes, config.default_mem_bytes, "memory")?.to_string(),
    );
    if let Some(disk) = spec.resources.disk_bytes.or(config.default_disk_bytes) {
        limits.insert("ephemeral-storage", disk.to_string());
    }
    Ok(json!({"limits":limits,"requests":limits}))
}

fn cpu_cores(spec: &TaskSpec, config: &KubernetesConfig) -> Result<u64, BackendError> {
    enforced_limit(
        spec.resources.cpu_cores.map(u64::from),
        config.default_cpu_cores.map(u64::from),
        "cpu",
    )
}

/// Seconds of the Job's `activeDeadlineSeconds`, at least one.
fn deadline_secs(spec: &TaskSpec) -> Option<i64> {
    spec.resources
        .max_walltime
        .map(|duration| i64::try_from(duration.as_secs().max(1)).unwrap_or(i64::MAX))
}

/// The workspace volume of a run: its disk request, else its staged inputs plus room to work.
pub fn workspace_bytes(spec: &TaskSpec) -> u64 {
    spec.resources.disk_bytes.unwrap_or_else(|| {
        (spec.inputs.iter()).fold(super::DEFAULT_WORKSPACE_BYTES, |total, input| {
            total.saturating_add(input.size())
        })
    })
}

/// The limits the task pod and its workspace volume are created with.
pub fn granted_resources(
    spec: &TaskSpec,
    config: &KubernetesConfig,
) -> Result<GrantedResources, BackendError> {
    Ok(GrantedResources {
        nano_cpus: Some(cpu_cores(spec, config)?.saturating_mul(1_000_000_000)),
        ram_bytes: Some(enforced_limit(
            spec.resources.ram_bytes,
            config.default_mem_bytes,
            "memory",
        )?),
        disk_bytes: spec.resources.disk_bytes.or(config.default_disk_bytes),
        workspace_bytes: needs_workspace(spec).then(|| workspace_bytes(spec)),
        walltime_ms: deadline_secs(spec).map(|secs| (secs as u64).saturating_mul(1000)),
    })
}

/// The selector every task and helper pod carries, so a declared worker site is
/// the site Kubernetes actually schedules on.
fn node_selector(config: &KubernetesConfig) -> Option<serde_json::Value> {
    match config.node_selector.is_empty() {
        true => None,
        false => Some(json!(config.node_selector)),
    }
}

/// Runs with cached inputs prefer workers with a cache volume, but may run elsewhere.
fn cache_affinity(config: &KubernetesConfig, layout: &StageLayout) -> Option<serde_json::Value> {
    let cache = cache_of(config).filter(|cache| !cache.worker_labels.is_empty())?;
    if !layout.mounts.iter().any(|input| input.cached) {
        return None;
    }
    let expressions = cache
        .worker_labels
        .iter()
        .map(|(key, value)| json!({"key":key,"operator":"In","values":[value]}))
        .collect::<Vec<_>>();
    Some(
        json!({"nodeAffinity":{"preferredDuringSchedulingIgnoredDuringExecution":[
            {"weight":100,"preference":{"matchExpressions":expressions}}
        ]}}),
    )
}

fn cache_of(config: &KubernetesConfig) -> Option<&KubedalCache> {
    config.kubedal.as_ref()?.cache.as_ref()
}

fn manifest_error(error: serde_json::Error) -> BackendError {
    BackendError::Api(format!("build Kubernetes manifest: {error}"))
}

#[cfg(test)]
mod tests {
    use aruna_core::compute::{
        AttemptRef, S3Mount, Secret, SessionMount, TaskInput, WorkspaceBinding,
    };

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
            s3_mount_driver: Some("s3.csi.scality.com".to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn stamps_worker_selector() {
        // Every pod of a declared worker site must be pinned to it, or the
        // advertised placement would be a claim Kubernetes never honours.
        let base = config();
        let mut config = base.clone();
        config.node_selector = BTreeMap::from([("zone".to_string(), "dc-b".to_string())]);
        config.service_account = "aruna-restricted".to_string();
        let spec = TaskSpec::new(context().attempt, "registry.example/task:latest");
        let layout = StageLayout::from_spec(&spec).unwrap();

        let job = serde_json::to_value(job_manifest(&context(), &spec, &config, &layout).unwrap())
            .unwrap();
        let pod = &job["spec"]["template"]["spec"];
        assert_eq!(pod["nodeSelector"]["zone"], "dc-b");
        assert_eq!(pod["serviceAccountName"], "aruna-restricted");

        let helper =
            serde_json::to_value(helper_pod(&context(), &config, "drain").unwrap()).unwrap();
        assert_eq!(helper["spec"]["nodeSelector"]["zone"], "dc-b");
        assert_eq!(helper["spec"]["serviceAccountName"], "aruna-restricted");

        // Without a configured selector no pod carries an empty one.
        let plain =
            serde_json::to_value(job_manifest(&context(), &spec, &base, &layout).unwrap()).unwrap();
        assert!(plain["spec"]["template"]["spec"]["nodeSelector"].is_null());
    }

    #[test]
    fn granted_matches_manifest() {
        let mut spec = TaskSpec::new(context().attempt, "registry.example/task:latest");
        spec.resources.ram_bytes = Some(2_048);
        spec.resources.max_walltime = Some(std::time::Duration::from_millis(1_500));
        let mut config = config();
        config.default_cpu_cores = Some(3);
        let layout = StageLayout::from_spec(&spec).unwrap();
        let job = serde_json::to_value(job_manifest(&context(), &spec, &config, &layout).unwrap())
            .unwrap();
        let granted = granted_resources(&spec, &config).unwrap();
        let limits = &job["spec"]["template"]["spec"]["containers"][0]["resources"]["limits"];
        assert_eq!(limits["cpu"], "3");
        assert_eq!(granted.nano_cpus, Some(3_000_000_000));
        assert_eq!(limits["memory"], "2048");
        assert_eq!(granted.ram_bytes, Some(2_048));
        assert!(limits["ephemeral-storage"].is_null());
        assert_eq!(granted.disk_bytes, None);
        assert_eq!(job["spec"]["activeDeadlineSeconds"], 1);
        assert_eq!(granted.walltime_ms, Some(1_000));
        assert_eq!(granted.workspace_bytes, Some(workspace_bytes(&spec)));
        spec.staging_mode = StagingMode::DirectS3;
        assert_eq!(
            granted_resources(&spec, &config).unwrap().workspace_bytes,
            None
        );
    }

    #[test]
    fn bounds_task_resources() {
        // The stored envelope wins, the backend default fills a gap, and an
        // attempt neither of them bounds never becomes a manifest.
        let mut spec = TaskSpec::new(context().attempt, "registry.example/task:latest");
        spec.resources.cpu_cores = Some(4);
        spec.resources.ram_bytes = Some(2_048);
        spec.resources.disk_bytes = Some(4_096);
        let layout = StageLayout::from_spec(&spec).unwrap();

        let job =
            serde_json::to_value(job_manifest(&context(), &spec, &config(), &layout).unwrap())
                .unwrap();
        let limits = &job["spec"]["template"]["spec"]["containers"][0]["resources"]["limits"];
        assert_eq!(limits["cpu"], "4");
        assert_eq!(limits["memory"], "2048");
        assert_eq!(limits["ephemeral-storage"], "4096");

        let empty = TaskSpec::new(context().attempt, "registry.example/task:latest");
        let job =
            serde_json::to_value(job_manifest(&context(), &empty, &config(), &layout).unwrap())
                .unwrap();
        let limits = &job["spec"]["template"]["spec"]["containers"][0]["resources"]["limits"];
        assert_eq!(limits["cpu"], "2");
        assert_eq!(limits["memory"], (2u64 * 1024 * 1024 * 1024).to_string());

        let mut unbounded = config();
        unbounded.default_cpu_cores = None;
        unbounded.default_mem_bytes = None;
        assert!(job_manifest(&context(), &empty, &unbounded, &layout).is_err());
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
    fn keeps_exit_evidence() {
        // The task container must fall back to its logs, or a non-zero exit
        // leaves no terminated message behind.
        let spec = TaskSpec::new(context().attempt, "registry.example/task:latest");
        let layout = StageLayout::from_spec(&spec).unwrap();
        let job = job_manifest(&context(), &spec, &config(), &layout).unwrap();
        let pod = job.spec.unwrap().template.spec.unwrap();
        assert_eq!(
            pod.containers[0].termination_message_policy.as_deref(),
            Some("FallbackToLogsOnError")
        );
    }

    #[test]
    fn runs_without_workspace() {
        let mut spec = TaskSpec::new(context().attempt, "registry.example/task:latest");
        spec.staging_mode = StagingMode::S3Mount;
        let layout = StageLayout::from_spec(&spec).unwrap();
        let job = job_manifest(&context(), &spec, &config(), &layout).unwrap();
        let pod = job.spec.unwrap().template.spec.unwrap();

        assert!(pod.volumes.unwrap_or_default().is_empty());
        assert!(pod.init_containers.unwrap_or_default().is_empty());
        assert!(pod.containers[0].startup_probe.is_none());
    }

    #[test]
    fn session_uses_s3() {
        let mut spec = TaskSpec::new(context().attempt, "registry.example/session:latest");
        spec.session = true;
        spec.workdir = Some("/work".to_string());
        spec.inputs.push(TaskInput::from_bytes(
            "/inputs/requirements.txt",
            b"numpy".to_vec(),
        ));
        spec.secret_env.insert(
            "AWS_ACCESS_KEY_ID".to_string(),
            Secret::new("session-access"),
        );
        spec.secret_env.insert(
            "AWS_SECRET_ACCESS_KEY".to_string(),
            Secret::new("session-secret"),
        );
        let layout = StageLayout::from_spec(&spec).unwrap();
        for (network, label) in [
            (NetworkAccess::Isolated, "s3"),
            (NetworkAccess::Open, "task"),
        ] {
            spec.security.network = network;
            let job = job_manifest(&context(), &spec, &config(), &layout).unwrap();
            let template = job.spec.unwrap().template;
            assert_eq!(
                template.metadata.unwrap().labels.unwrap()["aruna-engine.org/network"],
                label
            );
            let pod = template.spec.unwrap();
            let task = &pod.containers[0];
            assert_eq!(
                task.env_from.as_ref().unwrap()[0]
                    .secret_ref
                    .as_ref()
                    .unwrap()
                    .name,
                secret_name(&context().attempt.external_name())
            );
            assert!(
                task.env
                    .as_ref()
                    .unwrap()
                    .iter()
                    .all(|entry| entry.name != "AWS_SECRET_ACCESS_KEY")
            );
            assert!(task.volume_mounts.as_ref().unwrap().iter().any(|mount| {
                mount.mount_path == "/inputs/requirements.txt" && mount.read_only == Some(true)
            }));
        }
    }

    #[test]
    fn mounts_through_kubedal() {
        // KubeDAL reads the run's own DataNode: this node's gateway, the view bucket and the
        // run secret, read only, as one inline FUSE volume with a sub path per input.
        let mut kubedal = config();
        kubedal.kubedal = Some(Box::new(crate::executor::config::KubedalMount {
            driver: "kubedal.arunaengine.org".to_string(),
            endpoint: "https://s3.node-2.example".to_string(),
            region: "us-east-1".to_string(),
            cache: None,
        }));
        let mut spec = TaskSpec::new(context().attempt, "registry.example/task:latest");
        spec.staging_mode = StagingMode::S3Mount;
        spec.s3_mounts.push(S3Mount {
            bucket: "in-run".to_string(),
            key: "db/bakta.db".to_string(),
            path: "/db/bakta.db".to_string(),
            cached: false,
        });
        spec.secret_env
            .insert("access_key_id".to_string(), Secret::new("temporary-access"));
        spec.secret_env.insert(
            "secret_access_key".to_string(),
            Secret::new("temporary-secret"),
        );
        let layout = StageLayout::from_spec(&spec).unwrap();
        let job = serde_json::to_value(job_manifest(&context(), &spec, &kubedal, &layout).unwrap())
            .unwrap();
        let pod = &job["spec"]["template"]["spec"];
        let volume = pod["volumes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|volume| volume.get("csi").is_some())
            .expect("inline volume");
        let name = mount_name(&context().attempt.external_name(), 0);
        assert_eq!(volume["csi"]["driver"], "kubedal.arunaengine.org");
        assert_eq!(volume["csi"]["readOnly"], true);
        assert_eq!(volume["csi"]["volumeAttributes"]["dataNode"], name.as_str());
        assert_eq!(volume["csi"]["volumeAttributes"]["mount"], "fuse-read-only");
        assert!(volume.get("persistentVolumeClaim").is_none());
        let mounts = pod["containers"][0]["volumeMounts"].as_array().unwrap();
        assert!(mounts.iter().any(|mount| mount["subPath"] == "db/bakta.db"
            && mount["mountPath"] == "/db/bakta.db"
            && mount["readOnly"] == true));
        let node = data_node_manifest(&context(), &kubedal, 0, "in-run").unwrap();
        assert_eq!(node["kind"], "DataNode");
        assert_eq!(node["spec"]["read_only"], true);
        assert_eq!(node["spec"]["config"]["bucket"], "in-run");
        assert_eq!(
            node["spec"]["config"]["endpoint"],
            "https://s3.node-2.example"
        );
        assert_eq!(
            node["spec"]["secretRef"]["name"],
            secret_name(&context().attempt.external_name()).as_str()
        );
        assert_eq!(node["metadata"]["labels"][ROLE_LABEL], "s3-mount");
        assert!(data_node_manifest(&context(), &config(), 0, "in-run").is_err());
    }

    #[test]
    fn caches_inputs_through_kubedal() {
        // Cached inputs come from a KubeDAL file list with the shared scope; others stay FUSE.
        let mut cached = config();
        cached.kubedal = Some(Box::new(crate::executor::config::KubedalMount {
            driver: "kubedal.arunaengine.org".to_string(),
            endpoint: "https://s3.node-2.example".to_string(),
            region: "us-east-1".to_string(),
            cache: Some(KubedalCache {
                scope: "node-2".to_string(),
                worker_labels: BTreeMap::from([("cache".to_string(), "large".to_string())]),
            }),
        }));
        let mut spec = TaskSpec::new(context().attempt, "registry.example/task:latest");
        spec.staging_mode = StagingMode::S3Mount;
        for (key, cached) in [("db/bakta.db", true), ("reads.fq", false)] {
            spec.s3_mounts.push(S3Mount {
                bucket: "in-run".to_string(),
                key: key.to_string(),
                path: format!("/{key}"),
                cached,
            });
        }
        spec.secret_env
            .insert("access_key_id".to_string(), Secret::new("temporary-access"));
        spec.secret_env.insert(
            "secret_access_key".to_string(),
            Secret::new("temporary-secret"),
        );
        let layout = StageLayout::from_spec(&spec).unwrap();
        let job = serde_json::to_value(job_manifest(&context(), &spec, &cached, &layout).unwrap())
            .unwrap();
        let pod = &job["spec"]["template"]["spec"];
        let name = context().attempt.external_name();
        let volumes = pod["volumes"].as_array().unwrap();
        let volume = |volume: &str| {
            volumes
                .iter()
                .find(|entry| entry["name"] == volume)
                .unwrap_or_else(|| panic!("volume {volume}"))
        };
        let list = volume(&cache_name(&name, 0));
        assert_eq!(list["csi"]["readOnly"], true);
        assert_eq!(
            list["csi"]["volumeAttributes"]["manifest"],
            CACHE_MANIFEST_KEY
        );
        assert_eq!(
            list["csi"]["volumeAttributes"]["dataNode"],
            mount_name(&name, 0).as_str()
        );
        let fuse = volume(&mount_name(&name, 0));
        assert_eq!(fuse["csi"]["volumeAttributes"]["mount"], "fuse-read-only");
        let mounts = pod["containers"][0]["volumeMounts"].as_array().unwrap();
        let source = |path: &str| {
            mounts
                .iter()
                .find(|mount| mount["mountPath"] == path)
                .map(|mount| mount["name"].clone())
        };
        assert_eq!(source("/db/bakta.db"), Some(json!(cache_name(&name, 0))));
        assert_eq!(source("/reads.fq"), Some(json!(mount_name(&name, 0))));
        let preferred =
            &pod["affinity"]["nodeAffinity"]["preferredDuringSchedulingIgnoredDuringExecution"][0];
        assert_eq!(
            preferred["preference"]["matchExpressions"][0],
            json!({"key":"cache","operator":"In","values":["large"]})
        );
        let node = data_node_manifest(&context(), &cached, 0, "in-run").unwrap();
        assert_eq!(node["spec"]["cacheScope"], "node-2");
        let session = session_node_manifest(&context(), &cached, "work").unwrap();
        assert!(session["spec"].get("cacheScope").is_none());

        // Without a configured cache, the same spec mounts everything through FUSE.
        cached.kubedal.as_mut().unwrap().cache = None;
        let job = serde_json::to_value(job_manifest(&context(), &spec, &cached, &layout).unwrap())
            .unwrap();
        let pod = &job["spec"]["template"]["spec"];
        assert_eq!(pod["volumes"].as_array().unwrap().len(), 1);
        assert!(pod["affinity"].is_null());
        let node = data_node_manifest(&context(), &cached, 0, "in-run").unwrap();
        assert!(node["spec"].get("cacheScope").is_none());
    }

    #[test]
    fn mounts_s3_inputs() {
        let mut spec = TaskSpec::new(context().attempt, "registry.example/task:latest");
        spec.staging_mode = StagingMode::S3Mount;
        spec.s3_mounts.push(S3Mount {
            bucket: "input-bucket".to_string(),
            key: "data/input.txt".to_string(),
            path: "/input.txt".to_string(),
            cached: false,
        });
        spec.secret_env
            .insert("access_key_id".to_string(), Secret::new("temporary-access"));
        spec.secret_env.insert(
            "secret_access_key".to_string(),
            Secret::new("temporary-secret"),
        );
        let layout = StageLayout::from_spec(&spec).unwrap();
        let job = job_manifest(&context(), &spec, &config(), &layout).unwrap();
        let pod = job.spec.unwrap().template.spec.unwrap();
        let mount = pod.containers[0]
            .volume_mounts
            .as_ref()
            .unwrap()
            .iter()
            .find(|mount| mount.mount_path == "/input.txt")
            .unwrap();
        assert_eq!(mount.sub_path.as_deref(), Some("data/input.txt"));
        assert_eq!(mount.read_only, Some(true));

        let pv = serde_json::to_value(
            mount_pv_manifest(&context(), &config(), 0, "input-bucket").unwrap(),
        )
        .unwrap();
        assert_eq!(pv["spec"]["csi"]["driver"], "s3.csi.scality.com");
        assert_eq!(
            pv["spec"]["csi"]["volumeAttributes"]["bucketName"],
            "input-bucket"
        );
        assert_eq!(
            pv["spec"]["csi"]["nodePublishSecretRef"]["name"],
            "aruna-job-a1-env"
        );
        assert_eq!(pv["spec"]["csi"]["readOnly"], true);
    }

    #[test]
    fn stages_pinned_input() {
        // A version-pinned input is delivered as a read-only copy in the
        // per-attempt workspace; no CSI volume can serve a version.
        let mut spec = TaskSpec::new(context().attempt, "registry.example/task:latest");
        spec.inputs
            .push(TaskInput::from_bytes("/in/data.csv", "a,b"));
        spec.output_paths.push("/out/report.txt".to_string());
        let layout = StageLayout::from_spec(&spec).unwrap();
        let job = job_manifest(&context(), &spec, &config(), &layout).unwrap();
        let pod = job.spec.unwrap().template.spec.unwrap();

        let mount = pod.containers[0]
            .volume_mounts
            .as_ref()
            .unwrap()
            .iter()
            .find(|mount| mount.mount_path == "/in/data.csv")
            .unwrap();
        assert_eq!(mount.name, "workspace");
        assert_eq!(mount.sub_path.as_deref(), Some("in/data.csv"));
        assert_eq!(mount.read_only, Some(true));

        let volumes: Vec<_> = pod
            .volumes
            .unwrap()
            .into_iter()
            .map(|volume| volume.name)
            .collect();
        assert_eq!(volumes, ["workspace", "marker", "tools"]);
        assert_eq!(pod.init_containers.unwrap().len(), 1);
        assert!(pod.containers[0].startup_probe.is_some());
    }

    #[test]
    fn uses_configured_driver() {
        let mut config = config();
        config.s3_mount_driver = Some("s3.csi.example.org".to_string());
        let pv = serde_json::to_value(
            mount_pv_manifest(&context(), &config, 0, "input-bucket").unwrap(),
        )
        .unwrap();
        assert_eq!(pv["spec"]["csi"]["driver"], "s3.csi.example.org");
    }

    fn session_spec() -> TaskSpec {
        let mut spec = TaskSpec::new(context().attempt, "registry.example/session:latest");
        spec.session = true;
        spec.workdir = Some("/work".to_string());
        spec.workspace = Some(WorkspaceBinding {
            s3_endpoint: "https://s3.example".to_string(),
            bucket_name: "workspace-bucket".to_string(),
            region: "us-east-1".to_string(),
        });
        spec.session_mount = Some(SessionMount {
            prefix: "data/".to_string(),
            path: "/work/data".to_string(),
        });
        spec.secret_env.insert(
            "AWS_ACCESS_KEY_ID".to_string(),
            Secret::new("session-access"),
        );
        spec.secret_env.insert(
            "AWS_SECRET_ACCESS_KEY".to_string(),
            Secret::new("session-secret"),
        );
        spec
    }

    #[test]
    fn mounts_session_data() {
        // The workspace bucket's data prefix shows up writable below the workdir.
        let spec = session_spec();
        let layout = StageLayout::from_spec(&spec).unwrap();
        let job = job_manifest(&context(), &spec, &config(), &layout).unwrap();
        let pod = job.spec.unwrap().template.spec.unwrap();
        let mounts = pod.containers[0].volume_mounts.clone().unwrap();
        assert!(mounts.iter().any(|mount| mount.mount_path == "/work"));
        let data = mounts
            .iter()
            .find(|mount| mount.mount_path == "/work/data")
            .unwrap();
        assert_eq!(data.name, "aruna-job-a1-data");
        assert_eq!(data.read_only, Some(false));
        let volume = pod
            .volumes
            .unwrap()
            .into_iter()
            .find(|volume| volume.name == "aruna-job-a1-data")
            .unwrap();
        assert_eq!(
            volume.persistent_volume_claim.unwrap().claim_name,
            "aruna-job-a1-data"
        );

        let pv = serde_json::to_value(
            data_pv_manifest(&context(), &config(), "workspace-bucket", "data/").unwrap(),
        )
        .unwrap();
        assert_eq!(pv["metadata"]["name"], "aruna-job-a1-data");
        assert_eq!(pv["metadata"]["labels"][ROLE_LABEL], "s3-mount");
        assert_eq!(pv["metadata"]["annotations"][EPOCH_ANNOTATION], "7");
        assert_eq!(
            pv["spec"]["mountOptions"],
            json!([
                "uid=65534",
                "gid=65534",
                "allow-other",
                "prefix=data/",
                "allow-delete",
                "allow-overwrite",
                "file-mode=0644",
                "dir-mode=0755"
            ])
        );
        assert_eq!(pv["spec"]["csi"]["readOnly"], false);
        assert_eq!(
            pv["spec"]["csi"]["volumeAttributes"]["bucketName"],
            "workspace-bucket"
        );
        assert_eq!(
            pv["spec"]["csi"]["nodePublishSecretRef"]["name"],
            "aruna-job-a1-env"
        );
        let pvc = serde_json::to_value(data_pvc_manifest(&context(), &config()).unwrap()).unwrap();
        assert_eq!(pvc["metadata"]["labels"][ROLE_LABEL], "s3-mount");
        assert_eq!(pvc["metadata"]["annotations"][EPOCH_ANNOTATION], "7");
        assert_eq!(pvc["spec"]["volumeName"], "aruna-job-a1-data");

        let secret = secret_manifest(&context(), &config(), &spec).unwrap();
        let data = secret.string_data.unwrap();
        assert_eq!(data["access_key_id"], "session-access");
        assert_eq!(data["secret_access_key"], "session-secret");
        assert_eq!(data["AWS_ACCESS_KEY_ID"], "session-access");
    }

    #[test]
    fn workspace_run_reaches() {
        // A batch run bound to its bucket gets the credential, endpoint and S3-only egress.
        let mut spec = session_spec();
        spec.session = false;
        let layout = StageLayout::from_spec(&spec).unwrap();
        let job =
            serde_json::to_value(job_manifest(&context(), &spec, &config(), &layout).unwrap())
                .unwrap();
        let template = &job["spec"]["template"];
        assert_eq!(
            template["metadata"]["labels"]["aruna-engine.org/network"],
            "s3"
        );
        let container = &template["spec"]["containers"][0];
        assert_eq!(
            container["envFrom"][0]["secretRef"]["name"],
            "aruna-job-a1-env"
        );
        let env = container["env"].as_array().unwrap();
        assert!(
            env.iter().any(|entry| entry["name"] == "AWS_ENDPOINT_URL"
                && entry["value"] == "https://s3.example")
        );
        assert!(
            env.iter()
                .any(|entry| entry["name"] == "ARUNA_WORKSPACE_BUCKET"
                    && entry["value"] == "workspace-bucket")
        );
        let volumes = template["spec"]["volumes"].as_array().unwrap();
        assert!(
            volumes
                .iter()
                .all(|volume| volume["name"] != "aruna-job-a1-data")
        );

        // An unbound batch run stays without any network.
        spec.workspace = None;
        spec.secret_env.clear();
        let job =
            serde_json::to_value(job_manifest(&context(), &spec, &config(), &layout).unwrap())
                .unwrap();
        let template = &job["spec"]["template"];
        assert_eq!(
            template["metadata"]["labels"]["aruna-engine.org/network"],
            "none"
        );
        assert!(
            template["spec"]["containers"][0]["envFrom"]
                .as_array()
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn mounts_session_kubedal() {
        // With KubeDAL the data volume never uses the driver wide Mountpoint endpoint.
        let spec = session_spec();
        let mut config = config();
        config.kubedal = Some(Box::new(crate::executor::config::KubedalMount {
            driver: "kubedal.arunaengine.org".to_string(),
            endpoint: "https://s3.node-2.example".to_string(),
            region: "us-east-1".to_string(),
            cache: None,
        }));
        let layout = StageLayout::from_spec(&spec).unwrap();
        let job = serde_json::to_value(job_manifest(&context(), &spec, &config, &layout).unwrap())
            .unwrap();
        let pod = &job["spec"]["template"]["spec"];
        let volume = pod["volumes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|volume| volume["name"] == "aruna-job-a1-data")
            .unwrap();
        assert!(volume.get("persistentVolumeClaim").is_none());
        assert_eq!(volume["csi"]["driver"], "kubedal.arunaengine.org");
        assert_eq!(volume["csi"]["readOnly"], false);
        let attributes = &volume["csi"]["volumeAttributes"];
        assert_eq!(attributes["dataNode"], "aruna-job-a1-data");
        assert_eq!(attributes["mount"], "fuse-read-write");
        assert_eq!(attributes["path"], "data/");
        let mounts = pod["containers"][0]["volumeMounts"].as_array().unwrap();
        assert!(
            mounts
                .iter()
                .any(|mount| mount["mountPath"] == "/work/data" && mount["readOnly"] == false)
        );
        let node = session_node_manifest(&context(), &config, "workspace-bucket").unwrap();
        assert_eq!(node["metadata"]["name"], "aruna-job-a1-data");
        assert_eq!(node["metadata"]["labels"][ROLE_LABEL], "s3-mount");
        assert_eq!(node["spec"]["read_only"], false);
        assert_eq!(node["spec"]["config"]["bucket"], "workspace-bucket");
        assert_eq!(
            node["spec"]["config"]["endpoint"],
            "https://s3.node-2.example"
        );
        assert_eq!(node["spec"]["secretRef"]["name"], "aruna-job-a1-env");
        let secret = secret_manifest(&context(), &config, &spec).unwrap();
        assert_eq!(
            secret.string_data.unwrap()["access_key_id"],
            "session-access"
        );

        // The whole bucket needs no path; KubeDAL alone also enables the mount.
        let mut spec = spec;
        spec.session_mount.as_mut().unwrap().prefix = String::new();
        config.s3_mount_driver = None;
        let job = serde_json::to_value(job_manifest(&context(), &spec, &config, &layout).unwrap())
            .unwrap();
        let volume = job["spec"]["template"]["spec"]["volumes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|volume| volume["name"] == "aruna-job-a1-data")
            .cloned()
            .unwrap();
        assert!(volume["csi"]["volumeAttributes"].get("path").is_none());
    }

    #[test]
    fn mounts_chosen_prefix() {
        // The submission picks the bucket slice and the folder it appears at;
        // the whole bucket needs no prefix option at all.
        let mut spec = session_spec();
        spec.session_mount = Some(SessionMount {
            prefix: "raw/2024/".to_string(),
            path: "/work/project/raw".to_string(),
        });
        let layout = StageLayout::from_spec(&spec).unwrap();
        let job = job_manifest(&context(), &spec, &config(), &layout).unwrap();
        let pod = job.spec.unwrap().template.spec.unwrap();
        let mounts = pod.containers[0].volume_mounts.clone().unwrap();
        let data = mounts
            .iter()
            .find(|mount| mount.name == "aruna-job-a1-data")
            .unwrap();
        assert_eq!(data.mount_path, "/work/project/raw");
        assert!(mounts.iter().all(|mount| mount.mount_path != "/work/data"));
        let pv = serde_json::to_value(
            data_pv_manifest(&context(), &config(), "workspace-bucket", "raw/2024/").unwrap(),
        )
        .unwrap();
        assert_eq!(pv["spec"]["mountOptions"][3], "prefix=raw/2024/");

        let whole = serde_json::to_value(
            data_pv_manifest(&context(), &config(), "workspace-bucket", "").unwrap(),
        )
        .unwrap();
        let options = whole["spec"]["mountOptions"].as_array().unwrap();
        assert!(
            options
                .iter()
                .all(|option| !option.as_str().unwrap().starts_with("prefix="))
        );
        assert!(options.iter().any(|option| option == "allow-delete"));

        // A session that recorded no mount keeps its plain scratch workdir.
        spec.session_mount = None;
        let job = job_manifest(&context(), &spec, &config(), &layout).unwrap();
        let pod = job.spec.unwrap().template.spec.unwrap();
        let mounts = pod.containers[0].volume_mounts.clone().unwrap();
        assert!(mounts.iter().all(|mount| mount.name != "aruna-job-a1-data"));
    }

    #[test]
    fn skips_data_mount() {
        // Without a CSI driver a session keeps its plain scratch workdir.
        let spec = session_spec();
        let mut config = config();
        config.s3_mount_driver = None;
        let layout = StageLayout::from_spec(&spec).unwrap();
        let job = job_manifest(&context(), &spec, &config, &layout).unwrap();
        let pod = job.spec.unwrap().template.spec.unwrap();
        let mounts = pod.containers[0].volume_mounts.clone().unwrap();
        assert!(mounts.iter().any(|mount| mount.mount_path == "/work"));
        assert!(mounts.iter().all(|mount| mount.mount_path != "/work/data"));
        assert!(
            pod.volumes
                .unwrap()
                .iter()
                .all(|volume| volume.name != "aruna-job-a1-data")
        );
        let secret = secret_manifest(&context(), &config, &spec).unwrap();
        assert!(!secret.string_data.unwrap().contains_key("access_key_id"));
    }

    #[test]
    fn mounts_scratch_dir() {
        // Mounted jobs get a writable working directory instead of a workspace.
        let mut spec = TaskSpec::new(context().attempt, "registry.example/task:latest");
        spec.staging_mode = StagingMode::S3Mount;
        spec.workdir = Some("/work".to_string());
        let layout = StageLayout::from_spec(&spec).unwrap();
        let job = job_manifest(&context(), &spec, &config(), &layout).unwrap();
        let pod = job.spec.unwrap().template.spec.unwrap();

        let mount = pod.containers[0]
            .volume_mounts
            .as_ref()
            .unwrap()
            .iter()
            .find(|mount| mount.mount_path == "/work")
            .unwrap();
        assert_eq!(mount.name, "scratch");
        assert_ne!(mount.read_only, Some(true));
        let volume = pod
            .volumes
            .unwrap()
            .into_iter()
            .find(|volume| volume.name == "scratch")
            .unwrap();
        assert!(volume.empty_dir.is_some());
    }

    #[test]
    fn files_workdir_writable() {
        // Staged jobs mount inputs read-only, so the workdir needs its own scratch.
        let mut spec = TaskSpec::new(context().attempt, "registry.example/task:latest");
        spec.staging_mode = StagingMode::Files;
        spec.workdir = Some("/work".to_string());
        spec.inputs.push(TaskInput::from_bytes(
            "/work/quickruns/hello.py",
            "print(1)",
        ));
        let layout = StageLayout::from_spec(&spec).unwrap();
        let job = job_manifest(&context(), &spec, &config(), &layout).unwrap();
        let pod = job.spec.unwrap().template.spec.unwrap();
        let mounts = pod.containers[0].volume_mounts.clone().unwrap();
        let scratch = mounts
            .iter()
            .find(|mount| mount.mount_path == "/work")
            .unwrap();
        assert_eq!(scratch.name, "scratch");
        assert_ne!(scratch.read_only, Some(true));
        let script = mounts
            .iter()
            .find(|mount| mount.mount_path == "/work/quickruns/hello.py")
            .unwrap();
        assert_eq!(script.read_only, Some(true));
    }

    #[test]
    fn selects_pull_policy() {
        let mut spec = TaskSpec::new(context().attempt, "registry.example/task:latest");
        let layout = StageLayout::from_spec(&spec).unwrap();
        let job = job_manifest(&context(), &spec, &config(), &layout).unwrap();
        let pod = job.spec.unwrap().template.spec.unwrap();
        assert_eq!(
            pod.containers[0].image_pull_policy.as_deref(),
            Some("Always")
        );

        spec.image = "registry.example/task@sha256:0000000000000000000000000000000000000000000000000000000000000000".to_string();
        let layout = StageLayout::from_spec(&spec).unwrap();
        let job = job_manifest(&context(), &spec, &config(), &layout).unwrap();
        let pod = job.spec.unwrap().template.spec.unwrap();
        assert_eq!(
            pod.containers[0].image_pull_policy.as_deref(),
            Some("IfNotPresent")
        );
    }

    #[test]
    fn labels_open_network() {
        let mut spec = TaskSpec::new(context().attempt, "registry.example/task:latest");
        spec.security.network = NetworkAccess::Open;
        let layout = StageLayout::from_spec(&spec).unwrap();
        let job = job_manifest(&context(), &spec, &config(), &layout).unwrap();

        assert_eq!(
            job.spec.unwrap().template.metadata.unwrap().labels.unwrap()["aruna-engine.org/network"],
            "task"
        );
    }

    #[test]
    fn allows_dns_port() {
        // A host-network resolver such as node-local DNS is neither a kube-system
        // pod nor a CIDR peer under Cilium, so the DNS rule carries no peer.
        let mut config = config();
        config.s3_cidrs.push("10.0.0.0/8".to_string());

        let policies = network_policies(&config).unwrap();
        let policy = serde_json::to_value(&policies[1]).unwrap();
        let s3 = &policy["spec"]["egress"][0];
        let dns = &policy["spec"]["egress"][1];

        assert_eq!(s3["to"][0]["ipBlock"]["cidr"], "10.0.0.0/8");
        assert_eq!(s3["ports"][0]["port"], 443);
        assert!(dns.get("to").is_none());
        assert_eq!(dns["ports"][0]["protocol"], "UDP");
        assert_eq!(dns["ports"][0]["port"], 53);
        assert_eq!(dns["ports"][1]["protocol"], "TCP");
        assert_eq!(dns["ports"][1]["port"], 53);
    }

    fn policy_file(dir: &Path, name: &str, text: &str) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, text).expect("write manifest");
        path
    }

    #[test]
    fn loads_policy_documents() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = policy_file(
            dir.path(),
            "policies.yaml",
            "---\napiVersion: cilium.io/v2\nkind: CiliumNetworkPolicy\nmetadata:\n  name: s3-ingress\n---\napiVersion: networking.k8s.io/v1\nkind: NetworkPolicy\nmetadata:\n  name: extra\n  namespace: compute\n",
        );
        let mut config = config();
        config.policy_manifests = vec![path.clone()];

        let manifests = policy_manifests(&config).expect("the documents load");

        assert_eq!(manifests.len(), 2);
        assert_eq!(manifests[0].source, path);
        assert_eq!(
            manifests[0].object.types.as_ref().expect("types").kind,
            "CiliumNetworkPolicy"
        );
        assert_eq!(
            manifests[0].object.metadata.namespace.as_deref(),
            Some("compute")
        );
        assert_eq!(manifests[1].object.metadata.name.as_deref(), Some("extra"));
    }

    #[test]
    fn refuses_foreign_namespace() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = policy_file(
            dir.path(),
            "foreign.yaml",
            "apiVersion: cilium.io/v2\nkind: CiliumNetworkPolicy\nmetadata:\n  name: s3\n  namespace: other\n",
        );
        let mut config = config();
        config.policy_manifests = vec![path];

        let error = policy_manifests(&config).expect_err("a foreign namespace is refused");

        let message = error.to_string();
        assert!(message.contains("foreign.yaml"), "{message}");
        assert!(message.contains("document 0"), "{message}");
        assert!(message.contains("namespace `compute`"), "{message}");
    }

    #[test]
    fn refuses_broken_document() {
        let dir = tempfile::tempdir().expect("temp dir");
        let missing_kind = policy_file(
            dir.path(),
            "kindless.yaml",
            "apiVersion: cilium.io/v2\nmetadata:\n  name: s3\n",
        );
        let missing_name = policy_file(
            dir.path(),
            "nameless.yaml",
            "apiVersion: cilium.io/v2\nkind: CiliumNetworkPolicy\nmetadata: {}\n",
        );
        let mut config = config();
        config.policy_manifests = vec![missing_kind];
        let kind_error =
            policy_manifests(&config).expect_err("a document without a kind is refused");
        config.policy_manifests = vec![missing_name];
        let name_error =
            policy_manifests(&config).expect_err("a document without a name is refused");

        let kind_error = kind_error.to_string();
        let name_error = name_error.to_string();
        assert!(kind_error.contains("kindless.yaml") && kind_error.contains("has no kind"));
        assert!(name_error.contains("nameless.yaml") && name_error.contains("metadata.name"));
    }

    #[test]
    fn refuses_unreadable_file() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut config = config();
        config.policy_manifests = vec![dir.path().join("missing.yaml")];

        let error = policy_manifests(&config).expect_err("a missing file is refused");

        assert!(error.to_string().contains("missing.yaml"), "{error}");
    }
}
