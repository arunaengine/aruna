//! Volumes that mount a node's embedded SMB or NFS share through the stock CSI drivers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::compute::{BackendError, FenceContext, NfsBindings, SessionMount, TaskSpec};
use k8s_openapi::api::core::v1::{PersistentVolume, PersistentVolumeClaim, PodSpec, Secret};
use serde_json::json;

use super::{
    annotations_base, credential_secret, labels, manifest_error, mount_secret_name, s3_pvc_manifest,
};
use crate::executor::config::{KubernetesConfig, ShareDriver};

pub const SMB_DRIVER: &str = "smb.csi.k8s.io";
pub const NFS_DRIVER: &str = "nfs.csi.k8s.io";

/// Where one attempt's share volume reads from. The host stays the same for the volume's
/// lifetime, so every client of the volume talks to one node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ShareSource {
    /// `//host/share`, below `folder` when it is not empty. The attempt's key logs in.
    Smb {
        host: String,
        share: String,
        folder: String,
    },
    /// `host:/m/<mount_id>`, a binding the node made for this attempt. The worker's mTLS
    /// identity logs in, so only isolated pods may use it.
    Nfs { host: String, mount_id: String },
}

impl ShareSource {
    /// The read-only view of one job's captured inputs, named like its view bucket.
    pub fn smb_inputs(host: &str, job_id: &str) -> Self {
        Self::Smb {
            host: host.to_string(),
            share: format!("in-{}", job_id.to_lowercase()),
            folder: String::new(),
        }
    }

    /// A notebook session's workspace bucket, below the session's key prefix.
    pub fn smb_session(host: &str, bucket: &str, mount: &SessionMount) -> Self {
        let folder = mount.prefix.strip_suffix('/').unwrap_or(&mount.prefix);
        Self::Smb {
            host: host.to_string(),
            share: bucket.to_string(),
            folder: folder.to_string(),
        }
    }
}

/// The share a run's mounted inputs come from, when a share takes them over. An SMB
/// share serves only the run's own input view; NFS needs the attempt's own binding.
pub fn input_source(
    config: &KubernetesConfig,
    spec: &TaskSpec,
    job_id: &str,
    buckets: &[String],
) -> Result<Option<ShareSource>, BackendError> {
    let Some(share) = config.share_inputs().filter(|_| !buckets.is_empty()) else {
        return Ok(None);
    };
    let source = match &share.driver {
        ShareDriver::Smb { .. } => ShareSource::smb_inputs(&share.host, job_id),
        ShareDriver::Nfs { .. } => nfs_source(&share.host, spec, false)?,
    };
    if let ShareSource::Smb { share, .. } = &source
        && buckets.iter().any(|bucket| bucket != share)
    {
        return Err(BackendError::InvalidSpec(format!(
            "SMB share mounts serve only the input view `{share}`"
        )));
    }
    Ok(Some(source))
}

/// The share a session's workspace data comes from, when a share takes it over. The
/// attempt's NFS binding already names the session's folder, so it ignores the prefix.
pub fn session_source(
    config: &KubernetesConfig,
    spec: &TaskSpec,
    bucket: &str,
    mount: &SessionMount,
) -> Result<Option<ShareSource>, BackendError> {
    let Some(share) = config.share_sessions() else {
        return Ok(None);
    };
    Ok(Some(match &share.driver {
        ShareDriver::Smb { .. } => ShareSource::smb_session(&share.host, bucket, mount),
        ShareDriver::Nfs { .. } => nfs_source(&share.host, spec, true)?,
    }))
}

/// The export of one attempt's own NFS binding. The worker's mTLS identity covers every
/// pod there, so a missing, malformed or reused binding is refused, never shared.
fn nfs_source(host: &str, spec: &TaskSpec, session: bool) -> Result<ShareSource, BackendError> {
    let NfsBindings {
        input,
        session: data,
    } = &spec.nfs_mounts;
    if input.is_some() && input == data {
        return Err(BackendError::InvalidSpec(
            "NFS input and session shares need distinct mount bindings".to_string(),
        ));
    }
    let (binding, what) = match session {
        true => (data, "session"),
        false => (input, "input"),
    };
    let mount_id = binding.as_deref().ok_or_else(|| {
        BackendError::InvalidSpec(format!(
            "NFS {what} share requires a per-attempt mount binding"
        ))
    })?;
    Ok(ShareSource::Nfs {
        host: host.to_string(),
        mount_id: plain_part(mount_id, "mount ID")?.to_string(),
    })
}

/// The CSI driver the configured share needs on the cluster.
pub fn share_driver(config: &KubernetesConfig) -> Option<&'static str> {
    config.share_mount.as_ref().map(|share| match share.driver {
        ShareDriver::Smb { .. } => SMB_DRIVER,
        ShareDriver::Nfs { .. } => NFS_DRIVER,
    })
}

/// Refuses a share setting the manifests could not use. Workers resolve the host by name:
/// an IPv6 literal does not fit the SMB source or the NFS server attribute.
pub fn check_config(config: &KubernetesConfig) -> Result<(), BackendError> {
    let Some(share) = &config.share_mount else {
        return Ok(());
    };
    if share.host.contains(':') {
        return Err(BackendError::InvalidSpec(format!(
            "share host `{}` must be a DNS name, not an IPv6 address",
            share.host
        )));
    }
    plain_part(&share.host, "host")?;
    match &share.driver {
        // Each attempt brings its own NFS bindings, which the volumes check.
        ShareDriver::Nfs { .. } => {}
        // A KubeDAL session takes the mount Secret that the SMB login of inputs needs.
        ShareDriver::Smb {
            inputs: true,
            sessions: false,
        } if config.kubedal.is_some() => {
            return Err(BackendError::InvalidSpec(
                "SMB input shares with KubeDAL need SMB session shares too".to_string(),
            ));
        }
        ShareDriver::Smb { .. } => {}
    }
    Ok(())
}

pub fn share_input_name(name: &str) -> String {
    format!("{name}-share-in")
}

pub fn share_data_name(name: &str) -> String {
    format!("{name}-share-data")
}

/// The volume of one attempt's share. It carries the `s3-mount` role, so the existing
/// cleanup removes it together with the attempt's other data mounts.
pub fn share_pv_manifest(
    context: &FenceContext,
    config: &KubernetesConfig,
    name: &str,
    source: &ShareSource,
    read_only: bool,
) -> Result<PersistentVolume, BackendError> {
    // Kubelet stages a CSI volume by its handle, so two attempts must never share one.
    let handle = format!("{name}-e{}", context.attempt_epoch);
    let (file_mode, dir_mode) = match read_only {
        true => ("0444", "0555"),
        false => ("0644", "0755"),
    };
    let (mount_options, csi) = match source {
        ShareSource::Smb {
            host,
            share,
            folder,
        } => {
            let mut attributes = json!({
                "source":format!("//{}/{}", plain_part(host, "host")?, plain_part(share, "share")?)
            });
            if !folder.is_empty() {
                attributes["subDir"] = json!(folder_path(folder)?);
            }
            // `seal` requires SMB 3 encryption; without `guest` the mount always logs in.
            let options = json!([
                "vers=3.1.1",
                "seal",
                "uid=65534",
                "gid=65534",
                format!("file_mode={file_mode}"),
                format!("dir_mode={dir_mode}")
            ]);
            let csi = json!({
                "driver":SMB_DRIVER,
                "volumeHandle":handle,
                "readOnly":read_only,
                "volumeAttributes":attributes,
                "nodeStageSecretRef":{
                    "name":mount_secret_name(&context.attempt.external_name()),
                    "namespace":config.namespace
                }
            });
            (options, csi)
        }
        ShareSource::Nfs { host, mount_id } => {
            let csi = json!({
                "driver":NFS_DRIVER,
                "volumeHandle":handle,
                "readOnly":read_only,
                "volumeAttributes":{
                    "server":plain_part(host, "host")?,
                    "share":format!("/m/{}", plain_part(mount_id, "mount ID")?)
                }
            });
            (json!(["nfsvers=4.1", "xprtsec=mtls"]), csi)
        }
    };
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
            "csi":csi
        }
    }))
    .map_err(manifest_error)
}

pub fn share_pvc_manifest(
    context: &FenceContext,
    config: &KubernetesConfig,
    name: &str,
) -> Result<PersistentVolumeClaim, BackendError> {
    s3_pvc_manifest(context, config, name)
}

/// The SMB login of one attempt: its own access key and secret under the driver's key
/// names, and none of the task env. It takes the mount Secret's name, which cleanup removes.
pub fn share_secret_manifest(
    context: &FenceContext,
    config: &KubernetesConfig,
    spec: &TaskSpec,
) -> Result<Secret, BackendError> {
    let mut values = BTreeMap::new();
    for (env, key) in [
        ("AWS_ACCESS_KEY_ID", "username"),
        ("AWS_SECRET_ACCESS_KEY", "password"),
    ] {
        let value = spec.secret_env.get(env).ok_or_else(|| {
            BackendError::InvalidSpec(format!("SMB share mounts need the attempt's {env}"))
        })?;
        values.insert(key.to_string(), value.expose().to_string());
    }
    let name = mount_secret_name(&context.attempt.external_name());
    credential_secret(context, config, &name, values)
}

/// Refuses a pod that could reach the worker's network, files or keys. A worker mTLS
/// identity covers every pod on that worker, so NFS mounts need this isolation.
pub fn check_isolation(pod: &PodSpec) -> Result<(), BackendError> {
    let pod = serde_json::to_value(pod).map_err(manifest_error)?;
    for field in ["hostNetwork", "hostPID", "hostIPC"] {
        if pod[field] == true {
            return Err(unsafe_pod(&format!("sets {field}")));
        }
    }
    if pod["automountServiceAccountToken"] != false {
        return Err(unsafe_pod("mounts a service account token"));
    }
    let mut volumes = pod["volumes"].as_array().into_iter().flatten();
    if volumes.any(|volume| !volume["hostPath"].is_null()) {
        return Err(unsafe_pod("mounts a host path"));
    }
    let root_denied = pod["securityContext"]["runAsNonRoot"] == true;
    let containers = ["initContainers", "containers"]
        .into_iter()
        .flat_map(|field| pod[field].as_array().into_iter().flatten());
    for container in containers {
        let security = &container["securityContext"];
        if security["privileged"] == true {
            return Err(unsafe_pod("runs a privileged container"));
        }
        if security["allowPrivilegeEscalation"] != false {
            return Err(unsafe_pod("allows privilege escalation"));
        }
        if security["capabilities"]["add"]
            .as_array()
            .is_some_and(|added| !added.is_empty())
        {
            return Err(unsafe_pod("adds capabilities"));
        }
        let non_root = match &security["runAsNonRoot"] {
            serde_json::Value::Bool(value) => *value,
            _ => root_denied,
        };
        if !non_root {
            return Err(unsafe_pod("may run as root"));
        }
    }
    Ok(())
}

fn unsafe_pod(detail: &str) -> BackendError {
    BackendError::InvalidSpec(format!("share mounts refuse a pod that {detail}"))
}

/// A host, share or mount ID: one plain name that cannot add a path or a mount option.
fn plain_part<'a>(value: &'a str, what: &str) -> Result<&'a str, BackendError> {
    let plain = |letter: char| letter.is_ascii_alphanumeric() || "-_.".contains(letter);
    if !value.chars().all(plain) || value.chars().all(|letter| letter == '.') {
        return Err(BackendError::InvalidSpec(format!(
            "share {what} `{value}` is not a plain name"
        )));
    }
    Ok(value)
}

/// A relative folder below the share root, without empty, `.` or `..` components.
fn folder_path(folder: &str) -> Result<&str, BackendError> {
    let bad = |part: &str| {
        matches!(part, "" | "." | "..")
            || part
                .chars()
                .any(|letter| letter == '\\' || letter.is_control())
    };
    if folder.split('/').any(bad) {
        return Err(BackendError::InvalidSpec(format!(
            "share folder `{folder}` is not a relative path"
        )));
    }
    Ok(folder)
}

#[cfg(test)]
#[path = "csi_tests.rs"]
mod tests;
