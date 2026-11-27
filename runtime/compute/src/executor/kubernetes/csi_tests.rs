//! Manifest tests for SMB and NFS share volumes and the pod isolation they need.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::compute::{
    AttemptRef, NfsBindings, S3Mount, Secret as SecretValue, StagingMode, WorkspaceBinding,
};
use k8s_openapi::api::core::v1::PodSpec;

use super::super::job_manifest;
use super::*;
use crate::executor::config::{KubedalMount, ShareMount};
use crate::executor::staging::StageLayout;

fn context(attempt: u32, epoch: u64) -> FenceContext {
    FenceContext {
        attempt: AttemptRef::new("job", attempt),
        attempt_epoch: epoch,
        controller_generation: 3,
    }
}

fn config() -> KubernetesConfig {
    KubernetesConfig {
        namespace: "compute".to_string(),
        ..Default::default()
    }
}

fn pv_value(
    context: &FenceContext,
    name: &str,
    source: &ShareSource,
    read_only: bool,
) -> serde_json::Value {
    let pv = share_pv_manifest(context, &config(), name, source, read_only).unwrap();
    serde_json::to_value(pv).unwrap()
}

fn task_pod() -> PodSpec {
    let spec = TaskSpec::new(context(1, 7).attempt, "registry.example/task:latest");
    let layout = StageLayout::from_spec(&spec).unwrap();
    let job = job_manifest(&context(1, 7), &spec, &config(), &layout).unwrap();
    job.spec.unwrap().template.spec.unwrap()
}

#[test]
fn pins_smb_inputs() {
    let context = context(1, 7);
    let name = share_input_name(&context.attempt.external_name());
    let source = ShareSource::smb_inputs("node-a.example", "job");
    let pv = pv_value(&context, &name, &source, true);
    let csi = &pv["spec"]["csi"];
    assert_eq!(csi["driver"], SMB_DRIVER);
    assert_eq!(csi["readOnly"], true);
    assert_eq!(csi["volumeAttributes"]["source"], "//node-a.example/in-job");
    assert!(csi["volumeAttributes"]["subDir"].is_null());
    assert_eq!(csi["nodeStageSecretRef"]["name"], "aruna-job-a1-mount");
    assert_eq!(csi["nodeStageSecretRef"]["namespace"], "compute");
    let options = pv["spec"]["mountOptions"].as_array().unwrap();
    for option in ["vers=3.1.1", "seal", "file_mode=0444", "dir_mode=0555"] {
        assert!(options.contains(&json!(option)), "missing {option}");
    }
    assert!(
        !options
            .iter()
            .any(|option| option.as_str().is_some_and(|text| text.contains("guest"))),
        "an SMB share never mounts as guest"
    );
    // Existing cleanup finds the volume and its claim by the shared data mount role.
    assert_eq!(
        pv["metadata"]["labels"]["aruna-engine.org/role"],
        "s3-mount"
    );
    let pvc = share_pvc_manifest(&context, &config(), &name).unwrap();
    assert_eq!(
        pvc.spec.unwrap().volume_name.as_deref(),
        Some(name.as_str())
    );

    let mut spec = TaskSpec::new(context.attempt.clone(), "registry.example/task:latest");
    spec.secret_env.insert(
        "AWS_ACCESS_KEY_ID".to_string(),
        SecretValue::new("attempt-access"),
    );
    spec.secret_env.insert(
        "AWS_SECRET_ACCESS_KEY".to_string(),
        SecretValue::new("attempt-secret"),
    );
    spec.secret_env
        .insert("TOKEN".to_string(), SecretValue::new("task-only"));
    let secret = share_secret_manifest(&context, &config(), &spec).unwrap();
    assert_eq!(secret.metadata.name.as_deref(), Some("aruna-job-a1-mount"));
    let values = secret.string_data.unwrap();
    assert_eq!(values.len(), 2, "the mount Secret holds only the login");
    assert_eq!(values["username"], "attempt-access");
    assert_eq!(values["password"], "attempt-secret");

    spec.secret_env.remove("AWS_SECRET_ACCESS_KEY");
    assert!(share_secret_manifest(&context, &config(), &spec).is_err());
}

#[test]
fn maps_session_folder() {
    let context = context(1, 7);
    let name = share_data_name(&context.attempt.external_name());
    let mount = SessionMount {
        prefix: "raw/2024/".to_string(),
        path: "/work/raw".to_string(),
    };
    let source = ShareSource::smb_session("node-a.example", "notebooks", &mount);
    let pv = pv_value(&context, &name, &source, false);
    let csi = &pv["spec"]["csi"];
    assert_eq!(csi["readOnly"], false);
    assert_eq!(
        csi["volumeAttributes"]["source"],
        "//node-a.example/notebooks"
    );
    assert_eq!(csi["volumeAttributes"]["subDir"], "raw/2024");
    let options = pv["spec"]["mountOptions"].as_array().unwrap();
    for option in ["vers=3.1.1", "seal", "file_mode=0644", "dir_mode=0755"] {
        assert!(options.contains(&json!(option)), "missing {option}");
    }

    // An empty prefix mounts the whole bucket.
    let whole = SessionMount {
        prefix: String::new(),
        path: "/work".to_string(),
    };
    let source = ShareSource::smb_session("node-a.example", "notebooks", &whole);
    let pv = pv_value(&context, &name, &source, false);
    assert!(pv["spec"]["csi"]["volumeAttributes"]["subDir"].is_null());

    for prefix in ["../other/", "a//", "/abs/", "a/./b/"] {
        let mount = SessionMount {
            prefix: prefix.to_string(),
            path: "/work".to_string(),
        };
        let source = ShareSource::smb_session("node-a.example", "notebooks", &mount);
        assert!(
            share_pv_manifest(&context, &config(), &name, &source, false).is_err(),
            "prefix {prefix} must be refused"
        );
    }
}

#[test]
fn pins_nfs_export() {
    let context = context(1, 7);
    let name = share_input_name(&context.attempt.external_name());
    let source = ShareSource::Nfs {
        host: "node-a.example".to_string(),
        mount_id: "Zx81mQ".to_string(),
    };
    let pv = pv_value(&context, &name, &source, true);
    let csi = &pv["spec"]["csi"];
    assert_eq!(csi["driver"], NFS_DRIVER);
    assert_eq!(csi["volumeAttributes"]["server"], "node-a.example");
    assert_eq!(csi["volumeAttributes"]["share"], "/m/Zx81mQ");
    assert!(
        csi["nodeStageSecretRef"].is_null(),
        "NFS logs in with worker mTLS"
    );
    assert_eq!(
        pv["spec"]["mountOptions"],
        json!(["nfsvers=4.1", "xprtsec=mtls"])
    );

    for (host, mount_id) in [
        ("node-a.example", "../x"),
        ("node-a.example", ""),
        ("a,b", "id"),
        ("..", "id"),
    ] {
        let source = ShareSource::Nfs {
            host: host.to_string(),
            mount_id: mount_id.to_string(),
        };
        assert!(share_pv_manifest(&context, &config(), &name, &source, true).is_err());
    }
}

#[test]
fn distinct_attempt_handles() {
    let source = ShareSource::smb_inputs("node-a.example", "job");
    let handle = |attempt, epoch| {
        let context = context(attempt, epoch);
        let name = share_input_name(&context.attempt.external_name());
        pv_value(&context, &name, &source, true)["spec"]["csi"]["volumeHandle"].clone()
    };
    let first = handle(1, 7);
    assert_ne!(first, handle(2, 7), "a retry gets its own volume handle");
    assert_ne!(
        first,
        handle(1, 8),
        "a fenced restart gets its own volume handle"
    );
    assert_eq!(first, handle(1, 7));
}

#[test]
fn admits_task_pod() {
    // The standard task pod already meets the isolation an NFS mount relies on.
    check_isolation(&task_pod()).unwrap();
}

#[test]
fn refuses_unsafe_pods() {
    let unsafe_pods: [fn(&mut serde_json::Value); 7] = [
        |pod| pod["hostNetwork"] = json!(true),
        |pod| pod["hostPID"] = json!(true),
        |pod| pod["automountServiceAccountToken"] = json!(true),
        |pod| {
            pod["volumes"] = json!([{"name":"keys","hostPath":{"path":"/etc/tlshd"}}]);
        },
        |pod| pod["containers"][0]["securityContext"]["privileged"] = json!(true),
        |pod| pod["containers"][0]["securityContext"]["allowPrivilegeEscalation"] = json!(true),
        |pod| pod["containers"][0]["securityContext"]["runAsNonRoot"] = json!(false),
    ];
    let base = serde_json::to_value(task_pod()).unwrap();
    for (index, change) in unsafe_pods.iter().enumerate() {
        let mut pod = base.clone();
        change(&mut pod);
        let pod: PodSpec = serde_json::from_value(pod).unwrap();
        assert!(
            check_isolation(&pod).is_err(),
            "unsafe pod {index} was admitted"
        );
    }
    let mut pod = base;
    pod["containers"][0]["securityContext"]["capabilities"]["add"] = json!(["NET_ADMIN"]);
    let pod: PodSpec = serde_json::from_value(pod).unwrap();
    assert!(check_isolation(&pod).is_err());
}

fn share_config(driver: ShareDriver) -> KubernetesConfig {
    KubernetesConfig {
        share_mount: Some(Box::new(ShareMount {
            host: "node-a.example".to_string(),
            driver,
        })),
        ..config()
    }
}

fn input_spec(bucket: &str) -> TaskSpec {
    let mut spec = TaskSpec::new(context(1, 7).attempt, "registry.example/task:latest");
    spec.staging_mode = StagingMode::S3Mount;
    spec.s3_mounts.push(S3Mount {
        bucket: bucket.to_string(),
        key: "data/input.txt".to_string(),
        path: "/input.txt".to_string(),
        cached: false,
    });
    for (env, value) in [
        ("AWS_ACCESS_KEY_ID", "attempt-access"),
        ("AWS_SECRET_ACCESS_KEY", "attempt-secret"),
    ] {
        spec.secret_env
            .insert(env.to_string(), SecretValue::new(value));
    }
    spec
}

fn session_spec(mount: &SessionMount) -> TaskSpec {
    let mut spec = TaskSpec::new(context(1, 7).attempt, "registry.example/session:latest");
    spec.session = true;
    spec.workspace = Some(WorkspaceBinding {
        s3_endpoint: "https://s3.example".to_string(),
        bucket_name: "notebooks".to_string(),
        region: "us-east-1".to_string(),
    });
    spec.session_mount = Some(mount.clone());
    spec
}

fn raw_mount() -> SessionMount {
    SessionMount {
        prefix: "raw/".to_string(),
        path: "/work/raw".to_string(),
    }
}

fn nfs_bindings(input: &str, session: &str) -> NfsBindings {
    NfsBindings {
        input: Some(input.to_string()),
        session: Some(session.to_string()),
    }
}

fn pod_value(
    config: &KubernetesConfig,
    spec: &TaskSpec,
) -> Result<serde_json::Value, BackendError> {
    let layout = StageLayout::from_spec(spec).unwrap();
    let job = job_manifest(&context(1, 7), spec, config, &layout)?;
    Ok(serde_json::to_value(job).unwrap()["spec"]["template"]["spec"].clone())
}

fn volume<'a>(pod: &'a serde_json::Value, name: &str) -> Option<&'a serde_json::Value> {
    let mut volumes = pod["volumes"].as_array().into_iter().flatten();
    volumes.find(|volume| volume["name"] == name)
}

fn mount_at<'a>(pod: &'a serde_json::Value, path: &str) -> &'a serde_json::Value {
    let mut mounts = pod["containers"][0]["volumeMounts"]
        .as_array()
        .into_iter()
        .flatten();
    mounts.find(|mount| mount["mountPath"] == path).unwrap()
}

#[test]
fn mounts_smb_inputs() {
    let config = share_config(ShareDriver::Smb {
        inputs: true,
        sessions: false,
    });
    let pod = pod_value(&config, &input_spec("in-job")).unwrap();
    let share = volume(&pod, "aruna-job-a1-share-in").unwrap();
    assert_eq!(
        share["persistentVolumeClaim"]["claimName"],
        "aruna-job-a1-share-in"
    );
    assert!(volume(&pod, "aruna-job-a1-s3-0").is_none());
    let mount = mount_at(&pod, "/input.txt");
    assert_eq!(mount["name"], "aruna-job-a1-share-in");
    assert_eq!(mount["subPath"], "data/input.txt");
    assert_eq!(mount["readOnly"], true);

    // The share takes the view bucket's name, which lowercases the job ID.
    let spec = input_spec("in-job");
    let source = input_source(&config, &spec, "JOB", &["in-job".to_string()]).unwrap();
    assert_eq!(
        source,
        Some(ShareSource::smb_inputs("node-a.example", "job"))
    );
    assert_eq!(share_driver(&config), Some(SMB_DRIVER));
    // An SMB share serves only the run's own input view.
    assert!(pod_value(&config, &input_spec("other-bucket")).is_err());
}

#[test]
fn mounts_smb_session() {
    let config = share_config(ShareDriver::Smb {
        inputs: false,
        sessions: true,
    });
    let mount = raw_mount();
    let spec = session_spec(&mount);
    let pod = pod_value(&config, &spec).unwrap();
    let share = volume(&pod, "aruna-job-a1-share-data").unwrap();
    assert_eq!(
        share["persistentVolumeClaim"]["claimName"],
        "aruna-job-a1-share-data"
    );
    assert!(volume(&pod, "aruna-job-a1-data").is_none());
    let data = mount_at(&pod, "/work/raw");
    assert_eq!(data["name"], "aruna-job-a1-share-data");
    assert_eq!(data["readOnly"], false);
    assert_eq!(
        session_source(&config, &spec, "notebooks", &mount).unwrap(),
        Some(ShareSource::smb_session(
            "node-a.example",
            "notebooks",
            &mount
        ))
    );
    // Inputs stay off the share when only sessions use it.
    assert_eq!(input_source(&config, &spec, "job", &[]).unwrap(), None);
}

#[test]
fn mounts_nfs_inputs() {
    let config = share_config(ShareDriver::Nfs {
        host_id: ulid::Ulid::from_bytes([9; 16]),
        inputs: true,
        sessions: false,
    });
    let mut spec = input_spec("in-job");
    spec.nfs_mounts.input = Some("Zx81mQ".to_string());
    // The NFS binding names its own target, so the manifest maps the keys below it.
    let pod = pod_value(&config, &spec).unwrap();
    assert!(volume(&pod, "aruna-job-a1-share-in").is_some());
    assert_eq!(mount_at(&pod, "/input.txt")["subPath"], "data/input.txt");
    let pod: PodSpec = serde_json::from_value(pod).unwrap();
    check_isolation(&pod).unwrap();
    assert_eq!(
        input_source(&config, &spec, "job", &["in-job".to_string()]).unwrap(),
        Some(ShareSource::Nfs {
            host: "node-a.example".to_string(),
            mount_id: "Zx81mQ".to_string(),
        })
    );
    assert_eq!(share_driver(&config), Some(NFS_DRIVER));
}

#[test]
fn keeps_default_mounts() {
    assert_eq!(KubernetesConfig::default().share_mount, None);
    let mut config = config();
    config.s3_mount_driver = Some("s3.csi.example.org".to_string());
    let pod = pod_value(&config, &input_spec("in-job")).unwrap();
    let s3 = volume(&pod, "aruna-job-a1-s3-0").unwrap();
    assert_eq!(
        s3["persistentVolumeClaim"]["claimName"],
        "aruna-job-a1-s3-0"
    );
    assert!(volume(&pod, "aruna-job-a1-share-in").is_none());
    let spec = input_spec("in-job");
    assert_eq!(input_source(&config, &spec, "job", &[]).unwrap(), None);
    assert_eq!(share_driver(&config), None);

    // A share for sessions only leaves inputs on the S3 driver.
    config.share_mount = share_config(ShareDriver::Smb {
        inputs: false,
        sessions: true,
    })
    .share_mount;
    let pod = pod_value(&config, &input_spec("in-job")).unwrap();
    assert!(volume(&pod, "aruna-job-a1-s3-0").is_some());
    assert!(volume(&pod, "aruna-job-a1-share-in").is_none());
}

#[test]
fn checks_share_config() {
    let smb = |inputs, sessions| share_config(ShareDriver::Smb { inputs, sessions });
    check_config(&config()).unwrap();
    check_config(&smb(true, false)).unwrap();
    for host in ["fd00::1", "[fd00::1]", "a/b", ""] {
        let mut config = smb(true, true);
        config.share_mount.as_mut().unwrap().host = host.to_string();
        assert!(
            check_config(&config).is_err(),
            "host {host} must be refused"
        );
    }
    let nfs = share_config(ShareDriver::Nfs {
        host_id: ulid::Ulid::from_bytes([9; 16]),
        inputs: true,
        sessions: true,
    });
    check_config(&nfs).unwrap();

    // A KubeDAL session would take the mount Secret the SMB input login needs.
    let mut kubedal = smb(true, false);
    kubedal.kubedal = Some(Box::new(KubedalMount {
        driver: "kubedal.arunaengine.org".to_string(),
        endpoint: "https://s3.example".to_string(),
        region: "us-east-1".to_string(),
        cache: None,
    }));
    assert!(check_config(&kubedal).is_err());
    kubedal.share_mount = smb(true, true).share_mount;
    check_config(&kubedal).unwrap();
}

/// The export and volume handle of one attempt's NFS share volume.
fn nfs_volume(
    context: &FenceContext,
    name: &str,
    source: &ShareSource,
    read_only: bool,
) -> (String, String) {
    let csi = pv_value(context, name, source, read_only)["spec"]["csi"].clone();
    assert_eq!(csi["readOnly"], read_only);
    let text = |value: &serde_json::Value| value.as_str().unwrap().to_string();
    (
        text(&csi["volumeAttributes"]["share"]),
        text(&csi["volumeHandle"]),
    )
}

#[test]
fn isolates_nfs_attempts() {
    let config = share_config(ShareDriver::Nfs {
        host_id: ulid::Ulid::from_bytes([9; 16]),
        inputs: true,
        sessions: true,
    });
    let mount = raw_mount();
    let buckets = ["in-job".to_string()];
    let (mut exports, mut handles) = (BTreeSet::new(), BTreeSet::new());
    // Two attempts run on one worker at once, each with its own bindings.
    for (attempt, input, session) in [(1, "in-one", "ws-one"), (2, "in-two", "ws-two")] {
        let context = context(attempt, 7);
        let mut spec = session_spec(&mount);
        spec.attempt = context.attempt.clone();
        spec.nfs_mounts = nfs_bindings(input, session);
        let name = context.attempt.external_name();
        let inputs = input_source(&config, &spec, "job", &buckets)
            .unwrap()
            .unwrap();
        let data = session_source(&config, &spec, "notebooks", &mount)
            .unwrap()
            .unwrap();
        let (export, handle) = nfs_volume(&context, &share_input_name(&name), &inputs, true);
        assert_eq!(export, format!("/m/{input}"));
        exports.insert(export);
        handles.insert(handle);
        let (export, handle) = nfs_volume(&context, &share_data_name(&name), &data, false);
        assert_eq!(export, format!("/m/{session}"));
        exports.insert(export);
        handles.insert(handle);
    }
    // Neither the read-only inputs nor the writable workspaces share an export or handle.
    assert_eq!((exports.len(), handles.len()), (4, 4));
}

#[test]
fn refuses_unbound_nfs() {
    let config = share_config(ShareDriver::Nfs {
        host_id: ulid::Ulid::from_bytes([9; 16]),
        inputs: true,
        sessions: true,
    });
    let mount = raw_mount();
    let buckets = ["in-job".to_string()];
    let refused = |bindings: NfsBindings| {
        let mut spec = session_spec(&mount);
        spec.nfs_mounts = bindings;
        let input = input_source(&config, &spec, "job", &buckets);
        let data = session_source(&config, &spec, "notebooks", &mount);
        matches!(input, Err(BackendError::InvalidSpec(_)))
            && matches!(data, Err(BackendError::InvalidSpec(_)))
    };
    assert!(refused(NfsBindings::default()));
    assert!(refused(nfs_bindings("", ".")));
    assert!(refused(nfs_bindings("../x", "a,ro")));
    // One export serving both mounts would let the workspace reach the inputs.
    assert!(refused(nfs_bindings("shared", "shared")));
    // The manifests refuse the attempt rather than mount a missing binding.
    for spec in [input_spec("in-job"), session_spec(&mount)] {
        let error = pod_value(&config, &spec).unwrap_err();
        assert!(matches!(error, BackendError::InvalidSpec(text) if text.contains("per-attempt")));
    }
}
