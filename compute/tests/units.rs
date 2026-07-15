use std::collections::BTreeMap;
use std::time::Duration;

use aruna_compute::spec::{
    AttemptRef, LogLimits, ResourceRequest, Secret, TaskSpec, WorkspaceBinding,
};
use aruna_compute::status::{AttemptPhase, AttemptStatus, TesState};

#[test]
fn external_name() {
    // Deterministic, lowercased, and the reconciliation key.
    let a = AttemptRef::new("Job-XYZ", 3);
    assert_eq!(a.external_name(), "aruna-job-xyz-a3");
    assert!(AttemptRef::new("ok.name-1", 0).validate().is_ok());
    assert!(AttemptRef::new("bad name", 0).validate().is_err());
    assert!(AttemptRef::new("", 0).validate().is_err());
    // Mixed case is rejected: `JobA` and `joba` would collide on one container
    // name, breaking adopt-by-name.
    assert!(AttemptRef::new("Job-XYZ", 0).validate().is_err());
}

#[test]
fn spec_roundtrip() {
    // Full TaskSpec including secrets survives a serde round-trip.
    let mut env = BTreeMap::new();
    env.insert("FOO".to_string(), "bar".to_string());
    let mut secret_env = BTreeMap::new();
    secret_env.insert("AWS_SECRET_ACCESS_KEY".to_string(), Secret::new("shh"));

    let spec = TaskSpec {
        attempt: AttemptRef::new("j1", 1),
        image: "alpine:3.20".to_string(),
        entrypoint: Some(vec!["/bin/sh".into()]),
        command: vec!["sh".into(), "-c".into(), "true".into()],
        workdir: Some("/work".to_string()),
        inputs: Vec::new(),
        output_paths: Vec::new(),
        env,
        secret_env,
        resources: ResourceRequest {
            cpu_cores: Some(2),
            ram_bytes: Some(1 << 30),
            disk_bytes: None,
            max_walltime: Some(Duration::from_secs(600)),
            preemptible: true,
            backend_extensions: BTreeMap::new(),
        },
        workspace: Some(WorkspaceBinding {
            s3_endpoint: "http://s3:9000".to_string(),
            bucket_name: "ws-j1".to_string(),
            region: "us-east-1".to_string(),
        }),
        log_limits: LogLimits::default(),
    };

    let json = serde_json::to_string(&spec).unwrap();
    let back: TaskSpec = serde_json::from_str(&json).unwrap();
    assert_eq!(spec, back);
}

#[test]
fn secret_redacts() {
    // Debug never leaks the secret, expose() returns it.
    let s = Secret::new("topsecret");
    assert_eq!(format!("{s:?}"), "Secret(***)");
    assert_eq!(s.expose(), "topsecret");
}

#[test]
fn effective_env() {
    // Workspace binding and secrets are folded into the injected env.
    let mut spec = TaskSpec::new(AttemptRef::new("j1", 0), "alpine");
    spec.workspace = Some(WorkspaceBinding {
        s3_endpoint: "http://s3:9000".to_string(),
        bucket_name: "ws-j1".to_string(),
        region: "eu".to_string(),
    });
    spec.secret_env
        .insert("AWS_SECRET_ACCESS_KEY".to_string(), Secret::new("k"));
    let env = spec.effective_env();
    assert_eq!(env.get("AWS_ENDPOINT_URL").unwrap(), "http://s3:9000");
    assert_eq!(env.get("ARUNA_WORKSPACE_BUCKET").unwrap(), "ws-j1");
    assert_eq!(env.get("ARUNA_JOB_ID").unwrap(), "j1");
    assert_eq!(env.get("AWS_SECRET_ACCESS_KEY").unwrap(), "k");
}

#[test]
fn status_mapping() {
    // Phase -> TES external state per the plan's mapping table.
    assert_eq!(AttemptPhase::Submitted.tes_state(), TesState::Initializing);
    assert_eq!(AttemptPhase::Running.tes_state(), TesState::Running);
    assert_eq!(
        AttemptPhase::Exited { code: 0 }.tes_state(),
        TesState::Complete
    );
    assert_eq!(
        AttemptPhase::Exited { code: 42 }.tes_state(),
        TesState::ExecutorError
    );
    assert_eq!(
        AttemptPhase::Failed { reason: "x".into() }.tes_state(),
        TesState::SystemError
    );
    assert_eq!(AttemptPhase::Cancelled.tes_state(), TesState::Canceled);
}

#[test]
fn terminal_flags() {
    // Only exit/failed/cancelled are terminal evidence.
    assert!(!AttemptPhase::Submitted.is_terminal());
    assert!(!AttemptPhase::Running.is_terminal());
    assert!(AttemptPhase::Exited { code: 1 }.is_terminal());
    assert!(AttemptPhase::Cancelled.is_terminal());
    let st = AttemptStatus {
        phase: AttemptPhase::Exited { code: 0 },
        backend_ref: "abc".into(),
        started_at_ms: None,
        finished_at_ms: None,
    };
    assert!(st.is_terminal());
}
