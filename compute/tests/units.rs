use std::collections::BTreeMap;

use aruna_compute::spec::{AttemptRef, Secret, TaskSpec, WorkspaceBinding};
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
fn secret_roundtrip() {
    // Secrets serialize transparently so a plan owner can persist them.
    let mut secret_env = BTreeMap::new();
    secret_env.insert("AWS_SECRET_ACCESS_KEY".to_string(), Secret::new("shh"));

    let json = serde_json::to_string(&secret_env).unwrap();
    assert_eq!(json, r#"{"AWS_SECRET_ACCESS_KEY":"shh"}"#);
    let back: BTreeMap<String, Secret> = serde_json::from_str(&json).unwrap();
    assert_eq!(back, secret_env);
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
