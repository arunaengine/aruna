//! Tests how a WES run request maps to a workflow run request.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

fn form(url: &str, parameters: Option<Value>) -> WesRunRequest {
    WesRunRequest {
        workflow_url: url.to_owned(),
        workflow_params: Some(Value::String(r#"{"message": "Hola"}"#.to_owned())),
        workflow_engine_parameters: parameters,
        ..WesRunRequest::default()
    }
}

// A version url, bare or behind the node's address, with form text or objects, maps to the
// workflow, version, values and engine parameters of a run request.
#[test]
fn maps_wes_requests() {
    let parameters = json!({"group_id": "01JABCDEF0123456789ABCDEFG",
        "output": {"bucket": "results", "prefix": "hello/"}});
    let text = Some(Value::String(parameters.to_string()));
    let url =
        "https://node.example.test/api/compute/workflows/01K8W1B3D5F7H9KMQ1S3V5X7Z2/versions/2/";
    let request = workflow_request(form(url, text)).unwrap();
    assert_eq!(request.workflow_id, "01K8W1B3D5F7H9KMQ1S3V5X7Z2");
    assert_eq!(request.version, "2");
    assert_eq!(request.values, json!({"message": "Hola"}));
    assert_eq!(request.output.unwrap().bucket, "results");
    let bare = form(
        "01K8W1B3D5F7H9KMQ1S3V5X7Z2/versions/1",
        Some(parameters.clone()),
    );
    assert_eq!(workflow_request(bare).unwrap().version, "1");

    for refused in [
        form("01K8W1B3D5F7H9KMQ1S3V5X7Z2", Some(parameters.clone())),
        form("01K8W1B3D5F7H9KMQ1S3V5X7Z2/versions/1", None),
        form("01K8W1B3D5F7H9KMQ1S3V5X7Z2/versions/1", Some(json!("[1]"))),
    ] {
        let error = workflow_request(refused).unwrap_err();
        assert!(matches!(error, ServerError::BadRequestReason(_)), "{error}");
    }
}
