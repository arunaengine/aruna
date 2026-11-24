//! Tests the workflow tools through the MCP server: listing, refused bodies and unknown runs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::mcp::caller;
use crate::tests::distributed::node_state;

fn code(result: CallToolResult) -> serde_json::Value {
    result.structured_content.expect("a structured error")["code"].clone()
}

// The tools answer what the REST routes answer: a list, a refused body with the field it
// misses, and an unknown run that is not found.
#[tokio::test]
async fn serves_workflow_tools() {
    let node = node_state().await;
    let server = McpServer::new(node.state.clone());
    let owner = caller(&node.owner);

    let input = ListWorkflowsInput {
        group_id: Some(node.group_id.to_string()),
        q: None,
        cursor: None,
    };
    let listed = server.list_workflows(Extension(owner.clone()), Parameters(input));
    let listed = listed.await.unwrap().0.0;
    assert_eq!(listed["items"], json!([]), "{listed}");

    let body = BodyInput {
        body: JsonPayload(json!({"group_id": node.group_id.to_string()})),
    };
    let refused = server.start_workflow_run(Extension(owner.clone()), Parameters(body));
    let Err(refused) = refused.await else {
        panic!("a body without workflow_id is refused");
    };
    assert_eq!(code(refused.clone()), "Bad request");
    let text = format!("{:?}", refused.content);
    assert!(text.contains("workflow_id"), "{text}");

    let input = RunInput {
        run_id: ulid::Ulid::from_parts(9, 9).to_string(),
    };
    let missing = server.get_workflow_run(Extension(owner), Parameters(input));
    let Err(missing) = missing.await else {
        panic!("an unknown run is not found");
    };
    assert_eq!(code(missing), "Not found");
}
