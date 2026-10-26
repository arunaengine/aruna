//! Tests the routes that change a distributed bucket: the admin checks, input refusals, record
//! refusals and changes this node publishes as the set's publisher.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::distributed::{BUCKET, Node, PEER, distribute, node_id, node_state};
use aruna_core::structs::storage::distribution::DistributionSnapshot;

fn bearer() -> Extension<Option<ValidatedBearer>> {
    Extension(Some(ValidatedBearer::new_for_test("caller-token")))
}

async fn distributed() -> (Node, DistributionSnapshot) {
    let node = node_state().await;
    let snapshot = distribute(&node.state, node.group_id, BUCKET).await;
    (node, snapshot)
}

async fn settings(
    node: &Node,
    auth: Option<&AuthContext>,
    set_id: Ulid,
    request: SettingsRequest,
) -> ServerResult<Json<ChangeResponse>> {
    settings_distributed(
        State(node.state.clone()),
        Extension(auth.cloned()),
        bearer(),
        Path((node.group_id, set_id)),
        Json(request),
    )
    .await
}

#[tokio::test]
async fn admin_checks_refuse() {
    let (node, snapshot) = distributed().await;
    let set_id = snapshot.set_id();
    let request = SettingsRequest::default();
    assert!(matches!(
        settings(&node, None, set_id, request.clone()).await,
        Err(ServerError::Unauthorized)
    ));
    assert!(matches!(
        settings(&node, Some(&node.stranger), set_id, request.clone()).await,
        Err(ServerError::Forbidden)
    ));
    assert!(matches!(
        settings(&node, Some(&node.owner), Ulid::nil(), request.clone()).await,
        Err(ServerError::NotFound)
    ));
}

#[tokio::test]
async fn publishes_changes() {
    let (node, snapshot) = distributed().await;
    let set_id = snapshot.set_id();
    let before = snapshot.revision().number;
    let request = SettingsRequest {
        refresh_secs: Some(600),
        range_behaviour: Some(BehaviourName::FillFirst),
        search_index: true,
        ..SettingsRequest::default()
    };
    let Json(changed) = settings(&node, Some(&node.owner), set_id, request)
        .await
        .unwrap();
    assert_eq!(changed.set_id, set_id.to_string());
    assert_eq!(changed.revision, before + 1);

    let (status, Json(deleted)) = delete_distributed(
        State(node.state.clone()),
        Extension(Some(node.owner.clone())),
        bearer(),
        Path((node.group_id, set_id)),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(deleted.revision, before + 2);
    // A set being deleted takes no settings change.
    let late = settings(&node, Some(&node.owner), set_id, SettingsRequest::default()).await;
    assert!(late.is_err());
}

#[tokio::test]
async fn member_changes_refused() {
    let (node, snapshot) = distributed().await;
    let set_id = snapshot.set_id();
    let leave = |member_id: Ulid| {
        leave_distributed(
            State(node.state.clone()),
            Extension(Some(node.owner.clone())),
            bearer(),
            Path((node.group_id, set_id, member_id)),
        )
    };
    // Member 2 coordinates the upper range and must transfer it before it leaves.
    let coordinator = snapshot.config.ranges[1].coordinator;
    assert!(matches!(
        leave(coordinator).await,
        Err(ServerError::BadRequestMessage(_))
    ));

    let join = |request: JoinRequest| {
        join_distributed(
            State(node.state.clone()),
            Extension(Some(node.owner.clone())),
            bearer(),
            Path((node.group_id, set_id)),
            Json(request),
        )
    };
    let request = JoinRequest {
        node_id: node_id([13; 32]).to_string(),
        coverage: CoverageName::Assigned,
        strategy: StrategyName::Copy,
        range_behaviour: Some(BehaviourName::Passthrough),
        ranges: vec![snapshot.config.ranges[0].range_id.to_string()],
    };
    // Only realm infrastructure nodes hold members.
    assert!(matches!(
        join(request.clone()).await,
        Err(ServerError::BadRequestMessage(_))
    ));
    let unknown = JoinRequest {
        node_id: "not-a-node".to_string(),
        ..request.clone()
    };
    assert!(matches!(
        join(unknown).await,
        Err(ServerError::BadRequestMessage(_))
    ));
    let bad_range = JoinRequest {
        ranges: vec!["not-a-range".to_string()],
        coverage: CoverageName::All,
        ..request
    };
    assert!(matches!(
        join(bad_range).await,
        Err(ServerError::BadRequestMessage(_))
    ));
}

#[tokio::test]
async fn transfers_refused() {
    let (node, snapshot) = distributed().await;
    let set_id = snapshot.set_id();
    let transfer = |request: TransferRequest| {
        transfer_distributed(
            State(node.state.clone()),
            Extension(Some(node.owner.clone())),
            bearer(),
            Path((node.group_id, set_id)),
            Json(request),
        )
    };
    let range = &snapshot.config.ranges[0];
    let peer = (snapshot.config.members.iter())
        .find(|member| member.node_id == node_id(PEER))
        .unwrap();
    // The peer keeps no copy of the lower range, so it cannot coordinate it.
    let request = TransferRequest {
        range_id: range.range_id.to_string(),
        to: peer.member_id.to_string(),
        role: RoleName::Leave,
    };
    let Err(ServerError::BadRequestMessage(message)) = transfer(request.clone()).await else {
        panic!("a member without a copy cannot take the range");
    };
    assert!(message.contains("eligible members"));
    let replica = TransferRequest {
        role: RoleName::Replica,
        to: "not-an-id".to_string(),
        ..request
    };
    assert!(matches!(
        transfer(replica).await,
        Err(ServerError::BadRequestMessage(_))
    ));
}
