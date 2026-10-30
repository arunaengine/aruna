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
    let Json(changed) = settings(&node, Some(&node.owner), set_id, request.clone())
        .await
        .unwrap();
    assert_eq!(changed.set_id, set_id.to_string());
    assert_eq!(changed.revision, before + 1);
    // The status reads back every setting, unset ones as null, in the shape the change takes.
    let Json(read) = crate::routes::storage::distributed::get_distributed(
        State(node.state.clone()),
        Extension(Some(node.owner.clone())),
        Path((node.group_id, set_id)),
    )
    .await
    .unwrap();
    assert_eq!(read.settings, request);
    assert_eq!(
        serde_json::to_value(&read).unwrap()["settings"],
        serde_json::json!({
            "refresh_secs": 600,
            "range_behaviour": "fill_first",
            "search_index": true,
            "public": false,
            "split_objects": null,
            "split_bytes": null
        })
    );

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

async fn status(node: &Node, set_id: Ulid) -> super::super::distributed::DistributedResponse {
    let Json(read) = crate::routes::storage::distributed::get_distributed(
        State(node.state.clone()),
        Extension(Some(node.owner.clone())),
        Path((node.group_id, set_id)),
    )
    .await
    .unwrap();
    read
}

fn peer_member(snapshot: &DistributionSnapshot) -> Ulid {
    let peer = snapshot.config.members.iter();
    peer.map(|member| (member.node_id, member.member_id))
        .find(|(node, _)| *node == node_id(PEER))
        .map(|(_, member_id)| member_id)
        .unwrap()
}

#[tokio::test]
async fn copy_waits_fill() {
    let (node, snapshot) = distributed().await;
    let set_id = snapshot.set_id();
    let copy = |auth: &AuthContext, request: CopyRequest| {
        copy_distributed(
            State(node.state.clone()),
            Extension(Some(auth.clone())),
            bearer(),
            Path((node.group_id, set_id)),
            Json(request),
        )
    };
    let (lower, peer) = (snapshot.config.ranges[0].range_id, peer_member(&snapshot));
    let request = CopyRequest {
        range_id: lower.to_string(),
        member_id: peer.to_string(),
    };
    let stranger = copy(&node.stranger, request.clone()).await;
    assert!(matches!(stranger, Err(ServerError::Forbidden)));
    // The coordinator keeps the range already; the refusal names the member that can.
    let own = CopyRequest {
        member_id: snapshot.config.ranges[0].coordinator.to_string(),
        ..request.clone()
    };
    let Err(ServerError::BadRequestMessage(message)) = copy(&node.owner, own).await else {
        panic!("the coordinator takes no copy of its own range");
    };
    assert!(message.contains(&peer.to_string()), "{message}");

    let (code, Json(added)) = copy(&node.owner, request.clone()).await.unwrap();
    assert_eq!(code, StatusCode::ACCEPTED);
    assert_eq!(added.set_id, set_id.to_string());
    let read = status(&node, set_id).await;
    assert_eq!(read.revision, snapshot.revision().number + 1);
    assert_eq!(read.ranges[0].replicas, vec![peer.to_string()]);
    let [fill] = &read.ranges[0].filling[..] else {
        panic!("the new copy is filling: {:?}", read.ranges[0].filling);
    };
    assert_eq!(fill.member_id, peer.to_string());
    // This node coordinates the range, so it reports how far the fill got.
    assert_eq!(fill.copied_versions, Some(0));
    // The filling copy takes no transfer and blocks a split of the range.
    let transfer = transfer_distributed(
        State(node.state.clone()),
        Extension(Some(node.owner.clone())),
        bearer(),
        Path((node.group_id, set_id)),
        Json(TransferRequest {
            range_id: lower.to_string(),
            to: peer.to_string(),
            role: RoleName::Replica,
        }),
    )
    .await;
    assert!(matches!(transfer, Err(ServerError::BadRequestMessage(_))));
    let blocked = split(&node, set_id, lower, Some("c"), false).await;
    assert!(
        matches!(blocked, Err(ServerError::Conflict(_))),
        "{blocked:?}"
    );
    assert!(
        copy(&node.owner, request).await.is_err(),
        "a second copy is refused"
    );
}

async fn split(
    node: &Node,
    set_id: Ulid,
    range_id: Ulid,
    split_key: Option<&str>,
    dry_run: bool,
) -> ServerResult<SplitResponse> {
    let request = SplitRequest {
        range_id: range_id.to_string(),
        split_key: split_key.map(str::to_string),
        dry_run,
    };
    split_distributed(
        State(node.state.clone()),
        Extension(Some(node.owner.clone())),
        bearer(),
        Path((node.group_id, set_id)),
        Json(request),
    )
    .await
    .map(|Json(answer)| answer)
}

#[tokio::test]
async fn splits_by_hand() {
    let (node, snapshot) = distributed().await;
    let set_id = snapshot.set_id();
    let lower = snapshot.config.ranges[0].range_id;
    // A dry run names the split without making it.
    let planned = split(&node, set_id, lower, Some("g/"), true).await.unwrap();
    assert_eq!(planned.revision, None);
    assert_eq!(status(&node, set_id).await.ranges.len(), 2);
    // Without a key this node needs a key sample of the range, and this one holds no keys.
    let Err(ServerError::BadRequestMessage(message)) =
        split(&node, set_id, lower, None, false).await
    else {
        panic!("an empty range has no middle key");
    };
    assert!(message.contains("split_key"), "{message}");
    let outside = split(&node, set_id, lower, Some("x"), false).await;
    assert!(
        matches!(outside, Err(ServerError::Conflict(_))),
        "{outside:?}"
    );

    let made = split(&node, set_id, lower, Some("g/"), false)
        .await
        .unwrap();
    assert_eq!(made.revision, Some(snapshot.revision().number + 1));
    assert_eq!(
        (&made.lower_id, &made.upper_id),
        (&planned.lower_id, &planned.upper_id)
    );
    let read = status(&node, set_id).await;
    let bounds: Vec<(&str, Option<&str>)> = (read.ranges.iter())
        .map(|range| (range.lower.as_str(), range.upper.as_deref()))
        .collect();
    assert_eq!(bounds, [("", Some("g/")), ("g/", Some("m")), ("m", None)]);
    assert_eq!(read.ranges[1].range_id, made.upper_id);
    assert_eq!(read.ranges[1].lineage, Some(lower.to_string()));
    // The split range is retired and splits no more.
    assert!(split(&node, set_id, lower, Some("c"), false).await.is_err());
}
