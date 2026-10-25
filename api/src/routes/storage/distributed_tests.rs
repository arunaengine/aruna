//! Tests the distributed bucket routes: request conversion, outage and degraded answers, refusals.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::structs::storage::distribution::{DistributionError, DistributionRevision};
use aruna_operations::distribution::status::{
    DeletionStatus, LifecycleStatus, MemberStatus, RangeStatus, TransferStatus,
};

use super::*;

fn node_text(seed: u8) -> String {
    iroh::SecretKey::from_bytes(&[seed; 32])
        .public()
        .to_string()
}

fn request(layout: LayoutName) -> CreateDistributedRequest {
    CreateDistributedRequest {
        bucket: "spire".to_string(),
        layout,
        nodes: vec![node_text(1), node_text(2)],
        boundaries: Vec::new(),
        replicas: 0,
        edges: vec![EdgeRequest {
            node_id: node_text(3),
            strategy: StrategyName::Stream,
        }],
        settings: SettingsRequest::default(),
    }
}

#[test]
fn create_request_converts() {
    let group_id = Ulid::from_bytes([9; 16]);
    let input = create_input(group_id, request(LayoutName::Replica)).unwrap();
    assert_eq!(input.layout, LayoutKind::Replica);
    assert_eq!(input.owner_group, group_id);
    assert_eq!(input.nodes.len(), 2);
    assert_eq!(input.edges[0].1, DataStrategy::Stream);
    let mut bounded = request(LayoutName::Replica);
    bounded.boundaries = vec!["m".to_string()];
    assert!(matches!(
        create_input(group_id, bounded),
        Err(ServerError::BadRequestMessage(_))
    ));
    for invalid in ["Spire", "ab", "a..b", "-spire", "192.168.0.1"] {
        let mut named = request(LayoutName::Replica);
        named.bucket = invalid.to_string();
        assert!(
            matches!(
                create_input(group_id, named),
                Err(ServerError::BadRequestMessage(_))
            ),
            "{invalid} must be refused"
        );
    }
    let mut unknown = request(LayoutName::Sharding);
    unknown.nodes.push("not-a-node".to_string());
    assert!(matches!(
        create_input(group_id, unknown),
        Err(ServerError::BadRequestMessage(_))
    ));
}

#[test]
fn publisher_outage_unavailable() {
    let unavailable = PublishRefusal::Unavailable("publisher node x is unreachable".to_string());
    let ServerError::ServiceUnavailableReason(message) =
        map_create_error(CreateError::Publish(unavailable))
    else {
        panic!("an unreachable publisher answers 503");
    };
    assert!(message.starts_with("configuration changes unavailable"));
    assert!(matches!(
        map_create_error(CreateError::Publish(PublishRefusal::Forbidden)),
        ServerError::Forbidden
    ));
    assert!(matches!(
        map_create_error(CreateError::UnknownSet(Ulid::nil())),
        ServerError::NotFound
    ));
}

#[test]
fn degraded_names_coordinator() {
    let node: NodeId = node_text(2).parse().unwrap();
    let status = DistributionStatus {
        set_id: Ulid::from_bytes([1; 16]),
        bucket: "spire".to_string(),
        owner_group: Ulid::from_bytes([9; 16]),
        publisher: node,
        revision: DistributionRevision {
            number: 4,
            digest: [0; 32],
        },
        phase: SetPhase::Active,
        serving: true,
        conflicted: false,
        configuration_unavailable: true,
        degraded: true,
        members: vec![MemberStatus {
            member_id: Ulid::from_bytes([2; 16]),
            node_id: node,
            state: MemberState::CatchingUp,
            coverage: MetadataCoverage::All,
            strategy: DataStrategy::CacheOnUse,
            admitted: true,
        }],
        ranges: vec![RangeStatus {
            range_id: Ulid::from_bytes([3; 16]),
            lower: String::new(),
            upper: None,
            lineage: None,
            coordinator: Ulid::from_bytes([2; 16]),
            coordinator_node: Some(node),
            epoch: 1,
            replicas: Vec::new(),
            degraded: Some(Reach::Unreachable { since_ms: Some(77) }),
        }],
        recommended_grant: "/r/g/9/data/*/spire/**".to_string(),
        lifecycle: LifecycleStatus {
            transfers: vec![TransferStatus {
                transfer_id: Ulid::from_bytes([4; 16]),
                range_id: Ulid::from_bytes([3; 16]),
                from: Ulid::from_bytes([2; 16]),
                to: Ulid::from_bytes([5; 16]),
                epoch: 2,
                role: TransferRole::Leave,
                fenced: true,
                completed: false,
            }],
            deletion: None,
            fenced_ranges: vec![Ulid::from_bytes([3; 16])],
            publications_closed: false,
        },
    };
    let response = status_response(status);
    assert!(response.configuration_changes_unavailable);
    assert_eq!(response.members[0].state, "catching_up");
    assert_eq!(response.members[0].strategy, StrategyName::CacheOnUse);
    assert!(response.ranges[0].degraded);
    assert_eq!(response.ranges[0].unreachable_since_ms, Some(77));
    assert_eq!(response.ranges[0].coordinator_node, Some(node_text(2)));
    assert_eq!(response.transfers[0].role, "leave");
    assert!(response.transfers[0].fenced && !response.transfers[0].completed);
    assert_eq!(
        response.fenced_ranges,
        vec![Ulid::from_bytes([3; 16]).to_string()]
    );
    assert!(response.deletion.is_none());
}

#[test]
fn deletion_names_phase() {
    // A deleted set reports the deletion step, never `active`, in step with `deletion`.
    let (first, second) = (Ulid::from_bytes([2; 16]), Ulid::from_bytes([3; 16]));
    let status = |deletion: Option<DeletionStatus>| DistributionStatus {
        set_id: Ulid::from_bytes([1; 16]),
        bucket: "spire".to_string(),
        owner_group: Ulid::from_bytes([9; 16]),
        publisher: node_text(2).parse().unwrap(),
        revision: DistributionRevision {
            number: 7,
            digest: [0; 32],
        },
        phase: SetPhase::Active,
        serving: true,
        conflicted: false,
        configuration_unavailable: false,
        degraded: false,
        members: Vec::new(),
        ranges: Vec::new(),
        recommended_grant: "/r/g/9/data/*/spire/**".to_string(),
        lifecycle: LifecycleStatus {
            transfers: Vec::new(),
            deletion,
            fenced_ranges: Vec::new(),
            publications_closed: true,
        },
    };
    let deletion = |committed, fenced: Vec<Ulid>, waiting: Vec<Ulid>| DeletionStatus {
        committed,
        fenced,
        waiting,
    };
    let phase = |deletion| status_response(status(deletion)).phase;
    assert_eq!(phase(None), "active");
    assert_eq!(
        phase(Some(deletion(false, vec![first], vec![second]))),
        "deletion_prepared"
    );
    assert_eq!(
        phase(Some(deletion(false, vec![first, second], Vec::new()))),
        "deletion_fenced"
    );
    assert_eq!(
        phase(Some(deletion(true, vec![first, second], Vec::new()))),
        "deletion_committed"
    );
}

#[test]
fn guard_refusals_map() {
    let set_id = Ulid::from_bytes([1; 16]);
    let wrong = GuardError::NotCoordinator {
        set_id,
        range_id: Ulid::from_bytes([2; 16]),
        node_id: node_text(2).parse().unwrap(),
    };
    assert!(matches!(
        map_guard_error(wrong),
        ServerError::ServiceUnavailableReason(_)
    ));
    assert!(matches!(
        map_guard_error(GuardError::Unbound(set_id)),
        ServerError::Conflict(_)
    ));
}

#[test]
fn partial_names_set() {
    let report = CreationReport {
        set_id: Ulid::from_bytes([1; 16]),
        active: false,
        members: Vec::new(),
        error: Some("configuration changes unavailable: publisher node x is unreachable".into()),
    };
    let response = admission_response(report);
    assert_eq!(response.set_id, Ulid::from_bytes([1; 16]).to_string());
    assert!(
        response
            .error
            .is_some_and(|error| error.contains("unavailable"))
    );
}

#[test]
fn names_round_trip() {
    for name in [
        StrategyName::Copy,
        StrategyName::CopyOnUse,
        StrategyName::CacheOnUse,
        StrategyName::Stream,
    ] {
        assert_eq!(strategy_name(strategy(name)), name);
    }
    assert_eq!(
        behaviour(BehaviourName::Passthrough),
        RangeBehaviour::Passthrough
    );
    assert_eq!(
        behaviour(BehaviourName::ServeAndFill),
        RangeBehaviour::ServeAndFill
    );
    let settings = settings_of(&SettingsRequest {
        refresh_secs: Some(60),
        range_behaviour: Some(BehaviourName::FillFirst),
        search_index: true,
        public: true,
        split_objects: Some(10),
        split_bytes: Some(20),
    });
    assert_eq!(settings.range_behaviour, Some(RangeBehaviour::FillFirst));
    assert!(settings.search_index && settings.public);
    assert_eq!(
        (settings.split_objects, settings.split_bytes),
        (Some(10), Some(20))
    );
    let states = [
        MemberState::Created,
        MemberState::Ready,
        MemberState::Leaving,
        MemberState::Failed,
    ];
    let names: Vec<_> = states.into_iter().map(state_name).collect();
    assert_eq!(names, vec!["created", "ready", "leaving", "failed"]);
}

#[test]
fn sharding_request_converts() {
    let mut sharded = request(LayoutName::Sharding);
    sharded.boundaries = vec!["m".to_string()];
    sharded.replicas = 1;
    let input = create_input(Ulid::nil(), sharded).unwrap();
    assert!(matches!(
        input.layout,
        LayoutKind::Sharding { ref boundaries, replicas: 1 } if boundaries == &["m".to_string()]
    ));
    let mut edge = request(LayoutName::Replica);
    edge.edges[0].node_id = "not-a-node".to_string();
    assert!(matches!(
        create_input(Ulid::nil(), edge),
        Err(ServerError::BadRequestMessage(_))
    ));
}

#[test]
fn errors_map_to_status() {
    let set_id = Ulid::from_bytes([1; 16]);
    for retryable in [
        GuardError::RecordMissing(set_id),
        GuardError::Unreadable("broken".to_string()),
        GuardError::Route(RouteError::Pending(set_id)),
        GuardError::Route(RouteError::Moving(set_id)),
        GuardError::Route(RouteError::StaleRange(set_id)),
    ] {
        assert!(matches!(
            map_guard_error(retryable),
            ServerError::ServiceUnavailableReason(_)
        ));
    }
    for refused in [RouteError::Deleting(set_id), RouteError::NoRange(set_id)] {
        assert!(matches!(
            map_guard_error(GuardError::Route(refused)),
            ServerError::Conflict(_)
        ));
    }
    let conversion =
        aruna_core::errors::ConversionError::from(postcard::Error::DeserializeUnexpectedEnd);
    assert!(matches!(
        map_guard_error(GuardError::Conversion(conversion)),
        ServerError::InternalError(_)
    ));
    let cases = [
        (CreateError::Invalid("bad".into()), "400"),
        (
            CreateError::Distribution(DistributionError::MemberCount),
            "400",
        ),
        (CreateError::Publish(PublishRefusal::Unauthorized), "401"),
        (CreateError::Publish(PublishRefusal::Replayed), "409"),
        (CreateError::Conflicted, "409"),
        (CreateError::Blocked("degraded".into()), "503"),
    ];
    for (error, status) in cases {
        let mapped = match map_create_error(error) {
            ServerError::BadRequestMessage(_) => "400",
            ServerError::Unauthorized => "401",
            ServerError::Conflict(_) => "409",
            ServerError::ServiceUnavailableReason(_) => "503",
            other => panic!("unexpected mapping {other:?}"),
        };
        assert_eq!(mapped, status);
    }
}

#[test]
fn report_lists_members() {
    let node: NodeId = node_text(2).parse().unwrap();
    let report = CreationReport {
        set_id: Ulid::from_bytes([1; 16]),
        active: true,
        members: vec![aruna_operations::distribution::create::MemberOutcome {
            member_id: Ulid::from_bytes([2; 16]),
            node_id: node,
            admitted: false,
            error: Some("bucket exists".to_string()),
        }],
        error: None,
    };
    let response = admission_response(report);
    assert!(response.active);
    assert_eq!(response.members[0].node_id, node_text(2));
    assert_eq!(response.members[0].error.as_deref(), Some("bucket exists"));
}

mod routes {
    use super::*;
    use crate::tests::distributed::{BUCKET, Node, distribute, node_state};

    async fn status(
        node: &Node,
        auth: &AuthContext,
        group_id: Ulid,
        set_id: Ulid,
    ) -> ServerResult<Json<DistributedResponse>> {
        get_distributed(
            State(node.state.clone()),
            Extension(Some(auth.clone())),
            Path((group_id, set_id)),
        )
        .await
    }

    async fn of_bucket(node: &Node, auth: &AuthContext) -> ServerResult<Json<DistributedResponse>> {
        get_bucket_distribution(
            State(node.state.clone()),
            Extension(Some(auth.clone())),
            Path(BUCKET.to_string()),
        )
        .await
    }

    #[tokio::test]
    async fn reads_set_status() {
        let node = node_state().await;
        // An ordinary bucket belongs to no distributed bucket.
        assert!(matches!(
            of_bucket(&node, &node.owner).await,
            Err(ServerError::NotFound)
        ));
        let set_id = distribute(&node.state, node.group_id, BUCKET)
            .await
            .set_id();

        let Json(response) = status(&node, &node.owner, node.group_id, set_id)
            .await
            .unwrap();
        assert_eq!(response.bucket, BUCKET);
        assert_eq!(response.phase, "active");
        assert_eq!((response.members.len(), response.ranges.len()), (2, 2));
        let Json(bound) = of_bucket(&node, &node.owner).await.unwrap();
        assert_eq!(bound.set_id, set_id.to_string());

        assert!(matches!(
            status(&node, &node.stranger, node.group_id, set_id).await,
            Err(ServerError::Forbidden)
        ));
        assert!(matches!(
            of_bucket(&node, &node.stranger).await,
            Err(ServerError::Forbidden)
        ));
        // A set is found only under its owner group.
        assert!(matches!(
            status(&node, &node.owner, Ulid::nil(), set_id).await,
            Err(ServerError::NotFound)
        ));
    }

    #[tokio::test]
    async fn admission_retry_checks() {
        let node = node_state().await;
        let set_id = distribute(&node.state, node.group_id, BUCKET)
            .await
            .set_id();
        let retry = |auth: &AuthContext, set_id: Ulid| {
            retry_admission(
                State(node.state.clone()),
                Extension(Some(auth.clone())),
                Extension(Some(ValidatedBearer::new_for_test("caller-token"))),
                Path((node.group_id, set_id)),
            )
        };
        assert!(matches!(
            retry(&node.owner, Ulid::nil()).await,
            Err(ServerError::NotFound)
        ));
        assert!(matches!(
            retry(&node.stranger, set_id).await,
            Err(ServerError::Forbidden)
        ));
        let Json(report) = retry(&node.owner, set_id).await.unwrap();
        assert_eq!(report.set_id, set_id.to_string());
        assert!(report.active);
    }

    #[tokio::test]
    async fn creation_refused() {
        let node = node_state().await;
        let create = |auth: &AuthContext, request: CreateDistributedRequest| {
            create_distributed(
                State(node.state.clone()),
                Extension(Some(auth.clone())),
                Extension(Some(ValidatedBearer::new_for_test("caller-token"))),
                Path(node.group_id),
                Json(request),
            )
        };
        assert!(matches!(
            create(&node.stranger, request(LayoutName::Replica)).await,
            Err(ServerError::Forbidden)
        ));
        let mut named = request(LayoutName::Replica);
        named.bucket = "Not-Valid".to_string();
        assert!(matches!(
            create(&node.owner, named).await,
            Err(ServerError::BadRequestMessage(_))
        ));
        // Members must be realm infrastructure nodes; these are not.
        assert!(matches!(
            create(&node.owner, request(LayoutName::Replica)).await,
            Err(ServerError::BadRequestMessage(_))
        ));
    }
}
