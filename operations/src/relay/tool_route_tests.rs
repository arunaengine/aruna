//! Tests where a tool call runs and how the holder search pages, call by call.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::SystemTime;

use aruna_core::UserId;
use ulid::Ulid;

use crate::metadata::api::fanout_types::MetadataFanoutStats;
use crate::s3::bucket::search::BucketSearchHit;

use super::*;

const BUCKET: &str = "spire";

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn route(named: Option<NodeId>) -> ToolRoute {
    ToolRoute {
        realm_id: realm(),
        here: node(1),
        auth: AuthContext {
            user_id: UserId::local(Ulid::from_bytes([2; 16]), realm()),
            realm_id: realm(),
            path_restrictions: None,
            session: None,
        },
        bearer: Some("caller-token".to_string()),
        bucket: BUCKET.to_string(),
        named,
        key: "a".to_string(),
    }
}

fn child<T, E>(answer: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(answer))
}

fn info() -> BucketInfo {
    BucketInfo {
        group_id: Ulid::from_bytes([3; 16]),
        created_at: SystemTime::UNIX_EPOCH,
        created_by: Default::default(),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Default::default(),
        distribution: None,
    }
}

fn holder(seed: u8, set: Option<u8>) -> Holder {
    Holder {
        node_id: node(seed),
        set_id: set.map(|set| Ulid::from_parts(1, set.into())),
    }
}

fn found(holders: Vec<Holder>, missed: Missed) -> IoResult {
    child(Ok::<_, ToolRouteError>(HolderFound { holders, missed }))
}

/// A tool call that found no local bucket and asked the holder search.
fn searching() -> ToolNodeOperation {
    let mut tool = ToolNodeOperation::new(route(None));
    tool.first().unwrap();
    let missing = child(Err::<BucketInfo, _>(GetBucketError::NotFound));
    assert!(matches!(
        tool.next(missing),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    tool
}

fn hit(seed: u8, bucket: &str) -> BucketSearchHit {
    BucketSearchHit {
        arn: format!("arn:{bucket}"),
        bucket: bucket.to_string(),
        node_id: node(seed),
        group_id: Ulid::from_bytes([3; 16]),
        group_name: None,
        created_at: SystemTime::UNIX_EPOCH,
        set_id: None,
        metadata: None,
        public: false,
    }
}

fn page(hits: Vec<BucketSearchHit>, next: Option<&str>, failed: Vec<NodeId>) -> IoResult {
    let page = BucketSearchExecution {
        hits,
        next_cursor: next.map(str::to_string),
        fanout_stats: MetadataFanoutStats {
            nodes_failed: failed.len(),
            failed_partitions: failed,
            ..Default::default()
        },
    };
    let event = Event::Child(ChildOutcome::new(Ok::<_, MetadataApiError>(page)));
    let results = vec![GroupResult::Done(Box::new(event))];
    IoResult::Group(GroupEvent::Finished { results })
}

fn deadline(next: Result<Next<HolderFound>, ToolRouteError>) -> Option<Duration> {
    let Ok(Next::Call(IoCall::Group(group))) = next else {
        panic!("a search page is asked for");
    };
    assert_eq!(group.members.len(), 1);
    match group.form {
        GroupForm::All { deadline } => deadline,
        GroupForm::Window => panic!("a page is one bounded member"),
    }
}

#[test]
fn other_node_named() {
    let mut tool = ToolNodeOperation::new(route(Some(node(2))));
    assert_eq!(tool.first(), Ok(Next::Done(Some(node(2)))));
}

#[test]
fn local_bucket_stays() {
    let mut tool = ToolNodeOperation::new(route(None));
    tool.first().unwrap();
    let answer = tool.next(child(Ok::<_, GetBucketError>(info())));
    assert_eq!(answer, Ok(Next::Done(None)));
}

#[test]
fn bucket_failure_refused() {
    let mut tool = ToolNodeOperation::new(route(None));
    tool.first().unwrap();
    let failed = child(Err::<BucketInfo, _>(GetBucketError::Incomplete));
    let refused = ToolRouteError::Bucket(GetBucketError::Incomplete);
    assert_eq!(tool.next(failed), Err(refused));
}

#[test]
fn one_holder_chosen() {
    let mut tool = searching();
    let answer = tool.next(found(vec![holder(2, None)], Missed::default()));
    assert_eq!(answer, Ok(Next::Done(Some(node(2)))));
}

#[test]
fn set_asks_members() {
    let mut tool = searching();
    let next = tool.next(found(vec![holder(2, Some(7))], Missed::default()));
    assert!(matches!(next, Ok(Next::Call(IoCall::Child(_)))));
    let members = child(Ok::<_, IoError>(vec![node(1), node(3)]));
    assert_eq!(tool.next(members), Ok(Next::Done(Some(node(3)))));
}

#[test]
fn absent_keeps_missed() {
    let mut tool = searching();
    let missed = Missed {
        nodes: vec![node(4)],
        incomplete: true,
    };
    let refused = ToolRouteError::Route(RouteError::Missing, missed.clone());
    assert_eq!(tool.next(found(Vec::new(), missed)), Err(refused));
}

#[test]
fn pages_share_deadline() {
    let mut search = HolderSearchOperation::new(route(None));
    search.stamp(1_000);
    assert_eq!(deadline(search.first()), Some(DISCOVERY_TIMEOUT));
    search.stamp(11_000);
    let next = search.next(page(vec![hit(2, "other")], Some("more"), Vec::new()));
    assert_eq!(deadline(next), Some(Duration::from_secs(20)));
}

#[test]
fn spent_time_stops() {
    let mut search = HolderSearchOperation::new(route(None));
    search.stamp(1_000);
    search.first().unwrap();
    search.stamp(31_000);
    let next = search.next(page(Vec::new(), Some("more"), Vec::new()));
    assert_eq!(next, Err(ToolRouteError::SearchLate));
}

#[test]
fn late_page_stops() {
    let mut search = HolderSearchOperation::new(route(None));
    search.first().unwrap();
    let results = vec![GroupResult::Unfinished];
    let late = IoResult::Group(GroupEvent::Finished { results });
    assert_eq!(search.next(late), Err(ToolRouteError::SearchLate));
}

#[test]
fn holders_keep_name() {
    let mut search = HolderSearchOperation::new(route(None));
    search.first().unwrap();
    let hits = vec![hit(2, BUCKET), hit(3, "spire-copy")];
    let Ok(Next::Done(done)) = search.next(page(hits, None, vec![node(4)])) else {
        panic!("the last page ends the search");
    };
    assert_eq!(done.holders, [holder(2, None)]);
    assert_eq!(
        done.missed,
        Missed {
            nodes: vec![node(4)],
            incomplete: true,
        }
    );
}

#[test]
fn prints_hide_token() {
    let printed = format!("{:?}", ToolNodeOperation::new(route(None)));
    assert!(!printed.contains("caller-token"), "{printed}");
}
