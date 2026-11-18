//! Tests the distributed bucket summary: degraded ranges, publisher reach and heartbeats.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;
use aruna_core::structs::identity::realm::RealmNodeKind;
use aruna_core::structs::storage::distribution::fence::RangeFence;
use aruna_core::structs::storage::distribution::presets::shard_layout;
use aruna_core::structs::storage::distribution::{
    DistributionConfig, DistributionRequest, SetRoot, SignedSnapshot, next_snapshot,
};

use super::*;

fn secret(seed: u8) -> iroh::SecretKey {
    iroh::SecretKey::from_bytes(&[seed; 32])
}

fn realm() -> RealmId {
    RealmId::from_bytes([9; 32])
}

/// Two shards: node 1 publishes and coordinates the lower range, node 2 the upper one.
fn view() -> DistributionView {
    copied_view(0)
}

/// The shards of [`view`], each with `replicas` copies on the other node.
fn copied_view(replicas: usize) -> DistributionView {
    let root = SetRoot {
        publisher: secret(1).public(),
        owner_group: Ulid::from_bytes([90; 16]),
        nonce: [7; 16],
    };
    let nodes = [
        (Ulid::from_bytes([1; 16]), secret(1).public()),
        (Ulid::from_bytes([2; 16]), secret(2).public()),
    ];
    let layout = shard_layout(root.set_id(), &nodes, &["m".to_string()], replicas).unwrap();
    let create = DistributionRequest::Create {
        root: root.clone(),
        config: DistributionConfig {
            bucket: "spire".to_string(),
            owner_group: root.owner_group,
            node_set: None,
            members: layout.members,
            ranges: layout.ranges,
            settings: DistributionSettings::default(),
        },
    };
    let snapshot = next_snapshot(realm(), None, &create).unwrap();
    DistributionView {
        head: SignedSnapshot::sign(snapshot, realm(), &secret(1)).unwrap(),
        conflict: None,
    }
}

fn only(reachable: u8) -> impl Fn(NodeId) -> Reach {
    move |node_id| match node_id == secret(reachable).public() {
        true => Reach::Reachable,
        false => Reach::Unreachable { since_ms: Some(5) },
    }
}

#[test]
fn coordinator_degrades_range() {
    let status = summarize(realm(), &view(), only(1), true);
    assert!(status.degraded);
    assert!(!status.configuration_unavailable);
    assert_eq!(status.ranges[0].degraded, None);
    assert_eq!(
        status.ranges[1].degraded,
        Some(Reach::Unreachable { since_ms: Some(5) })
    );
    assert!(status.members.iter().all(|member| !member.admitted));
    assert!(!status.serving);
    assert_eq!(
        status.recommended_grant,
        format!(
            "/{}/g/{}/data/*/spire/**",
            realm(),
            Ulid::from_bytes([90; 16])
        )
    );
}

#[test]
fn publisher_stops_configuration() {
    let status = summarize(realm(), &view(), only(2), true);
    assert!(status.configuration_unavailable);
    assert_eq!(status.ranges[1].degraded, None);
    assert!(status.ranges[0].degraded.is_some());
}

#[test]
fn heartbeats_decide_reach() {
    let heard = BTreeMap::from([
        (secret(2).public(), (0, 77)),
        (secret(3).public(), (u64::MAX, 77)),
    ]);
    let reach = heartbeat_reach(Some(secret(1).public()), heard);
    assert_eq!(reach(secret(1).public()), Reach::Reachable);
    assert_eq!(reach(secret(2).public()), Reach::Reachable);
    assert_eq!(
        reach(secret(3).public()),
        Reach::Unreachable { since_ms: Some(77) }
    );
    assert_eq!(
        reach(secret(4).public()),
        Reach::Unreachable { since_ms: None }
    );
}

#[test]
fn removed_publisher_unavailable() {
    // Node 1 is reachable, but the realm config no longer lets it publish.
    let status = summarize(realm(), &view(), only(1), false);
    assert!(status.configuration_unavailable);
    assert!(status.ranges[0].degraded.is_none());
}

#[test]
fn reports_current_settings() {
    // The status carries the settings a change replaced, so a client can edit one field.
    let settings = DistributionSettings {
        refresh_secs: Some(600),
        search_index: true,
        split_bytes: Some(1 << 30),
        ..DistributionSettings::default()
    };
    let change = DistributionRequest::ChangeSettings {
        settings: settings.clone(),
    };
    let changed = next_snapshot(realm(), Some(view().snapshot()), &change).unwrap();
    let view = DistributionView {
        head: SignedSnapshot::sign(changed, realm(), &secret(1)).unwrap(),
        conflict: None,
    };
    assert_eq!(summarize(realm(), &view, only(1), true).settings, settings);
}

fn set_id() -> Ulid {
    view().head.snapshot.set_id()
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn is_child<T>(next: Result<Next<T>, StatusError>) -> bool {
    matches!(next, Ok(Next::Call(IoCall::Child(_))))
}

fn row(bytes: Vec<u8>) -> IoResult {
    IoResult::Values(vec![Some(Value::from(bytes))])
}

fn stored_view() -> IoResult {
    row(view().to_bytes().unwrap())
}

fn timeout() -> IoError {
    IoError::Storage(StorageError::Timeout)
}

/// A status read waiting for the realm config.
fn status_read() -> ReadStatusOperation {
    let heard = Heard {
        local: Some(secret(1).public()),
        nodes: BTreeMap::new(),
    };
    let mut read = ReadStatusOperation::new(realm(), set_id(), heard);
    read.first().unwrap();
    assert!(is_child(read.next(stored_view())));
    read
}

fn publisher_config() -> RealmConfigDocument {
    let mut config = RealmConfigDocument::new(realm(), Vec::new(), 1);
    config.ensure_node(secret(1).public(), RealmNodeKind::Server);
    config
}

// The record row decides the view: none, a stored one, or a failure on a bad row.
#[test]
fn reads_stored_view() {
    let read = || ReadViewOperation { set_id: set_id() };
    let call = read_row(DISTRIBUTION_KEYSPACE, set_id().to_bytes().to_vec());
    assert_eq!(read().first(), Ok(Next::Call(call)));
    let absent = read().next(IoResult::Values(vec![None]));
    assert_eq!(absent, Ok(Next::Done(None)));
    assert_eq!(read().next(stored_view()), Ok(Next::Done(Some(view()))));
    let bad = read().next(row(vec![0xFF]));
    assert!(matches!(bad, Err(StatusError::Conversion(_))));
}

// A set without a record has no status, and asks for nothing more.
#[test]
fn status_needs_record() {
    let heard = Heard {
        local: None,
        nodes: BTreeMap::new(),
    };
    let mut read = ReadStatusOperation::new(realm(), set_id(), heard);
    let call = read_row(DISTRIBUTION_KEYSPACE, set_id().to_bytes().to_vec());
    assert_eq!(read.first(), Ok(Next::Call(call)));
    let absent = read.next(IoResult::Values(vec![None]));
    assert_eq!(absent, Ok(Next::Done(None)));
}

// An eligible publisher accepts changes, and the fence row fills the lifecycle.
#[test]
fn status_reads_fence() {
    let range_id = Ulid::from_bytes([4; 16]);
    let fence = LocalFence {
        ranges: vec![RangeFence {
            range_id,
            transfer_id: Ulid::from_bytes([5; 16]),
            epoch: 1,
            since_ms: 2,
            uploads_open: false,
        }],
        closed: true,
        ..LocalFence::default()
    };
    let mut read = status_read();
    let config = Ok::<_, GetConfigError>(publisher_config());
    assert!(is_child(read.next(child(config))));
    let answer = child(Ok::<_, IoError>(row(fence.to_bytes().unwrap())));
    let Ok(Next::Done(Some(status))) = read.next(answer) else {
        panic!("expected the status");
    };
    assert!(!status.configuration_unavailable);
    assert_eq!(status.lifecycle.fenced_ranges, [range_id]);
    assert!(status.lifecycle.publications_closed);
}

// An unreadable config counts as a removed publisher; an unreadable fence is left out.
#[test]
fn status_tolerates_failures() {
    let mut read = status_read();
    let config = Err::<RealmConfigDocument, _>(GetConfigError::DocumentNotFound);
    assert!(is_child(read.next(child(config))));
    let answer = child(Err::<IoResult, _>(timeout()));
    let Ok(Next::Done(Some(status))) = read.next(answer) else {
        panic!("expected the status");
    };
    assert!(status.configuration_unavailable);
    assert!(status.lifecycle.fenced_ranges.is_empty());
    assert!(!status.lifecycle.publications_closed);
}

// A fence row that does not decode fails the read.
#[test]
fn bad_fence_fails() {
    let mut read = status_read();
    let config = Ok::<_, GetConfigError>(publisher_config());
    assert!(is_child(read.next(child(config))));
    let answer = child(Ok::<_, IoError>(row(vec![0xFF])));
    assert!(matches!(read.next(answer), Err(StatusError::Conversion(_))));
}

fn plain_status(source: &DistributionView) -> DistributionStatus {
    summarize(realm(), source, only(1), true)
}

fn local() -> Option<NodeId> {
    Some(secret(1).public())
}

// A node without range indexes goes straight to the record and reports no index progress.
#[test]
fn skips_absent_indexes() {
    let mut add = AddProgressOperation::new(plain_status(&view()), local(), false);
    assert!(is_child(add.first()));
    assert_eq!(add.step, ProgressStep::View);
}

// Index progress lands on its range; a failed read leaves every range without one.
#[test]
fn index_progress_hint() {
    let status = plain_status(&view());
    let range_id = status.ranges[0].range_id;
    let entry = IndexProgress {
        ready: true,
        indexed: 3,
        keys: 4,
    };
    let mut add = AddProgressOperation::new(status.clone(), local(), true);
    assert!(is_child(add.first()));
    let read = Ok::<_, FeedError>(BTreeMap::from([(range_id, entry)]));
    assert!(is_child(add.next(child(read))));
    assert_eq!(add.step, ProgressStep::View);
    assert_eq!(add.status.ranges[0].search_index, Some(entry));
    assert_eq!(add.status.ranges[1].search_index, None);

    let mut failed = AddProgressOperation::new(status, local(), true);
    failed.first().unwrap();
    let read = Err::<BTreeMap<Ulid, IndexProgress>, _>(FeedError::NotFinished);
    assert!(is_child(failed.next(child(read))));
    assert_eq!(failed.status.ranges[0].search_index, None);
}

// Without a record, or as no member of it, the status stays as it is.
#[test]
fn ends_without_member() {
    let status = plain_status(&view());
    let viewing = |node| {
        let mut add = AddProgressOperation::new(status.clone(), node, false);
        add.first().unwrap();
        add
    };
    let done = Ok(Next::Done(status.clone()));
    let stranger = Some(secret(9).public());
    let found = Ok::<_, StatusError>(Some(view()));
    assert_eq!(viewing(stranger).next(child(found)), done);
    let absent = Ok::<Option<DistributionView>, StatusError>(None);
    assert_eq!(viewing(local()).next(child(absent)), done);
    let failed = Err::<Option<DistributionView>, _>(StatusError::Storage(StorageError::Timeout));
    assert_eq!(viewing(local()).next(child(failed)), done);
}

// A failed lag read leaves the copies out, and the status is still returned.
#[test]
fn lag_failure_ends() {
    let status = plain_status(&view());
    let mut add = AddProgressOperation::new(status.clone(), local(), false);
    add.first().unwrap();
    assert!(is_child(
        add.next(child(Ok::<_, StatusError>(Some(view()))))
    ));
    let failed = Err::<(LagMap, bool), _>(RecordError::from(IoError::Unfinished));
    assert_eq!(add.next(child(failed)), Ok(Next::Done(status)));
}

// The lag fills the copies; each fill of a coordinated range reads its cursor, and only a
// cursor of the fill's own walk counts.
#[test]
fn adds_copies_fills() {
    let mut source = copied_view(1);
    for member in &mut source.head.snapshot.config.members {
        member.incarnation = Some(5);
    }
    let mut status = plain_status(&source);
    let range_id = status.ranges[0].range_id;
    let member_id = Ulid::from_bytes([2; 16]);
    let fill = |seed| FillStatus {
        fill_id: Ulid::from_bytes([seed; 16]),
        member_id,
        progress: None,
    };
    status.ranges[0].fills = vec![fill(6), fill(7)];
    status.ranges[1].fills = vec![fill(8)];
    let mut add = AddProgressOperation::new(status, local(), false);
    add.first().unwrap();
    assert!(is_child(
        add.next(child(Ok::<_, StatusError>(Some(source))))
    ));
    let owed = CopyLag {
        pending: 4,
        oldest_ms: None,
    };
    let lag = LagMap::from([((range_id, member_id), owed)]);
    assert!(is_child(add.next(child(Ok::<_, RecordError>((lag, true))))));
    let intent = RangeFill {
        fill_id: Ulid::from_bytes([6; 16]),
        range_id,
        member_id,
        admitted: true,
    }
    .intent();
    let cursor = BootstrapCursor {
        versions: 2,
        bytes: 9,
        walked: true,
        ..BootstrapCursor::start(intent, 1)
    };
    let own = Ok::<_, BootstrapError>(Some(cursor.clone()));
    assert!(is_child(add.next(child(own))));
    let other = Ok::<_, BootstrapError>(Some(cursor));
    let Ok(Next::Done(done)) = add.next(child(other)) else {
        panic!("expected the status");
    };
    assert!(done.copies_counted);
    let lower = &done.ranges[0];
    assert_eq!(lower.copies[0].member_id, member_id);
    assert_eq!(lower.copies[0].lag, owed);
    let walked = FillProgress {
        versions: 2,
        bytes: 9,
        walked: true,
    };
    assert_eq!(lower.fills[0].progress, Some(walked));
    assert_eq!(lower.fills[1].progress, Some(FillProgress::default()));
    assert_eq!(done.ranges[1].fills[0].progress, None);
}

// A cursor that cannot be read counts as a walk that has not started.
#[test]
fn unread_cursor_empty() {
    let mut status = plain_status(&view());
    status.ranges[0].fills = vec![FillStatus {
        fill_id: Ulid::from_bytes([6; 16]),
        member_id: Ulid::from_bytes([2; 16]),
        progress: None,
    }];
    let mut add = AddProgressOperation::new(status, local(), false);
    add.first().unwrap();
    assert!(is_child(
        add.next(child(Ok::<_, StatusError>(Some(view()))))
    ));
    let lag = Ok::<_, RecordError>((LagMap::new(), false));
    assert!(is_child(add.next(child(lag))));
    let failed = Err::<Option<BootstrapCursor>, _>(BootstrapError::Io(timeout()));
    let Ok(Next::Done(done)) = add.next(child(failed)) else {
        panic!("expected the status");
    };
    assert_eq!(
        done.ranges[0].fills[0].progress,
        Some(FillProgress::default())
    );
}
