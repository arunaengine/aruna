//! Tests of run copies on the holders of a group's records: the frames the run's node sends,
//! what a holder keeps or refuses, and the resume of a copy on the surviving node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;
use aruna_core::metadata::protocol::decode_message;
use aruna_core::structs::execution::workflow_run::RunState;
use aruna_runner::driver::drive;
use serde_json::json;

use super::super::control::{Follow, FollowRunOperation};
use super::super::log::{LineageLogOperation, LoadLogOperation};
use super::super::tests::runs;
use super::*;
use crate::workflows::{Access, CatalogError};
use aruna_core::structs::identity::auth::AuthContext;

fn pages(changes: u64) -> Vec<RunLogPage> {
    let begin = RunLogEntry::Begin {
        run_start_json: json!({"run": "start"}),
    };
    let mut entries = vec![begin];
    entries.extend((1..=changes).map(|seq| RunLogEntry::Change {
        seq,
        change_json: json!({"task": seq}),
    }));
    entries
        .chunks(2)
        .map(|entries| RunLogPage {
            entries: entries.to_vec(),
        })
        .collect()
}

fn encoded(pages: &[RunLogPage]) -> Vec<Vec<u8>> {
    pages
        .iter()
        .map(|page| encode_page(page).unwrap())
        .collect()
}

/// A copy of a run that `node` coordinates, from a run this node started.
fn copy_of(record: &WorkflowRunRecord, node: NodeId) -> WorkflowRunRecord {
    WorkflowRunRecord {
        run_id: Ulid::from_bytes([42; 16]),
        lineage: Ulid::from_bytes([42; 16]),
        node_id: node,
        state: RunState::Running,
        ..record.clone()
    }
}

// The run's node sends its run and log pages from the last sent page on, in bounded frames.
#[test]
fn frames_log_pages() {
    let record = super::super::tests::sample_run();
    let local = (record.node_id, record.realm_id);
    let pages = pages(5);
    let send = ReplicateLogOperation::new(record.run_id, local, 1);
    let frames = send.frames(&record, &pages).unwrap();
    assert_eq!(frames.len(), 1);
    let MetadataTransportMessage::StoreRunLog {
        run,
        first_page,
        pages: sent,
    } = decode_message(&frames[0]).unwrap()
    else {
        panic!("a store request");
    };
    assert_eq!((*run, first_page), (record.clone(), 1));
    assert_eq!(sent, encoded(&pages[1..]));
    // A node that is not the run's own never hands its copy on.
    let mut copy = ReplicateLogOperation::new(
        record.run_id,
        (aruna_ops_identity::testing::node(9), record.realm_id),
        0,
    );
    copy.first().unwrap();
    let stored = IoResult::Values(vec![Some(record.to_bytes().unwrap().into())]);
    assert_eq!(copy.next(stored).unwrap(), Next::Done(0));
    // A local device run never replicates and a holder refuses it.
    let mut local_run = record.clone();
    local_run.local_only = true;
    let mut send_local = ReplicateLogOperation::new(local_run.run_id, local, 0);
    send_local.first().unwrap();
    let stored = IoResult::Values(vec![Some(local_run.to_bytes().unwrap().into())]);
    assert_eq!(send_local.next(stored).unwrap(), Next::Done(0));
    let mut store_local =
        StoreReplicaOperation::new(local_run.node_id, local, Box::new(local_run), 0, Vec::new());
    assert_eq!(store_local.first(), Err(MetadataReadError::Forbidden));
    // A sender that is not the run's node is refused before anything is read.
    let peer = aruna_ops_identity::testing::node(9);
    let mut store = StoreReplicaOperation::new(peer, local, Box::new(record), 0, Vec::new());
    assert_eq!(store.first(), Err(MetadataReadError::Forbidden));
}

// A holder keeps the copy and its pages; when the run's node is gone, the owner resumes the
// copy on the holder, and the new run replays the copied log so finished tasks are not rerun.
#[tokio::test]
async fn resumes_copied_run() {
    let runs = runs().await;
    let context = &runs.fixture.context;
    let here = runs.fixture.node;
    let gone = aruna_ops_identity::testing::node(9);
    let own = runs.started().await;
    let copy = copy_of(&own, gone);
    let pages = pages(5);
    let write =
        |run: &WorkflowRunRecord, first_page: u32, pages: Vec<Vec<u8>>| WriteReplicaOperation {
            local: here,
            run: Box::new(run.clone()),
            first_page,
            pages,
            reading: true,
            kept: 0,
        };
    let kept = drive(write(&copy, 0, encoded(&pages[..2])), context)
        .await
        .unwrap();
    assert_eq!(kept, 2);
    let kept = drive(write(&copy, 1, encoded(&pages[1..])), context)
        .await
        .unwrap();
    assert_eq!(kept, 3);
    let loaded = drive(LoadLogOperation::new(copy.run_id), context)
        .await
        .unwrap();
    assert_eq!(loaded, pages);
    // This node's own run is never overwritten by a copy.
    let refused = drive(write(&own, 0, Vec::new()), context).await;
    assert_eq!(refused, Err(CatalogError::Forbidden));
    assert_eq!(replica_note(&copy, here), Some(UNREACHABLE));
    assert_eq!(replica_note(&own, here), None);

    let follow = FollowRunOperation::new(
        &runs.fixture.alice,
        runs.serving(),
        copy.run_id,
        Follow::Resume,
    );
    let resumed = drive(follow, context).await.unwrap();
    assert_eq!(resumed.resumes_run_id, Some(copy.run_id));
    assert_eq!((resumed.lineage, resumed.node_id), (copy.lineage, here));
    let replayed = drive(LineageLogOperation::new(resumed.run_id), context)
        .await
        .unwrap();
    assert_eq!(replayed, pages);
    // A run this node coordinates itself is not resumed while it runs.
    let follow = FollowRunOperation::new(
        &runs.fixture.alice,
        runs.serving(),
        own.run_id,
        Follow::Resume,
    );
    assert!(matches!(
        drive(follow, context).await,
        Err(CatalogError::Conflict(_))
    ));
}

// Someone other than the owner resumes a copy only as a realm admin.
#[test]
fn admin_resumes_copy() {
    let mut record = super::super::tests::sample_run();
    record.node_id = aruna_ops_identity::testing::node(9);
    record.created_by =
        aruna_core::user::id::UserId::new(Ulid::from_bytes([1; 16]), record.realm_id);
    let auth = AuthContext::new(
        aruna_core::user::id::UserId::new(Ulid::from_bytes([2; 16]), record.realm_id),
        record.realm_id,
    );
    let node = (aruna_ops_identity::testing::node(3), record.realm_id);
    let access = Access {
        read: true,
        write: true,
    };
    let answer = |record: &WorkflowRunRecord| {
        IoResult::Child(ChildOutcome::new(Ok::<_, CatalogError>((
            record.clone(),
            access,
        ))))
    };
    for (admin, resumed) in [(false, false), (true, true)] {
        let mut follow = FollowRunOperation::new(&auth, node, record.run_id, Follow::Resume);
        follow.first().unwrap();
        let check = follow.next(answer(&record)).unwrap();
        assert!(matches!(check, Next::Call(IoCall::Child(_))), "{check:?}");
        let decided = IoResult::Child(ChildOutcome::new(Ok::<
            _,
            aruna_core::errors::AuthorizationError,
        >(admin)));
        match follow.next(decided) {
            Ok(Next::Call(IoCall::Child(_))) => assert!(resumed),
            Err(CatalogError::Forbidden) => assert!(!resumed),
            other => panic!("{other:?}"),
        }
    }
    // A repeat never takes a copy that has not ended.
    let mut follow = FollowRunOperation::new(&auth, node, record.run_id, Follow::Repeat);
    follow.first().unwrap();
    assert!(matches!(
        follow.next(answer(&record)),
        Err(CatalogError::Conflict(_))
    ));
}
