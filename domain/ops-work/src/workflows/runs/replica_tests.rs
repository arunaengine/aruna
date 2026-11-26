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
    let frames = send.frames(&record, &pages, None).unwrap();
    assert_eq!(frames.len(), 1);
    let MetadataTransportMessage::StoreRunLog {
        run,
        first_page,
        pages: sent,
        workflow: None,
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
    let mut store_local = StoreReplicaOperation::new(
        local_run.node_id,
        local,
        Box::new(local_run),
        0,
        Vec::new(),
        None,
    );
    assert_eq!(store_local.first(), Err(MetadataReadError::Forbidden));
    // A sender that is not the run's node is refused before anything is read.
    let peer = aruna_ops_identity::testing::node(9);
    let mut store = StoreReplicaOperation::new(peer, local, Box::new(record), 0, Vec::new(), None);
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
            peer: gone,
            local: here,
            run: Box::new(run.clone()),
            first_page,
            pages,
            workflow: None,
            now_ms: 0,
            step: WriteStep::Read,
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

// A replica refuses a page gap and never lets an older shorter tail overwrite a newer tail page.
#[tokio::test]
async fn guards_replica_gaps() {
    let runs = runs().await;
    let context = &runs.fixture.context;
    let here = runs.fixture.node;
    let gone = aruna_ops_identity::testing::node(9);
    let own = runs.started().await;
    let copy = copy_of(&own, gone);
    let write =
        |run: &WorkflowRunRecord, first_page: u32, pages: Vec<Vec<u8>>| WriteReplicaOperation {
            peer: gone,
            local: here,
            run: Box::new(run.clone()),
            first_page,
            pages,
            workflow: None,
            now_ms: 0,
            step: WriteStep::Read,
            kept: 0,
        };
    let page = |entries: Vec<RunLogEntry>| vec![encode_page(&RunLogPage { entries }).unwrap()];
    // Page 1 before page 0 is a gap and must return NotFound so the sender backfills from 0.
    let gap = drive(write(&copy, 1, page(vec![])), context).await;
    assert_eq!(gap, Err(CatalogError::NotFound));

    // Write a 2-entry tail page, then try to overwrite it with an older 1-entry snapshot.
    let newer = vec![
        RunLogEntry::Begin {
            run_start_json: serde_json::json!({"run": 1}),
        },
        RunLogEntry::Change {
            seq: 1,
            change_json: serde_json::json!({"task": 1}),
        },
    ];
    let older = vec![newer[0].clone()];
    drive(write(&copy, 0, page(newer.clone())), context)
        .await
        .unwrap();
    drive(write(&copy, 0, page(older)), context).await.unwrap();
    let loaded = drive(LoadLogOperation::new(copy.run_id), context)
        .await
        .unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].entries, newer);

    // Delayed non-terminal snapshots never overwrite a terminal run record on the replica.
    let mut done_copy = copy.clone();
    done_copy.state = RunState::Succeeded;
    done_copy.last_event_seq = 5;
    drive(write(&done_copy, 0, Vec::new()), context)
        .await
        .unwrap();
    let mut stale = copy.clone();
    stale.state = RunState::Running;
    stale.last_event_seq = 2;
    drive(write(&stale, 0, Vec::new()), context).await.unwrap();
    let stored = drive(
        super::super::reads::RunSnapshotOperation::new(&runs.fixture.alice, copy.run_id),
        context,
    )
    .await
    .unwrap();
    assert_eq!(stored.record.state, RunState::Succeeded);
    assert_eq!(stored.record.last_event_seq, 5);

    // Cross-node Repeat never silently rebinds an output pinned to the original coordinator.
    let mut pinned = done_copy.clone();
    pinned.run_id = Ulid::from_bytes([43; 16]);
    pinned.lineage = pinned.run_id;
    pinned.output = Some(aruna_core::structs::execution::workflow_run::RunOutput {
        bucket: "results".into(),
        prefix: "out/".into(),
        node_id: Some(gone),
        dataset_json: None,
    });
    drive(write(&pinned, 0, Vec::new()), context).await.unwrap();
    let repeated = drive(
        FollowRunOperation::new(
            &runs.fixture.alice,
            runs.serving(),
            pinned.run_id,
            Follow::Repeat,
        ),
        context,
    )
    .await;
    assert!(
        matches!(repeated, Err(CatalogError::Invalid(_))),
        "{repeated:?}"
    );
    let resumed = drive(
        FollowRunOperation::new(
            &runs.fixture.alice,
            runs.serving(),
            pinned.run_id,
            Follow::Resume,
        ),
        context,
    )
    .await
    .unwrap();
    assert_eq!(resumed.output.unwrap().node_id, Some(here));
}

// Large workflow sources and updated tools transfer across bounded frames and only expose the
// copy as resumable once the final metadata frame commits.
#[tokio::test]
async fn chunks_large_replicas() {
    use aruna_core::effects::StorageEffect;
    use aruna_core::structs::execution::workflow::{StoredFile, WorkflowFiles};

    let runs = runs().await;
    let context = &runs.fixture.context;
    let here = runs.fixture.node;
    let gone = aruna_ops_identity::testing::node(9);
    let own = runs.started().await;
    let copy = copy_of(&own, gone);
    let wf_id = Ulid::from_bytes([77; 16]);
    let mut remote = copy.clone();
    remote.run_id = Ulid::from_bytes([78; 16]);
    remote.lineage = remote.run_id;
    remote.workflow_id = wf_id;
    remote.version = 1;
    remote.values_json = serde_json::json!({"big": "r".repeat(2600)}).to_string();

    let wf = WorkflowRecord {
        workflow_id: wf_id,
        group_id: remote.group_id,
        name: "chunked".into(),
        description: Some("w".repeat(2300)),
        language: StoredWorkflowLanguage::Uwe,
        visibility: aruna_core::structs::execution::workflow::WorkflowVisibility::Group,
        version_count: 1,
        edam_formats: Vec::new(),
        created_by: remote.created_by,
        created_at_ms: 1,
        updated_at_ms: 1,
    };
    let source_files = WorkflowFiles {
        files: vec![StoredFile {
            path: "workflow.uwe.yaml".into(),
            content: vec![b'x'; 2500],
            executable: false,
        }],
    };
    let ver = WorkflowVersionRecord {
        workflow_id: wf_id,
        version: 1,
        language: StoredWorkflowLanguage::Uwe,
        entrypoint: "workflow.uwe.yaml".into(),
        files: vec!["workflow.uwe.yaml".into()],
        digest: aruna_core::workflow::source_digest(&source_files.source()),
        commit: None,
        message: Some("v".repeat(2300)),
        uwe_yaml: Some("steps:\n  a:\n    tool: bigtool\n".into()),
        images: Default::default(),
        inspection: Default::default(),
        created_by: remote.created_by,
        created_at_ms: 1,
    };
    let tool_v1 = ToolRecord {
        tool_id: "bigtool".into(),
        version: "1.0".into(),
        group_id: remote.group_id,
        title: "bigtool".into(),
        curation: Default::default(),
        definition: serde_json::json!({"id": "bigtool", "rev": 1}).to_string(),
        files: vec![StoredFile {
            path: "run.sh".into(),
            content: vec![b't'; 2200],
            executable: true,
        }],
        created_by: remote.created_by,
        created_at_ms: 10,
    };
    let mut tool_v2 = tool_v1.clone();
    tool_v2.definition = serde_json::json!({"id": "bigtool", "rev": 2}).to_string();
    tool_v2.created_at_ms = 20;

    // Seed an older tool definition first to verify replica updates replace stale tool rows.
    context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: WORKFLOW_TOOL_KEYSPACE.to_string(),
            key: tool_key(&tool_v1.tool_id, &tool_v1.version, tool_v1.group_id),
            value: tool_v1.to_bytes().unwrap().into(),
            txn_id: None,
        })
        .await;

    let bundle = ReplicatedWorkflow {
        workflow: wf.to_bytes().unwrap(),
        version: ver.to_bytes().unwrap(),
        source: Some(source_files.to_bytes().unwrap()),
        tools: vec![tool_v2.to_bytes().unwrap()],
        chunks: Vec::new(),
        more: false,
        stub_run: false,
        transfer_id: 0,
    };
    let send = ReplicateLogOperation::new(remote.run_id, (gone, remote.realm_id), 0);
    let mut log = pages(3);
    // Include one oversized log page (> limit) so page chunking and frame bounds are exercised.
    log.push(RunLogPage {
        entries: vec![RunLogEntry::Change {
            seq: 99,
            change_json: serde_json::json!({ "blob": "z".repeat(2600) }),
        }],
    });
    let limit = 2048;
    let frames = send
        .frames_bounded(0, &remote, &log, Some(bundle), limit)
        .unwrap();
    assert!(frames.len() >= 6, "expected bounded multi-frame transfer");
    for frame in &frames {
        assert!(
            frame.len() <= limit,
            "frame {} exceeds limit {limit}",
            frame.len()
        );
    }

    // Apply all frames in order; `more: true` frames before the commit must keep the run hidden.
    let mut applied_more = 0;
    let mut committed = false;
    for frame in &frames {
        let MetadataTransportMessage::StoreRunLog {
            run,
            first_page,
            pages,
            workflow,
        } = decode_message(frame).unwrap()
        else {
            panic!("expected StoreRunLog");
        };
        let is_more = workflow.as_ref().is_some_and(|w| w.more);
        drive(
            WriteReplicaOperation {
                peer: gone,
                local: here,
                run,
                first_page,
                pages,
                workflow,
                now_ms: 0,
                step: WriteStep::Read,
                kept: 0,
            },
            context,
        )
        .await
        .unwrap();
        if is_more {
            applied_more += 1;
            if !committed {
                let hidden = drive(
                    super::super::reads::RunSnapshotOperation::new(
                        &runs.fixture.alice,
                        remote.run_id,
                    ),
                    context,
                )
                .await;
                assert_eq!(hidden, Err(CatalogError::NotFound));
                let hidden_ver = drive(
                    crate::workflows::versions::GetVersionOperation::new(
                        &runs.fixture.alice,
                        wf_id,
                        1,
                    )
                    .with_files(),
                    context,
                )
                .await;
                assert_eq!(hidden_ver, Err(CatalogError::NotFound));
                let live_tools = drive(
                    crate::workflows::ScanOperation::<ToolRecord>::of_groups(Some(
                        [remote.group_id].into(),
                    )),
                    context,
                )
                .await
                .unwrap();
                let live = live_tools
                    .into_iter()
                    .find(|(_, t)| t.tool_id == "bigtool")
                    .unwrap()
                    .1;
                assert_eq!(live.definition, tool_v1.definition);
            }
        } else {
            committed = true;
        }
    }
    assert!(applied_more >= 3);
    let stored = drive(
        super::super::reads::RunSnapshotOperation::new(&runs.fixture.alice, remote.run_id),
        context,
    )
    .await
    .unwrap();
    assert_eq!(stored.record.run_id, remote.run_id);
    assert_eq!(stored.record.values_json, remote.values_json);
    let stored_ver = drive(
        crate::workflows::versions::GetVersionOperation::new(&runs.fixture.alice, wf_id, 1)
            .with_files(),
        context,
    )
    .await
    .unwrap();
    assert_eq!(stored_ver.workflow.description, wf.description);
    assert_eq!(stored_ver.version.message, ver.message);
    assert_eq!(stored_ver.files.as_ref(), Some(&source_files));
    assert_eq!(
        drive(LoadLogOperation::new(remote.run_id), context)
            .await
            .unwrap(),
        log
    );
    let stored_tool = drive(
        crate::workflows::ScanOperation::<ToolRecord>::of_groups(Some([remote.group_id].into())),
        context,
    )
    .await
    .unwrap();
    let updated = stored_tool
        .into_iter()
        .find(|(_, t)| t.tool_id == "bigtool")
        .unwrap()
        .1;
    assert_eq!(updated.definition, tool_v2.definition);

    // Reimporting identical bytes with a newer timestamp advances the timestamp so a delayed
    // intermediate definition cannot regress it; equal timestamps converge deterministically.
    let mut tool_reimport = tool_v2.clone();
    tool_reimport.created_at_ms = 40;
    let mut tool_mid = tool_v2.clone();
    tool_mid.definition = serde_json::json!({"id": "bigtool", "rev": 25}).to_string();
    tool_mid.created_at_ms = 30;
    let v2_bytes = tool_v2.to_bytes().unwrap();
    let v3_bytes = tool_reimport.to_bytes().unwrap();
    let mid_bytes = tool_mid.to_bytes().unwrap();
    assert!(
        tool_update(Some(&v2_bytes), &v3_bytes, remote.group_id)
            .unwrap()
            .is_some(),
        "newer reimport of identical definition must advance stored timestamp"
    );
    assert!(
        tool_update(Some(&v3_bytes), &mid_bytes, remote.group_id)
            .unwrap()
            .is_none(),
        "delayed intermediate timestamp must not overwrite newer reimport"
    );
    let mut tool_tie_a = tool_reimport.clone();
    tool_tie_a.definition = serde_json::json!({"id": "bigtool", "rev": "a"}).to_string();
    let mut tool_tie_b = tool_reimport.clone();
    tool_tie_b.definition = serde_json::json!({"id": "bigtool", "rev": "z"}).to_string();
    let tie_a = tool_tie_a.to_bytes().unwrap();
    let tie_b = tool_tie_b.to_bytes().unwrap();
    assert!(
        tool_update(Some(&tie_a), &tie_b, remote.group_id)
            .unwrap()
            .is_some(),
        "equal timestamp converges to the lexicographically larger record"
    );
    assert!(
        tool_update(Some(&tie_b), &tie_a, remote.group_id)
            .unwrap()
            .is_none(),
        "equal timestamp does not oscillate back to the smaller record"
    );

    // Composed tool selection uses parsed graph references and exact/latest version rules.
    let graph = crate::workflows::GraphView {
        graph: aruna_workflow_contract::preflight::WorkflowGraph {
            nodes: vec![
                aruna_workflow_contract::preflight::GraphNode {
                    id: "s1".into(),
                    label: "s1".into(),
                    kind: NodeKind::Step,
                    tool: Some("bigtool".into()),
                    version: Some("1.0".into()),
                },
                aruna_workflow_contract::preflight::GraphNode {
                    id: "s2".into(),
                    label: "s2".into(),
                    kind: NodeKind::Step,
                    tool: Some("unpinned".into()),
                    version: None,
                },
            ],
            edges: Vec::new(),
        },
        complete: true,
    };
    let graph_json = serde_json::to_string(&graph).unwrap();
    let mut big_v2_ver = tool_v1.clone();
    big_v2_ver.version = "2.0".into();
    let mut unpinned_v1 = tool_v1.clone();
    unpinned_v1.tool_id = "unpinned".into();
    unpinned_v1.version = "1.0".into();
    let mut unpinned_v2 = unpinned_v1.clone();
    unpinned_v2.version = "2.0".into();
    let mut unrelated = tool_v1.clone();
    unrelated.tool_id = "other".into();
    let picked = used_tools(
        &[],
        Some(&graph_json),
        vec![
            tool_v1.clone(),
            big_v2_ver.clone(),
            unpinned_v1,
            unpinned_v2.clone(),
            unrelated,
        ],
    );
    let picked_ids: Vec<(String, String)> =
        picked.into_iter().map(|t| (t.tool_id, t.version)).collect();
    assert_eq!(
        picked_ids,
        vec![
            ("bigtool".to_string(), "1.0".to_string()),
            ("unpinned".to_string(), "2.0".to_string()),
        ]
    );
    let override_picked = used_tools(
        &[("bigtool".to_string(), "2.0".to_string())],
        Some(&graph_json),
        vec![tool_v1, big_v2_ver],
    );
    assert_eq!(override_picked.len(), 1);
    assert_eq!(override_picked[0].version, "2.0");

    // An existing source row must still reject a malformed or mismatched incoming source payload.
    let wrong_files = WorkflowFiles {
        files: vec![StoredFile {
            path: "workflow.uwe.yaml".into(),
            content: b"tampered".to_vec(),
            executable: false,
        }],
    };
    for bad_source in [b"not-postcard".to_vec(), wrong_files.to_bytes().unwrap()] {
        let bad_bundle = ReplicatedWorkflow {
            source: Some(bad_source),
            ..ReplicatedWorkflow::default()
        };
        let rejected = drive(
            WriteReplicaOperation {
                peer: gone,
                local: here,
                run: Box::new(remote.clone()),
                first_page: 0,
                pages: Vec::new(),
                workflow: Some(bad_bundle),
                now_ms: 0,
                step: WriteStep::Read,
                kept: 0,
            },
            context,
        )
        .await;
        assert_eq!(rejected, Err(CatalogError::Forbidden));
    }

    // Zero-ID frames carrying chunks are rejected even when `more` is false.
    let zero_chunk = ReplicatedWorkflow {
        chunks: vec![ReplicatedChunk {
            kind: ChunkKind::Source,
            index: 0,
            total: 2,
            data: b"part".to_vec(),
        }],
        more: false,
        transfer_id: 0,
        ..ReplicatedWorkflow::default()
    };
    assert_eq!(
        drive(
            WriteReplicaOperation {
                peer: gone,
                local: here,
                run: Box::new(remote.clone()),
                first_page: 0,
                pages: Vec::new(),
                workflow: Some(zero_chunk),
                now_ms: 0,
                step: WriteStep::Read,
                kept: 0,
            },
            context,
        )
        .await,
        Err(CatalogError::Forbidden)
    );

    // Duplicate source representations (assembled chunk plus inline source) are rejected.
    let good_src = source_files.to_bytes().unwrap();
    let dup_source = ReplicatedWorkflow {
        source: Some(b"tampered".to_vec()),
        chunks: vec![ReplicatedChunk {
            kind: ChunkKind::Source,
            index: 0,
            total: 1,
            data: good_src.clone(),
        }],
        more: false,
        transfer_id: 90,
        ..ReplicatedWorkflow::default()
    };
    assert_eq!(
        drive(
            WriteReplicaOperation {
                peer: gone,
                local: here,
                run: Box::new(remote.clone()),
                first_page: 0,
                pages: Vec::new(),
                workflow: Some(dup_source),
                now_ms: 0,
                step: WriteStep::Read,
                kept: 0,
            },
            context,
        )
        .await,
        Err(CatalogError::Forbidden)
    );

    // Duplicate completed chunks for the same target within an intermediate frame are rejected.
    let dup_more = ReplicatedWorkflow {
        chunks: vec![
            ReplicatedChunk {
                kind: ChunkKind::Source,
                index: 0,
                total: 1,
                data: good_src.clone(),
            },
            ReplicatedChunk {
                kind: ChunkKind::Source,
                index: 0,
                total: 1,
                data: wrong_files.to_bytes().unwrap(),
            },
        ],
        more: true,
        transfer_id: 90,
        ..ReplicatedWorkflow::default()
    };
    assert_eq!(
        drive(
            WriteReplicaOperation {
                peer: gone,
                local: here,
                run: Box::new(remote.clone()),
                first_page: 0,
                pages: Vec::new(),
                workflow: Some(dup_more),
                now_ms: 0,
                step: WriteStep::Read,
                kept: 0,
            },
            context,
        )
        .await,
        Err(CatalogError::Forbidden)
    );

    // LoadLogOperation reads head_key and numbered page keys directly without scanning manifests.
    let mut loader = LoadLogOperation::new(remote.run_id);
    assert!(matches!(
        loader.first().unwrap(),
        Next::Call(IoCall::Read(keys)) if keys.len() == 1 && keys[0].1 == head_key(remote.run_id)
    ));

    // An expired staged transfer is reclaimed when a new transfer begins, and finalization
    // deletes all remaining staged rows for the peer.
    let expired_key = staged_key(remote.run_id, gone, 80, ChunkKind::Source);
    let expired_meta = stage_meta_key(remote.run_id, gone, 80);
    context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: RUN_LOG_KEYSPACE.to_string(),
            key: expired_key.clone(),
            value: good_src.clone().into(),
            txn_id: None,
        })
        .await;
    context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: RUN_LOG_KEYSPACE.to_string(),
            key: expired_meta.clone(),
            value: 1u64.to_be_bytes().to_vec().into(),
            txn_id: None,
        })
        .await;

    // An abandoned intermediate transfer's staged tool must never publish on a later transfer.
    let mut abandoned = tool_v2.clone();
    abandoned.tool_id = "abandoned".into();
    abandoned.version = "1.0".into();
    let stage_old = ReplicatedWorkflow {
        tools: vec![abandoned.to_bytes().unwrap()],
        more: true,
        transfer_id: 91,
        ..ReplicatedWorkflow::default()
    };
    drive(
        WriteReplicaOperation {
            peer: gone,
            local: here,
            run: Box::new(remote.clone()),
            first_page: 0,
            pages: Vec::new(),
            workflow: Some(stage_old),
            now_ms: 0,
            step: WriteStep::Read,
            kept: 0,
        },
        context,
    )
    .await
    .unwrap();
    // Transfer 91 starting must have reclaimed expired transfer 80.
    let gone_old = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: RUN_LOG_KEYSPACE.to_string(),
            key: expired_key,
            txn_id: None,
        })
        .await;
    assert!(matches!(
        gone_old,
        aruna_core::events::Event::Storage(aruna_core::events::StorageEvent::ReadResult {
            value: None,
            ..
        })
    ));
    let retry_commit = ReplicatedWorkflow {
        more: false,
        transfer_id: 92,
        ..ReplicatedWorkflow::default()
    };
    drive(
        WriteReplicaOperation {
            peer: gone,
            local: here,
            run: Box::new(remote.clone()),
            first_page: 0,
            pages: Vec::new(),
            workflow: Some(retry_commit),
            now_ms: 0,
            step: WriteStep::Read,
            kept: 0,
        },
        context,
    )
    .await
    .unwrap();
    let after_retry = drive(
        crate::workflows::ScanOperation::<ToolRecord>::of_groups(Some([remote.group_id].into())),
        context,
    )
    .await
    .unwrap();
    assert!(
        after_retry.iter().all(|(_, t)| t.tool_id != "abandoned"),
        "abandoned tool from transfer 91 must not publish under transfer 92"
    );
    // Transfer 92 committing must also delete abandoned transfer 91's durable staged rows.
    let staged_left = context
        .storage_handle
        .send_storage_effect(StorageEffect::Iter {
            key_space: RUN_LOG_KEYSPACE.to_string(),
            prefix: Some(stage_prefix(remote.run_id, gone)),
            start: None,
            limit: 16,
            txn_id: None,
        })
        .await;
    assert!(matches!(
        staged_left,
        aruna_core::events::Event::Storage(aruna_core::events::StorageEvent::IterResult {
            values,
            ..
        }) if values.is_empty()
    ));

    // Missing source row on a replacement holder triggers NotFound so sender backfills from 0.
    context
        .storage_handle
        .send_storage_effect(StorageEffect::Delete {
            key_space: WORKFLOW_SOURCE_KEYSPACE.to_string(),
            key: version_key(wf_id, 1),
            txn_id: None,
        })
        .await;
    let missing_src = drive(
        WriteReplicaOperation {
            peer: gone,
            local: here,
            run: Box::new(remote.clone()),
            first_page: 0,
            pages: Vec::new(),
            workflow: None,
            now_ms: 0,
            step: WriteStep::Read,
            kept: 0,
        },
        context,
    )
    .await;
    assert_eq!(missing_src, Err(CatalogError::NotFound));

    // Malformed source bytes or a digest mismatch with the stored version are rejected.
    for bad_source in [b"not-postcard".to_vec(), wrong_files.to_bytes().unwrap()] {
        let bad_bundle = ReplicatedWorkflow {
            source: Some(bad_source),
            ..ReplicatedWorkflow::default()
        };
        let rejected = drive(
            WriteReplicaOperation {
                peer: gone,
                local: here,
                run: Box::new(remote.clone()),
                first_page: 0,
                pages: Vec::new(),
                workflow: Some(bad_bundle),
                now_ms: 0,
                step: WriteStep::Read,
                kept: 0,
            },
            context,
        )
        .await;
        assert_eq!(rejected, Err(CatalogError::Forbidden));
    }
}
