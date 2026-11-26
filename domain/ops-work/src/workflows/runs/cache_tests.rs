//! Tests of the task cache, resume and repeat on memory storage: scopes, reuse within a group,
//! isolation of groups, the READ recheck, anonymous foreign hits and the lineage of new runs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::keyspaces::WORKFLOW_CACHE_KEYSPACE;
use aruna_core::structs::execution::workflow_run::{
    CacheScope, RunFile, RunState, WorkflowCacheRecord, WorkflowRunRecord, cache_row,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::routing::NodeRouting;
use aruna_ops_data::s3::object::put::{PutObjectError, PutObjectOperation, PutObjectResult};
use aruna_ops_data::testing_objects::put_config;
use aruna_runner::driver::{DriverContext, drive};
use aruna_workflow_contract::HostRpcResult;
use aruna_workflow_contract::host::{SecretPlaceholder, StartTask};
use aruna_workflow_contract::message::HostToCoordinator;
use aruna_workflow_contract::preflight::RunLogEntry;
use serde_json::json;
use ulid::Ulid;

use super::cache::{
    CacheEntry, CacheHit, LookupCacheOperation, StoreCacheOperation, deterministic, namespaces,
    task_scope,
};
use super::control::{Follow, FollowRunOperation};
use super::events::{ReadEventsOperation, task_json};
use super::log::AppendLogOperation;
use super::reads::GetTaskOperation;
use super::run::{FinishRunOperation, Prepared};
use super::tasks::{RunTaskOperation, task_key_of};
use super::tests::{Runs, runs, sample_run, start_task};
use crate::workflows::CatalogError;
use crate::workflows::tests::write;

/// A task whose result depends only on its key: no network or secrets, a pinned image.
fn pinned_task() -> StartTask {
    let mut task = start_task();
    task.image = Some(format!("debian@sha256:{}", "a".repeat(64)));
    task
}

/// The runs' context with a file system blob store below `root`, so objects can be written.
async fn blobs(runs: &Runs, root: &std::path::Path) -> DriverContext {
    aruna_runner::testing::with_blob(&runs.fixture.context, root).await
}

async fn put(
    runs: &Runs,
    context: &DriverContext,
    (bucket, key): (&str, &str),
    data: &'static [u8],
) -> Ulid {
    let fixture = &runs.fixture;
    let config = put_config(
        fixture.alice.realm_id,
        fixture.group_a,
        fixture.node,
        bucket,
        key,
        data,
    );
    let put = drive(PutObjectOperation::new(config), context).await;
    let put: Result<PutObjectResult, PutObjectError> = put;
    put.expect("the object is written").version_id
}

fn output(key: &str, version: Option<Ulid>) -> RunFile {
    RunFile {
        name: "out.txt".into(),
        bucket: Some("results".into()),
        key: Some(key.into()),
        version_id: version.map(|version| version.to_string()),
        ..RunFile::default()
    }
}

fn entry(task_key: &str, outputs: Vec<RunFile>) -> CacheEntry {
    CacheEntry {
        task_key: task_key.into(),
        outputs,
        stdout_tail: b"said".to_vec(),
        stderr_tail: Vec::new(),
        deterministic: true,
        has_secrets: false,
    }
}

async fn lookup(
    context: &DriverContext,
    run: &WorkflowRunRecord,
    scope: CacheScope,
    key: &str,
) -> Option<CacheHit> {
    let lookup = LookupCacheOperation::new(run, scope, key);
    drive(lookup, context).await.expect("the cache is read")
}

/// The same run as created by another caller of another group, or of the same group.
fn run_of(run: &WorkflowRunRecord, auth: &AuthContext, group: Ulid) -> WorkflowRunRecord {
    let mut other = run.clone();
    other.run_id = Ulid::from_bytes([42; 16]);
    other.workflow_id = Ulid::from_bytes([43; 16]);
    other.created_by = auth.user_id;
    other.group_id = group;
    other
}

// Only a deterministic task uses the run's scope; network, secrets, an unpinned image or an
// unpinned input narrow it to the run, as a repeat does for every task.
#[test]
fn narrows_scopes() {
    let mut run = sample_run();
    assert!(deterministic(&pinned_task()));
    assert_eq!(task_scope(&run, &pinned_task()), CacheScope::Group);
    let mut open = pinned_task();
    open.network_allowed = true;
    let mut secret = pinned_task();
    secret.secret_placeholders.push(SecretPlaceholder {
        name: "TOKEN".into(),
        placeholder: "@@TOKEN@@".into(),
        env_name: None,
    });
    let mut unpinned = pinned_task();
    unpinned.direct_inputs[0].address = "aruna://reads/a.txt".into();
    let mut undigested = pinned_task();
    undigested.direct_inputs[0].digest = String::new();
    for task in [open, secret, unpinned, undigested, start_task()] {
        assert!(!deterministic(&task), "{task:?}");
        assert_eq!(task_scope(&run, &task), CacheScope::Run);
    }
    run.cache_scope = CacheScope::Realm;
    let group = format!("group:{}", run.group_id);
    assert_eq!(namespaces(&run, CacheScope::Realm), [group, "realm".into()]);
    run.cache_scope = CacheScope::Run;
    assert_eq!(task_scope(&run, &pinned_task()), CacheScope::Run);
    assert_eq!(
        namespaces(&run, CacheScope::Run),
        [format!("run:{}", run.run_id)]
    );
}

// A result is reused across runs and workflows of its group, never by another group, never
// without READ on its outputs, and never once an output version is gone.
#[tokio::test]
async fn reuses_group_results() {
    let runs = runs().await;
    let root = tempfile::tempdir().unwrap();
    let context = &blobs(&runs, root.path()).await;
    let first = runs.started().await;
    let version = put(&runs, context, ("results", "hello/out.txt"), b"hello").await;
    let store = StoreCacheOperation::new(
        &first,
        CacheScope::Group,
        entry("k1", vec![output("hello/out.txt", None)]),
    );
    let pinned = drive(store, context).await.unwrap();
    assert_eq!(pinned[0].version_id, Some(version.to_string()));
    assert!(pinned[0].digest.is_some());

    let alice = runs.fixture.alice.clone();
    let other = run_of(&first, &alice, runs.fixture.group_a);
    let hit = lookup(context, &other, CacheScope::Group, "k1")
        .await
        .unwrap();
    assert!(!hit.foreign);
    assert_eq!(hit.record.outputs, pinned);
    assert_eq!(hit.record.stdout_tail, b"said");

    let bob = runs.fixture.bob.clone();
    let foreign = run_of(&first, &bob, runs.fixture.group_b);
    assert_eq!(
        lookup(context, &foreign, CacheScope::Group, "k1").await,
        None
    );
    let shared = StoreCacheOperation::new(
        &first,
        CacheScope::Realm,
        entry("k2", vec![output("hello/out.txt", None)]),
    );
    drive(shared, context).await.unwrap();
    // The realm row is there, but bob may not read the outputs of group a.
    assert_eq!(
        lookup(context, &foreign, CacheScope::Realm, "k2").await,
        None
    );
    let stranger = runs.fixture.stranger.clone();
    let revoked = run_of(&first, &stranger, runs.fixture.group_a);
    assert_eq!(
        lookup(context, &revoked, CacheScope::Group, "k1").await,
        None
    );

    let namespace = format!("group:{}", first.group_id);
    let gone = WorkflowCacheRecord {
        task_key: "k3".into(),
        scope_namespace: namespace.clone(),
        exit_code: 0,
        outputs: vec![output("hello/out.txt", Some(Ulid::from_bytes([1; 16])))],
        stdout_tail: Vec::new(),
        stderr_tail: Vec::new(),
        created_at_ms: 1,
        producer_group_id: first.group_id,
        producer_run_id: first.run_id,
        deterministic: true,
        has_secrets: false,
    };
    let row = cache_row(&namespace, "k3").to_vec();
    write(
        context,
        WORKFLOW_CACHE_KEYSPACE,
        row,
        gone.to_bytes().unwrap(),
    )
    .await;
    assert_eq!(lookup(context, &other, CacheScope::Group, "k3").await, None);
    let mut failed = gone;
    failed.task_key = "k4".into();
    failed.exit_code = 1;
    failed.outputs = pinned;
    let row = cache_row(&namespace, "k4").to_vec();
    write(
        context,
        WORKFLOW_CACHE_KEYSPACE,
        row,
        failed.to_bytes().unwrap(),
    )
    .await;
    assert_eq!(lookup(context, &other, CacheScope::Group, "k4").await, None);
}

// A realm hit of another group is copied into the run's area: the task is answered as cached
// without a child, and nothing the run stores or streams names the producer.
#[tokio::test]
async fn hides_foreign_producer() {
    let runs = runs().await;
    let root = tempfile::tempdir().unwrap();
    let context = &blobs(&runs, root.path()).await;
    let mut run = runs.started().await;
    run.cache_scope = CacheScope::Realm;
    let version = put(&runs, context, ("results", "elsewhere/out.txt"), b"shared").await;
    let producer = Ulid::from_bytes([77; 16]);
    let request = pinned_task();
    let key = task_key_of(&request.task_key_seed);
    let record = WorkflowCacheRecord {
        task_key: key.clone(),
        scope_namespace: "realm".into(),
        exit_code: 0,
        outputs: vec![output("elsewhere/out.txt", Some(version))],
        stdout_tail: b"from elsewhere".to_vec(),
        stderr_tail: Vec::new(),
        created_at_ms: 1,
        producer_group_id: runs.fixture.group_b,
        producer_run_id: producer,
        deterministic: true,
        has_secrets: false,
    };
    let row = cache_row("realm", &key).to_vec();
    write(
        context,
        WORKFLOW_CACHE_KEYSPACE,
        row,
        record.to_bytes().unwrap(),
    )
    .await;

    let task = RunTaskOperation::new(run.clone(), runs.call_node().ingress, Box::new(request))
        .routing(NodeRouting::default());
    let answered = tokio::time::timeout(Duration::from_secs(60), drive(task, context))
        .await
        .expect("a cached task needs no child")
        .unwrap();
    let HostRpcResult::TaskFinished(finished) = answered else {
        panic!("a finished task: {answered:?}");
    };
    assert_eq!(finished.exit_code, Some(0));
    assert_eq!(finished.stdout_tail, b"from elsewhere");
    let address = &finished.outputs[0].address;
    assert!(address.contains(&run.run_id.to_string()), "{address}");
    assert!(!address.contains("elsewhere"), "{address}");

    let read = GetTaskOperation::new(&runs.fixture.alice, run.run_id, 7);
    let stored = drive(read, context).await.unwrap();
    assert!(stored.cached && stored.child_job_id.is_none());
    let step = drive(
        super::RunAccessOperation::new(
            &runs.fixture.alice,
            run.run_id,
            crate::workflows::Need::Read,
        ),
        context,
    )
    .await
    .unwrap()
    .0;
    assert_eq!(step.steps[0].cached, 1);
    let events = ReadEventsOperation::new(&runs.fixture.alice, run.run_id, 0, 100);
    let events = drive(events, context).await.unwrap();
    let mut shown: Vec<String> = (events.events.iter())
        .map(|event| event.payload_json.clone())
        .collect();
    shown.push(task_json(&stored).to_string());
    for text in shown {
        assert!(!text.contains(&producer.to_string()), "{text}");
        assert!(!text.contains(&runs.fixture.group_b.to_string()), "{text}");
        assert!(
            !text.contains(&runs.fixture.bob.user_id.to_string()),
            "{text}"
        );
    }
}

// Resume and repeat refuse a run that has not ended; both start a new run of the same lineage,
// a resume with the lineage's run log, a repeat in a cache of its own.
#[tokio::test]
async fn follows_lineage() {
    let runs = runs().await;
    let context = &runs.fixture.context;
    let alice = runs.fixture.alice.clone();
    let first = runs.started().await;
    let resume = |run_id, how| FollowRunOperation::new(&alice, runs.serving(), run_id, how);
    let active = drive(resume(first.run_id, Follow::Resume), context).await;
    assert!(
        matches!(active, Err(CatalogError::Conflict(_))),
        "{active:?}"
    );
    let hidden = FollowRunOperation::new(
        &runs.fixture.stranger,
        runs.serving(),
        first.run_id,
        Follow::Repeat,
    );
    assert_eq!(drive(hidden, context).await, Err(CatalogError::NotFound));

    let begin = RunLogEntry::Begin {
        run_start_json: json!({"run": first.run_id.to_string()}),
    };
    drive(AppendLogOperation::new(first.run_id, begin), context)
        .await
        .unwrap();
    drive(
        super::ChangeRunOperation::new(
            first.run_id,
            vec![super::RunChange::Tools(vec![(
                "reverse".into(),
                "1".into(),
            )])],
        ),
        context,
    )
    .await
    .unwrap();
    let finish = FinishRunOperation::new(first.run_id, RunState::Succeeded, None);
    drive(finish, context).await.unwrap();

    let resumed = drive(resume(first.run_id, Follow::Resume), context)
        .await
        .unwrap();
    assert_ne!(resumed.run_id, first.run_id);
    assert_eq!(resumed.lineage, first.lineage);
    assert_eq!(resumed.resumes_run_id, Some(first.run_id));
    assert_eq!(resumed.cache_scope, CacheScope::Group);
    assert_eq!(resumed.values_json, first.values_json);
    assert_eq!(resumed.used_tools, vec![("reverse".into(), "1".into())]);
    let repeated = drive(resume(first.run_id, Follow::Repeat), context)
        .await
        .unwrap();
    assert_eq!(repeated.lineage, first.lineage);
    assert_eq!(repeated.repeats_run_id, Some(first.run_id));
    assert_eq!(repeated.cache_scope, CacheScope::Run);
    assert!(repeated.used_tools.is_empty());

    let tool = |version: &str| aruna_core::structs::execution::workflow::ToolRecord {
        tool_id: "reverse".into(),
        version: version.into(),
        group_id: first.group_id,
        title: "reverse".into(),
        curation: Default::default(),
        definition: "{}".into(),
        files: Vec::new(),
        created_by: first.created_by,
        created_at_ms: 1,
    };
    let pinned = super::run::filter_tools(&resumed, vec![tool("1"), tool("2")]);
    assert_eq!(pinned.len(), 1);
    assert_eq!(pinned[0].version, "1");
    let unpinned = super::run::filter_tools(&repeated, vec![tool("1"), tool("2")]);
    assert_eq!(unpinned.len(), 2);

    assert!(matches!(runs.prepare(&resumed).await, Prepared::Ready(_)));
    assert!(matches!(runs.prepare(&repeated).await, Prepared::Ready(_)));
    let sent = runs.host.sent();
    let replayed: Vec<_> = sent
        .iter()
        .filter_map(|message| match message {
            HostToCoordinator::RunReplayPages { pages } => Some(pages.len()),
            _ => None,
        })
        .collect();
    assert_eq!(replayed, vec![1]);
    let starts: Vec<_> = (sent.into_iter())
        .filter_map(|message| match message {
            HostToCoordinator::RunStart(start) => Some(start),
            _ => None,
        })
        .collect();
    assert_eq!(starts.len(), 2);
    assert_eq!(starts[0].resume_lineage, Some(first.run_id.to_string()));
    assert!(starts[0].prior_log_pages.is_empty());
    assert_eq!(
        starts[0].cache_namespace,
        Some(format!("group:{}", first.group_id))
    );
    assert_eq!(starts[1].resume_lineage, None);
    assert!(starts[1].prior_log_pages.is_empty());
    assert_eq!(
        starts[1].cache_namespace,
        Some(format!("run:{}", repeated.run_id))
    );
}

// Listing a folder outside the run's own area filters objects and common prefixes through the
// creator's scoped READ permissions so explicitly denied child names are never disclosed.
#[tokio::test]
async fn hides_denied_listing() {
    use aruna_core::structs::identity::auth::{PathRestriction, Permission};
    use aruna_core::structs::storage::blob::bucket_permission_path;
    use aruna_workflow_contract::host::{HostRpcResult, ListEntry};

    let runs = runs().await;
    let root = tempfile::tempdir().unwrap();
    let context = blobs(&runs, root.path()).await;
    let group = runs.fixture.group_a;
    let owner = runs.fixture.alice.user_id;
    let node = runs.fixture.node;
    let realm = runs.fixture.alice.realm_id;
    aruna_runner::testing::seed_bucket(&context, "reads", group, owner, Vec::new()).await;
    for key in [
        "dir/public.txt",
        "dir/hidden.txt",
        "dir/open/a.txt",
        "dir/secret/b.txt",
    ] {
        put(&runs, &context, ("reads", key), b"ok").await;
    }

    let mut record = runs.started().await;
    let base = bucket_permission_path(realm, group, node, "reads");
    record.path_restrictions = Some(vec![
        PathRestriction {
            pattern: format!("{base}/**"),
            permission: Permission::READ,
        },
        PathRestriction {
            pattern: format!("{base}/dir/hidden.txt"),
            permission: Permission::DENY,
        },
        PathRestriction {
            pattern: format!("{base}/dir/secret/**"),
            permission: Permission::DENY,
        },
    ]);
    let listed = drive(
        super::data::ReadDataOperation::new(
            record.clone(),
            node,
            super::data::DataCall::List {
                address: "aruna://reads/dir/".into(),
            },
        ),
        &context,
    )
    .await
    .unwrap();
    let HostRpcResult::StorageList { mut entries } = listed else {
        panic!("expected StorageList, got {listed:?}");
    };
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    assert_eq!(
        entries,
        vec![
            ListEntry {
                name: "open".into(),
                directory: true,
            },
            ListEntry {
                name: "public.txt".into(),
                directory: false,
            },
        ]
    );
}
