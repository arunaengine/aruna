//! Tests what each caller sees of the jobs a node runs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{JobTargetResponse, NodeJobsResponse, visible_jobs};
use crate::server::state::ServerState;
use crate::tests::routes::{seed_group_docs, test_context, test_storage};
use aruna_core::UserId;
use aruna_core::jobs::{JobKind, JobTarget, RunningJobView};
use aruna_core::keys::generate_signing_key;
use aruna_core::structs::execution::job::JobProgress;
use aruna_core::structs::identity::auth::{Actor, AuthContext, NodeCapabilities};
use aruna_core::structs::identity::realm::RealmId;
use aruna_operations::driver::drive;
use aruna_operations::realm::claim_admin::{ClaimInitialInput, ClaimInitialOperation};
use aruna_operations::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
use std::sync::Arc;
use tempfile::TempDir;
use ulid::Ulid;

struct Fixture {
    state: ServerState,
    admin: UserId,
    member: UserId,
    group: Ulid,
    other_group: Ulid,
    _dir: TempDir,
}

async fn fixture() -> Fixture {
    let (dir, storage) = test_storage();
    let context = Arc::new(test_context(storage));
    let realm_key = generate_signing_key();
    let realm_id = RealmId::from_bytes(realm_key.verifying_key().to_bytes());
    let node_id = iroh::SecretKey::generate().public();
    let admin = UserId::local(Ulid::generate(), realm_id);
    let actor = Actor {
        node_id,
        user_id: admin,
        realm_id,
    };
    let realm = CreateRealmConfig {
        actor: actor.clone(),
        realm_description: "Realm".to_string(),
        oidc_providers: vec![],
        node_location: None,
        node_weight: None,
        node_labels: Default::default(),
    };
    drive(CreateRealmOperation::new(realm), &context)
        .await
        .unwrap();
    let claim = ClaimInitialInput {
        actor: actor.clone(),
    };
    drive(ClaimInitialOperation::new(claim), &context)
        .await
        .unwrap();
    let member = UserId::local(Ulid::generate(), realm_id);
    let stranger = UserId::local(Ulid::generate(), realm_id);
    let (group, other_group) = (Ulid::generate(), Ulid::generate());
    seed_group_docs(&context, realm_id, &actor, group, "bakta", member).await;
    seed_group_docs(&context, realm_id, &actor, other_group, "soil", stranger).await;
    let state = ServerState::new(
        context,
        realm_id,
        node_id,
        NodeCapabilities::management_node(realm_key).unwrap(),
        false,
        None,
        aruna_operations::jobs::runtime::JobsRuntime::new(),
    )
    .await;
    Fixture {
        state,
        admin,
        member,
        group,
        other_group,
        _dir: dir,
    }
}

fn auth(fixture: &Fixture, user_id: UserId) -> AuthContext {
    AuthContext {
        user_id,
        realm_id: fixture.state.get_realm_id(),
        path_restrictions: None,
        session: None,
    }
}

fn copy_job(group: Ulid, bucket: &str, created_by: UserId, started: u64) -> RunningJobView {
    RunningJobView {
        kind: JobKind::CopyObject,
        target: Some(JobTarget::Bucket {
            bucket: bucket.to_string(),
            path: Some("assemblies/2025/".to_string()),
            exact: false,
        }),
        group_id: Some(group),
        created_by,
        progress: Some(JobProgress {
            total: Some(10),
            current: 4,
            ..JobProgress::new("bytes")
        }),
        started_at_ms: started,
    }
}

/// A member's run, another group's copy and a dataset job of nobody's group.
fn node_jobs(fixture: &Fixture) -> Vec<RunningJobView> {
    let stranger = UserId::local(Ulid::generate(), fixture.state.get_realm_id());
    let dataset = RunningJobView {
        kind: JobKind::RegisterIdentifiers,
        target: Some(JobTarget::Dataset {
            dataset_id: Ulid::generate(),
        }),
        group_id: None,
        created_by: stranger,
        progress: None,
        started_at_ms: 3,
    };
    vec![
        dataset,
        copy_job(fixture.other_group, "soil-metagenomes", stranger, 1),
        copy_job(fixture.group, "bakta-runs", stranger, 2),
    ]
}

fn buckets(response: &NodeJobsResponse) -> Vec<&str> {
    let targets = response.jobs.iter().filter_map(|job| job.target.as_ref());
    targets
        .filter_map(|target| match target {
            JobTargetResponse::Bucket { bucket, .. } => Some(bucket.as_str()),
            JobTargetResponse::Dataset { .. } => None,
        })
        .collect()
}

#[tokio::test]
async fn admin_sees_everything() {
    let fixture = fixture().await;
    let caller = auth(&fixture, fixture.admin);
    let response = visible_jobs(&fixture.state, Some(&caller), node_jobs(&fixture))
        .await
        .unwrap();
    assert_eq!(response.hidden, 0);
    assert_eq!(response.jobs.len(), 3);
    assert_eq!(buckets(&response), ["soil-metagenomes", "bakta-runs"]);
    assert!(response.jobs.iter().all(|job| job.started_at.is_some()));
    let dataset = &response.jobs[2];
    assert_eq!(dataset.kind, "register_identifiers");
    assert!(matches!(
        dataset.target,
        Some(JobTargetResponse::Dataset { .. })
    ));
    assert_eq!(response.jobs[0].progress.as_ref().unwrap().total, Some(10));
}

#[tokio::test]
async fn member_sees_group() {
    let fixture = fixture().await;
    let caller = auth(&fixture, fixture.member);
    let response = visible_jobs(&fixture.state, Some(&caller), node_jobs(&fixture))
        .await
        .unwrap();
    assert_eq!(buckets(&response), ["bakta-runs"]);
    assert_eq!(response.jobs.len(), 1, "no other group's bucket or dataset");
    assert_eq!(response.hidden, 2);
}

#[tokio::test]
async fn creator_sees_job() {
    let fixture = fixture().await;
    let mut jobs = node_jobs(&fixture);
    jobs[0].created_by = fixture.member;
    let mut caller = auth(&fixture, fixture.member);
    let response = visible_jobs(&fixture.state, Some(&caller), jobs.clone())
        .await
        .unwrap();
    assert_eq!((response.jobs.len(), response.hidden), (2, 1));

    // A path-restricted token sees only what its paths allow.
    caller.path_restrictions = Some(Vec::new());
    let response = visible_jobs(&fixture.state, Some(&caller), jobs)
        .await
        .unwrap();
    assert!(response.jobs.is_empty());
    assert_eq!(response.hidden, 3);
}

#[tokio::test]
async fn visitor_sees_kinds() {
    let fixture = fixture().await;
    let foreign = AuthContext {
        realm_id: RealmId::from_bytes([7u8; 32]),
        ..auth(&fixture, fixture.admin)
    };
    for caller in [None, Some(&foreign)] {
        let response = visible_jobs(&fixture.state, caller, node_jobs(&fixture))
            .await
            .unwrap();
        assert_eq!(response.hidden, 0);
        let kinds: Vec<_> = response.jobs.iter().map(|job| job.kind.as_str()).collect();
        assert_eq!(
            kinds,
            ["copy_object", "copy_object", "register_identifiers"]
        );
        assert!(response.jobs.iter().all(|job| {
            job.target.is_none() && job.progress.is_none() && job.started_at.is_none()
        }));
    }
}

#[tokio::test]
async fn deny_below_hides() {
    // A deny below the job's prefix hides the job; a deny beside it does not.
    use aruna_core::structs::identity::auth::{PathRestriction, Permission};
    use aruna_core::structs::storage::blob::bucket_permission_path;
    let fixture = fixture().await;
    let (realm_id, node_id) = (fixture.state.get_realm_id(), fixture.state.get_node_id());
    let root = bucket_permission_path(realm_id, fixture.group, node_id, "bakta-runs");
    let caller = |denied: &str| AuthContext {
        path_restrictions: Some(vec![
            PathRestriction {
                pattern: root.clone(),
                permission: Permission::READ,
            },
            PathRestriction {
                pattern: format!("{root}/**"),
                permission: Permission::READ,
            },
            PathRestriction {
                pattern: format!("{root}/{denied}"),
                permission: Permission::DENY,
            },
        ]),
        ..auth(&fixture, fixture.member)
    };
    let jobs = || vec![copy_job(fixture.group, "bakta-runs", fixture.admin, 1)];
    let below = caller("assemblies/2025/private/**");
    let response = visible_jobs(&fixture.state, Some(&below), jobs())
        .await
        .unwrap();
    assert_eq!((response.jobs.len(), response.hidden), (0, 1));
    let beside = caller("reads/**");
    let response = visible_jobs(&fixture.state, Some(&beside), jobs())
        .await
        .unwrap();
    assert_eq!((response.jobs.len(), response.hidden), (1, 0));
}

/// A job of the member's group on `path` of the test bucket, an exact key or a prefix.
fn target_job(fixture: &Fixture, path: &str, exact: bool) -> RunningJobView {
    RunningJobView {
        target: Some(JobTarget::Bucket {
            bucket: "bakta-runs".to_string(),
            path: Some(path.to_string()),
            exact,
        }),
        ..copy_job(fixture.group, "bakta-runs", fixture.admin, 1)
    }
}

/// The member's view of `jobs` with a token restricted to `patterns`, each a READ below the
/// test bucket's path on node `node`.
async fn restricted(
    fixture: &Fixture,
    node: &str,
    patterns: &[&str],
    jobs: Vec<RunningJobView>,
) -> NodeJobsResponse {
    use aruna_core::structs::identity::auth::{PathRestriction, Permission};
    let realm_id = fixture.state.get_realm_id();
    let root = format!("/{realm_id}/g/{}/data/{node}/bakta-runs", fixture.group);
    let restrictions = (patterns.iter())
        .map(|pattern| PathRestriction {
            pattern: format!("{root}{pattern}"),
            permission: Permission::READ,
        })
        .collect();
    let caller = AuthContext {
        path_restrictions: Some(restrictions),
        ..auth(fixture, fixture.member)
    };
    visible_jobs(&fixture.state, Some(&caller), jobs)
        .await
        .unwrap()
}

#[tokio::test]
async fn prefix_needs_all() {
    // READ on the key `private` shows a copy to that key, never a purge of every key that
    // starts with it; a grant on all keys below shows the purge.
    let fixture = fixture().await;
    let node = fixture.state.get_node_id().to_string();
    let jobs = || {
        vec![
            target_job(&fixture, "private", true),
            target_job(&fixture, "private", false),
            target_job(&fixture, "private/", false),
        ]
    };
    let key = restricted(&fixture, &node, &["", "/private"], jobs()).await;
    assert_eq!((key.jobs.len(), key.hidden), (1, 2));
    let all = restricted(&fixture, &node, &["", "/**"], jobs()).await;
    assert_eq!((all.jobs.len(), all.hidden), (3, 0));
    // The distributed bucket grant names every node by a wildcard segment.
    let any_node = restricted(&fixture, "*", &["", "/**"], jobs()).await;
    assert_eq!((any_node.jobs.len(), any_node.hidden), (3, 0));
}

#[tokio::test]
async fn path_policy_hides() {
    // A group policy that decides by path may deny a key below a prefix, so a prefix job is
    // hidden while an exact key it allows still shows.
    use aruna_core::keyspaces::AUTH_KEYSPACE;
    use aruna_core::request_policy::{PolicyKind, RequestPolicy};
    use aruna_core::structs::identity::group::GroupAuthorizationDocument;
    let fixture = fixture().await;
    let realm_id = fixture.state.get_realm_id();
    let actor = Actor {
        node_id: fixture.state.get_node_id(),
        user_id: fixture.admin,
        realm_id,
    };
    let mut document =
        GroupAuthorizationDocument::default_group_doc(fixture.member, realm_id, fixture.group);
    document.policies = vec![RequestPolicy {
        policy_id: Ulid::from_parts(3, 3),
        name: "no secrets".to_string(),
        kind: PolicyKind::Deny,
        when: None,
        expression: "path.endsWith('/secret')".to_string(),
        enabled: true,
    }];
    let bytes = document.to_bytes(&actor).unwrap();
    let key = fixture.group.to_bytes().to_vec().into();
    crate::tests::routes::write_doc(&fixture.state.get_ctx(), AUTH_KEYSPACE, key, bytes.into())
        .await;
    let caller = auth(&fixture, fixture.member);
    let jobs = vec![
        target_job(&fixture, "data", true),
        target_job(&fixture, "data", false),
    ];
    let response = visible_jobs(&fixture.state, Some(&caller), jobs)
        .await
        .unwrap();
    assert_eq!((response.jobs.len(), response.hidden), (1, 1));
}
