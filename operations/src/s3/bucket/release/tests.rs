//! Tests the release membership operation with hand-fed results: pinned versions, access, gone
//! or untagged crates, and coordinators asked for the ranges this node does not coordinate.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::time::SystemTime;

use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::{RealmAuthorizationDocument, RealmConfigDocument};
use aruna_core::structs::storage::bucket_metadata::release_key;
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::release::RangeMembers;
use serde_json::json;

use super::access::LoadAccessOperation;
use super::holder::AskHolderError;
use super::*;
use crate::distribution::test_record::{binding, record, secret};
use crate::driver::{DriverContext, drive};

const BUCKET: &str = "bakta-db";

struct Fixture {
    _temp: tempfile::TempDir,
    context: DriverContext,
    realm_id: RealmId,
    node_id: NodeId,
    group_id: Ulid,
    owner: AuthContext,
    stranger: AuthContext,
}

async fn fixture() -> Fixture {
    let temp = tempfile::tempdir().unwrap();
    let context = DriverContext {
        storage_handle: aruna_storage::FjallStorage::open(temp.path().to_str().unwrap()).unwrap(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };
    let realm_id = RealmId::from_bytes([1; 32]);
    let auth = |seed: u8| AuthContext {
        user_id: UserId::local(Ulid::from_bytes([seed; 16]), realm_id),
        realm_id,
        path_restrictions: None,
        session: None,
    };
    let (owner, stranger) = (auth(2), auth(3));
    let group_id = Ulid::from_bytes([4; 16]);
    let node_id = secret(1).public();
    let actor = Actor {
        node_id,
        user_id: owner.user_id,
        realm_id,
    };
    let realm = RealmAuthorizationDocument::default_realm_doc(realm_id);
    let group = GroupAuthorizationDocument::default_group_doc(owner.user_id, realm_id, group_id);
    let config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    let record = Group {
        display_name: "db".to_string(),
        group_id,
        realm_id,
        roles: group.roles.keys().copied().collect(),
        owner: owner.user_id,
    };
    let realm_key = realm_id.as_bytes().to_vec();
    for (key_space, key, value) in [
        (
            AUTH_KEYSPACE,
            realm_key.clone(),
            realm.to_bytes(&actor).unwrap(),
        ),
        (
            AUTH_KEYSPACE,
            group_id.to_bytes().to_vec(),
            group.to_bytes(&actor).unwrap(),
        ),
        (
            GROUP_KEYSPACE,
            group_id.to_bytes().to_vec(),
            record.to_bytes(&actor).unwrap(),
        ),
        (
            REALM_CONFIG_KEYSPACE,
            realm_key,
            config.to_bytes(&actor).unwrap(),
        ),
    ] {
        let write = StorageEffect::Write {
            key_space: key_space.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        };
        context.storage_handle.send_storage_effect(write).await;
    }
    Fixture {
        _temp: temp,
        context,
        realm_id,
        node_id,
        group_id,
        owner,
        stranger,
    }
}

fn info(group_id: Ulid) -> BucketInfo {
    BucketInfo {
        group_id,
        created_at: SystemTime::UNIX_EPOCH,
        created_by: UserId::default(),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: None,
    }
}

async fn operation(
    fixture: &Fixture,
    (auth, info): (&AuthContext, BucketInfo),
    queries: Vec<MembershipQuery>,
    bearer: Option<&str>,
) -> ReleaseMembersOperation {
    let scope = BucketScope {
        realm_id: fixture.realm_id,
        node_id: fixture.node_id,
        bucket: BUCKET,
        info: &info,
    };
    let access = drive(LoadAccessOperation::new(scope, auth), &fixture.context)
        .await
        .unwrap();
    ReleaseMembersOperation::new(MembersConfig {
        realm_id: fixture.realm_id,
        node_id: fixture.node_id,
        bucket: BUCKET.to_string(),
        info: info.clone(),
        auth: auth.clone(),
        access,
        bearer: bearer.map(str::to_string),
        queries,
        ranges: None,
        retention: false,
    })
}

fn rows(pointers: &[(Ulid, &str)]) -> IoResult {
    let values = (pointers.iter())
        .map(|(version_id, key)| {
            let pointer = ReleasePointer {
                key: key.to_string(),
                label: "6.0".to_string(),
                prefix: "db/".to_string(),
            };
            (
                release_key(BUCKET, *version_id).into(),
                pointer.to_bytes().unwrap().into(),
            )
        })
        .collect();
    IoResult::Page {
        rows: values,
        next: None,
    }
}

fn crate_read(label: &str, pinned: &[String]) -> IoResult {
    let files: Vec<_> = (pinned.iter())
        .map(|id| json!({"@id": id, "@type": "File"}))
        .collect();
    let mut graph = vec![json!({"@id": "./", "@type": "Dataset"})];
    graph.extend(files);
    let bytes = serde_json::to_vec(&json!({ "@graph": graph })).unwrap();
    let metadata = HashMap::from([(RELEASE_TAG.to_string(), label.to_string())]);
    crate_answer(Ok(CrateBytes { metadata, bytes }))
}

fn crate_answer(read: Result<CrateBytes, CrateReadError>) -> IoResult {
    let read = read.map(|found| (found, 0u64));
    IoResult::Child(aruna_core::calls::ChildOutcome::new(read))
}

fn reads_crate(turn: &Step) -> bool {
    matches!(turn, Ok(Next::Call(IoCall::Child(_))))
}

fn output(turn: Step) -> MembersOutput {
    match turn {
        Ok(Next::Done(output)) => output,
        other => panic!("expected the membership answer, got {other:?}"),
    }
}

fn query(key: &str, seed: u8) -> MembershipQuery {
    MembershipQuery {
        key: key.to_string(),
        version_id: Ulid::from_bytes([seed; 16]),
    }
}

#[tokio::test]
async fn pins_exact_versions() {
    let fixture = fixture().await;
    let info = info(fixture.group_id);
    let queries = vec![
        query("db/a.fasta", 10),
        query("db/a.fasta", 11),
        query("x/a", 10),
    ];
    let mut op = operation(&fixture, (&fixture.owner, info.clone()), queries, None).await;
    let scope = BucketScope {
        realm_id: fixture.realm_id,
        node_id: fixture.node_id,
        bucket: BUCKET,
        info: &info,
    };
    let pinned = scope
        .exact("db/a.fasta", Ulid::from_bytes([10; 16]))
        .unwrap()
        .to_id();
    assert!(matches!(op.first(), Ok(Next::Call(IoCall::Page { .. }))));
    let crate_id = Ulid::from_bytes([20; 16]);
    assert!(reads_crate(
        &op.next(rows(&[(crate_id, "db/release.json")]))
    ));
    let output = output(op.next(crate_read("6.0", &[pinned])));
    assert_eq!(
        output.answers[0].releases.len(),
        1,
        "the pinned version is a member"
    );
    assert!(
        output.answers[1].releases.is_empty(),
        "a newer version is not"
    );
    assert!(
        output.answers[2].releases.is_empty(),
        "a key outside the prefix is not"
    );
    assert_eq!(output.releases.len(), 1);
    assert_eq!(output.releases[0].version_id, crate_id);
    assert!(output.complete);
}

#[tokio::test]
async fn access_and_tags() {
    let fixture = fixture().await;
    let info = info(fixture.group_id);
    let crate_id = Ulid::from_bytes([20; 16]);
    // A caller who may not read the crate never has it opened.
    let mut stranger = operation(&fixture, (&fixture.stranger, info.clone()), vec![], None).await;
    let _ = stranger.first();
    let listed = output(stranger.next(rows(&[(crate_id, "db/release.json")])));
    assert!(listed.releases.is_empty());

    // A gone version and a version without the release tag are no release.
    let gone = crate_answer(Err(CrateReadError::Missing));
    for (answer, complete) in [(gone, true), (crate_read("5.0", &[]), true)] {
        let mut op = operation(&fixture, (&fixture.owner, info.clone()), vec![], None).await;
        let _ = op.first();
        assert!(reads_crate(
            &op.next(rows(&[(crate_id, "db/release.json")]))
        ));
        let output = output(op.next(answer));
        assert!(output.releases.is_empty());
        assert_eq!(output.complete, complete);
    }
    // A crate that cannot be read leaves the answer incomplete.
    let failed = crate_answer(Err(CrateReadError::Failed("backend down".into())));
    let mut op = operation(
        &fixture,
        (&fixture.owner, info.clone()),
        vec![query("db/a", 1)],
        None,
    )
    .await;
    let _ = op.first();
    let _ = op.next(rows(&[(crate_id, "db/release.json")]));
    let output = output(op.next(failed));
    assert!(!output.complete && !output.answers[0].complete);
}

#[tokio::test]
async fn asks_range_coordinators() {
    let fixture = fixture().await;
    let (snapshot, bytes) = record(true);
    let mut info = info(fixture.group_id);
    info.distribution = Some(binding(&snapshot, 1, 5));
    let set = |bytes: &Vec<u8>| IoResult::Values(vec![Some(bytes.clone().into()), None]);
    let other = (snapshot.config.ranges.iter())
        .find(|range| range.coordinator != binding(&snapshot, 1, 5).member_id)
        .unwrap();
    let remote_answer = |refused: Vec<Ulid>| {
        let answer: Result<HolderAnswer, AskHolderError> =
            Ok(HolderAnswer::Members(RangeMembers {
                answers: vec![ReleaseMembers::complete()],
                releases: Vec::new(),
                complete: true,
                refused,
            }));
        IoResult::Child(aruna_core::calls::ChildOutcome::new(answer))
    };
    // With a bearer the coordinator of the other range is asked, and its answer counts.
    let queries = vec![query("db/a", 1)];
    let mut op = operation(
        &fixture,
        (&fixture.owner, info.clone()),
        queries.clone(),
        Some("token"),
    )
    .await;
    assert!(matches!(op.first(), Ok(Next::Call(IoCall::Read(_)))));
    let _ = op.next(set(&bytes));
    let asked = op.next(rows(&[]));
    assert!(
        matches!(asked, Ok(Next::Call(IoCall::Child(_)))),
        "{asked:?}"
    );
    assert_eq!(op.config.bearer.as_deref(), Some("token"));
    assert!(
        op.asked
            .last()
            .is_some_and(|(_, ranges)| ranges == &vec![other.range_id])
    );
    assert!(output(op.next(remote_answer(Vec::new()))).complete);

    // A refusing coordinator, or no bearer to ask with, leaves the answer incomplete.
    let mut op = operation(
        &fixture,
        (&fixture.owner, info.clone()),
        queries.clone(),
        Some("token"),
    )
    .await;
    let _ = op.first();
    let _ = op.next(set(&bytes));
    let _ = op.next(rows(&[]));
    let refused = output(op.next(remote_answer(vec![other.range_id])));
    assert!(!refused.answers[0].complete);
    let mut op = operation(&fixture, (&fixture.owner, info), queries, None).await;
    let _ = op.first();
    let _ = op.next(set(&bytes));
    // No other node is asked without a bearer.
    assert!(!output(op.next(rows(&[]))).complete);
}
