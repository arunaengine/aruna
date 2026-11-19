//! Tests candidate opens, hash lookups, consent and entity resolution of exports on one node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::tests::hash_lookup;
use super::*;
use crate::auth::enforcement::PolicyBulkOperation;
use crate::auth::permission_rules::PermissionRulesConfig;
use crate::auth::permission_rules::PermissionRulesOperation;
use crate::jobs::export::archive::failure_outcome;
use crate::jobs::export::consent::UnheldBucketOperation;
use crate::jobs::export::resolve::CandidateSource;
use crate::jobs::export::resolve::ExportCandidate;
use crate::jobs::export::sources::{AliasState, ResolveEntriesOperation, alias_allowed};
use crate::replication::bao_read::BaoReadOutput;
use crate::replication::plaintext::HolderCheckOperation;
use crate::tests::export::{
    crate_document, job_context, open_candidate, probe_sources, recognized_entities, scope_of,
};
use aruna_core::UserId;
use aruna_core::effects::BlobEffect;
use aruna_core::effects::StorageEffect;
use aruna_core::events::BlobEvent;
use aruna_core::events::Event;
use aruna_core::events::StorageEvent;
use aruna_core::id::NodeId;
use aruna_core::jobs::JobRunOutcome;
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, BLOB_HEAD_KEYSPACE, GROUP_KEYSPACE, PATHS_INDEX_KEYSPACE, REALM_CONFIG_KEYSPACE,
};
use aruna_core::keyspaces::{BLOB_VERSIONS_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::structs::execution::job::ExportReportSource;
use aruna_core::structs::execution::job::ExportRoCrateSpec;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::group::GroupAuthorizationDocument;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
use aruna_core::structs::storage::blob::BackendRef;
use aruna_core::structs::storage::blob::HashIndex;
use aruna_core::structs::storage::blob::{
    BlobVersion, BucketInfo, VersionKey, object_permission_path,
};
use aruna_core::structs::storage::encryption::BucketKeyRef;
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_drivers::driver::DriverContext;
use aruna_drivers::driver::drive;
use aruna_test_support::export::{
    BaoNode, FIXTURE_BYTES, byte_stream, encrypting_bucket, export_node, remote_spec, seed_bao,
};
use futures_util::StreamExt;
use serde_json::json;
use std::collections::BTreeMap;
use ulid::Ulid;

/// Seeds one node with a locally stored object and the checkpoint candidate
/// that names it.
async fn local_candidate() -> (BaoNode, UserId, ExportCandidate) {
    let realm_id = RealmId::from_bytes([101; 32]);
    let owner = UserId::local(Ulid::from_bytes([102; 16]), realm_id);
    let group_id = Ulid::from_bytes([103; 16]);
    let version_id = Ulid::from_bytes([104; 16]);
    let node = export_node(realm_id, false).await;
    let node_id = node.net.node_id();
    let Event::Blob(BlobEvent::WriteFinished { location }) = node
        .driver
        .blob_handle
        .as_ref()
        .unwrap()
        .send_blob_effect(BlobEffect::Write {
            resolved: aruna_core::structs::storage::blob::ResolvedBackend::node_default(),
            bucket: "remote".to_string(),
            key: "payload".to_string(),
            created_by: owner,
            blob: byte_stream(FIXTURE_BYTES),
            size: None,
        })
        .await
    else {
        panic!("local blob write failed")
    };
    let hash: [u8; 32] = location.get_blake3().unwrap().try_into().unwrap();
    seed_bao(&node, node_id, owner, group_id, version_id, &location).await;
    let candidate = ExportCandidate {
        source: CandidateSource::Local {
            location,
            group_id,
            permission_path: object_permission_path(
                realm_id, group_id, node_id, "remote", "payload",
            ),
            node_id,
            bucket: "remote".to_string(),
            key: "payload".to_string(),
        },
        report_source: ExportReportSource::Local,
        resolved_version: Some(version_id),
        expected_blake3: Some(hash),
    };
    (node, owner, candidate)
}

/// One payload stored twice on a node: a plain copy in encrypting bucket `remote` and one in
/// plain bucket `plain`, both with a hash alias. A viewer reads both but holds no bucket key.
async fn two_copies() -> (BaoNode, UserId, UserId, [ExportCandidate; 2]) {
    let realm_id = RealmId::from_bytes([131; 32]);
    let owner = UserId::local(Ulid::from_bytes([132; 16]), realm_id);
    let viewer = UserId::local(Ulid::from_bytes([133; 16]), realm_id);
    let group_id = Ulid::from_bytes([134; 16]);
    let versions = [Ulid::from_bytes([135; 16]), Ulid::from_bytes([136; 16])];
    let node = export_node(realm_id, false).await;
    let node_id = node.net.node_id();
    let blob = node.driver.blob_handle.as_ref().unwrap();
    let Event::Blob(BlobEvent::WriteFinished { location }) = blob
        .send_blob_effect(BlobEffect::Write {
            resolved: aruna_core::structs::storage::blob::ResolvedBackend::node_default(),
            bucket: "remote".to_string(),
            key: "payload".to_string(),
            created_by: owner,
            blob: byte_stream(FIXTURE_BYTES),
            size: None,
        })
        .await
    else {
        panic!("blob write failed")
    };
    let hash: [u8; 32] = location.get_blake3().unwrap().try_into().unwrap();
    seed_bao(&node, node_id, owner, group_id, versions[0], &location).await;
    encrypting_bucket(&node, "remote", Ulid::from_bytes([137; 16])).await;
    let mut group_auth = GroupAuthorizationDocument::default_group_doc(owner, realm_id, group_id);
    let role = group_auth
        .roles
        .values_mut()
        .find(|role| role.name == "viewer");
    role.unwrap().assigned_users.insert(viewer);
    let group = aruna_core::structs::identity::group::Group {
        display_name: "export".to_string(),
        group_id,
        realm_id,
        roles: group_auth.roles.keys().copied().collect(),
        owner,
    };
    let actor = Actor {
        node_id,
        user_id: owner,
        realm_id,
    };
    let bucket = BucketInfo {
        group_id,
        created_at: std::time::SystemTime::UNIX_EPOCH,
        created_by: owner,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: None,
    };
    let version = BlobVersion::materialized(
        hash,
        BackendRef::node_default(),
        location.format.encoding(),
        std::time::SystemTime::UNIX_EPOCH,
        owner,
        None,
    );
    let group_key = group_id.to_bytes().to_vec();
    let plain_key = VersionKey::new("plain", "payload", versions[1]);
    let mut writes = vec![
        (
            AUTH_KEYSPACE.to_string(),
            group_key.clone().into(),
            group_auth.to_bytes(&actor).unwrap().into(),
        ),
        (
            GROUP_KEYSPACE.to_string(),
            group_key.into(),
            group.to_bytes(&actor).unwrap().into(),
        ),
        (
            S3_BUCKET_KEYSPACE.to_string(),
            b"plain".to_vec().into(),
            bucket.to_bytes().unwrap().into(),
        ),
        (
            BLOB_VERSIONS_KEYSPACE.to_string(),
            plain_key.to_bytes().unwrap().into(),
            version.to_bytes().unwrap().into(),
        ),
    ];
    let buckets = ["remote", "plain"];
    for (bucket, version_id) in buckets.into_iter().zip(versions) {
        let alias = HashIndex::new(
            hash, version_id, realm_id, group_id, node_id, bucket, "payload",
        );
        let key = alias.to_bytes().unwrap().into();
        writes.push((PATHS_INDEX_KEYSPACE.to_string(), key, Vec::new().into()));
    }
    let write = StorageEffect::BatchWrite {
        writes,
        txn_id: None,
    };
    assert!(matches!(
        node.driver.storage_handle.send_storage_effect(write).await,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));
    let candidates = [0, 1].map(|index| ExportCandidate {
        source: CandidateSource::Local {
            location: location.clone(),
            group_id,
            permission_path: object_permission_path(
                realm_id,
                group_id,
                node_id,
                buckets[index],
                "payload",
            ),
            node_id,
            bucket: buckets[index].to_string(),
            key: "payload".to_string(),
        },
        report_source: ExportReportSource::Hash,
        resolved_version: Some(versions[index]),
        expected_blake3: Some(hash),
    });
    (node, owner, viewer, candidates)
}

#[tokio::test]
async fn stale_holder_offline() {
    let realm_id = RealmId::from_bytes([61; 32]);
    let node = export_node(realm_id, false).await;
    node.net.shutdown().await;
    let hash = [62; 32];
    let candidate = ExportCandidate {
        source: CandidateSource::RemoteHash {
            node_id: iroh::SecretKey::from_bytes(&[63; 32]).public(),
            hash,
        },
        report_source: ExportReportSource::Hash,
        resolved_version: None,
        expected_blake3: Some(hash),
    };

    assert!(matches!(
        open_candidate(
            node.driver.as_ref(),
            &remote_spec(realm_id, UserId::nil(realm_id)),
            &candidate,
            true,
        )
        .await
        .unwrap(),
        CandidateOpen::Status(OpenStatus::Offline)
    ));
}

#[tokio::test]
async fn selection_reads_locally() {
    // An export into another realm never reads a selected file through another node.
    let realm_id = RealmId::from_bytes([64; 32]);
    let node = export_node(realm_id, false).await;
    let hash = [65; 32];
    let candidate = ExportCandidate {
        source: CandidateSource::RemoteHash {
            node_id: iroh::SecretKey::from_bytes(&[66; 32]).public(),
            hash,
        },
        report_source: ExportReportSource::Hash,
        resolved_version: None,
        expected_blake3: Some(hash),
    };
    let mut spec = remote_spec(realm_id, UserId::nil(realm_id));
    spec.selection = Some(aruna_core::structs::execution::job::ExportSelection {
        files: vec!["data.csv".to_string()],
        audience: RealmId::from_bytes([67; 32]),
        intent_digest: String::new(),
    });
    let opened = open_candidate(node.driver.as_ref(), &spec, &candidate, true).await;
    assert!(matches!(
        opened.unwrap(),
        CandidateOpen::Status(OpenStatus::Denied)
    ));
    node.net.shutdown().await;
}

#[tokio::test]
async fn sealed_local_export() {
    use aruna_core::compute::{SecretBytes, SharedSecret};
    use aruna_core::structs::storage::encryption::{SealPlan, public_key_of};
    use aruna_core::structs::storage::format::StoredLayout;

    let realm_id = RealmId::from_bytes([111; 32]);
    let owner = UserId::local(Ulid::from_bytes([112; 16]), realm_id);
    let group_id = Ulid::from_bytes([113; 16]);
    let version_id = Ulid::from_bytes([114; 16]);
    let node = export_node(realm_id, false).await;
    let node_id = node.net.node_id();
    let blob = node.driver.blob_handle.as_ref().unwrap();
    let private = || SecretBytes::new(vec![9; 32]);
    let key = BucketKeyRef::new(Ulid::from_bytes([115; 16]), 1);
    let plan = SealPlan {
        key,
        public_key: public_key_of(&private()).unwrap(),
        cipher: Default::default(),
        block_keys: Default::default(),
        storage_generation: 1,
    };
    let resolved = aruna_core::structs::storage::blob::ResolvedBackend::node_default()
        .with_encryption(Some(plan));
    let Event::Blob(BlobEvent::WriteFinished { location }) = blob
        .send_blob_effect(BlobEffect::Write {
            resolved,
            bucket: "remote".to_string(),
            key: "payload".to_string(),
            created_by: owner,
            blob: byte_stream(FIXTURE_BYTES),
            size: None,
        })
        .await
    else {
        panic!("sealed blob write failed")
    };
    assert!(matches!(location.format.layout, StoredLayout::Pithos(_)));
    let hash: [u8; 32] = location.get_blake3().unwrap().try_into().unwrap();
    seed_bao(&node, node_id, owner, group_id, version_id, &location).await;
    let candidate = ExportCandidate {
        source: CandidateSource::Local {
            location,
            group_id,
            permission_path: object_permission_path(
                realm_id, group_id, node_id, "remote", "payload",
            ),
            node_id,
            bucket: "remote".to_string(),
            key: "payload".to_string(),
        },
        report_source: ExportReportSource::Local,
        resolved_version: Some(version_id),
        expected_blake3: Some(hash),
    };
    let spec = remote_spec(realm_id, owner);
    let driver = node.driver.as_ref();

    // Locked, the input parks the export on its key instead of counting as offline.
    let opened = open_candidate(driver, &spec, &candidate, false).await;
    let wait = match opened {
        Err(ExportFailure::KeyLocked(wait)) => wait,
        Err(error) => panic!("a locked input must wait for its key, got {error:?}"),
        Ok(CandidateOpen::Status(status)) => panic!("a locked input got status {status:?}"),
        Ok(CandidateOpen::Opened(_)) => panic!("a locked input must not open"),
    };
    assert_eq!((wait.key, wait.node_id), (key, node_id));
    assert!(matches!(
        failure_outcome(ExportFailure::KeyLocked(wait)),
        JobRunOutcome::AwaitingKey(waits) if waits.len() == 1
    ));

    // Unlocked, the sealed copy is read under a lease and yields the original bytes.
    let prepare = BlobEffect::PrepareKey {
        key,
        public_key: plan.public_key,
        private_key: SharedSecret::new(private()),
        duration: None,
        max: None,
    };
    let Event::Blob(BlobEvent::KeyPrepared { ticket }) = blob.send_blob_effect(prepare).await
    else {
        panic!("prepare failed")
    };
    blob.send_blob_effect(BlobEffect::ActivateKey { ticket })
        .await;
    let CandidateOpen::Opened(BaoReadOutput::Stream { mut blob, .. }) =
        open_candidate(driver, &spec, &candidate, false)
            .await
            .unwrap()
    else {
        panic!("an unlocked sealed input must open")
    };
    let mut received = Vec::new();
    while let Some(chunk) = blob.next().await {
        received.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(received, FIXTURE_BYTES);

    // An export into another realm refuses this copy for a reader without the key.
    let reader = UserId::local(Ulid::from_bytes([116; 16]), realm_id);
    let mut group_auth = GroupAuthorizationDocument::default_group_doc(owner, realm_id, group_id);
    let viewer = group_auth
        .roles
        .values_mut()
        .find(|role| role.name == "viewer");
    viewer.unwrap().assigned_users.insert(reader);
    let group = aruna_core::structs::identity::group::Group {
        display_name: "export".to_string(),
        group_id,
        realm_id,
        roles: group_auth.roles.keys().copied().collect(),
        owner,
    };
    let actor = Actor {
        node_id,
        user_id: owner,
        realm_id,
    };
    let key = group_id.to_bytes().to_vec();
    let writes = vec![
        (
            AUTH_KEYSPACE.to_string(),
            key.clone().into(),
            group_auth.to_bytes(&actor).unwrap().into(),
        ),
        (
            GROUP_KEYSPACE.to_string(),
            key.into(),
            group.to_bytes(&actor).unwrap().into(),
        ),
    ];
    let write = StorageEffect::BatchWrite {
        writes,
        txn_id: None,
    };
    driver.storage_handle.send_storage_effect(write).await;
    let mut spec = remote_spec(realm_id, reader);
    spec.selection = Some(aruna_core::structs::execution::job::ExportSelection {
        files: Vec::new(),
        audience: RealmId::from_bytes([117; 32]),
        intent_digest: String::new(),
    });
    assert!(matches!(
        open_candidate(driver, &spec, &candidate, true).await,
        Ok(CandidateOpen::Status(OpenStatus::NotHolder))
    ));
    node.net.shutdown().await;
}

#[tokio::test]
async fn revalidates_local_candidate() {
    // A checkpoint outlives the state it was planned from, so a candidate
    // naming a stale group, path, or hash must be refused, not opened.
    let (node, owner, candidate) = local_candidate().await;
    let spec = remote_spec(owner.realm_id, owner);
    let driver = node.driver.as_ref();

    assert!(matches!(
        open_candidate(driver, &spec, &candidate, true).await.unwrap(),
        CandidateOpen::Opened(BaoReadOutput::Metadata { blake3, .. })
            if Some(blake3) == candidate.expected_blake3
    ));

    let mut foreign_group = candidate.clone();
    let CandidateSource::Local { group_id, .. } = &mut foreign_group.source else {
        panic!("expected a local candidate")
    };
    *group_id = Ulid::from_bytes([105; 16]);
    let mut foreign_path = candidate.clone();
    let CandidateSource::Local {
        permission_path, ..
    } = &mut foreign_path.source
    else {
        panic!("expected a local candidate")
    };
    *permission_path = format!("{permission_path}-sibling");
    let mut stale_hash = candidate.clone();
    stale_hash.expected_blake3 = Some([106; 32]);

    for stale in [foreign_group, foreign_path, stale_hash] {
        assert!(matches!(
            open_candidate(driver, &spec, &stale, true).await.unwrap(),
            CandidateOpen::Status(OpenStatus::Missing)
        ));
    }
    node.net.shutdown().await;
}

#[tokio::test]
async fn denies_local_read() {
    // Each refusal toggles one input away from an open that succeeded: first
    // the requesting identity, then a realm policy denying the export read.
    let (node, owner, candidate) = local_candidate().await;
    let realm_id = owner.realm_id;
    let driver = node.driver.as_ref();

    assert!(matches!(
        open_candidate(driver, &remote_spec(realm_id, owner), &candidate, true)
            .await
            .unwrap(),
        CandidateOpen::Opened(_)
    ));

    let stranger = UserId::local(Ulid::from_bytes([107; 16]), realm_id);
    assert!(matches!(
        open_candidate(driver, &remote_spec(realm_id, stranger), &candidate, true)
            .await
            .unwrap(),
        CandidateOpen::Status(OpenStatus::Denied)
    ));

    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.ensure_node(node.net.node_id(), RealmNodeKind::Server);
    config
        .request_policies
        .push(aruna_core::request_policy::RequestPolicy {
            policy_id: Ulid::from_bytes([108; 16]),
            name: "no export reads".to_string(),
            kind: aruna_core::request_policy::PolicyKind::Deny,
            when: None,
            expression: "operation == 's3.GetObject'".to_string(),
            enabled: true,
        });
    let actor = Actor {
        node_id: node.net.node_id(),
        user_id: owner,
        realm_id,
    };
    assert!(matches!(
        driver
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: REALM_CONFIG_KEYSPACE.to_string(),
                key: realm_id.as_bytes().to_vec().into(),
                value: config.to_bytes(&actor).unwrap().into(),
                txn_id: None,
            })
            .await,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));

    assert!(matches!(
        open_candidate(driver, &remote_spec(realm_id, owner), &candidate, true)
            .await
            .unwrap(),
        CandidateOpen::Status(OpenStatus::Denied)
    ));
    node.net.shutdown().await;
}

#[tokio::test]
async fn alias_needs_scopes() {
    // A hash alias is authorized only from rules and policy loaded for its
    // own group; an unloaded scope must fail closed instead of allowing.
    let (node, owner, candidate) = local_candidate().await;
    let CandidateSource::Local {
        group_id,
        permission_path,
        ..
    } = &candidate.source
    else {
        panic!("expected a local candidate")
    };
    let spec = remote_spec(owner.realm_id, owner);
    let ctx = job_context(node.driver.clone(), node.net.node_id());
    let auth = &spec.auth_context;
    let config = PermissionRulesConfig {
        auth_context: auth.clone(),
        path: format!("/{}/g/{group_id}", auth.realm_id),
    };
    let loaded = drive(PermissionRulesOperation::new(config), ctx.driver.as_ref());
    let rules = BTreeMap::from([(*group_id, loaded.await.unwrap())]);
    let scope = (auth.realm_id, *group_id);
    let loaded = drive(PolicyBulkOperation::new([scope]), ctx.driver.as_ref());
    let evaluator = loaded.await.unwrap().remove(&scope).unwrap();
    let policies = BTreeMap::from([(*group_id, evaluator)]);

    assert!(alias_allowed(&spec, *group_id, permission_path, &rules, &policies).unwrap());
    assert!(!alias_allowed(&spec, *group_id, "/other/object", &rules, &policies).unwrap());
    assert!(matches!(
        alias_allowed(&spec, *group_id, permission_path, &BTreeMap::new(), &policies),
        Err(ExportFailure::Retryable(message)) if message == "authorization rules unavailable"
    ));
    assert!(matches!(
        alias_allowed(&spec, *group_id, permission_path, &rules, &BTreeMap::new()),
        Err(ExportFailure::Retryable(message)) if message == "object policy unavailable"
    ));
    node.net.shutdown().await;
}

#[tokio::test]
async fn denies_foreign_alias() {
    // A hash alias in a group whose authorization document this node cannot
    // read must deny that alias, not fail the export with a retryable error.
    let (node, owner, candidate) = local_candidate().await;
    let realm_id = owner.realm_id;
    let hash = candidate.expected_blake3.unwrap();
    let foreign = Ulid::from_bytes([200; 16]);
    let alias = HashIndex::new(
        hash,
        Ulid::from_bytes([201; 16]),
        realm_id,
        foreign,
        node.net.node_id(),
        "restricted",
        "secret.csv",
    );
    assert!(matches!(
        node.driver
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: PATHS_INDEX_KEYSPACE.to_string(),
                key: alias.to_bytes().unwrap().into(),
                value: Vec::new().into(),
                txn_id: None,
            })
            .await,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));

    let spec = remote_spec(realm_id, owner);
    let ctx = job_context(node.driver.clone(), node.net.node_id());
    let found = hash_lookup(&ctx, &spec, hash)
        .await
        .expect("a foreign group must deny instead of failing the export");

    assert!(found.denied);
    assert!(!found.candidates.iter().any(|candidate| matches!(
        &candidate.source,
        CandidateSource::Local { group_id, .. } if *group_id == foreign
    )));
    node.net.shutdown().await;
}

#[tokio::test]
async fn foreign_arn_located() {
    // A versioned ARN of another realm with an `s3://` location of this node resolves to the
    // current version there, like a File entity without an ARN.
    let (node, owner, candidate) = local_candidate().await;
    let realm_id = owner.realm_id;
    let version = candidate.resolved_version.unwrap();
    let head = aruna_core::structs::storage::blob::CurrentVersionPointer::new(version);
    let head_key = aruna_core::structs::storage::blob::BlobHeadKey::new("remote", "payload");
    let write = StorageEffect::Write {
        key_space: BLOB_HEAD_KEYSPACE.to_string(),
        key: head_key.to_bytes().unwrap().into(),
        value: head.to_bytes().unwrap().into(),
        txn_id: None,
    };
    node.driver.storage_handle.send_storage_effect(write).await;
    let other = RealmId::from_bytes([107; 32]);
    let foreign = VersionedObjectArn::new(
        other,
        node.net.node_id(),
        "other",
        "file",
        Ulid::from_bytes([108; 16]),
    )
    .unwrap();
    let document = json!({"@graph": [{"@id": foreign.to_string(), "@type": "File",
        "contentUrl": "s3://remote/payload"}]});
    let mut checkpoint = ExportCheckpoint {
        entities: recognized_entities(&document, realm_id).unwrap(),
        ..Default::default()
    };
    let ctx = job_context(node.driver.clone(), node.net.node_id());
    let resolve = ResolveEntriesOperation::new(
        scope_of(&ctx, &remote_spec(realm_id, owner)),
        std::mem::take(&mut checkpoint.entities),
        AliasState::default(),
    );
    checkpoint.entities = drive(resolve, ctx.driver.as_ref()).await.unwrap().entities;
    let entity = &checkpoint.entities[0];
    assert_eq!(entity.omission, None);
    assert!(entity.candidates.iter().any(|found| matches!(
        &found.source,
        CandidateSource::Local { bucket, key, .. } if bucket == "remote" && key == "payload"
    ) && found.resolved_version == Some(version)));
    node.net.shutdown().await;
}

#[tokio::test]
async fn unheld_copy_skipped() {
    // A copy whose bucket key the caller lacks does not stop an export into another realm from
    // reading another copy; only when none opens does the job fail with the holder message.
    let (node, _, viewer, candidates) = two_copies().await;
    let ctx = job_context(node.driver.clone(), node.net.node_id());
    let mut spec = remote_spec(viewer.realm_id, viewer);
    spec.selection = Some(aruna_core::structs::execution::job::ExportSelection {
        files: vec!["data/payload".to_string()],
        audience: RealmId::from_bytes([138; 32]),
        intent_digest: String::new(),
    });
    let entity = |candidates: &[ExportCandidate]| ExportEntity {
        entity_id: "data/payload".to_string(),
        local_path: None,
        storage_key: None,
        exact: None,
        hash: candidates[0].expected_blake3,
        hash_realm: None,
        candidates: candidates.to_vec(),
        omission: None,
        message: None,
        zip_path: None,
        report_source: None,
        resolved_version: None,
        path_synthesized: false,
        content: None,
    };
    let mut checkpoint = ExportCheckpoint {
        entities: vec![entity(&candidates)],
        ..Default::default()
    };
    let probed = probe_sources(&ctx, &spec, &mut checkpoint, &BTreeMap::new()).await;
    assert_eq!(probed.unwrap()[0].candidate_index, 1);
    let mut checkpoint = ExportCheckpoint {
        entities: vec![entity(&candidates[..1])],
        ..Default::default()
    };
    let refused = aruna_core::transfer::GrantError::NotHolder("remote".to_string());
    assert!(matches!(
        probe_sources(&ctx, &spec, &mut checkpoint, &BTreeMap::new()).await,
        Err(ExportFailure::Permanent(message)) if message == refused.to_string()
    ));
    node.net.shutdown().await;
}

#[tokio::test]
async fn consent_resolves_hashes() {
    // Consent resolves a file named only by its hash as the job does: one copy the caller may
    // read is enough, and a file whose every copy needs a key the caller lacks is refused.
    let (node, owner, viewer, candidates) = two_copies().await;
    let node_id = node.net.node_id();
    let hash = hex::encode(candidates[0].expected_blake3.unwrap());
    let file = format!(
        "{}{hash}",
        aruna_core::structs::storage::replication::ARUNA_DATA_PREFIX
    );
    let jsonld = crate_document(&[json!({"@id": file, "@type": "File", "name": "payload"})]);
    let jsonld = jsonld.to_string();
    let spec = |user_id| {
        let mut spec = remote_spec(viewer.realm_id, user_id);
        spec.selection = Some(aruna_core::structs::execution::job::ExportSelection {
            files: vec![file.clone()],
            audience: RealmId::from_bytes([138; 32]),
            intent_digest: String::new(),
        });
        spec
    };
    let (viewing, owning) = (spec(viewer), spec(owner));
    let unheld = unheld_bucket(&node.driver, &viewing, node_id, &jsonld).await;
    assert_eq!(unheld, Ok(None));
    encrypting_bucket(&node, "plain", Ulid::from_bytes([139; 16])).await;
    let unheld = unheld_bucket(&node.driver, &viewing, node_id, &jsonld).await;
    assert!(matches!(unheld, Ok(Some(bucket)) if bucket == "plain" || bucket == "remote"));
    let unheld = unheld_bucket(&node.driver, &owning, node_id, &jsonld).await;
    assert_eq!(unheld, Ok(None));
    // A foreign user is refused like any non-holder, not with a storage failure.
    let foreign = UserId::new(viewer.user_ulid, RealmId::from_bytes([140; 32]));
    let mut foreign_spec = spec(viewer);
    foreign_spec.auth_context.user_id = foreign;
    let CandidateSource::Local { bucket, .. } = &candidates[0].source else {
        panic!("a local copy")
    };
    let auth = &foreign_spec.auth_context;
    let holder = HolderCheckOperation::new(auth.realm_id, bucket, auth.user_id);
    let allowed = drive(holder, node.driver.as_ref()).await;
    assert!(matches!(allowed, Ok(false)));
    node.net.shutdown().await;
}

async fn unheld_bucket(
    driver: &DriverContext,
    spec: &ExportRoCrateSpec,
    node_id: NodeId,
    jsonld: &str,
) -> Result<Option<String>, String> {
    let check = UnheldBucketOperation::new(spec.clone(), node_id, jsonld.to_string());
    drive(check, driver).await.map_err(|error| error.0)
}
