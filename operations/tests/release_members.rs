//! Tests release membership: a tagged release crate pins exact versions, only for callers who
//! may read it, and a holder answers another node only for a bearer it validates itself.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::topology;

use std::collections::HashMap;
use std::time::SystemTime;

use aruna_core::effects::StorageEffect;
use aruna_core::keyspaces::AUTH_KEYSPACE;
use aruna_core::metadata::AuthToken;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::execution::job::{InputMode, InputSelection, InputSource, SubmissionId};
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction, Permission, Role};
use aruna_core::structs::identity::group::GroupAuthorizationDocument;
use aruna_core::structs::storage::blob::{BucketInfo, object_permission_path};
use aruna_core::structs::storage::bucket_metadata::{
    BucketMetadata, RELEASE_PREFIX_TAG, RELEASE_TAG,
};
use aruna_core::structs::storage::distribution::KeyRange;
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::release::{CRATE_CHUNK, CrateChunk, HolderAnswer, HolderAsk};
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_core::structs::storage::routing::RoutingSnapshot;
use aruna_core::types::GroupId;
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::jobs::lifecycle::ingress::InputResolveRequest;
use aruna_operations::metadata::MetadataReadError;
use aruna_operations::s3::bucket::create::CreateBucketOperation;
use aruna_operations::s3::bucket::metadata::PutMetadataOperation;
use aruna_operations::s3::bucket::release::{
    BucketScope, MembershipCaller, MembershipQuery, load_access, release_members,
};
use aruna_operations::s3::bucket::release_publish::release_files;
use aruna_operations::s3::object::put::{PutObjectConfig, PutObjectInput, PutObjectOperation};
use serde_json::json;
use topology::{TestResult, Topology};
use ulid::Ulid;

const BUCKET: &str = "bakta-db";

async fn put(
    context: &DriverContext,
    realm: &Topology,
    group_id: GroupId,
    (key, bytes): (&str, Vec<u8>),
    metadata: HashMap<String, String>,
) -> TestResult<Ulid> {
    let node_id = context.net_handle.as_ref().ok_or("a realm node")?.node_id();
    let length = bytes.len() as u64;
    let body = futures_util::stream::iter([Ok::<_, StreamError>(bytes::Bytes::from(bytes))]);
    let result = drive(
        PutObjectOperation::new(PutObjectConfig {
            user_id: realm.user_id,
            group_id,
            realm_id: realm.realm_id,
            node_id,
            request: PutObjectInput {
                bucket: BUCKET.to_string(),
                key: key.to_string(),
                content_length: Some(length),
                body: Some(BackendStream::new(body)),
            },
            expected_checksums: Vec::new(),
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: None,
            quota_ceiling: None,
            routing: RoutingSnapshot::single(group_id),
        })
        .with_metadata(metadata),
        context,
    )
    .await?;
    Ok(result.version_id)
}

/// The releases that pin `queries` for a caller this node validated, asking no other node.
async fn ask(
    context: &DriverContext,
    scope: BucketScope<'_>,
    auth: AuthContext,
    queries: &[MembershipQuery],
) -> TestResult<Vec<aruna_operations::s3::bucket::release::ReleaseMembers>> {
    let caller = MembershipCaller {
        auth: &auth,
        bearer: None,
    };
    Ok(release_members(context, scope, caller, queries)
        .await?
        .answers)
}

#[tokio::test]
async fn releases_pin_versions() -> TestResult<()> {
    let realm = Topology::spawn(2, 1, 1).await?;
    let group_id = realm.seed_group().await?;
    let holder = realm.node(0);
    let context = holder.context.as_ref();
    let info = BucketInfo {
        group_id,
        created_at: SystemTime::now(),
        created_by: realm.user_id,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: None,
    };
    drive(
        CreateBucketOperation::new(BUCKET.to_string(), info.clone()),
        context,
    )
    .await?;
    let pinned = put(
        context,
        &realm,
        group_id,
        ("db/a.fasta", b">a".to_vec()),
        HashMap::new(),
    )
    .await?;
    let exact = |key: &str, version_id| {
        VersionedObjectArn::new(realm.realm_id, holder.node_id(), BUCKET, key, version_id)
            .map(|arn| arn.to_w3id())
    };
    let release = json!({"@graph": [
        {"@id": "./", "@type": "Dataset", "identifier": [{"@id": "#upstream-0"}]},
        {"@id": "#upstream-0", "@type": "PropertyValue", "name": "DOI",
         "value": "10.5281/zenodo.1", "url": "https://doi.org/10.5281/zenodo.1"},
        {"@id": exact("db/a.fasta", pinned)?, "@type": "File"}
    ]});
    let tags = HashMap::from([
        (RELEASE_TAG.to_string(), "6.0".to_string()),
        (RELEASE_PREFIX_TAG.to_string(), "db/".to_string()),
    ]);
    let crate_bytes = serde_json::to_vec(&release)?;
    let crate_version = put(
        context,
        &realm,
        group_id,
        ("db/release.json", crate_bytes.clone()),
        tags,
    )
    .await?;
    let newer = put(
        context,
        &realm,
        group_id,
        ("db/a.fasta", b">b".to_vec()),
        HashMap::new(),
    )
    .await?;

    let scope = BucketScope {
        realm_id: realm.realm_id,
        node_id: holder.node_id(),
        bucket: BUCKET,
        info: &info,
    };
    let queries = [
        MembershipQuery {
            key: "db/a.fasta".to_string(),
            version_id: pinned,
        },
        MembershipQuery {
            key: "db/a.fasta".to_string(),
            version_id: newer,
        },
        MembershipQuery {
            key: "other/a.fasta".to_string(),
            version_id: pinned,
        },
    ];
    let answers = ask(context, scope, realm.auth_context(), &queries).await?;
    assert_eq!(answers[0].releases.len(), 1);
    assert_eq!(answers[0].releases[0].label, "6.0");
    assert_eq!(answers[0].releases[0].upstream[0].value, "10.5281/zenodo.1");
    assert!(
        answers[1].releases.is_empty(),
        "a newer version is not pinned"
    );
    assert!(
        answers[2].releases.is_empty(),
        "a key outside the prefix is no member"
    );
    assert!(answers.iter().all(|answer| answer.complete));

    // A token restricted to the file, not the release crate, learns of no release.
    let file_path = object_permission_path(
        realm.realm_id,
        group_id,
        holder.node_id(),
        BUCKET,
        "db/a.fasta",
    );
    let restricted = AuthContext {
        path_restrictions: Some(vec![PathRestriction {
            pattern: file_path.clone(),
            permission: Permission::READ,
        }]),
        ..realm.auth_context()
    };
    let answers = ask(context, scope, restricted, &queries).await?;
    assert!(answers[0].releases.is_empty());
    let stranger = realm.auth_for(aruna_core::UserId::local(
        Ulid::from(77u128),
        realm.realm_id,
    ));
    assert!(
        ask(context, scope, stranger, &queries).await?[0]
            .releases
            .is_empty()
    );

    // Another node asks with a bearer the holder validates; a forged one or a device is refused.
    let handle = realm
        .node(1)
        .context
        .metadata_handle
        .as_ref()
        .ok_or("handle")?;
    let ask = || HolderAsk::Members {
        range_ids: vec![Ulid::from(5u128)],
        queries: queries.to_vec(),
    };
    let bucket = || BUCKET.to_string();
    let answer = handle
        .request_release_holder(
            holder.node_id(),
            Some(realm.bearer_token()),
            bucket(),
            ask(),
        )
        .await
        .map_err(|error| format!("{error:?}"))?;
    let HolderAnswer::Members(answer) = answer else {
        return Err("a membership answer".into());
    };
    assert_eq!(
        answer.refused,
        vec![Ulid::from(5u128)],
        "an ordinary bucket has no ranges"
    );
    // Admission on the input's node captures the releases that pin it, checked with the
    // caller's own access, and the bucket's metadata then.
    let input = InputSelection {
        source: InputSource::S3 {
            bucket: BUCKET.to_string(),
            key: "db/a.fasta".to_string(),
            version_id: Some(pinned.to_string()),
        },
        source_node_id: Some(holder.node_id()),
        dest_key: "a.fasta".to_string(),
        mode: InputMode::Snapshot,
        container_path: Some("/inputs/a.fasta".to_string()),
        name: None,
        description: None,
    };
    let metadata = BucketMetadata {
        database: true,
        title: Some("Bakta".to_string()),
        ..BucketMetadata::default()
    };
    let set_metadata = PutMetadataOperation::new(
        (realm.auth_context(), realm.realm_id),
        BUCKET.to_string(),
        group_id,
        Some(metadata.clone()),
    );
    drive(set_metadata, context).await?;
    let request = InputResolveRequest {
        created_by: realm.user_id,
        submission_id: SubmissionId([7; 32]),
        expires_at: u64::MAX,
        inputs: vec![input],
    };
    let resolved = handle
        .request_input_resolve(holder.node_id(), realm.bearer_token(), request)
        .await?
        .map_err(|refusal| format!("{refusal:?}"))?;
    let captured = &resolved[0].1;
    assert_eq!(captured.database.as_ref(), Some(&metadata));
    assert_eq!(captured.releases.len(), 1);
    assert_eq!(captured.releases[0].label, "6.0");
    assert!(captured.releases_complete);

    // READ on the one input key admits it but shows nothing of the bucket's metadata.
    let one_key = aruna_core::UserId::local(Ulid::from(78u128), realm.realm_id);
    let bytes = topology::read_group_auth(holder, group_id)
        .await?
        .ok_or("the group's authorization document")?;
    let mut doc: GroupAuthorizationDocument = postcard::from_bytes(&bytes)?;
    let role_id = Ulid::from(79u128);
    doc.roles.insert(
        role_id,
        Role {
            role_id,
            name: "one-key".to_string(),
            permissions: HashMap::from([(file_path.clone(), Permission::READ)]),
            assigned_users: std::collections::HashSet::from([one_key]),
        },
    );
    let write = StorageEffect::Write {
        key_space: AUTH_KEYSPACE.to_string(),
        key: group_id.to_bytes().to_vec().into(),
        value: postcard::to_allocvec(&doc)?.into(),
        txn_id: None,
    };
    context.storage_handle.send_storage_effect(write).await;
    let request = InputResolveRequest {
        created_by: one_key,
        submission_id: SubmissionId([8; 32]),
        expires_at: u64::MAX,
        inputs: resolved.iter().map(|(input, _)| input.clone()).collect(),
    };
    let resolved = handle
        .request_input_resolve(holder.node_id(), realm.bearer_for(one_key), request)
        .await?
        .map_err(|refusal| format!("{refusal:?}"))?;
    assert_eq!(resolved[0].1.database, None, "no bucket READ, no metadata");

    // The holder reads a tagged crate version for the caller; an untagged version is none.
    let crate_ask = |key: &str, version_id| HolderAsk::Crate {
        key: key.to_string(),
        version_id,
        offset: 0,
    };
    let whole = CrateChunk {
        size: crate_bytes.len() as u64,
        bytes: crate_bytes.clone(),
    };
    for (key, version_id, expected) in [
        ("db/release.json", crate_version, Some(whole)),
        ("db/a.fasta", pinned, None),
    ] {
        let answer = handle
            .request_release_holder(
                holder.node_id(),
                Some(realm.bearer_token()),
                bucket(),
                crate_ask(key, version_id),
            )
            .await
            .map_err(|error| format!("{error:?}"))?;
        assert_eq!(answer, HolderAnswer::Crate(expected));
    }
    // A caller without READ gets the generic refusal, not a holder failure.
    let stranger_token = realm.bearer_for(aruna_core::UserId::local(
        Ulid::from(77u128),
        realm.realm_id,
    ));
    let unreadable = handle
        .request_release_holder(
            holder.node_id(),
            Some(stranger_token),
            bucket(),
            crate_ask("db/release.json", crate_version),
        )
        .await;
    assert_eq!(unreadable, Err(MetadataReadError::Forbidden));
    // A crate larger than one reply comes in chunks of the one exact version.
    let size = CRATE_CHUNK + 10;
    let large_tags = HashMap::from([
        (RELEASE_TAG.to_string(), "7.0".to_string()),
        (RELEASE_PREFIX_TAG.to_string(), "db/".to_string()),
    ]);
    let large = ("db/large.json", vec![b' '; size as usize]);
    let large_version = put(context, &realm, group_id, large, large_tags).await?;
    for (offset, length) in [(0, CRATE_CHUNK), (CRATE_CHUNK, 10)] {
        let ask = HolderAsk::Crate {
            key: "db/large.json".to_string(),
            version_id: large_version,
            offset,
        };
        let answer = handle
            .request_release_holder(holder.node_id(), Some(realm.bearer_token()), bucket(), ask)
            .await
            .map_err(|error| format!("{error:?}"))?;
        let HolderAnswer::Crate(Some(chunk)) = answer else {
            return Err("a chunk of the large crate".into());
        };
        assert_eq!((chunk.bytes.len() as u64, chunk.size), (length, size));
    }
    let forged = AuthToken::bearer("forged").map_err(|error| format!("{error:?}"))?;
    let refused = handle
        .request_release_holder(holder.node_id(), Some(forged), bucket(), ask())
        .await;
    assert_eq!(refused, Err(MetadataReadError::Unauthorized));
    let missing = handle
        .request_release_holder(holder.node_id(), None, bucket(), ask())
        .await;
    assert_eq!(missing, Err(MetadataReadError::Unauthorized));
    let device = realm
        .user_node()
        .context
        .metadata_handle
        .as_ref()
        .ok_or("handle")?;
    let from_device = device
        .request_release_holder(
            holder.node_id(),
            Some(realm.bearer_token()),
            bucket(),
            ask(),
        )
        .await;
    assert!(from_device.is_err(), "a device is no infrastructure peer");
    Ok(())
}

#[tokio::test]
async fn release_files_page() -> TestResult<()> {
    // A page of release files stops at a bounded number of keys and names where the next starts.
    let realm = Topology::spawn(2, 1, 1).await?;
    let group_id = realm.seed_group().await?;
    let holder = realm.node(0);
    let context = holder.context.as_ref();
    let info = BucketInfo {
        group_id,
        created_at: SystemTime::now(),
        created_by: realm.user_id,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: None,
    };
    drive(
        CreateBucketOperation::new(BUCKET.to_string(), info.clone()),
        context,
    )
    .await?;
    let count = 257;
    for index in 0..count {
        let key = format!("db/{index:04}.fasta");
        put(
            context,
            &realm,
            group_id,
            (&key, b">a".to_vec()),
            HashMap::new(),
        )
        .await?;
    }
    let scope = BucketScope {
        realm_id: realm.realm_id,
        node_id: holder.node_id(),
        bucket: BUCKET,
        info: &info,
    };
    let access = load_access(context, scope, &realm.auth_context()).await?;
    let asked = ("db/", "db/release.json");
    let (first, next) = release_files(context, scope, &access, asked, (None, None)).await?;
    assert_eq!(first.len(), 256);
    assert_eq!(next.as_deref(), Some("db/0255.fasta"));
    let (rest, next) = release_files(context, scope, &access, asked, (None, next)).await?;
    assert_eq!(rest.len(), count - 256);
    assert_eq!(rest[0].key, "db/0256.fasta");
    assert_eq!(next, None);

    // A range's page lists only the range: unreadable keys before it neither refuse the page
    // nor name its end.
    let range = KeyRange {
        range_id: Ulid::from(1u128),
        lower: "db/0100.fasta".to_string(),
        upper: Some("db/0110.fasta".to_string()),
        lineage: None,
        coordinator: Ulid::from(2u128),
        epoch: 1,
        replicas: Vec::new(),
    };
    let inside = object_permission_path(
        realm.realm_id,
        group_id,
        holder.node_id(),
        BUCKET,
        "db/010*",
    );
    let restricted = AuthContext {
        path_restrictions: Some(vec![PathRestriction {
            pattern: inside,
            permission: Permission::READ,
        }]),
        ..realm.auth_context()
    };
    let access = load_access(context, scope, &restricted).await?;
    let (files, next) = release_files(context, scope, &access, asked, (Some(&range), None)).await?;
    let keys: Vec<_> = files.iter().map(|file| file.key.as_str()).collect();
    assert_eq!(keys.len(), 10);
    assert_eq!(keys[0], "db/0100.fasta");
    assert_eq!(next, None);
    Ok(())
}
