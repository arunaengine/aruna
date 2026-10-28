//! Tests release membership: a tagged release crate pins exact versions, only for callers who
//! may read it, and a holder answers another node only for a bearer it validates itself.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::topology;

use std::collections::HashMap;
use std::time::SystemTime;

use aruna_core::metadata::AuthToken;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction, Permission};
use aruna_core::structs::storage::blob::{BucketInfo, object_permission_path};
use aruna_core::structs::storage::bucket_metadata::{RELEASE_PREFIX_TAG, RELEASE_TAG};
use aruna_core::structs::storage::distribution::KeyRange;
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::release::{HolderAnswer, HolderAsk};
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_core::structs::storage::routing::RoutingSnapshot;
use aruna_core::types::GroupId;
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::metadata::MetadataReadError;
use aruna_operations::s3::bucket::create::CreateBucketOperation;
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
            pattern: file_path,
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
    // The holder reads a tagged crate version for the caller; an untagged version is none.
    let crate_ask = |key: &str, version_id| HolderAsk::Crate {
        key: key.to_string(),
        version_id,
    };
    for (key, version_id, expected) in [
        ("db/release.json", crate_version, Some(crate_bytes.clone())),
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
