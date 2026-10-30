//! Tests identifiers on origin reads: the bucket's current metadata and its rules apply.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::keyspaces::{
    BUCKET_CREATION_KEYSPACE, BUCKET_METADATA_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::identifier::{
    IdentifierParser, IdentifierRule, IdentifierTarget,
};
use aruna_core::structs::storage::blob::{BackendRef, BucketInfo};
use aruna_core::structs::storage::bucket_metadata::StoredMetadata;
use aruna_core::structs::storage::format::{Compression, EncodingClass};

const BUCKET: &str = "bakrep";
const KEY: &str = "genes/SAMN15803895.faa.gz";

async fn put(context: &DriverContext, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: key_space.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}

/// A bucket with one version and database metadata whose rules find BioSample ids.
async fn seed(context: &DriverContext, actor: &Actor, listed: bool) -> Ulid {
    let bucket = BucketInfo {
        group_id: Ulid::from_bytes([2; 16]),
        created_at: SystemTime::UNIX_EPOCH,
        created_by: Default::default(),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: None,
    };
    let name = BUCKET.as_bytes().to_vec();
    put(
        context,
        S3_BUCKET_KEYSPACE,
        name,
        bucket.to_bytes().unwrap(),
    )
    .await;
    let version_id = Ulid::from_bytes([3; 16]);
    let version = BlobVersion::materialized(
        [4; 32],
        BackendRef::node_default(),
        EncodingClass::Raw,
        SystemTime::UNIX_EPOCH,
        actor.user_id,
        None,
    );
    let row = VersionKey::new(BUCKET, KEY, version_id).to_bytes().unwrap();
    put(
        context,
        BLOB_VERSIONS_KEYSPACE,
        row,
        version.to_bytes().unwrap(),
    )
    .await;

    let parser = IdentifierParser {
        rules: vec![IdentifierRule {
            name: "BioSample".to_string(),
            pattern: r"(?P<id>SAM(?:N|EA|D)\d+)".to_string(),
            target: IdentifierTarget::Key,
            url_template: "https://www.ebi.ac.uk/biosamples/samples/{id}".to_string(),
        }],
    };
    let metadata = BucketMetadata {
        database: true,
        dataset: Some(Ulid::from_bytes([9; 16])),
        parser: Some(parser),
        ..BucketMetadata::default()
    };
    let stored = StoredMetadata {
        incarnation: 0,
        metadata,
    };
    put(
        context,
        BUCKET_METADATA_KEYSPACE,
        BUCKET.as_bytes().to_vec(),
        stored.to_bytes().unwrap(),
    )
    .await;
    if !listed {
        // The bucket is created again under its name: the old metadata no longer applies.
        put(
            context,
            BUCKET_CREATION_KEYSPACE,
            BUCKET.as_bytes().to_vec(),
            Ulid::from_bytes([9; 16]).to_bytes().to_vec(),
        )
        .await;
    }
    version_id
}

#[tokio::test]
async fn database_identifiers_read() {
    for listed in [true, false] {
        let temp = tempfile::tempdir().unwrap();
        let context = DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(temp.path().to_str().unwrap())
                .unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let realm_id = RealmId::from_bytes([1; 32]);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[7; 32]).public(),
            user_id: UserId::local(Ulid::from_bytes([8; 16]), realm_id),
            realm_id,
        };
        let version_id = seed(&context, &actor, listed).await;
        let scope = (realm_id, actor.node_id);
        let view = read_origin(&context, scope, BUCKET, KEY, Some(version_id))
            .await
            .unwrap();
        if !listed {
            assert_eq!(view.bucket_metadata, None);
            assert!(view.identifiers.is_empty());
            assert!(view.projection.is_empty());
            continue;
        }
        let metadata = view.bucket_metadata.as_ref().unwrap();
        assert!(metadata.database);
        assert_eq!(metadata.dataset, Some(Ulid::from_bytes([9; 16])));
        let url = "https://www.ebi.ac.uk/biosamples/samples/SAMN15803895";
        assert_eq!(view.identifiers.len(), 1);
        assert_eq!(view.identifiers[0].value, "SAMN15803895");
        assert_eq!(view.identifiers[0].url, url);
        assert_eq!(
            view.projection,
            vec![OriginProjection::Identifier {
                name: "BioSample".to_string(),
                value: "SAMN15803895".to_string(),
                url: url.to_string(),
            }]
        );
    }
}
