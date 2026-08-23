#![recursion_limit = "512"]

mod topology;

use std::time::UNIX_EPOCH;

use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::structs::{
    BackendRef, BlobHeadKey, BlobVersion, BucketInfo, CurrentVersionPointer, VersionKey,
};
use aruna_operations::metadata::api::{
    BucketSearchRequest, ObjectSearchQueryMode, ObjectSearchRequest, search_buckets_distributed,
    search_objects,
};
use aruna_operations::s3::search_objects::ObjectKeyMatch;
use ulid::Ulid;

use topology::{TestNode, TestResult, Topology};

async fn write_value(
    node: &TestNode,
    key_space: &str,
    key: Vec<u8>,
    value: Vec<u8>,
) -> TestResult<()> {
    match node
        .context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: key_space.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        event => Err(format!("unexpected fixture write: {event:?}").into()),
    }
}

async fn seed_bucket(node: &TestNode, realm: &Topology, group_id: Ulid) -> TestResult<()> {
    write_value(
        node,
        S3_BUCKET_KEYSPACE,
        b"data".to_vec(),
        BucketInfo {
            group_id,
            created_at: UNIX_EPOCH,
            created_by: realm.user_id,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
        }
        .to_bytes()?,
    )
    .await
}

async fn seed_object(node: &TestNode, realm: &Topology, key: &str, tag: u8) -> TestResult<()> {
    let version_id = Ulid::generate();
    write_value(
        node,
        BLOB_HEAD_KEYSPACE,
        BlobHeadKey::new("data", key).to_bytes()?,
        CurrentVersionPointer::new(version_id).to_bytes()?,
    )
    .await?;
    write_value(
        node,
        BLOB_VERSIONS_KEYSPACE,
        VersionKey::new("data", key, version_id).to_bytes()?,
        BlobVersion::materialized(
            [tag; 32],
            BackendRef::node_default(),
            UNIX_EPOCH,
            realm.user_id,
            None,
        )
        .to_bytes()?,
    )
    .await
}

#[tokio::test]
async fn distributed_search_routes() -> TestResult<()> {
    // A distributed cursor remains valid when a load balancer selects another realm node.
    let realm = Topology::spawn_sharded(2, 0, 1, 1).await?;
    let origin = realm.node(0);
    let responder = realm.node(1);
    let group_id = realm.seed_group().await?;
    seed_bucket(origin, &realm, group_id).await?;
    seed_object(origin, &realm, "reads/a.fastq", 1).await?;
    seed_object(origin, &realm, "reads/b.fastq", 2).await?;

    let first = search_objects(
        origin.context.as_ref(),
        realm.realm_id,
        origin.node_id(),
        ObjectSearchRequest {
            auth: realm.auth_context(),
            bearer_token: Some(realm.bearer_string()),
            query: "reads/".to_string(),
            key_match: ObjectKeyMatch::Prefix,
            bucket: Some("data".to_string()),
            limit: 1,
            cursor: None,
            mode: ObjectSearchQueryMode::DistributedBestEffort,
            target_nodes: Some(vec![origin.node_id()]),
        },
    )
    .await?;
    assert_eq!(first.hits.len(), 1);
    assert_eq!(first.hits[0].key, "reads/a.fastq");
    let cursor = first.next_cursor.expect("the first page is truncated");

    let second = search_objects(
        responder.context.as_ref(),
        realm.realm_id,
        responder.node_id(),
        ObjectSearchRequest {
            auth: realm.auth_context(),
            bearer_token: Some(realm.bearer_string()),
            query: "reads/".to_string(),
            key_match: ObjectKeyMatch::Prefix,
            bucket: Some("data".to_string()),
            limit: 1,
            cursor: Some(cursor),
            mode: ObjectSearchQueryMode::DistributedBestEffort,
            target_nodes: None,
        },
    )
    .await?;
    assert_eq!(second.hits.len(), 1);
    assert_eq!(second.hits[0].key, "reads/b.fastq");
    assert!(second.next_cursor.is_none());
    assert!(second.complete);

    let buckets = search_buckets_distributed(
        responder.context.as_ref(),
        realm.realm_id,
        responder.node_id(),
        BucketSearchRequest {
            auth: realm.auth_context(),
            bearer_token: Some(realm.bearer_string()),
            query: "data".to_string(),
            limit: 10,
            target_nodes: Some(vec![origin.node_id()]),
        },
    )
    .await?;
    assert_eq!(buckets.hits.len(), 1);
    assert_eq!(buckets.hits[0].bucket, "data");

    realm.shutdown().await;
    Ok(())
}
