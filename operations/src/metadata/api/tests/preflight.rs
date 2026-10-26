//! Tests reference preflight fan-out reporting, cursor pagination, plan limits and failures.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

use super::super::preflight::{
    assemble_preflight_execution, index_state, plan_preflight_request, resolve_all_preflight,
    resolve_preflight_targets, resolve_preflight_versions, verify_preflight_cursor, visible_prefix,
};

// Documents not materialized yet have no postings, so their absence is never reported current.
#[test]
fn unsettled_not_current() {
    use crate::metadata::iri_index::IriFreshnessState as Returned;
    assert_eq!(
        index_state(Returned::Current, false, false),
        MetadataIndexState::Current
    );
    assert_eq!(
        index_state(Returned::Current, true, false),
        MetadataIndexState::Pending
    );
    assert_eq!(
        index_state(Returned::Current, false, true),
        MetadataIndexState::Failed
    );
    assert_eq!(
        index_state(Returned::Pending, false, true),
        MetadataIndexState::Mixed
    );
    assert_eq!(
        index_state(Returned::Mixed, false, false),
        MetadataIndexState::Mixed
    );
}

#[tokio::test]
async fn preflight_fanout_reports() {
    let directory = tempdir().unwrap();
    let context = DriverContext {
        storage_handle: storage::FjallStorage::open(directory.path().to_str().unwrap()).unwrap(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };
    let local = iroh::SecretKey::from_bytes(&[61u8; 32]).public();
    let healthy = iroh::SecretKey::from_bytes(&[62u8; 32]).public();
    let failed = iroh::SecretKey::from_bytes(&[63u8; 32]).public();
    let result_for = |node_id| ReferenceNodeExecution {
        visible_references: Vec::new(),
        targets: Vec::new(),
        freshness: MetadataNodeFreshness {
            node_id,
            index_state: MetadataIndexState::Current,
            oldest_status_updated: None,
        },
        path_style_available: true,
        saturated: false,
    };
    let local_call: MetadataNodeCall<ReferenceNodeExecution> = metadata_node_call(
        (),
        move |(), node_id| async move { Ok(result_for(node_id)) },
    );
    let remote_call: MetadataNodeCall<ReferenceNodeExecution> =
        metadata_node_call(failed, move |failed, node_id| async move {
            if node_id == failed {
                Err(MetadataReadError::Unavailable)
            } else {
                Ok(result_for(node_id))
            }
        });

    let (parts, stats) = run_metadata_fanout(
        &context,
        RealmId::from_bytes([19u8; 32]),
        local,
        MetadataFanoutScope::new(
            Some(ApiQueryMode::Distributed),
            Some(vec![local, healthy, failed]),
            true,
        ),
        MetadataFanoutOperation::ReferencePreflight,
        local_call.clone(),
        remote_call.clone(),
        record_preflight_node,
        map_read_error,
    )
    .await
    .unwrap();

    assert_eq!(parts.len(), 2);
    assert_eq!(stats.nodes_queried, 3);
    assert_eq!(stats.nodes_failed, 1);
    assert_eq!(stats.failed_partitions, vec![failed]);

    let strict = run_metadata_fanout(
        &context,
        RealmId::from_bytes([19u8; 32]),
        local,
        MetadataFanoutScope::new(
            Some(ApiQueryMode::Distributed),
            Some(vec![local, healthy, failed]),
            false,
        ),
        MetadataFanoutOperation::ReferencePreflight,
        local_call,
        remote_call,
        record_preflight_node,
        map_read_error,
    )
    .await;

    assert!(matches!(strict, Err(MetadataApiError::ServiceUnavailable)));
}

#[test]
fn preflight_cursor_pagination() {
    let secret = iroh::SecretKey::from_bytes(&[64u8; 32]);
    let node_id = secret.public();
    let hash = [65u8; 32];
    let content_w3id = format!("{ARUNA_DATA_PREFIX}{}", hex::encode(hash));
    let targets = vec![MetadataResolvedTarget {
        content_w3id: content_w3id.clone(),
        content_hash: hash,
        queried_iris: vec![content_w3id.clone()],
        targeted_versions: Vec::new(),
        removed_locations: Vec::new(),
        remove_resolvable_locations: false,
    }];
    let fingerprint = preflight_fingerprint(&targets, Some(ApiQueryMode::Local));
    let visible = [0x11_u128, 0x22, 0x33, 0x44]
        .map(|value| (content_w3id.clone(), Ulid::from(value)))
        .into_iter()
        .collect::<BTreeSet<_>>();
    // Each fetch gets the node's real prefix, as `visible_prefix` cuts it.
    let node_page = |depth: usize| {
        let (selected, saturated) = visible_prefix(visible.clone(), depth);
        let hits = selected
            .into_iter()
            .map(|(graph_iri, document_id)| MetadataSearchHit {
                document_id: document_id.to_string(),
                group_id: String::new(),
                document_path: String::new(),
                graph_iri,
                subject_iri: document_id.to_string(),
                score: 0.0,
                title: String::new(),
                snippet: None,
                subject_types: Vec::new(),
            })
            .collect::<Vec<_>>();
        (hits, saturated)
    };
    let mut watermark = None;
    let mut returned = Vec::new();

    for depth in 1..=visible.len() {
        let (hits, saturated) = node_page(depth);
        let page = paginate(
            vec![NodeSearchResult {
                node_id,
                hits,
                saturated,
            }],
            watermark,
            1,
            MAX_PAGINATION_DEPTH,
        );
        returned.push(page.hits[0].document_id.clone());
        watermark = page.next.map(|next| {
            let cursor = SearchCursor::new_signed(
                fingerprint,
                next.watermark,
                next.resume,
                node_id,
                |bytes| secret.sign(bytes),
            )
            .expect("search cursor signs");
            let decoded = SearchCursor::decode(&cursor.encode().unwrap(), &[node_id]).unwrap();
            assert_eq!(decoded.fingerprint, fingerprint);
            decoded.payload.watermark
        });
    }

    let by_id = visible
        .iter()
        .map(|(_, document_id)| document_id.to_string())
        .collect::<Vec<_>>();
    assert_ne!(
        returned, by_id,
        "the fixture must order ties unlike document ids"
    );
    let mut emitted = returned.clone();
    emitted.sort();
    assert_eq!(emitted, by_id, "every reference is emitted exactly once");
    assert!(watermark.is_none());
}

fn content_w3id(hash: [u8; 32]) -> String {
    format!("{ARUNA_DATA_PREFIX}{}", hex::encode(hash))
}

fn preflight_request(
    realm_id: RealmId,
    content_w3ids: Vec<String>,
    limit: Option<usize>,
) -> ReferenceRequest {
    ReferenceRequest {
        auth: AuthContext {
            user_id: UserId::nil(realm_id),
            realm_id,
            path_restrictions: None,
            session: None,
        },
        bearer_token: None,
        target: ReferenceTarget::ContentW3ids {
            content_w3ids,
            remove_resolvable_locations: false,
        },
        s3_endpoint: None,
        limit,
        cursor: None,
        mode: None,
        target_nodes: None,
        allow_partial: false,
    }
}

#[test]
fn preflight_plan_limits() {
    let realm_id = RealmId::from_bytes([76u8; 32]);
    let target = || vec![content_w3id([1u8; 32])];
    assert!(matches!(
        plan_preflight_request(
            realm_id,
            preflight_request(RealmId::from_bytes([77u8; 32]), target(), None)
        ),
        Err(MetadataApiError::Forbidden)
    ));

    let (plan, target_value) =
        plan_preflight_request(realm_id, preflight_request(realm_id, target(), None))
            .expect("default plan");
    assert_eq!(plan.page_size, REFERENCES_LIMIT);
    assert!(matches!(target_value, ReferenceTarget::ContentW3ids { .. }));
    let (plan, _) =
        plan_preflight_request(realm_id, preflight_request(realm_id, target(), Some(0)))
            .expect("zero limit clamps");
    assert_eq!(plan.page_size, 1);
    let (plan, _) = plan_preflight_request(
        realm_id,
        preflight_request(realm_id, target(), Some(usize::MAX)),
    )
    .expect("large limit clamps");
    assert_eq!(plan.page_size, REFERENCES_MAX_LIMIT);
}

#[tokio::test]
async fn preflight_stage_failures() {
    let test = metadata_test();
    let realm_id = RealmId::from_bytes([78u8; 32]);
    let local_node_id = iroh::SecretKey::from_bytes(&[79u8; 32]).public();

    let (plan, target) = plan_preflight_request(
        realm_id,
        preflight_request(realm_id, vec!["not-a-w3id".to_string()], None),
    )
    .expect("plan");
    assert!(matches!(
        resolve_preflight_targets(
            &test.context,
            realm_id,
            local_node_id,
            &plan.auth,
            target,
            None
        )
        .await,
        Err(MetadataApiError::BadRequest)
    ));

    let (mut plan, target) = plan_preflight_request(
        realm_id,
        preflight_request(realm_id, vec![content_w3id([2u8; 32])], None),
    )
    .expect("plan");
    let resolved = resolve_preflight_targets(
        &test.context,
        realm_id,
        local_node_id,
        &plan.auth,
        target,
        None,
    )
    .await
    .expect("content w3ids resolve without storage");
    plan.cursor = Some("not-a-cursor".to_string());
    plan.mode = Some(ApiQueryMode::Local);
    let deadline = tokio::time::Instant::now() + DISTRIBUTED_QUERY_DEADLINE;
    assert!(matches!(
        verify_preflight_cursor(
            &test.context,
            realm_id,
            local_node_id,
            &plan,
            &resolved,
            deadline
        )
        .await,
        Err(MetadataApiError::InvalidCursor(_))
    ));

    let (partial_plan, target) = plan_preflight_request(
        realm_id,
        preflight_request(realm_id, vec![content_w3id([3u8; 32])], None),
    )
    .expect("plan");
    let partial_resolved = resolve_preflight_targets(
        &test.context,
        realm_id,
        local_node_id,
        &partial_plan.auth,
        target,
        None,
    )
    .await
    .expect("content w3ids resolve");
    let cursor = verify_preflight_cursor(
        &test.context,
        realm_id,
        local_node_id,
        &partial_plan,
        &partial_resolved,
        deadline,
    )
    .await
    .expect("first page has no cursor");
    let part = ReferenceNodeExecution {
        visible_references: Vec::new(),
        targets: Vec::new(),
        freshness: MetadataNodeFreshness {
            node_id: local_node_id,
            index_state: MetadataIndexState::Current,
            oldest_status_updated: None,
        },
        path_style_available: true,
        saturated: false,
    };
    let denied = assemble_preflight_execution(
        &test.context,
        partial_resolved,
        &partial_plan,
        cursor,
        vec![(local_node_id, part)],
        MetadataFanoutStats {
            nodes_queried: 2,
            nodes_failed: 1,
            failed_partitions: vec![local_node_id],
            discovery_failed: false,
        },
    );
    assert!(matches!(denied, Err(MetadataApiError::ServiceUnavailable)));
}

async fn write_versions(test: &MetadataTest, key: &str, count: u8, head: bool) {
    let user = UserId::nil(RealmId::from_bytes([80u8; 32]));
    let mut rows = Vec::new();
    for index in 0..count {
        let version_id = Ulid::from_parts(u64::from(index) + 1, 0);
        let version = VersionKey::new("genes", key, version_id);
        let value = BlobVersion::deleted(SystemTime::UNIX_EPOCH, user);
        rows.push((BLOB_VERSIONS_KEYSPACE, version.to_bytes(), value.to_bytes()));
        if head {
            let head = BlobHeadKey::new("genes", key).to_bytes();
            let pointer = CurrentVersionPointer::new(version_id).to_bytes();
            rows.push((BLOB_HEAD_KEYSPACE, head, pointer));
        }
    }
    for (key_space, key, value) in rows {
        let event = test
            .context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.to_string(),
                key: key.expect("key encodes").into(),
                value: value.expect("value encodes").into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }
}

#[tokio::test]
async fn large_scope_bounded() {
    let test = metadata_test();
    for index in 0..130 {
        write_versions(&test, &format!("big/{index:03}"), 1, true).await;
    }
    let (heads, complete) = resolve_preflight_versions(&test.context, "genes", None)
        .await
        .expect("large bucket resolves");
    assert_eq!((heads.len(), complete), (128, false));
    let (versions, complete) = resolve_all_preflight(&test.context, "genes", Some("big/"))
        .await
        .expect("large prefix resolves");
    assert_eq!((versions.len(), complete), (128, false));
    let (versions, complete) = resolve_all_preflight(&test.context, "genes", Some("big/12"))
        .await
        .expect("small prefix resolves");
    assert_eq!((versions.len(), complete), (10, true));
}

#[tokio::test]
async fn exact_key_purge() {
    let test = metadata_test();
    write_versions(&test, "a", 3, true).await;
    write_versions(&test, "solo", 2, true).await;
    write_versions(&test, "zeta", 4, true).await;
    let (versions, complete) = resolve_all_preflight(&test.context, "genes", Some("solo"))
        .await
        .expect("exact key resolves");
    assert!(complete);
    assert_eq!(versions.len(), 2);
    assert!(versions.iter().all(|(key, _)| key.key == "solo"));
    let (heads, complete) = resolve_preflight_versions(&test.context, "genes", Some("solo"))
        .await
        .expect("exact head resolves");
    assert_eq!((heads.len(), complete), (1, true));
}

#[tokio::test]
async fn headless_versions_purge() {
    let test = metadata_test();
    write_versions(&test, "dir/a", 2, true).await;
    write_versions(&test, "dir/bb", 1, false).await;
    write_versions(&test, "dirt", 1, false).await;
    let long = format!("dir/{}", "x".repeat(1100));
    write_versions(&test, &long, 1, false).await;
    write_versions(&test, &format!("other/{}", "x".repeat(1100)), 1, false).await;
    let (versions, complete) = resolve_all_preflight(&test.context, "genes", Some("dir/"))
        .await
        .expect("prefix resolves");
    let mut keys: Vec<_> = versions.iter().map(|(key, _)| key.key.as_str()).collect();
    keys.sort();
    assert!(complete);
    assert_eq!(keys, ["dir/a", "dir/a", "dir/bb", long.as_str()]);
    let (versions, complete) = resolve_all_preflight(&test.context, "genes", Some("dir/bb"))
        .await
        .expect("headless key resolves");
    assert_eq!((versions.len(), complete), (1, true));
    let (versions, complete) = resolve_all_preflight(&test.context, "genes", None)
        .await
        .expect("bucket resolves");
    assert_eq!((versions.len(), complete), (6, true));
}
