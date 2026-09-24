//! Tests reference preflight fan-out reporting, cursor pagination, plan limits and failures.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

use super::super::preflight::{
    assemble_preflight_execution, plan_preflight_request, resolve_preflight_targets,
    verify_preflight_cursor, visible_prefix,
};

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
