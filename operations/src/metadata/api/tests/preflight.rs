use super::*;

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
    let result_for = |node_id| MetadataReferencePreflightNodeExecution {
        visible_references: Vec::new(),
        targets: Vec::new(),
        freshness: MetadataPreflightNodeFreshness {
            node_id,
            index_state: MetadataPreflightIndexState::Current,
            oldest_status_updated_at_ms: None,
        },
        path_style_endpoint_available: true,
        saturated: false,
    };
    let local_call: MetadataNodeCall<MetadataReferencePreflightNodeExecution> = metadata_node_call(
        (),
        move |(), node_id| async move { Ok(result_for(node_id)) },
    );
    let remote_call: MetadataNodeCall<MetadataReferencePreflightNodeExecution> =
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
            Some(MetadataApiQueryMode::Distributed),
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
            Some(MetadataApiQueryMode::Distributed),
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
    let targets = vec![MetadataPreflightResolvedTarget {
        content_w3id: content_w3id.clone(),
        content_hash: hash,
        queried_iris: vec![content_w3id.clone()],
        targeted_versions: Vec::new(),
        removed_locations: Vec::new(),
        remove_all_resolvable_locations: false,
    }];
    let fingerprint = preflight_fingerprint(&targets, Some(MetadataApiQueryMode::Local));
    let hits = (0..3)
        .map(|index| MetadataSearchHit {
            document_id: format!("document-{index}"),
            group_id: String::new(),
            document_path: String::new(),
            graph_iri: content_w3id.clone(),
            subject_iri: format!("document-{index}"),
            score: 0.0,
            title: format!("Document {index}"),
            snippet: None,
            subject_types: Vec::new(),
        })
        .collect::<Vec<_>>();
    let mut watermark = None;
    let mut returned = Vec::new();

    for depth in 1..=3 {
        let page = paginate(
            vec![NodeSearchResult {
                node_id,
                hits: hits[..depth].to_vec(),
                saturated: depth < hits.len(),
            }],
            watermark,
            1,
            METADATA_SEARCH_MAX_PAGINATION_DEPTH,
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

    assert_eq!(returned, vec!["document-0", "document-1", "document-2"]);
    assert!(watermark.is_none());
}
