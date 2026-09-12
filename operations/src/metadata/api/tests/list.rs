use super::export::{seed_registry_cache, write_entry, write_pending_marker};
use super::*;

#[tokio::test]
async fn filters_graph_delete() {
    let test = metadata_test();
    let group_id = Ulid::generate();
    let live = public_record(group_id, Ulid::generate());
    let deleted = public_record(group_id, Ulid::generate());
    let tombstone = MetadataGraphLifecycleRecord::deleted(
        deleted.graph_iri.clone(),
        deleted.realm_id,
        deleted.group_id,
        deleted.document_id,
        2,
    );
    write_entry(
        &test,
        graph_lifecycle_entry(&tombstone).expect("lifecycle entry"),
    )
    .await;

    let records = filter_live_records(&test.context.storage_handle, &[live.clone(), deleted])
        .await
        .expect("lifecycle filter succeeds");
    assert_eq!(records, vec![live]);
}

#[tokio::test]
async fn filters_document_delete() {
    let test = metadata_test();
    let group_id = Ulid::generate();
    let deleted = public_record(group_id, Ulid::generate());
    let tombstone = MetadataGraphLifecycleRecord::deleted(
        deleted.graph_iri.clone(),
        deleted.realm_id,
        deleted.group_id,
        deleted.document_id,
        2,
    );
    let lifecycle = aruna_core::metadata::MetadataDocumentLifecycleRecord::Delete {
        event: aruna_core::metadata::MetadataDocumentDeleteRecord {
            event_id: Ulid::generate(),
            tombstone,
            deleted_after_event_id: deleted.last_event_id,
        },
    };
    write_entry(
        &test,
        document_lifecycle_entry(&lifecycle).expect("lifecycle entry"),
    )
    .await;

    let records = filter_live_records(&test.context.storage_handle, &[deleted])
        .await
        .expect("lifecycle filter succeeds");
    assert!(records.is_empty());
}

#[tokio::test]
async fn rejects_bad_lifecycle() {
    let test = metadata_test();
    let record = public_record(Ulid::generate(), Ulid::generate());
    write_entry(
        &test,
        (
            METADATA_GRAPH_LIFECYCLE_KEYSPACE.to_string(),
            graph_lifecycle_key(&record.graph_iri),
            ByteView::from(vec![1u8]),
        ),
    )
    .await;

    assert!(
        filter_live_records(&test.context.storage_handle, &[record])
            .await
            .is_err()
    );
}

#[tokio::test]
async fn foreign_lifecycle_rejected() {
    // A well-formed lifecycle entry that belongs to another document must
    // not be allowed to decide this record's visibility.
    let test = metadata_test();
    let graph_record = public_record(Ulid::generate(), Ulid::generate());
    let document_record = public_record(Ulid::generate(), Ulid::generate());
    let stranger = public_record(Ulid::generate(), Ulid::generate());
    let tombstone = MetadataGraphLifecycleRecord::deleted(
        stranger.graph_iri.clone(),
        stranger.realm_id,
        stranger.group_id,
        stranger.document_id,
        2,
    );
    write_entry(
        &test,
        (
            METADATA_GRAPH_LIFECYCLE_KEYSPACE.to_string(),
            graph_lifecycle_key(&graph_record.graph_iri),
            ByteView::from(postcard::to_allocvec(&tombstone).expect("tombstone encodes")),
        ),
    )
    .await;
    let lifecycle = MetadataDocumentLifecycleRecord::Delete {
        event: aruna_core::metadata::MetadataDocumentDeleteRecord {
            event_id: Ulid::generate(),
            tombstone,
            deleted_after_event_id: stranger.last_event_id,
        },
    };
    write_entry(
        &test,
        (
            METADATA_DOCUMENT_LIFECYCLE_KEYSPACE.to_string(),
            document_lifecycle_key(document_record.document_id),
            ByteView::from(postcard::to_allocvec(&lifecycle).expect("lifecycle encodes")),
        ),
    )
    .await;

    assert!(
        filter_live_records(&test.context.storage_handle, &[graph_record])
            .await
            .is_err()
    );
    assert!(
        filter_live_records(&test.context.storage_handle, &[document_record])
            .await
            .is_err()
    );
}

#[tokio::test]
async fn group_scan_capped() {
    // Without the visibility cache the listing scans storage directly, and
    // must refuse rather than hand back a truncated group.
    let test = metadata_test();
    let group_id = Ulid::generate();
    let first = public_record(group_id, Ulid::generate());
    let second = public_record(group_id, Ulid::generate());
    for record in [&first, &second] {
        for entry in aruna_core::storage_entries::registry_write_entries(record)
            .expect("registry entries encode")
        {
            write_entry(&test, entry).await;
        }
    }
    let context = DriverContext {
        storage_handle: test.context.storage_handle.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };

    let within = load_group_records(&context, group_id, 2)
        .await
        .expect("group scan succeeds");
    let over = load_group_records(&context, group_id, 1).await;

    assert_eq!(within.len(), 2);
    assert!(matches!(over, Err(MetadataApiError::ServiceUnavailable)));
}

#[tokio::test]
async fn pending_scan_capped() {
    // The pending-projection sweep is bounded by the candidate budget so a
    // large backlog cannot be silently cut short.
    let test = metadata_test();
    let group_id = Ulid::generate();
    let first = public_record(group_id, Ulid::generate());
    let second = public_record(group_id, Ulid::generate());
    write_pending_marker(&test, &first).await;
    write_pending_marker(&test, &second).await;

    let within = load_pending_records(&test.context, Some(group_id), 2)
        .await
        .expect("pending scan succeeds");
    let over = load_pending_records(&test.context, Some(group_id), 1).await;

    assert_eq!(within.get(&group_id).map(Vec::len), Some(2));
    assert!(matches!(over, Err(MetadataApiError::ServiceUnavailable)));
}

// The page window must not truncate the estimate, and paging must not move it.
#[tokio::test]
async fn estimate_beyond_page() {
    let test = metadata_test();
    let group_id = Ulid::generate();
    let seeded = METADATA_ESTIMATE_MIN_LIMIT + 2;
    for _ in 0..seeded {
        seed_registry_cache(&test, &public_record(group_id, Ulid::generate())).await;
    }

    let page = list_visible_documents(
        &test.context,
        TEST_REALM_ID,
        ListVisibleMetadataDocumentsRequest {
            limit: Some(METADATA_ESTIMATE_MIN_LIMIT),
            ..summary_request(group_id, false)
        },
    )
    .await
    .expect("listing succeeds");
    assert_eq!(page.documents.len(), METADATA_ESTIMATE_MIN_LIMIT);
    assert_eq!(page.total_returned, METADATA_ESTIMATE_MIN_LIMIT);
    assert_eq!(page.total_estimate, Some(seeded));

    let tail = list_visible_documents(
        &test.context,
        TEST_REALM_ID,
        ListVisibleMetadataDocumentsRequest {
            limit: Some(METADATA_ESTIMATE_MIN_LIMIT),
            offset: Some(seeded - 1),
            ..summary_request(group_id, false)
        },
    )
    .await
    .expect("listing succeeds");
    assert_eq!(tail.documents.len(), 1);
    assert_eq!(tail.total_estimate, Some(seeded));
}

// A targeted lookup must not pay for the realm-wide estimate scan, and
// must report the estimate as absent rather than as a truncated count.
#[tokio::test]
async fn lookup_omits_estimate() {
    let test = metadata_test();
    let group_id = Ulid::generate();
    for _ in 0..3 {
        seed_registry_cache(&test, &public_record(group_id, Ulid::generate())).await;
    }

    let lookup = list_visible_documents(
        &test.context,
        TEST_REALM_ID,
        ListVisibleMetadataDocumentsRequest {
            limit: Some(METADATA_ESTIMATE_MIN_LIMIT - 1),
            ..summary_request(group_id, false)
        },
    )
    .await
    .expect("listing succeeds");
    assert_eq!(lookup.total_returned, 3);
    assert_eq!(lookup.total_estimate, None);

    let browse = list_visible_documents(
        &test.context,
        TEST_REALM_ID,
        ListVisibleMetadataDocumentsRequest {
            limit: Some(METADATA_ESTIMATE_MIN_LIMIT),
            ..summary_request(group_id, false)
        },
    )
    .await
    .expect("listing succeeds");
    assert_eq!(browse.total_estimate, Some(3));
}

// Anonymous callers collect no rules, so only public records count.
#[tokio::test]
async fn estimate_skips_private() {
    let test = metadata_test();
    let group_id = Ulid::generate();
    let readable = public_record(group_id, Ulid::generate());
    seed_registry_cache(&test, &readable).await;
    let mut private = public_record(group_id, Ulid::generate());
    private.public = false;
    seed_registry_cache(&test, &private).await;

    let result = list_visible_documents(
        &test.context,
        TEST_REALM_ID,
        summary_request(group_id, false),
    )
    .await
    .expect("listing succeeds");

    assert_eq!(result.documents.len(), 1);
    assert_eq!(result.documents[0].record.document_id, readable.document_id);
    assert_eq!(result.total_estimate, Some(1));
}

#[tokio::test]
async fn cross_shard_unknown() {
    // Local listings cannot resolve claims that may live on another registry shard.
    let test = metadata_test();
    let group_id = Ulid::generate();
    let mut config = RealmConfigDocument::new(TEST_REALM_ID, Vec::new(), 3);
    config.seed_default_placement();
    let mut first = public_record(group_id, Ulid::generate());
    let first_shard = registry_placement(&config, &first).shard;
    let mut second = loop {
        let candidate = public_record(group_id, Ulid::generate());
        if registry_placement(&config, &candidate).shard != first_shard {
            break candidate;
        }
    };
    second.document_path = first.document_path.clone();
    second.permission_path = MetadataRegistryRecord::permission_path_for(
        &TEST_REALM_ID,
        group_id,
        &second.document_path,
        second.document_id,
    );
    let claims = [&first, &second]
        .into_iter()
        .map(|record| PathClaimRecord {
            document_id: MetaResourceId::from_bytes(record.document_id.to_bytes()).unwrap(),
            establishing_event_id: record.establishing_event_id,
            requested_path: record.document_path.clone(),
        })
        .collect::<Vec<_>>();
    let resolution = aruna_core::structs::resolve_path_claim(&claims).unwrap();
    let winner_id = resolution.winner.document_id.as_ulid();
    let loser_id = resolution.conflicts[0].document_id.as_ulid();
    first.public = first.document_id == loser_id;
    second.public = second.document_id == loser_id;
    seed_registry_cache(&test, &first).await;
    seed_registry_cache(&test, &second).await;

    let listed = list_visible_documents(
        &test.context,
        TEST_REALM_ID,
        summary_request(group_id, false),
    )
    .await
    .unwrap();
    assert_eq!(listed.documents.len(), 1);
    assert_eq!(listed.documents[0].record.document_id, loser_id);
    assert_ne!(winner_id, loser_id);
}

#[test]
fn anonymous_limit_clamped() {
    assert_eq!(
        effective_list_limit(None, true),
        DEFAULT_LIST_METADATA_LIMIT
    );
    assert_eq!(
        effective_list_limit(Some(MAX_LIST_METADATA_LIMIT), true),
        ANONYMOUS_LIST_METADATA_LIMIT
    );
    assert_eq!(
        effective_list_limit(Some(MAX_LIST_METADATA_LIMIT), false),
        MAX_LIST_METADATA_LIMIT
    );
    assert_eq!(
        effective_list_limit(Some(usize::MAX), false),
        MAX_LIST_METADATA_LIMIT
    );
    assert_eq!(effective_list_limit(Some(0), true), 1);
}

#[test]
fn policy_scope_limit() {
    let within = (0..METADATA_REGISTRY_CANDIDATE_LIMIT)
        .map(|_| Ulid::generate())
        .collect();
    assert!(check_policy_limit(within).is_ok());

    let over = (0..=METADATA_REGISTRY_CANDIDATE_LIMIT)
        .map(|_| Ulid::generate())
        .collect();
    assert!(matches!(
        check_policy_limit(over),
        Err(MetadataApiError::ServiceUnavailable)
    ));
}

#[test]
fn config_filters_cached() {
    // A cached candidate that realm config no longer lists must disappear
    // from fan-out immediately, without waiting for the snapshot to expire.
    let kept = iroh::SecretKey::from_bytes(&[11u8; 32]).public();
    let removed = iroh::SecretKey::from_bytes(&[12u8; 32]).public();
    let mut config = RealmConfigDocument::new(TEST_REALM_ID, Vec::new(), 2);
    config.ensure_node(kept, RealmNodeKind::Server);

    let authorized =
        authorized_realm_nodes(&config, HashSet::from([kept, removed])).expect("node ids parse");

    assert_eq!(authorized, HashSet::from([kept]));
}

#[test]
fn peers_are_bounded() {
    let local = iroh::SecretKey::from_bytes(&[255u8; 32]).public();
    let mut config = RealmConfigDocument::new(TEST_REALM_ID, Vec::new(), 2);
    config.ensure_node(local, aruna_core::structs::RealmNodeKind::Server);
    for seed in 1u8..=40 {
        config.ensure_node(
            iroh::SecretKey::from_bytes(&[seed; 32]).public(),
            aruna_core::structs::RealmNodeKind::Server,
        );
    }
    config.nodes.push(aruna_core::structs::RealmNode {
        node_id: "invalid-node".to_string(),
        kind: aruna_core::structs::RealmNodeKind::Server,
    });
    let mut reversed = config.clone();
    reversed.nodes.reverse();
    let first = select_forward_peers(
        &config,
        TEST_REALM_ID,
        Ulid::from_parts(0, 1),
        "datasets/lookup",
        local,
    )
    .expect("peer selection succeeds");
    let second = select_forward_peers(
        &reversed,
        TEST_REALM_ID,
        Ulid::from_parts(0, 1),
        "datasets/lookup",
        local,
    )
    .expect("peer selection succeeds");
    assert_eq!(first.len(), METADATA_DISTRIBUTED_QUERY_MAX_NODES);
    assert_eq!(first, second);
}

#[test]
fn fanout_nodes_bounded() {
    let local = iroh::SecretKey::from_bytes(&[255u8; 32]).public();
    let mut nodes = (1u8..=64)
        .map(|seed| iroh::SecretKey::from_bytes(&[seed; 32]).public())
        .collect::<Vec<_>>();
    nodes.push(local);
    let mut reversed = nodes.clone();
    reversed.reverse();
    let first = select_fanout_nodes(&nodes, local, b"metadata-query");
    let second = select_fanout_nodes(&reversed, local, b"metadata-query");

    assert_eq!(first, second);
    assert_eq!(first.len(), METADATA_DISTRIBUTED_QUERY_MAX_NODES);
    assert!(first.contains(&local));
    assert_eq!(first.iter().collect::<HashSet<_>>().len(), first.len());
}

#[tokio::test]
async fn hidden_ids_match() {
    // Missing, private-denied, and policy-denied public ids must all return
    // NotFound so read-by-id cannot probe existence.
    let test = metadata_test();
    let group_id = Ulid::generate();
    let stranger = UserId::local(Ulid::generate(), TEST_REALM_ID);
    write_policy_docs(
        &test,
        group_id,
        user_role(
            UserId::local(Ulid::generate(), TEST_REALM_ID),
            HashMap::from([(
                format!("/{TEST_REALM_ID}/g/{group_id}/**"),
                Permission::WRITE,
            )]),
        ),
        vec![aruna_core::request_policy::RequestPolicy {
            policy_id: Ulid::generate(),
            name: "no-reads".to_string(),
            kind: aruna_core::request_policy::PolicyKind::Deny,
            when: None,
            expression: "permission == 'read'".to_string(),
            enabled: true,
        }],
    )
    .await;

    let public = public_record(group_id, Ulid::generate());
    seed_registry_cache(&test, &public).await;
    let mut private = public_record(group_id, Ulid::generate());
    private.public = false;
    seed_registry_cache(&test, &private).await;
    let missing = public_record(group_id, Ulid::generate());

    for document_id in [public.document_id, private.document_id, missing.document_id] {
        let result = get_visible_document(
            &test.context,
            TEST_REALM_ID,
            GetVisibleMetadataDocumentRequest {
                document_id,
                auth: Some(auth_for(stranger)),
            },
        )
        .await;
        assert!(matches!(result, Err(MetadataApiError::NotFound)));
    }
}

#[tokio::test]
async fn policy_hides_record() {
    // A group deny policy removes one public record from the bulk listing
    // while leaving an allowed public record visible.
    let test = metadata_test();
    let group_id = Ulid::generate();
    let stranger = UserId::local(Ulid::generate(), TEST_REALM_ID);
    let hidden = public_record(group_id, Ulid::generate());
    let visible = public_record(group_id, Ulid::generate());
    write_policy_docs(
        &test,
        group_id,
        HashMap::new(),
        vec![aruna_core::request_policy::RequestPolicy {
            policy_id: Ulid::generate(),
            name: "hide-one".to_string(),
            kind: aruna_core::request_policy::PolicyKind::Deny,
            when: None,
            expression: format!("path.contains('{}')", hidden.document_id),
            enabled: true,
        }],
    )
    .await;
    seed_registry_cache(&test, &hidden).await;
    seed_registry_cache(&test, &visible).await;

    let page = list_visible_documents(
        &test.context,
        TEST_REALM_ID,
        ListVisibleMetadataDocumentsRequest {
            auth: Some(auth_for(stranger)),
            ..summary_request(group_id, false)
        },
    )
    .await
    .expect("listing succeeds");
    assert_eq!(listed_ids(&page), vec![visible.document_id]);
    assert_eq!(page.total_estimate, Some(1));
}

// A caller who holds no role in the group sees the public records only, and
// the counts describe the visible set rather than the scanned one.
#[tokio::test]
async fn stranger_sees_public() {
    let test = metadata_test();
    let group_id = Ulid::generate();
    let stranger = UserId::local(Ulid::generate(), TEST_REALM_ID);
    write_auth_docs(
        &test,
        group_id,
        user_role(
            UserId::local(Ulid::generate(), TEST_REALM_ID),
            HashMap::from([(
                format!("/{TEST_REALM_ID}/g/{group_id}/**"),
                Permission::WRITE,
            )]),
        ),
    )
    .await;
    let visible = public_record(group_id, Ulid::generate());
    seed_registry_cache(&test, &visible).await;
    for _ in 0..2 {
        let mut hidden = public_record(group_id, Ulid::generate());
        hidden.public = false;
        seed_registry_cache(&test, &hidden).await;
    }

    let page = list_visible_documents(
        &test.context,
        TEST_REALM_ID,
        ListVisibleMetadataDocumentsRequest {
            auth: Some(auth_for(stranger)),
            ..summary_request(group_id, false)
        },
    )
    .await
    .expect("listing succeeds");
    assert_eq!(listed_ids(&page), vec![visible.document_id]);
    assert_eq!(page.total_returned, 1);
    assert_eq!(page.total_estimate, Some(1));

    let beyond = list_visible_documents(
        &test.context,
        TEST_REALM_ID,
        ListVisibleMetadataDocumentsRequest {
            offset: Some(1),
            auth: Some(auth_for(stranger)),
            ..summary_request(group_id, false)
        },
    )
    .await
    .expect("listing succeeds");
    assert!(beyond.documents.is_empty());
    assert_eq!(beyond.total_returned, 0);
    assert_eq!(beyond.total_estimate, Some(1));
}

// An unauthenticated caller must not inherit a member's grants.
#[tokio::test]
async fn anonymous_sees_public() {
    let test = metadata_test();
    let group_id = Ulid::generate();
    let member = UserId::local(Ulid::generate(), TEST_REALM_ID);
    write_auth_docs(
        &test,
        group_id,
        user_role(
            member,
            HashMap::from([(
                format!("/{TEST_REALM_ID}/g/{group_id}/meta/**"),
                Permission::READ,
            )]),
        ),
    )
    .await;
    let visible = public_record(group_id, Ulid::generate());
    seed_registry_cache(&test, &visible).await;
    let mut hidden = public_record(group_id, Ulid::generate());
    hidden.public = false;
    seed_registry_cache(&test, &hidden).await;

    let anonymous = list_visible_documents(
        &test.context,
        TEST_REALM_ID,
        summary_request(group_id, false),
    )
    .await
    .expect("listing succeeds");
    assert_eq!(listed_ids(&anonymous), vec![visible.document_id]);
    assert_eq!(anonymous.total_estimate, Some(1));

    let signed = list_visible_documents(
        &test.context,
        TEST_REALM_ID,
        ListVisibleMetadataDocumentsRequest {
            auth: Some(auth_for(member)),
            ..summary_request(group_id, false)
        },
    )
    .await
    .expect("listing succeeds");
    assert_eq!(signed.total_returned, 2);
    assert_eq!(signed.total_estimate, Some(2));
}

#[tokio::test]
async fn foreign_policy_identity() {
    let test = metadata_test();
    let group_id = Ulid::generate();
    let foreign_realm = RealmId([8u8; 32]);
    let foreign_user = UserId::local(Ulid::generate(), foreign_realm);
    let record = public_record(group_id, Ulid::generate());
    write_policy_docs(
        &test,
        group_id,
        HashMap::new(),
        vec![aruna_core::request_policy::RequestPolicy {
            policy_id: Ulid::generate(),
            name: "foreign-user".to_string(),
            kind: aruna_core::request_policy::PolicyKind::Require,
            when: None,
            expression: format!("user == '{foreign_user}'"),
            enabled: true,
        }],
    )
    .await;
    seed_registry_cache(&test, &record).await;
    let auth = AuthContext {
        user_id: foreign_user,
        realm_id: foreign_realm,
        path_restrictions: None,
        session: None,
    };

    let listed = list_visible_documents(
        &test.context,
        TEST_REALM_ID,
        ListVisibleMetadataDocumentsRequest {
            auth: Some(auth.clone()),
            ..summary_request(group_id, false)
        },
    )
    .await
    .expect("listing succeeds");
    assert_eq!(listed_ids(&listed), vec![record.document_id]);

    let candidates = local_path_candidates(
        &test.context,
        TEST_REALM_ID,
        group_id,
        &record.document_path,
        Some(&auth),
    )
    .await
    .expect("path lookup succeeds");
    assert_eq!(candidates.len(), 1);
    assert!(candidates[0].record.is_some());
}

// A per-document DENY inside a group-wide grant: the estimate must decide
// each document, not reuse one representative answer for the whole group.
#[tokio::test]
async fn estimate_counts_exact() {
    let test = metadata_test();
    let group_id = Ulid::generate();
    let member = UserId::local(Ulid::generate(), TEST_REALM_ID);
    let mut allowed = public_record(group_id, Ulid::generate());
    allowed.public = false;
    let mut denied = public_record(group_id, Ulid::generate());
    denied.public = false;
    write_auth_docs(
        &test,
        group_id,
        user_role(
            member,
            HashMap::from([
                (
                    format!("/{TEST_REALM_ID}/g/{group_id}/meta/**"),
                    Permission::READ,
                ),
                (denied.permission_path.clone(), Permission::DENY),
            ]),
        ),
    )
    .await;
    seed_registry_cache(&test, &allowed).await;
    seed_registry_cache(&test, &denied).await;

    let page = list_visible_documents(
        &test.context,
        TEST_REALM_ID,
        ListVisibleMetadataDocumentsRequest {
            auth: Some(auth_for(member)),
            ..summary_request(group_id, false)
        },
    )
    .await
    .expect("listing succeeds");
    assert_eq!(listed_ids(&page), vec![allowed.document_id]);
    assert_eq!(page.total_estimate, Some(1));

    // A targeted lookup still reports no estimate for the same caller.
    let lookup = list_visible_documents(
        &test.context,
        TEST_REALM_ID,
        ListVisibleMetadataDocumentsRequest {
            limit: Some(METADATA_ESTIMATE_MIN_LIMIT - 1),
            auth: Some(auth_for(member)),
            ..summary_request(group_id, false)
        },
    )
    .await
    .expect("listing succeeds");
    assert_eq!(lookup.total_returned, 1);
    assert_eq!(lookup.total_estimate, None);
}

// path_prefix must scope the estimate to the same set the page came from.
#[tokio::test]
async fn estimate_honours_prefix() {
    let test = metadata_test();
    let group_id = Ulid::generate();
    for _ in 0..2 {
        seed_registry_cache(&test, &public_record(group_id, Ulid::generate())).await;
    }
    let mut other = public_record(group_id, Ulid::generate());
    other.document_path = "other/excluded".to_string();
    seed_registry_cache(&test, &other).await;

    let result = list_visible_documents(
        &test.context,
        TEST_REALM_ID,
        ListVisibleMetadataDocumentsRequest {
            path_prefix: Some("datasets".to_string()),
            ..summary_request(group_id, false)
        },
    )
    .await
    .expect("listing succeeds");

    assert_eq!(result.total_returned, 2);
    assert_eq!(result.total_estimate, Some(2));
}

// Update stamps deliberately disagree with the ascending document ids.
async fn seed_timed_records(test: &MetadataTest, group_id: GroupId) -> Vec<MetadataRegistryRecord> {
    let mut records = Vec::new();
    for updated_at_ms in [10u64, 30, 20] {
        let mut record = public_record(group_id, Ulid::generate());
        record.updated_at_ms = updated_at_ms;
        seed_registry_cache(test, &record).await;
        records.push(record);
    }
    records
}

fn listed_ids(result: &ListVisibleMetadataDocumentsResult) -> Vec<Ulid> {
    result
        .documents
        .iter()
        .map(|document| document.record.document_id)
        .collect()
}

// Recency ordering must precede the offset window so pages walk it too.
#[tokio::test]
async fn orders_recent_first() {
    let test = metadata_test();
    let group_id = Ulid::generate();
    let records = seed_timed_records(&test, group_id).await;

    let page = list_visible_documents(
        &test.context,
        TEST_REALM_ID,
        ListVisibleMetadataDocumentsRequest {
            order: MetadataListOrder::Recent,
            ..summary_request(group_id, false)
        },
    )
    .await
    .expect("listing succeeds");
    assert_eq!(
        listed_ids(&page),
        vec![
            records[1].document_id,
            records[2].document_id,
            records[0].document_id
        ]
    );

    let second = list_visible_documents(
        &test.context,
        TEST_REALM_ID,
        ListVisibleMetadataDocumentsRequest {
            limit: Some(1),
            offset: Some(1),
            order: MetadataListOrder::Recent,
            ..summary_request(group_id, false)
        },
    )
    .await
    .expect("listing succeeds");
    assert_eq!(listed_ids(&second), vec![records[2].document_id]);
}

// The default page stays in ascending document id order.
#[tokio::test]
async fn default_keeps_created() {
    let test = metadata_test();
    let group_id = Ulid::generate();
    let records = seed_timed_records(&test, group_id).await;

    let page = list_visible_documents(
        &test.context,
        TEST_REALM_ID,
        summary_request(group_id, false),
    )
    .await
    .expect("listing succeeds");

    let mut expected = records
        .iter()
        .map(|record| record.document_id)
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(listed_ids(&page), expected);
}

pub(super) fn auth_for(user_id: UserId) -> AuthContext {
    AuthContext {
        user_id,
        realm_id: TEST_REALM_ID,
        path_restrictions: None,
        session: None,
    }
}

pub(super) fn user_role(
    user_id: UserId,
    permissions: HashMap<String, Permission>,
) -> HashMap<RoleId, Role> {
    let role_id = Ulid::generate();
    HashMap::from([(
        role_id,
        Role {
            role_id,
            name: "listing".to_string(),
            permissions,
            assigned_users: HashSet::from([user_id]),
        },
    )])
}

// The rules collection reads both documents; without them a group yields no
// rules and every non-public record in it stays hidden.
pub(super) async fn write_auth_docs(
    test: &MetadataTest,
    group_id: GroupId,
    roles: HashMap<RoleId, Role>,
) {
    write_policy_docs(test, group_id, roles, Vec::new()).await;
}

pub(super) async fn write_policy_docs(
    test: &MetadataTest,
    group_id: GroupId,
    roles: HashMap<RoleId, Role>,
    policies: Vec<aruna_core::request_policy::RequestPolicy>,
) {
    let actor = Actor {
        node_id: iroh::SecretKey::from_bytes(&[7u8; 32]).public(),
        user_id: UserId::local(Ulid::generate(), TEST_REALM_ID),
        realm_id: TEST_REALM_ID,
    };
    let realm_doc = RealmAuthorizationDocument::default_realm_doc(TEST_REALM_ID);
    let group = Group {
        display_name: "Test".to_string(),
        group_id,
        realm_id: TEST_REALM_ID,
        owner: actor.user_id,
        roles: roles.keys().copied().collect(),
    };
    let group_doc = GroupAuthorizationDocument {
        group_id,
        roles,
        policies,
    };
    // The policy evaluator reads the group through GetGroupOperation, which needs the group
    // record as well as the auth doc, and fails closed without the realm config.
    let entries = [
        (
            aruna_core::keyspaces::REALM_CONFIG_KEYSPACE,
            Key::from(*TEST_REALM_ID.as_bytes()),
            RealmConfigDocument::default_for_realm(TEST_REALM_ID, Vec::new())
                .to_bytes(&actor)
                .expect("realm config encodes"),
        ),
        (
            AUTH_KEYSPACE,
            Key::from(*TEST_REALM_ID.as_bytes()),
            realm_doc.to_bytes(&actor).expect("realm doc encodes"),
        ),
        (
            AUTH_KEYSPACE,
            Key::from(group_id.to_bytes()),
            group_doc.to_bytes(&actor).expect("group doc encodes"),
        ),
        (
            GROUP_KEYSPACE,
            Key::from(group_id.to_bytes()),
            group.to_bytes(&actor).expect("group encodes"),
        ),
    ];
    for (key_space, key, value) in entries {
        match test
            .context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.to_string(),
                key,
                value: value.into(),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected write event: {other:?}"),
        }
    }
}

pub(super) fn summary_request(
    group_id: GroupId,
    include_summary: bool,
) -> ListVisibleMetadataDocumentsRequest {
    ListVisibleMetadataDocumentsRequest {
        group_id: Some(group_id),
        path_prefix: None,
        include_summary,
        limit: None,
        offset: None,
        order: MetadataListOrder::default(),
        auth: None,
    }
}

// The visibility cache only accepts upserts once it has been filled.
