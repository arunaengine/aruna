use super::*;

#[test]
fn user_result_opaque() {
    // An unreadable winner still participates and cannot promote a readable loser.
    let group_id = Ulid::generate();
    let first = public_record(group_id, Ulid::generate());
    let mut second = public_record(group_id, Ulid::generate());
    second.document_path = first.document_path.clone();
    let mut candidates = [&first, &second]
        .into_iter()
        .map(|record| MetadataPathCandidate {
            claim: PathClaimRecord {
                document_id: MetaResourceId::from_bytes(record.document_id.to_bytes()).unwrap(),
                establishing_event_id: record.establishing_event_id,
                requested_path: record.document_path.clone(),
            },
            record: Some(record.clone()),
        })
        .collect::<Vec<_>>();
    let claims = candidates
        .iter()
        .map(|candidate| candidate.claim.clone())
        .collect::<Vec<_>>();
    let winner = aruna_core::structs::resolve_path_claim(&claims)
        .unwrap()
        .winner;
    let hidden_id = winner.document_id.to_bytes();
    candidates
        .iter_mut()
        .find(|candidate| candidate.claim == winner)
        .unwrap()
        .record = None;

    let result = reduce_path_candidates(candidates);
    assert!(matches!(result, Err(MetadataApiError::NotFound)));
    let response = MetadataTransportMessage::ForwardedPathResolution {
        result: Err(MetadataReadError::NotFound),
    };
    let encoded = postcard::to_allocvec(&response).unwrap();
    assert!(
        !encoded
            .windows(hidden_id.len())
            .any(|window| window == hidden_id)
    );
}

fn path_candidate(record: &MetadataRegistryRecord) -> MetadataPathCandidate {
    MetadataPathCandidate {
        claim: PathClaimRecord {
            document_id: MetaResourceId::from_bytes(record.document_id.to_bytes()).unwrap(),
            establishing_event_id: record.establishing_event_id,
            requested_path: record.document_path.clone(),
        },
        record: Some(record.clone()),
    }
}

#[test]
fn missing_replica_fails() {
    let record = public_record(Ulid::generate(), Ulid::generate());
    let views = vec![PathShardView {
        shard: 0,
        candidates: vec![path_candidate(&record)],
    }];

    assert!(matches!(
        merge_path_views(&[2], views),
        Err(MetadataApiError::ServiceUnavailable)
    ));
}

#[test]
fn stale_replica_fails() {
    let record = public_record(Ulid::generate(), Ulid::generate());
    let views = vec![
        PathShardView {
            shard: 0,
            candidates: vec![path_candidate(&record)],
        },
        PathShardView {
            shard: 0,
            candidates: Vec::new(),
        },
    ];

    assert!(matches!(
        merge_path_views(&[2], views),
        Err(MetadataApiError::ServiceUnavailable)
    ));
}

#[test]
fn divergent_claims_fail() {
    let record = public_record(Ulid::generate(), Ulid::generate());
    let mut divergent = record.clone();
    divergent.establishing_event_id = Ulid::generate();
    let views = vec![
        PathShardView {
            shard: 0,
            candidates: vec![path_candidate(&record)],
        },
        PathShardView {
            shard: 0,
            candidates: vec![path_candidate(&divergent)],
        },
    ];

    assert!(matches!(
        merge_path_views(&[2], views),
        Err(MetadataApiError::ServiceUnavailable)
    ));
}

#[test]
fn divergent_evidence_fails() {
    let record = public_record(Ulid::generate(), Ulid::generate());
    let mut divergent = record.clone();
    divergent.public = false;
    let views = vec![
        PathShardView {
            shard: 0,
            candidates: vec![path_candidate(&record)],
        },
        PathShardView {
            shard: 0,
            candidates: vec![path_candidate(&divergent)],
        },
    ];

    assert!(matches!(
        merge_path_views(&[2], views),
        Err(MetadataApiError::ServiceUnavailable)
    ));
}

#[test]
fn invalid_resolution_fails() {
    let record = public_record(Ulid::generate(), Ulid::generate());
    let mut unrelated = MetadataPathResolution {
        winner: sanitize_path_winner(record.clone()).unwrap(),
        conflicts: Vec::new(),
    };
    unrelated.winner.group_id = Ulid::generate();
    assert!(matches!(
        validate_path_resolution(
            TEST_REALM_ID,
            record.group_id,
            &record.document_path,
            &unrelated,
        ),
        Err(MetadataApiError::ServiceUnavailable)
    ));

    let duplicate = MetadataPathResolution {
        winner: sanitize_path_winner(record.clone()).unwrap(),
        conflicts: vec![record.document_id],
    };
    assert!(matches!(
        validate_path_resolution(
            TEST_REALM_ID,
            record.group_id,
            &record.document_path,
            &duplicate,
        ),
        Err(MetadataApiError::ServiceUnavailable)
    ));
}

#[test]
fn winner_wire_sanitized() {
    let mut record = public_record(Ulid::generate(), Ulid::generate());
    let holder = iroh::SecretKey::from_bytes(&[42u8; 32]).public();
    record.holder_node_ids = vec![holder];
    let permission_path = record.permission_path.clone();
    let winner = sanitize_path_winner(record).unwrap();
    let encoded = postcard::to_allocvec(&winner).unwrap();

    assert!(
        !encoded
            .windows(holder.as_bytes().len())
            .any(|window| window == holder.as_bytes())
    );
    assert!(
        !encoded
            .windows(permission_path.len())
            .any(|window| window == permission_path.as_bytes())
    );
}

fn path_config(
    nodes: u8,
    shard_count: u32,
    replica_count: Option<u32>,
) -> (RealmConfigDocument, Ulid) {
    let mut config = RealmConfigDocument::new(TEST_REALM_ID, Vec::new(), 3);
    let strategy = aruna_core::structs::PlacementStrategy {
        strategy_id: Ulid::from_bytes([5u8; 16]),
        name: "metadata-registry".to_string(),
        replica_count,
        distinct_locations: false,
        affinity: Vec::new(),
        shard_count,
    };
    config.default_strategy_id = Some(strategy.strategy_id);
    config.strategies = vec![strategy.clone()];
    for seed in 1..=nodes {
        config.ensure_node(
            iroh::SecretKey::from_bytes(&[seed; 32]).public(),
            RealmNodeKind::Server,
        );
    }
    (config, strategy.strategy_id)
}

fn holder_deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(30)
}

#[tokio::test]
async fn shard_counts_match() {
    // The reported replica count per shard is what the merge waits for, so
    // it must equal the holders the selection actually dispatches to.
    let (config, strategy_id) = path_config(6, 8, Some(2));
    let local = iroh::SecretKey::from_bytes(&[1u8; 32]).public();

    let (selections, replica_counts) = select_path_holders(
        &config,
        TEST_REALM_ID,
        Ulid::from_parts(0, 1),
        "datasets/lookup",
        strategy_id,
        8,
        Some(2),
        local,
        holder_deadline(),
    )
    .expect("holder selection succeeds");

    assert_eq!(replica_counts.len(), 8);
    assert!(selections.len() <= METADATA_DISTRIBUTED_QUERY_MAX_NODES);
    for (shard, expected) in replica_counts.iter().copied().enumerate() {
        let dispatched = selections
            .iter()
            .filter(|selection| selection.shards.contains(&(shard as u32)))
            .count();
        assert_eq!(expected, dispatched);
        assert!(expected > 0);
    }
}

#[tokio::test]
async fn everywhere_covers_shards() {
    // An everywhere strategy places every holder on every shard, so each
    // selection must answer for all of them.
    let (config, strategy_id) = path_config(4, 8, None);
    let local = iroh::SecretKey::from_bytes(&[1u8; 32]).public();

    let (selections, replica_counts) = select_path_holders(
        &config,
        TEST_REALM_ID,
        Ulid::from_parts(0, 1),
        "datasets/lookup",
        strategy_id,
        8,
        None,
        local,
        holder_deadline(),
    )
    .expect("holder selection succeeds");

    assert_eq!(selections.len(), 4);
    assert_eq!(replica_counts, vec![4; 8]);
    for selection in &selections {
        assert_eq!(selection.shards, (0..8).collect::<Vec<_>>());
    }
}

#[tokio::test]
async fn capped_shard_rejected() {
    // More shard holders than the fan-out cap leaves shards with nobody to
    // ask; the lookup must fail instead of resolving from the rest.
    let (config, strategy_id) = path_config(200, 64, Some(1));
    let local = iroh::SecretKey::from_bytes(&[1u8; 32]).public();

    let result = select_path_holders(
        &config,
        TEST_REALM_ID,
        Ulid::from_parts(0, 1),
        "datasets/lookup",
        strategy_id,
        64,
        Some(1),
        local,
        holder_deadline(),
    );

    assert!(matches!(result, Err(MetadataApiError::ServiceUnavailable)));
}

#[test]
fn divergent_paths_fail() {
    // Two peers that resolved the same path differently must not be
    // reduced to one of the two answers.
    let winner = sanitize_path_winner(public_record(Ulid::generate(), Ulid::generate()))
        .expect("valid path winner");
    let result = MetadataPathLookupResult {
        winner,
        conflicts: Vec::new(),
    };

    let resolved = reduce_path_response(Some(result.clone()), None, false, false, false)
        .expect("a single agreeing answer resolves");
    assert_eq!(resolved.winner, result.winner);
    assert!(matches!(
        reduce_path_response(Some(result), None, true, false, false),
        Err(MetadataApiError::ServiceUnavailable)
    ));
    assert!(matches!(
        reduce_path_response(None, None, false, true, false),
        Err(MetadataApiError::NotFound)
    ));
    assert!(matches!(
        reduce_path_response(None, None, false, false, false),
        Err(MetadataApiError::ServiceUnavailable)
    ));
}

#[test]
fn path_denial_wins() {
    let record = public_record(Ulid::generate(), Ulid::generate());
    let result = MetadataPathLookupResult {
        winner: sanitize_path_winner(record).expect("valid path winner"),
        conflicts: Vec::new(),
    };

    assert!(matches!(
        reduce_path_response(
            Some(result.clone()),
            Some(MetadataReadError::Forbidden),
            false,
            false,
            false,
        ),
        Err(MetadataApiError::Forbidden)
    ));
    assert!(matches!(
        reduce_path_response(Some(result.clone()), None, false, true, false),
        Err(MetadataApiError::ServiceUnavailable)
    ));

    assert!(matches!(
        reduce_path_response(Some(result), None, false, false, true,),
        Err(MetadataApiError::ServiceUnavailable)
    ));
}
