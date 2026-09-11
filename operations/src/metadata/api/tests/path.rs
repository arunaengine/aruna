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
