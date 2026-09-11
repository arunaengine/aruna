use super::*;

pub(super) fn revoke_token(event_seed: u8, origin_seed: u8, token: &str) -> AdminDocumentEvent {
    realm_config_event(
        event_seed,
        node(origin_seed),
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::RealmConfigTokenRevoked {
            token_hash: crate::auth::bearer_token_hash(token),
            expires_at: 2_000,
            token_owner: user_id(),
        },
    )
}

pub(super) fn revoke_token_at(
    event_seed: u8,
    origin_seed: u8,
    token: &str,
    expires_at: u64,
) -> AdminDocumentEvent {
    realm_config_event(
        event_seed,
        node(origin_seed),
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::RealmConfigTokenRevoked {
            token_hash: crate::auth::bearer_token_hash(token),
            expires_at,
            token_owner: user_id(),
        },
    )
}

pub(super) fn revoke_token_owned(
    event_seed: u8,
    origin_seed: u8,
    token: &str,
    expires_at: u64,
    token_owner: UserId,
) -> AdminDocumentEvent {
    realm_config_event(
        event_seed,
        node(origin_seed),
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::RealmConfigTokenRevoked {
            token_hash: crate::auth::bearer_token_hash(token),
            expires_at,
            token_owner,
        },
    )
}

#[test]
fn revocations_accumulate() {
    // Two origins revoking different tokens both survive, and a repeat of
    // one revocation is not a conflict.
    let mut state = realm_config_state();
    state.apply(&revoke_token(1, 1, "first")).unwrap();
    state.apply(&revoke_token(2, 2, "second")).unwrap();
    state.apply(&revoke_token(3, 2, "first")).unwrap();

    assert!(state.conflicts.is_empty());
    assert_eq!(
        state.materialized_revoked_tokens(),
        BTreeMap::from([
            (crate::auth::bearer_token_hash("first"), 2_000),
            (crate::auth::bearer_token_hash("second"), 2_000),
        ])
    );
}

#[test]
fn repeated_revocations_bound() {
    let mut state = realm_config_state();
    let events: Vec<_> = (1..=8)
        .map(|seed| revoke_token(seed, seed, "repeat"))
        .collect();

    for (index, event) in events.iter().enumerate() {
        let expected = if index == 0 {
            AdminDocumentApplyStatus::Applied
        } else {
            AdminDocumentApplyStatus::Redundant
        };
        assert_eq!(state.apply(event), Ok(expected));
    }

    let path =
        super::revoked_token_path(&crate::auth::bearer_token_hash("repeat"), 2_000, &user_id());
    assert_eq!(state.user_subject_ids.len(), 1);
    assert_eq!(state.user_subject_ids[&path].dot, events[0].dot());
    assert!(state.equivalent_value_dots.is_empty());
    assert_eq!(
        state.applied_event_ids,
        BTreeSet::from([events[0].event_id])
    );
    assert_eq!(state.clock.sequence_for(&node(8)), 1);
}

#[test]
fn revocation_order_converges() {
    let equal_first = revoke_token_at(10, 1, "equal", 4_000);
    let equal_second = revoke_token_at(11, 2, "equal", 4_000);
    let mut equal_left = realm_config_state();
    equal_left.apply(&equal_first).unwrap();
    equal_left.apply(&equal_second).unwrap();
    let mut equal_right = realm_config_state();
    equal_right.apply(&equal_second).unwrap();
    equal_right.apply(&equal_first).unwrap();

    assert_eq!(equal_left, equal_right);
    let equal_path =
        super::revoked_token_path(&crate::auth::bearer_token_hash("equal"), 4_000, &user_id());
    assert_eq!(
        equal_left.user_subject_ids[&equal_path].dot,
        equal_first.dot()
    );

    let shorter = revoke_token_at(12, 1, "different", 2_000);
    let longer = revoke_token_at(13, 2, "different", 5_000);
    let mut different_left = realm_config_state();
    different_left.apply(&shorter).unwrap();
    different_left.apply(&longer).unwrap();
    let mut different_right = realm_config_state();
    different_right.apply(&longer).unwrap();
    different_right.apply(&shorter).unwrap();

    assert_eq!(different_left, different_right);
    let longer_path = super::revoked_token_path(
        &crate::auth::bearer_token_hash("different"),
        5_000,
        &user_id(),
    );
    assert_eq!(different_left.user_subject_ids.len(), 1);
    assert_eq!(
        different_left.user_subject_ids[&longer_path].dot,
        longer.dot()
    );
}

#[test]
fn compaction_canonicalizes() {
    let hash = crate::auth::bearer_token_hash("legacy");
    let longer = revoke_token_at(20, 1, "legacy", 5_000);
    let equal = revoke_token_at(21, 2, "legacy", 5_000);
    let shorter = revoke_token_at(22, 3, "legacy", 3_000);
    let longer_path = super::revoked_token_path(&hash, 5_000, &user_id());
    let shorter_path = super::revoked_token_path(&hash, 3_000, &user_id());
    let mut state = realm_config_state();
    state.user_subject_ids.insert(
        longer_path.clone(),
        AdminDocumentAttributeVersion {
            value: Some("5000".to_string()),
            dot: longer.dot(),
        },
    );
    state
        .equivalent_value_dots
        .insert(longer_path.clone(), BTreeSet::from([equal.dot()]));
    state.user_subject_ids.insert(
        shorter_path,
        AdminDocumentAttributeVersion {
            value: Some("3000".to_string()),
            dot: shorter.dot(),
        },
    );
    state
        .applied_event_ids
        .extend([longer.event_id, equal.event_id, shorter.event_id]);
    let description_first = set_realm_config_description(23, 4, "first");
    let description_second = set_realm_config_description(24, 5, "second");
    state.apply(&description_first).unwrap();
    state.apply(&description_second).unwrap();

    state.compact_revocations(1_000);

    assert_eq!(state.user_subject_ids.len(), 1);
    assert_eq!(state.user_subject_ids[&longer_path].dot, longer.dot());
    assert!(state.equivalent_value_dots.is_empty());
    assert!(!state.applied_event_ids.contains(&equal.event_id));
    assert!(!state.applied_event_ids.contains(&shorter.event_id));
    assert!(
        state
            .conflicts
            .contains_key(super::REALM_CONFIG_DESCRIPTION_PATH)
    );
    assert!(
        state
            .applied_event_ids
            .contains(&description_first.event_id)
    );
    assert!(
        state
            .applied_event_ids
            .contains(&description_second.event_id)
    );
}

#[test]
fn revocation_origin_count() {
    let mut state = realm_config_state();
    state.apply(&revoke_token_at(30, 1, "one", 5_000)).unwrap();
    state.apply(&revoke_token_at(31, 1, "two", 5_000)).unwrap();
    state
        .apply(&revoke_token_at(32, 2, "three", 5_000))
        .unwrap();
    state.compact_revocations(1_000);

    assert_eq!(state.live_revocation_count(&node(1), 1_000), 2);
    assert_eq!(state.live_revocation_count(&node(2), 1_000), 1);
    assert_eq!(state.live_revocation_count(&node(3), 1_000), 0);
    assert_eq!(
        state.revocation_origin(&crate::auth::bearer_token_hash("one")),
        Some(node(1))
    );
    assert_eq!(
        state.revocation_origin(&crate::auth::bearer_token_hash("three")),
        Some(node(2))
    );
    assert_eq!(state.revocation_origin("missing"), None);
    assert_eq!(user_state().revocation_origin("missing"), None);
}

#[test]
fn owner_conflict_order() {
    let owner_a = user_id_with_seed(1);
    let owner_b = user_id_with_seed(2);
    let first = revoke_token_owned(40, 1, "owned", 5_000, owner_a);
    let second = revoke_token_owned(41, 2, "owned", 5_000, owner_b);
    let mut left = realm_config_state();
    left.apply(&first).unwrap();
    left.apply(&second).unwrap();
    let mut right = realm_config_state();
    right.apply(&second).unwrap();
    right.apply(&first).unwrap();

    assert_eq!(left, right);
    let hash = crate::auth::bearer_token_hash("owned");
    let path = super::revoked_token_path(&hash, 5_000, &owner_a);
    assert_eq!(left.user_subject_ids.len(), 1);
    assert!(left.user_subject_ids.contains_key(&path));
}

#[test]
fn stale_conflict_removed() {
    let owner = user_id();
    let canonical = revoke_token_owned(42, 1, "stale", 5_000, owner);
    let stale = revoke_token_owned(43, 2, "stale", 5_000, owner);
    let path = super::revoked_token_path(&crate::auth::bearer_token_hash("stale"), 5_000, &owner);
    let mut state = realm_config_state();
    state.user_subject_ids.insert(
        path.clone(),
        AdminDocumentAttributeVersion {
            value: Some("5000".to_string()),
            dot: canonical.dot(),
        },
    );
    state.conflicts.insert(
        path.clone(),
        AdminDocumentConflict {
            path: path.clone(),
            values: vec![AdminDocumentConflictValue {
                value: Some("4000".to_string()),
                dot: stale.dot(),
            }],
        },
    );
    state
        .applied_event_ids
        .extend([canonical.event_id, stale.event_id]);

    state.compact_revocations(1_000);

    assert!(state.conflicts.is_empty());
    assert_eq!(state.user_subject_ids[&path].dot, canonical.dot());
    assert!(state.applied_event_ids.contains(&canonical.event_id));
    assert!(!state.applied_event_ids.contains(&stale.event_id));
}

#[test]
fn expired_count() {
    let mut state = realm_config_state();
    state
        .apply(&revoke_token_at(44, 1, "expired-count", 1_000))
        .unwrap();
    state
        .apply(&revoke_token_at(45, 1, "live-count", 2_000))
        .unwrap();

    assert_eq!(state.live_revocation_count(&node(1), 1_000), 2);
    assert_eq!(state.live_revocation_count(&node(1), 1_001), 2);
    state.compact_revocations(1_001);
    assert_eq!(state.live_revocation_count(&node(1), 1_001), 2);
}

#[test]
fn owner_count() {
    let owner_a = user_id_with_seed(3);
    let owner_b = user_id_with_seed(4);
    let mut state = realm_config_state();
    for (seed, token, owner) in [
        (46u8, "owner-a-one", owner_a),
        (47u8, "owner-a-two", owner_a),
        (48u8, "owner-b-one", owner_b),
    ] {
        state
            .apply(&revoke_token_owned(seed, 1, token, 2_000, owner))
            .unwrap();
    }
    state
        .apply(&revoke_token_owned(49, 2, "owner-a-three", 2_000, owner_a))
        .unwrap();
    state.compact_revocations(1_000);

    assert_eq!(state.live_revocation_count(&node(1), 1_000), 3);
    assert_eq!(state.live_owner_count(&node(1), &owner_a, 1_000), 2);
    assert_eq!(state.live_owner_count(&node(1), &owner_b, 1_000), 1);
    assert_eq!(state.live_owner_count(&node(2), &owner_a, 1_000), 1);
}

#[test]
fn index_counts_grace() {
    let mut state = realm_config_state();
    state.apply(&revoke_token_at(50, 1, "grace", 900)).unwrap();

    let index = state.revocation_index(1_000);
    assert_eq!(index.count(&node(1)), 1);
    assert_eq!(index.materialized(), BTreeMap::new());
    assert_eq!(
        index.origin(&crate::auth::bearer_token_hash("grace")),
        Some(node(1))
    );

    state.compact_revocations(1_200);
    let path = super::revoked_token_path(&crate::auth::bearer_token_hash("grace"), 900, &user_id());
    assert!(state.user_subject_ids.contains_key(&path));
    state.compact_revocations(1_201);
    assert!(!state.user_subject_ids.contains_key(&path));
}

#[test]
fn expiry_schedule_bounds() {
    let mut state = realm_config_state();
    let event = revoke_token_at(52, 1, "scheduled", 2_000);
    state.apply(&event).unwrap();

    assert_eq!(
        state.revocation_next_expiry,
        Some(2_000 + REVOCATION_GRACE_SECS)
    );
    assert!(!state.revocation_compaction_due(2_000 + REVOCATION_GRACE_SECS));

    state
        .apply(&set_realm_config_description(53, 2, "unrelated"))
        .unwrap();
    state.advance_revocation_floor(2_100);
    assert!(!state.revocation_compaction_due(2_100));
    assert_eq!(
        state.revocation_next_expiry,
        Some(2_000 + REVOCATION_GRACE_SECS)
    );

    state.compact_revocations(2_000 + REVOCATION_GRACE_SECS + 1);
    assert_eq!(state.revocation_next_expiry, None);
    assert!(
        state
            .user_subject_ids
            .keys()
            .all(|path| !path.contains("scheduled"))
    );
}

#[test]
fn indexed_apply_refreshes() {
    let mut state = realm_config_state();
    state
        .apply(&revoke_token_at(51, 1, "indexed", 2_000))
        .unwrap();
    let mut index = state.revocation_index(1_000);
    let event = revoke_token_at(52, 2, "indexed", 3_000);

    assert_eq!(
        state.apply_revocation_event(&event, &mut index),
        Ok(AdminDocumentApplyStatus::Applied)
    );
    assert_eq!(
        index.origin(&crate::auth::bearer_token_hash("indexed")),
        Some(node(2))
    );
    assert_eq!(index.count(&node(1)), 0);
    assert_eq!(index.count(&node(2)), 1);
    index.compact(&mut state);
    assert_eq!(
        state.materialized_revoked_tokens(),
        BTreeMap::from([(crate::auth::bearer_token_hash("indexed"), 3_000)])
    );
}
