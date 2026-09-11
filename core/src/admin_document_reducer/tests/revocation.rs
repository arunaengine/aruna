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
