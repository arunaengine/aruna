use super::*;

#[test]
fn outsider_reports_rejected() {
    // A configured User node's barrier, an unrelated server's proof, and an
    // unknown-transition stall never enter replicated state; its own report does.
    let realm_id = RealmId::from_bytes([61u8; 32]);
    let strategy_id = Ulid::from_parts(1_700, 1);
    let transition_id = Ulid::from_parts(1_701, 1);
    let mut config = aruna_core::structs::RealmConfigDocument::new(realm_id, Vec::new(), 3);
    for (seed, kind) in [
        (1u8, RealmNodeKind::Server),
        (2, RealmNodeKind::Server),
        (
            3,
            RealmNodeKind::User {
                owner: UserId::nil(realm_id),
            },
        ),
        (4, RealmNodeKind::Server),
    ] {
        config.ensure_node(node(seed), kind);
    }
    config
        .placement_transitions
        .push(aruna_core::structs::PlacementTransition::new(
            aruna_core::structs::TransitionPlan {
                transition_id,
                strategy_id,
                buckets: vec![aruna_core::structs::BucketPlan {
                    bucket: 0,
                    old_holders: vec![node(1)],
                    target_holders: vec![node(2)],
                    predecessor_epoch: 1,
                }],
                target_map_epoch: 2,
                limits: Default::default(),
                created_by: node(1),
                created_at_ms: 1,
            },
        ));
    let barrier = |seed: u8| {
        let actor = test_actor(seed, UserId::nil(realm_id), realm_id);
        test_admin_event(
            Ulid::from_parts(1_702, seed as u128),
            AdminDocumentTarget::RealmConfig { realm_id },
            &actor,
            1,
            AdminDocumentOperation::RealmConfigTransitionBarrierReported {
                transition_id,
                bucket: 0,
                reported_by: node(seed),
                frontier: vec![seed],
            },
        )
    };

    let rejected = |event: &AdminDocumentEvent| {
        matches!(
            validate_config_authority(Some(&config), event, None),
            Ok(AdminEventValidation::Rejected(_))
        )
    };
    assert!(rejected(&barrier(3)), "a User node is never a participant");
    assert!(rejected(&barrier(4)), "an unrelated server is rejected");
    assert!(
        matches!(
            validate_config_authority(Some(&config), &barrier(1), None),
            Ok(AdminEventValidation::Accepted)
        ),
        "the planned old holder's own barrier is accepted"
    );

    // A proof from a non-target is rejected before any signature check.
    let foreign_proof = {
        let actor = test_actor(4, UserId::nil(realm_id), realm_id);
        let claim = aruna_core::structs::ProofClaim {
            realm_id,
            transition_id,
            strategy_id,
            bucket: 0,
            old_activation_epoch: 1,
            target_map_epoch: 2,
            barrier_digest: [0; 32],
            checkpoint_root: [0; 32],
            holder: node(4),
        };
        test_admin_event(
            Ulid::from_parts(1_703, 1),
            AdminDocumentTarget::RealmConfig { realm_id },
            &actor,
            2,
            AdminDocumentOperation::RealmConfigTransitionProofSubmitted {
                transition_id,
                strategy_id,
                proof: claim.sign(&iroh::SecretKey::from_bytes(&[4; 32])),
            },
        )
    };
    assert!(rejected(&foreign_proof));

    // A report naming an unknown transition defers on the same topic only:
    // a cross-topic dependency would park realm-config replay forever.
    let unknown = {
        let actor = test_actor(1, UserId::nil(realm_id), realm_id);
        test_admin_event(
            Ulid::from_parts(1_704, 1),
            AdminDocumentTarget::RealmConfig { realm_id },
            &actor,
            3,
            AdminDocumentOperation::RealmConfigTransitionStallReported {
                transition_id: Ulid::from_parts(1_705, 1),
                bucket: 0,
                reported_by: node(1),
                reason: "sources unreachable".to_string(),
            },
        )
    };
    assert!(matches!(
        validate_config_authority(Some(&config), &unknown, None),
        Ok(AdminEventValidation::Deferred {
            dependency: None,
            ..
        })
    ));
}

#[test]
fn removal_needs_management() {
    // Eviction is an ordinary realm-config admin event, so admission keeps
    // it to Management origins whoever relayed it.
    let realm_id = RealmId::from_bytes([62u8; 32]);
    let mut config = aruna_core::structs::RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.ensure_node(node(1), RealmNodeKind::Management);
    config.ensure_node(node(2), RealmNodeKind::Server);
    let device = node(3);
    config.ensure_node(
        device,
        RealmNodeKind::User {
            owner: UserId::nil(realm_id),
        },
    );

    let removal = |seed: u8| {
        let actor = test_actor(seed, UserId::nil(realm_id), realm_id);
        test_admin_event(
            Ulid::from_parts(1_710, seed as u128),
            AdminDocumentTarget::RealmConfig { realm_id },
            &actor,
            1,
            AdminDocumentOperation::RealmConfigNodeRemoved { node_id: device },
        )
    };

    assert!(matches!(
        validate_config_authority(Some(&config), &removal(1), None),
        Ok(AdminEventValidation::Accepted)
    ));
    for origin in [2, 3] {
        assert!(matches!(
            validate_config_authority(Some(&config), &removal(origin), None),
            Ok(AdminEventValidation::Rejected(_))
        ));
    }
}
