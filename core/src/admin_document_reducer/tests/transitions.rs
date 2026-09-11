use super::*;

pub(super) fn secret(seed: u8) -> iroh::SecretKey {
    iroh::SecretKey::from_bytes(&[seed; 32])
}

pub(super) fn map_with(epoch: u64, seeds: &[u8]) -> CandidatePlacementMap {
    CandidatePlacementMap {
        epoch,
        nodes: seeds
            .iter()
            .map(|seed| CandidateMapNode {
                node_id: node(*seed),
                kind: RealmNodeKind::Server,
                location: "eu".to_string(),
                weight: 100,
                full: false,
                draining: false,
                labels: BTreeMap::new(),
            })
            .collect(),
        selectors: vec![crate::structs::FrozenStrategySelector {
            strategy_id: transition_strategy().strategy_id,
            replica_count: Some(1),
            distinct_locations: false,
            affinity: Vec::new(),
        }],
        shard_overrides: Vec::new(),
    }
}

pub(super) fn transition_strategy() -> PlacementStrategy {
    PlacementStrategy {
        strategy_id: Ulid::from_bytes([21; 16]),
        name: "moved".to_string(),
        replica_count: Some(1),
        distinct_locations: false,
        affinity: Vec::new(),
        shard_count: 2,
    }
}

pub(super) fn transition_plan(old: &[u8], target: &[u8]) -> TransitionPlan {
    let bucket = |bucket: u32| BucketPlan {
        bucket,
        old_holders: old.iter().map(|seed| node(*seed)).collect(),
        target_holders: target.iter().map(|seed| node(*seed)).collect(),
        predecessor_epoch: 1,
    };
    TransitionPlan {
        transition_id: Ulid::from_bytes([31; 16]),
        strategy_id: transition_strategy().strategy_id,
        buckets: vec![bucket(0), bucket(1)],
        target_map_epoch: 2,
        limits: TransitionLimits::default(),
        created_by: node(1),
        created_at_ms: 5,
    }
}

/// The digest of the fixture's reduced barrier set (holders 1 and 2).
pub(super) fn fixture_digest(plan: &TransitionPlan, bucket: u32) -> [u8; 32] {
    let mut transition = crate::structs::PlacementTransition::new(plan.clone());
    transition.barriers = [1u8, 2]
        .iter()
        .map(|seed| crate::structs::BucketBarrier {
            bucket,
            reported_by: node(*seed),
            frontier: vec![*seed],
        })
        .collect();
    transition.barrier_digest(bucket)
}

pub(super) fn proof_for(plan: &TransitionPlan, bucket: u32, seed: u8) -> CompletionProof {
    ProofClaim {
        realm_id: realm_id(),
        transition_id: plan.transition_id,
        strategy_id: plan.strategy_id,
        bucket,
        old_activation_epoch: 1,
        target_map_epoch: plan.target_map_epoch,
        barrier_digest: fixture_digest(plan, bucket),
        checkpoint_root: [7; 32],
        holder: node(seed),
    }
    .sign(&secret(seed))
}

/// Publish two maps, activate epoch 1, and start a 1 -> 2 transition.
pub(super) fn transition_events(plan: &TransitionPlan) -> Vec<AdminDocumentEvent> {
    vec![
        realm_config_event(
            40,
            node(1),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigPlacementStrategyUpserted {
                strategy: transition_strategy(),
            },
        ),
        realm_config_event(
            41,
            node(1),
            2,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigCandidateMapPublished {
                map: map_with(1, &[1, 2]),
            },
        ),
        realm_config_event(
            42,
            node(1),
            3,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigCandidateMapPublished {
                map: map_with(2, &[3, 4]),
            },
        ),
        realm_config_event(
            43,
            node(1),
            4,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigActivationsInitialized {
                strategy_id: plan.strategy_id,
                candidate_map_epoch: 1,
            },
        ),
        realm_config_event(
            44,
            node(1),
            5,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigTransitionStarted { plan: plan.clone() },
        ),
    ]
}

/// Every barrier and proof bucket 0 needs to cut over.
pub(super) fn completion_events(plan: &TransitionPlan) -> Vec<AdminDocumentEvent> {
    let barrier = |seed: u8, event_seed: u8| {
        realm_config_event(
            event_seed,
            node(seed),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigTransitionBarrierReported {
                transition_id: plan.transition_id,
                bucket: 0,
                reported_by: node(seed),
                frontier: vec![seed],
            },
        )
    };
    let proof = |seed: u8, event_seed: u8| {
        realm_config_event(
            event_seed,
            node(seed),
            2,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigTransitionProofSubmitted {
                transition_id: plan.transition_id,
                strategy_id: plan.strategy_id,
                proof: proof_for(plan, 0, seed),
            },
        )
    };
    vec![barrier(1, 50), barrier(2, 51), proof(3, 52), proof(4, 53)]
}

pub(super) fn transition_config(state: &AdminDocumentReducerState) -> RealmConfigDocument {
    let mut config = RealmConfigDocument::new(realm_id(), Vec::new(), 3);
    overlay_realm_config_placement_reducer_materialization(&mut config, state, 0);
    config
}

#[test]
fn foreign_reports_dropped() {
    // A barrier from a non-old-holder and a stall from an outsider reduce
    // as values but never materialize; oversized reports fail at apply.
    let plan = transition_plan(&[1, 2], &[3, 4]);
    let mut state = realm_config_state();
    for event in transition_events(&plan) {
        state.apply(&event).unwrap();
    }
    state
        .apply(&realm_config_event(
            80,
            node(3),
            5,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigTransitionBarrierReported {
                transition_id: plan.transition_id,
                bucket: 0,
                reported_by: node(3),
                frontier: vec![3],
            },
        ))
        .unwrap();
    state
        .apply(&realm_config_event(
            81,
            node(5),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigTransitionStallReported {
                transition_id: plan.transition_id,
                bucket: 0,
                reported_by: node(5),
                reason: "spoofed".to_string(),
            },
        ))
        .unwrap();

    let transitions = state.materialized_transitions();
    let transition = transitions
        .iter()
        .find(|transition| transition.plan.transition_id == plan.transition_id)
        .expect("transition materializes");
    assert!(
        transition
            .barriers
            .iter()
            .all(|barrier| barrier.reported_by != node(3))
    );
    assert!(transition.stalls.is_empty());

    let oversized = realm_config_event(
        82,
        node(1),
        9,
        AdminDocumentClock::default(),
        AdminDocumentOperation::RealmConfigTransitionBarrierReported {
            transition_id: plan.transition_id,
            bucket: 0,
            reported_by: node(1),
            frontier: vec![0; crate::structs::MAX_BARRIER_FRONTIER_BYTES + 1],
        },
    );
    assert!(matches!(
        state.apply(&oversized),
        Err(AdminDocumentReducerError::TransitionReportOversized)
    ));
}

#[test]
fn concurrent_plans_gated() {
    // Two complete plans derived from one activation base: only the
    // ULID-first one advances the bucket, in either delivery order, and
    // the other can never replay as its successor.
    let plan_a = transition_plan(&[1, 2], &[3, 4]);
    let mut plan_b = transition_plan(&[1, 2], &[3, 4]);
    plan_b.transition_id = Ulid::from_bytes([32; 16]);
    plan_b.target_map_epoch = 3;

    let start_b = realm_config_event(
        45,
        node(1),
        6,
        AdminDocumentClock::default(),
        AdminDocumentOperation::RealmConfigTransitionStarted {
            plan: plan_b.clone(),
        },
    );
    let completion_b: Vec<AdminDocumentEvent> = (0..4)
        .map(|index| {
            let seed = (index + 1) as u8;
            if index < 2 {
                realm_config_event(
                    70 + index as u8,
                    node(seed),
                    3,
                    AdminDocumentClock::default(),
                    AdminDocumentOperation::RealmConfigTransitionBarrierReported {
                        transition_id: plan_b.transition_id,
                        bucket: 0,
                        reported_by: node(seed),
                        frontier: vec![seed],
                    },
                )
            } else {
                realm_config_event(
                    70 + index as u8,
                    node(seed),
                    4,
                    AdminDocumentClock::default(),
                    AdminDocumentOperation::RealmConfigTransitionProofSubmitted {
                        transition_id: plan_b.transition_id,
                        strategy_id: plan_b.strategy_id,
                        proof: proof_for(&plan_b, 0, seed),
                    },
                )
            }
        })
        .collect();

    let mut forward: Vec<AdminDocumentEvent> = transition_events(&plan_a);
    forward.extend(completion_events(&plan_a));
    forward.push(start_b.clone());
    forward.extend(completion_b.clone());

    let mut reversed: Vec<AdminDocumentEvent> = transition_events(&plan_a);
    reversed.push(start_b);
    reversed.extend(completion_b);
    reversed.extend(completion_events(&plan_a));

    for events in [forward, reversed] {
        let mut state = realm_config_state();
        for event in events {
            state.apply(&event).unwrap();
        }
        let config = transition_config(&state);
        let activation = config
            .activation(&plan_a.strategy_id, 0)
            .expect("activation");
        assert_eq!(activation.activation_epoch, 2);
        assert_eq!(activation.candidate_map_epoch, plan_a.target_map_epoch);
    }
}

#[test]
fn map_conflict_fails_closed() {
    // Two divergent maps at one epoch keep the epoch unusable, both retained.
    let mut state = realm_config_state();
    for (event_seed, origin, seeds) in [(60u8, node(1), &[1u8, 2][..]), (61, node(2), &[3][..])] {
        state
            .apply(&realm_config_event(
                event_seed,
                origin,
                1,
                AdminDocumentClock::default(),
                AdminDocumentOperation::RealmConfigCandidateMapPublished {
                    map: map_with(1, seeds),
                },
            ))
            .unwrap();
    }

    let config = transition_config(&state);
    assert_eq!(config.candidate_maps.len(), 2);
    assert!(config.candidate_map(1).is_none());
    assert!(state.materialized_candidate_maps().is_empty());
}

#[test]
fn activation_init_covers_buckets() {
    let plan = transition_plan(&[1, 2], &[3, 4]);
    let mut state = realm_config_state();
    for event in transition_events(&plan).iter().take(4) {
        state.apply(event).unwrap();
    }

    let config = transition_config(&state);
    assert_eq!(config.placement_activations.len(), 2);
    for shard in 0..2 {
        let activation = config
            .activation(&plan.strategy_id, shard)
            .expect("bucket activated");
        assert_eq!(activation.activation_epoch, 1);
        assert_eq!(activation.candidate_map_epoch, 1);
        assert_eq!(activation.transition_id, None);
    }
}

#[test]
fn proof_admission_rejects_forgery() {
    let plan = transition_plan(&[1, 2], &[3, 4]);
    let mut state = realm_config_state();
    for event in transition_events(&plan) {
        state.apply(&event).unwrap();
    }
    let submit = |proof: CompletionProof, origin: NodeId, event_seed: u8| {
        realm_config_event(
            event_seed,
            origin,
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigTransitionProofSubmitted {
                transition_id: plan.transition_id,
                strategy_id: plan.strategy_id,
                proof,
            },
        )
    };

    // A proof relayed by anyone but its holder never enters the record.
    assert_eq!(
        state.apply(&submit(proof_for(&plan, 0, 3), node(1), 70)),
        Err(AdminDocumentReducerError::TransitionOriginMismatch)
    );
    // A tampered epoch invalidates the signature over the claim.
    let mut retargeted = proof_for(&plan, 0, 3);
    retargeted.target_map_epoch = 9;
    assert_eq!(
        state.apply(&submit(retargeted, node(3), 71)),
        Err(AdminDocumentReducerError::InvalidTransitionProof)
    );
    // So does a signature made by another node key.
    let mut forged = proof_for(&plan, 0, 4);
    forged.holder = node(3);
    assert_eq!(
        state.apply(&submit(forged, node(3), 72)),
        Err(AdminDocumentReducerError::InvalidTransitionProof)
    );

    let config = transition_config(&state);
    assert!(config.placement_transitions[0].proofs.is_empty());
}

#[test]
fn duplicate_proof_is_idempotent() {
    let plan = transition_plan(&[1, 2], &[3, 4]);
    let mut state = realm_config_state();
    for event in transition_events(&plan) {
        state.apply(&event).unwrap();
    }
    let first = realm_config_event(
        73,
        node(3),
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::RealmConfigTransitionProofSubmitted {
            transition_id: plan.transition_id,
            strategy_id: plan.strategy_id,
            proof: proof_for(&plan, 0, 3),
        },
    );
    let mut resent = first.clone();
    resent.event_id = Ulid::from_bytes([74; 16]);
    resent.origin_seq = 2;

    state.apply(&first).unwrap();
    assert_eq!(state.apply(&first), Ok(AdminDocumentApplyStatus::Duplicate));
    state.apply(&resent).unwrap();

    let config = transition_config(&state);
    assert_eq!(config.placement_transitions[0].proofs.len(), 1);
}

#[test]
fn activation_advances_on_completion() {
    let plan = transition_plan(&[1, 2], &[3, 4]);
    let mut state = realm_config_state();
    for event in transition_events(&plan)
        .into_iter()
        .chain(completion_events(&plan))
    {
        state.apply(&event).unwrap();
    }

    let config = transition_config(&state);
    let cut = config.activation(&plan.strategy_id, 0).expect("bucket 0");
    assert_eq!(cut.candidate_map_epoch, 2);
    assert_eq!(cut.activation_epoch, 2);
    assert_eq!(cut.transition_id, None);

    // Bucket 1 has no barrier or proof, so it stays where it was and keeps
    // naming the transition still working on it.
    let pending = config.activation(&plan.strategy_id, 1).expect("bucket 1");
    assert_eq!(pending.candidate_map_epoch, 1);
    assert_eq!(pending.activation_epoch, 1);
    assert_eq!(pending.transition_id, Some(plan.transition_id));

    let transition = &config.placement_transitions[0];
    assert_eq!(transition.completed.len(), 1);
    assert_eq!(transition.completed[0].bucket, 0);
    assert_eq!(
        transition.completed[0].completed_at_ms,
        Ulid::from_bytes([53; 16]).timestamp_ms()
    );
    assert!(!transition.is_terminal());
}

#[test]
fn advance_ignores_event_order() {
    // Every replica reduces the same set into the same activations, whatever
    // order the events arrive in.
    let plan = transition_plan(&[1, 2], &[3, 4]);
    let events: Vec<AdminDocumentEvent> = transition_events(&plan)
        .into_iter()
        .chain(completion_events(&plan))
        .collect();
    let mut forward = realm_config_state();
    for event in &events {
        forward.apply(event).unwrap();
    }
    let mut permuted = realm_config_state();
    for index in [8, 3, 6, 1, 7, 0, 5, 4, 2] {
        permuted.apply(&events[index]).unwrap();
    }

    let expected = transition_config(&forward);
    let actual = transition_config(&permuted);
    assert_eq!(expected.placement_activations, actual.placement_activations);
    assert_eq!(expected.placement_transitions, actual.placement_transitions);
    assert_eq!(expected.candidate_maps, actual.candidate_maps);
}

#[test]
fn abort_keeps_cut_buckets() {
    let plan = transition_plan(&[1, 2], &[3, 4]);
    let mut state = realm_config_state();
    for event in transition_events(&plan)
        .into_iter()
        .chain(completion_events(&plan))
    {
        state.apply(&event).unwrap();
    }
    state
        .apply(&realm_config_event(
            80,
            node(1),
            6,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigTransitionAborted {
                transition_id: plan.transition_id,
            },
        ))
        .unwrap();

    let config = transition_config(&state);
    let transition = &config.placement_transitions[0];
    assert!(matches!(transition.status, TransitionStatus::Aborted));
    assert!(transition.is_terminal());
    // The cut bucket stays cut; the un-cut one keeps its old activation.
    assert_eq!(
        config
            .activation(&plan.strategy_id, 0)
            .unwrap()
            .candidate_map_epoch,
        2
    );
    assert_eq!(
        config
            .activation(&plan.strategy_id, 1)
            .unwrap()
            .candidate_map_epoch,
        1
    );
    assert_eq!(
        config
            .activation(&plan.strategy_id, 1)
            .unwrap()
            .transition_id,
        None
    );
}
