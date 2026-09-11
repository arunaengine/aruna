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
