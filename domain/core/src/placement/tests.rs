//! Tests bucket origins, holder overrides, turnovers, activation pins and read unions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use crate::reducer::AdminDocumentState;
use crate::reducer::placement::overlay_placement;
use crate::structs::identity::auth::Actor;
use crate::structs::identity::realm::{RealmId, RealmNodeKind};
use crate::structs::placement::record::{
    AffinityRule, BindingScope, NodePlacementEntry, StrategyBinding,
};
use crate::structs::placement::transition::{
    CandidateMapNode, CandidatePlacementMap, PlacementActivation,
};
use crate::structs::storage::metadata_registry::MetadataRegistryRecord;
use ulid::Ulid;

use crate::placement::transition::holders_in_map;

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn config_and_placement() -> (RealmConfigDocument, PlacementRef) {
    let mut config = RealmConfigDocument::new(RealmId::from_bytes([3u8; 32]), Vec::new(), 3);
    let strategy = PlacementStrategy {
        strategy_id: Ulid::from_bytes([5u8; 16]),
        name: "default".to_string(),
        replica_count: Some(2),
        distinct_locations: false,
        affinity: Vec::new(),
        shard_count: 64,
    };
    config.default_strategy_id = Some(strategy.strategy_id);
    config.strategies = vec![strategy.clone()];
    for seed in 1..=4u8 {
        config.ensure_node(node(seed), RealmNodeKind::Server);
    }
    (
        config,
        PlacementRef {
            strategy_id: strategy.strategy_id,
            shard: 7,
        },
    )
}

fn strategy_of(config: &RealmConfigDocument) -> &PlacementStrategy {
    config
        .strategy(&config.default_strategy_id.expect("default strategy"))
        .expect("default strategy resolves")
}

fn subject(seed: u64) -> [u8; 32] {
    *blake3::hash(&seed.to_le_bytes()).as_bytes()
}

#[test]
fn registry_uses_class() {
    let (mut config, _) = config_and_placement();
    let group_id = Ulid::from_bytes([6u8; 16]);
    let document_id = Ulid::from_bytes([7u8; 16]);
    let general_strategy_id = config.default_strategy_id.unwrap();
    let class_strategy = PlacementStrategy {
        strategy_id: Ulid::from_bytes([8u8; 16]),
        name: "registry".to_string(),
        replica_count: None,
        distinct_locations: false,
        affinity: Vec::new(),
        shard_count: 16,
    };
    config.strategies.push(class_strategy.clone());
    config.strategy_bindings = vec![
        StrategyBinding {
            scope: BindingScope::Group(group_id),
            strategy_id: general_strategy_id,
        },
        StrategyBinding {
            scope: BindingScope::Class(DocumentClass::MetadataRegistry),
            strategy_id: class_strategy.strategy_id,
        },
    ];
    config.placement_overrides = vec![PlacementOverride {
        subject: document_id.to_bytes().to_vec(),
        pinned: Vec::new(),
        excluded: Vec::new(),
        strategy_id: Some(general_strategy_id),
    }];
    let record = MetadataRegistryRecord {
        realm_id: RealmId::from_bytes([3u8; 32]),
        group_id,
        document_id,
        document_path: "datasets/example".to_string(),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        public: false,
        permission_path: String::new(),
        placement: PlacementRef::NIL,
        holder_node_ids: Vec::new(),
        created_at_ms: 0,
        updated_at_ms: 0,
        establishing_event_id: Ulid::from_bytes([9u8; 16]),
        last_event_id: Ulid::from_bytes([9u8; 16]),
    };

    let placement = registry_placement(&config, &record);
    assert_eq!(placement.strategy_id, class_strategy.strategy_id);
    assert_eq!(
        placement.shard,
        shard_for_subject(&document_id.to_bytes(), class_strategy.shard_count)
    );

    config.strategy_bindings[1].strategy_id = Ulid::from_bytes([99u8; 16]);
    assert_eq!(registry_placement(&config, &record), PlacementRef::NIL);

    config.strategy_bindings.truncate(1);
    let fallback = registry_placement(&config, &record);
    assert_eq!(fallback.strategy_id, config.default_strategy_id.unwrap());

    config.default_strategy_id = Some(Ulid::from_bytes([99u8; 16]));
    assert_eq!(registry_placement(&config, &record), PlacementRef::NIL);

    config.default_strategy_id = None;
    assert_eq!(
        registry_placement(&config, &record).strategy_id,
        config.strategies[0].strategy_id
    );

    config.strategies.clear();
    assert_eq!(registry_placement(&config, &record), PlacementRef::NIL);
}

#[test]
fn origin_bucket_deterministic() {
    let (config, _) = config_and_placement();
    let strategy = strategy_of(&config);
    let first = choose_origin_bucket(&config, strategy, node(1), &subject(1));
    let second = choose_origin_bucket(&config, strategy, node(1), &subject(1));
    assert_eq!(first, second);
    assert_eq!(
        first.expect("origin holds buckets").strategy_id,
        strategy.strategy_id
    );
}

#[test]
fn origin_holds_bucket() {
    let (config, _) = config_and_placement();
    let strategy = strategy_of(&config);
    // Replica 2 of 4 nodes: no node holds every bucket, so a blind hash
    // would land outside the origin's held set for some subjects.
    let held = held_buckets(&config, strategy, node(1));
    assert!(!held.is_empty() && held.len() < strategy.shard_count as usize);
    for seed in 0..256u64 {
        let placement = choose_origin_bucket(&config, strategy, node(1), &subject(seed))
            .expect("origin holds buckets");
        assert!(held.contains(&placement.shard));
        assert!(resolve_shard_holders(&config, &placement).contains(&node(1)));
    }
}

#[test]
fn origin_buckets_spread() {
    // Creates spread across the buckets the origin leads, never landing
    // in one it merely holds while another node leads it.
    let (config, _) = config_and_placement();
    let strategy = strategy_of(&config);
    let led: Vec<u32> = held_buckets(&config, strategy, node(1))
        .into_iter()
        .filter(|shard| {
            let placement = PlacementRef {
                strategy_id: strategy.strategy_id,
                shard: *shard,
            };
            resolve_shard_holders(&config, &placement).first() == Some(&node(1))
        })
        .collect();
    assert!(!led.is_empty(), "fixture node leads no bucket");
    let chosen: std::collections::HashSet<u32> = (0..1_000u64)
        .filter_map(|seed| choose_origin_bucket(&config, strategy, node(1), &subject(seed)))
        .map(|placement| placement.shard)
        .collect();
    assert!(chosen.iter().all(|shard| led.contains(shard)));
    assert!(
        chosen.len() * 2 > led.len(),
        "chosen {} of {} led buckets",
        chosen.len(),
        led.len()
    );
}

#[test]
fn unknown_origin_empty() {
    let (config, _) = config_and_placement();
    let strategy = strategy_of(&config);
    assert!(held_buckets(&config, strategy, node(9)).is_empty());
    assert_eq!(
        choose_origin_bucket(&config, strategy, node(9), &subject(1)),
        None
    );
}

#[test]
fn empty_holders_rejected() {
    let (mut config, placement) = config_and_placement();
    assert!(holds_placement(&config, &placement, node(1)));

    config.nodes.clear();
    assert!(resolve_shard_holders(&config, &placement).is_empty());
    assert!(!holds_placement(&config, &placement, node(1)));

    let dangling = PlacementRef {
        strategy_id: Ulid::from_bytes([99u8; 16]),
        ..placement
    };
    assert!(resolve_shard_holders(&config, &dangling).is_empty());
    assert!(!holds_placement(&config, &dangling, node(1)));
    assert!(holds_placement(&config, &PlacementRef::NIL, node(1)));
}

#[test]
fn user_origin_empty() {
    let (mut config, _) = config_and_placement();
    let owner = crate::user::id::UserId::nil(config.realm_id);
    config.ensure_node(node(5), RealmNodeKind::User { owner });
    let strategy = strategy_of(&config);
    assert!(held_buckets(&config, strategy, node(5)).is_empty());
    assert_eq!(
        choose_origin_bucket(&config, strategy, node(5), &subject(1)),
        None
    );
}

#[test]
fn shard_override_filters() {
    let (mut config, placement) = config_and_placement();
    let baseline = resolve_shard_holders(&config, &placement);
    assert_eq!(baseline.len(), 2);

    let pinned = *baseline.last().unwrap();
    let excluded = baseline[0];
    config.placement_overrides = vec![PlacementOverride {
        subject: shard_subject_bytes(&placement),
        pinned: vec![pinned],
        excluded: vec![excluded],
        strategy_id: None,
    }];

    let overridden = resolve_shard_holders(&config, &placement);
    assert_eq!(overridden[0], pinned);
    assert!(!overridden.contains(&excluded));
}

#[test]
fn document_override_ignored() {
    let (mut config, placement) = config_and_placement();
    let baseline = resolve_shard_holders(&config, &placement);

    // Same pin/exclude, but keyed by a document subject: holder resolution
    // ignores it (strategy selection is its only remaining effect).
    config.placement_overrides = vec![PlacementOverride {
        subject: Ulid::from_bytes([9u8; 16]).to_bytes().to_vec(),
        pinned: vec![*baseline.last().unwrap()],
        excluded: vec![baseline[0]],
        strategy_id: None,
    }];

    assert_eq!(resolve_shard_holders(&config, &placement), baseline);
}

#[test]
fn activation_pins_holders() {
    // Once a map is activated, edits to the live placement map are inert:
    // only a completed transition may move the bucket.
    let (mut config, placement) = config_and_placement();
    config.snapshot_candidate_map();
    let pinned = resolve_shard_holders(&config, &placement);
    assert_eq!(pinned.len(), 2);

    for seed in 5..=8u8 {
        config.ensure_node(node(seed), RealmNodeKind::Server);
    }
    config.placement_map.push(NodePlacementEntry {
        node_id: pinned[0],
        location: "moved".to_string(),
        weight: 1,
        full: true,
        draining: false,
        labels: std::collections::BTreeMap::new(),
    });
    assert_eq!(resolve_shard_holders(&config, &placement), pinned);

    // Publishing a newer map is not activation either.
    config.snapshot_candidate_map();
    assert_eq!(resolve_shard_holders(&config, &placement), pinned);
    assert_ne!(
        resolve_holders(
            &view_from_map(config.candidate_map(2).expect("newest map")),
            strategy_of(&config),
            &shard_subject_bytes(&placement),
            None,
        ),
        pinned
    );
}

#[test]
fn edits_stay_inert() {
    // Selection uses the activated map, so later selector and shard edits cannot move holders.
    let (mut config, placement) = config_and_placement();
    config.snapshot_candidate_map();
    let pinned = resolve_shard_holders(&config, &placement);
    assert_eq!(pinned.len(), 2);

    let strategy_id = placement.strategy_id;
    let strategy = config
        .strategies
        .iter_mut()
        .find(|strategy| strategy.strategy_id == strategy_id)
        .expect("strategy");
    strategy.replica_count = Some(4);
    strategy.distinct_locations = true;
    strategy.affinity.push(AffinityRule {
        matcher: crate::structs::placement::record::LabelMatch {
            key: "zone".to_string(),
            value: "a".to_string(),
        },
        effect: crate::structs::placement::record::AffinityEffect::Filter,
    });
    config.placement_overrides.push(PlacementOverride {
        subject: shard_subject_bytes(&placement),
        pinned: vec![node(4)],
        excluded: vec![pinned[0]],
        strategy_id: None,
    });
    assert_eq!(resolve_shard_holders(&config, &placement), pinned);

    // A fresh snapshot freezes the edits, but only activation applies it.
    config.snapshot_candidate_map();
    assert_eq!(resolve_shard_holders(&config, &placement), pinned);
    let newest = config.candidate_map(2).expect("newest map");
    assert_ne!(
        holders_in_map(&config, strategy_of(&config), &placement, newest).expect("frozen selector"),
        pinned
    );
}

fn membership_fixture() -> (RealmConfigDocument, Ulid) {
    // Bucket 7 completed at t=100 (grace 1000, departing holder node 9),
    // buckets 8 and 9 incomplete with a limit-one window.
    let (mut config, placement) = config_and_placement();
    config.snapshot_candidate_map();
    let strategy_id = placement.strategy_id;
    let bucket = |bucket: u32, old: Vec<NodeId>, target: Vec<NodeId>| {
        crate::structs::placement::transition::BucketPlan {
            bucket,
            old_holders: old,
            target_holders: target,
            predecessor_epoch: 1,
        }
    };
    let mut transition = crate::structs::placement::transition::PlacementTransition::new(
        crate::structs::placement::transition::TransitionPlan {
            transition_id: Ulid::from_bytes([8; 16]),
            strategy_id,
            buckets: vec![
                bucket(7, vec![node(9)], vec![node(2)]),
                bucket(8, Vec::new(), vec![node(8)]),
                bucket(9, Vec::new(), vec![node(7)]),
            ],
            target_map_epoch: 2,
            limits: crate::structs::placement::transition::TransitionLimits {
                max_incomplete_buckets: 1,
                grace_ms: 1_000,
            },
            created_by: node(1),
            created_at_ms: 1,
        },
    );
    transition
        .completed
        .push(crate::structs::placement::transition::BucketCompletion {
            bucket: 7,
            completed_at_ms: 100,
        });
    transition
        .drained
        .push(crate::structs::placement::transition::BucketDrain {
            bucket: 7,
            reported_by: node(9),
        });
    config.placement_transitions.push(transition);
    (config, strategy_id)
}

#[test]
fn membership_per_bucket() {
    // Each bucket releases independently; abort drops incomplete targets immediately.
    let (config, strategy_id) = membership_fixture();
    let at = |shard: u32, now_ms: u64| {
        bucket_membership(&config, &PlacementRef { strategy_id, shard }, now_ms)
    };

    let retained = at(7, 500);
    assert!(retained.members.contains(&node(9)));
    assert!(retained.publishers.contains(&node(9)));
    let released = at(7, 1_100);
    assert!(!released.members.contains(&node(9)));
    assert!(!released.publishers.contains(&node(9)));

    let admitted = at(8, 500);
    assert!(admitted.members.contains(&node(8)));
    assert!(!admitted.publishers.contains(&node(8)));

    let mut aborted_config = config.clone();
    aborted_config.placement_transitions[0].status =
        crate::structs::placement::transition::TransitionStatus::Aborted;
    let aborted = bucket_membership(
        &aborted_config,
        &PlacementRef {
            strategy_id,
            shard: 8,
        },
        500,
    );
    assert!(!aborted.members.contains(&node(8)));
    // The completed bucket's retention survives the abort.
    let still_retained = bucket_membership(
        &aborted_config,
        &PlacementRef {
            strategy_id,
            shard: 7,
        },
        500,
    );
    assert!(still_retained.publishers.contains(&node(9)));
}

/// A full turnover of bucket 7 from `old` to `target`: barriers from `frozen`, proofs from every
/// target, and a forced cutover when an old holder never froze.
fn full_turnover(
    strategy_id: Ulid,
    seed: u8,
    (old, target, frozen): (&[u8], &[u8], &[u8]),
) -> crate::structs::placement::transition::PlacementTransition {
    use crate::structs::placement::transition::{
        BucketBarrier, BucketForceFinalize, BucketPlan, PlacementTransition, ProofClaim,
        TransitionLimits, TransitionPlan,
    };
    let nodes = |seeds: &[u8]| seeds.iter().map(|seed| node(*seed)).collect::<Vec<_>>();
    let mut transition = PlacementTransition::new(TransitionPlan {
        transition_id: Ulid::from_bytes([seed; 16]),
        strategy_id,
        buckets: vec![BucketPlan {
            bucket: 7,
            old_holders: nodes(old),
            target_holders: nodes(target),
            predecessor_epoch: u64::from(seed),
        }],
        target_map_epoch: u64::from(seed) + 1,
        limits: TransitionLimits {
            max_incomplete_buckets: 1,
            grace_ms: 1_000,
        },
        created_by: node(1),
        created_at_ms: 1,
    });
    for holder in frozen {
        transition.barriers.push(BucketBarrier {
            bucket: 7,
            reported_by: node(*holder),
            frontier: vec![*holder],
        });
    }
    for holder in target {
        let digest = transition.barrier_digest(7);
        let claim = ProofClaim {
            realm_id: RealmId::from_bytes([3u8; 32]),
            transition_id: transition.plan.transition_id,
            strategy_id,
            bucket: 7,
            old_activation_epoch: u64::from(seed),
            target_map_epoch: u64::from(seed) + 1,
            barrier_digest: digest,
            checkpoint_root: [seed; 32],
            holder: node(*holder),
        };
        let secret = iroh::SecretKey::from_bytes(&[*holder; 32]);
        transition.proofs.push(claim.sign(&secret));
    }
    if !transition.bucket_ready(7) {
        transition.forced.push(BucketForceFinalize {
            bucket: 7,
            at_risk_report: "old holder unavailable".to_string(),
        });
    }
    transition
}

#[test]
fn repeated_turnover_bounded() {
    // Two full turnovers with grace, while old holder 9 never answers: the cutover needs
    // the operator's force, holder 9 stays retained without its drain report, and the next
    // turnover adds only its own departing holders until they drain.
    use crate::structs::placement::transition::{
        BucketCompletion, BucketDrain, FrozenStrategySelector, PlacementTransition,
    };
    let (mut config, placement) = config_and_placement();
    let strategy_id = placement.strategy_id;
    // Map 1 places bucket 7 on 9 and 10, map 2 on 12 and 13, map 3 on 14 and 15.
    let map = |epoch: u64, seeds: [u8; 2]| CandidatePlacementMap {
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
                labels: std::collections::BTreeMap::new(),
            })
            .collect(),
        selectors: vec![FrozenStrategySelector {
            strategy_id,
            replica_count: Some(2),
            distinct_locations: false,
            affinity: Vec::new(),
        }],
        shard_overrides: Vec::new(),
    };
    config.candidate_maps = vec![map(1, [9, 10]), map(2, [12, 13]), map(3, [14, 15])];
    config.placement_activations = vec![PlacementActivation {
        strategy_id,
        shard: 7,
        activation_epoch: 1,
        candidate_map_epoch: 1,
        transition_id: None,
    }];
    // The cutover rule of the reducer: a ready bucket planned on the current epoch advances.
    let activate = |config: &mut RealmConfigDocument, transition: &PlacementTransition| {
        let plan = transition.plan.bucket_plan(7).expect("bucket 7 planned");
        let activation = &mut config.placement_activations[0];
        assert!(transition.bucket_ready(7));
        assert_eq!(plan.predecessor_epoch, activation.activation_epoch);
        activation.candidate_map_epoch = transition.plan.target_map_epoch;
        activation.activation_epoch += 1;
    };
    let sorted = |seeds: &[u8]| {
        let mut nodes: Vec<NodeId> = seeds.iter().map(|seed| node(*seed)).collect();
        nodes.sort_by_key(|node_id| *node_id.as_bytes());
        nodes
    };
    let sets = |config: &RealmConfigDocument, now_ms: u64| {
        let membership = bucket_membership(config, &placement, now_ms);
        let mut members = membership.members;
        let mut publishers = membership.publishers;
        members.sort_by_key(|node_id| *node_id.as_bytes());
        publishers.sort_by_key(|node_id| *node_id.as_bytes());
        (members, publishers)
    };
    let both = |seeds: &[u8]| (sorted(seeds), sorted(seeds));
    let active = |config: &RealmConfigDocument| {
        let mut holders = resolve_shard_holders(config, &placement);
        holders.sort_by_key(|node_id| *node_id.as_bytes());
        holders
    };
    assert_eq!(active(&config), sorted(&[9, 10]));

    let mut first = full_turnover(strategy_id, 1, (&[9, 10], &[12, 13], &[10]));
    assert_eq!(
        first.forced.len(),
        1,
        "a missing barrier needs a forced cutover"
    );
    first.completed.push(BucketCompletion {
        bucket: 7,
        completed_at_ms: 100,
    });
    first.drained.push(BucketDrain {
        bucket: 7,
        reported_by: node(10),
    });
    activate(&mut config, &first);
    config.placement_transitions.push(first);
    assert_eq!(
        active(&config),
        sorted(&[12, 13]),
        "the replacements are active"
    );
    assert_eq!(sets(&config, 500), both(&[9, 10, 12, 13]));
    assert_eq!(sets(&config, 1_100), both(&[9, 12, 13]));
    assert!(config.placement_transitions[0].is_terminal());
    assert!(!config.placement_transitions[0].released(u64::MAX));

    let mut second = full_turnover(strategy_id, 2, (&[12, 13], &[14, 15], &[12, 13]));
    assert!(second.forced.is_empty(), "every old holder froze");
    second.completed.push(BucketCompletion {
        bucket: 7,
        completed_at_ms: 2_000,
    });
    for holder in [12, 13] {
        second.drained.push(BucketDrain {
            bucket: 7,
            reported_by: node(holder),
        });
    }
    activate(&mut config, &second);
    config.placement_transitions.push(second);
    assert_eq!(
        active(&config),
        sorted(&[14, 15]),
        "the replacements are active"
    );
    assert_eq!(sets(&config, 2_500), both(&[9, 12, 13, 14, 15]));
    assert_eq!(sets(&config, 3_100), both(&[9, 14, 15]));
    assert!(config.placement_transitions[1].released(3_100));
    assert!(
        drain_pending(&config, 3_100),
        "holder 9 still owes its report"
    );
}

#[test]
fn release_deadline_derived() {
    // Before the grace end the deadline is completion plus grace; past it
    // a missing drain report demands a retry rather than a deadline.
    let (config, _) = membership_fixture();
    let mut undrained = config.clone();
    undrained.placement_transitions[0].drained.clear();

    assert_eq!(next_release_ms(&config, 500), Some(1_100));
    assert!(!drain_pending(&config, 500));
    // Bucket 7's holder drained, so past grace nothing pends.
    assert_eq!(next_release_ms(&config, 1_100), None);
    assert!(!drain_pending(&config, 1_100));
    // Without the drain report the deadline passes into a pending retry.
    assert_eq!(next_release_ms(&undrained, 1_100), None);
    assert!(drain_pending(&undrained, 1_100));
}

#[test]
fn window_bounds_joins() {
    // Limit-one: only the first incomplete bucket's target is admitted.
    let (config, strategy_id) = membership_fixture();
    let outside = bucket_membership(
        &config,
        &PlacementRef {
            strategy_id,
            shard: 9,
        },
        500,
    );
    assert!(!outside.members.contains(&node(7)));
}

#[test]
fn policy_bucket_agrees() {
    // The adapter derives a policy bucket from the config alone, so that
    // derivation and this resolver must never disagree.
    let (mut config, _) = config_and_placement();
    config.strategy_bindings = vec![StrategyBinding {
        scope: BindingScope::Class(DocumentClass::PlacementPolicy),
        strategy_id: config.default_strategy_id.expect("default strategy"),
    }];
    let policy_id = Ulid::from_bytes([8u8; 16]);
    let target = crate::structs::placement::policy::document::placement_policy_target(policy_id);
    let planned = plan_target_placement(&config, &target, Default::default())
        .expect("resolves")
        .expect("governed");
    assert_eq!(config.policy_placement(policy_id), Some(planned.placement));
}

#[test]
fn planner_pins_activation() {
    // A published-but-unactivated newer map must not move the planner:
    // selected peers stay on the activated holder set.
    let (mut config, _) = config_and_placement();
    config.strategy_bindings = vec![StrategyBinding {
        scope: BindingScope::Class(DocumentClass::Group),
        strategy_id: config.default_strategy_id.unwrap(),
    }];
    config.snapshot_candidate_map();
    let target = DocumentTarget::Group {
        group_id: Ulid::from_bytes([6u8; 16]),
    };
    let pinned = plan_target_placement(&config, &target, Default::default())
        .expect("resolves")
        .expect("governed");

    for seed in 5..=8u8 {
        config.ensure_node(node(seed), RealmNodeKind::Server);
    }
    config.snapshot_candidate_map();
    let after = plan_target_placement(&config, &target, Default::default())
        .expect("resolves")
        .expect("governed");
    assert_eq!(after.holders, pinned.holders);
    assert_eq!(after.desired_count, pinned.desired_count);
}

#[test]
fn missing_selector_fails() {
    // A strategy added after the activated map was published must fail
    // closed rather than fall back to live selector fields.
    let (mut config, _placement) = config_and_placement();
    config.snapshot_candidate_map();
    let late = PlacementStrategy {
        strategy_id: Ulid::from_bytes([9u8; 16]),
        name: "late".to_string(),
        replica_count: Some(1),
        distinct_locations: false,
        affinity: Vec::new(),
        shard_count: 4,
    };
    config.strategies.push(late.clone());
    config.placement_activations.push(PlacementActivation {
        strategy_id: late.strategy_id,
        shard: 0,
        activation_epoch: 1,
        candidate_map_epoch: 1,
        transition_id: None,
    });
    assert_eq!(
        try_resolve_holders(
            &config,
            &PlacementRef {
                strategy_id: late.strategy_id,
                shard: 0,
            }
        ),
        Err(PlacementResolveError::CandidateMapUnavailable(1))
    );
}

#[test]
fn missing_activation_fails() {
    let (mut config, placement) = config_and_placement();
    config.snapshot_candidate_map();
    let strategy_id = placement.strategy_id;

    config
        .placement_activations
        .retain(|activation| activation.shard != placement.shard);
    assert_eq!(
        try_resolve_holders(&config, &placement),
        Err(PlacementResolveError::ActivationUnavailable(
            placement.shard
        ))
    );
    assert!(resolve_shard_holders(&config, &placement).is_empty());
    assert!(!holds_placement(&config, &placement, node(1)));

    // Two divergent activations for one bucket are a conflict, not a winner.
    for epoch in [1, 2] {
        config.placement_activations.push(
            crate::structs::placement::transition::PlacementActivation {
                strategy_id,
                shard: placement.shard,
                activation_epoch: 1,
                candidate_map_epoch: epoch,
                transition_id: None,
            },
        );
    }
    assert_eq!(
        try_resolve_holders(&config, &placement),
        Err(PlacementResolveError::ActivationConflicted(placement.shard))
    );

    // An activation naming an unpublished map resolves nothing either.
    config
        .placement_activations
        .retain(|activation| activation.candidate_map_epoch != 2);
    config.candidate_maps.clear();
    config.candidate_maps.push(CandidatePlacementMap {
        epoch: 5,
        nodes: Vec::new(),
        selectors: Vec::new(),
        shard_overrides: Vec::new(),
    });
    assert_eq!(
        try_resolve_holders(&config, &placement),
        Err(PlacementResolveError::CandidateMapUnavailable(1))
    );

    let unknown = PlacementRef {
        strategy_id: Ulid::from_bytes([99u8; 16]),
        shard: 0,
    };
    assert_eq!(
        try_resolve_holders(&config, &unknown),
        Err(PlacementResolveError::StrategyUnknown(unknown.strategy_id))
    );
}

#[test]
fn empty_shard_newest() {
    // The guard validates the map a future activation would pin, not the
    // one already activated.
    let (mut config, _) = config_and_placement();
    for seed in 1..=4u8 {
        config.placement_map.push(NodePlacementEntry {
            node_id: node(seed),
            location: String::new(),
            weight: crate::structs::placement::record::DEFAULT_NODE_WEIGHT,
            full: false,
            draining: false,
            labels: std::collections::BTreeMap::from([("tier".to_string(), "hot".to_string())]),
        });
    }
    config.snapshot_candidate_map();
    for strategy in config.strategies.iter_mut() {
        strategy.affinity = vec![crate::structs::placement::record::AffinityRule {
            matcher: crate::structs::placement::record::LabelMatch {
                key: "tier".to_string(),
                value: "hot".to_string(),
            },
            effect: crate::structs::placement::record::AffinityEffect::Filter,
        }];
    }
    assert_eq!(first_empty_shard(&config), None);

    // The frozen map still carries the matching labels, so nothing is empty
    // until the edit is snapshotted into a new map.
    for entry in config.placement_map.iter_mut() {
        entry.labels.insert("tier".to_string(), "cold".to_string());
    }
    assert_eq!(first_empty_shard(&config), None);
    config.snapshot_candidate_map();
    assert!(first_empty_shard(&config).is_some());
}

#[test]
fn read_union_spans() {
    // Readers must reach both sides of a bucket in flight; writers must not.
    let (mut config, placement) = config_and_placement();
    config.snapshot_candidate_map();
    let activated = resolve_shard_holders(&config, &placement);
    assert_eq!(read_holder_sets(&config, &placement), Ok(activated.clone()));

    let targets: Vec<NodeId> = (5..=6u8).map(node).collect();
    config.placement_transitions.push(
        crate::structs::placement::transition::PlacementTransition::new(
            crate::structs::placement::transition::TransitionPlan {
                transition_id: Ulid::from_bytes([7; 16]),
                strategy_id: placement.strategy_id,
                buckets: vec![crate::structs::placement::transition::BucketPlan {
                    bucket: placement.shard,
                    old_holders: activated.clone(),
                    target_holders: targets.clone(),
                    predecessor_epoch: 1,
                }],
                target_map_epoch: 2,
                limits: Default::default(),
                created_by: node(1),
                created_at_ms: 1,
            },
        ),
    );

    let union = read_holder_sets(&config, &placement).expect("bucket resolves");
    assert_eq!(union[..activated.len()], activated[..]);
    for target in &targets {
        assert!(union.contains(target));
    }
    assert_eq!(union.len(), activated.len() + targets.len());
    // Write authority stays the activation alone.
    assert_eq!(resolve_shard_holders(&config, &placement), activated);
    for target in &targets {
        assert!(!holds_placement(&config, &placement, *target));
    }
}

/// Reduces a full two-bucket handoff from map one onto map two and returns
/// the state that replays it.
fn reduced_handoff(realm_id: RealmId, strategy_id: Ulid) -> AdminDocumentState {
    let mut state = AdminDocumentState::new(AdminDocumentTarget::RealmConfig { realm_id });
    let actor = |seed: u8| Actor {
        node_id: node(seed),
        user_id: crate::user::id::UserId::nil(realm_id),
        realm_id,
    };
    let map = |epoch: u64, seeds: [u8; 2]| CandidatePlacementMap {
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
                labels: std::collections::BTreeMap::new(),
            })
            .collect(),
        selectors: vec![
            crate::structs::placement::transition::FrozenStrategySelector {
                strategy_id,
                replica_count: Some(1),
                distinct_locations: false,
                affinity: Vec::new(),
            },
        ],
        shard_overrides: Vec::new(),
    };
    for operation in [
        AdminDocumentOperation::PlacementStrategyUpserted {
            strategy: PlacementStrategy {
                strategy_id,
                name: "moved".to_string(),
                replica_count: Some(1),
                distinct_locations: false,
                affinity: Vec::new(),
                shard_count: 2,
            },
        },
        AdminDocumentOperation::CandidateMapPublished {
            map: map(1, [1, 2]),
        },
        AdminDocumentOperation::ConfigActivationsInitialized {
            strategy_id,
            candidate_map_epoch: 1,
        },
        AdminDocumentOperation::CandidateMapPublished {
            map: map(2, [3, 4]),
        },
    ] {
        state
            .apply_operation(&actor(1), operation, ulid::Ulid::generate())
            .expect("applies");
    }

    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    overlay_placement(&mut config, &state, 0);
    let plan = crate::placement::transition::plan_transition(
        &config,
        crate::placement::transition::TransitionRequest {
            transition_id: Ulid::from_bytes([7; 16]),
            strategy_id,
            buckets: Vec::new(),
            target_map_epoch: 2,
            limits: Default::default(),
            created_by: node(1),
            created_at_ms: 1,
        },
    )
    .expect("the plan derives");
    state
        .apply_operation(
            &actor(1),
            AdminDocumentOperation::ConfigTransitionStarted { plan: plan.clone() },
            ulid::Ulid::generate(),
        )
        .expect("applies");

    for bucket in &plan.buckets {
        for holder in &bucket.old_holders {
            let seed = (1..=4u8)
                .find(|seed| node(*seed) == *holder)
                .expect("known");
            state
                .apply_operation(
                    &actor(seed),
                    AdminDocumentOperation::TransitionBarrierReported {
                        transition_id: plan.transition_id,
                        bucket: bucket.bucket,
                        reported_by: *holder,
                        frontier: vec![seed],
                    },
                    ulid::Ulid::generate(),
                )
                .expect("applies");
        }
        for holder in &bucket.old_holders {
            if bucket.target_holders.contains(holder) {
                continue;
            }
            let seed = (1..=4u8)
                .find(|seed| node(*seed) == *holder)
                .expect("known");
            state
                .apply_operation(
                    &actor(seed),
                    AdminDocumentOperation::TransitionDrainReported {
                        transition_id: plan.transition_id,
                        bucket: bucket.bucket,
                        reported_by: *holder,
                    },
                    ulid::Ulid::generate(),
                )
                .expect("applies");
        }
        let mut fenced =
            crate::structs::placement::transition::PlacementTransition::new(plan.clone());
        fenced.barriers = bucket
            .old_holders
            .iter()
            .map(
                |holder| crate::structs::placement::transition::BucketBarrier {
                    bucket: bucket.bucket,
                    reported_by: *holder,
                    frontier: vec![
                        (1..=4u8)
                            .find(|seed| node(*seed) == *holder)
                            .expect("known"),
                    ],
                },
            )
            .collect();
        let digest = fenced.barrier_digest(bucket.bucket);
        for holder in &bucket.target_holders {
            let seed = (1..=4u8)
                .find(|seed| node(*seed) == *holder)
                .expect("known");
            let proof = crate::structs::placement::transition::ProofClaim {
                realm_id,
                transition_id: plan.transition_id,
                strategy_id,
                bucket: bucket.bucket,
                old_activation_epoch: 1,
                target_map_epoch: 2,
                barrier_digest: digest,
                checkpoint_root: [1; 32],
                holder: *holder,
            }
            .sign(&iroh::SecretKey::from_bytes(&[seed; 32]));
            state
                .apply_operation(
                    &actor(seed),
                    AdminDocumentOperation::TransitionProofSubmitted {
                        transition_id: plan.transition_id,
                        strategy_id,
                        proof,
                    },
                    ulid::Ulid::generate(),
                )
                .expect("applies");
        }
    }
    state
}

#[test]
fn prune_keeps_holders() {
    // Pruning a released record preserves the bucket placements produced by the reduced chain.
    let realm_id = RealmId::from_bytes([3u8; 32]);
    let strategy_id = Ulid::from_bytes([5u8; 16]);
    let state = reduced_handoff(realm_id, strategy_id);
    let materialize = |now_ms: u64| {
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        overlay_placement(&mut config, &state, now_ms);
        config
    };
    let buckets = [
        PlacementRef {
            strategy_id,
            shard: 0,
        },
        PlacementRef {
            strategy_id,
            shard: 1,
        },
    ];

    let live = materialize(0);
    assert_eq!(live.placement_transitions.len(), 1);
    let moved: Vec<Vec<NodeId>> = buckets
        .iter()
        .map(|placement| resolve_shard_holders(&live, placement))
        .collect();
    assert!(moved.iter().all(|holders| holders.len() == 1));
    assert!(
        moved
            .iter()
            .all(|holders| [node(3), node(4)].contains(&holders[0])),
        "the handoff moved both buckets onto the target map"
    );

    let pruned = materialize(u64::MAX);
    assert!(pruned.placement_transitions.is_empty());
    for (placement, holders) in buckets.iter().zip(&moved) {
        assert_eq!(&resolve_shard_holders(&pruned, placement), holders);
        assert!(bucket_membership(&pruned, placement, u64::MAX).members == *holders);
    }
    assert_eq!(pruned.placement_activations, live.placement_activations);
}

#[test]
fn holders_limit_order() {
    let mut config = RealmConfigDocument::new(RealmId::from_bytes([3u8; 32]), Vec::new(), 3);
    let strategy = PlacementStrategy {
        strategy_id: Ulid::from_bytes([5u8; 16]),
        name: "default".to_string(),
        replica_count: None,
        distinct_locations: false,
        affinity: Vec::new(),
        shard_count: 1,
    };
    config.default_strategy_id = Some(strategy.strategy_id);
    config.strategies = vec![strategy];
    for seed in 1..=64u8 {
        config.ensure_node(node(seed), RealmNodeKind::Server);
    }
    let placement = PlacementRef {
        strategy_id: config.default_strategy_id.expect("default strategy"),
        shard: 0,
    };
    let full = resolve_shard_holders(&config, &placement);
    let bounded = resolve_holders_limit(&config, &placement, MAX_READ_HOLDERS);

    assert_eq!(full.len(), 64);
    assert_eq!(bounded, full[..MAX_READ_HOLDERS].to_vec());
}
