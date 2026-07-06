//! Deterministic statistical distribution gates for the weighted resolver.
//!
//! Subjects are drawn from a blake3 counter stream (no RNG) so the observed
//! shares are reproducible on every run and platform.

use std::collections::BTreeMap;

use aruna_core::NodeId;
use aruna_core::structs::{
    AffinityEffect, AffinityRule, LabelMatch, PlacementStrategy, RealmNodeKind,
};
use ulid::Ulid;

use crate::placement::{PlacementView, ResolvedNode, resolve_holders};

const SUBJECTS: u64 = 10_000;

fn node_id(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn subject(counter: u64) -> [u8; 32] {
    *blake3::hash(&counter.to_le_bytes()).as_bytes()
}

fn view(weights: &[(u8, u32)]) -> PlacementView {
    let nodes = weights
        .iter()
        .map(|&(seed, weight)| ResolvedNode {
            node_id: node_id(seed),
            kind: RealmNodeKind::Server,
            location: "default".to_string(),
            weight,
            full: false,
            draining: false,
            labels: BTreeMap::new(),
        })
        .collect();
    PlacementView { nodes }
}

fn located_node(seed: u8, location: &str, weight: u32) -> ResolvedNode {
    ResolvedNode {
        node_id: node_id(seed),
        kind: RealmNodeKind::Server,
        location: location.to_string(),
        weight,
        full: false,
        draining: false,
        labels: BTreeMap::new(),
    }
}

fn replica_one() -> PlacementStrategy {
    PlacementStrategy {
        strategy_id: Ulid::nil(),
        name: "r1".to_string(),
        replica_count: Some(1),
        distinct_locations: false,
        affinity: Vec::new(),
        bucket_count: 64,
    }
}

fn top_holder(view: &PlacementView, strategy: &PlacementStrategy, counter: u64) -> NodeId {
    resolve_holders(view, strategy, &subject(counter), 0, None)[0]
}

fn share_of(view: &PlacementView, strategy: &PlacementStrategy, seed: u8) -> f64 {
    let target = node_id(seed);
    let hits = (0..SUBJECTS)
        .filter(|&counter| top_holder(view, strategy, counter) == target)
        .count();
    hits as f64 / SUBJECTS as f64
}

fn multiply_rule(key: &str, value: &str, permille: u32) -> AffinityRule {
    AffinityRule {
        matcher: LabelMatch {
            key: key.to_string(),
            value: value.to_string(),
        },
        effect: AffinityEffect::Multiply { permille },
    }
}

#[test]
fn uniform_weights_balance_within_binomial_band() {
    // 5 uniform nodes, replica 1: each expects 20%. σ = √(n·p·(1-p)) = 40 over
    // 10_000, so 3σ ≈ 0.012; a ±2pp band clears it comfortably.
    let seeds = [1u8, 2, 3, 4, 5];
    let view = view(&seeds.map(|seed| (seed, 100)));
    let strategy = replica_one();
    for seed in seeds {
        let share = share_of(&view, &strategy, seed);
        assert!((share - 0.20).abs() < 0.02, "node {seed} share {share}");
    }
}

#[test]
fn weighted_shares_track_weight_within_band() {
    // Weights 100/200/300 ⇒ expected shares 1/6, 2/6, 3/6.
    let view = view(&[(1, 100), (2, 200), (3, 300)]);
    let strategy = replica_one();
    for (seed, want) in [(1u8, 100.0 / 600.0), (2, 200.0 / 600.0), (3, 300.0 / 600.0)] {
        let share = share_of(&view, &strategy, seed);
        assert!(
            (share - want).abs() < 0.02,
            "node {seed} share {share} want {want}"
        );
    }
}

#[test]
fn reweight_moves_only_toward_bumped_node() {
    // Bump one of 5 uniform nodes 100 → 150 (total 500 → 550). Its replica-1
    // share grows from 100/500 to 150/550, and only those keys move — all of
    // them onto the reweighted node, none reshuffled between the others.
    let seeds = [1u8, 2, 3, 4, 5];
    let before = view(&seeds.map(|seed| (seed, 100)));
    let mut after_weights = seeds.map(|seed| (seed, 100));
    after_weights[0].1 = 150;
    let after = view(&after_weights);
    let strategy = replica_one();
    let bumped = node_id(1);

    let mut changed = 0u64;
    for counter in 0..SUBJECTS {
        let old_holder = top_holder(&before, &strategy, counter);
        let new_holder = top_holder(&after, &strategy, counter);
        if old_holder != new_holder {
            changed += 1;
            assert_eq!(new_holder, bumped, "subject {counter} moved off-target");
        }
    }
    let fraction = changed as f64 / SUBJECTS as f64;
    // Proportional-minimum movement: 150/550 − 100/500 ≈ 0.073, banded loosely.
    assert!(
        (0.04..0.12).contains(&fraction),
        "movement fraction {fraction} outside proportional-minimum band"
    );
}

#[test]
fn multiply_affinity_changes_cross_location_distribution() {
    let mut boosted = located_node(1, "a", 100);
    boosted
        .labels
        .insert("tier".to_string(), "boosted".to_string());
    let view = PlacementView {
        nodes: vec![boosted, located_node(2, "b", 100)],
    };
    let baseline = replica_one();
    let mut multiplied = baseline.clone();
    multiplied.affinity = vec![multiply_rule("tier", "boosted", 3_000)];

    let baseline_share = share_of(&view, &baseline, 1);
    let multiplied_share = share_of(&view, &multiplied, 1);

    assert!(
        (baseline_share - 0.50).abs() < 0.02,
        "baseline location share {baseline_share}"
    );
    assert!(
        (multiplied_share - 0.75).abs() < 0.02,
        "multiplied location share {multiplied_share}"
    );
}
