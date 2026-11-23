//! Picks which equal source of a reference serves one read: a weighted random choice by
//! expected speed, so faster and less loaded sources get more reads and all stay measured.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::execution::source_access::SourceStats;

/// Share of reads spread evenly over the healthy sources, so idle and recovered ones get measured.
const EXPLORE_SHARE: f64 = 0.05;
/// Assumed while no source was measured; only the ratio between sources matters.
const FIRST_BYTE_MS: f64 = 100.0;
const BYTES_PER_SEC: f64 = 10_000_000.0;

/// One source a read may use.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Candidate {
    pub stats: SourceStats,
    /// The operator's weight, 1 by default.
    pub weight: f64,
    /// Bytes the read moves from this source.
    pub bytes: u64,
    /// Whether the source may serve first; otherwise it is tried only after the others.
    pub eligible: bool,
}

/// The order to try sources in, and the chance the first one had to be picked.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Choice {
    pub order: Vec<usize>,
    pub share: f64,
}

/// A healthy eligible source picked by `draw` in proportion to its expected speed, then the
/// other healthy sources in their order, then the failing ones in theirs.
pub(crate) fn choose_source(candidates: &[Candidate], draw: u64) -> Choice {
    let pool: Vec<usize> = (0..candidates.len())
        .filter(|&index| candidates[index].eligible && !candidates[index].stats.failing)
        .collect();
    let picked = pick(candidates, &pool, draw);
    let first = picked.map(|(index, _)| index);
    let rest = (0..candidates.len()).filter(|index| Some(*index) != first);
    let mut order: Vec<usize> = first.into_iter().collect();
    order.extend(
        rest.clone()
            .filter(|&index| !candidates[index].stats.failing),
    );
    order.extend(rest.filter(|&index| candidates[index].stats.failing));
    Choice {
        order,
        share: picked.map_or(0.0, |(_, share)| share),
    }
}

fn pick(candidates: &[Candidate], pool: &[usize], draw: u64) -> Option<(usize, f64)> {
    let stats = || candidates.iter().map(|candidate| candidate.stats);
    let first_ms = mean(
        stats().filter_map(|stats| stats.first_byte_ms),
        FIRST_BYTE_MS,
    );
    let rate = mean(
        stats().filter_map(|stats| stats.bytes_per_sec),
        BYTES_PER_SEC,
    );
    // Weights count relative to the largest, so no score overflows.
    let weights = pool.iter().map(|&index| candidates[index].weight);
    let heaviest = weights
        .filter(|weight| weight.is_finite())
        .fold(0.0, f64::max);
    let scores: Vec<f64> = (pool.iter())
        .map(|&index| score(&candidates[index], (first_ms, rate), heaviest))
        .collect();
    let total: f64 = scores.iter().sum();
    let total = if total.is_finite() { total } else { 0.0 };
    let even = 1.0 / pool.len() as f64;
    let mut point = (draw >> 11) as f64 / (1u64 << 53) as f64;
    let mut picked = None;
    for (&index, score) in pool.iter().zip(scores) {
        let share = match total > 0.0 {
            true => (1.0 - EXPLORE_SHARE) * score / total + EXPLORE_SHARE * even,
            false => even,
        };
        picked = Some((index, share));
        if point < share {
            break;
        }
        point -= share;
    }
    picked
}

/// The mean of the measured values; unmeasured sources count as average, so they get tried.
fn mean(values: impl Iterator<Item = f64>, default: f64) -> f64 {
    let values = values.filter(|value| value.is_finite());
    let (sum, count) = values.fold((0.0, 0u32), |(sum, count), value| (sum + value, count + 1));
    match count {
        0 => default,
        count => sum / f64::from(count),
    }
}

/// Reads per second the source is expected to finish, scaled by its weight and health; 0 for
/// anything not a finite number. Values that are no finite numbers count as unmeasured.
fn score(candidate: &Candidate, (first_ms, rate): (f64, f64), heaviest: f64) -> f64 {
    let stats = candidate.stats;
    let finite = |value: Option<f64>| value.filter(|value| value.is_finite());
    let first = finite(stats.first_byte_ms).unwrap_or(first_ms) / 1000.0;
    let rate = finite(stats.bytes_per_sec).unwrap_or(rate).max(1.0);
    // Reads in flight share the source's rate with this one.
    let sharing = 1.0 + f64::from(stats.in_flight);
    let seconds = (first + candidate.bytes as f64 * sharing / rate).max(1e-6);
    let healthy = (1.0 - stats.error_rate.clamp(0.0, 1.0)).powi(2);
    let score = candidate.weight / heaviest * healthy / seconds;
    match score.is_finite() && score > 0.0 {
        true => score,
        false => 0.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    const MB: f64 = 1_000_000.0;

    fn measured(first_byte_ms: f64, bytes_per_sec: f64) -> Candidate {
        Candidate {
            stats: SourceStats {
                first_byte_ms: Some(first_byte_ms),
                bytes_per_sec: Some(bytes_per_sec),
                ..SourceStats::default()
            },
            weight: 1.0,
            bytes: 100_000_000,
            eligible: true,
        }
    }

    /// The share of `reads` seeded draws each source is picked first for.
    fn shares(candidates: &[Candidate], reads: u32) -> Vec<f64> {
        let mut rng = StdRng::seed_from_u64(7);
        let mut picks = vec![0u32; candidates.len()];
        for _ in 0..reads {
            picks[choose_source(candidates, rng.next_u64()).order[0]] += 1;
        }
        picks
            .into_iter()
            .map(|count| f64::from(count) / f64::from(reads))
            .collect()
    }

    #[test]
    fn prefers_faster_source() {
        // Ten times the rate wins most large reads, the slower one is still measured.
        let shares = shares(
            &[measured(50.0, 100.0 * MB), measured(50.0, 10.0 * MB)],
            20_000,
        );
        assert!((0.86..0.91).contains(&shares[0]), "{shares:?}");
        assert!(shares[1] > 0.08, "{shares:?}");
    }

    #[test]
    fn latency_decides_small() {
        let mut quick = measured(20.0, MB);
        let mut slow = measured(400.0, 100.0 * MB);
        (quick.bytes, slow.bytes) = (1_000, 1_000);
        let shares = shares(&[slow, quick], 20_000);
        assert!(shares[1] > 0.85, "{shares:?}");
    }

    #[test]
    fn equal_sources_split() {
        let unmeasured = Candidate {
            stats: SourceStats::default(),
            ..measured(0.0, 0.0)
        };
        let split = shares(&[unmeasured; 3], 30_000);
        assert!(
            split.iter().all(|share| (share - 1.0 / 3.0).abs() < 0.02),
            "{split:?}"
        );
        // An unmeasured source counts as average, so it gets its share and is measured.
        let split = shares(&[measured(50.0, 10.0 * MB), unmeasured], 20_000);
        assert!((split[0] - 0.5).abs() < 0.02, "{split:?}");
    }

    #[test]
    fn load_favors_idle() {
        let mut busy = measured(50.0, 10.0 * MB);
        busy.stats.in_flight = 3;
        let shares = shares(&[busy, measured(50.0, 10.0 * MB)], 20_000);
        assert!((0.19..0.25).contains(&shares[0]), "{shares:?}");
    }

    #[test]
    fn errors_shift_traffic() {
        let mut flaky = measured(50.0, 10.0 * MB);
        flaky.stats.error_rate = 0.8;
        let shares_flaky = shares(&[flaky, measured(50.0, 10.0 * MB)], 20_000);
        assert!((0.03..0.08).contains(&shares_flaky[0]), "{shares_flaky:?}");
        // A failing source is tried last; once it answers again its rate fades and it recovers.
        flaky.stats.failing = true;
        let order = choose_source(&[flaky, measured(50.0, 10.0 * MB)], 0).order;
        assert_eq!(order, [1, 0]);
        (flaky.stats.failing, flaky.stats.error_rate) = (false, 0.05);
        let shares = shares(&[flaky, measured(50.0, 10.0 * MB)], 20_000);
        assert!((0.43..0.5).contains(&shares[0]), "{shares:?}");
    }

    #[test]
    fn weights_scale_shares() {
        let mut heavy = measured(50.0, 10.0 * MB);
        heavy.weight = 3.0;
        let shares = shares(&[heavy, measured(50.0, 10.0 * MB)], 20_000);
        assert!((shares[0] - 0.7375).abs() < 0.02, "{shares:?}");
    }

    #[test]
    fn extreme_weights_finite() {
        // Huge, invalid or unmeasurable inputs never turn the shares into NaN.
        let mut huge = measured(50.0, 10.0 * MB);
        huge.weight = 1e308;
        let mut nan = measured(f64::NAN, 10.0 * MB);
        nan.weight = f64::NAN;
        let split = shares(&[huge, measured(50.0, 10.0 * MB), nan], 20_000);
        assert!(split[0] > 0.9, "{split:?}");
        let mut lost = [nan, nan];
        lost[1].stats.error_rate = f64::NAN;
        let split = shares(&lost, 20_000);
        assert!(
            split.iter().all(|share| (share - 0.5).abs() < 0.02),
            "{split:?}"
        );
    }

    #[test]
    fn ineligible_waits_turn() {
        // An ineligible source is never first but stays a fallback before failing ones.
        let mut mirror = measured(5.0, 100.0 * MB);
        mirror.eligible = false;
        let mut failing = measured(5.0, 100.0 * MB);
        failing.stats.failing = true;
        let candidates = [failing, mirror, measured(50.0, MB)];
        let mut rng = StdRng::seed_from_u64(3);
        for _ in 0..100 {
            assert_eq!(choose_source(&candidates, rng.next_u64()).order, [2, 1, 0]);
        }
        let none = choose_source(&[failing, mirror], 0);
        assert_eq!((none.order, none.share), (vec![1, 0], 0.0));
    }
}
