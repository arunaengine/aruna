//! Integer-only weighted two-level rendezvous primitives.
//!
//! Determinism is the whole contract: identical inputs must produce identical
//! rankings on every platform, so no floating point appears outside `#[cfg(test)]`.

pub const PLACEMENT_DOMAIN: &[u8] = b"aruna-placement-rendezvous-v3";
pub const ROLE_LOCATION: u8 = b'L';
pub const ROLE_NODE: u8 = b'N';

/// Rendezvous hash of `(role, epoch, subject, id)`, forced nonzero via `| 1`.
pub fn selector_hash(role: u8, epoch: u64, subject: &[u8], id: &[u8]) -> u64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(PLACEMENT_DOMAIN);
    hasher.update(&[role]);
    hasher.update(&epoch.to_le_bytes());
    hasher.update(subject);
    hasher.update(id);
    let digest = hasher.finalize();
    let mut head = [0u8; 8];
    head.copy_from_slice(&digest.as_bytes()[..8]);
    u64::from_be_bytes(head) | 1
}

/// Exact UQ16.48 fixed-point encoding of `-log2(h / 2^64)` for nonzero `h`.
///
/// Normalises `h` by its leading zeros to a mantissa `m ∈ [1, 2)`; the integer
/// part is `leading_zeros + 1`. The 48 fraction bits are peeled by repeated
/// squaring: `m² ≥ 2` yields a set bit and halves the mantissa back into range.
pub fn neg_log2_q48(h: u64) -> u64 {
    debug_assert!(h != 0);
    let z = h.leading_zeros();
    let mut x = h << z;
    let mut f: u64 = 0;
    for _ in 0..48 {
        let mut y = ((x as u128) * (x as u128)) >> 63;
        if y >= 1u128 << 64 {
            f = (f << 1) | 1;
            y >>= 1;
        } else {
            f <<= 1;
        }
        x = y as u64;
    }
    (((z as u64) + 1) << 48) - f
}

/// Ranks candidate indices best-first by weighted rendezvous score `-log2(u)/weight`.
///
/// `i` precedes `j` iff `L_i·w_j < L_j·w_i`; ties break by `(L, id bytes)` ascending,
/// so zero-weight candidates (never a numerator advantage) sort after all positive ones.
pub fn rank_weighted<I: AsRef<[u8]>>(
    role: u8,
    epoch: u64,
    subject: &[u8],
    candidates: &[(I, u64)],
) -> Vec<usize> {
    let scores: Vec<u64> = candidates
        .iter()
        .map(|(id, _)| neg_log2_q48(selector_hash(role, epoch, subject, id.as_ref())))
        .collect();
    let mut order: Vec<usize> = (0..candidates.len()).collect();
    order.sort_unstable_by(|&i, &j| {
        let lhs = (scores[i] as u128) * (candidates[j].1 as u128);
        let rhs = (scores[j] as u128) * (candidates[i].1 as u128);
        lhs.cmp(&rhs)
            .then_with(|| scores[i].cmp(&scores[j]))
            .then_with(|| candidates[i].0.as_ref().cmp(candidates[j].0.as_ref()))
    });
    order
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const Q48_ONE: u64 = 1 << 48;

    fn counter_hash(counter: u64) -> u64 {
        let digest = blake3::hash(&counter.to_le_bytes());
        let mut head = [0u8; 8];
        head.copy_from_slice(&digest.as_bytes()[..8]);
        u64::from_le_bytes(head) | 1
    }

    fn reference_neg_log2(h: u64) -> f64 {
        -(h as f64 / 2f64.powi(64)).log2()
    }

    fn unique_ids(values: Vec<([u8; 6], u64)>) -> Vec<([u8; 6], u64)> {
        let mut seen = std::collections::HashSet::new();
        values
            .into_iter()
            .filter(|(id, _)| seen.insert(*id))
            .collect()
    }

    fn ids_of(order: &[usize], candidates: &[([u8; 6], u64)]) -> Vec<[u8; 6]> {
        order.iter().map(|&i| candidates[i].0).collect()
    }

    fn candidate_strategy() -> impl Strategy<Value = Vec<([u8; 6], u64)>> {
        prop::collection::vec((any::<[u8; 6]>(), 0u64..1_048_576), 1..12).prop_map(unique_ids)
    }

    #[test]
    fn neg_log2_exact_vectors() {
        assert_eq!(neg_log2_q48(1 << 63), Q48_ONE);
        assert_eq!(neg_log2_q48(1 << 62), 2 * Q48_ONE);
        assert_eq!(neg_log2_q48(1), 64 * Q48_ONE);
        let max = neg_log2_q48(u64::MAX);
        assert!(max > 0 && max < Q48_ONE);
    }

    #[test]
    fn neg_log2_matches_float_reference() {
        let mut worst = 0f64;
        for counter in 0u64..4096 {
            let h = counter_hash(counter);
            let fixed = neg_log2_q48(h) as f64 / 2f64.powi(48);
            let reference = reference_neg_log2(h);
            worst = worst.max((fixed - reference).abs());
        }
        // 48-bit fraction ⇒ granularity 2^-48 ≈ 3.6e-15; 1e-9 leaves ample margin.
        assert!(worst < 1e-9, "worst absolute error {worst}");
    }

    #[test]
    fn rank_weighted_golden_order() {
        let ids: [[u8; 32]; 6] = [[1; 32], [2; 32], [3; 32], [4; 32], [5; 32], [6; 32]];
        let weights = [100u64, 100, 100, 300, 50, 200];
        let candidates: Vec<([u8; 32], u64)> =
            ids.iter().zip(weights).map(|(id, w)| (*id, w)).collect();
        let order = rank_weighted(ROLE_NODE, 0, b"golden-subject", &candidates);
        assert_eq!(order, vec![2, 5, 3, 1, 4, 0]);
    }

    #[test]
    fn weighted_top_one_frequency_tracks_weight() {
        let light = [0xAAu8; 32];
        let heavy = [0xBBu8; 32];
        let candidates = [(light, 100u64), (heavy, 300u64)];
        let total = 20_000u32;
        let mut heavy_wins = 0u32;
        for counter in 0..total {
            let subject = blake3::hash(&counter.to_le_bytes());
            let order = rank_weighted(ROLE_NODE, 0, subject.as_bytes(), &candidates);
            if order[0] == 1 {
                heavy_wins += 1;
            }
        }
        let frequency = heavy_wins as f64 / total as f64;
        assert!((frequency - 0.75).abs() < 0.02, "frequency {frequency}");
    }

    proptest! {
        #[test]
        fn neg_log2_is_monotone(a in any::<u64>(), b in any::<u64>()) {
            let h1 = a | 1;
            let h2 = b | 1;
            let (lo, hi) = if h1 <= h2 { (h1, h2) } else { (h2, h1) };
            prop_assert!(neg_log2_q48(lo) >= neg_log2_q48(hi));
        }

        #[test]
        fn rank_is_permutation_and_deterministic(candidates in candidate_strategy()) {
            let first = rank_weighted(ROLE_NODE, 0, b"subject", &candidates);
            let second = rank_weighted(ROLE_NODE, 0, b"subject", &candidates);
            prop_assert_eq!(&first, &second);
            let mut sorted = first;
            sorted.sort_unstable();
            prop_assert_eq!(sorted, (0..candidates.len()).collect::<Vec<usize>>());
        }

        #[test]
        fn rank_is_input_order_independent(
            candidates in candidate_strategy(),
            keys in prop::collection::vec(any::<u64>(), 0..12),
        ) {
            let base_ids = ids_of(&rank_weighted(ROLE_NODE, 7, b"subject", &candidates), &candidates);
            let mut paired: Vec<(u64, ([u8; 6], u64))> = candidates
                .iter()
                .enumerate()
                .map(|(i, c)| (keys.get(i).copied().unwrap_or(i as u64), *c))
                .collect();
            paired.sort_by_key(|(k, _)| *k);
            let permuted: Vec<([u8; 6], u64)> = paired.into_iter().map(|(_, c)| c).collect();
            let permuted_ids = ids_of(&rank_weighted(ROLE_NODE, 7, b"subject", &permuted), &permuted);
            prop_assert_eq!(base_ids, permuted_ids);
        }

        #[test]
        fn rank_is_weight_scale_invariant(
            candidates in candidate_strategy(),
            k in 1u64..1_048_576,
        ) {
            let base = ids_of(&rank_weighted(ROLE_NODE, 0, b"s", &candidates), &candidates);
            let scaled: Vec<([u8; 6], u64)> =
                candidates.iter().map(|(id, w)| (*id, w * k)).collect();
            let scaled_rank = ids_of(&rank_weighted(ROLE_NODE, 0, b"s", &scaled), &scaled);
            prop_assert_eq!(base, scaled_rank);
        }

        #[test]
        fn zero_weight_ranks_after_positive(candidates in candidate_strategy()) {
            prop_assume!(candidates.len() >= 2);
            let mut candidates = candidates;
            candidates[0].1 = 0;
            candidates[1].1 = candidates[1].1.max(1);
            let order = rank_weighted(ROLE_NODE, 0, b"s", &candidates);
            let mut seen_zero = false;
            for &i in &order {
                if candidates[i].1 == 0 {
                    seen_zero = true;
                } else {
                    prop_assert!(!seen_zero);
                }
            }
        }

        #[test]
        fn removing_candidate_preserves_relative_order(
            candidates in candidate_strategy(),
            victim in any::<u64>(),
        ) {
            prop_assume!(candidates.len() >= 2);
            let full = ids_of(&rank_weighted(ROLE_NODE, 3, b"s", &candidates), &candidates);
            let idx = (victim % candidates.len() as u64) as usize;
            let removed_id = candidates[idx].0;
            let mut reduced = candidates.clone();
            reduced.remove(idx);
            let reduced_ids = ids_of(&rank_weighted(ROLE_NODE, 3, b"s", &reduced), &reduced);
            let expected: Vec<[u8; 6]> = full.into_iter().filter(|id| *id != removed_id).collect();
            prop_assert_eq!(reduced_ids, expected);
        }
    }
}
