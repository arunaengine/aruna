//! Mints ULIDs inside an operation from the runner's time stamp and per-run entropy, so an
//! operation never reads a clock or a random source.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use ulid::Ulid;

/// Low bits of an id's random part that count the ids of one run in one millisecond.
pub const COUNTER_BITS: u32 = 16;
/// Bits above the counter that hold the run slot, which no other run of the process shares.
pub const SLOT_BITS: u32 = 32;
const COUNTER_MASK: u128 = (1 << COUNTER_BITS) - 1;

/// The entropy of one run: `process` bits above the run `slot`, and a zero counter below. The low
/// 48 bits of each id's random part (slot and counter) serve as the nonce of a structured id.
pub fn run_entropy(process: u32, slot: u32) -> u128 {
    (u128::from(process) << (SLOT_BITS + COUNTER_BITS)) | (u128::from(slot) << COUNTER_BITS)
}

/// The ids one operation run mints: the time part from the runner's stamp, the random part from
/// its entropy. Each id is greater than the one before, so two ids of one run never collide, and
/// the counter never runs into the slot bits of another run.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IdMint {
    entropy: u128,
    last: Option<Ulid>,
}

impl IdMint {
    /// A mint for the run the runner seeded with `entropy`.
    pub fn new(entropy: u128) -> Self {
        Self {
            entropy,
            last: None,
        }
    }

    /// An id at `now_ms` with the run's entropy, or the next one after the last id when that one
    /// is not smaller, as within one millisecond or after the clock stepped back.
    pub fn mint(&mut self, now_ms: u64) -> Ulid {
        let fresh = Ulid::from_parts(now_ms, self.entropy);
        let id = match self.last {
            Some(last) if fresh <= last => self.after(last),
            _ => fresh,
        };
        self.last = Some(id);
        id
    }

    /// One past `last` in its millisecond, or the run's first id of the next millisecond once the
    /// counter bits are spent.
    fn after(&self, last: Ulid) -> Ulid {
        match last.random() & COUNTER_MASK == COUNTER_MASK {
            true => Ulid::from_parts(last.timestamp_ms() + 1, self.entropy),
            false => Ulid(last.0 + 1),
        }
    }
}

/// `N` nonce bytes as unique as `id`, for a cipher whose nonce must never repeat under one key.
/// An id minted in a run is unique, so the nonce is too; it need not be secret.
pub fn id_nonce<const N: usize>(id: Ulid) -> [u8; N] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"aruna-id-nonce-v1\0");
    hasher.update(&id.to_bytes());
    let mut nonce = [0u8; N];
    hasher.finalize_xof().fill(&mut nonce);
    nonce
}

/// An id fixed by `scope` and `name`, the same on every node, for the built-in parts of a new
/// document such as the default roles of a group.
pub fn named_id(scope: &[u8], name: &str) -> Ulid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"aruna-named-id-v1\0");
    hasher.update(&(scope.len() as u64).to_le_bytes());
    hasher.update(scope);
    hasher.update(name.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    Ulid::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::{COUNTER_BITS, IdMint, named_id, run_entropy};
    use ulid::Ulid;

    const NONCE: u128 = (1 << 48) - 1;

    #[test]
    fn named_ids_stable() {
        let id = named_id(b"group", "admin");
        assert_eq!(id, named_id(b"group", "admin"));
        assert_ne!(id, named_id(b"group", "user"));
        assert_ne!(id, named_id(b"other", "admin"));
    }

    #[test]
    fn ids_only_increase() {
        let mut mint = IdMint::new(7);
        let first = mint.mint(1_000);
        let second = mint.mint(1_000);
        let stepped_back = mint.mint(999);
        let later = mint.mint(2_000);
        assert_eq!(first, Ulid::from_parts(1_000, 7));
        assert!(first < second && second < stepped_back && stepped_back < later);
        assert_eq!(later, Ulid::from_parts(2_000, 7));
    }

    #[test]
    fn entropy_separates_runs() {
        assert_ne!(IdMint::new(1).mint(1_000), IdMint::new(2).mint(1_000));
    }

    // Pinned time and entropy give the same ids on every run, as tests need.
    #[test]
    fn pinned_inputs_repeat() {
        let run = || {
            let mut mint = IdMint::new(42);
            [mint.mint(5), mint.mint(5), mint.mint(6)]
        };
        assert_eq!(run(), run());
    }

    #[test]
    fn overflow_moves_forward() {
        let mut mint = IdMint::new(u128::MAX);
        let first = mint.mint(5);
        let second = mint.mint(5);
        assert!(first < second);
        assert_eq!(second.timestamp_ms(), 6);
    }

    // A run that spends its counter in one millisecond goes on in the next one, in its own slot.
    #[test]
    fn counter_keeps_slot() {
        let entropy = run_entropy(9, 5);
        let mut mint = IdMint::new(entropy);
        let ids: Vec<Ulid> = (0..=1u32 << COUNTER_BITS).map(|_| mint.mint(5)).collect();
        assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(
            ids.iter()
                .all(|id| id.random() >> COUNTER_BITS == entropy >> COUNTER_BITS)
        );
        assert_eq!(ids[ids.len() - 1], Ulid::from_parts(6, entropy));
    }

    // A later run's nonces in the same millisecond are above every nonce of an earlier run.
    #[test]
    fn later_slots_rise() {
        let mut first = IdMint::new(run_entropy(9, 5));
        let earlier: Vec<u128> = (0..3).map(|_| first.mint(5).random() & NONCE).collect();
        let later = IdMint::new(run_entropy(9, 6)).mint(5).random() & NONCE;
        assert!(earlier.iter().all(|nonce| *nonce < later));
    }
}
