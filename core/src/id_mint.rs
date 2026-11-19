//! Mints ULIDs inside an operation from the runner's time stamp and per-run entropy, so an
//! operation never reads a clock or a random source.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use ulid::Ulid;

/// The ids one operation run mints: the time part from the runner's stamp, the random part from
/// its entropy. Each id is greater than the one before, so two ids of one run never collide.
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

    /// An id at `now_ms` with the run's entropy, or one past the last id when that one is not
    /// smaller, as within one millisecond or after the clock stepped back.
    pub fn mint(&mut self, now_ms: u64) -> Ulid {
        let fresh = Ulid::from_parts(now_ms, self.entropy);
        let id = match self.last {
            Some(last) if fresh <= last => match last.increment() {
                Ok(next) | Err(next) => next,
            },
            _ => fresh,
        };
        self.last = Some(id);
        id
    }
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
    use super::{IdMint, named_id};
    use ulid::Ulid;

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
}
