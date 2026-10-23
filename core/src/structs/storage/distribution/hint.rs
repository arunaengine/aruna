//! Mints version ids that carry a 20-bit set hint in the ULID random part (D5, D6) and maps
//! hints back to candidate sets. The hint finds candidates; it never proves membership.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::{DistributionBinding, DistributionRecord};

/// Hint bits sit right below the 48 timestamp bits, leaving 60 random bits.
pub const HINT_BITS: u32 = 20;
const LOW_BITS: u32 = 60;
const HINT_MASK: u128 = (1 << HINT_BITS) - 1;
const LOW_MASK: u128 = (1 << LOW_BITS) - 1;

/// The first 20 bits of BLAKE3 over a set id.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct SetHint(u32);

impl SetHint {
    pub fn of_set(set_id: Ulid) -> Self {
        let hash = blake3::hash(&set_id.to_bytes());
        let bytes = hash.as_bytes();
        Self(u32::from(bytes[0]) << 12 | u32::from(bytes[1]) << 4 | u32::from(bytes[2]) >> 4)
    }

    /// The hint bits of any version id; for an ordinary bucket's id they are random.
    pub fn of_version(version_id: Ulid) -> Self {
        Self(((version_id.random() >> LOW_BITS) & HINT_MASK) as u32)
    }

    pub fn value(self) -> u32 {
        self.0
    }
}

impl DistributionBinding {
    pub fn hint(&self) -> SetHint {
        SetHint::of_set(self.set_id)
    }
}

/// A version id minted at `time_ms` with the hint above the low 60 bits of `random`.
pub fn hinted_id(time_ms: u64, random: u128, hint: SetHint) -> Ulid {
    Ulid::from_parts(
        time_ms,
        u128::from(hint.0) << LOW_BITS | (random & LOW_MASK),
    )
}

/// Mints a new version id now: hinted inside a distributed bucket, a plain ULID otherwise.
/// The mint time is always the timestamp. Collisions are checked by the version id index.
pub fn mint_version(hint: Option<SetHint>) -> Ulid {
    let fresh = Ulid::generate();
    match hint {
        Some(hint) => hinted_id(fresh.timestamp_ms(), fresh.random(), hint),
        None => fresh,
    }
}

/// Local index from hint to the sets that share it, built from distribution records.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HintIndex {
    sets: BTreeMap<SetHint, BTreeSet<Ulid>>,
}

impl HintIndex {
    pub fn from_records<'a>(records: impl IntoIterator<Item = &'a DistributionRecord>) -> Self {
        let mut index = Self::default();
        for record in records {
            index.insert(record.set_id);
        }
        index
    }

    pub fn insert(&mut self, set_id: Ulid) {
        self.sets
            .entry(SetHint::of_set(set_id))
            .or_default()
            .insert(set_id);
    }

    pub fn remove(&mut self, set_id: Ulid) {
        let hint = SetHint::of_set(set_id);
        let empty = self.sets.get_mut(&hint).is_some_and(|sets| {
            sets.remove(&set_id);
            sets.is_empty()
        });
        if empty {
            self.sets.remove(&hint);
        }
    }

    /// Every set whose hint the version id carries; usually one, each to be probed.
    pub fn candidates(&self, version_id: Ulid) -> impl Iterator<Item = Ulid> + '_ {
        self.sets
            .get(&SetHint::of_version(version_id))
            .into_iter()
            .flatten()
            .copied()
    }
}

#[cfg(test)]
mod tests {
    use super::{HintIndex, SetHint, hinted_id, mint_version};
    use std::time::{Duration, SystemTime};
    use ulid::Ulid;

    fn set(seed: u8) -> Ulid {
        Ulid::from_bytes([seed; 16])
    }

    #[test]
    fn hint_uses_blake3() {
        let set_id = set(1);
        let hash = blake3::hash(&set_id.to_bytes());
        let bits = u32::from_be_bytes([
            0,
            hash.as_bytes()[0],
            hash.as_bytes()[1],
            hash.as_bytes()[2],
        ]);
        assert_eq!(SetHint::of_set(set_id).value(), bits >> 4);
        assert!(SetHint::of_set(set_id).value() < 1 << 20);
    }

    #[test]
    fn hint_keeps_time() {
        let hint = SetHint::of_set(set(2));
        let time_ms = (1 << 48) - 1;
        let id = hinted_id(time_ms, u128::MAX, hint);
        assert_eq!(id.timestamp_ms(), time_ms);
        assert_eq!(SetHint::of_version(id), hint);
        assert_eq!(id.random() & ((1 << 60) - 1), (1 << 60) - 1);
        assert_eq!(SetHint::of_version(hinted_id(7, 0, hint)), hint);
    }

    #[test]
    fn minted_ids_hinted() {
        let before = SystemTime::now() - Duration::from_secs(1);
        let hint = SetHint::of_set(set(3));
        let id = mint_version(Some(hint));
        assert_eq!(SetHint::of_version(id), hint);
        assert!(id.datetime() >= before);
        // Distinct ids for one set still sort by mint time first.
        let later = hinted_id(id.timestamp_ms() + 1, 0, hint);
        assert!(later > id);
    }

    #[test]
    fn index_lists_candidates() {
        let mut index = HintIndex::default();
        index.insert(set(4));
        index.insert(set(5));
        let id = mint_version(Some(SetHint::of_set(set(4))));
        assert!(index.candidates(id).any(|candidate| candidate == set(4)));
        index.remove(set(4));
        assert!(index.candidates(id).all(|candidate| candidate != set(4)));
        assert_eq!(
            index
                .candidates(hinted_id(1, 0, SetHint::of_set(set(5))))
                .count(),
            1
        );
        index.remove(set(5));
        assert_eq!(index, HintIndex::default());
    }
}
