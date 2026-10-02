//! Hashes a multipart upload's parts in part-number order while they arrive.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::hash::Hasher;
use ulid::Ulid;

/// One upload attempt of a part. A repeated upload of the same number is a new attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PartAttempt {
    pub part_number: u16,
    pub attempt: Ulid,
    pub size: u64,
}

struct Link {
    part: PartAttempt,
    after: Hasher,
}

/// Hash states saved at each part boundary of one upload. Only a part that directly follows the
/// last saved one is hashed while it streams; completion reads the rest of the object.
#[derive(Default)]
pub(crate) struct PartChain {
    links: Vec<Link>,
    active: Option<(u16, Ulid)>,
}

impl std::fmt::Debug for PartChain {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PartChain")
            .field("links", &self.links.len())
            .field("active", &self.active)
            .finish()
    }
}

impl PartChain {
    /// The state to feed this part's bytes into, if it is next in order. A repeated part
    /// number drops every state from that part on.
    pub fn begin(&mut self, part_number: u16, attempt: Ulid) -> Option<Hasher> {
        self.links
            .retain(|link| link.part.part_number < part_number);
        match self.active {
            // Two attempts of one part: which bytes the backend keeps is unknown.
            Some((active, _)) if active == part_number => {
                self.active = None;
                return None;
            }
            Some((active, _)) if active > part_number => self.active = None,
            _ => {}
        }
        let next = self
            .links
            .last()
            .map_or(1, |link| link.part.part_number.saturating_add(1));
        if part_number != next || self.active.is_some() {
            return None;
        }
        self.active = Some((part_number, attempt));
        Some(
            self.links
                .last()
                .map_or_else(Hasher::new, |link| link.after.clone()),
        )
    }

    /// Saves the state after a part that streamed completely.
    pub fn finish(&mut self, part: PartAttempt, after: Hasher) {
        if self.active == Some((part.part_number, part.attempt)) {
            self.active = None;
            self.links.push(Link { part, after });
        }
    }

    /// Forgets a part that did not stream completely.
    pub fn abandon(&mut self, part_number: u16, attempt: Ulid) {
        if self.active == Some((part_number, attempt)) {
            self.active = None;
        }
    }

    /// The state after the longest prefix of `listed` hashed here, its byte count and how many
    /// listed parts it covers. The caller hashes the object from that byte on.
    pub fn resume(&self, listed: &[PartAttempt]) -> (Hasher, u64, usize) {
        let covered = self
            .links
            .iter()
            .zip(listed)
            .take_while(|(link, part)| link.part == **part)
            .count();
        let offset = listed[..covered].iter().map(|part| part.size).sum();
        let state = covered
            .checked_sub(1)
            .map_or_else(Hasher::new, |last| self.links[last].after.clone());
        (state, offset, covered)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(part_number: u16, size: usize) -> Vec<u8> {
        (0..size)
            .map(|index| (index as u8).wrapping_mul(31) ^ part_number as u8)
            .collect()
    }

    fn attempt(part_number: u16, size: usize) -> PartAttempt {
        PartAttempt {
            part_number,
            attempt: Ulid::generate(),
            size: size as u64,
        }
    }

    /// Streams one part through the chain the way an upload would.
    fn upload(chain: &mut PartChain, part: PartAttempt) {
        if let Some(mut state) = chain.begin(part.part_number, part.attempt) {
            state.update(&bytes(part.part_number, part.size as usize));
            chain.finish(part, state);
        }
    }

    /// Completes from the chain plus a read of the rest, and checks it against one pass.
    fn completes(chain: &PartChain, listed: &[PartAttempt]) -> usize {
        let object: Vec<u8> = listed
            .iter()
            .flat_map(|part| bytes(part.part_number, part.size as usize))
            .collect();
        let (mut state, offset, covered) = chain.resume(listed);
        state.update(&object[offset as usize..]);
        assert_eq!(state.to_map(), Hasher::new_with_bytes(&object).to_map());
        covered
    }

    #[test]
    fn ordered_skips_read() {
        let mut chain = PartChain::default();
        let parts = [attempt(1, 3000), attempt(2, 3000), attempt(3, 17)];
        parts.iter().for_each(|part| upload(&mut chain, *part));

        assert_eq!(completes(&chain, &parts), 3);
    }

    #[test]
    fn overlap_stops_chain() {
        let mut chain = PartChain::default();
        let parts = [attempt(1, 1024), attempt(2, 1024), attempt(3, 5)];
        let first = chain.begin(1, parts[0].attempt).unwrap();
        // Part 2 starts while part 1 still streams, so it cannot be hashed in order.
        assert!(chain.begin(2, parts[1].attempt).is_none());
        let mut first = first;
        first.update(&bytes(1, 1024));
        chain.finish(parts[0], first);
        upload(&mut chain, parts[2]);

        assert_eq!(completes(&chain, &parts), 1);
    }

    #[test]
    fn skipped_part_rereads() {
        let mut chain = PartChain::default();
        let parts = [attempt(1, 700), attempt(2, 900), attempt(3, 64)];
        parts.iter().for_each(|part| upload(&mut chain, *part));

        assert_eq!(completes(&chain, &[parts[0], parts[2]]), 1);
        assert_eq!(completes(&chain, &[parts[1], parts[2]]), 0);
    }

    #[test]
    fn repeated_part_replaces() {
        let mut chain = PartChain::default();
        let parts = [attempt(1, 500), attempt(2, 500), attempt(3, 500)];
        parts.iter().for_each(|part| upload(&mut chain, *part));
        let again = attempt(2, 600);
        upload(&mut chain, again);

        assert_eq!(completes(&chain, &[parts[0], again, parts[2]]), 2);
        assert_eq!(completes(&chain, &parts), 1);
    }

    #[test]
    fn repeat_drops_active() {
        let mut chain = PartChain::default();
        let parts = [attempt(1, 300), attempt(2, 300)];
        upload(&mut chain, parts[0]);
        let mut active = chain.begin(2, parts[1].attempt).unwrap();
        let again = attempt(1, 300);
        upload(&mut chain, again);
        active.update(&bytes(2, 300));
        chain.finish(parts[1], active);

        assert_eq!(completes(&chain, &[again, parts[1]]), 1);
    }

    #[test]
    fn concurrent_attempts_read() {
        let mut chain = PartChain::default();
        let first = attempt(1, 400);
        let second = attempt(1, 400);
        let state = chain.begin(1, first.attempt).unwrap();
        assert!(chain.begin(1, second.attempt).is_none());
        chain.finish(first, state);

        assert_eq!(completes(&chain, &[first]), 0);
    }

    #[test]
    fn failure_frees_slot() {
        let mut chain = PartChain::default();
        let failed = attempt(1, 100);
        assert!(chain.begin(1, failed.attempt).is_some());
        chain.abandon(1, failed.attempt);
        let parts = [attempt(1, 100), attempt(2, 1)];
        parts.iter().for_each(|part| upload(&mut chain, *part));

        assert_eq!(completes(&chain, &parts), 2);
        assert_eq!(completes(&chain, &[failed, parts[1]]), 0);
    }

    #[test]
    fn size_mismatch_reads() {
        let mut chain = PartChain::default();
        let part = attempt(1, 2048);
        upload(&mut chain, part);

        assert_eq!(completes(&chain, &[PartAttempt { size: 2047, ..part }]), 0);
    }
}
