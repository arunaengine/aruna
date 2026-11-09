//! Orders the immutable writes of a last-writer-wins register and admits received writes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use crate::NodeId;

/// The order of one register write: the counter, then the writing node, then the write id.
/// A write made after seeing another always has a higher counter, so no clock is needed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RegisterStamp {
    pub counter: u64,
    pub node_id: NodeId,
    pub write_id: Ulid,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum RegisterError {
    /// The counter never wraps, so a register at the limit takes no more writes.
    #[error("the register counter is at its limit")]
    CounterLimit,
}

impl RegisterStamp {
    /// The stamp of a new write by `node_id`, one above the highest counter it holds.
    pub fn next(held: Option<u64>, node_id: NodeId, write_id: Ulid) -> Result<Self, RegisterError> {
        let counter = held
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(RegisterError::CounterLimit)?;
        Ok(Self {
            counter,
            node_id,
            write_id,
        })
    }

    /// Whether the stamp names `actor`, the node sync authenticated as the publisher.
    pub fn written_by(&self, actor: NodeId) -> bool {
        self.node_id == actor
    }
}

/// The current write of a register: the one with the highest stamp.
pub fn current_write<T>(
    writes: impl IntoIterator<Item = T>,
    stamp: impl Fn(&T) -> RegisterStamp,
) -> Option<T> {
    writes.into_iter().max_by_key(|write| stamp(write))
}

/// What a node does with a received write whose target it may already hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Admission {
    Store,
    /// The same bytes are already stored; nothing changes.
    Replay,
    /// Other bytes are stored under the target; writes are immutable.
    Conflict,
}

pub fn admit_write(stored: Option<&[u8]>, incoming: &[u8]) -> Admission {
    match stored {
        None => Admission::Store,
        Some(stored) if stored == incoming => Admission::Replay,
        Some(_) => Admission::Conflict,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn stamp(counter: u64, seed: u8, write: u8) -> RegisterStamp {
        RegisterStamp {
            counter,
            node_id: node(seed),
            write_id: Ulid::from_bytes([write; 16]),
        }
    }

    #[test]
    fn converges_any_order() {
        let writes = [
            stamp(2, 1, 1),
            stamp(3, 2, 2),
            stamp(3, 1, 3),
            stamp(1, 9, 9),
        ];
        let expected = current_write(writes, |write| *write);
        let mut order = writes;
        for _ in 0..writes.len() {
            order.rotate_left(1);
            assert_eq!(current_write(order, |write| *write), expected);
            order.reverse();
            assert_eq!(current_write(order, |write| *write), expected);
        }
        assert_eq!(
            current_write(Vec::<RegisterStamp>::new(), |write| *write),
            None
        );
    }

    #[test]
    fn ties_by_node() {
        let (low, high) = match node(1) < node(2) {
            true => (1, 2),
            false => (2, 1),
        };
        let pick = |writes: [RegisterStamp; 2]| current_write(writes, |write| *write);
        assert_eq!(
            pick([stamp(4, low, 9), stamp(4, high, 1)]),
            Some(stamp(4, high, 1))
        );
        assert_eq!(
            pick([stamp(4, low, 9), stamp(4, low, 1)]),
            Some(stamp(4, low, 9))
        );
        assert_eq!(
            pick([stamp(5, low, 1), stamp(4, high, 9)]),
            Some(stamp(5, low, 1))
        );
    }

    #[test]
    fn counter_limit() {
        let id = Ulid::from_bytes([1; 16]);
        assert_eq!(RegisterStamp::next(None, node(1), id).unwrap().counter, 1);
        assert_eq!(
            RegisterStamp::next(Some(7), node(1), id).unwrap().counter,
            8
        );
        let full = RegisterStamp::next(Some(u64::MAX), node(1), id);
        assert_eq!(full, Err(RegisterError::CounterLimit));
    }

    #[test]
    fn replay_idempotent() {
        assert_eq!(admit_write(None, b"a"), Admission::Store);
        assert_eq!(admit_write(Some(b"a"), b"a"), Admission::Replay);
        assert_eq!(admit_write(Some(b"a"), b"b"), Admission::Conflict);
    }

    #[test]
    fn actor_bound() {
        let written = stamp(1, 1, 1);
        assert!(written.written_by(node(1)));
        assert!(!written.written_by(node(2)));
    }
}
