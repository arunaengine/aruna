//! Encodes a topic's applied cursor and checks that a stored one still describes its history.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use irokle::{ActorClock, ActorId, OpId};
use serde::{Deserialize, Serialize};

/// An applied-ops cursor with the history it describes: the genesis it was built
/// from and the op at each actor position. A chain replacement or orphan rebuild
/// renumbers sequences, so a position alone is never evidence its op stands.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct AppliedCursor {
    pub lineage: OpId,
    pub clock: ActorClock,
    pub marks: BTreeMap<ActorId, OpId>,
}

impl AppliedCursor {
    /// The cursor `stored` holds for `genesis`; `None` when it is unreadable or of another one.
    pub fn decode(stored: Option<&[u8]>, genesis: OpId) -> Option<Self> {
        stored
            .and_then(|value| postcard::from_bytes::<Self>(value).ok())
            .filter(|cursor| cursor.lineage == genesis)
    }

    /// Marks each position of `clock` with the op `held` finds there. A position with no op is
    /// left unmarked, which [`Self::stands`] treats as untrusted.
    pub fn marked<E>(
        genesis: OpId,
        clock: ActorClock,
        mut held: impl FnMut(&ActorId, u64) -> Result<Option<OpId>, E>,
    ) -> Result<Self, E> {
        let mut marks = BTreeMap::new();
        for (actor_id, actor_seq) in clock.iter().filter(|(_, seq)| **seq > 0) {
            if let Some(op_id) = held(actor_id, *actor_seq)? {
                marks.insert(*actor_id, op_id);
            }
        }
        Ok(Self {
            lineage: genesis,
            clock,
            marks,
        })
    }

    /// Whether every position still holds its marked op, as `held` reads the positions.
    pub fn stands<E>(
        &self,
        mut held: impl FnMut(&ActorId, u64) -> Result<Option<OpId>, E>,
    ) -> Result<bool, E> {
        for (actor_id, actor_seq) in self.clock.iter().filter(|(_, seq)| **seq > 0) {
            let current = held(actor_id, *actor_seq)?;
            if current.is_none() || current.as_ref() != self.marks.get(actor_id) {
                return Ok(false);
            }
        }
        Ok(true)
    }
}
