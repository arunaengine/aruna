//! Deletes plugin rows that are no longer needed: gap counts past the dedup retention on senders
//! and receivers, and the replay, waiting, gap and create key rows of removed installs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoResult, Next};
use aruna_core::keyspaces::{
    PLUGIN_CREATES_KEYSPACE, PLUGIN_GAPS_KEYSPACE, PLUGIN_QUEUE_KEYSPACE, PLUGIN_REPLAYS_KEYSPACE,
    PLUGIN_WAITING_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::plugin_installs::InstallState;
use aruna_core::plugin_runs::{DEDUP_RETENTION_MS, GapRow, RECEIVER_GAP};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use std::collections::BTreeMap;

use ulid::Ulid;

use super::rows::{Rows, Scan, StoreError, decode, delete_call, done};

/// How a row's install stands in the realm config.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Standing {
    Kept,
    Removing,
    Absent,
}

fn install_at(key: &[u8], at: usize) -> Ulid {
    Ulid::from_bytes(
        key.get(at..at + 16)
            .and_then(|id| id.try_into().ok())
            .unwrap_or_default(),
    )
}

/// Deletes the rows this node no longer needs; returns how many. Replay and create key rows go
/// once their install is absent or removing, as only this node wrote them; waiting and reported
/// rows go only while it is removing, as a receiver may not have the install record yet.
pub fn sweep_events(config: &RealmConfigDocument, now_ms: u64) -> SweepEvents {
    let installs = (config.installs.iter())
        .map(|install| {
            let standing = match install.state == InstallState::Removing {
                true => Standing::Removing,
                false => Standing::Kept,
            };
            (install.install_id, standing)
        })
        .collect();
    SweepEvents {
        installs,
        now_ms,
        phase: SweepPhase::Scanning(Table::Replays, Table::Replays.scan()),
        deletes: Vec::new(),
        dropped: 0,
    }
}

#[derive(Debug, PartialEq)]
pub struct SweepEvents {
    installs: BTreeMap<Ulid, Standing>,
    now_ms: u64,
    phase: SweepPhase,
    /// Deletes still to send before the next table, last first.
    deletes: Vec<Vec<(&'static str, Vec<u8>)>>,
    dropped: usize,
}

#[derive(Debug, PartialEq)]
enum SweepPhase {
    Scanning(Table, Scan),
    /// Sends the deletes of a table; the next table follows.
    Deleting(Option<Table>),
    /// The sweep ended; another answer is refused.
    Finished,
}

/// The tables a sweep reads, in order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Table {
    Replays,
    Waiting,
    Gaps,
    Creates,
}

impl Table {
    fn scan(self) -> Scan {
        let space = match self {
            Table::Replays => PLUGIN_REPLAYS_KEYSPACE,
            Table::Waiting => PLUGIN_WAITING_KEYSPACE,
            Table::Gaps => PLUGIN_GAPS_KEYSPACE,
            Table::Creates => PLUGIN_CREATES_KEYSPACE,
        };
        Scan::new(space, Vec::new())
    }

    fn after(self) -> Option<Table> {
        match self {
            Table::Replays => Some(Table::Waiting),
            Table::Waiting => Some(Table::Gaps),
            Table::Gaps => Some(Table::Creates),
            Table::Creates => None,
        }
    }
}

impl SweepEvents {
    fn standing(&self, install_id: Ulid) -> Standing {
        (self.installs.get(&install_id).copied()).unwrap_or(Standing::Absent)
    }

    /// The deletes of one table's rows, in order.
    fn dropped(&self, table: Table, rows: Rows) -> Vec<Vec<(&'static str, Vec<u8>)>> {
        match table {
            Table::Replays => {
                let keys = (rows.into_iter().map(|(key, _)| key))
                    .filter(|key| self.standing(install_at(key, 1)) != Standing::Kept)
                    .map(|key| (PLUGIN_REPLAYS_KEYSPACE, key));
                vec![keys.collect()]
            }
            Table::Waiting => {
                let keys: Vec<_> = (rows.into_iter().map(|(key, _)| key))
                    .filter(|key| self.standing(install_at(key, 0)) == Standing::Removing)
                    .collect();
                let queue = (keys.iter()).map(|key| (PLUGIN_QUEUE_KEYSPACE, key.clone()));
                let queue = queue.collect();
                let waiting = (keys.into_iter()).map(|key| (PLUGIN_WAITING_KEYSPACE, key));
                vec![queue, waiting.collect()]
            }
            Table::Gaps => {
                let keys = (rows.into_iter()).filter(|(key, value)| {
                    let expired = decode::<GapRow>(value).is_ok_and(|row| {
                        self.now_ms > row.latest_ms.saturating_add(DEDUP_RETENTION_MS)
                    });
                    // A sender row starts with the install id, a receiver row with the sender's
                    // 32 bytes.
                    let at = match key.first() == Some(&RECEIVER_GAP) {
                        true => 33,
                        false => 1,
                    };
                    expired || self.standing(install_at(key, at)) == Standing::Removing
                });
                vec![keys.map(|(key, _)| (PLUGIN_GAPS_KEYSPACE, key)).collect()]
            }
            Table::Creates => {
                let keys = (rows.into_iter())
                    .filter(|(key, _)| self.standing(install_at(key, 0)) != Standing::Kept);
                vec![
                    keys.map(|(key, _)| (PLUGIN_CREATES_KEYSPACE, key))
                        .collect(),
                ]
            }
        }
    }

    /// The next delete, or the next table once every delete was sent.
    fn advance(&mut self, next: Option<Table>) -> Next<usize> {
        if let Some(keys) = self.deletes.pop() {
            self.phase = SweepPhase::Deleting(next);
            return Next::Call(delete_call(keys));
        }
        let Some(table) = next else {
            return Next::Done(self.dropped);
        };
        let scan = table.scan();
        let ask = scan.ask();
        self.phase = SweepPhase::Scanning(table, scan);
        Next::Call(ask)
    }
}

impl Operation for SweepEvents {
    type Output = usize;
    type Error = StoreError;

    fn first(&mut self) -> Result<Next<usize>, StoreError> {
        match &self.phase {
            SweepPhase::Scanning(_, scan) => Ok(Next::Call(scan.ask())),
            SweepPhase::Deleting(_) | SweepPhase::Finished => Err(StoreError("swept twice".into())),
        }
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<usize>, StoreError> {
        match std::mem::replace(&mut self.phase, SweepPhase::Finished) {
            SweepPhase::Finished => Err(StoreError("swept twice".into())),
            SweepPhase::Deleting(next) => {
                done(answer)?;
                Ok(self.advance(next))
            }
            SweepPhase::Scanning(table, mut scan) => {
                let Some(rows) = scan.take(answer)? else {
                    let ask = scan.ask();
                    self.phase = SweepPhase::Scanning(table, scan);
                    return Ok(Next::Call(ask));
                };
                let mut deletes = self.dropped(table, rows);
                self.dropped += deletes.last().map_or(0, Vec::len);
                deletes.reverse();
                self.deletes = deletes;
                Ok(self.advance(table.after()))
            }
        }
    }
}

#[cfg(test)]
#[path = "cleanup_tests.rs"]
mod tests;
