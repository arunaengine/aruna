//! Periodic automation upkeep: prunes expired counters, dedup rows, old kept events and repairs
//! automation service accounts when they drift from their records.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::automation::keys::{KEPT_ROW, REPLAY_ROW, SEEN_ROW, USAGE_ROW, record_prefix};
use aruna_core::automation::record::{Automation, KeptEvent};
use aruna_core::automation::usage::{count_u64, counter_ended};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::AUTOMATIONS_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::Key;

use super::AutoError;
use super::identity::SyncIdentityOperation;
use super::start::parse_replay;

const RETENTION_MS: u64 = 30 * 24 * 60 * 60 * 1000;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UpkeepReport {
    pub pruned_counters: usize,
    pub pruned_seen: usize,
    pub pruned_kept: usize,
    pub repaired: usize,
}

#[derive(Debug, PartialEq)]
enum UpkeepPhase {
    Counters(Option<Key>),
    Seen(Option<Key>),
    Kept(Option<Key>),
    Replay(Option<Key>),
    DeleteBatch(Box<UpkeepPhase>),
    Records(Option<Key>),
    Sync,
}

/// Sweeps expired automation rows and repairs any drifted automation service accounts.
#[derive(Debug, PartialEq)]
pub struct AutoUpkeepOperation {
    local: (NodeId, RealmId),
    now_ms: u64,
    repair: Vec<Automation>,
    report: UpkeepReport,
    phase: UpkeepPhase,
}

impl AutoUpkeepOperation {
    pub fn new(local: (NodeId, RealmId), now_ms: u64) -> Self {
        Self {
            local,
            now_ms,
            repair: Vec::new(),
            report: UpkeepReport::default(),
            phase: UpkeepPhase::Counters(None),
        }
    }

    fn page(prefix: Key, after: Option<Key>) -> Next<UpkeepReport> {
        Next::Call(IoCall::Page {
            space: AUTOMATIONS_KEYSPACE.to_string(),
            prefix,
            after,
            limit: 256,
        })
    }

    fn next_repair(&mut self) -> Next<UpkeepReport> {
        if let Some(auto) = self.repair.pop() {
            self.phase = UpkeepPhase::Sync;
            return Next::Call(IoCall::child(SyncIdentityOperation::new(&auto, self.local)));
        }
        Next::Done(self.report)
    }

    fn continue_phase(&mut self, next: UpkeepPhase) -> Next<UpkeepReport> {
        match next {
            UpkeepPhase::Counters(after) => {
                self.phase = UpkeepPhase::Counters(after.clone());
                Self::page(Key::from(vec![USAGE_ROW]), after)
            }
            UpkeepPhase::Seen(after) => {
                self.phase = UpkeepPhase::Seen(after.clone());
                Self::page(Key::from(vec![SEEN_ROW]), after)
            }
            UpkeepPhase::Kept(after) => {
                self.phase = UpkeepPhase::Kept(after.clone());
                Self::page(Key::from(vec![KEPT_ROW]), after)
            }
            UpkeepPhase::Replay(after) => {
                self.phase = UpkeepPhase::Replay(after.clone());
                Self::page(Key::from(vec![REPLAY_ROW]), after)
            }
            UpkeepPhase::Records(after) => {
                self.phase = UpkeepPhase::Records(after.clone());
                Self::page(record_prefix(), after)
            }
            _ => self.next_repair(),
        }
    }
}

impl Operation for AutoUpkeepOperation {
    type Output = UpkeepReport;
    type Error = AutoError;

    fn first(&mut self) -> Result<Next<UpkeepReport>, AutoError> {
        Ok(Self::page(Key::from(vec![USAGE_ROW]), None))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<UpkeepReport>, AutoError> {
        let cutoff = self.now_ms.saturating_sub(RETENTION_MS);
        match std::mem::replace(&mut self.phase, UpkeepPhase::Sync) {
            UpkeepPhase::Counters(_) => {
                let (rows, next) = result.page()?;
                let dead: Vec<_> = rows
                    .into_iter()
                    .filter(|(k, _)| counter_ended(k, self.now_ms))
                    .map(|(k, _)| (AUTOMATIONS_KEYSPACE.to_string(), k))
                    .collect();
                let follow = match next {
                    Some(after) => UpkeepPhase::Counters(Some(after)),
                    None => UpkeepPhase::Seen(None),
                };
                if !dead.is_empty() {
                    self.report.pruned_counters += dead.len();
                    self.phase = UpkeepPhase::DeleteBatch(Box::new(follow));
                    return Ok(Next::Call(IoCall::Delete(dead)));
                }
                Ok(self.continue_phase(follow))
            }
            UpkeepPhase::Seen(_) => {
                let (rows, next) = result.page()?;
                let dead: Vec<_> = rows
                    .into_iter()
                    .filter(|(_, v)| count_u64(Some(v)) < cutoff)
                    .map(|(k, _)| (AUTOMATIONS_KEYSPACE.to_string(), k))
                    .collect();
                let follow = match next {
                    Some(after) => UpkeepPhase::Seen(Some(after)),
                    None => UpkeepPhase::Kept(None),
                };
                if !dead.is_empty() {
                    self.report.pruned_seen += dead.len();
                    self.phase = UpkeepPhase::DeleteBatch(Box::new(follow));
                    return Ok(Next::Call(IoCall::Delete(dead)));
                }
                Ok(self.continue_phase(follow))
            }
            UpkeepPhase::Kept(_) => {
                let (rows, next) = result.page()?;
                let dead: Vec<_> = rows
                    .into_iter()
                    .filter(|(_, v)| {
                        KeptEvent::from_bytes(v).is_ok_and(|ev| ev.kept_at_ms < cutoff)
                    })
                    .map(|(k, _)| (AUTOMATIONS_KEYSPACE.to_string(), k))
                    .collect();
                let follow = match next {
                    Some(after) => UpkeepPhase::Kept(Some(after)),
                    None => UpkeepPhase::Replay(None),
                };
                if !dead.is_empty() {
                    self.report.pruned_kept += dead.len();
                    self.phase = UpkeepPhase::DeleteBatch(Box::new(follow));
                    return Ok(Next::Call(IoCall::Delete(dead)));
                }
                Ok(self.continue_phase(follow))
            }
            UpkeepPhase::Replay(_) => {
                let (rows, next) = result.page()?;
                let dead: Vec<_> = rows
                    .into_iter()
                    .filter(|(_, v)| parse_replay(v).is_some_and(|(_, at)| at < cutoff))
                    .map(|(k, _)| (AUTOMATIONS_KEYSPACE.to_string(), k))
                    .collect();
                let follow = match next {
                    Some(after) => UpkeepPhase::Replay(Some(after)),
                    None => UpkeepPhase::Records(None),
                };
                if !dead.is_empty() {
                    self.report.pruned_seen += dead.len();
                    self.phase = UpkeepPhase::DeleteBatch(Box::new(follow));
                    return Ok(Next::Call(IoCall::Delete(dead)));
                }
                Ok(self.continue_phase(follow))
            }
            UpkeepPhase::DeleteBatch(follow) => {
                result.done()?;
                Ok(self.continue_phase(*follow))
            }
            UpkeepPhase::Records(_) => {
                let (rows, next) = result.page()?;
                for (_, value) in rows {
                    let auto = Automation::from_bytes(&value)?;
                    if auto.needs_identity_sync() && auto.granter().is_some() {
                        self.repair.push(auto);
                    }
                }
                match next {
                    Some(after) => Ok(self.continue_phase(UpkeepPhase::Records(Some(after)))),
                    None => Ok(self.next_repair()),
                }
            }
            UpkeepPhase::Sync => {
                if result.child::<Automation, AutoError>()?.is_ok() {
                    self.report.repaired += 1;
                }
                Ok(self.next_repair())
            }
        }
    }
}
