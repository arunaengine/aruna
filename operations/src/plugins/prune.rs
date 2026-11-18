//! Deletes rows that outlived their purpose: secret use audit rows with their index rows after
//! their retention, and webhook delivery ids after theirs or once their install is removing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::{ADMIN_AUDIT_KEYSPACE, PLUGIN_HOOKS_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::plugin_installs::InstallState;
use aruna_core::plugin_runs::{HOOK_RETENTION_MS, HOOK_ROW, HOOK_TIME_ROW};
use aruna_core::structs::identity::admin_audit::{AdminAuditRecord, AuditIndex, audit_prefix};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use ulid::Ulid;

use super::rows::{
    Row, Scan, StoreError, decode, delete_call, done, page_call, page_of, read_call, row, values,
};

/// How long a node keeps the audit rows of the secret uses it served, like finished runs.
pub const USE_AUDIT_RETENTION: u64 = 30 * 24 * 3600 * 1000;
/// Rows of one prune page.
pub const PAGE: usize = 1_000;
/// The realm audit key the use prune checked last, kept in the webhook keyspace. Every older
/// row was pruned or is kept for good, so the prune goes on from there after a restart.
const AUDIT_CURSOR: &[u8] = b"c";

/// One page of at most `page` use audit rows past their retention, deleted with its cursor in
/// one transaction; answers how many records went and whether older rows may follow.
pub(crate) fn prune_uses(now_ms: u64, page: usize) -> PruneUses {
    PruneUses {
        cutoff: now_ms.saturating_sub(USE_AUDIT_RETENTION),
        page,
        phase: UsesPhase::Cursor,
    }
}

#[derive(Debug, PartialEq)]
pub struct PruneUses {
    cutoff: u64,
    page: usize,
    phase: UsesPhase,
}

#[derive(Debug, PartialEq)]
enum UsesPhase {
    Cursor,
    Page,
    Deleting(Vec<Row>, (usize, bool)),
    Writing((usize, bool)),
}

impl Operation for PruneUses {
    type Output = (usize, bool);
    type Error = StoreError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<Self::Output>, StoreError> {
        let cursor = vec![(PLUGIN_HOOKS_KEYSPACE, AUDIT_CURSOR.to_vec())];
        Ok(Next::Call(read_call(cursor)))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<Self::Output>, StoreError> {
        let prefix = audit_prefix(AuditIndex::Realm);
        match std::mem::replace(&mut self.phase, UsesPhase::Writing((0, false))) {
            UsesPhase::Cursor => {
                let [after] = values::<1>(answer)?;
                self.phase = UsesPhase::Page;
                Ok(Next::Call(page_call(
                    ADMIN_AUDIT_KEYSPACE,
                    prefix,
                    after,
                    self.page,
                )))
            }
            UsesPhase::Page => {
                let (rows, next) = page_of(answer)?;
                let (mut dropped, mut count, mut checked, mut young) = (Vec::new(), 0, None, false);
                for (key, value) in rows {
                    let id = (key.get(prefix.len()..)).and_then(|id| <[u8; 16]>::try_from(id).ok());
                    if id.is_some_and(|id| Ulid::from_bytes(id).timestamp_ms() >= self.cutoff) {
                        young = true;
                        break;
                    }
                    let record = AdminAuditRecord::from_bytes(&value).ok();
                    let record = record.filter(|record| record.secret_use.is_some());
                    if let Some(entries) = record.and_then(|record| record.entries().ok()) {
                        let keys = entries.into_iter().map(|(_, key, _)| key.to_vec());
                        dropped.extend(keys.map(|key| (ADMIN_AUDIT_KEYSPACE, key)));
                        count += 1;
                    }
                    checked = Some(key);
                }
                let cursor =
                    checked.map(|key| row(PLUGIN_HOOKS_KEYSPACE, AUDIT_CURSOR.to_vec(), key));
                let outcome = (count, !young && next.is_some());
                self.phase = UsesPhase::Deleting(cursor.into_iter().collect(), outcome);
                Ok(Next::Call(delete_call(dropped)))
            }
            UsesPhase::Deleting(cursor, outcome) => {
                done(answer)?;
                self.phase = UsesPhase::Writing(outcome);
                Ok(Next::Call(IoCall::Write(cursor)))
            }
            UsesPhase::Writing(outcome) => {
                done(answer)?;
                Ok(Next::Done(outcome))
            }
        }
    }
}

/// Deletes the webhook delivery ids past their retention, and every one of a removing
/// install; answers how many went.
pub(crate) fn prune_hooks(config: &RealmConfigDocument, now_ms: u64) -> PruneHooks {
    let mut removing: Vec<Ulid> = (config.installs.iter())
        .filter(|install| install.state == InstallState::Removing)
        .map(|install| install.install_id)
        .collect();
    removing.reverse();
    PruneHooks {
        cutoff: now_ms.saturating_sub(HOOK_RETENTION_MS),
        removing,
        phase: HooksPhase::Times,
        pruned: 0,
    }
}

#[derive(Debug, PartialEq)]
pub struct PruneHooks {
    cutoff: u64,
    /// The removing installs whose delivery ids are still to delete, last first.
    removing: Vec<Ulid>,
    phase: HooksPhase,
    pruned: usize,
}

#[derive(Debug, PartialEq)]
enum HooksPhase {
    /// Reads the oldest time rows, deleted a page at a time with their ids.
    Times,
    /// Deletes a page of old rows; `true` when more old rows may follow.
    DeletingOld(bool),
    Removing(Scan),
    DeletingRemoved,
}

impl PruneHooks {
    fn times() -> IoCall {
        page_call(PLUGIN_HOOKS_KEYSPACE, vec![HOOK_TIME_ROW], None, PAGE)
    }

    /// The scan of the next removing install, or the outcome once none is left.
    fn next_removing(&mut self) -> Next<usize> {
        let Some(install_id) = self.removing.pop() else {
            return Next::Done(self.pruned);
        };
        let prefix = [&[HOOK_ROW][..], &install_id.to_bytes()].concat();
        let scan = Scan::new(PLUGIN_HOOKS_KEYSPACE, prefix);
        let ask = scan.ask();
        self.phase = HooksPhase::Removing(scan);
        Next::Call(ask)
    }
}

impl Operation for PruneHooks {
    type Output = usize;
    type Error = StoreError;

    fn first(&mut self) -> Result<Next<usize>, StoreError> {
        Ok(Next::Call(Self::times()))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<usize>, StoreError> {
        match std::mem::replace(&mut self.phase, HooksPhase::DeletingRemoved) {
            HooksPhase::Times => {
                let (rows, next) = page_of(answer)?;
                let total = rows.len();
                let old: Vec<Vec<u8>> = (rows.into_iter())
                    .map(|(key, _)| key)
                    .take_while(|key| received(key).is_none_or(|at| at < self.cutoff))
                    .collect();
                let more = old.len() == total && next.is_some();
                self.pruned += old.len();
                let ids =
                    (old.iter()).filter_map(|key| Some([&[HOOK_ROW][..], key.get(9..)?].concat()));
                let mut keys: Vec<_> = ids.map(|key| (PLUGIN_HOOKS_KEYSPACE, key)).collect();
                keys.extend(old.into_iter().map(|key| (PLUGIN_HOOKS_KEYSPACE, key)));
                self.phase = HooksPhase::DeletingOld(more);
                Ok(Next::Call(delete_call(keys)))
            }
            HooksPhase::DeletingOld(more) => {
                done(answer)?;
                if more {
                    self.phase = HooksPhase::Times;
                    return Ok(Next::Call(Self::times()));
                }
                Ok(self.next_removing())
            }
            HooksPhase::Removing(mut scan) => {
                let Some(rows) = scan.take(answer)? else {
                    let ask = scan.ask();
                    self.phase = HooksPhase::Removing(scan);
                    return Ok(Next::Call(ask));
                };
                let (mut ids, mut times) = (Vec::new(), Vec::new());
                for (key, value) in rows {
                    if let (Ok(at), Some(rest)) = (decode::<u64>(&value), key.get(1..)) {
                        let time = [&[HOOK_TIME_ROW][..], &at.to_be_bytes(), rest].concat();
                        times.push((PLUGIN_HOOKS_KEYSPACE, time));
                    }
                    ids.push((PLUGIN_HOOKS_KEYSPACE, key));
                }
                self.pruned += ids.len();
                ids.append(&mut times);
                Ok(Next::Call(delete_call(ids)))
            }
            HooksPhase::DeletingRemoved => {
                done(answer)?;
                Ok(self.next_removing())
            }
        }
    }
}

/// The receipt time of a time row.
fn received(key: &[u8]) -> Option<u64> {
    Some(u64::from_be_bytes(key.get(1..9)?.try_into().ok()?))
}

/// Deletes the use audit rows past their retention, a page per transaction; answers how many
/// records went.
#[derive(Debug, PartialEq)]
pub(crate) struct PruneAudit {
    now_ms: u64,
    pruned: usize,
}

impl PruneAudit {
    pub(crate) fn new(now_ms: u64) -> Self {
        Self { now_ms, pruned: 0 }
    }

    fn page(&self) -> IoCall {
        IoCall::child(prune_uses(self.now_ms, PAGE))
    }
}

impl Operation for PruneAudit {
    type Output = usize;
    type Error = StoreError;

    fn first(&mut self) -> Result<Next<usize>, StoreError> {
        Ok(Next::Call(self.page()))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<usize>, StoreError> {
        let (count, more) = answer.child::<(usize, bool), StoreError>()??;
        self.pruned += count;
        Ok(match more {
            true => Next::Call(self.page()),
            false => Next::Done(self.pruned),
        })
    }
}

#[cfg(test)]
#[path = "prune_tests.rs"]
mod tests;
