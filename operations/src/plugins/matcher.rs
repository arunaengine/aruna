//! Matches this node's pending journal rows to the active installs with event triggers and
//! queues forward rows for their receivers, or counts a gap where a row cannot be queued.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use aruna_core::NodeId;
use aruna_core::keyspaces::{EVENT_JOURNAL_KEYSPACE, PLUGIN_GAPS_KEYSPACE, PLUGIN_OUTBOX_KEYSPACE};
use aruna_core::plugin_installs::InstallState;
use aruna_core::plugin_runs::{
    EVENTS_QUEUED, ForwardRow, GapReason, GapRow, JOURNAL_RETENTION_MS, JOURNAL_ROW, PENDING_ROW,
    SourceEvent, SourceKind, gap_key, outbox_key,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
use aruna_core::types::GroupId;
use aruna_plugins::trigger::TriggerKind;
use ulid::Ulid;

use super::check::covers;
use super::dispatch::event_trigger;
use super::grants::parse_grants;
use super::install::{pick_version, read_manifest};
use super::journal::JOURNAL_PAGE;
use super::lifecycle::coverage;
use super::store::{StoreError, Txn, decode, encode, transact};
use crate::driver::DriverContext;
use crate::realm::mutate_placement::node_kind;

/// An active install with event triggers, as the matcher sees it.
pub struct Target {
    pub(crate) install_id: Ulid,
    pub(crate) group_id: GroupId,
    pub(crate) revision: u64,
    pub(crate) coverage: BTreeMap<TriggerKind, BTreeSet<String>>,
}

impl Target {
    /// Whether the install's trigger of the event's kind covers the event's path.
    pub(crate) fn matches(&self, event: &SourceEvent) -> bool {
        self.group_id == event.group_id
            && (self.coverage.get(&event_trigger(event.kind)))
                .is_some_and(|patterns| patterns.iter().any(|pattern| covers(pattern, &event.path)))
    }
}

/// The active installs whose manifest declares event triggers, with their coverage.
pub fn targets(config: &RealmConfigDocument) -> Vec<Target> {
    (config.installs.iter())
        .filter(|install| install.state == InstallState::Active)
        .filter_map(|install| {
            let plugin = pick_version(config, &install.plugin, Some(&install.digest)).ok()?;
            let manifest = read_manifest(&plugin).ok()?;
            let coverage = coverage(&manifest, &parse_grants(&install.grants));
            (!coverage.is_empty()).then_some(Target {
                install_id: install.install_id,
                group_id: install.group_id,
                revision: install.revision,
                coverage,
            })
        })
        .collect()
}

/// Matches every pending row of this node; returns how many rows were read. A user node keeps
/// no journal: its rows are deleted unmatched.
pub async fn match_pending(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    local: NodeId,
    now_ms: u64,
) -> Result<usize, StoreError> {
    match_bounded(ctx, config, local, now_ms, EVENTS_QUEUED).await
}

pub(crate) async fn match_bounded(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    local: NodeId,
    now_ms: u64,
    bound: usize,
) -> Result<usize, StoreError> {
    let (first, _) = (Txn::direct(ctx))
        .page(EVENT_JOURNAL_KEYSPACE, &[PENDING_ROW], None, 1)
        .await?;
    if first.is_empty() {
        return Ok(0);
    }
    let keep = !matches!(node_kind(config, local), Some(RealmNodeKind::User { .. }));
    let targets = match keep {
        true => targets(config),
        false => Vec::new(),
    };
    let mut read = 0;
    loop {
        let page = transact(ctx, async |txn: &Txn<'_>| {
            match_page(txn, &targets, keep, now_ms, bound).await
        })
        .await?;
        read += page;
        if page < JOURNAL_PAGE {
            return Ok(read);
        }
    }
}

/// One page of pending rows in one transaction: the forward rows and gaps of the targets each
/// row matches, and the pending rows deleted.
async fn match_page(
    txn: &Txn<'_>,
    targets: &[Target],
    keep: bool,
    now_ms: u64,
    bound: usize,
) -> Result<usize, StoreError> {
    let (rows, _) = (txn.page(EVENT_JOURNAL_KEYSPACE, &[PENDING_ROW], None, JOURNAL_PAGE)).await?;
    let stale = now_ms.saturating_sub(JOURNAL_RETENTION_MS);
    let (mut done, mut outbox) = (Vec::new(), Outbox::new(now_ms));
    for (key, value) in &rows {
        done.push(key.clone());
        if !keep {
            done.push([&[JOURNAL_ROW][..], &key[1..]].concat());
            continue;
        }
        let Ok(event) = SourceEvent::from_bytes(value) else {
            continue;
        };
        for target in targets.iter().filter(|target| target.matches(&event)) {
            let install = (target.install_id, target.revision);
            let stale = event.journaled_ms < stale;
            outbox
                .offer(txn, install, key, &event, stale, bound)
                .await?;
        }
    }
    outbox.commit(txn).await?;
    txn.delete(EVENT_JOURNAL_KEYSPACE, done).await?;
    Ok(rows.len())
}

/// The forward rows and gap counts of one transaction, with each install's queued count.
pub(crate) struct Outbox {
    now_ms: u64,
    forward: Vec<(Vec<u8>, Vec<u8>)>,
    queued: BTreeMap<Ulid, usize>,
    gaps: BTreeMap<Vec<u8>, GapRow>,
}

impl Outbox {
    pub(crate) fn new(now_ms: u64) -> Self {
        Self {
            now_ms,
            forward: Vec::new(),
            queued: BTreeMap::new(),
            gaps: BTreeMap::new(),
        }
    }

    /// Queues the event of the journal row `key` for the install at `revision`, or counts a gap
    /// when the row is stale or the install has `bound` rows queued.
    pub(crate) async fn offer(
        &mut self,
        txn: &Txn<'_>,
        (install_id, revision): (Ulid, u64),
        key: &[u8],
        event: &SourceEvent,
        stale: bool,
        bound: usize,
    ) -> Result<(), StoreError> {
        let count = match self.queued.get(&install_id) {
            Some(count) => *count,
            None => (txn.page(PLUGIN_OUTBOX_KEYSPACE, &install_id.to_bytes(), None, bound))
                .await?
                .0
                .len(),
        };
        self.queued.insert(install_id, count);
        let reason = match () {
            () if stale => GapReason::Pruned,
            () if count >= bound => GapReason::Full,
            () => {
                let row = ForwardRow {
                    install_id,
                    revision,
                    event: event.clone(),
                    queued_ms: self.now_ms,
                };
                self.forward
                    .push((outbox_key(install_id, key), encode(&row)?));
                self.queued.insert(install_id, count + 1);
                return Ok(());
            }
        };
        self.gap(txn, install_id, (event.kind, event.anchor_ms), reason)
            .await
    }

    /// Counts one undelivered event of `kind` with the anchor `anchor_ms`.
    pub(crate) async fn gap(
        &mut self,
        txn: &Txn<'_>,
        install_id: Ulid,
        (kind, anchor_ms): (SourceKind, u64),
        reason: GapReason,
    ) -> Result<(), StoreError> {
        let key = gap_key(install_id, kind, reason);
        if !self.gaps.contains_key(&key) {
            let stored = match txn.read(PLUGIN_GAPS_KEYSPACE, &key).await? {
                Some(bytes) => decode(&bytes)?,
                None => GapRow::new(kind, reason),
            };
            self.gaps.insert(key.clone(), stored);
        }
        if let Some(row) = self.gaps.get_mut(&key) {
            row.add(anchor_ms);
        }
        Ok(())
    }

    pub(crate) async fn commit(self, txn: &Txn<'_>) -> Result<(), StoreError> {
        txn.write(PLUGIN_OUTBOX_KEYSPACE, self.forward).await?;
        let gaps = (self.gaps.into_iter())
            .map(|(key, row)| Ok((key, encode(&row)?)))
            .collect::<Result<_, StoreError>>()?;
        txn.write(PLUGIN_GAPS_KEYSPACE, gaps).await
    }
}

#[cfg(test)]
#[path = "matcher_tests.rs"]
pub(crate) mod tests;
