//! Schedule trigger worker firing due slots and recording missed slots while unavailable.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::automation::record::{Automation, Lifecycle};
use aruna_core::automation::rules::schedule_due_slots;
use aruna_core::automation::usage::count_u64;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id_mint::IdMint;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::features::{Feature, resolve};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::types::GroupId;
use aruna_ops_identity::document_repository::{GetConfigError, GetConfigOperation};
use ulid::Ulid;

use super::admit::{AdmitFiringOp, AdmitOutcome};
use super::firing_reads::decode_ulid;
use super::{AutoError, auto_answer, read_auto, schedule_page};

/// Maximum due or missed schedule slots processed for one automation per worker tick.
const SLOTS_PER_TICK: usize = 16;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TimerReport {
    pub checked: usize,
    pub fired: usize,
    pub skipped: usize,
}

#[derive(Debug, PartialEq)]
enum TimerStep {
    Config,
    Index,
    LoadAutomation(u64),
    AdmitSlot(Box<Automation>),
}

/// Evaluates active scheduled automations via the `SCHEDULE_ROW` index, firing live slots and
/// recording missed slots as `skipped` without retroactively making them up.
#[derive(Debug, PartialEq)]
pub struct TickTimersOperation {
    realm_id: RealmId,
    disabled_features: BTreeSet<Feature>,
    ids: IdMint,
    now_ms: u64,
    config: Option<RealmConfigDocument>,
    entries: Vec<(Ulid, u64)>,
    slots: Vec<u64>,
    report: TimerReport,
    step: TimerStep,
}

impl TickTimersOperation {
    pub fn new(realm_id: RealmId) -> Self {
        Self {
            realm_id,
            disabled_features: BTreeSet::new(),
            ids: IdMint::default(),
            now_ms: 0,
            config: None,
            entries: Vec::new(),
            slots: Vec::new(),
            report: TimerReport::default(),
            step: TimerStep::Config,
        }
    }

    pub fn with_disabled_features(mut self, disabled: BTreeSet<Feature>) -> Self {
        self.disabled_features = disabled;
        self
    }

    fn feature_on(&self, group_id: GroupId) -> bool {
        self.config.as_ref().is_some_and(|config| {
            resolve(
                config,
                &self.disabled_features,
                Feature::Automations,
                Some(&group_id),
            )
            .on
        })
    }

    fn next_entry(&mut self) -> Next<TimerReport> {
        let Some((automation_id, last_ms)) = self.entries.pop() else {
            return Next::Done(self.report);
        };
        self.report.checked += 1;
        self.step = TimerStep::LoadAutomation(last_ms);
        Next::Call(read_auto(automation_id))
    }

    fn next_slot(&mut self, automation: Box<Automation>) -> Next<TimerReport> {
        let Some(slot_ms) = self.slots.pop() else {
            return self.next_entry();
        };
        let automation_id = automation.automation_id;
        let firing_id = self.ids.mint(self.now_ms);
        let admit = AdmitFiringOp::schedule(automation_id, firing_id, slot_ms);
        self.step = TimerStep::AdmitSlot(automation);
        Next::Call(IoCall::retry(admit, &automation_id.to_bytes()))
    }
}

impl Operation for TickTimersOperation {
    type Output = TimerReport;
    type Error = AutoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn seed(&mut self, seed: u128) {
        self.ids = IdMint::new(seed);
    }

    fn first(&mut self) -> Result<Next<TimerReport>, AutoError> {
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(
            self.realm_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<TimerReport>, AutoError> {
        match std::mem::replace(&mut self.step, TimerStep::Config) {
            TimerStep::Config => {
                self.config = result.child::<RealmConfigDocument, GetConfigError>()?.ok();
                self.step = TimerStep::Index;
                Ok(Next::Call(schedule_page(None)))
            }
            TimerStep::Index => {
                let (rows, next) = result.page()?;
                for (key, value) in rows {
                    if let Some(automation_id) = key.get(1..17).and_then(decode_ulid) {
                        self.entries.push((automation_id, count_u64(Some(&value))));
                    }
                }
                if let Some(after) = next {
                    self.step = TimerStep::Index;
                    return Ok(Next::Call(schedule_page(Some(after))));
                }
                Ok(self.next_entry())
            }
            TimerStep::LoadAutomation(last_ms) => {
                let Some(automation) = auto_answer(result)? else {
                    return Ok(self.next_entry());
                };
                if automation.lifecycle != Lifecycle::Active
                    || !automation.spec.has_schedules()
                    || !self.feature_on(automation.group_id)
                {
                    return Ok(self.next_entry());
                }
                let mut due = schedule_due_slots(&automation.spec, last_ms, self.now_ms);
                if due.len() > SLOTS_PER_TICK {
                    let drop = due.len() - SLOTS_PER_TICK;
                    due.drain(..drop);
                }
                due.reverse();
                self.slots = due;
                Ok(self.next_slot(Box::new(automation)))
            }
            TimerStep::AdmitSlot(automation) => {
                match result.child::<AdmitOutcome, AutoError>()? {
                    Ok(AdmitOutcome::Created(_)) => self.report.fired += 1,
                    Ok(AdmitOutcome::Skipped(_)) => self.report.skipped += 1,
                    Ok(_) | Err(AutoError::Conflict(_)) | Err(AutoError::NotFound) => {
                        self.slots.clear();
                        return Ok(self.next_entry());
                    }
                    Err(error) => return Err(error),
                }
                Ok(self.next_slot(automation))
            }
        }
    }
}

#[cfg(test)]
#[path = "timer_tests.rs"]
mod tests;
