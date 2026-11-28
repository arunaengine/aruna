//! Due-time-indexed schedule trigger worker firing live slots and summarizing missed ranges.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::automation::firing::TriggerPayload;
use aruna_core::automation::keys::parse_due_key;
use aruna_core::automation::schedule::next_start;
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
use super::{AutoError, schedule_due_page};

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
    AdmitSlot {
        automation_id: Ulid,
        previous_at_ms: Option<u64>,
    },
}

/// Evaluates due scheduled automations via the due-time-ordered `SCHEDULE_DUE_ROW` index,
/// stopping at the first future key and skipping disabled groups without reading records.
#[derive(Debug, PartialEq)]
pub struct TickTimersOperation {
    realm_id: RealmId,
    disabled_features: BTreeSet<Feature>,
    ids: IdMint,
    now_ms: u64,
    config: Option<RealmConfigDocument>,
    seen_ids: BTreeSet<Ulid>,
    due_ids: Vec<Ulid>,
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
            seen_ids: BTreeSet::new(),
            due_ids: Vec::new(),
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
        let Some(automation_id) = self.due_ids.pop() else {
            return Next::Done(self.report);
        };
        self.report.checked += 1;
        self.admit_slot(automation_id, None)
    }

    fn admit_slot(
        &mut self,
        automation_id: Ulid,
        previous_at_ms: Option<u64>,
    ) -> Next<TimerReport> {
        let firing_id = self.ids.mint(self.now_ms);
        let admit = AdmitFiringOp::schedule(automation_id, firing_id, self.now_ms);
        self.step = TimerStep::AdmitSlot {
            automation_id,
            previous_at_ms,
        };
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
        self.step = TimerStep::Config;
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(
            self.realm_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<TimerReport>, AutoError> {
        match std::mem::replace(&mut self.step, TimerStep::Config) {
            TimerStep::Config => {
                let config = result
                    .child::<RealmConfigDocument, GetConfigError>()?
                    .map_err(|error| AutoError::Storage(error.to_string()))?;
                self.config = Some(config);
                self.step = TimerStep::Index;
                Ok(Next::Call(schedule_due_page(None)))
            }
            TimerStep::Index => {
                let (rows, next) = result.page()?;
                let mut reached_future = false;
                for (key, value) in rows {
                    let Some(entry) = parse_due_key(&key) else {
                        continue;
                    };
                    if entry.due_ms > self.now_ms {
                        reached_future = true;
                        break;
                    }
                    if decode_ulid(&value).is_some_and(|group| !self.feature_on(group)) {
                        continue;
                    }
                    if self.seen_ids.insert(entry.automation_id) {
                        self.due_ids.push(entry.automation_id);
                    }
                }
                if !reached_future && let Some(after) = next {
                    self.step = TimerStep::Index;
                    return Ok(Next::Call(schedule_due_page(Some(after))));
                }
                self.due_ids.reverse();
                Ok(self.next_entry())
            }
            TimerStep::AdmitSlot {
                automation_id,
                previous_at_ms,
            } => {
                let firing = match result.child::<AdmitOutcome, AutoError>()? {
                    Ok(AdmitOutcome::Created(firing)) => {
                        self.report.fired += 1;
                        firing
                    }
                    Ok(AdmitOutcome::Skipped(firing)) => {
                        self.report.skipped += 1;
                        firing
                    }
                    Ok(_) | Err(AutoError::Conflict(_)) | Err(AutoError::NotFound) => {
                        return Ok(self.next_entry());
                    }
                    Err(error) => return Err(error),
                };
                if let TriggerPayload::Schedule { at_ms } = &firing.trigger
                    && previous_at_ms.is_none_or(|previous| *at_ms > previous)
                    && next_start(&firing.spec, *at_ms).is_some_and(|due| due <= self.now_ms)
                {
                    return Ok(self.admit_slot(automation_id, Some(*at_ms)));
                }
                Ok(self.next_entry())
            }
        }
    }
}

#[cfg(test)]
#[path = "timer_tests.rs"]
mod tests;
