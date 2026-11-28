//! Atomic firing admission checking lifecycle, conditions, limits, active slots and dedup rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::applet::record::{Applet, Lifecycle as AppletLifecycle, applet_key};
use aruna_core::automation::firing::{Firing, TriggerPayload};
use aruna_core::automation::glob::glob_match;
use aruna_core::automation::keys::{
    active_key, firing_key, index_key, kept_key, record_key, replay_key, schedule_key, seen_key,
};
use aruna_core::automation::record::{Automation, Lifecycle, Trigger};
use aruna_core::automation::rules::{ConditionCheck, LIVE_GRACE_MS, evaluate_conditions};
use aruna_core::automation::usage::{CounterKind, Window, count_u64, counter_key};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::{APPLETS_KEYSPACE, AUTOMATION_RUNS_KEYSPACE, AUTOMATIONS_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::types::{Key, KeySpace, Value};
use aruna_core::user::id::UserId;
use byteview::ByteView;
use ulid::Ulid;

use super::firing_reads::decode_ulid;
use super::{AutoError, active_page, firing_row, schedule_row};

/// Writes a newly created firing along with its active-set (when active) and firing-id index rows.
pub(crate) fn new_firing_rows(firing: &Firing) -> Result<Vec<(KeySpace, Key, Value)>, AutoError> {
    let auto_bytes = ByteView::from(firing.automation_id.to_bytes().to_vec());
    let mut rows = vec![
        firing_row(firing)?,
        (
            AUTOMATION_RUNS_KEYSPACE.to_string(),
            index_key(firing.firing_id),
            auto_bytes.clone(),
        ),
    ];
    if firing.state.is_active() {
        rows.push((
            AUTOMATION_RUNS_KEYSPACE.to_string(),
            active_key(firing.firing_id),
            auto_bytes,
        ));
    }
    Ok(rows)
}

fn replay_value(firing_id: Ulid, now_ms: u64) -> Value {
    let mut bytes = Vec::with_capacity(24);
    bytes.extend_from_slice(&firing_id.to_bytes());
    bytes.extend_from_slice(&now_ms.to_be_bytes());
    ByteView::from(bytes)
}

pub(crate) fn parse_replay(bytes: &[u8]) -> Option<(Ulid, u64)> {
    let firing_id = decode_ulid(bytes.get(..16)?)?;
    let at_ms = bytes
        .get(16..24)
        .and_then(|s| <[u8; 8]>::try_from(s).ok())
        .map(u64::from_be_bytes)
        .unwrap_or(0);
    Some((firing_id, at_ms))
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum AdmitSource {
    Manual,
    Journal {
        source: String,
        matched_revision: u64,
        actor: Option<UserId>,
    },
    Replay {
        event_id: Ulid,
    },
    Schedule {
        slot_ms: u64,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum AdmitOutcome {
    Created(Firing),
    Skipped(Firing),
    Existing(Firing),
    AlreadySeen,
    Unmatched,
    ChainDepth,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PayloadMatch {
    Matched,
    Unmatched,
    ChainDepth,
}

fn match_payload(
    auto: &Automation,
    payload: &TriggerPayload,
    actor: Option<UserId>,
    applet_linked: bool,
) -> PayloadMatch {
    if auto.account.is_some() && actor == auto.account {
        return PayloadMatch::Unmatched;
    }
    match payload {
        TriggerPayload::Manual { .. } => {
            if auto
                .spec
                .triggers
                .iter()
                .any(|t| matches!(t, Trigger::Manual { .. }))
            {
                PayloadMatch::Matched
            } else {
                PayloadMatch::Unmatched
            }
        }
        TriggerPayload::Schedule { .. } => match auto.spec.has_schedules() {
            true => PayloadMatch::Matched,
            false => PayloadMatch::Unmatched,
        },
        TriggerPayload::DataUploaded { bucket, key, .. } => {
            let file = key.rsplit('/').next().unwrap_or(key);
            let hit = auto.spec.triggers.iter().any(|trigger| match trigger {
                Trigger::DataUploaded {
                    bucket: tb,
                    prefix,
                    pattern,
                } => {
                    bucket == tb
                        && key.starts_with(prefix)
                        && pattern.as_deref().is_none_or(|p| glob_match(p, file))
                }
                _ => false,
            });
            match hit {
                true => PayloadMatch::Matched,
                false => PayloadMatch::Unmatched,
            }
        }
        TriggerPayload::AppletSubmission { applet_id, .. } => {
            let hit = auto.spec.triggers.iter().any(|t| match t {
                Trigger::AppletSubmission { applet_id: Some(a) } => a == applet_id,
                Trigger::AppletSubmission { applet_id: None } => applet_linked,
                _ => false,
            });
            match hit {
                true => PayloadMatch::Matched,
                false => PayloadMatch::Unmatched,
            }
        }
        TriggerPayload::RunFinished {
            workflow_id,
            status,
            applet_id,
            depth,
            ..
        } => {
            let hit = auto.spec.triggers.iter().any(|t| match t {
                Trigger::RunFinished {
                    workflow_id: w,
                    status: want_status,
                    applet_id: want_applet,
                } => {
                    w == workflow_id
                        && want_status.is_none_or(|s| s == *status)
                        && want_applet.is_none_or(|a| Some(a) == *applet_id)
                }
                _ => false,
            });
            if !hit {
                PayloadMatch::Unmatched
            } else if *depth >= auto.spec.scope.limits.chain_depth() {
                PayloadMatch::ChainDepth
            } else {
                PayloadMatch::Matched
            }
        }
    }
}

fn limit_check(kind: &str, text: String, why: String) -> ConditionCheck {
    ConditionCheck {
        kind: kind.into(),
        text,
        held: false,
        why,
    }
}

#[derive(Clone, Debug, PartialEq)]
enum AdmitStep {
    Read,
    LoadExisting,
    Active {
        auto: Box<Automation>,
        counters: Vec<(Key, u64)>,
        checks: Vec<ConditionCheck>,
        active_count: u32,
    },
    Write {
        firing: Box<Firing>,
        skipped: bool,
        drop_kept: Option<Key>,
    },
    DropKept(Box<Firing>),
}

/// Atomically verifies lifecycle, dedup/replay state, conditions, rate counters and active slots,
/// then creates the firing (or terminal `Skipped` record) in one write transaction.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AdmitFiringOp {
    auto_id: Ulid,
    firing_id: Ulid,
    payload: TriggerPayload,
    source: AdmitSource,
    now_ms: u64,
    step: AdmitStep,
}

impl AdmitFiringOp {
    pub(crate) fn manual(auto_id: Ulid, firing_id: Ulid, payload: TriggerPayload) -> Self {
        Self::new(auto_id, firing_id, payload, AdmitSource::Manual)
    }

    pub(crate) fn journal(
        auto_id: Ulid,
        firing_id: Ulid,
        source: String,
        matched_revision: u64,
        actor: Option<UserId>,
        payload: TriggerPayload,
    ) -> Self {
        Self::new(
            auto_id,
            firing_id,
            payload,
            AdmitSource::Journal {
                source,
                matched_revision,
                actor,
            },
        )
    }

    pub(crate) fn replay(
        auto_id: Ulid,
        event_id: Ulid,
        firing_id: Ulid,
        payload: TriggerPayload,
    ) -> Self {
        Self::new(
            auto_id,
            firing_id,
            payload,
            AdmitSource::Replay { event_id },
        )
    }

    pub(crate) fn schedule(auto_id: Ulid, firing_id: Ulid, slot_ms: u64) -> Self {
        Self::new(
            auto_id,
            firing_id,
            TriggerPayload::Schedule { at_ms: slot_ms },
            AdmitSource::Schedule { slot_ms },
        )
    }

    fn new(auto_id: Ulid, firing_id: Ulid, payload: TriggerPayload, source: AdmitSource) -> Self {
        Self {
            auto_id,
            firing_id,
            payload,
            source,
            now_ms: 0,
            step: AdmitStep::Read,
        }
    }

    fn counter_keys(&self) -> (Key, Key) {
        let hour = counter_key(
            self.auto_id,
            CounterKind::Firings,
            (Window::Hour, Window::Hour.start(self.now_ms)),
        );
        let day = counter_key(
            self.auto_id,
            CounterKind::Firings,
            (Window::Day, Window::Day.start(self.now_ms)),
        );
        (hour, day)
    }

    fn write_skipped(
        &mut self,
        auto: &Automation,
        reason: String,
        checks: Vec<ConditionCheck>,
    ) -> Result<Next<AdmitOutcome>, AutoError> {
        let firing = Firing::skipped(
            self.firing_id,
            auto.automation_id,
            auto.group_id,
            auto.revision,
            &auto.spec,
            self.payload.clone(),
            reason,
            checks,
            self.now_ms,
        );
        let mut writes = new_firing_rows(&firing)?;
        match &self.source {
            AdmitSource::Manual | AdmitSource::Replay { .. } => {}
            AdmitSource::Journal { source, .. } => {
                writes.push((
                    AUTOMATIONS_KEYSPACE.to_string(),
                    seen_key(auto.automation_id, source),
                    ByteView::from(self.now_ms.to_be_bytes().to_vec()),
                ));
            }
            AdmitSource::Schedule { slot_ms } => {
                writes.push(schedule_row(auto.automation_id, *slot_ms));
            }
        }
        self.step = AdmitStep::Write {
            firing: Box::new(firing),
            skipped: true,
            drop_kept: None,
        };
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

impl Operation for AdmitFiringOp {
    type Output = AdmitOutcome;
    type Error = AutoError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<AdmitOutcome>, AutoError> {
        self.step = AdmitStep::Read;
        let (hour_key, day_key) = self.counter_keys();
        let mut reads = vec![
            (AUTOMATIONS_KEYSPACE.to_string(), record_key(self.auto_id)),
            (AUTOMATIONS_KEYSPACE.to_string(), hour_key),
            (AUTOMATIONS_KEYSPACE.to_string(), day_key),
        ];
        match &self.source {
            AdmitSource::Manual => {}
            AdmitSource::Journal { source, .. } => {
                reads.push((
                    AUTOMATIONS_KEYSPACE.to_string(),
                    seen_key(self.auto_id, source),
                ));
                if let TriggerPayload::AppletSubmission { applet_id, .. } = &self.payload {
                    reads.push((APPLETS_KEYSPACE.to_string(), applet_key(*applet_id)));
                }
            }
            AdmitSource::Replay { event_id } => {
                reads.push((
                    AUTOMATIONS_KEYSPACE.to_string(),
                    replay_key(self.auto_id, *event_id),
                ));
                reads.push((
                    AUTOMATIONS_KEYSPACE.to_string(),
                    kept_key(self.auto_id, *event_id),
                ));
            }
            AdmitSource::Schedule { .. } => {
                reads.push((AUTOMATIONS_KEYSPACE.to_string(), schedule_key(self.auto_id)));
            }
        }
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<AdmitOutcome>, AutoError> {
        match std::mem::replace(&mut self.step, AdmitStep::Read) {
            AdmitStep::Read => {
                let values = result.values()?;
                let auto_bytes = values
                    .first()
                    .and_then(Option::as_ref)
                    .ok_or(AutoError::NotFound)?;
                let auto = Automation::from_bytes(auto_bytes)?;
                if let AdmitSource::Replay { .. } = &self.source
                    && let Some(bytes) = values.get(3).and_then(Option::as_ref)
                    && let Some((existing_id, _)) = parse_replay(bytes)
                {
                    self.step = AdmitStep::LoadExisting;
                    return Ok(Next::Call(IoCall::Read(vec![(
                        AUTOMATION_RUNS_KEYSPACE.to_string(),
                        firing_key(self.auto_id, existing_id),
                    )])));
                }
                if auto.lifecycle != Lifecycle::Active {
                    return Err(AutoError::Conflict(
                        "automation must be active to start".into(),
                    ));
                }
                match &self.source {
                    AdmitSource::Manual => {
                        if !auto
                            .spec
                            .triggers
                            .iter()
                            .any(|t| matches!(t, Trigger::Manual { .. }))
                        {
                            return Err(AutoError::Invalid(
                                "automation has no manual trigger".into(),
                            ));
                        }
                    }
                    AdmitSource::Journal {
                        matched_revision,
                        actor,
                        ..
                    } => {
                        if values.get(3).and_then(Option::as_ref).is_some() {
                            return Ok(Next::Done(AdmitOutcome::AlreadySeen));
                        }
                        let applet_linked = values
                            .get(4)
                            .and_then(Option::as_deref)
                            .and_then(|bytes| Applet::from_bytes(bytes).ok())
                            .is_some_and(|applet| {
                                applet.lifecycle != AppletLifecycle::TakenDown
                                    && applet.group_id == auto.group_id
                                    && applet.automations.contains(&auto.automation_id)
                            });
                        let is_wildcard =
                            auto.spec.triggers.iter().any(|t| {
                                matches!(t, Trigger::AppletSubmission { applet_id: None })
                            });
                        if auto.revision != *matched_revision || is_wildcard {
                            match match_payload(&auto, &self.payload, *actor, applet_linked) {
                                PayloadMatch::Matched => {}
                                PayloadMatch::Unmatched => {
                                    return Ok(Next::Done(AdmitOutcome::Unmatched));
                                }
                                PayloadMatch::ChainDepth => {
                                    return Ok(Next::Done(AdmitOutcome::ChainDepth));
                                }
                            }
                        }
                    }
                    AdmitSource::Replay { .. } => {
                        if values.get(4).and_then(Option::as_ref).is_none() {
                            return Err(AutoError::NotFound);
                        }
                    }
                    AdmitSource::Schedule { slot_ms } => {
                        if !auto.spec.has_schedules() {
                            return Ok(Next::Done(AdmitOutcome::Unmatched));
                        }
                        let last_ms = count_u64(values.get(3).and_then(Option::as_deref));
                        if last_ms >= *slot_ms {
                            return Ok(Next::Done(AdmitOutcome::AlreadySeen));
                        }
                        if self.now_ms.saturating_sub(*slot_ms) > LIVE_GRACE_MS {
                            return self.write_skipped(
                                &auto,
                                "Missed scheduled start while the node was unavailable.".into(),
                                Vec::new(),
                            );
                        }
                    }
                }
                let eval = evaluate_conditions(&auto.spec.conditions, self.now_ms, &self.payload);
                if !eval.ok {
                    let reason = eval
                        .reason
                        .unwrap_or_else(|| "Conditions were not met.".into());
                    return self.write_skipped(&auto, reason, eval.checked);
                }
                let (hour_key, day_key) = self.counter_keys();
                let limits = &auto.spec.scope.limits;
                let hour_cap = limits.hour_limit();
                let day_cap = limits.day_limit();
                let hour_cur = count_u64(values.get(1).and_then(Option::as_deref));
                let day_cur = count_u64(values.get(2).and_then(Option::as_deref));
                let hour_next = hour_cur.saturating_add(1);
                let day_next = day_cur.saturating_add(1);
                if hour_next > u64::from(hour_cap) || day_next > u64::from(day_cap) {
                    if matches!(self.source, AdmitSource::Schedule { .. }) {
                        let (text, why) = if hour_next > u64::from(hour_cap) {
                            (
                                format!("At most {hour_cap} starts per hour"),
                                format!("{hour_cur} starts in this hour already."),
                            )
                        } else {
                            (
                                format!("At most {day_cap} starts per day"),
                                format!("{day_cur} starts today already."),
                            )
                        };
                        let mut checks = eval.checked;
                        let reason = format!("{text}. {why}");
                        checks.push(limit_check("starts", text, why));
                        return self.write_skipped(&auto, reason, checks);
                    }
                    return Err(AutoError::Limited(
                        "automation rate or byte cap reached".into(),
                    ));
                }
                self.step = AdmitStep::Active {
                    auto: Box::new(auto),
                    counters: vec![(hour_key, hour_next), (day_key, day_next)],
                    checks: eval.checked,
                    active_count: 0,
                };
                Ok(Next::Call(active_page(None)))
            }
            AdmitStep::LoadExisting => {
                let bytes = result.value()?.ok_or(AutoError::NotFound)?;
                let firing = Firing::from_bytes(&bytes)?;
                Ok(Next::Done(AdmitOutcome::Existing(firing)))
            }
            AdmitStep::Active {
                auto,
                counters,
                mut checks,
                mut active_count,
            } => {
                let (rows, next) = result.page()?;
                for (_, value) in rows {
                    if decode_ulid(&value) == Some(auto.automation_id) {
                        active_count = active_count.saturating_add(1);
                    }
                }
                if let Some(after) = next {
                    self.step = AdmitStep::Active {
                        auto,
                        counters,
                        checks,
                        active_count,
                    };
                    return Ok(Next::Call(active_page(Some(after))));
                }
                let active_cap = auto.spec.scope.limits.active_firings();
                if active_count >= active_cap {
                    if matches!(self.source, AdmitSource::Schedule { .. }) {
                        let text = if active_cap == 1 {
                            "Only one run at a time".into()
                        } else {
                            format!("At most {active_cap} runs at a time")
                        };
                        let why = if active_cap == 1 {
                            "A run is still going.".into()
                        } else {
                            format!("{active_count} runs are still going.")
                        };
                        let reason = format!("{text}. {why}");
                        checks.push(limit_check("one_at_a_time", text, why));
                        return self.write_skipped(&auto, reason, checks);
                    }
                    return Err(AutoError::Limited(
                        "max_active_firings limit reached".into(),
                    ));
                }
                let firing = Firing::new(
                    self.firing_id,
                    auto.automation_id,
                    auto.group_id,
                    auto.revision,
                    &auto.spec,
                    self.payload.clone(),
                    self.now_ms,
                )
                .with_checks(checks);
                let mut writes = new_firing_rows(&firing)?;
                for (key, next_val) in counters {
                    writes.push((
                        AUTOMATIONS_KEYSPACE.to_string(),
                        key,
                        Value::from(next_val.to_be_bytes().to_vec()),
                    ));
                }
                let mut drop_kept = None;
                match &self.source {
                    AdmitSource::Manual => {}
                    AdmitSource::Journal { source, .. } => {
                        writes.push((
                            AUTOMATIONS_KEYSPACE.to_string(),
                            seen_key(auto.automation_id, source),
                            ByteView::from(self.now_ms.to_be_bytes().to_vec()),
                        ));
                    }
                    AdmitSource::Replay { event_id } => {
                        writes.push((
                            AUTOMATIONS_KEYSPACE.to_string(),
                            replay_key(auto.automation_id, *event_id),
                            replay_value(firing.firing_id, self.now_ms),
                        ));
                        drop_kept = Some(kept_key(auto.automation_id, *event_id));
                    }
                    AdmitSource::Schedule { slot_ms } => {
                        writes.push(schedule_row(auto.automation_id, *slot_ms));
                    }
                }
                self.step = AdmitStep::Write {
                    firing: Box::new(firing),
                    skipped: false,
                    drop_kept,
                };
                Ok(Next::Call(IoCall::Write(writes)))
            }
            AdmitStep::Write {
                firing,
                skipped,
                drop_kept,
            } => {
                result.done()?;
                if let Some(key) = drop_kept {
                    self.step = AdmitStep::DropKept(firing);
                    return Ok(Next::Call(IoCall::Delete(vec![(
                        AUTOMATIONS_KEYSPACE.to_string(),
                        key,
                    )])));
                }
                let out = match skipped {
                    true => AdmitOutcome::Skipped(*firing),
                    false => AdmitOutcome::Created(*firing),
                };
                Ok(Next::Done(out))
            }
            AdmitStep::DropKept(firing) => {
                result.done()?;
                Ok(Next::Done(AdmitOutcome::Created(*firing)))
            }
        }
    }

    fn expected_error(error: &AutoError) -> bool {
        !matches!(error, AutoError::Storage(_))
    }
}
