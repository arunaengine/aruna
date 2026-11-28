//! Atomic firing admission checking lifecycle, triggers, outside-input approval, conditions,
//! chain depth, pinned rate windows, active slots, schedule cursors and dedup rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::applet::record::{Access, Applet, Lifecycle as AppletLifecycle, applet_key};
use aruna_core::automation::firing::{Firing, SkippedFiring, TriggerPayload};
use aruna_core::automation::glob::glob_match;
use aruna_core::automation::keys::{
    active_key, firing_key, index_key, kept_key, record_key, replay_key, schedule_cursor_key,
    seen_key,
};
use aruna_core::automation::record::{Automation, KeptEvent, Lifecycle, Trigger};
use aruna_core::automation::rules::{ConditionCheck, ConditionCheckKind, evaluate_conditions};
use aruna_core::automation::schedule::{ScheduleCursor, schedule_due};
use aruna_core::automation::usage::{CounterKind, Window, count_u64, counter_key};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::{APPLETS_KEYSPACE, AUTOMATION_RUNS_KEYSPACE, AUTOMATIONS_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::types::{Key, KeySpace, Value};
use aruna_core::user::id::UserId;
use byteview::ByteView;
use ulid::Ulid;

use super::firing_reads::decode_ulid;
use super::{AutoError, ScheduleSync, active_page, firing_row, plan_cursor_transition};

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
        .and_then(|slice| <[u8; 8]>::try_from(slice).ok())
        .map(u64::from_be_bytes)
        .unwrap_or(0);
    Some((firing_id, at_ms))
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum AdmitSource {
    Manual,
    Journal {
        source: String,
        actor: Option<UserId>,
    },
    Replay {
        event_id: Ulid,
    },
    Schedule,
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
enum WriteOutcome {
    Created,
    Skipped,
    Idle(IdleOutcome),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IdleOutcome {
    AlreadySeen,
    Unmatched,
}

fn matches_trigger(
    auto: &Automation,
    payload: &TriggerPayload,
    actor: Option<UserId>,
    applet: Option<&Applet>,
) -> bool {
    if auto.account.is_some() && actor == auto.account {
        return false;
    }
    match payload {
        TriggerPayload::Manual { .. } => auto
            .spec
            .triggers
            .iter()
            .any(|trigger| matches!(trigger, Trigger::Manual { .. })),
        TriggerPayload::Schedule { .. } => auto.spec.has_schedules(),
        TriggerPayload::DataUploaded { bucket, key, .. } => {
            let file = key.rsplit('/').next().unwrap_or(key);
            auto.spec.triggers.iter().any(|trigger| match trigger {
                Trigger::DataUploaded {
                    bucket: trigger_bucket,
                    prefix,
                    pattern,
                } => {
                    bucket == trigger_bucket
                        && key.starts_with(prefix)
                        && pattern.as_deref().is_none_or(|glob| glob_match(glob, file))
                }
                _ => false,
            })
        }
        TriggerPayload::AppletSubmission { applet_id, .. } => {
            let linked = applet.is_some_and(|record| {
                record.lifecycle != AppletLifecycle::TakenDown
                    && record.group_id == auto.group_id
                    && record.automations.contains(&auto.automation_id)
            });
            auto.spec.triggers.iter().any(|trigger| match trigger {
                Trigger::AppletSubmission {
                    applet_id: Some(target_id),
                } => target_id == applet_id,
                Trigger::AppletSubmission { applet_id: None } => linked,
                _ => false,
            })
        }
        TriggerPayload::RunFinished {
            workflow_id,
            status,
            applet_id,
            ..
        } => auto.spec.triggers.iter().any(|trigger| match trigger {
            Trigger::RunFinished {
                workflow_id: target_workflow,
                status: want_status,
                applet_id: want_applet,
            } => {
                target_workflow == workflow_id
                    && want_status.is_none_or(|wanted| wanted == *status)
                    && want_applet.is_none_or(|wanted| Some(wanted) == *applet_id)
            }
            _ => false,
        }),
    }
}

fn is_outside_submission(payload: &TriggerPayload, applet: Option<&Applet>) -> bool {
    if payload.outside_input() {
        return true;
    }
    matches!(payload, TriggerPayload::AppletSubmission { .. })
        && applet.is_some_and(|record| record.access == Access::Public || record.guests.is_some())
}

fn limit_check(kind: ConditionCheckKind, text: String, why: String) -> ConditionCheck {
    ConditionCheck {
        kind,
        text,
        held: false,
        why,
    }
}

fn missed_reason(missed_count: u64, first_ms: u64, last_ms: u64) -> String {
    if missed_count == 1 {
        "Missed scheduled start while the node was unavailable.".into()
    } else {
        format!(
            "Missed {missed_count} scheduled starts between {} and {} while the node was unavailable.",
            crate::rfc3339(first_ms),
            crate::rfc3339(last_ms)
        )
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
        schedule_sync: Option<ScheduleSync>,
        active_count: u32,
    },
    Write {
        firing: Option<Box<Firing>>,
        outcome: WriteOutcome,
        deletes: Vec<(KeySpace, Key)>,
    },
    DropRows {
        firing: Option<Box<Firing>>,
        outcome: WriteOutcome,
    },
}

/// Atomically verifies lifecycle, triggers, outside-input approval, dedup/replay state,
/// conditions, chain depth, rate counters, active slots and schedule cursors in one transaction.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AdmitFiringOp {
    auto_id: Ulid,
    firing_id: Ulid,
    payload: TriggerPayload,
    source: AdmitSource,
    now_ms: u64,
    rate_keys: Option<(Key, Key)>,
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
        actor: Option<UserId>,
        payload: TriggerPayload,
    ) -> Self {
        Self::new(
            auto_id,
            firing_id,
            payload,
            AdmitSource::Journal { source, actor },
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

    pub(crate) fn schedule(auto_id: Ulid, firing_id: Ulid) -> Self {
        Self::new(
            auto_id,
            firing_id,
            TriggerPayload::Schedule { at_ms: 0 },
            AdmitSource::Schedule,
        )
    }

    fn new(auto_id: Ulid, firing_id: Ulid, payload: TriggerPayload, source: AdmitSource) -> Self {
        Self {
            auto_id,
            firing_id,
            payload,
            source,
            now_ms: 0,
            rate_keys: None,
            step: AdmitStep::Read,
        }
    }

    fn compute_rate_keys(&self) -> (Key, Key) {
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

    fn finish_write(
        firing: Option<Box<Firing>>,
        outcome: WriteOutcome,
    ) -> Result<Next<AdmitOutcome>, AutoError> {
        match (outcome, firing) {
            (WriteOutcome::Created, Some(record)) => Ok(Next::Done(AdmitOutcome::Created(*record))),
            (WriteOutcome::Skipped, Some(record)) => Ok(Next::Done(AdmitOutcome::Skipped(*record))),
            (WriteOutcome::Idle(IdleOutcome::AlreadySeen), None) => {
                Ok(Next::Done(AdmitOutcome::AlreadySeen))
            }
            (WriteOutcome::Idle(IdleOutcome::Unmatched), None) => {
                Ok(Next::Done(AdmitOutcome::Unmatched))
            }
            _ => Err(IoError::Unfinished.into()),
        }
    }

    fn apply_idle_sync(
        &mut self,
        sync: ScheduleSync,
        idle: IdleOutcome,
    ) -> Result<Next<AdmitOutcome>, AutoError> {
        if !sync.writes.is_empty() {
            self.step = AdmitStep::Write {
                firing: None,
                outcome: WriteOutcome::Idle(idle),
                deletes: sync.deletes,
            };
            return Ok(Next::Call(IoCall::Write(sync.writes)));
        }
        if !sync.deletes.is_empty() {
            self.step = AdmitStep::DropRows {
                firing: None,
                outcome: WriteOutcome::Idle(idle),
            };
            return Ok(Next::Call(IoCall::Delete(sync.deletes)));
        }
        Self::finish_write(None, WriteOutcome::Idle(idle))
    }

    fn write_skipped(
        &mut self,
        auto: &Automation,
        reason: String,
        checks: Vec<ConditionCheck>,
        schedule_sync: Option<ScheduleSync>,
    ) -> Result<Next<AdmitOutcome>, AutoError> {
        let firing = Firing::skipped(SkippedFiring {
            firing_id: self.firing_id,
            automation_id: auto.automation_id,
            group_id: auto.group_id,
            revision: auto.revision,
            spec: &auto.spec,
            trigger: self.payload.clone(),
            reason,
            conditions_checked: checks,
            now_ms: self.now_ms,
        });
        let mut writes = new_firing_rows(&firing)?;
        let mut deletes = Vec::new();
        match &self.source {
            AdmitSource::Manual | AdmitSource::Replay { .. } => {}
            AdmitSource::Journal { source, .. } => {
                writes.push((
                    AUTOMATIONS_KEYSPACE.to_string(),
                    seen_key(auto.automation_id, source),
                    ByteView::from(self.now_ms.to_be_bytes().to_vec()),
                ));
            }
            AdmitSource::Schedule => {
                if let Some(sync) = schedule_sync {
                    writes.extend(sync.writes);
                    deletes.extend(sync.deletes);
                }
            }
        }
        self.step = AdmitStep::Write {
            firing: Some(Box::new(firing)),
            outcome: WriteOutcome::Skipped,
            deletes,
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
        let (hour_key, day_key) = self.compute_rate_keys();
        self.rate_keys = Some((hour_key.clone(), day_key.clone()));
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
                if let TriggerPayload::AppletSubmission { applet_id, .. } = &self.payload {
                    reads.push((APPLETS_KEYSPACE.to_string(), applet_key(*applet_id)));
                }
            }
            AdmitSource::Schedule => {
                reads.push((
                    AUTOMATIONS_KEYSPACE.to_string(),
                    schedule_cursor_key(self.auto_id),
                ));
            }
        }
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<AdmitOutcome>, AutoError> {
        match std::mem::replace(&mut self.step, AdmitStep::Read) {
            AdmitStep::Read => {
                let values = result.values()?;
                let Some(auto_bytes) = values.first().and_then(Option::as_ref) else {
                    if matches!(self.source, AdmitSource::Schedule)
                        && let Some(cursor_bytes) = values.get(3).and_then(Option::as_deref)
                    {
                        let cursor = ScheduleCursor::from_bytes(cursor_bytes)?;
                        let sync = plan_cursor_transition(self.auto_id, Some(&cursor), None)?;
                        return self.apply_idle_sync(sync, IdleOutcome::Unmatched);
                    }
                    return Err(AutoError::NotFound);
                };
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
                if matches!(self.source, AdmitSource::Schedule) {
                    let stored_cursor = values
                        .get(3)
                        .and_then(Option::as_deref)
                        .map(ScheduleCursor::from_bytes)
                        .transpose()?;
                    if auto.lifecycle != Lifecycle::Active || !auto.spec.has_schedules() {
                        let sync =
                            plan_cursor_transition(self.auto_id, stored_cursor.as_ref(), None)?;
                        return self.apply_idle_sync(sync, IdleOutcome::Unmatched);
                    }
                    let Some(cursor) = stored_cursor else {
                        return Ok(Next::Done(AdmitOutcome::AlreadySeen));
                    };
                    let due = schedule_due(&auto.spec, &cursor, self.now_ms);
                    if due.missed_count > 0
                        && let (Some(first_ms), Some(last_ms)) =
                            (due.first_missed_ms, due.last_missed_ms)
                    {
                        self.payload = TriggerPayload::Schedule { at_ms: last_ms };
                        let next_cursor = cursor.advance(&auto.spec, last_ms, self.now_ms);
                        let sync = plan_cursor_transition(
                            auto.automation_id,
                            Some(&cursor),
                            next_cursor.as_ref(),
                        )?;
                        let reason = missed_reason(due.missed_count, first_ms, last_ms);
                        return self.write_skipped(&auto, reason, Vec::new(), Some(sync));
                    }
                    let Some(slot_ms) = due.live_slot_ms else {
                        let refreshed =
                            ScheduleCursor::for_spec(&auto.spec, Some(&cursor), true, self.now_ms);
                        let sync = plan_cursor_transition(
                            auto.automation_id,
                            Some(&cursor),
                            refreshed.as_ref(),
                        )?;
                        return self.apply_idle_sync(sync, IdleOutcome::AlreadySeen);
                    };
                    self.payload = TriggerPayload::Schedule { at_ms: slot_ms };
                    let next_cursor = cursor.advance(&auto.spec, slot_ms, self.now_ms);
                    let schedule_sync = plan_cursor_transition(
                        auto.automation_id,
                        Some(&cursor),
                        next_cursor.as_ref(),
                    )?;
                    return self.evaluate_and_check(auto, values, Some(schedule_sync));
                }

                if auto.lifecycle != Lifecycle::Active {
                    return Err(AutoError::Conflict(
                        "automation must be active to start".into(),
                    ));
                }
                match &self.source {
                    AdmitSource::Manual => {
                        if !matches_trigger(&auto, &self.payload, None, None) {
                            return Err(AutoError::Invalid(
                                "automation has no manual trigger".into(),
                            ));
                        }
                    }
                    AdmitSource::Journal { actor, .. } => {
                        if values.get(3).and_then(Option::as_ref).is_some() {
                            return Ok(Next::Done(AdmitOutcome::AlreadySeen));
                        }
                        let applet = values
                            .get(4)
                            .and_then(Option::as_deref)
                            .map(Applet::from_bytes)
                            .transpose()?;
                        if !matches_trigger(&auto, &self.payload, *actor, applet.as_ref()) {
                            return Ok(Next::Done(AdmitOutcome::Unmatched));
                        }
                        if is_outside_submission(&self.payload, applet.as_ref())
                            && !auto.spec.scope.allow_outside_input
                        {
                            return self.write_skipped(
                                &auto,
                                "Outside input from public or guest applets is not approved on this automation.".into(),
                                Vec::new(),
                                None,
                            );
                        }
                    }
                    AdmitSource::Replay { .. } => {
                        let kept_bytes = values
                            .get(4)
                            .and_then(Option::as_deref)
                            .ok_or(AutoError::NotFound)?;
                        let kept = KeptEvent::from_bytes(kept_bytes)?;
                        self.payload = kept.payload;
                        let applet = values
                            .get(5)
                            .and_then(Option::as_deref)
                            .map(Applet::from_bytes)
                            .transpose()?;
                        if !matches_trigger(&auto, &self.payload, kept.event.actor, applet.as_ref())
                        {
                            return Ok(Next::Done(AdmitOutcome::Unmatched));
                        }
                        if is_outside_submission(&self.payload, applet.as_ref())
                            && !auto.spec.scope.allow_outside_input
                        {
                            return Err(AutoError::Conflict(
                                "outside input from public or guest applets is not approved on this automation".into(),
                            ));
                        }
                    }
                    AdmitSource::Schedule => unreachable!(),
                }
                self.evaluate_and_check(auto, values, None)
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
                schedule_sync,
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
                        schedule_sync,
                        active_count,
                    };
                    return Ok(Next::Call(active_page(Some(after))));
                }
                let active_cap = auto.spec.scope.limits.active_firings();
                if active_count >= active_cap {
                    if matches!(self.source, AdmitSource::Schedule) {
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
                        checks.push(limit_check(ConditionCheckKind::ActiveLimit, text, why));
                        return self.write_skipped(&auto, reason, checks, schedule_sync);
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
                let mut deletes = Vec::new();
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
                        deletes.push((
                            AUTOMATIONS_KEYSPACE.to_string(),
                            kept_key(auto.automation_id, *event_id),
                        ));
                    }
                    AdmitSource::Schedule => {
                        if let Some(sync) = schedule_sync {
                            writes.extend(sync.writes);
                            deletes.extend(sync.deletes);
                        }
                    }
                }
                self.step = AdmitStep::Write {
                    firing: Some(Box::new(firing)),
                    outcome: WriteOutcome::Created,
                    deletes,
                };
                Ok(Next::Call(IoCall::Write(writes)))
            }
            AdmitStep::Write {
                firing,
                outcome,
                deletes,
            } => {
                result.done()?;
                if !deletes.is_empty() {
                    self.step = AdmitStep::DropRows { firing, outcome };
                    return Ok(Next::Call(IoCall::Delete(deletes)));
                }
                Self::finish_write(firing, outcome)
            }
            AdmitStep::DropRows { firing, outcome } => {
                result.done()?;
                Self::finish_write(firing, outcome)
            }
        }
    }

    fn expected_error(error: &AutoError) -> bool {
        !matches!(error, AutoError::Storage(_))
    }
}

impl AdmitFiringOp {
    fn evaluate_and_check(
        &mut self,
        auto: Automation,
        values: Vec<Option<Value>>,
        schedule_sync: Option<ScheduleSync>,
    ) -> Result<Next<AdmitOutcome>, AutoError> {
        let evaluation = evaluate_conditions(&auto.spec.conditions, self.now_ms, &self.payload);
        if !evaluation.ok {
            let reason = evaluation
                .reason
                .unwrap_or_else(|| "Conditions were not met.".into());
            return self.write_skipped(&auto, reason, evaluation.checked, schedule_sync);
        }
        if matches!(self.payload, TriggerPayload::RunFinished { .. })
            && self.payload.chain_depth() >= auto.spec.scope.limits.chain_depth()
        {
            return Ok(Next::Done(AdmitOutcome::ChainDepth));
        }
        let (hour_key, day_key) = self
            .rate_keys
            .clone()
            .unwrap_or_else(|| self.compute_rate_keys());
        let limits = &auto.spec.scope.limits;
        let hour_cap = limits.hour_limit();
        let day_cap = limits.day_limit();
        let hour_count = count_u64(values.get(1).and_then(Option::as_deref));
        let day_count = count_u64(values.get(2).and_then(Option::as_deref));
        let hour_next = hour_count.saturating_add(1);
        let day_next = day_count.saturating_add(1);
        if hour_next > u64::from(hour_cap) || day_next > u64::from(day_cap) {
            if matches!(self.source, AdmitSource::Schedule) {
                let (text, why) = if hour_next > u64::from(hour_cap) {
                    (
                        format!("At most {hour_cap} starts per hour"),
                        format!("{hour_count} starts in this hour already."),
                    )
                } else {
                    (
                        format!("At most {day_cap} starts per day"),
                        format!("{day_count} starts today already."),
                    )
                };
                let mut checks = evaluation.checked;
                let reason = format!("{text}. {why}");
                checks.push(limit_check(ConditionCheckKind::Starts, text, why));
                return self.write_skipped(&auto, reason, checks, schedule_sync);
            }
            return Err(AutoError::Limited(
                "automation rate or byte cap reached".into(),
            ));
        }
        self.step = AdmitStep::Active {
            auto: Box::new(auto),
            counters: vec![(hour_key, hour_next), (day_key, day_next)],
            checks: evaluation.checked,
            schedule_sync,
            active_count: 0,
        };
        Ok(Next::Call(active_page(None)))
    }
}
