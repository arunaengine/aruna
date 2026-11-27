//! Atomic firing admission checking lifecycle, rate limits, active slots and dedup/replay rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::automation::firing::{Firing, TriggerPayload};
use aruna_core::automation::glob::glob_match;
use aruna_core::automation::keys::{
    active_key, firing_key, index_key, kept_key, record_key, replay_key, seen_key,
};
use aruna_core::automation::record::{Automation, Lifecycle, Trigger};
use aruna_core::automation::usage::{CounterKind, Window, count_u64, counter_key};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::{AUTOMATION_RUNS_KEYSPACE, AUTOMATIONS_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::types::{Key, KeySpace, Value};
use aruna_core::user::id::UserId;
use byteview::ByteView;
use ulid::Ulid;

use super::firing_reads::decode_ulid;
use super::{AutoError, active_page, firing_row};

/// Writes a newly created firing along with its active-set and firing-id index rows.
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
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum AdmitOutcome {
    Created(Firing),
    Existing(Firing),
    AlreadySeen,
    Unmatched,
}

fn matches_payload(auto: &Automation, payload: &TriggerPayload, actor: Option<UserId>) -> bool {
    if auto.account.is_some() && actor == auto.account {
        return false;
    }
    match payload {
        TriggerPayload::Manual { .. } => auto
            .spec
            .triggers
            .iter()
            .any(|t| matches!(t, Trigger::Manual { .. })),
        TriggerPayload::DataUploaded { bucket, key, .. } => {
            let file = key.rsplit('/').next().unwrap_or(key);
            auto.spec.triggers.iter().any(|trigger| match trigger {
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
            })
        }
        TriggerPayload::AppletSubmission { applet_id, .. } => auto
            .spec
            .triggers
            .iter()
            .any(|t| matches!(t, Trigger::AppletSubmission { applet_id: a } if a == applet_id)),
        TriggerPayload::RunFinished {
            workflow_id,
            status,
            applet_id,
            depth,
            ..
        } => {
            *depth < auto.spec.scope.limits.max_chain_depth
                && auto.spec.triggers.iter().any(|t| match t {
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
                })
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum AdmitStep {
    Read,
    LoadExisting,
    Active {
        auto: Box<Automation>,
        counters: Vec<(Key, u64)>,
        active_count: u32,
    },
    Write {
        firing: Box<Firing>,
        drop_kept: Option<Key>,
    },
    DropKept(Box<Firing>),
}

/// Atomically verifies lifecycle, dedup/replay state, rate counters and `max_active_firings`, then
/// creates the firing and claims its slot and kept/seen row in one write transaction.
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
                        if auto.revision != *matched_revision
                            && !matches_payload(&auto, &self.payload, *actor)
                        {
                            return Ok(Next::Done(AdmitOutcome::Unmatched));
                        }
                    }
                    AdmitSource::Replay { .. } => {
                        if values.get(4).and_then(Option::as_ref).is_none() {
                            return Err(AutoError::NotFound);
                        }
                    }
                }
                let (hour_key, day_key) = self.counter_keys();
                let limits = &auto.spec.scope.limits;
                let hour_next =
                    count_u64(values.get(1).and_then(Option::as_deref)).saturating_add(1);
                let day_next =
                    count_u64(values.get(2).and_then(Option::as_deref)).saturating_add(1);
                if hour_next > u64::from(limits.per_hour) || day_next > u64::from(limits.per_day) {
                    return Err(AutoError::Limited(
                        "automation rate or byte cap reached".into(),
                    ));
                }
                self.step = AdmitStep::Active {
                    auto: Box::new(auto),
                    counters: vec![(hour_key, hour_next), (day_key, day_next)],
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
                        active_count,
                    };
                    return Ok(Next::Call(active_page(Some(after))));
                }
                if active_count >= auto.spec.scope.limits.max_active_firings {
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
                );
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
                }
                self.step = AdmitStep::Write {
                    firing: Box::new(firing),
                    drop_kept,
                };
                Ok(Next::Call(IoCall::Write(writes)))
            }
            AdmitStep::Write { firing, drop_kept } => {
                result.done()?;
                if let Some(key) = drop_kept {
                    self.step = AdmitStep::DropKept(firing);
                    return Ok(Next::Call(IoCall::Delete(vec![(
                        AUTOMATIONS_KEYSPACE.to_string(),
                        key,
                    )])));
                }
                Ok(Next::Done(AdmitOutcome::Created(*firing)))
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
