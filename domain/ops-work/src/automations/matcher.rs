//! Dedicated journal cursor matcher turning node events into automation firings or kept events.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use aruna_core::applet::record::{SubmissionReceipt, submission_key};
use aruna_core::automation::firing::{Firing, TriggerPayload};
use aruna_core::automation::glob::glob_match;
use aruna_core::automation::keys::{cursor_key, kept_key, seen_key};
use aruna_core::automation::record::{
    Automation, KeptEvent, Lifecycle, PauseReason, RunStatus, Trigger,
};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id_mint::IdMint;
use aruna_core::keyspaces::{APPLETS_KEYSPACE, AUTOMATIONS_KEYSPACE, WORKFLOW_RUN_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::plugin_runs::{JOURNAL_ROW, SourceEvent, SourceKind};
use aruna_core::structs::execution::workflow_run::{RunState, WorkflowRunRecord, run_key};
use aruna_core::structs::identity::features::{Feature, resolve};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::types::GroupId;
use aruna_ops_identity::document_repository::{GetConfigError, GetConfigOperation};
use byteview::ByteView;
use ulid::Ulid;

use super::change::{AutoChange, WriteAutoOperation};
use super::firing_reads::decode_ulid;
use super::limits::CountUsageOperation;
use super::start::{new_firing_rows, parse_upload};
use super::{AutoError, active_page, autos_page};
use crate::plugins::journal::{JournalRow, journal_page};
use crate::plugins::rows::StoreError;

const MATCH_BATCH: usize = 128;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MatchReport {
    pub scanned: usize,
    pub fired: usize,
    pub kept: usize,
    pub paused: usize,
}

#[derive(Debug, PartialEq)]
enum MatchStep {
    Config,
    Autos,
    Active,
    Cursor,
    Journal,
    Seen(usize, Box<SourceEvent>),
    Enrich(usize, Box<SourceEvent>),
    PauseChain(usize, Box<SourceEvent>),
    Count(usize, Box<(SourceEvent, TriggerPayload)>),
    Write,
    SaveCursor,
}

/// Scans `EVENT_JOURNAL_KEYSPACE` `JOURNAL_ROW` after the automation cursor and fires or keeps
/// matched events while enforcing self-loop guards, feature switches, chain depth and limits.
#[derive(Debug, PartialEq)]
pub struct MatchEventsOperation {
    realm_id: RealmId,
    ids: IdMint,
    now_ms: u64,
    config: Option<RealmConfigDocument>,
    autos: Vec<Automation>,
    active_counts: BTreeMap<Ulid, u32>,
    pending: Vec<(usize, SourceEvent)>,
    last_cursor: Option<Vec<u8>>,
    report: MatchReport,
    step: MatchStep,
}

impl MatchEventsOperation {
    pub fn new(realm_id: RealmId) -> Self {
        Self {
            realm_id,
            ids: IdMint::default(),
            now_ms: 0,
            config: None,
            autos: Vec::new(),
            active_counts: BTreeMap::new(),
            pending: Vec::new(),
            last_cursor: None,
            report: MatchReport::default(),
            step: MatchStep::Config,
        }
    }

    fn feature_on(&self, feature: Feature, group_id: GroupId) -> bool {
        let Some(cfg) = &self.config else {
            return false;
        };
        resolve(cfg, &BTreeSet::new(), feature, Some(&group_id)).on
    }

    fn coarse_matches(auto: &Automation, event: &SourceEvent) -> bool {
        if auto.group_id != event.group_id || auto.lifecycle == Lifecycle::Removed {
            return false;
        }
        if auto.account.is_some() && event.actor == auto.account {
            return false;
        }
        match event.kind {
            SourceKind::DataUploaded => {
                let Some((bucket, _, key)) = parse_upload(event) else {
                    return false;
                };
                let file = key.rsplit('/').next().unwrap_or(&key);
                auto.spec.triggers.iter().any(|trigger| match trigger {
                    Trigger::DataUploaded {
                        bucket: tb,
                        prefix,
                        pattern,
                    } => {
                        bucket == *tb
                            && key.starts_with(prefix)
                            && pattern.as_deref().is_none_or(|p| glob_match(p, file))
                    }
                    _ => false,
                })
            }
            SourceKind::AppletSubmission => {
                let Ok(applet_id) = Ulid::from_string(&event.resource) else {
                    return false;
                };
                auto.spec.triggers.iter().any(
                    |t| matches!(t, Trigger::AppletSubmission { applet_id: a } if *a == applet_id),
                )
            }
            SourceKind::RunFinished => {
                let Ok(wf_id) = Ulid::from_string(&event.resource) else {
                    return false;
                };
                auto.spec.triggers.iter().any(
                    |t| matches!(t, Trigger::RunFinished { workflow_id: w, .. } if *w == wf_id),
                )
            }
            _ => false,
        }
    }

    fn next_pending(&mut self) -> Result<Next<MatchReport>, AutoError> {
        if let Some((idx, event)) = self.pending.pop() {
            let key = seen_key(self.autos[idx].automation_id, &event.source);
            self.step = MatchStep::Seen(idx, Box::new(event));
            return Ok(Next::Call(IoCall::Read(vec![(
                AUTOMATIONS_KEYSPACE.to_string(),
                key,
            )])));
        }
        if let Some(cursor) = self.last_cursor.take() {
            self.step = MatchStep::SaveCursor;
            return Ok(Next::Call(IoCall::Write(vec![(
                AUTOMATIONS_KEYSPACE.to_string(),
                cursor_key(),
                ByteView::from(cursor),
            )])));
        }
        Ok(Next::Done(self.report))
    }

    fn write_kept(
        &mut self,
        idx: usize,
        event: SourceEvent,
        reason: &str,
    ) -> Result<Next<MatchReport>, AutoError> {
        let auto_id = self.autos[idx].automation_id;
        let event_id = self.ids.mint(self.now_ms);
        let seen = seen_key(auto_id, &event.source);
        let kept = KeptEvent {
            event_id,
            automation_id: auto_id,
            event,
            reason: reason.to_string(),
            kept_at_ms: self.now_ms,
        };
        let rows = vec![
            (
                AUTOMATIONS_KEYSPACE.to_string(),
                kept_key(auto_id, event_id),
                ByteView::from(kept.to_bytes()?),
            ),
            (
                AUTOMATIONS_KEYSPACE.to_string(),
                seen,
                ByteView::from(self.now_ms.to_be_bytes().to_vec()),
            ),
        ];
        self.report.kept += 1;
        self.step = MatchStep::Write;
        Ok(Next::Call(IoCall::Write(rows)))
    }

    fn decide(
        &mut self,
        idx: usize,
        event: SourceEvent,
        payload: TriggerPayload,
    ) -> Result<Next<MatchReport>, AutoError> {
        let auto = &self.autos[idx];
        if auto.lifecycle != Lifecycle::Active {
            return self.write_kept(idx, event, auto.lifecycle.as_str());
        }
        if !self.feature_on(Feature::Automations, auto.group_id) {
            return self.write_kept(idx, event, "feature_disabled");
        }
        if matches!(payload, TriggerPayload::AppletSubmission { .. })
            && !self.feature_on(Feature::Applets, auto.group_id)
        {
            return self.write_kept(idx, event, "applets_disabled");
        }
        let active = self
            .active_counts
            .get(&auto.automation_id)
            .copied()
            .unwrap_or(0);
        if active >= auto.spec.scope.limits.max_active_firings {
            return self.write_kept(idx, event, "max_active_firings");
        }
        let count = CountUsageOperation::firing(auto.automation_id, &auto.spec.scope.limits);
        let retry_key = auto.automation_id.to_bytes();
        self.step = MatchStep::Count(idx, Box::new((event, payload)));
        Ok(Next::Call(IoCall::retry(count, &retry_key)))
    }
}

impl Operation for MatchEventsOperation {
    type Output = MatchReport;
    type Error = AutoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn seed(&mut self, seed: u128) {
        self.ids = IdMint::new(seed);
    }

    fn first(&mut self) -> Result<Next<MatchReport>, AutoError> {
        let cfg = GetConfigOperation::new(self.realm_id);
        Ok(Next::Call(IoCall::child(cfg)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<MatchReport>, AutoError> {
        match std::mem::replace(&mut self.step, MatchStep::Write) {
            MatchStep::Config => {
                self.config = result.child::<RealmConfigDocument, GetConfigError>()?.ok();
                self.step = MatchStep::Autos;
                Ok(Next::Call(autos_page(None)))
            }
            MatchStep::Autos => {
                let (rows, next) = result.page()?;
                for (_, value) in rows {
                    let auto = Automation::from_bytes(&value)?;
                    if auto.lifecycle != Lifecycle::Removed {
                        self.autos.push(auto);
                    }
                }
                if let Some(after) = next {
                    self.step = MatchStep::Autos;
                    return Ok(Next::Call(autos_page(Some(after))));
                }
                self.step = MatchStep::Active;
                Ok(Next::Call(active_page(None)))
            }
            MatchStep::Active => {
                let (rows, next) = result.page()?;
                for (_, value) in rows {
                    if let Some(auto_id) = decode_ulid(&value) {
                        *self.active_counts.entry(auto_id).or_default() += 1;
                    }
                }
                if let Some(after) = next {
                    self.step = MatchStep::Active;
                    return Ok(Next::Call(active_page(Some(after))));
                }
                self.step = MatchStep::Cursor;
                Ok(Next::Call(IoCall::Read(vec![(
                    AUTOMATIONS_KEYSPACE.to_string(),
                    cursor_key(),
                )])))
            }
            MatchStep::Cursor => {
                let after = result.value()?.map(|bytes| bytes.to_vec());
                self.step = MatchStep::Journal;
                Ok(Next::Call(IoCall::child(journal_page(
                    JOURNAL_ROW,
                    after,
                    MATCH_BATCH,
                ))))
            }
            MatchStep::Journal => {
                let (rows, _) = result
                    .child::<(Vec<JournalRow>, Option<Vec<u8>>), StoreError>()?
                    .map_err(|err| AutoError::Storage(err.detail()))?;
                self.report.scanned = rows.len();
                self.last_cursor = rows.last().map(|(key, _)| key.clone());
                for (_, event) in rows {
                    for idx in 0..self.autos.len() {
                        if Self::coarse_matches(&self.autos[idx], &event) {
                            self.pending.push((idx, event.clone()));
                        }
                    }
                }
                self.pending.reverse();
                self.next_pending()
            }
            MatchStep::Seen(idx, event) => {
                if result.value()?.is_some() {
                    return self.next_pending();
                }
                match event.kind {
                    SourceKind::DataUploaded => {
                        let Some((bucket, version_id, key)) = parse_upload(&event) else {
                            return self.next_pending();
                        };
                        let payload = TriggerPayload::DataUploaded {
                            bucket,
                            key,
                            version_id,
                            path: event.path.clone(),
                            size_bytes: 0,
                        };
                        self.decide(idx, *event, payload)
                    }
                    SourceKind::AppletSubmission => {
                        let (Ok(applet_id), Ok(ver)) = (
                            Ulid::from_string(&event.resource),
                            Ulid::from_string(&event.revision),
                        ) else {
                            return self.next_pending();
                        };
                        self.step = MatchStep::Enrich(idx, event);
                        Ok(Next::Call(IoCall::Read(vec![(
                            APPLETS_KEYSPACE.to_string(),
                            submission_key(applet_id, ver),
                        )])))
                    }
                    SourceKind::RunFinished => {
                        let Ok(run_id) = Ulid::from_string(&event.revision) else {
                            return self.next_pending();
                        };
                        self.step = MatchStep::Enrich(idx, event);
                        Ok(Next::Call(IoCall::Read(vec![(
                            WORKFLOW_RUN_KEYSPACE.to_string(),
                            run_key(run_id),
                        )])))
                    }
                    _ => self.next_pending(),
                }
            }
            MatchStep::Enrich(idx, event) => {
                let Some(bytes) = result.value()? else {
                    return self.next_pending();
                };
                match event.kind {
                    SourceKind::AppletSubmission => {
                        let rec = SubmissionReceipt::from_bytes(&bytes)?;
                        let payload = TriggerPayload::AppletSubmission {
                            applet_id: rec.applet_id,
                            version_id: rec.version_id,
                            pseudonym: rec.pseudonym,
                            guest: rec.guest,
                            bucket: rec.bucket,
                            key: rec.key,
                            files: rec.files,
                        };
                        self.decide(idx, *event, payload)
                    }
                    SourceKind::RunFinished => {
                        let rec = WorkflowRunRecord::from_bytes(&bytes)?;
                        let auto = &self.autos[idx];
                        if rec
                            .automation
                            .as_ref()
                            .is_some_and(|o| o.automation_id == auto.automation_id)
                        {
                            return self.next_pending();
                        }
                        let status = match rec.state {
                            RunState::Succeeded => RunStatus::Succeeded,
                            RunState::Failed => RunStatus::Failed,
                            _ => return self.next_pending(),
                        };
                        let rec_applet = rec.automation.as_ref().and_then(|o| o.applet_id);
                        let matched = auto.spec.triggers.iter().any(|t| match t {
                            Trigger::RunFinished {
                                workflow_id,
                                status: want_status,
                                applet_id: want_applet,
                            } => {
                                *workflow_id == rec.workflow_id
                                    && want_status.is_none_or(|s| s == status)
                                    && want_applet.is_none_or(|a| Some(a) == rec_applet)
                            }
                            _ => false,
                        });
                        if !matched {
                            return self.next_pending();
                        }
                        let depth = rec
                            .automation
                            .as_ref()
                            .map_or(0, |o| o.depth)
                            .saturating_add(1);
                        if depth >= auto.spec.scope.limits.max_chain_depth {
                            if auto.lifecycle == Lifecycle::Active {
                                let actor = auto.approved_by.unwrap_or(auto.created_by);
                                let pause = WriteAutoOperation::node(
                                    auto.automation_id,
                                    AutoChange::Pause(PauseReason::ChainDepth),
                                    actor,
                                    self.realm_id,
                                );
                                self.step = MatchStep::PauseChain(idx, event);
                                return Ok(Next::Call(IoCall::retry(
                                    pause,
                                    &auto.automation_id.to_bytes(),
                                )));
                            }
                            return self.write_kept(idx, *event, "chain_depth");
                        }
                        let payload = TriggerPayload::RunFinished {
                            workflow_id: rec.workflow_id,
                            run_id: rec.run_id,
                            status,
                            outputs: rec.outputs,
                            applet_id: rec_applet,
                            depth,
                        };
                        self.decide(idx, *event, payload)
                    }
                    _ => self.next_pending(),
                }
            }
            MatchStep::PauseChain(idx, event) => {
                if let Ok(updated) = result.child::<Automation, AutoError>()? {
                    self.autos[idx] = updated;
                    self.report.paused += 1;
                }
                self.write_kept(idx, *event, "chain_depth")
            }
            MatchStep::Count(idx, boxed) => {
                let (event, payload) = *boxed;
                match result.child::<(), AutoError>()? {
                    Ok(()) => {}
                    Err(AutoError::Limited(_)) => {
                        return self.write_kept(idx, event, "rate_limited");
                    }
                    Err(err) => return Err(err),
                }
                let auto = &self.autos[idx];
                let firing = Firing::new(
                    self.ids.mint(self.now_ms),
                    auto.automation_id,
                    auto.group_id,
                    auto.revision,
                    &auto.spec,
                    payload,
                    self.now_ms,
                );
                let mut writes = new_firing_rows(&firing)?;
                writes.push((
                    AUTOMATIONS_KEYSPACE.to_string(),
                    seen_key(auto.automation_id, &event.source),
                    ByteView::from(self.now_ms.to_be_bytes().to_vec()),
                ));
                *self.active_counts.entry(auto.automation_id).or_default() += 1;
                self.report.fired += 1;
                self.step = MatchStep::Write;
                Ok(Next::Call(IoCall::Write(writes)))
            }
            MatchStep::Write => {
                result.done()?;
                self.next_pending()
            }
            MatchStep::SaveCursor => {
                result.done()?;
                if self.last_cursor.is_some() {
                    return Err(IoError::Unfinished.into());
                }
                Ok(Next::Done(self.report))
            }
        }
    }
}
