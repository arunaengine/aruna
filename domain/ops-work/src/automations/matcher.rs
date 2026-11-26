//! Durable pending-row journal matcher turning node events into automation firings or kept rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::applet::record::{SubmissionReceipt, submission_key};
use aruna_core::automation::firing::TriggerPayload;
use aruna_core::automation::glob::glob_match;
use aruna_core::automation::keys::{kept_key, seen_key};
use aruna_core::automation::record::{
    Automation, KeptEvent, Lifecycle, PauseReason, RunStatus, Trigger,
};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id_mint::IdMint;
use aruna_core::keyspaces::{
    APPLETS_KEYSPACE, AUTOMATIONS_KEYSPACE, EVENT_JOURNAL_KEYSPACE, WORKFLOW_RUN_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::plugin_runs::{AUTO_PENDING_ROW, SourceEvent, SourceKind};
use aruna_core::structs::execution::workflow_run::{RunState, WorkflowRunRecord, run_key};
use aruna_core::structs::identity::features::{Feature, resolve};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::types::{GroupId, Key};
use aruna_ops_identity::document_repository::{GetConfigError, GetConfigOperation};
use byteview::ByteView;
use ulid::Ulid;

use super::change::{AutoChange, WriteAutoOperation};
use super::start::{AdmitFiringOp, AdmitOutcome, parse_upload};
use super::{AutoError, autos_page};
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
    Journal,
    Seen(usize, Box<SourceEvent>),
    Enrich(usize, Box<SourceEvent>),
    PauseChain(usize, Box<SourceEvent>),
    Admit(usize, Box<SourceEvent>),
    Write,
    DropPending,
}

/// Scans `EVENT_JOURNAL_KEYSPACE` `AUTO_PENDING_ROW` rows and fires or keeps matched events while
/// enforcing self-loop guards, node/group feature switches, chain depth and atomic limits.
#[derive(Debug, PartialEq)]
pub struct MatchEventsOperation {
    realm_id: RealmId,
    disabled_features: BTreeSet<Feature>,
    ids: IdMint,
    now_ms: u64,
    config: Option<RealmConfigDocument>,
    autos: Vec<Automation>,
    rows: Vec<JournalRow>,
    current_key: Option<Vec<u8>>,
    matches: Vec<(usize, SourceEvent)>,
    report: MatchReport,
    step: MatchStep,
}

impl MatchEventsOperation {
    pub fn new(realm_id: RealmId) -> Self {
        Self {
            realm_id,
            disabled_features: BTreeSet::new(),
            ids: IdMint::default(),
            now_ms: 0,
            config: None,
            autos: Vec::new(),
            rows: Vec::new(),
            current_key: None,
            matches: Vec::new(),
            report: MatchReport::default(),
            step: MatchStep::Config,
        }
    }

    pub fn with_disabled_features(mut self, disabled: BTreeSet<Feature>) -> Self {
        self.disabled_features = disabled;
        self
    }

    fn feature_on(&self, feature: Feature, group_id: GroupId) -> bool {
        let Some(cfg) = &self.config else {
            return false;
        };
        resolve(cfg, &self.disabled_features, feature, Some(&group_id)).on
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

    fn next_match(&mut self) -> Result<Next<MatchReport>, AutoError> {
        if let Some((idx, event)) = self.matches.pop() {
            let key = seen_key(self.autos[idx].automation_id, &event.source);
            self.step = MatchStep::Seen(idx, Box::new(event));
            return Ok(Next::Call(IoCall::Read(vec![(
                AUTOMATIONS_KEYSPACE.to_string(),
                key,
            )])));
        }
        if let Some(row_key) = self.current_key.take() {
            self.step = MatchStep::DropPending;
            return Ok(Next::Call(IoCall::Delete(vec![(
                EVENT_JOURNAL_KEYSPACE.to_string(),
                Key::from(row_key),
            )])));
        }
        self.next_row()
    }

    fn next_row(&mut self) -> Result<Next<MatchReport>, AutoError> {
        let Some((row_key, event)) = self.rows.pop() else {
            return Ok(Next::Done(self.report));
        };
        self.current_key = Some(row_key);
        for idx in 0..self.autos.len() {
            if Self::coarse_matches(&self.autos[idx], &event) {
                self.matches.push((idx, event.clone()));
            }
        }
        self.matches.reverse();
        self.next_match()
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
        let auto_id = auto.automation_id;
        let firing_id = self.ids.mint(self.now_ms);
        let admit = AdmitFiringOp::journal(auto_id, firing_id, event.source.clone(), payload);
        self.step = MatchStep::Admit(idx, Box::new(event));
        Ok(Next::Call(IoCall::retry(admit, &auto_id.to_bytes())))
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
                self.step = MatchStep::Journal;
                Ok(Next::Call(IoCall::child(journal_page(
                    AUTO_PENDING_ROW,
                    None,
                    MATCH_BATCH,
                ))))
            }
            MatchStep::Journal => {
                let (mut rows, _) = result
                    .child::<(Vec<JournalRow>, Option<Vec<u8>>), StoreError>()?
                    .map_err(|err| AutoError::Storage(err.detail()))?;
                self.report.scanned = rows.len();
                rows.reverse();
                self.rows = rows;
                self.next_row()
            }
            MatchStep::Seen(idx, event) => {
                if result.value()?.is_some() {
                    return self.next_match();
                }
                match event.kind {
                    SourceKind::DataUploaded => {
                        let Some((bucket, version_id, key)) = parse_upload(&event) else {
                            return self.next_match();
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
                            return self.next_match();
                        };
                        self.step = MatchStep::Enrich(idx, event);
                        Ok(Next::Call(IoCall::Read(vec![(
                            APPLETS_KEYSPACE.to_string(),
                            submission_key(applet_id, ver),
                        )])))
                    }
                    SourceKind::RunFinished => {
                        let Ok(run_id) = Ulid::from_string(&event.revision) else {
                            return self.next_match();
                        };
                        self.step = MatchStep::Enrich(idx, event);
                        Ok(Next::Call(IoCall::Read(vec![(
                            WORKFLOW_RUN_KEYSPACE.to_string(),
                            run_key(run_id),
                        )])))
                    }
                    _ => self.next_match(),
                }
            }
            MatchStep::Enrich(idx, event) => {
                let Some(bytes) = result.value()? else {
                    return self.next_match();
                };
                match event.kind {
                    SourceKind::AppletSubmission => {
                        let rec = SubmissionReceipt::from_bytes(&bytes)?;
                        let payload = TriggerPayload::AppletSubmission {
                            applet_id: rec.applet_id,
                            version_id: rec.version_id,
                            pseudonym: rec.pseudonym,
                            guest: rec.guest,
                            outside_input: rec.outside_input || rec.guest,
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
                            return self.next_match();
                        }
                        let status = match rec.state {
                            RunState::Succeeded => RunStatus::Succeeded,
                            RunState::Failed => RunStatus::Failed,
                            _ => return self.next_match(),
                        };
                        let rec_applet = rec
                            .automation
                            .as_ref()
                            .and_then(|o| o.applet_id)
                            .or_else(|| rec.applet.as_ref().map(|a| a.applet_id));
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
                            return self.next_match();
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
                        let outside_input =
                            rec.automation.as_ref().is_some_and(|o| o.outside_input)
                                || rec.applet.as_ref().is_some_and(|a| a.outside_input);
                        let payload = TriggerPayload::RunFinished {
                            workflow_id: rec.workflow_id,
                            run_id: rec.run_id,
                            status,
                            outputs: rec.outputs,
                            applet_id: rec_applet,
                            outside_input,
                            depth,
                        };
                        self.decide(idx, *event, payload)
                    }
                    _ => self.next_match(),
                }
            }
            MatchStep::PauseChain(idx, event) => {
                if let Ok(updated) = result.child::<Automation, AutoError>()? {
                    self.autos[idx] = updated;
                    self.report.paused += 1;
                }
                self.write_kept(idx, *event, "chain_depth")
            }
            MatchStep::Admit(idx, event) => match result.child::<AdmitOutcome, AutoError>()? {
                Ok(AdmitOutcome::Created(_)) => {
                    self.report.fired += 1;
                    self.next_match()
                }
                Ok(AdmitOutcome::AlreadySeen | AdmitOutcome::Existing(_)) => self.next_match(),
                Err(AutoError::Limited(msg)) => {
                    let reason = if msg.contains("active") {
                        "max_active_firings"
                    } else {
                        "rate_limited"
                    };
                    self.write_kept(idx, *event, reason)
                }
                Err(AutoError::Conflict(_)) => self.write_kept(idx, *event, "paused"),
                Err(AutoError::NotFound) => self.next_match(),
                Err(err) => Err(err),
            },
            MatchStep::Write => {
                result.done()?;
                self.next_match()
            }
            MatchStep::DropPending => {
                result.done()?;
                self.next_row()
            }
        }
    }
}
