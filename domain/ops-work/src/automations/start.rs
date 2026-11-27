//! Manual starts and idempotent replay of kept matched events into new automation firings.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::applet::record::{SubmissionReceipt, submission_key};
use aruna_core::automation::firing::{Firing, TriggerPayload};
use aruna_core::automation::keys::{firing_key, kept_key, replay_key};
use aruna_core::automation::record::{Automation, KeptEvent, RunStatus};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::id_mint::IdMint;
use aruna_core::keyspaces::{
    APPLETS_KEYSPACE, AUTOMATION_RUNS_KEYSPACE, AUTOMATIONS_KEYSPACE, WORKFLOW_RUN_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::plugin_runs::{SourceEvent, SourceKind};
use aruna_core::structs::execution::workflow_run::{RunState, WorkflowRunRecord, run_key};
use aruna_core::structs::identity::auth::AuthContext;
use ulid::Ulid;

#[cfg(test)]
pub(crate) use super::admit::new_firing_rows;
pub(crate) use super::admit::{AdmitFiringOp, AdmitOutcome, parse_replay};
use super::{AutoAccessOperation, AutoError, Need};

/// Parses a `DataUploaded` source event into `(bucket, version_id, key)`.
pub(crate) fn parse_upload(event: &SourceEvent) -> Option<(String, Ulid, String)> {
    let mut parts = event.source.splitn(5, '/');
    if parts.next()? != "data_uploaded" {
        return None;
    }
    let _node = parts.next()?;
    let bucket = parts.next()?.to_string();
    let version_id = Ulid::from_string(parts.next()?).ok()?;
    let key = parts.next()?.to_string();
    Some((bucket, version_id, key))
}

#[derive(Debug, PartialEq)]
enum ManualStep {
    Access,
    Admit,
}

/// Starts an active automation manually (`POST /compute/automations/{id}/start`).
#[derive(Debug, PartialEq)]
pub struct StartManualOperation {
    auth: AuthContext,
    id: Ulid,
    local: NodeId,
    values_json: String,
    ids: IdMint,
    now_ms: u64,
    step: ManualStep,
}

impl StartManualOperation {
    pub fn new(auth: &AuthContext, id: Ulid, local: NodeId, values_json: String) -> Self {
        Self {
            auth: auth.clone(),
            id,
            local,
            values_json,
            ids: IdMint::default(),
            now_ms: 0,
            step: ManualStep::Access,
        }
    }
}

impl Operation for StartManualOperation {
    type Output = Firing;
    type Error = AutoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn seed(&mut self, seed: u128) {
        self.ids = IdMint::new(seed);
    }

    fn first(&mut self) -> Result<Next<Firing>, AutoError> {
        let access = AutoAccessOperation::new(&self.auth, self.id, self.local, Need::Write);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Firing>, AutoError> {
        match std::mem::replace(&mut self.step, ManualStep::Admit) {
            ManualStep::Access => {
                let auto = result.child::<Automation, AutoError>()??;
                let firing_id = self.ids.mint(self.now_ms);
                let payload = TriggerPayload::Manual {
                    values_json: self.values_json.clone(),
                    actor: self.auth.user_id,
                };
                let admit = AdmitFiringOp::manual(auto.automation_id, firing_id, payload);
                Ok(Next::Call(IoCall::retry(
                    admit,
                    &auto.automation_id.to_bytes(),
                )))
            }
            ManualStep::Admit => match result.child::<AdmitOutcome, AutoError>()?? {
                AdmitOutcome::Created(firing) | AdmitOutcome::Existing(firing) => {
                    Ok(Next::Done(firing))
                }
                AdmitOutcome::AlreadySeen | AdmitOutcome::Unmatched => {
                    Err(IoError::Unfinished.into())
                }
            },
        }
    }

    fn expected_error(error: &AutoError) -> bool {
        !matches!(error, AutoError::Storage(_))
    }
}

#[derive(Debug, PartialEq)]
enum ReplayItem {
    Kept(Box<KeptEvent>),
    Replayed(Ulid),
}

#[derive(Debug, PartialEq)]
enum ReplayStep {
    Access,
    ReadKept,
    LoadReplayed,
    Enrich(Box<KeptEvent>),
    Admit,
}

/// Starts chosen kept events (`POST /compute/automations/{id}/events/start`), claiming each event
/// atomically and answering idempotently for duplicate or already-replayed event IDs.
#[derive(Debug, PartialEq)]
pub struct StartEventsOperation {
    auth: AuthContext,
    id: Ulid,
    local: NodeId,
    event_ids: Vec<Ulid>,
    ids: IdMint,
    now_ms: u64,
    pending: Vec<ReplayItem>,
    started: Vec<Firing>,
    step: ReplayStep,
}

impl StartEventsOperation {
    pub fn new(auth: &AuthContext, id: Ulid, local: NodeId, mut event_ids: Vec<Ulid>) -> Self {
        let mut seen = BTreeSet::new();
        event_ids.retain(|eid| seen.insert(*eid));
        Self {
            auth: auth.clone(),
            id,
            local,
            event_ids,
            ids: IdMint::default(),
            now_ms: 0,
            pending: Vec::new(),
            started: Vec::new(),
            step: ReplayStep::Access,
        }
    }

    fn next_item(&mut self) -> Result<Next<Vec<Firing>>, AutoError> {
        let Some(item) = self.pending.pop() else {
            return Ok(Next::Done(std::mem::take(&mut self.started)));
        };
        let kept = match item {
            ReplayItem::Replayed(firing_id) => {
                self.step = ReplayStep::LoadReplayed;
                return Ok(Next::Call(IoCall::Read(vec![(
                    AUTOMATION_RUNS_KEYSPACE.to_string(),
                    firing_key(self.id, firing_id),
                )])));
            }
            ReplayItem::Kept(kept) => *kept,
        };
        match kept.event.kind {
            SourceKind::DataUploaded => {
                let (bucket, version_id, key) = parse_upload(&kept.event)
                    .ok_or_else(|| AutoError::Invalid("malformed upload event".into()))?;
                let payload = TriggerPayload::DataUploaded {
                    bucket,
                    key,
                    version_id,
                    path: kept.event.path.clone(),
                    size_bytes: 0,
                };
                self.begin_admit(kept.event_id, payload)
            }
            SourceKind::AppletSubmission => {
                let applet_id = Ulid::from_string(&kept.event.resource)
                    .map_err(|_| AutoError::Invalid("bad applet_id in event".into()))?;
                let ver_id = Ulid::from_string(&kept.event.revision)
                    .map_err(|_| AutoError::Invalid("bad version_id in event".into()))?;
                self.step = ReplayStep::Enrich(Box::new(kept));
                Ok(Next::Call(IoCall::Read(vec![(
                    APPLETS_KEYSPACE.to_string(),
                    submission_key(applet_id, ver_id),
                )])))
            }
            SourceKind::RunFinished => {
                let run_id = Ulid::from_string(&kept.event.revision)
                    .map_err(|_| AutoError::Invalid("bad run_id in event".into()))?;
                self.step = ReplayStep::Enrich(Box::new(kept));
                Ok(Next::Call(IoCall::Read(vec![(
                    WORKFLOW_RUN_KEYSPACE.to_string(),
                    run_key(run_id),
                )])))
            }
            _ => self.next_item(),
        }
    }

    fn begin_admit(
        &mut self,
        event_id: Ulid,
        payload: TriggerPayload,
    ) -> Result<Next<Vec<Firing>>, AutoError> {
        let firing_id = self.ids.mint(self.now_ms);
        let admit = AdmitFiringOp::replay(self.id, event_id, firing_id, payload);
        self.step = ReplayStep::Admit;
        Ok(Next::Call(IoCall::retry(admit, &self.id.to_bytes())))
    }
}

impl Operation for StartEventsOperation {
    type Output = Vec<Firing>;
    type Error = AutoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn seed(&mut self, seed: u128) {
        self.ids = IdMint::new(seed);
    }

    fn first(&mut self) -> Result<Next<Vec<Firing>>, AutoError> {
        if self.event_ids.is_empty() {
            return Err(AutoError::Invalid("select at least one event_id".into()));
        }
        let access = AutoAccessOperation::new(&self.auth, self.id, self.local, Need::Write);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<Firing>>, AutoError> {
        match std::mem::replace(&mut self.step, ReplayStep::Admit) {
            ReplayStep::Access => {
                let auto = result.child::<Automation, AutoError>()??;
                let mut reads = Vec::with_capacity(self.event_ids.len() * 2);
                for eid in &self.event_ids {
                    reads.push((
                        AUTOMATIONS_KEYSPACE.to_string(),
                        kept_key(auto.automation_id, *eid),
                    ));
                    reads.push((
                        AUTOMATIONS_KEYSPACE.to_string(),
                        replay_key(auto.automation_id, *eid),
                    ));
                }
                self.step = ReplayStep::ReadKept;
                Ok(Next::Call(IoCall::Read(reads)))
            }
            ReplayStep::ReadKept => {
                let values = result.values()?;
                for pair in values.as_chunks::<2>().0 {
                    if let Some(bytes) = &pair[1]
                        && let Some((firing_id, _)) = parse_replay(bytes)
                    {
                        self.pending.push(ReplayItem::Replayed(firing_id));
                    } else if let Some(bytes) = &pair[0] {
                        let kept = KeptEvent::from_bytes(bytes)?;
                        self.pending.push(ReplayItem::Kept(Box::new(kept)));
                    }
                }
                if self.pending.is_empty() {
                    return Err(AutoError::NotFound);
                }
                self.pending.reverse();
                self.next_item()
            }
            ReplayStep::LoadReplayed => {
                if let Some(bytes) = result.value()? {
                    self.started.push(Firing::from_bytes(&bytes)?);
                }
                self.next_item()
            }
            ReplayStep::Enrich(kept) => {
                let Some(bytes) = result.value()? else {
                    return self.next_item();
                };
                let payload = match kept.event.kind {
                    SourceKind::AppletSubmission => {
                        let rec = SubmissionReceipt::from_bytes(&bytes)?;
                        TriggerPayload::AppletSubmission {
                            applet_id: rec.applet_id,
                            version_id: rec.version_id,
                            pseudonym: rec.pseudonym,
                            guest: rec.guest,
                            outside_input: rec.outside_input || rec.guest,
                            bucket: rec.bucket,
                            key: rec.key,
                            files: rec.files,
                        }
                    }
                    SourceKind::RunFinished => {
                        let rec = WorkflowRunRecord::from_bytes(&bytes)?;
                        let status = match rec.state {
                            RunState::Succeeded => RunStatus::Succeeded,
                            _ => RunStatus::Failed,
                        };
                        let depth = rec
                            .automation
                            .as_ref()
                            .map_or(0, |o| o.depth)
                            .saturating_add(1);
                        let applet_id = rec
                            .automation
                            .as_ref()
                            .and_then(|o| o.applet_id)
                            .or_else(|| rec.applet.as_ref().map(|a| a.applet_id));
                        let outside_input =
                            rec.automation.as_ref().is_some_and(|o| o.outside_input)
                                || rec.applet.as_ref().is_some_and(|a| a.outside_input);
                        TriggerPayload::RunFinished {
                            workflow_id: rec.workflow_id,
                            run_id: rec.run_id,
                            status,
                            outputs: rec.outputs,
                            applet_id,
                            outside_input,
                            depth,
                        }
                    }
                    _ => return self.next_item(),
                };
                self.begin_admit(kept.event_id, payload)
            }
            ReplayStep::Admit => {
                match result.child::<AdmitOutcome, AutoError>()?? {
                    AdmitOutcome::Created(firing) | AdmitOutcome::Existing(firing) => {
                        self.started.push(firing);
                    }
                    AdmitOutcome::AlreadySeen | AdmitOutcome::Unmatched => {}
                }
                self.next_item()
            }
        }
    }

    fn expected_error(error: &AutoError) -> bool {
        !matches!(error, AutoError::Storage(_))
    }
}
