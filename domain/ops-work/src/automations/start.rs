//! Manual starts and replaying kept matched events into new automation firings.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::applet::record::{SubmissionReceipt, submission_key};
use aruna_core::automation::firing::{Firing, TriggerPayload};
use aruna_core::automation::keys::{active_key, index_key, kept_key};
use aruna_core::automation::record::{Automation, KeptEvent, Lifecycle, RunStatus, Trigger};
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
use aruna_core::types::{Key, KeySpace, Value};
use byteview::ByteView;
use ulid::Ulid;

use super::firing_reads::decode_ulid;
use super::limits::CountUsageOperation;
use super::{AutoAccessOperation, AutoError, Need, active_page, firing_row};

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
    Active,
    Count,
    Write(Box<Firing>),
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
    auto: Option<Automation>,
    active_count: u32,
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
            auto: None,
            active_count: 0,
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
        match std::mem::replace(&mut self.step, ManualStep::Active) {
            ManualStep::Access => {
                let auto = result.child::<Automation, AutoError>()??;
                if auto.lifecycle != Lifecycle::Active {
                    return Err(AutoError::Conflict(
                        "automation must be active to start".into(),
                    ));
                }
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
                self.auto = Some(auto);
                Ok(Next::Call(active_page(None)))
            }
            ManualStep::Active => {
                let auto = self.auto.as_ref().ok_or(IoError::Unfinished)?;
                let (rows, next) = result.page()?;
                for (_, value) in rows {
                    if decode_ulid(&value) == Some(auto.automation_id) {
                        self.active_count = self.active_count.saturating_add(1);
                    }
                }
                if let Some(after) = next {
                    return Ok(Next::Call(active_page(Some(after))));
                }
                if self.active_count >= auto.spec.scope.limits.max_active_firings {
                    return Err(AutoError::Limited(
                        "max_active_firings limit reached".into(),
                    ));
                }
                self.step = ManualStep::Count;
                let count =
                    CountUsageOperation::firing(auto.automation_id, &auto.spec.scope.limits);
                Ok(Next::Call(IoCall::retry(
                    count,
                    &auto.automation_id.to_bytes(),
                )))
            }
            ManualStep::Count => {
                result.child::<(), AutoError>()??;
                let auto = self.auto.as_ref().ok_or(IoError::Unfinished)?;
                let firing_id = self.ids.mint(self.now_ms);
                let payload = TriggerPayload::Manual {
                    values_json: self.values_json.clone(),
                    actor: self.auth.user_id,
                };
                let firing = Firing::new(
                    firing_id,
                    auto.automation_id,
                    auto.group_id,
                    auto.revision,
                    &auto.spec,
                    payload,
                    self.now_ms,
                );
                let writes = new_firing_rows(&firing)?;
                self.step = ManualStep::Write(Box::new(firing));
                Ok(Next::Call(IoCall::Write(writes)))
            }
            ManualStep::Write(firing) => {
                result.done()?;
                Ok(Next::Done(*firing))
            }
        }
    }

    fn expected_error(error: &AutoError) -> bool {
        !matches!(error, AutoError::Storage(_))
    }
}

#[derive(Debug, PartialEq)]
enum ReplayStep {
    Access,
    ReadKept,
    Active,
    Enrich(Box<KeptEvent>),
    Count(Box<(KeptEvent, TriggerPayload)>),
    Write(Box<(KeptEvent, Firing)>),
    DropKept,
}

/// Starts chosen kept events (`POST /compute/automations/{id}/events/start`), deleting started ones.
#[derive(Debug, PartialEq)]
pub struct StartEventsOperation {
    auth: AuthContext,
    id: Ulid,
    local: NodeId,
    event_ids: Vec<Ulid>,
    ids: IdMint,
    now_ms: u64,
    auto: Option<Automation>,
    active_count: u32,
    pending: Vec<KeptEvent>,
    started: Vec<Firing>,
    step: ReplayStep,
}

impl StartEventsOperation {
    pub fn new(auth: &AuthContext, id: Ulid, local: NodeId, event_ids: Vec<Ulid>) -> Self {
        Self {
            auth: auth.clone(),
            id,
            local,
            event_ids,
            ids: IdMint::default(),
            now_ms: 0,
            auto: None,
            active_count: 0,
            pending: Vec::new(),
            started: Vec::new(),
            step: ReplayStep::Access,
        }
    }

    fn next_kept(&mut self) -> Result<Next<Vec<Firing>>, AutoError> {
        let auto = self.auto.as_ref().ok_or(IoError::Unfinished)?;
        let Some(kept) = self.pending.pop() else {
            return Ok(Next::Done(std::mem::take(&mut self.started)));
        };
        if self.active_count >= auto.spec.scope.limits.max_active_firings {
            return Err(AutoError::Limited(
                "max_active_firings limit reached".into(),
            ));
        }
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
                self.begin_count(kept, payload)
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
            _ => self.next_kept(),
        }
    }

    fn begin_count(
        &mut self,
        kept: KeptEvent,
        payload: TriggerPayload,
    ) -> Result<Next<Vec<Firing>>, AutoError> {
        let auto = self.auto.as_ref().ok_or(IoError::Unfinished)?;
        self.step = ReplayStep::Count(Box::new((kept, payload)));
        let count = CountUsageOperation::firing(auto.automation_id, &auto.spec.scope.limits);
        Ok(Next::Call(IoCall::retry(
            count,
            &auto.automation_id.to_bytes(),
        )))
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
        match std::mem::replace(&mut self.step, ReplayStep::DropKept) {
            ReplayStep::Access => {
                let auto = result.child::<Automation, AutoError>()??;
                if auto.lifecycle != Lifecycle::Active {
                    return Err(AutoError::Conflict(
                        "automation must be active to start".into(),
                    ));
                }
                let reads = self
                    .event_ids
                    .iter()
                    .map(|eid| {
                        (
                            AUTOMATIONS_KEYSPACE.to_string(),
                            kept_key(auto.automation_id, *eid),
                        )
                    })
                    .collect();
                self.auto = Some(auto);
                self.step = ReplayStep::ReadKept;
                Ok(Next::Call(IoCall::Read(reads)))
            }
            ReplayStep::ReadKept => {
                for value in result.values()?.into_iter().flatten() {
                    self.pending.push(KeptEvent::from_bytes(&value)?);
                }
                if self.pending.is_empty() {
                    return Err(AutoError::NotFound);
                }
                self.pending.reverse();
                self.step = ReplayStep::Active;
                Ok(Next::Call(active_page(None)))
            }
            ReplayStep::Active => {
                let auto = self.auto.as_ref().ok_or(IoError::Unfinished)?;
                let (rows, next) = result.page()?;
                for (_, value) in rows {
                    if decode_ulid(&value) == Some(auto.automation_id) {
                        self.active_count = self.active_count.saturating_add(1);
                    }
                }
                if let Some(after) = next {
                    self.step = ReplayStep::Active;
                    return Ok(Next::Call(active_page(Some(after))));
                }
                self.next_kept()
            }
            ReplayStep::Enrich(kept) => {
                let Some(bytes) = result.value()? else {
                    return self.next_kept();
                };
                let payload = match kept.event.kind {
                    SourceKind::AppletSubmission => {
                        let rec = SubmissionReceipt::from_bytes(&bytes)?;
                        TriggerPayload::AppletSubmission {
                            applet_id: rec.applet_id,
                            version_id: rec.version_id,
                            pseudonym: rec.pseudonym,
                            guest: rec.guest,
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
                        TriggerPayload::RunFinished {
                            workflow_id: rec.workflow_id,
                            run_id: rec.run_id,
                            status,
                            outputs: rec.outputs,
                            applet_id: rec.automation.as_ref().and_then(|o| o.applet_id),
                            depth,
                        }
                    }
                    _ => return self.next_kept(),
                };
                self.begin_count(*kept, payload)
            }
            ReplayStep::Count(boxed) => {
                result.child::<(), AutoError>()??;
                let (kept, payload) = *boxed;
                let auto = self.auto.as_ref().ok_or(IoError::Unfinished)?;
                let firing = Firing::new(
                    self.ids.mint(self.now_ms),
                    auto.automation_id,
                    auto.group_id,
                    auto.revision,
                    &auto.spec,
                    payload,
                    self.now_ms,
                );
                let writes = new_firing_rows(&firing)?;
                self.step = ReplayStep::Write(Box::new((kept, firing)));
                Ok(Next::Call(IoCall::Write(writes)))
            }
            ReplayStep::Write(boxed) => {
                result.done()?;
                let (kept, firing) = *boxed;
                self.active_count = self.active_count.saturating_add(1);
                self.started.push(firing);
                self.step = ReplayStep::DropKept;
                Ok(Next::Call(IoCall::Delete(vec![(
                    AUTOMATIONS_KEYSPACE.to_string(),
                    kept_key(self.id, kept.event_id),
                )])))
            }
            ReplayStep::DropKept => {
                result.done()?;
                self.next_kept()
            }
        }
    }

    fn expected_error(error: &AutoError) -> bool {
        !matches!(error, AutoError::Storage(_))
    }
}
