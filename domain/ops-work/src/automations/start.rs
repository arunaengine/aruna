//! Manual starts and idempotent replay of kept matched events into new automation firings.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::automation::firing::{Firing, TriggerPayload};
use aruna_core::automation::keys::{firing_key, kept_key, replay_key};
use aruna_core::automation::record::{Automation, KeptEvent};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::id_mint::IdMint;
use aruna_core::keyspaces::{AUTOMATION_RUNS_KEYSPACE, AUTOMATIONS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::plugin_runs::SourceEvent;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::features::Feature;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_ops_identity::document_repository::{GetConfigError, GetConfigOperation};
use ulid::Ulid;

#[cfg(test)]
pub(crate) use super::admit::new_firing_rows;
pub(crate) use super::admit::{AdmitFiringOp, AdmitOutcome, parse_replay};
use super::{AutoAccessOperation, AutoError, Need, check_feature};

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

/// Outcome of a manual automation start request.
#[derive(Clone, Debug, PartialEq)]
pub enum ManualStart {
    Started(Firing),
    Skipped(Firing),
}

#[derive(Debug, PartialEq)]
enum ManualStep {
    Access,
    Config(Box<Automation>),
    Admit,
}

/// Starts an active automation manually (`POST /compute/automations/{id}/start`).
#[derive(Debug, PartialEq)]
pub struct StartManualOperation {
    auth: AuthContext,
    id: Ulid,
    local: NodeId,
    values_json: String,
    disabled_features: BTreeSet<Feature>,
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
            disabled_features: BTreeSet::new(),
            ids: IdMint::default(),
            now_ms: 0,
            step: ManualStep::Access,
        }
    }

    pub fn with_disabled_features(mut self, disabled: BTreeSet<Feature>) -> Self {
        self.disabled_features = disabled;
        self
    }
}

impl Operation for StartManualOperation {
    type Output = ManualStart;
    type Error = AutoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn seed(&mut self, seed: u128) {
        self.ids = IdMint::new(seed);
    }

    fn first(&mut self) -> Result<Next<ManualStart>, AutoError> {
        self.step = ManualStep::Access;
        let access = AutoAccessOperation::new(&self.auth, self.id, self.local, Need::Write);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ManualStart>, AutoError> {
        match std::mem::replace(&mut self.step, ManualStep::Admit) {
            ManualStep::Access => {
                let auto = result.child::<Automation, AutoError>()??;
                self.step = ManualStep::Config(Box::new(auto));
                Ok(Next::Call(IoCall::child(GetConfigOperation::new(
                    self.auth.realm_id,
                ))))
            }
            ManualStep::Config(auto) => {
                let config = result
                    .child::<RealmConfigDocument, GetConfigError>()?
                    .map_err(|_| {
                        AutoError::Unavailable("the realm feature policy cannot be read".into())
                    })?;
                check_feature(
                    &config,
                    &self.disabled_features,
                    Feature::Automations,
                    auto.group_id,
                )?;
                let firing_id = self.ids.mint(self.now_ms);
                let payload = TriggerPayload::Manual {
                    values_json: self.values_json.clone(),
                    actor: self.auth.user_id,
                };
                let admit = AdmitFiringOp::manual(auto.automation_id, firing_id, payload);
                self.step = ManualStep::Admit;
                Ok(Next::Call(IoCall::retry(
                    admit,
                    &auto.automation_id.to_bytes(),
                )))
            }
            ManualStep::Admit => match result.child::<AdmitOutcome, AutoError>()?? {
                AdmitOutcome::Created(firing) | AdmitOutcome::Existing(firing) => {
                    Ok(Next::Done(ManualStart::Started(firing)))
                }
                AdmitOutcome::Skipped(firing) => Ok(Next::Done(ManualStart::Skipped(firing))),
                AdmitOutcome::AlreadySeen | AdmitOutcome::Unmatched | AdmitOutcome::ChainDepth => {
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
    Config(Box<Automation>),
    ReadKept,
    LoadReplayed,
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
    disabled_features: BTreeSet<Feature>,
    ids: IdMint,
    now_ms: u64,
    pending: Vec<ReplayItem>,
    started: Vec<Firing>,
    step: ReplayStep,
}

impl StartEventsOperation {
    pub fn new(auth: &AuthContext, id: Ulid, local: NodeId, mut event_ids: Vec<Ulid>) -> Self {
        let mut seen = BTreeSet::new();
        event_ids.retain(|event_id| seen.insert(*event_id));
        Self {
            auth: auth.clone(),
            id,
            local,
            event_ids,
            disabled_features: BTreeSet::new(),
            ids: IdMint::default(),
            now_ms: 0,
            pending: Vec::new(),
            started: Vec::new(),
            step: ReplayStep::Access,
        }
    }

    pub fn with_disabled_features(mut self, disabled: BTreeSet<Feature>) -> Self {
        self.disabled_features = disabled;
        self
    }

    fn next_item(&mut self) -> Result<Next<Vec<Firing>>, AutoError> {
        let Some(item) = self.pending.pop() else {
            return Ok(Next::Done(std::mem::take(&mut self.started)));
        };
        match item {
            ReplayItem::Replayed(firing_id) => {
                self.step = ReplayStep::LoadReplayed;
                Ok(Next::Call(IoCall::Read(vec![(
                    AUTOMATION_RUNS_KEYSPACE.to_string(),
                    firing_key(self.id, firing_id),
                )])))
            }
            ReplayItem::Kept(kept) => {
                let firing_id = self.ids.mint(self.now_ms);
                let admit = AdmitFiringOp::replay(self.id, kept.event_id, firing_id, kept.payload);
                self.step = ReplayStep::Admit;
                Ok(Next::Call(IoCall::retry(admit, &self.id.to_bytes())))
            }
        }
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
        self.step = ReplayStep::Access;
        let access = AutoAccessOperation::new(&self.auth, self.id, self.local, Need::Write);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<Firing>>, AutoError> {
        match std::mem::replace(&mut self.step, ReplayStep::Admit) {
            ReplayStep::Access => {
                let auto = result.child::<Automation, AutoError>()??;
                self.step = ReplayStep::Config(Box::new(auto));
                Ok(Next::Call(IoCall::child(GetConfigOperation::new(
                    self.auth.realm_id,
                ))))
            }
            ReplayStep::Config(auto) => {
                let config = result
                    .child::<RealmConfigDocument, GetConfigError>()?
                    .map_err(|_| {
                        AutoError::Unavailable("the realm feature policy cannot be read".into())
                    })?;
                check_feature(
                    &config,
                    &self.disabled_features,
                    Feature::Automations,
                    auto.group_id,
                )?;
                if auto.spec.uses_applets() {
                    check_feature(
                        &config,
                        &self.disabled_features,
                        Feature::Applets,
                        auto.group_id,
                    )?;
                }
                let mut reads = Vec::with_capacity(self.event_ids.len() * 2);
                for event_id in &self.event_ids {
                    reads.push((
                        AUTOMATIONS_KEYSPACE.to_string(),
                        kept_key(auto.automation_id, *event_id),
                    ));
                    reads.push((
                        AUTOMATIONS_KEYSPACE.to_string(),
                        replay_key(auto.automation_id, *event_id),
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
            ReplayStep::Admit => {
                match result.child::<AdmitOutcome, AutoError>()?? {
                    AdmitOutcome::Created(firing)
                    | AdmitOutcome::Existing(firing)
                    | AdmitOutcome::Skipped(firing) => {
                        self.started.push(firing);
                    }
                    AdmitOutcome::Unmatched => {
                        return Err(AutoError::Conflict(
                            "kept event no longer matches this automation's triggers".into(),
                        ));
                    }
                    AdmitOutcome::ChainDepth => {
                        return Err(AutoError::Conflict(
                            "kept event exceeds max_chain_depth".into(),
                        ));
                    }
                    AdmitOutcome::AlreadySeen => {}
                }
                self.next_item()
            }
        }
    }

    fn expected_error(error: &AutoError) -> bool {
        !matches!(error, AutoError::Storage(_))
    }
}
