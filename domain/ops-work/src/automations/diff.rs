//! Computes an automation's approval diff, loop warnings, outside-input and sandbox flags.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::applet::record::{Access, Applet, Lifecycle as AppletLifecycle};
use aruna_core::automation::diff::{
    ApprovalDiff, DiffContext, LinkedApplet, PeerSummary, approval_diff, run_workflows,
    write_targets,
};
use aruna_core::automation::record::{Automation, Lifecycle, Spec, Trigger};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::AuthContext;
use ulid::Ulid;

use super::{AutoAccessOperation, AutoError, Need, autos_page};
use crate::applets::AppletError;
use crate::applets::backends::{SandboxOperation, Sandboxing, public_ready};
use crate::applets::reads::{GroupAppletsOperation, LoadAppletOperation};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AutoDiffResult {
    pub diff: ApprovalDiff,
    pub pending: bool,
    pub revision: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PendingApplet {
    applet_id: Ulid,
    is_submission: bool,
}

#[derive(Debug, PartialEq)]
enum DiffStep {
    Access,
    Peers,
    GroupApplets,
    Applets,
    Backends,
}

/// Builds the full approval diff for an automation's pending or proposed spec.
#[derive(Debug, PartialEq)]
pub struct DiffAutoOperation {
    auth: AuthContext,
    id: Ulid,
    local: NodeId,
    proposed: Option<Spec>,
    auto: Option<Automation>,
    peers: Vec<PeerSummary>,
    has_wildcard: bool,
    pending_applets: Vec<PendingApplet>,
    outside_input: bool,
    start_applets: Vec<(Ulid, String)>,
    linked_applets: Vec<LinkedApplet>,
    step: DiffStep,
}

impl DiffAutoOperation {
    pub fn new(auth: &AuthContext, id: Ulid, local: NodeId, proposed: Option<Spec>) -> Self {
        Self {
            auth: auth.clone(),
            id,
            local,
            proposed,
            auto: None,
            peers: Vec::new(),
            has_wildcard: false,
            pending_applets: Vec::new(),
            outside_input: false,
            start_applets: Vec::new(),
            linked_applets: Vec::new(),
            step: DiffStep::Access,
        }
    }

    fn next_spec<'a>(&'a self, auto: &'a Automation) -> (&'a Spec, Option<&'a Spec>, bool) {
        if let Some(proposed) = &self.proposed {
            let current = (auto.approved_by.is_some()).then_some(&auto.spec);
            return (proposed, current, false);
        }
        if let Some(pending) = &auto.pending {
            let current = (auto.approved_by.is_some()).then_some(&auto.spec);
            return (&pending.spec, current, true);
        }
        let current = (auto.approved_by.is_some()).then_some(&auto.spec);
        (&auto.spec, current, false)
    }

    fn collect_applets(&mut self, spec: &Spec) {
        let mut collected: BTreeMap<Ulid, bool> = BTreeMap::new();
        for trigger in &spec.triggers {
            match trigger {
                Trigger::AppletSubmission {
                    applet_id: Some(applet_id),
                } => {
                    *collected.entry(*applet_id).or_insert(false) |= true;
                }
                Trigger::AppletSubmission { applet_id: None } => {
                    self.has_wildcard = true;
                }
                Trigger::RunFinished {
                    applet_id: Some(applet_id),
                    ..
                } => {
                    collected.entry(*applet_id).or_insert(false);
                }
                _ => {}
            }
        }
        self.pending_applets = collected
            .into_iter()
            .map(|(applet_id, is_submission)| PendingApplet {
                applet_id,
                is_submission,
            })
            .collect();
    }

    fn record_applet(&mut self, applet: &Applet, is_submission: bool) {
        let outside = applet.access == Access::Public || applet.guests.is_some();
        if outside {
            self.outside_input = true;
        }
        if is_submission {
            if applet.grants.start.is_some()
                && !self
                    .start_applets
                    .iter()
                    .any(|(id, _)| *id == applet.applet_id)
            {
                self.start_applets
                    .push((applet.applet_id, applet.slug.clone()));
            }
            if !self
                .linked_applets
                .iter()
                .any(|item| item.applet_id == applet.applet_id)
            {
                self.linked_applets.push(LinkedApplet {
                    applet_id: applet.applet_id,
                    name: applet.name.clone(),
                    slug: applet.slug.clone(),
                    outside_input: outside,
                });
            }
        }
    }

    fn after_peers(&mut self) -> Result<Next<AutoDiffResult>, AutoError> {
        if self.has_wildcard {
            let group_id = self.auto.as_ref().ok_or(IoError::Unfinished)?.group_id;
            self.step = DiffStep::GroupApplets;
            return Ok(Next::Call(IoCall::child(GroupAppletsOperation::new(
                group_id,
            ))));
        }
        Ok(self.next_applet())
    }

    fn next_applet(&mut self) -> Next<AutoDiffResult> {
        if let Some(pending) = self.pending_applets.last().copied() {
            self.step = DiffStep::Applets;
            return Next::Call(IoCall::child(LoadAppletOperation::new(pending.applet_id)));
        }
        self.step = DiffStep::Backends;
        Next::Call(IoCall::child(SandboxOperation))
    }
}

impl Operation for DiffAutoOperation {
    type Output = AutoDiffResult;
    type Error = AutoError;

    fn first(&mut self) -> Result<Next<AutoDiffResult>, AutoError> {
        self.step = DiffStep::Access;
        let access = AutoAccessOperation::new(&self.auth, self.id, self.local, Need::Read);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<AutoDiffResult>, AutoError> {
        match self.step {
            DiffStep::Access => {
                let auto = result.child::<Automation, AutoError>()??;
                let next_spec = self.next_spec(&auto).0.clone();
                self.collect_applets(&next_spec);
                self.auto = Some(auto);
                self.step = DiffStep::Peers;
                Ok(Next::Call(autos_page(None)))
            }
            DiffStep::Peers => {
                let auto = self.auto.as_ref().ok_or(IoError::Unfinished)?;
                let (rows, next) = result.page()?;
                for (_, value) in rows {
                    let peer = Automation::from_bytes(&value)?;
                    if peer.group_id == auto.group_id
                        && peer.automation_id != auto.automation_id
                        && peer.lifecycle != Lifecycle::Removed
                    {
                        self.peers.push(PeerSummary {
                            name: peer.name,
                            write_targets: write_targets(&peer.spec),
                            run_workflows: run_workflows(&peer.spec),
                            triggers: peer.spec.triggers,
                        });
                    }
                }
                match next {
                    Some(after) => Ok(Next::Call(autos_page(Some(after)))),
                    None => self.after_peers(),
                }
            }
            DiffStep::GroupApplets => {
                let auto = self.auto.as_ref().ok_or(IoError::Unfinished)?;
                let (auto_id, group_id) = (auto.automation_id, auto.group_id);
                let applets = result
                    .child::<Vec<Applet>, AppletError>()?
                    .map_err(|error| AutoError::Storage(error.to_string()))?;
                for applet in applets {
                    if applet.group_id == group_id
                        && applet.lifecycle != AppletLifecycle::TakenDown
                        && applet.automations.contains(&auto_id)
                    {
                        self.pending_applets
                            .retain(|pending| pending.applet_id != applet.applet_id);
                        self.record_applet(&applet, true);
                    }
                }
                Ok(self.next_applet())
            }
            DiffStep::Applets => {
                let Some(pending) = self.pending_applets.pop() else {
                    return Err(IoError::Unfinished.into());
                };
                let group_id = self.auto.as_ref().ok_or(IoError::Unfinished)?.group_id;
                let loaded = result
                    .child::<Option<Applet>, AppletError>()?
                    .map_err(|error| AutoError::Storage(error.to_string()))?;
                if let Some(applet) = loaded
                    && applet.group_id == group_id
                {
                    self.record_applet(&applet, pending.is_submission);
                }
                Ok(self.next_applet())
            }
            DiffStep::Backends => {
                let backends = result
                    .child::<Vec<Sandboxing>, AppletError>()?
                    .unwrap_or_default();
                let peers = std::mem::take(&mut self.peers);
                let start_applets = std::mem::take(&mut self.start_applets);
                let linked_applets = std::mem::take(&mut self.linked_applets);
                let auto = self.auto.as_ref().ok_or(IoError::Unfinished)?;
                let (next_spec, current, pending) = self.next_spec(auto);
                let ctx = DiffContext {
                    outside_input: self.outside_input,
                    unsandboxed: next_spec.has_runs() && !public_ready(&backends),
                    peers,
                    start_applets,
                    linked_applets,
                };
                let diff = approval_diff(current, next_spec, &ctx);
                Ok(Next::Done(AutoDiffResult {
                    diff,
                    pending,
                    revision: auto.revision,
                }))
            }
        }
    }

    fn expected_error(error: &AutoError) -> bool {
        !matches!(error, AutoError::Storage(_))
    }
}
