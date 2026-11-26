//! Computes an automation's approval diff, loop warnings, outside-input and sandbox flags.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::applet::record::{Access, Applet};
use aruna_core::automation::diff::{
    ApprovalDiff, DiffContext, PeerSummary, approval_diff, run_workflows, write_targets,
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
use crate::applets::reads::LoadAppletOperation;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AutoDiffResult {
    pub diff: ApprovalDiff,
    pub pending: bool,
    pub revision: u64,
}

#[derive(Debug, PartialEq)]
enum DiffStep {
    Access,
    Peers,
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
    applet_ids: Vec<(Ulid, bool)>,
    outside_input: bool,
    start_applets: Vec<(Ulid, String)>,
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
            applet_ids: Vec::new(),
            outside_input: false,
            start_applets: Vec::new(),
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
        let mut seen = BTreeSet::new();
        for trigger in &spec.triggers {
            match trigger {
                Trigger::AppletSubmission { applet_id } => {
                    if seen.insert(*applet_id) {
                        self.applet_ids.push((*applet_id, true));
                    }
                }
                Trigger::RunFinished {
                    applet_id: Some(applet_id),
                    ..
                } if seen.insert(*applet_id) => {
                    self.applet_ids.push((*applet_id, false));
                }
                _ => {}
            }
        }
    }

    fn next_applet(&mut self) -> Next<AutoDiffResult> {
        if let Some((applet_id, _)) = self.applet_ids.last().copied() {
            self.step = DiffStep::Applets;
            return Next::Call(IoCall::child(LoadAppletOperation::new(applet_id)));
        }
        self.step = DiffStep::Backends;
        Next::Call(IoCall::child(SandboxOperation))
    }
}

impl Operation for DiffAutoOperation {
    type Output = AutoDiffResult;
    type Error = AutoError;

    fn first(&mut self) -> Result<Next<AutoDiffResult>, AutoError> {
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
                    None => Ok(self.next_applet()),
                }
            }
            DiffStep::Applets => {
                let Some((applet_id, is_sub)) = self.applet_ids.pop() else {
                    return Err(IoError::Unfinished.into());
                };
                if let Ok(Some(applet)) = result.child::<Option<Applet>, AppletError>()? {
                    if applet.access == Access::Public || applet.guests.is_some() {
                        self.outside_input = true;
                    }
                    if is_sub && applet.grants.start.is_some() {
                        self.start_applets.push((applet_id, applet.slug));
                    }
                }
                Ok(self.next_applet())
            }
            DiffStep::Backends => {
                let backends = result
                    .child::<Vec<Sandboxing>, AppletError>()?
                    .unwrap_or_default();
                let peers = std::mem::take(&mut self.peers);
                let start_applets = std::mem::take(&mut self.start_applets);
                let auto = self.auto.as_ref().ok_or(IoError::Unfinished)?;
                let (next_spec, current, pending) = self.next_spec(auto);
                let ctx = DiffContext {
                    outside_input: self.outside_input,
                    unsandboxed: next_spec.has_runs() && !public_ready(&backends),
                    peers,
                    start_applets,
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
