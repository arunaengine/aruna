//! Runs one host call after its admission: the checks and the call rate of `AdmitCall`, then the
//! call a plan makes for the checked install, as one operation.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::plugin_runs::CallIntent;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::GroupId;
use aruna_plugin_types::permission::Permission;
use ulid::Ulid;

use super::check::{CallError, RunScope};
use super::grants::object_path;
use super::host_calls::{CallFailure, ObjectInfoCall, ObjectListCall, StateCall, StateChange};
use super::host_steps::{AdmitCall, NativeCall};
use super::job_work::CancelWork;
use super::state::{ReadState, read_state};
use super::submit::WorkRequest;
use super::work_calls::JobOutputs;
use crate::jobs::records::pages::ServingNode;

/// The call an admitted host call makes for the checked install at the run's time, and the error
/// both answer with.
pub trait AdmitPlan: Send + std::fmt::Debug + PartialEq + 'static {
    type Call: Operation + 'static;
    type Error: From<CallError>
        + From<<Self::Call as Operation>::Error>
        + From<IoError>
        + Send
        + std::fmt::Debug
        + PartialEq;

    fn call(self, install: &PluginInstall, now_ms: u64) -> Result<Self::Call, Self::Error>;
}

/// Admits a host call, optionally on a target, then runs the call `plan` makes.
#[derive(Debug, PartialEq)]
pub struct Admitted<P> {
    admit: Option<AdmitCall>,
    plan: Option<P>,
    now_ms: u64,
}

impl<P: AdmitPlan> Admitted<P> {
    pub fn new(scope: &RunScope, target: Option<(Permission, &str)>, plan: P) -> Self {
        Self {
            admit: Some(AdmitCall::new(scope, target)),
            plan: Some(plan),
            now_ms: 0,
        }
    }
}

impl<P> Operation for Admitted<P>
where
    P: AdmitPlan,
    <P::Call as Operation>::Output: PartialEq + 'static,
    <P::Call as Operation>::Error: PartialEq + 'static,
{
    type Output = <P::Call as Operation>::Output;
    type Error = P::Error;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<Self::Output>, P::Error> {
        let admit = self.admit.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(admit)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, P::Error> {
        if self.plan.is_none() {
            let answer = result.child::<Self::Output, <P::Call as Operation>::Error>()?;
            return Ok(Next::Done(answer?));
        }
        // A refused admission keeps the plan, so its call is never built.
        let install = result.child::<PluginInstall, CallError>()??;
        let plan = self.plan.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(plan.call(&install, self.now_ms)?)))
    }
}

/// One page of the outputs of the install's job `id`.
#[derive(Debug, PartialEq)]
pub struct OutputsAsk {
    pub local: NodeId,
    pub id: String,
    pub cursor: Option<String>,
}

impl AdmitPlan for OutputsAsk {
    type Call = JobOutputs;
    type Error = CallError;

    fn call(self, install: &PluginInstall, _: u64) -> Result<JobOutputs, CallError> {
        Ok(JobOutputs::new(
            (install, self.local),
            &self.id,
            self.cursor,
        ))
    }
}

/// The cancel of the install's job `id`, at the run's time.
#[derive(Debug, PartialEq)]
pub struct CancelAsk {
    pub id: String,
}

impl AdmitPlan for CancelAsk {
    type Call = CancelWork;
    type Error = CallError;

    fn call(self, install: &PluginInstall, now_ms: u64) -> Result<CancelWork, CallError> {
        Ok(CancelWork::new(install, &self.id, now_ms))
    }
}

/// The install's private state value at `key`.
#[derive(Debug, PartialEq)]
pub struct StateAsk {
    pub key: String,
}

impl AdmitPlan for StateAsk {
    type Call = ReadState;
    type Error = CallError;

    fn call(self, install: &PluginInstall, _: u64) -> Result<ReadState, CallError> {
        Ok(read_state(install.install_id, &self.key))
    }
}

/// A change of the install's private state, as call `run_id` asks for it.
#[derive(Debug, PartialEq)]
pub struct ChangeAsk {
    pub run_id: Ulid,
    pub change: StateChange,
}

impl AdmitPlan for ChangeAsk {
    type Call = StateCall;
    type Error = CallFailure;

    fn call(self, install: &PluginInstall, _: u64) -> Result<StateCall, CallFailure> {
        Ok(StateCall::new(
            (self.run_id, install.install_id),
            self.change,
        ))
    }
}

/// The information of the object at the full path `path`, the current or the given version.
#[derive(Debug, PartialEq)]
pub struct InfoAsk {
    pub place: (RealmId, NodeId),
    pub path: String,
    pub version: Option<String>,
}

impl AdmitPlan for InfoAsk {
    type Call = ObjectInfoCall;
    type Error = CallFailure;

    fn call(self, install: &PluginInstall, _: u64) -> Result<ObjectInfoCall, CallFailure> {
        let version = self.version.as_deref();
        Ok(ObjectInfoCall::new(
            self.place, install, &self.path, version,
        )?)
    }
}

/// One page of the objects below `prefix`, a path as the plugin gave it, of the run's group.
#[derive(Debug, PartialEq)]
pub struct ListAsk {
    pub place: (RealmId, NodeId),
    pub group_id: GroupId,
    pub prefix: String,
    pub cursor: Option<String>,
}

impl AdmitPlan for ListAsk {
    type Call = ObjectListCall;
    type Error = CallFailure;

    fn call(self, install: &PluginInstall, _: u64) -> Result<ObjectListCall, CallFailure> {
        let prefix = object_path(self.place, self.group_id, &self.prefix)?;
        let cursor = self.cursor.as_deref();
        Ok(ObjectListCall::new(self.place, install, &prefix, cursor)?)
    }
}

/// Native work the run starts through its call history, once the run may call.
#[derive(Debug, PartialEq)]
pub struct NativeAsk {
    pub scope: RunScope,
    pub intent: CallIntent,
    pub request: WorkRequest,
    /// The time and id the work is minted with, and the node that serves it.
    pub place: ((u64, Ulid), ServingNode),
}

impl AdmitPlan for NativeAsk {
    type Call = NativeCall;
    type Error = CallFailure;

    fn call(self, _: &PluginInstall, _: u64) -> Result<NativeCall, CallFailure> {
        Ok(NativeCall::new(
            &self.scope,
            self.intent,
            self.request,
            self.place,
        ))
    }
}

#[cfg(test)]
#[path = "admitted_tests.rs"]
mod tests;
