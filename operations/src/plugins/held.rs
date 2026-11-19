//! Gets released secrets for the runs of this node: node secrets and a management node's own
//! releases here, others from the management nodes in realm order, reusing a run's held fields.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::sync::Arc;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::id::NodeId;
use aruna_core::injection::{OutgoingRequest, inject_secret};
use aruna_core::operation::Operation;
use aruna_core::plugin_installs::{BoundScope, PluginInstall};
use aruna_core::plugins::{PluginEffect, PluginEvent, ReleaseError, ReleaseRequest, Released};
use aruna_core::secrets::{Delivery, ResponseExposure, SecretFields};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_plugin_types::calls::{ErrorKind, HostError};
use ulid::Ulid;

use super::artifacts::managers;
use super::check::{CallError, RunScope};
use super::host_steps::{
    CheckConfigured, CheckReply, ConfiguredCall, ConnectionAsk, ConnectionPlan, PrepareConnection,
    ReplyPlan,
};
use super::install::{pick_version, read_manifest};
use super::outgoing::outgoing;
use super::peer::{ask_peer, peer_answer};
use super::secret_use::{ReleaseFailure, ReleaseSecret};
use super::unavailable;
use super::work::{RequestContext, WorkRow};
use crate::realm::mutate_placement::is_management;
use aruna_core::metadata::protocol::MetadataTransportMessage;

/// The fields of a release that brought them; one that names a held value is refused.
pub(crate) fn fresh_fields(
    mut released: Released,
) -> Result<(Released, SecretFields), ReleaseError> {
    let lost = || ReleaseError::Unavailable(NOT_HELD.into());
    let bytes = released.fields.take().ok_or_else(lost)?;
    let fields = SecretFields::decode(&bytes);
    let fields = fields.map_err(|error| ReleaseError::Unavailable(error.to_string()))?;
    Ok((released, fields))
}

const NOT_HELD: &str = "the answer named a value this run does not hold";

/// The plugin event a host state effect answered.
fn plugin_answer(result: IoResult) -> Result<PluginEvent, IoError> {
    match result.event()? {
        Event::Plugin(event) => Ok(event),
        other => Err(IoError::Unexpected {
            expected: "Event::Plugin",
            got: other.label(),
        }),
    }
}

#[derive(Debug, PartialEq)]
enum ObtainPhase {
    Held(ReleaseRequest),
    Release,
    Hold(Box<Released>),
}

/// One checked release of a slot's secret for run `run_id` and its broker `holder`. The request
/// names the newest value the holder keeps, so a fresh check may answer without its fields;
/// fields that come are kept for the holder in the plugin host state.
#[derive(Debug, PartialEq)]
pub struct ObtainSecret {
    config: Arc<RealmConfigDocument>,
    local: NodeId,
    run_id: Ulid,
    holder: Ulid,
    slot: String,
    phase: ObtainPhase,
}

impl ObtainSecret {
    pub fn new(
        (config, local): (Arc<RealmConfigDocument>, NodeId),
        (run_id, holder): (Ulid, Ulid),
        mut request: ReleaseRequest,
    ) -> Self {
        request.run_id = Some(run_id);
        Self {
            config,
            local,
            run_id,
            holder,
            slot: request.slot.clone(),
            phase: ObtainPhase::Held(request),
        }
    }

    fn plugin_call(effect: PluginEffect) -> IoCall {
        IoCall::Effect(Box::new(Effect::Plugin(effect)))
    }
}

impl Operation for ObtainSecret {
    type Output = (Released, SecretFields);
    type Error = CallError;

    fn first(&mut self) -> Result<Next<Self::Output>, CallError> {
        let (run_id, holder, slot) = (self.run_id, self.holder, self.slot.clone());
        let held = PluginEffect::HeldValue {
            run_id,
            holder,
            slot,
        };
        Ok(Next::Call(Self::plugin_call(held)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, CallError> {
        match std::mem::replace(&mut self.phase, ObtainPhase::Release) {
            ObtainPhase::Held(mut request) => {
                let PluginEvent::Held(held) = plugin_answer(result)? else {
                    return Err(unexpected_plugin("PluginEvent::Held"));
                };
                request.held = held;
                let ask = AskRelease::new(self.config.clone(), self.local, request);
                Ok(Next::Call(IoCall::child(ask)))
            }
            ObtainPhase::Release => {
                let released = result.child::<Released, ReleaseFailure>()?;
                let mut released = released.map_err(|failure| call_failure(failure.0))?;
                let hold = PluginEffect::HoldValue {
                    run_id: self.run_id,
                    holder: self.holder,
                    slot: self.slot.clone(),
                    value_id: released.value_id,
                    fields: released.fields.take(),
                };
                self.phase = ObtainPhase::Hold(Box::new(released));
                Ok(Next::Call(Self::plugin_call(hold)))
            }
            ObtainPhase::Hold(released) => {
                let PluginEvent::Fields(bytes) = plugin_answer(result)? else {
                    return Err(unexpected_plugin("PluginEvent::Fields"));
                };
                let bytes = bytes.ok_or_else(|| CallError(unavailable(NOT_HELD)))?;
                let fields = SecretFields::decode(&bytes);
                let fields = fields.map_err(|error| CallError(unavailable(error.to_string())))?;
                Ok(Next::Done((*released, fields)))
            }
        }
    }
}

fn unexpected_plugin(expected: &'static str) -> CallError {
    let got = "another plugin event";
    IoError::Unexpected { expected, got }.into()
}

/// One release for a run on `local`. A refusal ends the search; an unavailable node is skipped.
#[derive(Debug, PartialEq)]
pub struct AskRelease {
    config: Arc<RealmConfigDocument>,
    local: NodeId,
    request: ReleaseRequest,
    node_bound: bool,
    peers: VecDeque<NodeId>,
    asked: Option<NodeId>,
    last: String,
}

impl AskRelease {
    pub fn new(config: Arc<RealmConfigDocument>, local: NodeId, request: ReleaseRequest) -> Self {
        let install =
            (config.installs.iter()).find(|install| install.install_id == request.install_id);
        let binding = install.and_then(|install| install.secrets.get(&request.slot));
        let node_bound = binding.is_some_and(|binding| binding.scope == BoundScope::Node);
        let peers = managers(&config).filter(|node| *node != local).collect();
        Self {
            config,
            local,
            request,
            node_bound,
            peers,
            asked: None,
            last: "the realm has no other management node".to_string(),
        }
    }

    fn ask_next(&mut self) -> Result<Next<Released>, ReleaseFailure> {
        while let Some(node) = self.peers.pop_front() {
            let request = self.request.clone();
            let message = MetadataTransportMessage::ReleaseSecret { request };
            let Some(call) = ask_peer(node, &message) else {
                self.last = format!("{node}: the request cannot be encoded");
                continue;
            };
            self.asked = Some(node);
            return Ok(Next::Call(call));
        }
        Err(ReleaseError::Unavailable(self.last.clone()).into())
    }
}

impl Operation for AskRelease {
    type Output = Released;
    type Error = ReleaseFailure;

    fn first(&mut self) -> Result<Next<Released>, ReleaseFailure> {
        if self.node_bound || is_management(&self.config, self.local) {
            let nodes = (self.local, self.local);
            let release = ReleaseSecret::new(self.config.clone(), nodes, self.request.clone());
            return Ok(Next::Call(IoCall::child(release)));
        }
        self.ask_next()
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Released>, ReleaseFailure> {
        let Some(node) = self.asked.take() else {
            let here = result.child::<Released, ReleaseFailure>()?;
            let unavailable = matches!(&here, Err(ReleaseFailure(ReleaseError::Unavailable(_))));
            if self.node_bound || !unavailable {
                return Ok(Next::Done(here?));
            }
            return self.ask_next();
        };
        let why = match peer_answer(node, result)? {
            Some(MetadataTransportMessage::SecretReleased {
                result: Err(ReleaseError::Unavailable(why)),
            }) => why,
            Some(MetadataTransportMessage::SecretReleased { result }) => {
                return Ok(Next::Done(result?));
            }
            _ => format!("node {node} did not answer"),
        };
        self.last = format!("{node}: {why}");
        self.ask_next()
    }
}

fn call_failure(error: ReleaseError) -> CallError {
    CallError(match error {
        ReleaseError::Refused(_) => HostError::new(ErrorKind::Denied, error.to_string()),
        ReleaseError::Unavailable(why) => unavailable(why),
    })
}

/// A transfer's request with its secret added, the exposure the rule allows now, and the secret's
/// fields for echo removal.
#[derive(Debug, PartialEq)]
pub struct PreparedRequest {
    pub outgoing: OutgoingRequest,
    pub exposure: ResponseExposure,
    pub fields: SecretFields,
}

/// The row's request checked against the install's current manifest, with the connection's secret
/// released and added.
#[derive(Debug, PartialEq)]
pub(crate) struct PrepareRequest {
    config: Arc<RealmConfigDocument>,
    local: NodeId,
    install: PluginInstall,
    row: WorkRow,
    outgoing: Option<OutgoingRequest>,
}

impl PrepareRequest {
    pub(crate) fn new(
        config: Arc<RealmConfigDocument>,
        local: NodeId,
        install: &PluginInstall,
        row: &WorkRow,
    ) -> Self {
        Self {
            config,
            local,
            install: install.clone(),
            row: row.clone(),
            outgoing: None,
        }
    }
}

impl Operation for PrepareRequest {
    type Output = PreparedRequest;
    type Error = CallError;

    fn first(&mut self) -> Result<Next<PreparedRequest>, CallError> {
        let context = self.row.context.as_ref();
        let no_request = || CallError(HostError::new(ErrorKind::Invalid, "no request"));
        let context = context.ok_or_else(no_request)?;
        let (plugin, digest) = (&self.install.plugin, &self.install.digest);
        let manifest = pick_version(&self.config, plugin, Some(digest));
        let manifest = manifest.and_then(|plugin| read_manifest(&plugin));
        let manifest = manifest.map_err(|error| CallError(unavailable(error.to_string())))?;
        let (request, slot) = outgoing(&manifest, context)?;
        let Some(slot) = slot else {
            return Ok(Next::Done(PreparedRequest {
                outgoing: request,
                exposure: ResponseExposure::Full,
                fields: SecretFields::new(Default::default()),
            }));
        };
        let release = ReleaseRequest {
            install_id: self.install.install_id,
            slot,
            delivery: Delivery::Inject,
            target: Some((request.method.clone(), request.url.to_string())),
            held: None,
            run_id: Some(self.row.run_id),
        };
        self.outgoing = Some(request);
        let ask = AskRelease::new(self.config.clone(), self.local, release);
        Ok(Next::Call(IoCall::child(ask)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PreparedRequest>, CallError> {
        let mut outgoing = self.outgoing.take().ok_or(IoError::Unfinished)?;
        let released = result.child::<Released, ReleaseFailure>()?;
        let mut released = released.map_err(|failure| call_failure(failure.0))?;
        let no_value = || CallError(unavailable("the release answered no value"));
        let bytes = released.fields.take().ok_or_else(no_value)?;
        let fields = SecretFields::decode(&bytes);
        let fields = fields.map_err(|error| CallError(unavailable(error.to_string())))?;
        let injected = inject_secret(&released.kind, &released.settings, &fields, &mut outgoing);
        injected
            .map_err(|error| CallError(HostError::new(ErrorKind::Denied, error.to_string())))?;
        Ok(Next::Done(PreparedRequest {
            outgoing,
            exposure: released.exposure,
            fields,
        }))
    }
}

/// A request with a released secret added as its kind says, and the release and its fields.
#[derive(Debug, PartialEq)]
pub struct Injected {
    pub outgoing: OutgoingRequest,
    pub released: Released,
    pub fields: SecretFields,
}

/// Releases a slot's secret for this run's request and adds it to the request.
#[derive(Debug, PartialEq)]
pub struct InjectSecret {
    obtain: Option<ObtainSecret>,
    outgoing: Option<OutgoingRequest>,
}

impl InjectSecret {
    pub fn new(
        call: &ConfiguredCall,
        (local, holder): (NodeId, (Ulid, Ulid)),
        slot: &str,
        outgoing: OutgoingRequest,
    ) -> Self {
        let request = ReleaseRequest {
            install_id: call.install.install_id,
            slot: slot.to_string(),
            delivery: Delivery::Inject,
            target: Some((outgoing.method.clone(), outgoing.url.to_string())),
            held: None,
            run_id: None,
        };
        let obtain = ObtainSecret::new((call.config.clone(), local), holder, request);
        Self {
            obtain: Some(obtain),
            outgoing: Some(outgoing),
        }
    }
}

impl Operation for InjectSecret {
    type Output = Injected;
    type Error = CallError;

    fn first(&mut self) -> Result<Next<Injected>, CallError> {
        let obtain = self.obtain.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(obtain)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Injected>, CallError> {
        let mut outgoing = self.outgoing.take().ok_or(IoError::Unfinished)?;
        let (released, fields) = result.child::<(Released, SecretFields), CallError>()??;
        let injected = inject_secret(&released.kind, &released.settings, &fields, &mut outgoing);
        let denied = |error: String| CallError(HostError::new(ErrorKind::Denied, error));
        injected.map_err(|error| denied(error.to_string()))?;
        Ok(Next::Done(Injected {
            outgoing,
            released,
            fields,
        }))
    }
}

/// A checked connection request with its secret added: the exposure its secret's rule allows,
/// the secret's fields for echo removal, the request as a work row keeps it, and whether the
/// connection injects a secret.
#[derive(Debug, PartialEq)]
pub struct ReadyRequest {
    pub outgoing: OutgoingRequest,
    pub exposure: ResponseExposure,
    pub fields: SecretFields,
    pub context: RequestContext,
    pub secret: bool,
}

/// The per call check of a connection request, the address below the connection and the guest's
/// own headers, then the secret released and added; every use of a secret passes the check.
#[derive(Debug, PartialEq)]
pub struct ReadyConnection {
    prepare: Option<PrepareConnection>,
    place: (NodeId, (Ulid, Ulid)),
    context: Option<RequestContext>,
}

impl ReadyConnection {
    /// The request of `ask` for the run's broker `holder`.
    pub fn new((scope, holder): (&RunScope, Ulid), ask: ConnectionAsk) -> Self {
        Self {
            prepare: Some(PrepareConnection::new(scope, ask)),
            place: (scope.local, (scope.run.run_id, holder)),
            context: None,
        }
    }
}

impl Operation for ReadyConnection {
    type Output = ReadyRequest;
    type Error = CallError;

    fn first(&mut self) -> Result<Next<ReadyRequest>, CallError> {
        let prepare = self.prepare.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(prepare)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ReadyRequest>, CallError> {
        if let Some(context) = self.context.take() {
            let injected = result.child::<Injected, CallError>()??;
            return Ok(Next::Done(ReadyRequest {
                outgoing: injected.outgoing,
                exposure: injected.released.exposure,
                fields: injected.fields,
                context,
                secret: true,
            }));
        }
        let plan = result.child::<ConnectionPlan, CallError>()??;
        let ConnectionPlan {
            call,
            context,
            outgoing,
            slot,
        } = plan;
        let Some(slot) = slot else {
            return Ok(Next::Done(ReadyRequest {
                outgoing,
                exposure: ResponseExposure::Full,
                fields: SecretFields::new(Default::default()),
                context,
                secret: false,
            }));
        };
        self.context = Some(context);
        let inject = InjectSecret::new(&call, self.place, &slot, outgoing);
        Ok(Next::Call(IoCall::child(inject)))
    }
}

/// Whether a kept reply of `context` may be shown now: the connection is still granted and
/// declared, and its secret's rule still releases for the request and shows it whole.
#[derive(Debug, PartialEq)]
pub struct ReplyShown {
    check: Option<CheckReply>,
    place: (NodeId, (Ulid, Ulid)),
    injecting: bool,
}

impl ReplyShown {
    pub fn new((scope, holder): (&RunScope, Ulid), context: &RequestContext) -> Self {
        Self {
            check: Some(CheckReply::new(scope, context)),
            place: (scope.local, (scope.run.run_id, holder)),
            injecting: false,
        }
    }
}

impl Operation for ReplyShown {
    type Output = bool;
    type Error = CallError;

    fn first(&mut self) -> Result<Next<bool>, CallError> {
        let check = self.check.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(check)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, CallError> {
        if self.injecting {
            let injected = result.child::<Injected, CallError>();
            let full = |injected: Injected| injected.released.exposure == ResponseExposure::Full;
            return Ok(Next::Done(
                injected.is_ok_and(|injected| injected.is_ok_and(full)),
            ));
        }
        let Ok(Ok(Some(plan))) = result.child::<Option<ReplyPlan>, CallError>() else {
            return Ok(Next::Done(false));
        };
        let Some(slot) = plan.slot else {
            return Ok(Next::Done(true));
        };
        self.injecting = true;
        let inject = InjectSecret::new(&plan.call, self.place, &slot, plan.outgoing);
        Ok(Next::Call(IoCall::child(inject)))
    }
}

/// The fields of a secret slot's value handed to the code, after the per call check.
#[derive(Debug, PartialEq)]
pub struct SecretValue {
    check: Option<CheckConfigured>,
    place: (NodeId, (Ulid, Ulid)),
    slot: String,
    obtaining: bool,
}

impl SecretValue {
    pub fn new((scope, holder): (&RunScope, Ulid), slot: &str) -> Self {
        Self {
            check: Some(CheckConfigured::secret(scope, slot)),
            place: (scope.local, (scope.run.run_id, holder)),
            slot: slot.to_string(),
            obtaining: false,
        }
    }
}

impl Operation for SecretValue {
    type Output = SecretFields;
    type Error = CallError;

    fn first(&mut self) -> Result<Next<SecretFields>, CallError> {
        let check = self.check.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(check)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<SecretFields>, CallError> {
        if self.obtaining {
            let (_, fields) = result.child::<(Released, SecretFields), CallError>()??;
            return Ok(Next::Done(fields));
        }
        let call = result.child::<ConfiguredCall, CallError>()??;
        let request = ReleaseRequest {
            install_id: call.install.install_id,
            slot: self.slot.clone(),
            delivery: Delivery::Value,
            target: None,
            held: None,
            run_id: None,
        };
        let (local, holder) = self.place;
        self.obtaining = true;
        let obtain = ObtainSecret::new((call.config, local), holder, request);
        Ok(Next::Call(IoCall::child(obtain)))
    }
}

#[cfg(test)]
#[path = "held_tests.rs"]
mod tests;
