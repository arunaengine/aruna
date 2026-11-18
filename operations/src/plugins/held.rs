//! Gets released secrets for the runs of this node: node secrets and a management node's own
//! releases here, others from the management nodes in realm order, reusing held fields per run.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::SecretBytes;
use aruna_core::injection::{OutgoingRequest, inject_secret};
use aruna_core::operation::Operation;
use aruna_core::plugin_installs::{BoundScope, PluginInstall};
use aruna_core::plugins::{ReleaseError, ReleaseRequest, Released};
use aruna_core::secrets::{Delivery, ResponseExposure, SecretFields};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_plugins::broker::{ErrorKind, HostError};
use ulid::Ulid;

use super::artifacts::managers;
use super::check::CallError;
use super::install::{pick_version, read_manifest};
use super::outgoing::outgoing;
use super::peer::{ask_peer, peer_answer};
use super::secret_use::{ReleaseFailure, ReleaseSecret};
use super::unavailable;
use super::work::WorkRow;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::realm::mutate_placement::is_management;

/// The fields one run holds, by slot, with the value write id they belong to. They are reused
/// only when a fresh check answers that this value is still current.
#[derive(Default)]
pub struct HeldSecrets {
    held: BTreeMap<String, (Ulid, SecretBytes)>,
}

impl HeldSecrets {
    /// Names the value held for the request's slot, so a fresh check may answer without it.
    pub fn prepare(&self, request: &mut ReleaseRequest) {
        request.held = self.held.get(&request.slot).map(|(value_id, _)| *value_id);
    }

    /// The fields of a checked use of `slot`: those just released, or the held ones it confirms.
    pub fn absorb(
        &mut self,
        slot: &str,
        mut released: Released,
    ) -> Result<(Released, SecretFields), ReleaseError> {
        let bytes = match released.fields.take() {
            Some(bytes) => {
                let held = (released.value_id, bytes.clone());
                self.held.insert(slot.to_string(), held);
                bytes
            }
            None => match self.held.get(slot) {
                Some((value_id, bytes)) if *value_id == released.value_id => bytes.clone(),
                _ => {
                    let lost = "the answer named a value this run does not hold";
                    return Err(ReleaseError::Unavailable(lost.into()));
                }
            },
        };
        let fields = SecretFields::decode(&bytes);
        let fields = fields.map_err(|error| ReleaseError::Unavailable(error.to_string()))?;
        Ok((released, fields))
    }
}

/// One release for a run on `local`. A refusal ends the search; an unavailable node is skipped.
#[derive(Debug, PartialEq)]
pub(crate) struct AskRelease {
    config: Arc<RealmConfigDocument>,
    local: NodeId,
    request: ReleaseRequest,
    node_bound: bool,
    peers: VecDeque<NodeId>,
    asked: Option<NodeId>,
    last: String,
}

impl AskRelease {
    pub(crate) fn new(
        config: Arc<RealmConfigDocument>,
        local: NodeId,
        request: ReleaseRequest,
    ) -> Self {
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
pub(crate) struct PreparedRequest {
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

#[cfg(test)]
#[path = "held_tests.rs"]
mod tests;
