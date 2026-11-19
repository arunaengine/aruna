//! Finds the nodes a relayed call may go to, from the realm configuration and node documents.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::NodeId;
use aruna_core::admin_documents::AdminDocumentTarget;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE;
use aruna_core::node_state::{NodeInfoError, ReadInfoOperation};
use aruna_core::operation::Operation;
use aruna_core::reducer::decode_reducer_state;
use aruna_core::rows::read_row;
use aruna_core::storage_entries::reducer_state_key;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::structs::storage::node_info::NodeInfoDocument;
use thiserror::Error;
use tracing::warn;

use crate::device::realm_documents::installed_management_urls;
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::users::merge::{Coordinator, coordinator};

/// Why no target of a relayed call is known.
#[derive(Debug, Error, PartialEq)]
pub enum TargetError {
    #[error("the realm configuration is not readable")]
    Config,
    #[error("no management node is known")]
    NoNode,
    #[error(transparent)]
    Io(#[from] IoError),
}

/// The realm's nodes, with the api urls their documents publish.
#[derive(Debug, PartialEq)]
pub struct RealmNodes {
    pub config: RealmConfigDocument,
    pub urls: BTreeMap<NodeId, String>,
}

/// The management nodes in realm order, each with its published url if any.
pub fn management_node_urls(
    config: &RealmConfigDocument,
    url_of: impl Fn(&NodeId) -> Option<String>,
) -> Vec<(NodeId, Option<String>)> {
    (config.nodes.iter())
        .filter(|node| matches!(node.kind, RealmNodeKind::Management))
        .filter_map(|node| node.node_id.parse::<NodeId>().ok())
        .map(|node_id| {
            let url = url_of(&node_id);
            (node_id, url)
        })
        .collect()
}

/// The targets a relayed route is tried against. A device holds no peer node-info document, so
/// the list a realm node installed on it is the only address it has for a management node.
pub fn relay_targets(peers: Vec<String>, installed: Vec<String>) -> Vec<String> {
    match peers.is_empty() {
        true => installed,
        false => peers,
    }
}

/// Management peers in node-id order, this node excluded. The order is stable so repeated calls
/// pin one peer: an onboarding secret lives in one node's store and its status must return there.
pub fn peer_management_urls(current: NodeId, nodes: &RealmNodes) -> Vec<String> {
    let ordered: BTreeMap<NodeId, String> =
        management_node_urls(&nodes.config, |node| nodes.urls.get(node).cloned())
            .into_iter()
            .filter(|(node_id, _)| *node_id != current)
            .filter_map(|(node_id, url)| url.map(|url| (node_id, url)))
            .collect();
    let mut urls: Vec<String> = Vec::new();
    for url in ordered.into_values() {
        if !urls.contains(&url) {
            urls.push(url);
        }
    }
    urls
}

fn document_read(config: &RealmConfigDocument) -> IoCall {
    let nodes = config.nodes.iter();
    let node_ids = nodes.filter_map(|node| node.node_id.parse().ok()).collect();
    IoCall::child(ReadInfoOperation::new(node_ids))
}

/// The api urls in a document read; documents that cannot be read publish none.
fn published_urls(result: IoResult) -> Result<BTreeMap<NodeId, String>, IoError> {
    let read = result.child::<BTreeMap<NodeId, NodeInfoDocument>, NodeInfoError>()?;
    let documents = read.unwrap_or_else(|error| {
        warn!(error = %error, "Failed to load node info documents for a relay");
        BTreeMap::new()
    });
    let urls = documents.into_iter();
    Ok(urls
        .filter_map(|(node, document)| Some((node, document.urls.api?)))
        .collect())
}

fn read_config(result: IoResult) -> Result<RealmConfigDocument, TargetError> {
    (result.child::<RealmConfigDocument, GetConfigError>()?).map_err(|_| TargetError::Config)
}

/// Reads the realm configuration and the urls its nodes publish.
#[derive(Debug, PartialEq)]
pub struct RealmNodesOperation {
    realm_id: RealmId,
    config: Option<RealmConfigDocument>,
}

impl RealmNodesOperation {
    pub fn new(realm_id: RealmId) -> Self {
        Self {
            realm_id,
            config: None,
        }
    }
}

impl Operation for RealmNodesOperation {
    type Output = RealmNodes;
    type Error = TargetError;

    fn first(&mut self) -> Result<Next<RealmNodes>, TargetError> {
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(
            self.realm_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<RealmNodes>, TargetError> {
        match self.config.take() {
            Some(config) => Ok(Next::Done(RealmNodes {
                config,
                urls: published_urls(result)?,
            })),
            None => {
                let config = read_config(result)?;
                let read = document_read(&config);
                self.config = Some(config);
                Ok(Next::Call(read))
            }
        }
    }
}

/// Where the central assistant's holder is, `None` when it is this node: the first management
/// node in realm order. A device knows no addresses and asks the node it was installed from.
#[derive(Debug, PartialEq)]
pub struct HolderTargetOperation {
    realm_id: RealmId,
    here: NodeId,
    asked_installed: bool,
}

impl HolderTargetOperation {
    pub fn new(realm_id: RealmId, here: NodeId) -> Self {
        Self {
            realm_id,
            here,
            asked_installed: false,
        }
    }
}

impl Operation for HolderTargetOperation {
    type Output = Option<(Option<NodeId>, String)>;
    type Error = TargetError;

    fn first(&mut self) -> Result<Next<Self::Output>, TargetError> {
        Ok(Next::Call(IoCall::child(RealmNodesOperation::new(
            self.realm_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, TargetError> {
        if self.asked_installed {
            let installed = result.child::<Vec<String>, IoError>()?.unwrap_or_default();
            let first = installed.into_iter().next().ok_or(TargetError::NoNode)?;
            return Ok(Next::Done(Some((None, first))));
        }
        let nodes = result.child::<RealmNodes, TargetError>()??;
        let managers = management_node_urls(&nodes.config, |node| nodes.urls.get(node).cloned());
        match managers.into_iter().next() {
            Some((node, _)) if node == self.here => Ok(Next::Done(None)),
            Some((node, Some(url))) => Ok(Next::Done(Some((Some(node), url)))),
            _ => {
                self.asked_installed = true;
                let installed = installed_management_urls(self.realm_id);
                Ok(Next::Call(IoCall::child(installed)))
            }
        }
    }
}

/// Where a coordinated route goes.
#[derive(Debug, PartialEq)]
pub enum CoordinatorTarget {
    /// This node answers: it is the pinned coordinator, or a management node while none is.
    Here,
    /// The pinned coordinator's url.
    Node(String),
    /// Any management node may answer.
    Managers,
}

#[derive(Debug, PartialEq)]
enum CoordinatorStep {
    Config,
    State(RealmConfigDocument),
    Urls {
        config: RealmConfigDocument,
        node: NodeId,
    },
}

/// Finds the merge coordinator's place. An unknown pinned node has no url, so the call fails.
#[derive(Debug, PartialEq)]
pub struct CoordinatorTargetOperation {
    realm_id: RealmId,
    here: NodeId,
    is_management: bool,
    step: CoordinatorStep,
}

impl CoordinatorTargetOperation {
    pub fn new(realm_id: RealmId, here: NodeId, is_management: bool) -> Self {
        Self {
            realm_id,
            here,
            is_management,
            step: CoordinatorStep::Config,
        }
    }
}

impl Operation for CoordinatorTargetOperation {
    type Output = CoordinatorTarget;
    type Error = TargetError;

    fn first(&mut self) -> Result<Next<CoordinatorTarget>, TargetError> {
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(
            self.realm_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<CoordinatorTarget>, TargetError> {
        match std::mem::replace(&mut self.step, CoordinatorStep::Config) {
            CoordinatorStep::Config => {
                self.step = CoordinatorStep::State(read_config(result)?);
                let target = AdminDocumentTarget::RealmConfig {
                    realm_id: self.realm_id,
                };
                let key = reducer_state_key(&target);
                Ok(Next::Call(read_row(DOCUMENT_STATE_KEYSPACE, key)))
            }
            CoordinatorStep::State(config) => {
                let state = result.value()?;
                let state = state.and_then(|bytes| decode_reducer_state(&bytes).ok());
                match coordinator(&config, state.as_ref()) {
                    Coordinator::Pinned(node) if node == self.here => {
                        Ok(Next::Done(CoordinatorTarget::Here))
                    }
                    Coordinator::Pinned(node) => {
                        let read = document_read(&config);
                        self.step = CoordinatorStep::Urls { config, node };
                        Ok(Next::Call(read))
                    }
                    Coordinator::Unpinned | Coordinator::Left if self.is_management => {
                        Ok(Next::Done(CoordinatorTarget::Here))
                    }
                    Coordinator::Unpinned | Coordinator::Left => {
                        Ok(Next::Done(CoordinatorTarget::Managers))
                    }
                }
            }
            CoordinatorStep::Urls { config, node } => {
                let urls = published_urls(result)?;
                (management_node_urls(&config, |node| urls.get(node).cloned()).into_iter())
                    .find(|(node_id, _)| *node_id == node)
                    .and_then(|(_, url)| url)
                    .map(|url| Next::Done(CoordinatorTarget::Node(url)))
                    .ok_or(TargetError::NoNode)
            }
        }
    }
}

/// The urls relayed routes go to: the management peers, or the list installed on a device.
#[derive(Debug, PartialEq)]
pub struct PeerTargetsOperation {
    realm_id: RealmId,
    here: NodeId,
    peers: Option<Vec<String>>,
}

impl PeerTargetsOperation {
    pub fn new(realm_id: RealmId, here: NodeId) -> Self {
        Self {
            realm_id,
            here,
            peers: None,
        }
    }
}

impl Operation for PeerTargetsOperation {
    type Output = Vec<String>;
    type Error = TargetError;

    fn first(&mut self) -> Result<Next<Vec<String>>, TargetError> {
        Ok(Next::Call(IoCall::child(RealmNodesOperation::new(
            self.realm_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<String>>, TargetError> {
        match self.peers.take() {
            Some(peers) => {
                let installed = result.child::<Vec<String>, IoError>()?.unwrap_or_default();
                Ok(Next::Done(relay_targets(peers, installed)))
            }
            None => {
                let nodes = result.child::<RealmNodes, TargetError>()??;
                self.peers = Some(peer_management_urls(self.here, &nodes));
                let installed = installed_management_urls(self.realm_id);
                Ok(Next::Call(IoCall::child(installed)))
            }
        }
    }
}

#[cfg(test)]
#[path = "targets_tests.rs"]
mod tests;
