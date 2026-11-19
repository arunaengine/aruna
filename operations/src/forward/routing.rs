//! Decides whether a metadata write runs locally or goes to the holders of its placement.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::fetch::{PeerFrame, peer_call, peer_reply};
use super::transport::MetadataWriteError;
use crate::metadata::create_document::CreateDocumentConfig;
use crate::metadata::create_document::resolve_metadata_id;
use crate::metadata::protocol::{MetadataTransportMessage, WriteAuthError};
use aruna_core::NodeId;
use aruna_core::calls::{IoError, IoResult, Next};
use aruna_core::events::PeerRequestError;
use aruna_core::operation::Operation;
use aruna_core::placement::holds_placement;
use aruna_core::placement::resolve_shard_holders;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::PlacementRef;
use std::time::Duration;
use thiserror::Error;
use tracing::{error, warn};
use ulid::Ulid;

/// Bounds one forwarded write above the transport's own timeouts, which end it first.
const HOLDER_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(20 * 60);

/// Where a metadata write must be applied: topic membership equals the bucket's
/// holder set, so a non-holder cannot publish and the mutation goes to a holder.
/// Widening membership to admit origins would dissolve sharding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataWriteRoute {
    Local,
    /// Holders of the document's bucket in rank order (rank-0 first).
    Forward(Vec<NodeId>),
}

/// Route for a write against `placement`, from the local node's point of view.
/// [`PlacementRef::NIL`] has no governing strategy, so the local node stays the
/// authority. A non-NIL placement without config fails closed as undeliverable.
pub fn write_route(
    config: Option<&RealmConfigDocument>,
    placement: &PlacementRef,
    local_node_id: NodeId,
) -> MetadataWriteRoute {
    let Some(config) = config else {
        return if *placement == PlacementRef::NIL {
            MetadataWriteRoute::Local
        } else {
            MetadataWriteRoute::Forward(Vec::new())
        };
    };
    if holds_placement(config, placement, local_node_id) {
        return MetadataWriteRoute::Local;
    }
    MetadataWriteRoute::Forward(resolve_shard_holders(config, placement))
}

/// Whether the local node is a device; `None` when the config does not list it. Devices hold no
/// metadata bucket, so an effectful write goes to an ingress, which stays its authority.
pub fn is_user_origin(config: &RealmConfigDocument, local_node_id: NodeId) -> Option<bool> {
    let local = local_node_id.to_string();
    let node = config.nodes.iter().find(|node| node.node_id == local)?;
    Some(!node.kind.is_sync_eligible())
}

/// Whether the origin currently holds a structured metadata document's bucket.
/// `None` when the config does not list the local node or the id has no placement.
pub fn origin_holds_document(
    config: &RealmConfigDocument,
    realm_id: RealmId,
    local_node_id: NodeId,
    document_id: Ulid,
) -> Option<bool> {
    if !config.has_node(local_node_id) {
        return None;
    }
    let placement = resolve_metadata_id(config, realm_id, None, document_id).ok()?;
    Some(holds_placement(config, &placement, local_node_id))
}

pub(crate) fn holds_metadata_id(
    config: &RealmConfigDocument,
    realm_id: RealmId,
    local_node_id: NodeId,
    document_id: Ulid,
) -> bool {
    resolve_metadata_id(config, realm_id, None, document_id)
        .is_ok_and(|placement| holds_placement(config, &placement, local_node_id))
}

/// Holders of the document's blind-hashed bucket: candidates for a create the
/// origin cannot place. Every candidate holds that one bucket and a forwarded
/// create stamps exactly it, so the answering candidate cannot change placement.
pub(crate) fn create_forward_holders(
    realm_config: &RealmConfigDocument,
    config: &CreateDocumentConfig,
    document_id: Ulid,
) -> Option<(PlacementRef, Vec<NodeId>)> {
    let placement = resolve_metadata_id(
        realm_config,
        config.actor.realm_id,
        Some(config.group_id),
        document_id,
    )
    .ok()?;
    let holders = resolve_shard_holders(realm_config, &placement);
    Some((placement, holders))
}

pub(crate) fn distinct_holders(holders: &[NodeId]) -> Vec<NodeId> {
    let mut distinct = Vec::with_capacity(holders.len());
    for holder in holders.iter().copied() {
        if !distinct.contains(&holder) {
            distinct.push(holder);
        }
    }
    distinct
}

pub(crate) fn holder_intersection(current: &[NodeId], frozen: &[NodeId]) -> Vec<NodeId> {
    let holders = current
        .iter()
        .copied()
        .filter(|holder| frozen.contains(holder))
        .collect::<Vec<_>>();
    distinct_holders(&holders)
}

/// Why no holder accepted a forwarded metadata write.
#[derive(Debug, Error, PartialEq)]
pub enum HolderWriteError {
    #[error("metadata write requires authentication")]
    Unauthorized,
    #[error("metadata write is forbidden")]
    Forbidden,
    #[error("metadata document not found")]
    NotFound,
    #[error("metadata write is undeliverable: {0}")]
    Undeliverable(String),
}

impl From<IoError> for HolderWriteError {
    fn from(error: IoError) -> Self {
        Self::Undeliverable(error.to_string())
    }
}

impl From<HolderWriteError> for MetadataWriteError {
    fn from(error: HolderWriteError) -> Self {
        match error {
            HolderWriteError::Unauthorized => Self::Unauthorized,
            HolderWriteError::Forbidden => Self::Forbidden,
            HolderWriteError::NotFound => Self::NotFound,
            HolderWriteError::Undeliverable(detail) => Self::Undeliverable(detail),
        }
    }
}

type HolderTurn = Result<Next<MetadataTransportMessage>, HolderWriteError>;

/// Offers a metadata write to its holders in rank order. A holder that may have applied it ends
/// the walk, so the write is never replayed on another one.
#[derive(Debug, PartialEq)]
pub(crate) struct ForwardHoldersOperation {
    frame: Result<PeerFrame, String>,
    holder_count: usize,
    /// Reversed, so the next holder pops off the end.
    remaining: Vec<NodeId>,
    asked: Option<NodeId>,
    tracks_not_found: bool,
    is_update: bool,
    failures: Vec<String>,
    not_found: usize,
    capacity: usize,
}

impl ForwardHoldersOperation {
    /// `local_miss` names this node when it already answered "not found" as a holder, and
    /// `local_capacity` says it answered with a full history.
    pub(crate) fn new(
        local: Option<NodeId>,
        holders: &[NodeId],
        message: &MetadataTransportMessage,
        local_miss: Option<NodeId>,
        local_capacity: bool,
    ) -> Self {
        let holders = distinct_holders(holders);
        let local_node_id = local_miss.or(local);
        let local_held = local_miss.is_some_and(|local| holders.contains(&local));
        let mut remaining: Vec<_> = (holders.iter().copied())
            .filter(|holder| Some(*holder) != local_node_id)
            .collect();
        remaining.reverse();
        Self {
            frame: PeerFrame::encode(message),
            holder_count: holders.len(),
            remaining,
            asked: None,
            tracks_not_found: matches!(
                message,
                MetadataTransportMessage::ForwardUpdateDocument { .. }
                    | MetadataTransportMessage::ForwardDeleteDocument { .. }
                    | MetadataTransportMessage::ForwardPersistentId { .. }
            ),
            is_update: matches!(
                message,
                MetadataTransportMessage::ForwardUpdateDocument { .. }
            ),
            failures: Vec::new(),
            not_found: usize::from(local_held),
            capacity: usize::from(local_capacity && local_held),
        }
    }

    fn ask(&mut self) -> HolderTurn {
        let Some(holder) = self.remaining.pop() else {
            return self.conclude();
        };
        let frame = (self.frame.clone()).map_err(HolderWriteError::Undeliverable)?;
        self.asked = Some(holder);
        Ok(Next::Call(peer_call(
            holder,
            frame.bytes(),
            HOLDER_ATTEMPT_TIMEOUT,
        )))
    }

    /// No holder took the write: every holder's capacity or miss, or an undeliverable write.
    fn conclude(&mut self) -> HolderTurn {
        let all = self.holder_count;
        if all > 0 && self.capacity == all {
            return Err(HolderWriteError::Undeliverable(
                "metadata history capacity reached on every holder".to_string(),
            ));
        }
        if self.tracks_not_found && all > 0 && self.not_found == all {
            return Err(HolderWriteError::NotFound);
        }
        let detail = match self.failures.is_empty() {
            true => "the document's bucket has no reachable holder".to_string(),
            false => self.failures.join("; "),
        };
        error!(
            holders = all,
            detail = %detail,
            "Metadata write reached a non-holder and no holder accepted the forward"
        );
        Err(HolderWriteError::Undeliverable(detail))
    }

    fn answered(&mut self, holder: NodeId, response: MetadataTransportMessage) -> HolderTurn {
        match response {
            MetadataTransportMessage::ForwardedWriteDenied {
                error: WriteAuthError::Unauthorized,
            } => return Err(HolderWriteError::Unauthorized),
            MetadataTransportMessage::ForwardedWriteDenied {
                error: WriteAuthError::Forbidden,
            } => return Err(HolderWriteError::Forbidden),
            MetadataTransportMessage::WriteNotFound if self.tracks_not_found => {
                self.not_found += 1;
            }
            MetadataTransportMessage::WriteNotFound => self.failures.push(format!(
                "{holder}: holder returned not found for a forwarded create"
            )),
            MetadataTransportMessage::ForwardedWriteUnavailable => {
                (self.failures).push(format!("{holder}: holder placement view is unavailable"));
            }
            MetadataTransportMessage::MetadataHistoryCapacity if self.is_update => {
                self.capacity += 1;
            }
            MetadataTransportMessage::MetadataHistoryCapacity => self.failures.push(format!(
                "{holder}: holder returned metadata history capacity for a non-update"
            )),
            MetadataTransportMessage::Reject(error) => {
                warn!(holder = %holder, error = %error, "Holder rejected a forwarded metadata write");
                return Err(HolderWriteError::Undeliverable(format!(
                    "holder `{holder}` rejected the forwarded metadata write; refusing to replay it: {error}"
                )));
            }
            response => return Ok(Next::Done(response)),
        }
        self.ask()
    }

    fn failed(&mut self, holder: NodeId, error: PeerRequestError) -> HolderTurn {
        warn!(holder = %holder, error = ?error, "Failed to forward a metadata write to holder");
        match error {
            PeerRequestError::NotSent(error) => {
                self.failures.push(format!("{holder}: {error}"));
                self.ask()
            }
            error => Err(HolderWriteError::Undeliverable(format!(
                "forward to holder `{holder}` may have applied the metadata write before failing; refusing to replay it: {error:?}"
            ))),
        }
    }
}

impl Operation for ForwardHoldersOperation {
    type Output = MetadataTransportMessage;
    type Error = HolderWriteError;

    fn first(&mut self) -> HolderTurn {
        self.ask()
    }

    fn next(&mut self, result: IoResult) -> HolderTurn {
        let holder = self.asked.take().ok_or_else(|| {
            HolderWriteError::Undeliverable("a holder answer nobody asked for".to_string())
        })?;
        match peer_reply(holder, result)? {
            Ok(response) => self.answered(holder, response),
            Err(error) => self.failed(holder, error),
        }
    }
}

#[cfg(test)]
#[path = "routing_tests.rs"]
mod tests;
