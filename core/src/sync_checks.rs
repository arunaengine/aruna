//! Checks replicated documents against their targets: shape, size bounds and owning node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use ulid::Ulid;

use crate::UserId;
use crate::document::{DocumentChange, DocumentChangeKind, DocumentTarget};
use crate::structs::execution::notification_watch::{
    INTEREST_BYTES_CAP, INTEREST_ENTRY_CAP, MAX_PREFIX_LEN, WatchEventMask, WatchInterestDigest,
    WatchSubscription, interest_node_id, interest_realm_id,
};
use crate::structs::execution::setting::validate_setting;
use crate::structs::identity::realm::RealmId;
use crate::structs::placement::policy::document::{
    PlacementPolicyDocument, placement_policy_change,
};
use crate::structs::storage::metadata_registry::MetadataRegistryRecord;
use crate::structs::storage::node_info::NodeInfoDocument;
use crate::structs::storage::usage::{NodeUsageSnapshot, usage_node_id};
use crate::structs::{
    PersistentIdKind, PersistentIdMapping, PersistentIdProvider, PersistentIdStatus,
    persistent_id_change,
};

/// Validates a replicated node-usage snapshot against its target: the payload
/// must decode, embed its target's node, and derive a key attributing to it.
/// The caller checks the publisher; zero-counter snapshots are valid.
pub fn validate_usage_upsert(target: &DocumentTarget, bytes: &[u8]) -> Result<(), String> {
    let DocumentTarget::NodeUsage { node_id, .. } = target else {
        return Err("target is not a node usage snapshot".to_string());
    };
    let snapshot = NodeUsageSnapshot::from_bytes(bytes)
        .map_err(|error| format!("undecodable node usage snapshot: {error}"))?;
    if snapshot.node_id != *node_id {
        return Err(format!(
            "snapshot node id {} does not match target node id {node_id}",
            snapshot.node_id
        ));
    }
    let key = target.storage_key();
    if usage_node_id(key.as_ref()) != Some(*node_id) {
        return Err(format!(
            "node usage storage key does not attribute to target node id {node_id}"
        ));
    }
    Ok(())
}

/// Validates a replicated watch-interest digest against its target. The caller
/// checks the publisher; empty digests are valid and clear a node's interest
/// while preserving single-writer ownership.
pub fn validate_watch_interest(target: &DocumentTarget, bytes: &[u8]) -> Result<(), String> {
    if bytes.len() > INTEREST_BYTES_CAP {
        return Err(format!(
            "watch interest digest exceeds serialized byte cap {}",
            INTEREST_BYTES_CAP
        ));
    }
    let DocumentTarget::WatchInterest { realm_id, node_id } = target else {
        return Err("target is not a watch interest digest".to_string());
    };
    let digest = WatchInterestDigest::from_bytes(bytes)
        .map_err(|error| format!("undecodable watch interest digest: {error}"))?;
    if digest.entries.len() > INTEREST_ENTRY_CAP {
        return Err(format!(
            "watch interest digest exceeds entry cap {}",
            INTEREST_ENTRY_CAP
        ));
    }
    if digest.node_id != *node_id {
        return Err(format!(
            "digest node id {} does not match target node id {node_id}",
            digest.node_id
        ));
    }
    let key = target.storage_key();
    if interest_node_id(key.as_ref()) != Some(*node_id) {
        return Err(format!(
            "watch interest storage key does not attribute to target node id {node_id}"
        ));
    }
    if interest_realm_id(key.as_ref()) != Some(*realm_id) {
        return Err(format!(
            "watch interest storage key does not attribute to target realm id {realm_id}"
        ));
    }
    Ok(())
}

pub fn validate_watch_upsert(
    target: &DocumentTarget,
    bytes: &[u8],
    change: &DocumentChange,
) -> Result<(), String> {
    let DocumentTarget::WatchSubscription { owner, watch_id } = target else {
        return Err("target is not a watch subscription".to_string());
    };
    validate_watch_target(*owner, *watch_id)?;
    if change.kind != DocumentChangeKind::Upsert || change.current.generation != 1 {
        return Err(
            "watch subscription upsert must carry generation 1 upsert revision".to_string(),
        );
    }
    let subscription = WatchSubscription::from_bytes(bytes)
        .map_err(|error| format!("undecodable watch subscription: {error}"))?;
    if subscription.owner != *owner || subscription.watch_id != *watch_id {
        return Err("watch subscription payload does not match its target".to_string());
    }
    if subscription.path_prefix.is_empty()
        || subscription.path_prefix.starts_with('/')
        || subscription.path_prefix.len() > MAX_PREFIX_LEN
    {
        return Err("watch subscription path prefix is invalid".to_string());
    }
    let known_mask = WatchEventMask::METADATA_CREATED
        | WatchEventMask::DATA_UPLOADED
        | WatchEventMask::SYNC_COMPLETED
        | WatchEventMask::SYNC_FAILED;
    if subscription.event_mask.is_empty() || subscription.event_mask.bits() & !known_mask != 0 {
        return Err("watch subscription event mask is invalid".to_string());
    }
    Ok(())
}

/// A register write: a notification setting or an owner secret document.
pub fn validate_register_upsert(
    target: &DocumentTarget,
    bytes: &[u8],
    change: &DocumentChange,
) -> Result<(), String> {
    match target {
        DocumentTarget::OwnerSecret { .. } => validate_secret_upsert(target, bytes, change),
        _ => validate_setting_upsert(target, bytes, change),
    }
}

/// An owner secret syncs as one generation 1 upsert per immutable write, stamped by its actor.
/// Whether the writer counts is decided at selection, never here, so every node stores alike.
pub fn validate_secret_upsert(
    target: &DocumentTarget,
    bytes: &[u8],
    change: &DocumentChange,
) -> Result<(), String> {
    if change.kind != DocumentChangeKind::Upsert || change.current.generation != 1 {
        return Err("a secret write must carry a generation 1 upsert revision".to_string());
    }
    let DocumentTarget::OwnerSecret {
        owner,
        name,
        part,
        write_id,
        ..
    } = target
    else {
        return Err("the target is not an owner secret".to_string());
    };
    let document = crate::secret_registers::SecretDocument::from_bytes(bytes)
        .map_err(|error| format!("undecodable secret write: {error}"))?;
    document.check()?;
    let matches = document.owner == *owner
        && document.name == *name
        && document.part() == *part
        && document.stamp.write_id == *write_id;
    if !matches {
        return Err("the secret write does not match its target".to_string());
    }
    match document.stamp.written_by(change.current.actor) {
        true => Ok(()),
        false => Err("the secret stamp names another node than its actor".to_string()),
    }
}

/// A notification setting syncs as one generation 1 upsert per immutable version.
pub fn validate_setting_upsert(
    target: &DocumentTarget,
    bytes: &[u8],
    change: &DocumentChange,
) -> Result<(), String> {
    if change.kind != DocumentChangeKind::Upsert || change.current.generation != 1 {
        return Err("a setting upsert must carry a generation 1 upsert revision".to_string());
    }
    let head = validate_setting(target, bytes)?;
    match head.stamp.written_by(change.current.actor) {
        true => Ok(()),
        false => Err("the setting stamp names another node than its actor".to_string()),
    }
}

pub fn validate_watch_delete(
    target: &DocumentTarget,
    change: &DocumentChange,
) -> Result<(), String> {
    let DocumentTarget::WatchSubscription { owner, watch_id } = target else {
        return Err("target is not a watch subscription".to_string());
    };
    validate_watch_target(*owner, *watch_id)?;
    if change.kind != DocumentChangeKind::Delete || change.current.generation != 2 {
        return Err(
            "watch subscription delete must carry generation 2 delete revision".to_string(),
        );
    }
    Ok(())
}

pub fn validate_watch_target(owner: UserId, watch_id: Ulid) -> Result<(), String> {
    if owner.is_nil() {
        return Err("watch subscription owner must not be nil".to_string());
    }
    if watch_id.is_nil() {
        return Err("watch subscription id must not be nil".to_string());
    }
    Ok(())
}

/// Validates a replicated node-info document against its target: the payload
/// must decode within bounds, advertise only its own node's execution sites,
/// and match the target's node. The caller checks the publisher.
pub fn validate_node_upsert(
    target: &DocumentTarget,
    bytes: &[u8],
) -> Result<NodeInfoDocument, String> {
    let DocumentTarget::NodeInfo { node_id, .. } = target else {
        return Err("target is not a node info document".to_string());
    };
    let document = NodeInfoDocument::from_bytes(bytes)
        .map_err(|error| format!("invalid node info document: {error}"))?;
    if document.node_id != *node_id {
        return Err(format!(
            "node info document node id {} does not match target node id {node_id}",
            document.node_id
        ));
    }
    Ok(document)
}

/// Whether an incoming advertisement replaces the stored one. Undecodable
/// stored bytes are replaced; an equal or older epoch is not applied, so a
/// delayed pre-rejoin advertisement cannot shadow the current one.
pub fn node_info_supersedes(incoming: &NodeInfoDocument, stored: Option<&[u8]>) -> bool {
    match stored.and_then(|bytes| NodeInfoDocument::from_bytes(bytes).ok()) {
        Some(current) => incoming.supersedes(&current),
        None => true,
    }
}

/// Structural gate for a replicated policy document: it must address this realm
/// and target, carry a verifiable definition, and derive its own sync change,
/// so a sender cannot restate a policy under someone else's revision.
pub fn validate_policy_document(
    policy_id: Ulid,
    realm_id: RealmId,
    document: &PlacementPolicyDocument,
    change: &DocumentChange,
) -> Result<(), String> {
    if document.policy_id() != policy_id {
        return Err(format!(
            "policy {} does not match target policy {policy_id}",
            document.policy_id()
        ));
    }
    if document.realm_id != realm_id {
        return Err(format!(
            "policy realm {} does not match this realm",
            document.realm_id
        ));
    }
    if let Err(error) = document.verified() {
        return Err(format!("policy definition is invalid: {error}"));
    }
    // Structural gate: bytes without an authentic publication are never a rule.
    if let Err(error) = document.verify_publication() {
        return Err(format!("policy publication is invalid: {error}"));
    }
    if change.kind != DocumentChangeKind::Upsert {
        return Err("policy event is not an upsert".to_string());
    }
    if *change != placement_policy_change(document, change.placement) {
        return Err("policy revision does not match its sync change".to_string());
    }
    Ok(())
}

/// Validates a replicated PID mapping against its target and change: the
/// canonical PID, a consistent kind/status/provenance triple, and the derived
/// change. The caller enforces publisher identity against the signed actor.
pub fn validate_pid_mapping(
    document_id: Ulid,
    mapping: &PersistentIdMapping,
    change: &DocumentChange,
) -> Result<(), String> {
    if mapping.target != document_id {
        return Err(format!(
            "mapping target {} does not match target document {document_id}",
            mapping.target
        ));
    }
    if mapping.pid != MetadataRegistryRecord::graph_iri_for(document_id)
        && mapping.pid != PersistentIdMapping::profile_pid(document_id)
    {
        return Err(format!("mapping pid `{}` is not canonical", mapping.pid));
    }
    if !matches!(mapping.kind, PersistentIdKind::Conceptual) {
        return Err("mapping kind is unsupported".to_string());
    }
    if !matches!(mapping.provider, PersistentIdProvider::W3id) {
        return Err("mapping provider is unsupported".to_string());
    }
    if mapping.requested_at_ms.is_some() != mapping.requested_by.is_some() {
        return Err("mapping request provenance is incomplete".to_string());
    }
    if mapping.minted_at_ms.is_some() != mapping.minted_by.is_some() {
        return Err("mapping mint provenance is incomplete".to_string());
    }
    match mapping.status {
        PersistentIdStatus::Active => {
            if mapping.minted_at_ms.is_none() || mapping.withdrawn_at_ms.is_some() {
                return Err("active mapping has inconsistent transition fields".to_string());
            }
        }
        PersistentIdStatus::Requested | PersistentIdStatus::Processing => {
            if mapping.requested_at_ms.is_none()
                || mapping.minted_at_ms.is_some()
                || mapping.withdrawn_at_ms.is_some()
            {
                return Err("pending mapping has inconsistent transition fields".to_string());
            }
        }
        PersistentIdStatus::Failed => {
            if mapping.requested_at_ms.is_none()
                || mapping.failure.is_none()
                || mapping.withdrawn_at_ms.is_some()
            {
                return Err("failed mapping has inconsistent transition fields".to_string());
            }
        }
        PersistentIdStatus::AdminWithdrawn => {
            if mapping.withdrawn_at_ms.is_none()
                || mapping.withdrawn_by.is_none()
                || mapping
                    .withdrawal_reason
                    .as_deref()
                    .is_none_or(str::is_empty)
            {
                return Err("admin-withdrawn mapping lacks actor or reason".to_string());
            }
        }
        PersistentIdStatus::Tombstoned => {
            if mapping.withdrawn_at_ms.is_none() {
                return Err("tombstoned mapping has no deletion timestamp".to_string());
            }
        }
    }
    if change.kind != DocumentChangeKind::Upsert {
        return Err("mapping event is not an upsert".to_string());
    }
    if *change != persistent_id_change(mapping, change.placement) {
        return Err("mapping revision does not match its sync change".to_string());
    }
    Ok(())
}

/// Whether a watch subscription or setting change applies over the stored revision kind. Watch
/// ids are never reused: the first upsert wins and any delete fences later creates for good.
pub fn watch_change_applies(
    stored: Option<DocumentChangeKind>,
    incoming: DocumentChangeKind,
) -> bool {
    match (stored, incoming) {
        (None, _) => true,
        (Some(DocumentChangeKind::Upsert), DocumentChangeKind::Delete) => true,
        (Some(DocumentChangeKind::Upsert), DocumentChangeKind::Upsert)
        | (Some(DocumentChangeKind::Delete), _) => false,
    }
}

#[cfg(test)]
#[path = "sync_checks_tests.rs"]
mod tests;
