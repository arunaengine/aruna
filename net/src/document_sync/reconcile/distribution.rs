//! Admits and applies distribution record changes: group admin authority for configuration
//! changes (P1), the member's own authenticated node for admissions (K1), one reduced record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::keyspaces::DISTRIBUTION_KEYSPACE;
use aruna_core::structs::placement::policy::document::group_admin_path;
use aruna_core::structs::storage::distribution::change::ChangeAuthority;
use aruna_core::structs::storage::distribution::{DistributionChange, DistributionOp};

/// Shape rules that need no stored state: the change names the target's set and is supported.
pub(in crate::document_sync) fn validate_distribution_shape(
    event: &AdminDocumentEvent,
) -> std::result::Result<(), String> {
    let (AdminDocumentTarget::Distribution { set_id, .. }, Some(change)) =
        (&event.target, distribution_change(event))
    else {
        return Err("distribution change must target a distribution record".to_string());
    };
    if change.set_id != *set_id {
        return Err("distribution change names another set".to_string());
    }
    if !change.op.supported() {
        return Err(format!(
            "distribution change `{}` is not supported yet",
            change.op.name()
        ));
    }
    change
        .validate_shape()
        .map_err(|error| format!("distribution change is malformed: {error}"))
}

fn distribution_change(event: &AdminDocumentEvent) -> Option<&DistributionChange> {
    match &event.op {
        AdminDocumentOperation::DistributionChanged { change } => Some(change),
        _ => None,
    }
}

/// Configuration changes need the actor's current group admin WRITE on the owner group;
/// member changes need the member's own node as the authenticated origin.
pub(in crate::document_sync) async fn validate_distribution_authority(
    storage: &StorageHandle,
    event: &AdminDocumentEvent,
    previous_state: Option<&AdminDocumentState>,
) -> Result<AdminEventValidation> {
    let reject = |reason: &str| Ok(AdminEventValidation::Rejected(reason.to_string()));
    let (AdminDocumentTarget::Distribution { realm_id, .. }, Some(change)) =
        (&event.target, distribution_change(event))
    else {
        return reject("admin event target is not a distribution record");
    };
    let realm_id = *realm_id;
    let Some(config) = read_realm_config(storage, realm_id).await? else {
        return Ok(AdminEventValidation::Deferred {
            dependency: Some(DocumentSyncDependency::RealmConfig(realm_id)),
            reason: "current realm config is unavailable".to_string(),
        });
    };
    if config.realm_id != realm_id || !origin_may_publish(&config, &event.origin_node_id) {
        return reject("distribution change origin is not a publisher-capable realm node");
    }
    let record = previous_state.and_then(AdminDocumentState::materialized_distribution);
    match change.op.authority() {
        ChangeAuthority::GroupAdmin => {
            let owner = match &change.op {
                DistributionOp::Create { config } => Some(config.owner_group),
                _ => record.map(|record| record.config.owner_group),
            };
            let Some(owner) = owner else {
                return Ok(AdminEventValidation::Deferred {
                    dependency: None,
                    reason: "distribution record is not yet known".to_string(),
                });
            };
            group_admin_authority(storage, event, realm_id, owner).await
        }
        ChangeAuthority::MemberNode(member_id) => {
            let foreign = record
                .as_ref()
                .and_then(|record| record.member(member_id))
                .is_some_and(|member| member.node_id != event.origin_node_id);
            if foreign {
                return reject("distribution member change does not come from the member's node");
            }
            // An unknown record defers the node check to the deterministic reduction.
            Ok(AdminEventValidation::Accepted)
        }
        ChangeAuthority::Coordinator(_) | ChangeAuthority::TransferTarget(_) => {
            reject("distribution change is not supported yet")
        }
    }
}

/// P1: the named actor's current WRITE on the owner group's admin path, from the realm and
/// group authorization documents. Missing group roles defer instead of granting.
async fn group_admin_authority(
    storage: &StorageHandle,
    event: &AdminDocumentEvent,
    realm_id: RealmId,
    owner: GroupId,
) -> Result<AdminEventValidation> {
    if event.actor.user_id.realm_id != realm_id {
        return Ok(AdminEventValidation::Rejected(
            "distribution change actor belongs to another realm".to_string(),
        ));
    }
    let Some(realm_auth) = read_realm_authorization(storage, realm_id).await? else {
        return Ok(AdminEventValidation::Deferred {
            dependency: Some(DocumentSyncDependency::RealmAuthorization(realm_id)),
            reason: "realm authorization is unavailable".to_string(),
        });
    };
    let Some(group_auth) = read_group_authorization(storage, owner)
        .await?
        .filter(|group| group.group_id == owner)
    else {
        return Ok(AdminEventValidation::Deferred {
            dependency: Some(DocumentSyncDependency::GroupAuthorization(owner)),
            reason: "owner group authorization is unavailable".to_string(),
        });
    };
    let roles = realm_auth.roles.values().chain(group_auth.roles.values());
    let path = group_admin_path(realm_id, owner);
    Ok(
        match has_write_permission(event.actor.user_id, &path, roles) {
            true => AdminEventValidation::Accepted,
            false => AdminEventValidation::Rejected(
                "actor lacks group admin write on the distributed bucket's owner group".to_string(),
            ),
        },
    )
}

/// Reduces the change into the set's state and stores the record every realm node reads.
pub(in crate::document_sync) async fn apply_distribution_operation(
    storage: &StorageHandle,
    document_target: DocumentTarget,
    event: AdminDocumentEvent,
) -> Result<()> {
    let (
        DocumentTarget::RealmConfig { realm_id },
        AdminDocumentTarget::Distribution {
            realm_id: event_realm_id,
            set_id,
        },
    ) = (&document_target, &event.target)
    else {
        return Err(NetError::Bootstrap(
            "distribution changes ride the realm config document".to_string(),
        ));
    };
    if realm_id != event_realm_id {
        return Err(NetError::Bootstrap(
            "distribution change realm does not match its document".to_string(),
        ));
    }
    let set_id = *set_id;
    for _ in 0..APPLY_CONFLICT_ATTEMPTS {
        tokio::task::yield_now().await;
        let txn_id = start_storage_transaction(storage).await?;
        let writes = match distribution_writes(storage, txn_id, &event, set_id).await {
            Ok(Some(writes)) => writes,
            Ok(None) => return abort_txn(storage, txn_id).await,
            Err(error) => return Err(abort_error(storage, txn_id, error).await),
        };
        match replace_batch_in(storage, txn_id, Vec::new(), writes).await {
            Ok(()) => return Ok(()),
            Err(NetError::Storage(StorageError::TransactionConflict)) => {
                let _ = abort_txn(storage, txn_id).await;
            }
            Err(error) => {
                let _ = abort_txn(storage, txn_id).await;
                return Err(error);
            }
        }
    }
    Err(NetError::Dht(
        "distribution change apply conflict retries exhausted".to_string(),
    ))
}

/// The reducer state and record writes of one attempt; `None` for a duplicate event.
async fn distribution_writes(
    storage: &StorageHandle,
    txn_id: TxnId,
    event: &AdminDocumentEvent,
    set_id: Ulid,
) -> Result<Option<Vec<(String, ByteView, Value)>>> {
    let bootstrap = |error: String| NetError::Bootstrap(error);
    let mut state = transaction_read(
        storage,
        DOCUMENT_STATE_KEYSPACE.to_string(),
        reducer_state_key(&event.target),
        Some(txn_id),
    )
    .await?
    .map(|bytes| decode_reducer_state(&bytes))
    .transpose()
    .map_err(|error| bootstrap(error.to_string()))?
    .unwrap_or_else(|| AdminDocumentState::new(event.target.clone()));
    if state
        .apply(event)
        .map_err(|error| bootstrap(error.to_string()))?
        == AdminApplyStatus::Duplicate
    {
        return Ok(None);
    }
    let mut writes =
        vec![reducer_state_entry(&state).map_err(|error| bootstrap(error.to_string()))?];
    if let Some(record) = state.materialized_distribution() {
        let bytes = record
            .to_bytes()
            .map_err(|error| bootstrap(error.to_string()))?;
        writes.push((
            DISTRIBUTION_KEYSPACE.to_string(),
            set_id.to_bytes().to_vec().into(),
            bytes.into(),
        ));
    }
    Ok(Some(writes))
}
