//! Admits and stores distribution snapshots: only the set's bound publisher publishes them (K1),
//! every embedded statement carries its member's own signature, and the signed form is kept.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::keyspaces::{DISTRIBUTION_KEYSPACE, DISTRIBUTION_PUBLISHER_KEYSPACE};
use aruna_core::structs::storage::distribution::{PublisherCutoff, SignedSnapshot, publisher_key};

/// Content rules that need no stored state: the target's set, the publisher as origin, the
/// publisher's signature and every embedded statement. A failure is invalid input.
pub(in crate::document_sync) fn validate_distribution_shape(
    event: &AdminDocumentEvent,
) -> std::result::Result<(), String> {
    let AdminDocumentTarget::Distribution { realm_id, set_id } = &event.target else {
        return Err("distribution snapshot must target a distribution record".to_string());
    };
    let AdminDocumentOperation::DistributionPublished { snapshot } = &event.op else {
        return Err("distribution records change only through published snapshots".to_string());
    };
    if snapshot.snapshot.set_id() != *set_id {
        return Err("distribution snapshot names another set".to_string());
    }
    if event.origin_node_id != snapshot.snapshot.root.publisher {
        return Err("distribution snapshot does not come from the set's publisher".to_string());
    }
    snapshot
        .verify(*realm_id)
        .map_err(|error| format!("distribution snapshot is invalid: {error}"))
}

/// Committed revisions carry the publisher's acceptance, so no actor's current role is
/// rechecked here. A publisher the config does not know yet defers; a removed one keeps
/// authority only up to the cutoff its removal recorded for the set.
pub(in crate::document_sync) async fn validate_distribution_authority(
    storage: &StorageHandle,
    event: &AdminDocumentEvent,
) -> Result<AdminEventValidation> {
    let (
        AdminDocumentTarget::Distribution { realm_id, .. },
        AdminDocumentOperation::DistributionPublished { snapshot },
    ) = (&event.target, &event.op)
    else {
        return Ok(AdminEventValidation::Rejected(
            "admin event is not a distribution snapshot".to_string(),
        ));
    };
    Ok(
        match publisher_authority(storage, *realm_id, snapshot).await? {
            Authority::Current => AdminEventValidation::Accepted,
            Authority::Ended(reason) => AdminEventValidation::Rejected(reason.to_string()),
            Authority::Unknown => AdminEventValidation::Deferred {
                dependency: Some(DocumentSyncDependency::RealmConfig(*realm_id)),
                reason: "distribution publisher is not yet a known realm node".to_string(),
            },
        },
    )
}

/// Whether the realm config lets a snapshot's publisher publish it.
pub(in crate::document_sync) enum Authority {
    Current,
    Unknown,
    Ended(&'static str),
}

pub(in crate::document_sync) async fn publisher_authority(
    storage: &StorageHandle,
    realm_id: RealmId,
    signed: &SignedSnapshot,
) -> Result<Authority> {
    let publisher = signed.snapshot.root.publisher;
    let Some(config) = read_realm_config(storage, realm_id).await? else {
        return Ok(Authority::Unknown);
    };
    if config.realm_id != realm_id {
        return Ok(Authority::Ended("stored realm config names another realm"));
    }
    match configured_node_kind(&config, &publisher) {
        Some(kind) if kind.is_sync_eligible() => return Ok(Authority::Current),
        Some(_) => return Ok(Authority::Ended("distribution publisher cannot publish")),
        None => {}
    }
    let Some(cutoffs) = removed_cutoffs(storage, realm_id, &publisher).await? else {
        return Ok(Authority::Unknown);
    };
    let revision = signed.snapshot.revision();
    let within = cutoffs
        .iter()
        .find(|cutoff| cutoff.set_id == signed.snapshot.set_id())
        .is_some_and(|cutoff| {
            revision.number < cutoff.revision.number || revision == cutoff.revision
        });
    Ok(match within {
        true => Authority::Current,
        false => Authority::Ended("snapshot is beyond the removed publisher's cutoff"),
    })
}

/// The cutoffs of a node the realm config once listed and then removed.
async fn removed_cutoffs(
    storage: &StorageHandle,
    realm_id: RealmId,
    node_id: &NodeId,
) -> Result<Option<Vec<PublisherCutoff>>> {
    let state = read_reducer_state(storage, &AdminDocumentTarget::RealmConfig { realm_id }).await?;
    let Some(state) = state else {
        return Ok(None);
    };
    let path = config_node_path(node_id);
    let removed = state
        .user_subject_ids
        .get(&path)
        .is_some_and(|version| version.value.is_none())
        || state.conflicts.contains_key(&path);
    Ok(removed.then(|| state.publisher_cutoffs(node_id).unwrap_or_default()))
}

/// Reduces the snapshot into the set's state and stores the signed view every node reads.
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
            "distribution snapshots ride the realm config document".to_string(),
        ));
    };
    if realm_id != event_realm_id {
        return Err(NetError::Bootstrap(
            "distribution snapshot realm does not match its document".to_string(),
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
        "distribution snapshot apply conflict retries exhausted".to_string(),
    ))
}

/// The reducer state and view writes of one attempt; `None` when nothing changes.
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
        != AdminApplyStatus::Applied
    {
        return Ok(None);
    }
    let Some(view) = state.distribution_view() else {
        return Ok(None);
    };
    let bytes = view
        .to_bytes()
        .map_err(|error| bootstrap(error.to_string()))?;
    let head = view.snapshot();
    let revision =
        postcard::to_allocvec(&head.revision()).map_err(|error| bootstrap(error.to_string()))?;
    Ok(Some(vec![
        (
            DISTRIBUTION_PUBLISHER_KEYSPACE.to_string(),
            publisher_key(&head.root.publisher, set_id).into(),
            revision.into(),
        ),
        reducer_state_entry(&state).map_err(|error| bootstrap(error.to_string()))?,
        (
            DISTRIBUTION_KEYSPACE.to_string(),
            set_id.to_bytes().to_vec().into(),
            bytes.into(),
        ),
    ]))
}
