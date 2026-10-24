//! Admits and stores distribution snapshots: only the set's bound publisher publishes them (K1),
//! every embedded statement carries its member's own signature, and the signed form is kept.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::keyspaces::{
    CATCH_UP_KEYSPACE, DISTRIBUTION_KEYSPACE, DISTRIBUTION_PUBLISHER_KEYSPACE,
};
use aruna_core::structs::storage::distribution::parking::{Authority, publisher_authority};
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
/// rechecked here. A publisher the config does not know yet is parked; a removed one keeps
/// authority only for the exact cutoff snapshot its removal recorded for the set.
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
        match Membership::read(storage, *realm_id)
            .await?
            .authority(snapshot)
        {
            Authority::Current => AdminEventValidation::Accepted,
            Authority::Ended(reason) => AdminEventValidation::Rejected(reason.to_string()),
            // Parked by the apply step, so the topic cursor moves on past a membership gap.
            Authority::Unknown => AdminEventValidation::Accepted,
        },
    )
}

/// The realm config and its removal cutoffs, read once for any number of authority decisions.
pub(in crate::document_sync) struct Membership {
    realm_id: RealmId,
    config: Option<RealmConfigDocument>,
    state: Option<AdminDocumentState>,
}

impl Membership {
    pub(in crate::document_sync) async fn read(
        storage: &StorageHandle,
        realm_id: RealmId,
    ) -> Result<Self> {
        Ok(Self {
            realm_id,
            config: read_realm_config(storage, realm_id).await?,
            state: read_reducer_state(storage, &AdminDocumentTarget::RealmConfig { realm_id })
                .await?,
        })
    }

    /// The cutoffs of a node the realm config once listed and then removed.
    fn removed(&self, node_id: &NodeId) -> Option<Vec<PublisherCutoff>> {
        let state = self.state.as_ref()?;
        let path = config_node_path(node_id);
        let removed = state
            .user_subject_ids
            .get(&path)
            .is_some_and(|version| version.value.is_none())
            || state.conflicts.contains_key(&path);
        removed.then(|| state.publisher_cutoffs(node_id).unwrap_or_default())
    }

    pub(in crate::document_sync) fn authority(&self, signed: &SignedSnapshot) -> Authority {
        let removed = self.removed(&signed.snapshot.root.publisher);
        publisher_authority(
            self.realm_id,
            self.config.as_ref(),
            removed.as_deref(),
            signed,
        )
    }
}

/// Reduces the snapshot into the set's state and stores the signed view every node reads.
pub(in crate::document_sync) async fn apply_distribution_operation(
    storage: &StorageHandle,
    document_target: DocumentTarget,
    event: AdminDocumentEvent,
) -> Result<Handled> {
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
    let (set_id, realm_id) = (*set_id, *realm_id);
    let AdminDocumentOperation::DistributionPublished { snapshot } = &event.op else {
        return Err(NetError::Bootstrap(
            "distribution records change only through published snapshots".to_string(),
        ));
    };
    match Membership::read(storage, realm_id)
        .await?
        .authority(snapshot)
    {
        Authority::Current => install_snapshot(storage, &event, set_id)
            .await
            .map(|()| Handled::Done),
        Authority::Unknown => park_snapshot(storage, &event, snapshot).await,
        Authority::Ended(reason) => {
            warn!(%set_id, %reason, "Dropping a distribution snapshot without authority");
            Ok(Handled::Done)
        }
    }
}

/// Installs a snapshot whose publisher may publish it, in one storage transaction.
pub(in crate::document_sync) async fn install_snapshot(
    storage: &StorageHandle,
    event: &AdminDocumentEvent,
    set_id: Ulid,
) -> Result<()> {
    for _ in 0..APPLY_CONFLICT_ATTEMPTS {
        tokio::task::yield_now().await;
        let txn_id = start_storage_transaction(storage).await?;
        let writes = match distribution_writes(storage, txn_id, event, set_id).await {
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
    // Coordinators find the sets whose catching-up members they bootstrap through this row.
    let catch_up = (!head.catching_up().is_empty()).then(|| {
        let key = set_id.to_bytes().to_vec().into();
        (CATCH_UP_KEYSPACE.to_string(), key, Value::from(Vec::new()))
    });
    Ok(Some(
        vec![
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
        ]
        .into_iter()
        .chain(catch_up)
        .collect(),
    ))
}
