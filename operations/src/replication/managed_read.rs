//! Runs a governed remote Bao read: the destination challenge with taught placement rules, or
//! one plain attempt on a device.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::effects::StorageEffect;
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::policy::PlacementPolicyRef;
use byteview::ByteView;

use super::bao_read::{BaoReadError, BaoReadOperation, BaoReadOutput};
use super::protocol::BaoReadRequest;
use crate::driver::{DriverContext, drive};
use crate::placement::policy::{PolicyGateError, gate_decision, union_refs, write_gate};
use crate::realm::mutate_placement::node_kind;

/// One teach-then-retry round. The source only ever teaches the refs it needs,
/// so a second `Required` for the same set is a protocol dead end, not a loop.
const CHALLENGE_ATTEMPTS: usize = 2;

/// A governed remote read with the plan's destination challenge (5.6/10): the
/// request carries this node's subject, refs resolve and cache locally, retries
/// need local compliance, and echoed refs are never authority.
pub async fn managed_read(
    context: &DriverContext,
    node_id: NodeId,
    mut request: BaoReadRequest,
) -> Result<BaoReadOutput, BaoReadError> {
    if local_is_user(context, request.realm_id).await {
        return device_read(context, node_id, request).await;
    }
    // An admission stop is not decided here: `write_gate` below refuses once
    // the source has taught the refs that make this read a governed one.
    let destination = match crate::driver::drive(
        crate::placement::policy::GateOperation::new(
            request.realm_id,
            aruna_core::time::unix_timestamp_millis(),
        ),
        context,
    )
    .await
    {
        Ok(destination) => destination,
        Err(_) => return Err(BaoReadError::NoDestination),
    };
    request.destination = destination.as_ref().map(|gate| gate.subject.clone());
    let mut taught: Vec<PlacementPolicyRef> = Vec::new();
    for _ in 0..CHALLENGE_ATTEMPTS {
        let refs = match drive(BaoReadOperation::new(node_id, request.clone()), context).await {
            Err(BaoReadError::PolicyRequired { refs }) => refs,
            other => return other,
        };
        let refs = PlacementPolicyRef::canonical_set(&refs).map_err(ConversionError::from)?;
        if refs.is_empty() || refs == taught {
            return Err(BaoReadError::PolicyRequired { refs });
        }
        // The refs are only a hint; this node resolves its own view and caches the
        // verified publication. The write checks the owning group, not the read.
        let Some(gate) = write_gate(destination.as_ref(), &refs, None)? else {
            return Err(BaoReadError::NoDestination);
        };
        let outcome = drive(gate, context).await.map_err(PolicyGateError::from)?;
        gate_decision(outcome)?;
        request.known_refs = union_refs(&request.known_refs, &refs)?;
        taught = refs;
    }
    Err(BaoReadError::PolicyRequired { refs: taught })
}

/// One attempt with no destination at all. A device never admits governed data,
/// so the challenge has no answer it could give: teaching it rules would only
/// dress a refusal up as a negotiation.
async fn device_read(
    context: &DriverContext,
    node_id: NodeId,
    mut request: BaoReadRequest,
) -> Result<BaoReadOutput, BaoReadError> {
    request.destination = None;
    device_answer(drive(BaoReadOperation::new(node_id, request), context).await)
}

/// Every placement answer is terminal here: a device can neither resolve a rule
/// nor become a destination, so a retry would only repeat the same refusal.
pub(super) fn device_answer(
    result: Result<BaoReadOutput, BaoReadError>,
) -> Result<BaoReadOutput, BaoReadError> {
    match result {
        Err(BaoReadError::PolicyRequired { .. })
        | Err(BaoReadError::PolicyDenied { .. })
        | Err(BaoReadError::NoDestination)
        | Err(BaoReadError::Gate(_)) => Err(BaoReadError::GovernedUnavailable),
        other => other,
    }
}

/// Whether this realm records the local node as an owner-bound User node.
pub(crate) async fn local_is_user(context: &DriverContext, realm_id: RealmId) -> bool {
    let Some(net_handle) = context.net_handle.as_ref() else {
        return false;
    };
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: ByteView::from(realm_id.as_bytes().to_vec()),
            txn_id: None,
        })
        .await;
    let Event::Storage(StorageEvent::ReadResult {
        value: Some(value), ..
    }) = event
    else {
        return false;
    };
    RealmConfigDocument::from_bytes(&value).is_ok_and(|document| {
        node_kind(&document, net_handle.node_id()).is_some_and(|kind| kind.owner().is_some())
    })
}
