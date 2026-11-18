//! Issues and accepts request-bound delegations for S3 writes forwarded to a range's
//! coordinator (D33). The issuer signs one request it verified.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// The target checks the issuer, the validity window and a single-use nonce, then runs its own
// permission checks.

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::structs::identity::auth::PathRestriction;
use aruna_core::structs::identity::realm::{NodeRealm, RealmId};
use aruna_core::structs::storage::distribution::delegation::{
    Delegation, Payload, SignedDelegation,
};
use aruna_core::types::GroupId;
use ulid::Ulid;

use super::accept_delegation::{AcceptDelegationOperation, DelegateError};

/// How long an issued delegation is valid.
const DELEGATION_SECS: u64 = 15;

/// What the issuing node verified about one request before it forwards it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DelegationInput {
    pub set_id: Ulid,
    pub range_id: Ulid,
    pub target: NodeId,
    pub user_id: UserId,
    pub group_id: GroupId,
    pub credential_id: String,
    pub restrictions: Option<Vec<PathRestriction>>,
    /// Unix seconds at which the originating credential expires.
    pub credential_expiry: u64,
    pub operation: String,
    pub bucket: String,
    pub key: String,
    pub copy_source: Option<String>,
    pub request_digest: [u8; 32],
    pub payload: Payload,
}

/// The delegation `issuer` makes for one verified request; `now` is Unix seconds.
pub fn unsigned_delegation(
    (issuer, realm_id): (NodeId, RealmId),
    input: DelegationInput,
    now: u64,
) -> Delegation {
    Delegation {
        realm_id,
        issuer,
        target: input.target,
        set_id: input.set_id,
        range_id: input.range_id,
        user_id: input.user_id,
        group_id: input.group_id,
        credential_id: input.credential_id,
        restrictions: input.restrictions,
        credential_expiry: input.credential_expiry,
        operation: input.operation,
        bucket: input.bucket,
        key: input.key,
        copy_source: input.copy_source,
        request_digest: input.request_digest,
        payload: input.payload,
        nonce: rand::random(),
        issued_at: now,
        valid_secs: DELEGATION_SECS,
    }
}

/// Accepts a delegation for this node once; see [`AcceptDelegationOperation`].
pub fn accept_delegation(
    ids: NodeRealm,
    signed: &SignedDelegation,
    now: u64,
) -> Result<AcceptDelegationOperation, DelegateError> {
    let (node_id, realm_id) = ids.ok_or(DelegateError::Unavailable)?;
    Ok(AcceptDelegationOperation::new(
        signed.clone(),
        realm_id,
        node_id,
        now,
    ))
}
