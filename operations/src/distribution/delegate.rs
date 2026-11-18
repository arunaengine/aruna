//! Issues and accepts request-bound delegations for S3 writes forwarded to a range's
//! coordinator (D33). The issuer signs one request it verified.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// The target checks the issuer, the validity window and a single-use nonce, then runs its own
// permission checks.

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::structs::identity::auth::PathRestriction;
use aruna_core::structs::storage::distribution::delegation::{
    BodyTrailer, Delegation, DelegationError, Payload, SignedDelegation,
};
use aruna_core::types::GroupId;
use ulid::Ulid;

use super::accept_delegation::{AcceptDelegationOperation, DelegateError};
use crate::driver::DriverContext;

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

/// Signs a delegation for one request with this node's key; `now` is Unix seconds.
pub fn issue_delegation(
    context: &DriverContext,
    input: DelegationInput,
    now: u64,
) -> Result<SignedDelegation, DelegateError> {
    let net = context
        .net_handle
        .as_ref()
        .ok_or(DelegateError::Unavailable)?;
    let delegation = Delegation {
        realm_id: *net.realm_id(),
        issuer: net.node_id(),
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
    };
    let bytes = delegation
        .signing_bytes()
        .map_err(|_| DelegationError::Encoding)?;
    Ok(SignedDelegation {
        signature: net.sign(&bytes),
        delegation,
    })
}

/// Signs the trailer of a framed body this node forwards under the delegation with `nonce`.
pub fn sign_body(
    context: &DriverContext,
    nonce: &[u8; 16],
    trailer: &BodyTrailer,
) -> Option<iroh::Signature> {
    let net = context.net_handle.as_ref()?;
    Some(net.sign(&trailer.signing_bytes(nonce)))
}

/// Accepts a delegation for this node once; see [`AcceptDelegationOperation`].
pub fn accept_delegation(
    context: &DriverContext,
    signed: &SignedDelegation,
    now: u64,
) -> Result<AcceptDelegationOperation, DelegateError> {
    let net = context
        .net_handle
        .as_ref()
        .ok_or(DelegateError::Unavailable)?;
    let realm_id = *net.realm_id();
    Ok(AcceptDelegationOperation::new(
        signed.clone(),
        realm_id,
        net.node_id(),
        now,
    ))
}
