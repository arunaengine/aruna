//! Issues and accepts request-bound delegations for S3 writes forwarded to a range's
//! coordinator (D33). The issuer signs one request it verified.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// The target checks the issuer, the validity window and a single-use nonce, then runs its own
// permission checks.

use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::structs::identity::auth::PathRestriction;
use aruna_core::structs::storage::distribution::delegation::{
    BodyTrailer, Delegation, DelegationError, MAX_VALID_SECS, Payload, SignedDelegation,
};
use aruna_core::types::GroupId;
use thiserror::Error;
use ulid::Ulid;

use super::nonce::{NonceError, SpendNonceOperation};
use crate::driver::{DriverContext, drive};
use crate::forward::authorize::is_sync_eligible;
use crate::placement::process_placements::load_realm_config;

/// How long an issued delegation is valid.
pub const DELEGATION_SECS: u64 = 15;

#[derive(Debug, Error, PartialEq)]
pub enum DelegateError {
    #[error(transparent)]
    Delegation(#[from] DelegationError),
    #[error("the delegation comes from a node that is not a realm infrastructure node")]
    Issuer,
    #[error("the delegation was already used")]
    Replayed,
    #[error("delegations need the realm transport and configuration")]
    Unavailable,
}

impl From<NonceError> for DelegateError {
    fn from(error: NonceError) -> Self {
        match error {
            NonceError::Replayed => DelegateError::Replayed,
            NonceError::Unavailable => DelegateError::Unavailable,
        }
    }
}

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

/// Accepts a delegation for this node once: the issuer must be a configured realm
/// infrastructure node, never a device. The caller still checks the request digest and the
/// user's permissions on its own path.
pub async fn accept_delegation(
    context: &Arc<DriverContext>,
    signed: &SignedDelegation,
    now: u64,
) -> Result<Delegation, DelegateError> {
    let net = context
        .net_handle
        .as_ref()
        .ok_or(DelegateError::Unavailable)?;
    let realm_id = *net.realm_id();
    let delegation = signed.verify(realm_id, net.node_id(), now)?.clone();
    let config = load_realm_config(context, realm_id)
        .await
        .ok_or(DelegateError::Unavailable)?;
    if !is_sync_eligible(&config, delegation.issuer) {
        return Err(DelegateError::Issuer);
    }
    let expires = delegation.issued_at.saturating_add(2 * MAX_VALID_SECS);
    drive(
        SpendNonceOperation::new(delegation.nonce, expires, now),
        context,
    )
    .await?;
    Ok(delegation)
}
