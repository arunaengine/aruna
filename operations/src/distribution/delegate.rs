//! Issues and accepts request-bound delegations for S3 writes forwarded to a range's
//! coordinator (D33). The issuer signs one request it verified; the target checks the issuer,
//! the validity window and a single-use nonce, then runs its own permission checks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock, Mutex};

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::structs::identity::auth::PathRestriction;
use aruna_core::structs::storage::distribution::delegation::{
    Delegation, DelegationError, MAX_VALID_SECS, SignedDelegation,
};
use aruna_core::structs::storage::node_info::NodeUrls;
use aruna_core::types::GroupId;
use thiserror::Error;
use ulid::Ulid;

use crate::driver::DriverContext;
use crate::forward::authorize::is_sync_eligible;
use crate::placement::process_placements::load_realm_config;

/// How long an issued delegation is valid.
pub const DELEGATION_SECS: u64 = 15;
/// Spent nonces kept at once; beyond it new delegations are refused until old ones expire.
const MAX_SPENT: usize = 65_536;

/// Nonces this node accepted, with the Unix second after which they cannot verify anymore.
static SPENT: LazyLock<Mutex<BTreeMap<[u8; 16], u64>>> = LazyLock::new(Default::default);

#[derive(Debug, Error, PartialEq)]
pub enum DelegateError {
    #[error(transparent)]
    Delegation(#[from] DelegationError),
    #[error("the delegation comes from a node that is not a realm infrastructure node")]
    Issuer,
    #[error("the delegation was already used")]
    Replayed,
    #[error("too many forwarded requests are in flight; retry shortly")]
    Busy,
    #[error("delegations need the realm transport and configuration")]
    Unavailable,
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
    let mut spent = SPENT.lock().map_err(|_| DelegateError::Unavailable)?;
    spend(&mut spent, delegation.nonce, expires, now)?;
    Ok(delegation)
}

/// Records a nonce as spent; expired nonces are dropped first so the set stays bounded.
fn spend(
    spent: &mut BTreeMap<[u8; 16], u64>,
    nonce: [u8; 16],
    expires: u64,
    now: u64,
) -> Result<(), DelegateError> {
    if spent.len() >= MAX_SPENT {
        spent.retain(|_, until| *until >= now);
    }
    if spent.contains_key(&nonce) {
        return Err(DelegateError::Replayed);
    }
    if spent.len() >= MAX_SPENT {
        return Err(DelegateError::Busy);
    }
    spent.insert(nonce, expires);
    Ok(())
}

/// The published endpoints of a node, from its info document.
pub async fn node_urls(context: &DriverContext, node_id: NodeId) -> Option<NodeUrls> {
    crate::node::node_info::read_info_documents(context, &[node_id])
        .await
        .ok()?
        .remove(&node_id)
        .map(|document| document.urls)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nonces_spend_once() {
        let mut spent = BTreeMap::new();
        assert_eq!(spend(&mut spent, [1; 16], 50, 10), Ok(()));
        assert_eq!(
            spend(&mut spent, [1; 16], 50, 11),
            Err(DelegateError::Replayed)
        );
        assert_eq!(spend(&mut spent, [2; 16], 50, 11), Ok(()));
    }

    #[test]
    fn spent_nonces_bounded() {
        let mut spent: BTreeMap<[u8; 16], u64> = (0..MAX_SPENT as u32)
            .map(|index| {
                let mut nonce = [0; 16];
                nonce[..4].copy_from_slice(&index.to_be_bytes());
                (nonce, 20)
            })
            .collect();
        // Every kept nonce is still valid at second 10, so a new one waits.
        assert_eq!(spend(&mut spent, [9; 16], 50, 10), Err(DelegateError::Busy));
        // Once they expired, they make room.
        assert_eq!(spend(&mut spent, [9; 16], 50, 21), Ok(()));
        assert_eq!(spent.len(), 1);
    }
}
