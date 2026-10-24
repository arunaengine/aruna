//! Issues and accepts request-bound delegations for S3 writes forwarded to a range's
//! coordinator (D33). The issuer signs one request it verified; the target checks the issuer,
//! the validity window and a single-use nonce, then runs its own permission checks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::DELEGATION_NONCE_KEYSPACE;
use aruna_core::structs::identity::auth::PathRestriction;
use aruna_core::structs::storage::distribution::delegation::{
    BodyTrailer, Delegation, DelegationError, MAX_VALID_SECS, Payload, SignedDelegation,
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
/// Expired nonce rows one acceptance deletes, so the keyspace never grows past live ones.
const PRUNE_PAGE: usize = 64;

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
    spend_nonce(context, delegation.nonce, expires).await?;
    prune_nonces(context, now).await;
    Ok(delegation)
}

/// The row of a spent nonce: expiry second first, so expired rows sort ahead.
fn nonce_key(expires: u64, nonce: [u8; 16]) -> Vec<u8> {
    let mut key = expires.to_be_bytes().to_vec();
    key.extend_from_slice(&nonce);
    key
}

async fn storage(context: &DriverContext, effect: StorageEffect) -> Result<Event, DelegateError> {
    match context.storage_handle.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::Error { error }) => Err(match error {
            StorageError::TransactionConflict => DelegateError::Replayed,
            _ => DelegateError::Unavailable,
        }),
        event => Ok(event),
    }
}

/// Records a nonce as spent in one transaction, durably until it expires; a restart never
/// accepts it again.
async fn spend_nonce(
    context: &DriverContext,
    nonce: [u8; 16],
    expires: u64,
) -> Result<(), DelegateError> {
    let start = StorageEffect::StartTransaction { read: false };
    let Event::Storage(StorageEvent::TransactionStarted { txn_id }) =
        storage(context, start).await?
    else {
        return Err(DelegateError::Unavailable);
    };
    let key = nonce_key(expires, nonce);
    let read = StorageEffect::Read {
        key_space: DELEGATION_NONCE_KEYSPACE.to_string(),
        key: key.clone().into(),
        txn_id: Some(txn_id),
    };
    let spent = match storage(context, read).await {
        Ok(Event::Storage(StorageEvent::ReadResult { value, .. })) => Ok(value.is_some()),
        Ok(_) => Err(DelegateError::Unavailable),
        Err(error) => Err(error),
    };
    let write = StorageEffect::Write {
        key_space: DELEGATION_NONCE_KEYSPACE.to_string(),
        key: key.into(),
        value: Vec::new().into(),
        txn_id: Some(txn_id),
    };
    let written = match spent {
        Ok(false) => storage(context, write).await.map(|_| ()),
        Ok(true) => Err(DelegateError::Replayed),
        Err(error) => Err(error),
    };
    if let Err(error) = written {
        let _ = storage(context, StorageEffect::AbortTransaction { txn_id }).await;
        return Err(error);
    }
    storage(context, StorageEffect::CommitTransaction { txn_id })
        .await
        .map(|_| ())
}

/// Deletes one bounded page of nonce rows that expired before `now`.
async fn prune_nonces(context: &DriverContext, now: u64) {
    let page = StorageEffect::Iter {
        key_space: DELEGATION_NONCE_KEYSPACE.to_string(),
        prefix: None,
        start: None,
        limit: PRUNE_PAGE,
        txn_id: None,
    };
    let Ok(Event::Storage(StorageEvent::IterResult { values, .. })) = storage(context, page).await
    else {
        return;
    };
    let deletes: Vec<_> = values
        .into_iter()
        .filter(|(key, _)| {
            key.get(..8)
                .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
                .is_some_and(|expires| u64::from_be_bytes(expires) < now)
        })
        .map(|(key, _)| (DELEGATION_NONCE_KEYSPACE.to_string(), key))
        .collect();
    if !deletes.is_empty() {
        let delete = StorageEffect::BatchDelete {
            deletes,
            txn_id: None,
        };
        let _ = storage(context, delete).await;
    }
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
    use crate::distribution::test_store::{context, get, open};

    #[tokio::test]
    async fn nonces_spend_once() {
        let (_dir, handle) = open();
        let context = context(&handle);
        assert_eq!(spend_nonce(&context, [1; 16], 50).await, Ok(()));
        assert_eq!(
            spend_nonce(&context, [1; 16], 50).await,
            Err(DelegateError::Replayed)
        );
        assert_eq!(spend_nonce(&context, [2; 16], 50).await, Ok(()));
    }

    #[tokio::test]
    async fn expired_nonces_pruned() {
        let (_dir, handle) = open();
        let context = context(&handle);
        spend_nonce(&context, [1; 16], 10).await.unwrap();
        spend_nonce(&context, [2; 16], 90).await.unwrap();
        prune_nonces(&context, 50).await;
        let key = DELEGATION_NONCE_KEYSPACE;
        assert!(get(&handle, key, nonce_key(10, [1; 16])).await.is_none());
        assert!(get(&handle, key, nonce_key(90, [2; 16])).await.is_some());
    }
}
