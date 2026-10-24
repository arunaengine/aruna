//! Issues and redeems download tickets for private redirects to a chosen holder (D33). The
//! issuer signs what it verified about the caller; the target redeems a ticket once, with the
//! same revocation check a direct authenticated read makes, then checks its own path.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction, SessionRef};
use aruna_core::structs::storage::backend_import::TokenGrant;
use aruna_core::structs::storage::distribution::ticket::{
    SignedTicket, Ticket, TicketError, ticket_expiry,
};
use thiserror::Error;

use crate::auth::bearer_token::grant_valid;
use crate::distribution::delegate::{DelegateError, prune_nonces, spend_nonce};
use crate::driver::DriverContext;
use crate::forward::authorize::is_sync_eligible;
use crate::placement::process_placements::load_realm_config;

/// Seconds a spent nonce outlives its ticket, for clocks that run behind the issuer's.
const SPENT_MARGIN_SECS: u64 = 60;

#[derive(Debug, Error, PartialEq)]
pub enum RedeemError {
    #[error(transparent)]
    Ticket(#[from] TicketError),
    #[error("the download ticket comes from a node that is not a realm infrastructure node")]
    Issuer,
    #[error("the token behind the download ticket is expired or revoked")]
    Revoked,
    #[error("the download ticket was already used")]
    Replayed,
    #[error("download tickets need the realm transport and configuration")]
    Unavailable,
}

/// What the choosing node verified about the caller of one private download.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TicketInput {
    pub target: NodeId,
    pub user_id: UserId,
    pub grant: TokenGrant,
    pub restrictions: Option<Vec<PathRestriction>>,
    pub session: Option<SessionRef>,
    pub object_id: String,
}

/// Signs a ticket for one redirect with this node's key; `now` is Unix seconds.
pub fn issue_ticket(context: &DriverContext, input: TicketInput, now: u64) -> Option<SignedTicket> {
    let net = context.net_handle.as_ref()?;
    let ticket = Ticket {
        realm_id: *net.realm_id(),
        issuer: net.node_id(),
        target: input.target,
        user_id: input.user_id,
        expires_at: ticket_expiry(now, input.grant.expires_at_secs),
        grant: input.grant,
        restrictions: input.restrictions,
        session: input.session,
        object_id: input.object_id,
        nonce: rand::random(),
        issued_at: now,
    };
    let bytes = ticket.signing_bytes().ok()?;
    Some(SignedTicket {
        signature: net.sign(&bytes),
        ticket,
    })
}

/// Redeems a ticket for `object_id` on this node once and returns the caller it names. The
/// caller still checks the user's permission on this node's own path.
pub async fn redeem_ticket(
    context: &Arc<DriverContext>,
    signed: &SignedTicket,
    object_id: &str,
    now: u64,
) -> Result<AuthContext, RedeemError> {
    let net = context
        .net_handle
        .as_ref()
        .ok_or(RedeemError::Unavailable)?;
    let realm_id = *net.realm_id();
    let ticket = signed.verify(realm_id, net.node_id(), object_id, now)?;
    let config = load_realm_config(context, realm_id)
        .await
        .ok_or(RedeemError::Unavailable)?;
    if !is_sync_eligible(&config, ticket.issuer) {
        return Err(RedeemError::Issuer);
    }
    match grant_valid(&context.storage_handle, &ticket.grant, &ticket.user_id).await {
        Ok(true) => {}
        Ok(false) => return Err(RedeemError::Revoked),
        Err(_) => return Err(RedeemError::Unavailable),
    }
    let expires = ticket.expires_at.saturating_add(SPENT_MARGIN_SECS);
    spend_nonce(context, ticket.nonce, expires)
        .await
        .map_err(|error| match error {
            DelegateError::Replayed => RedeemError::Replayed,
            _ => RedeemError::Unavailable,
        })?;
    prune_nonces(context, now).await;
    Ok(AuthContext {
        user_id: ticket.user_id,
        realm_id,
        path_restrictions: ticket.restrictions.clone(),
        session: ticket.session.clone(),
    })
}
