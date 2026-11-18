//! Issues and redeems download tickets for private redirects to a chosen holder (D33). The
//! issuer signs what it verified about the caller; the target redeems a ticket once.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Redemption makes the same revocation check as a direct authenticated read, then the target
// checks its own path.

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction, SessionRef};
use aruna_core::structs::identity::realm::{NodeRealm, RealmConfigDocument, RealmId};
use aruna_core::structs::storage::backend_import::TokenGrant;
use aruna_core::structs::storage::distribution::ticket::{
    SignedTicket, Ticket, TicketError, ticket_expiry,
};
use thiserror::Error;

use crate::auth::bearer_token::GrantValidOperation;
use crate::auth::error::ArunaBearerError;
use crate::distribution::nonce::{NonceError, SpendNonceOperation};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::realm::load_config::config_unavailable;
use crate::realm::peer_trust::is_sync_eligible;

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

impl From<IoError> for RedeemError {
    fn from(_: IoError) -> Self {
        Self::Unavailable
    }
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

/// The ticket `issuer` makes for one redirect; `now` is Unix seconds.
pub fn unsigned_ticket(
    (issuer, realm_id): (NodeId, RealmId),
    input: TicketInput,
    now: u64,
) -> Ticket {
    Ticket {
        realm_id,
        issuer,
        target: input.target,
        user_id: input.user_id,
        expires_at: ticket_expiry(now, input.grant.expires_at_secs),
        grant: input.grant,
        restrictions: input.restrictions,
        session: input.session,
        object_id: input.object_id,
        nonce: rand::random(),
        issued_at: now,
    }
}

/// What the pending call of a redemption answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RedeemStep {
    Config,
    Grant,
    Nonce,
}

/// Redeems a ticket for `object_id` on this node once and returns the caller it names. The
/// caller still checks the user's permission on this node's own path.
#[derive(Debug, PartialEq)]
pub struct RedeemTicketOperation {
    ids: NodeRealm,
    signed: SignedTicket,
    object_id: String,
    now: u64,
    step: RedeemStep,
    ticket: Option<Ticket>,
}

/// The redemption of `signed` on this node; `now` is Unix seconds.
pub fn redeem_ticket(
    ids: NodeRealm,
    signed: &SignedTicket,
    object_id: &str,
    now: u64,
) -> RedeemTicketOperation {
    RedeemTicketOperation {
        ids,
        signed: signed.clone(),
        object_id: object_id.to_string(),
        now,
        step: RedeemStep::Config,
        ticket: None,
    }
}

type Redeeming = Result<Next<AuthContext>, RedeemError>;

impl RedeemTicketOperation {
    fn ticket(&self) -> Result<&Ticket, IoError> {
        self.ticket.as_ref().ok_or(IoError::Unfinished)
    }
}

impl Operation for RedeemTicketOperation {
    type Output = AuthContext;
    type Error = RedeemError;

    fn first(&mut self) -> Redeeming {
        let (node_id, realm_id) = self.ids.ok_or(RedeemError::Unavailable)?;
        let ticket = (self.signed).verify(realm_id, node_id, &self.object_id, self.now)?;
        self.ticket = Some(ticket.clone());
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm_id))))
    }

    fn next(&mut self, result: IoResult) -> Redeeming {
        match self.step {
            RedeemStep::Config => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                let config = config
                    .inspect_err(config_unavailable)
                    .map_err(|_| RedeemError::Unavailable)?;
                let ticket = self.ticket()?;
                if !is_sync_eligible(&config, ticket.issuer) {
                    return Err(RedeemError::Issuer);
                }
                let check = GrantValidOperation::new(&ticket.grant, &ticket.user_id);
                self.step = RedeemStep::Grant;
                Ok(Next::Call(IoCall::child(check)))
            }
            RedeemStep::Grant => match result.child::<bool, ArunaBearerError>()? {
                Ok(true) => {
                    let ticket = self.ticket()?;
                    let expires = ticket.expires_at.saturating_add(SPENT_MARGIN_SECS);
                    let spend = SpendNonceOperation::new(ticket.nonce, expires, self.now);
                    self.step = RedeemStep::Nonce;
                    Ok(Next::Call(IoCall::child(spend)))
                }
                Ok(false) => Err(RedeemError::Revoked),
                Err(_) => Err(RedeemError::Unavailable),
            },
            RedeemStep::Nonce => {
                let spent = result.child::<(), NonceError>()?;
                spent.map_err(|error| match error {
                    NonceError::Replayed => RedeemError::Replayed,
                    _ => RedeemError::Unavailable,
                })?;
                let ticket = self.ticket.take().ok_or(IoError::Unfinished)?;
                let (_, realm_id) = self.ids.ok_or(RedeemError::Unavailable)?;
                Ok(Next::Done(AuthContext {
                    user_id: ticket.user_id,
                    realm_id,
                    path_restrictions: ticket.restrictions,
                    session: ticket.session,
                }))
            }
        }
    }
}

#[cfg(test)]
#[path = "ticket_tests.rs"]
mod tests;
