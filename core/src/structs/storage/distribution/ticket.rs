//! Defines the download ticket a node puts in a private download redirect (D33). It names one
//! caller, one exact object version and one target node for a few seconds, is redeemed once,
//! and is never a credential. Tickets are never logged.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::NodeId;
use crate::UserId;
use crate::structs::identity::auth::{PathRestriction, SessionRef};
use crate::structs::identity::realm::RealmId;
use crate::structs::storage::backend_import::TokenGrant;

/// The query parameter a redirect carries its ticket in; request logs redact it.
pub const TICKET_QUERY: &str = "ticket";
/// The longest a ticket is valid.
pub const TICKET_SECS: u64 = 10;
/// Clock difference between nodes a ticket tolerates at its start.
const SKEW_SECS: u64 = 30;
/// The largest encoded ticket a target decodes.
const MAX_ENCODED: usize = 16 * 1024;
const DOMAIN: &str = "aruna-download-ticket-v1";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Ticket {
    pub realm_id: RealmId,
    /// The node that verified the caller and chose the target.
    pub issuer: NodeId,
    pub target: NodeId,
    pub user_id: UserId,
    /// The non-secret identity of the caller's bearer token; redemption checks its revocation.
    pub grant: TokenGrant,
    pub restrictions: Option<Vec<PathRestriction>>,
    /// The session the caller's request ran in.
    pub session: Option<SessionRef>,
    /// The exact object version, as the original download named it.
    pub object_id: String,
    pub nonce: [u8; 16],
    /// Unix seconds at which the issuer signed.
    pub issued_at: u64,
    /// Unix seconds after which the ticket is spent; never later than the token's expiry.
    pub expires_at: u64,
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SignedTicket {
    pub ticket: Ticket,
    pub signature: iroh::Signature,
}

/// Leaves the signature out, so a ticket printed by accident cannot be redeemed.
impl std::fmt::Debug for SignedTicket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SignedTicket")
            .field("issuer", &self.ticket.issuer)
            .field("target", &self.ticket.target)
            .field("expires_at", &self.ticket.expires_at)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum TicketError {
    #[error("the download ticket does not decode")]
    Encoding,
    #[error("the download ticket signature does not verify")]
    Signature,
    #[error("the download ticket is for another realm or node")]
    WrongTarget,
    #[error("the download ticket is not valid at this time")]
    Expired,
    #[error("the download ticket names another object")]
    ObjectMismatch,
}

/// When a ticket issued at `now` ends: a few seconds later, never after the credential does.
pub fn ticket_expiry(now: u64, credential_expiry: u64) -> u64 {
    now.saturating_add(TICKET_SECS).min(credential_expiry)
}

impl Ticket {
    pub fn signing_bytes(&self) -> Result<Vec<u8>, TicketError> {
        postcard::to_allocvec(&(DOMAIN, self)).map_err(|_| TicketError::Encoding)
    }
}

impl SignedTicket {
    /// Checks the issuer's signature, that this realm and node are the target, that the
    /// ticket names `object_id`, and that `now` (Unix seconds) lies inside its validity.
    pub fn verify(
        &self,
        realm_id: RealmId,
        target: NodeId,
        object_id: &str,
        now: u64,
    ) -> Result<&Ticket, TicketError> {
        let ticket = &self.ticket;
        let bytes = ticket.signing_bytes()?;
        if ticket.issuer.verify(&bytes, &self.signature).is_err() {
            return Err(TicketError::Signature);
        }
        if ticket.realm_id != realm_id || ticket.target != target {
            return Err(TicketError::WrongTarget);
        }
        if ticket.object_id != object_id {
            return Err(TicketError::ObjectMismatch);
        }
        let started = now.saturating_add(SKEW_SECS) >= ticket.issued_at;
        let bounded = ticket.expires_at <= ticket.issued_at.saturating_add(TICKET_SECS);
        if !started || !bounded || now > ticket.expires_at {
            return Err(TicketError::Expired);
        }
        Ok(ticket)
    }

    pub fn encode(&self) -> Result<String, TicketError> {
        let bytes = postcard::to_allocvec(self).map_err(|_| TicketError::Encoding)?;
        Ok(URL_SAFE_NO_PAD.encode(bytes))
    }

    pub fn decode(value: &str) -> Result<Self, TicketError> {
        if value.len() > MAX_ENCODED {
            return Err(TicketError::Encoding);
        }
        let bytes = URL_SAFE_NO_PAD
            .decode(value)
            .map_err(|_| TicketError::Encoding)?;
        postcard::from_bytes(&bytes).map_err(|_| TicketError::Encoding)
    }
}

#[cfg(test)]
#[path = "ticket_tests.rs"]
mod tests;
