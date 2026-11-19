//! Checks the token a peer forwarded for a caller and that the peer may carry it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, NetEvent, PeerAuthRefusal};
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::metadata::{AuthToken, MetadataError};
use aruna_core::operation::Operation;
use aruna_core::rows::{event, value};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::types::Key;

use crate::auth::bearer_token::{
    DecodeTokenOperation, IssuerKeys, IssuerTrust, RevocationCheck, TokenChecks,
    UserCutoffOperation, claims_auth,
};
use crate::auth::error::ArunaBearerError;
use crate::realm::peer_trust::{PeerTrust, RealmPeerError, ensure_peer_trust};

type Turn = Result<Next<AuthContext>, PeerAuthRefusal>;

fn unavailable(error: MetadataError) -> PeerAuthRefusal {
    PeerAuthRefusal::Unavailable(error.to_string())
}

fn net_call(effect: NetEffect) -> IoCall {
    IoCall::Effect(Box::new(Effect::Net(effect)))
}

/// The checks of a forwarded bearer token: the stored issuers, and the issuing realm's
/// revocations once this node serves a realm.
pub(crate) fn forwarded_checks(local_realm: Option<RealmId>, blind: bool) -> TokenChecks {
    let revocation = match (blind, local_realm) {
        (false, Some(_)) => RevocationCheck::Realm,
        _ => RevocationCheck::Skipped,
    };
    TokenChecks {
        trust: IssuerTrust::Stored,
        revocation,
        linked_realm: local_realm,
        keys: IssuerKeys::Net,
    }
}

// Whether the realm config names `peer` with the trust the request needs.
fn peer_configured(
    config: Option<&[u8]>,
    (peer, realm_id): (NodeId, RealmId),
    trust: PeerTrust,
) -> Result<(), MetadataError> {
    let Some(bytes) = config else {
        return Err(MetadataError::InvalidInput(format!(
            "remote metadata peer `{peer}` is not configured in realm `{realm_id}`"
        )));
    };
    let document = RealmConfigDocument::from_bytes(bytes)
        .map_err(|error| MetadataError::Backend(error.to_string()))?;
    ensure_peer_trust(&document, peer, realm_id, trust).map_err(|error| {
        MetadataError::InvalidInput(match error {
            RealmPeerError::RealmMismatch { configured, .. } => format!(
                "realm config `{configured}` does not match remote metadata realm `{realm_id}`"
            ),
            RealmPeerError::NotConfigured { .. } => {
                format!("remote metadata peer `{peer}` is not configured in realm `{realm_id}`")
            }
            RealmPeerError::NotTrusted { .. } => format!(
                "remote metadata peer `{peer}` is not trusted for internal auth in realm `{realm_id}`"
            ),
        })
    })
}

#[derive(Debug, PartialEq)]
enum PeerStage {
    Realm,
    Bearer,
    Cutoff { auth: AuthContext, issued: u64 },
    Config { auth: AuthContext, trust: PeerTrust },
    Contact(AuthContext),
}

/// The caller a forwarded token stands for, accepted only from a peer of that caller's realm.
/// A bearer is verified against the stored issuers and revocations; an internal token, or a
/// device replay younger than its owner's token cutoff, needs a peer that may vouch for it.
#[derive(Debug, PartialEq)]
pub struct WritePeerOperation {
    peer: NodeId,
    token: Option<AuthToken>,
    contact: bool,
    /// Refuses device replays and checks the peer in the realm of every bearer.
    bound: bool,
    local_realm: Option<RealmId>,
    stage: PeerStage,
}

impl WritePeerOperation {
    /// The check of a forwarded caller that refreshes the peer's last-contact time once accepted.
    pub fn new(peer: NodeId, token: AuthToken) -> Self {
        Self {
            peer,
            token: Some(token),
            contact: true,
            bound: false,
            local_realm: None,
            stage: PeerStage::Realm,
        }
    }

    /// The same check, leaving the peer's last-contact time as it was.
    pub fn quiet(peer: NodeId, token: AuthToken) -> Self {
        Self {
            contact: false,
            ..Self::new(peer, token)
        }
    }

    /// The check of a sync mirror's caller: no device replay, a peer configured in the realm of
    /// every bearer, and the peer's last-contact time left as it was.
    pub(crate) fn mirror(peer: NodeId, token: AuthToken) -> Self {
        Self {
            bound: true,
            ..Self::quiet(peer, token)
        }
    }

    fn local_realm(&self, purpose: &str) -> Result<RealmId, PeerAuthRefusal> {
        self.local_realm.ok_or_else(|| {
            unavailable(MetadataError::InvalidInput(format!(
                "{purpose} requires a local serving realm"
            )))
        })
    }

    fn checked(&mut self) -> Turn {
        let token = (self.token.take()).ok_or_else(|| unavailable(MetadataError::InvalidEffect))?;
        match token {
            AuthToken::Bearer(token) => {
                self.stage = PeerStage::Bearer;
                let checks = forwarded_checks(self.local_realm, false);
                let decode = DecodeTokenOperation::new(token.as_str(), checks);
                Ok(Next::Call(IoCall::child(decode)))
            }
            AuthToken::Replay { .. } if self.bound => Err(PeerAuthRefusal::Unauthorized),
            AuthToken::Replay {
                auth,
                issued_at,
                authored_at_ms,
            } => {
                let cutoff = UserCutoffOperation::new(auth.realm_id, &auth.user_id);
                let issued = issued_at.min(authored_at_ms / 1000);
                self.stage = PeerStage::Cutoff { auth, issued };
                Ok(Next::Call(IoCall::child(cutoff)))
            }
            AuthToken::Internal(auth) => self.internal(auth),
        }
    }

    // A user of another realm may sign in here; an anonymous one never.
    fn internal(&mut self, auth: AuthContext) -> Turn {
        let local_realm = self.local_realm("internal metadata auth")?;
        if auth.realm_id != local_realm {
            return Err(unavailable(MetadataError::InvalidInput(format!(
                "internal metadata auth realm `{}` does not match local realm `{local_realm}`",
                auth.realm_id
            ))));
        }
        if auth.user_id.realm_id != auth.realm_id && auth.user_id.is_nil() {
            return Err(unavailable(MetadataError::InvalidInput(format!(
                "internal metadata auth user realm `{}` does not match token realm `{}`",
                auth.user_id.realm_id, auth.realm_id
            ))));
        }
        let trust = PeerTrust::Vouched(Some(auth.user_id));
        self.config(auth, trust)
    }

    fn config(&mut self, auth: AuthContext, trust: PeerTrust) -> Turn {
        let key = Key::from(auth.realm_id.as_bytes().to_vec());
        self.stage = PeerStage::Config { auth, trust };
        let read = vec![(REALM_CONFIG_KEYSPACE.to_string(), key)];
        Ok(Next::Call(IoCall::Read(read)))
    }

    fn accepted(&mut self, auth: AuthContext) -> Turn {
        if !self.contact {
            return Ok(Next::Done(auth));
        }
        self.stage = PeerStage::Contact(auth);
        Ok(Next::Call(net_call(NetEffect::PeerContact(self.peer))))
    }
}

impl Operation for WritePeerOperation {
    type Output = AuthContext;
    type Error = PeerAuthRefusal;

    fn first(&mut self) -> Turn {
        Ok(Next::Call(net_call(NetEffect::LocalRealm)))
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match std::mem::replace(&mut self.stage, PeerStage::Realm) {
            PeerStage::Realm => {
                let Event::Net(NetEvent::LocalRealm(local_realm)) = event(result)? else {
                    return Err(unavailable(MetadataError::InvalidEffect));
                };
                self.local_realm = local_realm;
                self.checked()
            }
            PeerStage::Bearer => {
                let claims = result.child::<_, ArunaBearerError>()?;
                let auth =
                    (claims.and_then(claims_auth)).map_err(|_| PeerAuthRefusal::Unauthorized)?;
                let local_realm = self.local_realm("forwarded metadata auth")?;
                match auth.realm_id == local_realm || self.bound {
                    true => self.config(auth, PeerTrust::Member),
                    false => self.accepted(auth),
                }
            }
            PeerStage::Cutoff { auth, issued } => {
                let cutoff = result.child::<Option<u64>, ArunaBearerError>()?;
                let cutoff = cutoff
                    .map_err(|error| unavailable(MetadataError::Backend(error.to_string())))?;
                match cutoff.is_some_and(|cutoff| issued < cutoff) {
                    true => Err(PeerAuthRefusal::Unauthorized),
                    false => self.internal(auth),
                }
            }
            PeerStage::Config { auth, trust } => {
                let config = value(result)?;
                peer_configured(config.as_deref(), (self.peer, auth.realm_id), trust)
                    .map_err(unavailable)?;
                self.accepted(auth)
            }
            // The caller is accepted; a missed contact refresh only dates the peer's liveness.
            PeerStage::Contact(auth) => {
                let _ = event(result);
                Ok(Next::Done(auth))
            }
        }
    }
}

#[cfg(test)]
#[path = "write_peer_tests.rs"]
mod call_tests;
