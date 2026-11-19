//! Checks the peer that forwards a metadata read and the caller its token stands for.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::events::PeerAuthRefusal;
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};

use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::metadata::protocol::MetadataReadError;
use crate::metadata::write_peer::WritePeerOperation;
use crate::realm::peer_trust::{PeerTrust, ensure_peer_trust};

type Turn<T> = Result<Next<T>, MetadataReadError>;

/// A forwarded caller as a read reports it: a token that failed is unauthorized, a check that
/// could not run leaves the read unavailable.
fn read_refusal(refusal: PeerAuthRefusal) -> MetadataReadError {
    match refusal {
        PeerAuthRefusal::Unauthorized => MetadataReadError::Unauthorized,
        PeerAuthRefusal::Unavailable(_) => MetadataReadError::Unavailable,
    }
}

fn caller_read(result: IoResult, realm_id: RealmId) -> Result<AuthContext, MetadataReadError> {
    let auth = result.child::<AuthContext, PeerAuthRefusal>()?;
    let auth = auth.map_err(read_refusal)?;
    match auth.realm_id == realm_id {
        true => Ok(auth),
        false => Err(MetadataReadError::Forbidden),
    }
}

fn config_read(result: IoResult) -> Result<RealmConfigDocument, MetadataReadError> {
    let config = result.child::<RealmConfigDocument, GetConfigError>()?;
    config.map_err(|_| MetadataReadError::Unavailable)
}

#[derive(Debug, PartialEq)]
enum ReadStage {
    Caller,
    Config,
}

/// The caller of a read a peer forwarded. A bearer or internal token is checked as a forwarded
/// write's is; without one the read is anonymous from a configured member. A trusted read also
/// needs a peer that may vouch for users.
#[derive(Debug, PartialEq)]
pub(crate) struct ReadPeerOperation {
    peer: NodeId,
    realm_id: RealmId,
    auth_token: Option<AuthToken>,
    trusted: bool,
    auth: Option<AuthContext>,
    stage: ReadStage,
}

impl ReadPeerOperation {
    pub(crate) fn new(
        (peer, realm_id): (NodeId, RealmId),
        auth_token: Option<AuthToken>,
        trusted: bool,
    ) -> Self {
        Self {
            peer,
            realm_id,
            auth_token,
            trusted,
            auth: None,
            stage: ReadStage::Caller,
        }
    }

    fn config(&mut self) -> Turn<Option<AuthContext>> {
        self.stage = ReadStage::Config;
        let config = GetConfigOperation::new(self.realm_id);
        Ok(Next::Call(IoCall::child(config)))
    }
}

impl Operation for ReadPeerOperation {
    type Output = Option<AuthContext>;
    type Error = MetadataReadError;

    fn first(&mut self) -> Turn<Option<AuthContext>> {
        match self.auth_token.take() {
            // A device replay vouches for a write, never for a read.
            Some(AuthToken::Replay { .. }) => Err(MetadataReadError::Unavailable),
            Some(token) => {
                let check = WritePeerOperation::new(self.peer, token);
                Ok(Next::Call(IoCall::child(check)))
            }
            None => self.config(),
        }
    }

    fn next(&mut self, result: IoResult) -> Turn<Option<AuthContext>> {
        match self.stage {
            ReadStage::Caller => {
                self.auth = Some(caller_read(result, self.realm_id)?);
                match self.trusted {
                    true => self.config(),
                    false => Ok(Next::Done(self.auth.take())),
                }
            }
            ReadStage::Config => {
                let config = config_read(result)?;
                let (peer, realm_id) = (self.peer, self.realm_id);
                let unavailable = |_| MetadataReadError::Unavailable;
                if self.auth.is_none() {
                    ensure_peer_trust(&config, peer, realm_id, PeerTrust::Member)
                        .map_err(unavailable)?;
                }
                if self.trusted {
                    ensure_peer_trust(&config, peer, realm_id, PeerTrust::Vouched(None))
                        .map_err(unavailable)?;
                }
                Ok(Next::Done(self.auth.take()))
            }
        }
    }
}

/// Which search or release request a peer forwards, and so what its caller must present.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PeerReader {
    /// An object search: a bearer of this realm, from a member.
    Objects,
    /// A bucket search: a bearer as for objects, or none for an anonymous reader.
    Buckets,
    /// A release membership request: a bearer, from a peer that may vouch for users.
    Release,
}

/// The caller of a search or release request a peer forwarded, as [`PeerReader`] admits it.
#[derive(Debug, PartialEq)]
pub(crate) struct SearchPeerOperation {
    peer: NodeId,
    realm_id: RealmId,
    auth_token: Option<AuthToken>,
    reader: PeerReader,
    auth: Option<AuthContext>,
    stage: ReadStage,
}

impl SearchPeerOperation {
    pub(crate) fn new(
        (peer, realm_id): (NodeId, RealmId),
        auth_token: Option<AuthToken>,
        reader: PeerReader,
    ) -> Self {
        Self {
            peer,
            realm_id,
            auth_token,
            reader,
            auth: None,
            stage: ReadStage::Caller,
        }
    }

    fn config(&mut self) -> Turn<AuthContext> {
        self.stage = ReadStage::Config;
        let config = GetConfigOperation::new(self.realm_id);
        Ok(Next::Call(IoCall::child(config)))
    }
}

impl Operation for SearchPeerOperation {
    type Output = AuthContext;
    type Error = MetadataReadError;

    fn first(&mut self) -> Turn<AuthContext> {
        match (self.auth_token.take(), self.reader) {
            (Some(token @ AuthToken::Bearer(_)), _) => {
                let check = WritePeerOperation::new(self.peer, token);
                Ok(Next::Call(IoCall::child(check)))
            }
            (None, PeerReader::Buckets) => self.config(),
            _ => Err(MetadataReadError::Unauthorized),
        }
    }

    fn next(&mut self, result: IoResult) -> Turn<AuthContext> {
        match self.stage {
            ReadStage::Caller => {
                let auth = caller_read(result, self.realm_id)?;
                if self.reader != PeerReader::Release {
                    return Ok(Next::Done(auth));
                }
                self.auth = Some(auth);
                self.config()
            }
            ReadStage::Config => {
                let (peer, realm_id) = (self.peer, self.realm_id);
                let Some(auth) = self.auth.take() else {
                    let config = config_read(result)?;
                    ensure_peer_trust(&config, peer, realm_id, PeerTrust::Member)
                        .map_err(|_| MetadataReadError::Unavailable)?;
                    return Ok(Next::Done(AuthContext::anonymous(realm_id)));
                };
                // A release asks an infrastructure peer; any doubt about it forbids the read.
                let config = config_read(result).map_err(|_| MetadataReadError::Forbidden)?;
                ensure_peer_trust(&config, peer, realm_id, PeerTrust::Vouched(None))
                    .map_err(|_| MetadataReadError::Forbidden)?;
                Ok(Next::Done(auth))
            }
        }
    }
}

#[cfg(test)]
#[path = "read_peer_tests.rs"]
mod call_tests;
