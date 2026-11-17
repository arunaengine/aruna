//! Deletes this node's S3 credentials issued before a live cutoff of their owner.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, SubOperationEvent};
use aruna_core::keyspaces::USER_ACCESS_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::rows::{PrefixPages, read_row};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::blob::UserAccess;
use thiserror::Error;
use ulid::Ulid;

use super::revoke::{RevokeUserError, RevokeUserOperation};

/// Credentials read per storage page.
const PAGE: usize = 256;

#[derive(Clone, Debug, PartialEq)]
pub struct CutoffSweepConfig {
    pub realm_id: RealmId,
    pub node_id: NodeId,
    /// Unix seconds the cutoffs are judged at.
    pub now: u64,
}

#[derive(Debug, Error, PartialEq)]
pub enum CutoffSweepError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("credential revocation failed: {0}")]
    Revocation(String),
    #[error("cutoff sweep failed: {0}")]
    Io(IoError),
}

impl From<IoError> for CutoffSweepError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            other => Self::Io(other),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SweepState {
    ReadConfig,
    Scan,
    Revoke,
}

/// Reads the realm's cutoffs, then pages through the credentials and revokes, one at a time,
/// each this node issued before its owner's live cutoff. The output counts the revoked ones.
#[derive(Debug, PartialEq)]
pub struct CutoffSweepOperation {
    config: CutoffSweepConfig,
    state: SweepState,
    cutoffs: Option<RealmConfigDocument>,
    pending: Vec<String>,
    pages: PrefixPages,
    revoked: usize,
}

impl CutoffSweepOperation {
    pub fn new(config: CutoffSweepConfig) -> Self {
        Self {
            config,
            state: SweepState::ReadConfig,
            cutoffs: None,
            pending: Vec::new(),
            pages: PrefixPages::new(USER_ACCESS_KEYSPACE, Vec::new(), PAGE),
            revoked: 0,
        }
    }

    /// Whether this node issued the credential before a live cutoff of its owner.
    fn cut_off(&self, access: &UserAccess) -> bool {
        let issued = Ulid::from_string(&access.access_key).map_or(0, |id| id.timestamp_ms() / 1000);
        let cutoff = (self.cutoffs.as_ref())
            .and_then(|config| config.user_cutoff(&access.user_identity, self.config.now));
        access.issued_by == *self.config.node_id.as_bytes()
            && cutoff.is_some_and(|cutoff| issued < cutoff)
    }

    fn handle_config(&mut self, result: IoResult) -> Result<Next<usize>, CutoffSweepError> {
        let Some(bytes) = result.value()? else {
            return Ok(Next::Done(0));
        };
        self.cutoffs = Some(RealmConfigDocument::from_bytes(&bytes)?);
        self.state = SweepState::Scan;
        Ok(Next::Call(self.pages.call()))
    }

    fn handle_page(&mut self, result: IoResult) -> Result<Next<usize>, CutoffSweepError> {
        for (_, value) in self.pages.take(result)? {
            let access = UserAccess::from_bytes(&value)?;
            if self.cut_off(&access) {
                self.pending.push(access.access_key);
            }
        }
        Ok(self.advance())
    }

    fn advance(&mut self) -> Next<usize> {
        if let Some(access_key) = self.pending.pop() {
            self.state = SweepState::Revoke;
            let revoke = RevokeUserOperation::new(access_key);
            return Next::Call(IoCall::Child(boxed_suboperation(revoke, revoked)));
        }
        if self.pages.done() {
            return Next::Done(self.revoked);
        }
        self.state = SweepState::Scan;
        Next::Call(self.pages.call())
    }

    fn handle_revoked(&mut self, result: IoResult) -> Result<Next<usize>, CutoffSweepError> {
        let Event::SubOperation(SubOperationEvent::TokenRevoked { result }) = result.event()?
        else {
            let expected = "SubOperationEvent::TokenRevoked";
            let got = "another event";
            return Err(IoError::Unexpected { expected, got }.into());
        };
        result.map_err(CutoffSweepError::Revocation)?;
        self.revoked += 1;
        Ok(self.advance())
    }
}

/// A credential deleted since the page was read is already gone, which is what the sweep wants.
fn revoked(result: Result<UserAccess, RevokeUserError>) -> Event {
    let result = match result {
        Ok(_) | Err(RevokeUserError::NotFound) => Ok(()),
        Err(error) => Err(error.to_string()),
    };
    Event::SubOperation(SubOperationEvent::TokenRevoked { result })
}

impl Operation for CutoffSweepOperation {
    type Output = usize;
    type Error = CutoffSweepError;

    fn first(&mut self) -> Result<Next<usize>, CutoffSweepError> {
        let target = DocumentTarget::RealmConfig {
            realm_id: self.config.realm_id,
        };
        let read = read_row(target.storage_keyspace(), target.storage_key());
        Ok(Next::Call(read))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<usize>, CutoffSweepError> {
        match self.state {
            SweepState::ReadConfig => self.handle_config(result),
            SweepState::Scan => self.handle_page(result),
            SweepState::Revoke => self.handle_revoked(result),
        }
    }
}

#[cfg(test)]
#[path = "cutoff_tests.rs"]
mod tests;
