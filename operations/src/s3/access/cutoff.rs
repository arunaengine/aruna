//! Deletes this node's S3 credentials issued before a live cutoff of their owner.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::USER_ACCESS_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::shutdown::Shutdown;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::blob::UserAccess;
use aruna_core::types::{Effects, Key};
use smallvec::smallvec;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use super::revoke::{RevokeUserError, RevokeUserOperation};
use crate::driver::{DriverContext, drive};

/// Credentials read per storage page.
const PAGE: usize = 256;
/// A cutoff stays in the realm's revocation set for 366 days, so hourly sweeps delete every
/// credential it ends long before then. A node offline for that whole time can miss it.
pub const SWEEP_INTERVAL: Duration = Duration::from_secs(60 * 60);

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
    #[error("cutoff sweep did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SweepState {
    Init,
    ReadConfig,
    Scan,
    Revoke,
    Finish,
}

/// Reads the realm's cutoffs, then pages through the credentials and revokes, one at a time,
/// each this node issued before its owner's live cutoff. The output counts the revoked ones.
#[derive(Debug, PartialEq)]
pub struct CutoffSweepOperation {
    config: CutoffSweepConfig,
    state: SweepState,
    cutoffs: Option<RealmConfigDocument>,
    pending: Vec<String>,
    after: Option<Key>,
    revoked: usize,
    output: Option<Result<usize, CutoffSweepError>>,
}

impl CutoffSweepOperation {
    pub fn new(config: CutoffSweepConfig) -> Self {
        Self {
            config,
            state: SweepState::Init,
            cutoffs: None,
            pending: Vec::new(),
            after: None,
            revoked: 0,
            output: None,
        }
    }

    fn finish(&mut self, result: Result<usize, CutoffSweepError>) -> Effects {
        self.state = SweepState::Finish;
        self.output = Some(result);
        smallvec![]
    }

    fn unexpected(&mut self, expected: &'static str, event: Event) -> Effects {
        let error = CutoffSweepError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            got: format!("{event:?}"),
        };
        self.finish(Err(error))
    }

    fn scan(&mut self, start: Option<Key>) -> Effects {
        self.state = SweepState::Scan;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: USER_ACCESS_KEYSPACE.to_string(),
            prefix: None,
            start: start.map(IterStart::After),
            limit: PAGE,
            txn_id: None,
        })]
    }

    /// Whether this node issued the credential before a live cutoff of its owner.
    fn cut_off(&self, access: &UserAccess) -> bool {
        let issued = Ulid::from_string(&access.access_key).map_or(0, |id| id.timestamp_ms() / 1000);
        let cutoff = (self.cutoffs.as_ref())
            .and_then(|config| config.user_cutoff(&access.user_identity, self.config.now));
        access.issued_by == *self.config.node_id.as_bytes()
            && cutoff.is_some_and(|cutoff| issued < cutoff)
    }

    fn handle_config(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected("realm config read", event);
        };
        let Some(bytes) = value else {
            return self.finish(Ok(0));
        };
        match RealmConfigDocument::from_bytes(&bytes) {
            Ok(config) => self.cutoffs = Some(config),
            Err(error) => return self.finish(Err(error.into())),
        }
        self.scan(None)
    }

    fn handle_page(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return self.unexpected("credential page", event);
        };
        for (_, value) in values {
            match UserAccess::from_bytes(&value) {
                Ok(access) if self.cut_off(&access) => self.pending.push(access.access_key),
                Ok(_) => {}
                Err(error) => return self.finish(Err(error.into())),
            }
        }
        self.after = next_start_after;
        self.next()
    }

    fn next(&mut self) -> Effects {
        if let Some(access_key) = self.pending.pop() {
            self.state = SweepState::Revoke;
            let revoke = RevokeUserOperation::new(access_key);
            return smallvec![Effect::SubOperation(boxed_suboperation(revoke, revoked))];
        }
        match self.after.take() {
            Some(after) => self.scan(Some(after)),
            None => self.finish(Ok(self.revoked)),
        }
    }

    fn handle_revoked(&mut self, event: Event) -> Effects {
        let Event::SubOperation(SubOperationEvent::TokenRevoked { result }) = event else {
            return self.unexpected("credential revocation", event);
        };
        match result {
            Ok(()) => self.revoked += 1,
            Err(error) => return self.finish(Err(CutoffSweepError::Revocation(error))),
        }
        self.next()
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

    fn start(&mut self) -> Effects {
        self.state = SweepState::ReadConfig;
        let target = DocumentTarget::RealmConfig {
            realm_id: self.config.realm_id,
        };
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key(),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.finish(Err(error.into()));
        }
        match self.state {
            SweepState::ReadConfig => self.handle_config(event),
            SweepState::Scan => self.handle_page(event),
            SweepState::Revoke => self.handle_revoked(event),
            SweepState::Init | SweepState::Finish => self.unexpected("no event", event),
        }
    }

    fn is_complete(&self) -> bool {
        self.state == SweepState::Finish
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(CutoffSweepError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// Sweeps once per `SWEEP_INTERVAL` until shutdown; a failed sweep is retried at the next one.
pub fn spawn_cutoff_sweep(
    context: Arc<DriverContext>,
    shutdown: &Shutdown,
    realm_id: RealmId,
    node_id: NodeId,
) {
    let token = shutdown.token();
    shutdown.spawn(async move {
        loop {
            tokio::select! {
                _ = token.cancelled() => return,
                _ = tokio::time::sleep(SWEEP_INTERVAL) => {}
            }
            let now = aruna_core::time::unix_timestamp_secs();
            let config = CutoffSweepConfig {
                realm_id,
                node_id,
                now,
            };
            match drive(CutoffSweepOperation::new(config), context.as_ref()).await {
                Ok(0) => {}
                Ok(revoked) => tracing::info!(revoked, "Deleted S3 credentials of cut-off owners"),
                Err(error) => warn!(error = %error, "S3 credential cutoff sweep failed"),
            }
        }
    });
}

#[cfg(test)]
#[path = "cutoff_tests.rs"]
mod tests;
