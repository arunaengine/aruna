//! Keyed plugin creates on the receiver: one row per install and key names the document, the
//! node that creates it and the arguments, so every retry sends the same create there.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::PLUGIN_CREATES_KEYSPACE;
use aruna_core::metadata::{AuthToken, MetadataError};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::plugin_runs::CallIntent;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::{NodeId, UserId};
use aruna_plugins::broker::{Created, ErrorKind, HostError};
use serde::{Deserialize, Serialize};
use tracing::warn;
use ulid::Ulid;

use super::host_calls::{CallEnd, CallFailure, RecordCall};
use super::host_steps::{Granted, ReadyCreate};
use super::rows::{StoreError, decode, done, encode, read_call, row, values};
use super::unavailable;
use crate::auth::error::AuthorizeError;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::forward::transport::MetadataWriteError;
use crate::metadata::api::MetadataApiError;
use crate::metadata::api::export_types::GetVisibleRequest;
use crate::metadata::create_document::CreateDocumentError;
use crate::metadata::forward::authorized_create::CreateWithIdOperation;
use crate::metadata::forward::pinned::PinnedCreate;
use crate::metadata::forward::routed_read::RoutedReadOperation;
use crate::metadata::forward::write::CreateAuthorizedError;
use crate::metadata::update_document::UpdateDocumentError;

/// How the key row of a create was found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Claim {
    /// This call wrote the row; nothing was sent for the key yet.
    Fresh,
    /// An earlier call wrote it; its create may have happened.
    Earlier,
}

/// What a routed lookup of an earlier create's document found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lookup {
    /// The document exists with this establishing revision.
    Found(Ulid),
    /// No holder knows the document.
    Missing,
    /// No holder answered.
    Unreachable,
}

/// What a keyed create does next.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Step {
    /// Send the stored create to its stored node.
    Dispatch,
    /// Look the stored document up first.
    Lookup,
    /// IoResult the existing document and its establishing revision.
    Adopt(Ulid),
    /// The outcome stays unknown until a person resolves it.
    Unknown,
}

/// The next step of a keyed create from how its row was found and what a lookup found. A
/// missing document is sent again only to the stored node, with the stored arguments.
pub fn next_step(claim: Claim, lookup: Option<Lookup>) -> Step {
    match (claim, lookup) {
        (Claim::Fresh, _) => Step::Dispatch,
        (Claim::Earlier, None) => Step::Lookup,
        (Claim::Earlier, Some(Lookup::Found(revision))) => Step::Adopt(revision),
        (Claim::Earlier, Some(Lookup::Missing)) => Step::Dispatch,
        (Claim::Earlier, Some(Lookup::Unreachable)) => Step::Unknown,
    }
}

/// The stored create of a key and what is known about its attempts.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct KeyRow {
    /// The account of the install that wrote it; a reinstall under the same id has another one.
    account: UserId,
    create: PinnedCreate,
    /// Attempts without a known outcome; one whose outcome stays unknown keeps this above zero.
    open: u32,
    /// The last finished attempt was refused for its arguments, so it committed nothing.
    rejected: bool,
}

fn create_key(install_id: Ulid, key: &str) -> Vec<u8> {
    [&install_id.to_bytes()[..], key.as_bytes()].concat()
}

/// Opens an attempt of `key` in one transaction, so concurrent calls adopt one document. A new key
/// (or a former install's) stores `minted`; a refused one with no open attempt takes its arguments.
pub fn claim_key((install, key): (&PluginInstall, &str), minted: PinnedCreate) -> ClaimKey {
    ClaimKey {
        row: create_key(install.install_id, key),
        account: install.account,
        minted: Box::new(minted),
        claimed: None,
    }
}

#[derive(Debug, PartialEq)]
pub struct ClaimKey {
    row: Vec<u8>,
    account: UserId,
    minted: Box<PinnedCreate>,
    claimed: Option<(Box<PinnedCreate>, Claim)>,
}

impl Operation for ClaimKey {
    type Output = (PinnedCreate, Claim);
    type Error = StoreError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<Self::Output>, StoreError> {
        Ok(Next::Call(read_call(vec![(
            PLUGIN_CREATES_KEYSPACE,
            self.row.clone(),
        )])))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<Self::Output>, StoreError> {
        if let Some((create, claim)) = self.claimed.take() {
            done(answer)?;
            return Ok(Next::Done((*create, claim)));
        }
        let [stored] = values::<1>(answer)?;
        let stored = stored.map(|bytes| decode::<KeyRow>(&bytes)).transpose()?;
        let stored = stored.filter(|stored| stored.account == self.account);
        let minted = (*self.minted).clone();
        let (create, claim, open) = match stored {
            None => (minted, Claim::Fresh, 1),
            Some(stored) if stored.rejected && stored.open == 0 => {
                let corrected = PinnedCreate {
                    group_id: stored.create.group_id,
                    document_id: stored.create.document_id,
                    target: stored.create.target,
                    ..minted
                };
                (corrected, Claim::Fresh, 1)
            }
            Some(stored) => (stored.create, Claim::Earlier, stored.open.saturating_add(1)),
        };
        let value = KeyRow {
            account: self.account,
            create: create.clone(),
            open,
            rejected: false,
        };
        self.claimed = Some((Box::new(create), claim));
        let write = row(PLUGIN_CREATES_KEYSPACE, self.row.clone(), encode(&value)?);
        Ok(Next::Call(IoCall::Write(vec![write])))
    }
}

/// Closes one attempt of `key` whose outcome is known: it committed, or it was refused without
/// an effect, `rejected` when the refusal concerns its arguments.
pub fn settle_key((install, key): (&PluginInstall, &str), rejected: bool) -> SettleKey {
    SettleKey {
        row: create_key(install.install_id, key),
        account: install.account,
        rejected,
        written: false,
    }
}

#[derive(Debug, PartialEq)]
pub struct SettleKey {
    row: Vec<u8>,
    account: UserId,
    rejected: bool,
    written: bool,
}

impl Operation for SettleKey {
    type Output = ();
    type Error = StoreError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<()>, StoreError> {
        Ok(Next::Call(read_call(vec![(
            PLUGIN_CREATES_KEYSPACE,
            self.row.clone(),
        )])))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<()>, StoreError> {
        if self.written {
            done(answer)?;
            return Ok(Next::Done(()));
        }
        let [stored] = values::<1>(answer)?;
        let Some(bytes) = stored else {
            return Ok(Next::Done(()));
        };
        let mut stored: KeyRow = decode(&bytes)?;
        if stored.account != self.account {
            return Ok(Next::Done(()));
        }
        stored.open = stored.open.saturating_sub(1);
        stored.rejected = self.rejected;
        self.written = true;
        let write = row(PLUGIN_CREATES_KEYSPACE, self.row.clone(), encode(&stored)?);
        Ok(Next::Call(IoCall::Write(vec![write])))
    }
}

/// The intent reply of a create: the document id and its establishing revision.
pub fn created_text(document_id: &Ulid, revision: &Ulid) -> String {
    format!("{document_id} {revision}")
}

/// A create reply read back from its stored form.
pub fn stored_created(reply: &str) -> Result<Created, HostError> {
    let (resource, revision) = (reply.split_once(' '))
        .ok_or_else(|| unavailable("the stored create reply is not readable"))?;
    Ok(Created {
        resource: resource.to_string(),
        revision: revision.to_string(),
    })
}

/// Whether a failed create had no effect, or may have had one.
fn create_outcome(error: CreateAuthorizedError) -> CallEnd {
    let none = |kind, message: String| CallEnd::None(HostError::new(kind, message));
    match error {
        CreateAuthorizedError::EmptyPath => none(ErrorKind::Invalid, error.to_string()),
        CreateAuthorizedError::Forbidden
        | CreateAuthorizedError::Authorize(
            AuthorizeError::PermissionDenied | AuthorizeError::Policy(_),
        ) => none(ErrorKind::Denied, error.to_string()),
        CreateAuthorizedError::Authorize(_) | CreateAuthorizedError::Api(_) => {
            CallEnd::None(unavailable(error.to_string()))
        }
        CreateAuthorizedError::Create(CreateDocumentError::MetadataError(
            MetadataError::ProfileValidation(_)
            | MetadataError::Validation(_)
            | MetadataError::InvalidInput(_),
        )) => none(ErrorKind::Invalid, error.to_string()),
        CreateAuthorizedError::Create(CreateDocumentError::HoldsNoBucket) => {
            CallEnd::None(unavailable(error.to_string()))
        }
        CreateAuthorizedError::Create(_) => CallEnd::Maybe(error.to_string()),
        CreateAuthorizedError::Write(error) => write_outcome(error),
    }
}

/// Whether a failed write had no effect, or may have had one.
pub fn write_outcome(error: MetadataWriteError) -> CallEnd {
    let none = |kind, message: String| CallEnd::None(HostError::new(kind, message));
    match error {
        MetadataWriteError::Unauthorized | MetadataWriteError::Forbidden => {
            none(ErrorKind::Denied, error.to_string())
        }
        MetadataWriteError::NotFound => none(ErrorKind::NotFound, error.to_string()),
        MetadataWriteError::Update(UpdateDocumentError::RevisionConflict { .. }) => {
            none(ErrorKind::Conflict, error.to_string())
        }
        MetadataWriteError::Update(UpdateDocumentError::GraphPending { .. }) => {
            CallEnd::None(unavailable(error.to_string()))
        }
        MetadataWriteError::Update(UpdateDocumentError::StorageError(_))
        | MetadataWriteError::Undeliverable(_) => CallEnd::Maybe(error.to_string()),
        other => none(ErrorKind::Invalid, other.to_string()),
    }
}

#[derive(Debug, PartialEq)]
enum CreatePhase {
    Claim,
    Lookup,
    Create,
    Settle(Box<Result<String, CallEnd>>),
    Record,
}

/// Creates the keyed document once and records the call's outcome: the stored create goes to its
/// stored node with its stored arguments, and a key from an earlier call is looked up first, so
/// a found document is adopted.
#[derive(Debug, PartialEq)]
pub struct FinishCreate {
    actor: Actor,
    granted: Granted,
    network_node: Option<NodeId>,
    key: String,
    run_id: Ulid,
    intent: CallIntent,
    minted: Option<Result<Box<PinnedCreate>, String>>,
    pinned: Option<Box<PinnedCreate>>,
    claim: Claim,
    lookup: Option<Lookup>,
    phase: CreatePhase,
}

impl FinishCreate {
    pub fn new(
        ready: ReadyCreate,
        network_node: Option<NodeId>,
        key: &str,
        (run_id, intent): (Ulid, CallIntent),
    ) -> Self {
        Self {
            actor: ready.actor,
            granted: ready.granted,
            network_node,
            key: key.to_string(),
            run_id,
            intent,
            minted: Some(ready.minted),
            pinned: None,
            claim: Claim::Fresh,
            lookup: None,
            phase: CreatePhase::Claim,
        }
    }

    fn record(&mut self, outcome: Result<String, CallEnd>) -> Next<String> {
        self.phase = CreatePhase::Record;
        let record = RecordCall::new(self.run_id, self.intent.clone(), outcome);
        Next::Call(IoCall::child(record))
    }

    fn settle(&mut self, rejected: bool, outcome: Result<String, CallEnd>) -> Next<String> {
        self.phase = CreatePhase::Settle(Box::new(outcome));
        let key = (&self.granted.install, self.key.as_str());
        Next::Call(IoCall::child(settle_key(key, rejected)))
    }

    fn step(&mut self) -> Result<Next<String>, CallFailure> {
        let pinned = self.pinned.as_ref().ok_or(IoError::Unfinished)?;
        let document_id = pinned.document_id;
        Ok(match next_step(self.claim, self.lookup) {
            Step::Dispatch => {
                let extras = PolicyRequestExtras::operation("plugin.metadata.create");
                let who = (self.actor.clone(), self.network_node);
                let auth = (self.granted.auth.clone(), extras);
                let create = CreateWithIdOperation::new(who, auth, (**pinned).clone());
                self.phase = CreatePhase::Create;
                Next::Call(IoCall::child(create))
            }
            Step::Lookup => {
                let auth = self.granted.auth.clone();
                let request = GetVisibleRequest {
                    document_id,
                    auth: Some(auth.clone()),
                };
                let token = Some(AuthToken::Internal(auth));
                let realm_id = self.actor.realm_id;
                let read = RoutedReadOperation::new(self.network_node, realm_id, request, token);
                self.phase = CreatePhase::Lookup;
                Next::Call(IoCall::child(read))
            }
            Step::Adopt(revision) => self.settle(false, Ok(created_text(&document_id, &revision))),
            Step::Unknown => {
                let reason = "the document of an earlier create with this key is not reachable";
                self.record(Err(CallEnd::Maybe(reason.to_string())))
            }
        })
    }

    /// A known outcome closes the attempt; a refusal of the arguments lets a later call correct
    /// them. An unknown one keeps them fixed.
    fn created(
        &mut self,
        created: Result<MetadataRegistryRecord, CreateAuthorizedError>,
    ) -> Next<String> {
        let outcome = (created.as_ref().ok())
            .map(|record| created_text(&record.document_id, &record.establishing_event_id));
        match (outcome, created.err().map(create_outcome)) {
            (Some(text), _) => self.settle(false, Ok(text)),
            (None, Some(CallEnd::None(error))) => {
                let rejected = error.kind == ErrorKind::Invalid;
                self.settle(rejected, Err(CallEnd::None(error)))
            }
            (None, Some(end)) => self.record(Err(end)),
            (None, None) => self.record(Err(CallEnd::Maybe("no outcome".to_string()))),
        }
    }
}

impl Operation for FinishCreate {
    type Output = String;
    type Error = CallFailure;

    fn first(&mut self) -> Result<Next<String>, CallFailure> {
        match self.minted.take().ok_or(IoError::Unfinished)? {
            Ok(minted) => {
                let key = (&self.granted.install, self.key.as_str());
                Ok(Next::Call(IoCall::child(claim_key(key, *minted))))
            }
            Err(why) => Ok(self.record(Err(CallEnd::None(unavailable(why))))),
        }
    }

    fn next(&mut self, result: IoResult) -> Result<Next<String>, CallFailure> {
        match std::mem::replace(&mut self.phase, CreatePhase::Record) {
            CreatePhase::Claim => match result.child::<(PinnedCreate, Claim), StoreError>()? {
                Ok((pinned, claim)) => {
                    (self.pinned, self.claim) = (Some(Box::new(pinned)), claim);
                    self.step()
                }
                Err(error) => Ok(self.record(Err(CallEnd::None(unavailable(error.0))))),
            },
            CreatePhase::Lookup => {
                let found = result.child::<MetadataRegistryRecord, MetadataApiError>()?;
                self.lookup = Some(match found {
                    Ok(record) => Lookup::Found(record.establishing_event_id),
                    Err(MetadataApiError::NotFound) => Lookup::Missing,
                    Err(_) => Lookup::Unreachable,
                });
                self.step()
            }
            CreatePhase::Create => {
                let created = result.child::<MetadataRegistryRecord, CreateAuthorizedError>()?;
                Ok(self.created(created))
            }
            CreatePhase::Settle(outcome) => {
                // A failure only keeps the key's arguments fixed.
                if let Err(error) = result.child::<(), StoreError>()? {
                    warn!(%error, "A plugin create key could not record its attempt's outcome");
                }
                Ok(self.record(*outcome))
            }
            CreatePhase::Record => Ok(Next::Done(result.child::<String, CallFailure>()??)),
        }
    }
}

#[cfg(test)]
#[path = "creates_tests.rs"]
mod tests;
