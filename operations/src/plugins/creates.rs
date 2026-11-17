//! Keyed plugin creates on the receiver: one row per install and key names the document, the
//! node that creates it and the arguments, so every retry sends the same create there.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::PLUGIN_CREATES_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::plugin_installs::PluginInstall;
use aruna_plugins::broker::{Created, HostError};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::rows::{StoreError, decode, done, encode, read_call, row, values};
use super::unavailable;
use crate::metadata::forward::write::PinnedCreate;

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
pub(crate) fn next_step(claim: Claim, lookup: Option<Lookup>) -> Step {
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

    fn transaction(&self) -> bool {
        true
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
pub(crate) fn settle_key((install, key): (&PluginInstall, &str), rejected: bool) -> SettleKey {
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

    fn transaction(&self) -> bool {
        true
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
pub(crate) fn created_text(document_id: &Ulid, revision: &Ulid) -> String {
    format!("{document_id} {revision}")
}

/// A create reply read back from its stored form.
pub(crate) fn stored_created(reply: &str) -> Result<Created, HostError> {
    let (resource, revision) = (reply.split_once(' '))
        .ok_or_else(|| unavailable("the stored create reply is not readable"))?;
    Ok(Created {
        resource: resource.to_string(),
        revision: revision.to_string(),
    })
}

#[cfg(test)]
#[path = "creates_tests.rs"]
mod tests;
