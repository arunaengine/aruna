//! Keyed plugin creates on the receiver: one row per install and key names the document, the
//! node that creates it and the arguments, so every retry sends the same create there.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::keyspaces::PLUGIN_CREATES_KEYSPACE;
use aruna_core::plugin_installs::PluginInstall;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::rows::{Answer, Ask, Plan, RowsOperation, Turn, done, row, values, StoreError, decode, encode};
use crate::metadata::forward::PinnedCreate;

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
    /// Answer the existing document and its establishing revision.
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
pub fn claim_key((install, key): (&PluginInstall, &str), minted: PinnedCreate) -> RowsOperation<ClaimKey> {
    RowsOperation::transaction(ClaimKey {
        row: create_key(install.install_id, key),
        account: install.account,
        minted: Box::new(minted),
        claimed: None,
    })
}

#[derive(Debug, PartialEq)]
pub struct ClaimKey {
    row: Vec<u8>,
    account: UserId,
    minted: Box<PinnedCreate>,
    claimed: Option<(Box<PinnedCreate>, Claim)>,
}

impl Plan for ClaimKey {
    type Output = (PinnedCreate, Claim);
    type Error = StoreError;

    fn first(&mut self) -> Result<Turn<Self::Output>, StoreError> {
        Ok(Turn::Ask(Ask::Read(vec![(PLUGIN_CREATES_KEYSPACE, self.row.clone())])))
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<Self::Output>, StoreError> {
        if let Some((create, claim)) = self.claimed.take() {
            done(answer)?;
            return Ok(Turn::Finish((*create, claim)));
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
        Ok(Turn::Ask(Ask::Write(vec![write])))
    }
}

/// Closes one attempt of `key` whose outcome is known: it committed, or it was refused without
/// an effect, `rejected` when the refusal concerns its arguments.
pub fn settle_key((install, key): (&PluginInstall, &str), rejected: bool) -> RowsOperation<SettleKey> {
    RowsOperation::transaction(SettleKey {
        row: create_key(install.install_id, key),
        account: install.account,
        rejected,
        written: false,
    })
}

#[derive(Debug, PartialEq)]
pub struct SettleKey {
    row: Vec<u8>,
    account: UserId,
    rejected: bool,
    written: bool,
}

impl Plan for SettleKey {
    type Output = ();
    type Error = StoreError;

    fn first(&mut self) -> Result<Turn<()>, StoreError> {
        Ok(Turn::Ask(Ask::Read(vec![(PLUGIN_CREATES_KEYSPACE, self.row.clone())])))
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<()>, StoreError> {
        if self.written {
            done(answer)?;
            return Ok(Turn::Finish(()));
        }
        let [stored] = values::<1>(answer)?;
        let Some(bytes) = stored else {
            return Ok(Turn::Finish(()));
        };
        let mut stored: KeyRow = decode(&bytes)?;
        if stored.account != self.account {
            return Ok(Turn::Finish(()));
        }
        stored.open = stored.open.saturating_sub(1);
        stored.rejected = self.rejected;
        self.written = true;
        let write = row(PLUGIN_CREATES_KEYSPACE, self.row.clone(), encode(&stored)?);
        Ok(Turn::Ask(Ask::Write(vec![write])))
    }
}

#[cfg(test)]
#[path = "creates_tests.rs"]
mod tests;
