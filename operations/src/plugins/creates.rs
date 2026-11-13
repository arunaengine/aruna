//! Keyed plugin creates on the receiver: one row per install and key names the document, the
//! node that creates it and the arguments, so every retry sends the same create there.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::keyspaces::PLUGIN_CREATES_KEYSPACE;
use aruna_core::plugin_installs::{InstallState, PluginInstall};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::store::{StoreError, Txn, decode, encode, transact};
use crate::driver::DriverContext;
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
pub async fn claim_key(
    ctx: &DriverContext,
    (install, key): (&PluginInstall, &str),
    minted: PinnedCreate,
) -> Result<(PinnedCreate, Claim), StoreError> {
    let row = create_key(install.install_id, key);
    transact(ctx, async |txn: &Txn<'_>| {
        let stored = match txn.read(PLUGIN_CREATES_KEYSPACE, &row).await? {
            Some(bytes) => Some(decode::<KeyRow>(&bytes)?),
            None => None,
        };
        let stored = stored.filter(|stored| stored.account == install.account);
        let (create, claim, open) = match stored {
            None => (minted.clone(), Claim::Fresh, 1),
            Some(stored) if stored.rejected && stored.open == 0 => {
                let corrected = PinnedCreate {
                    group_id: stored.create.group_id,
                    document_id: stored.create.document_id,
                    target: stored.create.target,
                    ..minted.clone()
                };
                (corrected, Claim::Fresh, 1)
            }
            Some(stored) => (stored.create, Claim::Earlier, stored.open.saturating_add(1)),
        };
        let value = KeyRow {
            account: install.account,
            create: create.clone(),
            open,
            rejected: false,
        };
        txn.write(
            PLUGIN_CREATES_KEYSPACE,
            vec![(row.clone(), encode(&value)?)],
        )
        .await?;
        Ok((create, claim))
    })
    .await
}

/// Closes one attempt of `key` whose outcome is known: it committed, or it was refused without
/// an effect, `rejected` when the refusal concerns its arguments.
pub async fn settle_key(
    ctx: &DriverContext,
    (install, key): (&PluginInstall, &str),
    rejected: bool,
) -> Result<(), StoreError> {
    let row = create_key(install.install_id, key);
    transact(ctx, async |txn: &Txn<'_>| {
        let Some(bytes) = txn.read(PLUGIN_CREATES_KEYSPACE, &row).await? else {
            return Ok(());
        };
        let mut stored: KeyRow = decode(&bytes)?;
        if stored.account != install.account {
            return Ok(());
        }
        stored.open = stored.open.saturating_sub(1);
        stored.rejected = rejected;
        txn.write(
            PLUGIN_CREATES_KEYSPACE,
            vec![(row.clone(), encode(&stored)?)],
        )
        .await
    })
    .await
}

/// Deletes the create keys of installs that are being removed or are gone; returns how many.
/// Keys are written only for installs this receiver ran, so an absent one was removed.
pub async fn drop_keys(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
) -> Result<usize, StoreError> {
    let txn = Txn::direct(ctx);
    let kept = |key: &[u8]| {
        let install_id = key.get(..16).and_then(|id| <[u8; 16]>::try_from(id).ok());
        let install_id = install_id.map(Ulid::from_bytes);
        (config.installs.iter()).any(|install| {
            Some(install.install_id) == install_id && install.state != InstallState::Removing
        })
    };
    let keys: Vec<_> = (txn.scan(PLUGIN_CREATES_KEYSPACE, &[]).await?)
        .into_iter()
        .map(|(key, _)| key)
        .filter(|key| !kept(key))
        .collect();
    let count = keys.len();
    txn.delete(PLUGIN_CREATES_KEYSPACE, keys).await?;
    Ok(count)
}

#[cfg(test)]
#[path = "creates_tests.rs"]
mod tests;
