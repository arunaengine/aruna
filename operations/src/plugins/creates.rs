//! Keyed plugin creates on the receiver: one row per install and key names the document, the
//! node that creates it and the arguments, so every retry sends the same create there.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::keyspaces::PLUGIN_CREATES_KEYSPACE;
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

fn create_key(install_id: Ulid, key: &str) -> Vec<u8> {
    [&install_id.to_bytes()[..], key.as_bytes()].concat()
}

/// Reads the row of `key` and writes `minted` when there is none, in one transaction, so two
/// concurrent calls with one key adopt the same document before either sends.
pub async fn claim_key(
    ctx: &DriverContext,
    (install_id, key): (Ulid, &str),
    minted: PinnedCreate,
) -> Result<(PinnedCreate, Claim), StoreError> {
    let row = create_key(install_id, key);
    transact(ctx, async |txn: &Txn<'_>| {
        if let Some(bytes) = txn.read(PLUGIN_CREATES_KEYSPACE, &row).await? {
            return Ok((decode(&bytes)?, Claim::Earlier));
        }
        let value = encode(&minted)?;
        txn.write(PLUGIN_CREATES_KEYSPACE, vec![(row.clone(), value)])
            .await?;
        Ok((minted.clone(), Claim::Fresh))
    })
    .await
}

/// Deletes the create keys of an install that is being removed; returns how many.
pub async fn drop_keys(ctx: &DriverContext, install_id: Ulid) -> Result<usize, StoreError> {
    let txn = Txn::direct(ctx);
    let keys: Vec<_> = (txn
        .scan(PLUGIN_CREATES_KEYSPACE, &install_id.to_bytes())
        .await?)
        .into_iter()
        .map(|(key, _)| key)
        .collect();
    let count = keys.len();
    txn.delete(PLUGIN_CREATES_KEYSPACE, keys).await?;
    Ok(count)
}

#[cfg(test)]
#[path = "creates_tests.rs"]
mod tests;
