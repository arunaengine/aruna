//! Schedules every job family the family prune index does not name, as alpha.159 wrote no
//! index row; without one the prune pass never reaches the family.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::keyspaces::FAMILY_PRUNE_KEYSPACE;
use aruna_core::structs::execution::job::family::{JobFamilyId, SubmissionId};
use aruna_core::structs::execution::job::record_keys::{prune_rows, prune_slot_key};
use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, OptimisticTxKeyspace, Readable};

use crate::explorer::ExplorerError;

/// Bytes of the family every record, pending and conflict key starts with.
const FAMILY_BYTES: usize = 64;

/// Schedules each family in `families` without index rows one job retention after `now_ms`, as a
/// fresh record would be. Runs after the main commit, so a removed family gets no row; answers how
/// many families it scheduled.
pub(super) fn missing_index(
    db: &OptimisticTxDatabase,
    families: [&OptimisticTxKeyspace; 3],
    now_ms: u64,
) -> Result<usize, ExplorerError> {
    let index = db.keyspace(FAMILY_PRUNE_KEYSPACE, KeyspaceCreateOptions::default)?;
    let mut seen = BTreeSet::new();
    let read = db.read_tx();
    for keyspace in families {
        for entry in read.iter(keyspace) {
            let key = entry.into_inner()?.0;
            if let Some(Ok(family)) = key.get(..FAMILY_BYTES).map(<[u8; FAMILY_BYTES]>::try_from) {
                seen.insert(family);
            }
        }
    }
    let mut missing = Vec::new();
    for bytes in seen {
        let family = family_of(bytes);
        if read
            .get(&index, prune_slot_key(&family).to_vec())?
            .is_none()
        {
            missing.push(family);
        }
    }
    if missing.is_empty() {
        return Ok(0);
    }
    let mut txn = db.write_tx()?;
    for family in &missing {
        for (_, key, value) in prune_rows(family, now_ms) {
            txn.insert(index.clone(), key.to_vec(), value.to_vec());
        }
    }
    txn.commit()?.map_err(|_| {
        ExplorerError::Decode("the prune index backfill conflicted with a running node".into())
    })?;
    Ok(missing.len())
}

fn family_of(bytes: [u8; FAMILY_BYTES]) -> JobFamilyId {
    let (mut submission, mut digest) = ([0u8; 32], [0u8; 32]);
    submission.copy_from_slice(&bytes[..32]);
    digest.copy_from_slice(&bytes[32..]);
    JobFamilyId {
        submission_id: SubmissionId(submission),
        request_digest: digest,
    }
}

#[cfg(test)]
#[path = "prune_index_tests.rs"]
mod tests;
