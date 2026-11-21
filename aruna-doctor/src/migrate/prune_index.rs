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
/// Families one backfill transaction checks and schedules, so memory stays bounded.
const BATCH_FAMILIES: usize = 256;

/// Schedules each family in `families` without index rows one job retention after `now_ms`, as a
/// fresh record would be. Runs after the main commit, so a removed family gets no row; answers how
/// many families it scheduled. Reads keys only; a family's keys are adjacent in each keyspace.
pub(super) fn missing_index(
    db: &OptimisticTxDatabase,
    families: [&OptimisticTxKeyspace; 3],
    now_ms: u64,
) -> Result<usize, ExplorerError> {
    let index = db.keyspace(FAMILY_PRUNE_KEYSPACE, KeyspaceCreateOptions::default)?;
    let (mut batch, mut scheduled) = (BTreeSet::new(), 0);
    for keyspace in families {
        let mut last = None;
        for entry in db.read_tx().iter(keyspace) {
            let key = entry.key()?;
            let Some(Ok(family)) = key.get(..FAMILY_BYTES).map(<[u8; FAMILY_BYTES]>::try_from)
            else {
                continue;
            };
            if last.replace(family) != Some(family) {
                batch.insert(family);
            }
            if batch.len() == BATCH_FAMILIES {
                scheduled += schedule(db, &index, std::mem::take(&mut batch), now_ms)?;
            }
        }
    }
    Ok(scheduled + schedule(db, &index, batch, now_ms)?)
}

/// Writes the index rows of each family in `batch` that has none yet, in one transaction, so a
/// family another batch or keyspace already scheduled is skipped.
fn schedule(
    db: &OptimisticTxDatabase,
    index: &OptimisticTxKeyspace,
    batch: BTreeSet<[u8; FAMILY_BYTES]>,
    now_ms: u64,
) -> Result<usize, ExplorerError> {
    let mut txn = db.write_tx()?;
    let mut scheduled = 0;
    for bytes in batch {
        let family = family_of(bytes);
        if txn.get(index, prune_slot_key(&family))?.is_some() {
            continue;
        }
        for (_, key, value) in prune_rows(&family, now_ms) {
            txn.insert(index.clone(), key.to_vec(), value.to_vec());
        }
        scheduled += 1;
    }
    if scheduled == 0 {
        return Ok(0);
    }
    txn.commit()?.map_err(|_| {
        ExplorerError::Decode("the prune index backfill conflicted with a running node".into())
    })?;
    Ok(scheduled)
}

pub(super) fn family_of(bytes: [u8; FAMILY_BYTES]) -> JobFamilyId {
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
