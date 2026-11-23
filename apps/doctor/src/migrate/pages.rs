//! Finds the input pages only removed job families name, so the migration deletes them with
//! their families, as a runtime prune of those families would.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use aruna_core::keyspaces::INPUT_PAGE_KEYSPACE;
use aruna_core::structs::execution::job::family::{
    JobFamilyRecord, JobRecordEnvelope, SubmissionId,
};
use aruna_core::structs::execution::job::pages::PageManifest;
use aruna_core::structs::execution::job::record_keys::submission_prefix;
use aruna_ops_work::jobs::records::pages::{page_key, stored_page};
use aruna_ops_work::jobs::records::prune::pages::{Chain, PageOwners};
use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, OptimisticTxKeyspace, Readable};

use super::compute;
use super::decode_error;
use super::families::exact;
use crate::explorer::ExplorerError;

/// Keys of the input pages that only the records in `removed` name, among the chains every
/// family of their submissions names. `rewrites` are the upgraded records of families that stay.
pub(super) fn owned_pages(
    db: &OptimisticTxDatabase,
    records: &OptimisticTxKeyspace,
    removed: &BTreeSet<Vec<u8>>,
    rewrites: &[(Vec<u8>, Vec<u8>)],
) -> Result<Vec<Vec<u8>>, ExplorerError> {
    let rewrites: BTreeMap<&[u8], &[u8]> = rewrites
        .iter()
        .map(|(key, value)| (key.as_slice(), value.as_slice()))
        .collect();
    let submissions: BTreeSet<[u8; 32]> = removed
        .iter()
        .filter_map(|key| key.get(..32)?.try_into().ok())
        .collect();
    let pages = db.keyspace(INPUT_PAGE_KEYSPACE, KeyspaceCreateOptions::default)?;
    let read = db.read_tx();
    let mut keys = Vec::new();
    for submission in submissions.into_iter().map(SubmissionId) {
        let mut owners = PageOwners::default();
        for entry in read.prefix(records, submission_prefix(submission)) {
            let (key, stored) = entry.into_inner()?;
            let value = rewrites.get(key.as_ref()).copied().unwrap_or(&stored);
            let Some(manifest) = manifest(value) else {
                continue;
            };
            let mut chain = Chain::new(manifest, removed.contains(key.as_ref()));
            while let Some(digest) = chain.pending() {
                let row_key = page_key(&submission, &digest);
                let row = read.get(&pages, &row_key)?;
                let page = row.map(|row| stored_page(&row, &digest)).transpose();
                let page =
                    page.map_err(|error| decode_error(INPUT_PAGE_KEYSPACE, &row_key, error))?;
                chain.read(page.as_ref());
            }
            owners.add(chain);
        }
        keys.extend(
            owners
                .owned()
                .map(|digest| page_key(&submission, digest).to_vec()),
        );
    }
    Ok(keys)
}

/// The page manifest a spec or launch names, read in the current or the alpha.206 layout. The
/// alpha.134 layout predates pages.
fn manifest(value: &[u8]) -> Option<PageManifest> {
    let Some(envelope) = exact::<JobRecordEnvelope>(value) else {
        return compute::input_pages(value);
    };
    match envelope.record {
        JobFamilyRecord::Spec(spec) => spec.input_pages,
        JobFamilyRecord::Launch(launch) => launch.input_pages,
        _ => None,
    }
}
