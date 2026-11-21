//! Reads the manifest entries one shard holds here, page by page, and digests them canonically.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::ShardManifestEntry;
use aruna_core::keyspaces::SHARD_MANIFEST_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::storage_entries::shard_manifest_prefix;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::types::Key;
use byteview::ByteView;

const MANIFEST_SCAN_PAGE: usize = 512;

/// Why the manifest entries of a shard could not be read.
#[derive(Debug, PartialEq, thiserror::Error)]
pub(crate) enum ManifestScanError {
    #[error("failed to scan shard manifest: {0}")]
    Io(#[from] IoError),
    #[error("failed to decode shard manifest entry: {0}")]
    Decode(String),
}

/// Every manifest entry of one shard on this node, read with a prefix scan.
#[derive(Debug, PartialEq)]
pub(crate) struct ManifestScanOperation {
    prefix: Key,
    entries: Vec<ShardManifestEntry>,
}

impl ManifestScanOperation {
    pub(crate) fn new(placement: &PlacementRef) -> Self {
        Self {
            prefix: ByteView::from(shard_manifest_prefix(placement)),
            entries: Vec::new(),
        }
    }

    fn page(&self, after: Option<Key>) -> Next<Vec<ShardManifestEntry>> {
        Next::Call(IoCall::Page {
            space: SHARD_MANIFEST_KEYSPACE.to_string(),
            prefix: self.prefix.clone(),
            after,
            limit: MANIFEST_SCAN_PAGE,
        })
    }
}

impl Operation for ManifestScanOperation {
    type Output = Vec<ShardManifestEntry>;
    type Error = ManifestScanError;

    fn first(&mut self) -> Result<Next<Self::Output>, Self::Error> {
        Ok(self.page(None))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, Self::Error> {
        let IoResult::Page { rows, next } = result else {
            let unexpected = IoError::Unexpected {
                expected: "a shard manifest page",
                got: "another result",
            };
            return Err(unexpected.into());
        };
        for (_, value) in &rows {
            let entry = postcard::from_bytes::<ShardManifestEntry>(value.as_ref())
                .map_err(|error| ManifestScanError::Decode(error.to_string()))?;
            self.entries.push(entry);
        }
        Ok(match next {
            Some(next) => self.page(Some(next)),
            None => Next::Done(std::mem::take(&mut self.entries)),
        })
    }
}

#[cfg(test)]
#[path = "manifest_tests.rs"]
mod tests;
