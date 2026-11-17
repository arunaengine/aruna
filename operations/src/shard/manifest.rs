//! Reads the manifest entries one shard holds here, page by page, and digests them canonically.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::ShardManifestEntry;
use aruna_core::keyspaces::SHARD_MANIFEST_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::storage_entries::{shard_manifest_prefix, sync_revision_key};
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

pub(crate) fn manifest_entry_digest(entries: &[ShardManifestEntry]) -> [u8; 32] {
    let mut encoded_entries: Vec<Vec<u8>> = entries.iter().map(canonical_entry_bytes).collect();
    encoded_entries.sort();

    let mut hasher = blake3::Hasher::new();
    hasher.update(&(encoded_entries.len() as u64).to_be_bytes());
    for encoded in encoded_entries {
        hasher.update(&(encoded.len() as u64).to_be_bytes());
        hasher.update(&encoded);
    }
    *hasher.finalize().as_bytes()
}

fn canonical_entry_bytes(entry: &ShardManifestEntry) -> Vec<u8> {
    let target_key = sync_revision_key(&entry.target);
    let target_key = target_key.as_ref();
    let mut bytes = Vec::with_capacity(4 + target_key.len() + 8 + 16 + 32 + 8);
    bytes.extend_from_slice(&(target_key.len() as u32).to_be_bytes());
    bytes.extend_from_slice(target_key);
    bytes.extend_from_slice(&entry.revision.generation.to_be_bytes());
    bytes.extend_from_slice(&entry.revision.event_id.to_bytes());
    bytes.extend_from_slice(entry.revision.actor.as_bytes());
    bytes.extend_from_slice(&entry.revision.updated_at_ms.to_be_bytes());
    bytes
}

#[cfg(test)]
#[path = "manifest_tests.rs"]
mod tests;
