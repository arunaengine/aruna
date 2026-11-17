//! Reads the manifest entries one shard holds here, page by page, and digests them canonically.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::document::ShardManifestEntry;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::SHARD_MANIFEST_KEYSPACE;
use aruna_core::operation::EffectOperation;
use aruna_core::storage_entries::{shard_manifest_prefix, sync_revision_key};
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::types::{Effects, Key};
use byteview::ByteView;
use smallvec::smallvec;

const MANIFEST_SCAN_PAGE: usize = 512;

/// Every manifest entry of one shard on this node, read with a prefix scan.
#[derive(Debug, PartialEq)]
pub(crate) struct ManifestScanOperation {
    prefix: Key,
    entries: Vec<ShardManifestEntry>,
    reading: bool,
    output: Option<Result<Vec<ShardManifestEntry>, String>>,
}

impl ManifestScanOperation {
    pub(crate) fn new(placement: &PlacementRef) -> Self {
        Self {
            prefix: ByteView::from(shard_manifest_prefix(placement)),
            entries: Vec::new(),
            reading: false,
            output: None,
        }
    }

    fn page(&self, start: Option<Key>) -> Effect {
        Effect::Storage(StorageEffect::Iter {
            key_space: SHARD_MANIFEST_KEYSPACE.to_string(),
            prefix: Some(self.prefix.clone()),
            start: start.map(IterStart::After),
            limit: MANIFEST_SCAN_PAGE,
            txn_id: None,
        })
    }

    fn finish(&mut self, output: Result<Vec<ShardManifestEntry>, String>) -> Effects {
        self.reading = false;
        self.output = Some(output);
        smallvec![]
    }
}

impl EffectOperation for ManifestScanOperation {
    type Output = Vec<ShardManifestEntry>;
    type Error = String;

    fn start(&mut self) -> Effects {
        self.reading = true;
        smallvec![self.page(None)]
    }

    fn step(&mut self, event: Event) -> Effects {
        let (values, next) = match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) if self.reading => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => {
                return self.finish(Err(format!("failed to scan shard manifest: {error}")));
            }
            other => {
                let label = other.label();
                return self.finish(Err(format!(
                    "unexpected shard manifest iter result: {label}"
                )));
            }
        };
        for (_, value) in &values {
            match postcard::from_bytes::<ShardManifestEntry>(value.as_ref()) {
                Ok(entry) => self.entries.push(entry),
                Err(error) => {
                    let error = format!("failed to decode shard manifest entry: {error}");
                    return self.finish(Err(error));
                }
            }
        }
        match next {
            Some(next) => smallvec![self.page(Some(next))],
            None => {
                let entries = std::mem::take(&mut self.entries);
                self.finish(Ok(entries))
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .unwrap_or_else(|| Err("the shard manifest scan did not finish".to_string()))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
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
