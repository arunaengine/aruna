//! Keeps the source roots of stored references once per node: a stored binding names its root
//! by digest, and readers that need the root's settings put them back from the record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::StorageEffect;
use aruna_core::errors::ConversionError;
use aruna_core::events::Event;
use aruna_core::keyspaces::ROOT_RECORD_KEYSPACE;
use aruna_core::structs::storage::blob::BlobVersion;
use aruna_core::structs::storage::distribution::source_root::{RootEntry, SourceRoot};
use aruna_core::types::{Key, KeySpace, TxnId, Value};
use byteview::ByteView;

use crate::driver::DriverContext;
use crate::storage_read::{StorageReadError, parse_storage_read};

/// The record row of a root; a blind write, as every version of the root writes the same row.
pub fn root_write(root: &SourceRoot) -> Result<(KeySpace, Key, Value), ConversionError> {
    let key = ByteView::from(root.digest()?.to_vec());
    let value = ByteView::from(root.to_bytes()?);
    Ok((ROOT_RECORD_KEYSPACE.to_string(), key, value))
}

pub fn read_root(digest: [u8; 32], txn_id: Option<TxnId>) -> StorageEffect {
    StorageEffect::Read {
        key_space: ROOT_RECORD_KEYSPACE.to_string(),
        key: ByteView::from(digest.to_vec()),
        txn_id,
    }
}

pub fn parse_root(event: Event) -> Result<Option<SourceRoot>, StorageReadError> {
    parse_storage_read(event, SourceRoot::from_bytes)
}

/// The version as a node stores it: an external source binding names its root by digest, and
/// the root comes back for the caller to write in the same transaction.
pub fn compact_version(
    mut version: BlobVersion,
) -> Result<(BlobVersion, Option<SourceRoot>), ConversionError> {
    let Some(source) = version.source_binding_mut() else {
        return Ok((version, None));
    };
    let Some((compact, root)) = RootEntry::compact(source)? else {
        return Ok((version, None));
    };
    *source = compact;
    Ok((version, Some(root)))
}

/// The root digest a stored version names, when its settings must be put back before use.
pub fn named_root(version: &BlobVersion) -> Option<[u8; 32]> {
    version.source_binding()?.descriptor.root
}

/// The version with the settings of `root` back in its binding.
pub fn hydrated(mut version: BlobVersion, root: &SourceRoot) -> Result<BlobVersion, String> {
    let Some(source) = version.source_binding_mut() else {
        return Ok(version);
    };
    let full = RootEntry::hydrate(source, root).map_err(|error| error.to_string())?;
    *source = full.ok_or_else(|| "the source root record names another root".to_string())?;
    Ok(version)
}

/// Puts the root's settings back into a version read with a compact binding.
pub async fn hydrate_version(
    context: &DriverContext,
    version: BlobVersion,
) -> Result<BlobVersion, String> {
    let Some(digest) = named_root(&version) else {
        return Ok(version);
    };
    let read = read_root(digest, None);
    let event = context.storage_handle.send_storage_effect(read).await;
    let root = parse_root(event).map_err(|error| error.to_string())?;
    let root = root.ok_or_else(|| "the source root record is missing".to_string())?;
    hydrated(version, &root)
}
