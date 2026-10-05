//! Grants a sealed copy to another bucket key while it is sent: the new header, the unchanged
//! blocks and a new directory, read by offset for a bao transfer. No block is decrypted.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::rewrite::StoredBytes;
use super::{open_limits, working_set};
use crate::bao_tree::OpenDalReader;
use crate::blob::BlobHandler;
use crate::blob::frames::read_range;
use aruna_core::errors::BlobError;
use aruna_core::structs::storage::blob::BackendLocation;
use aruna_core::structs::storage::encryption::{ReadLease, SealPlan};
use aruna_core::structs::storage::format::{PithosLayout, StoredFormat, StoredLayout};
use bytes::{Bytes, BytesMut};
use iroh_io::AsyncSliceReader;
use pithos_lib::archive::{AsyncArchive, OpenOptions};
use pithos_lib::crypto::PublicKey;
use std::collections::HashMap;
use std::io;
use std::ops::Range;

/// The stored bytes of a copy granted to another key. It keeps the lease, so the source archive
/// stays in use until the transfer ends.
pub(in crate::blob) struct RegrantReader {
    header: Bytes,
    /// Blocks of the old archive; they keep their offsets in the new one.
    blocks: Range<u64>,
    old: OpenDalReader,
    directory: Bytes,
    size: u64,
    _lease: ReadLease,
}

impl AsyncSliceReader for RegrantReader {
    async fn read_at(&mut self, offset: u64, len: usize) -> io::Result<Bytes> {
        let len = len.min(self.size.saturating_sub(offset) as usize);
        self.read_exact_at(offset, len).await
    }

    async fn read_exact_at(&mut self, offset: u64, len: usize) -> io::Result<Bytes> {
        let end = offset
            .checked_add(len as u64)
            .filter(|end| *end <= self.size)
            .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))?;
        let mut out = BytesMut::with_capacity(len);
        if offset < self.blocks.start {
            let stop = end.min(self.blocks.start);
            out.extend_from_slice(&self.header[offset as usize..stop as usize]);
        }
        let (start, stop) = (offset.max(self.blocks.start), end.min(self.blocks.end));
        if start < stop {
            let bytes = self
                .old
                .read_exact_at(start, (stop - start) as usize)
                .await?;
            out.extend_from_slice(&bytes);
        }
        let start = offset.max(self.blocks.end);
        if start < end {
            let from = (start - self.blocks.end) as usize;
            out.extend_from_slice(&self.directory[from..(end - self.blocks.end) as usize]);
        }
        Ok(out.freeze())
    }

    async fn size(&mut self) -> io::Result<u64> {
        Ok(self.size)
    }
}

impl BlobHandler {
    /// A reader over `source` granted only to the key of `plan`, and the location it is sent
    /// with. The key of `lease` opens the old grants; the sent location carries no hashes.
    pub(in crate::blob) async fn regrant_reader(
        &self,
        source: &BackendLocation,
        lease: ReadLease,
        plan: &SealPlan,
    ) -> Result<(RegrantReader, BackendLocation), BlobError> {
        let StoredLayout::Pithos(layout) = &source.format.layout else {
            return Err(BlobError::ReadError("not a Pithos copy".to_string()));
        };
        let keys = self.lease_keys(source, Some(&lease))?;
        // Covers the decoded view, the raw directory and the replacement held at once.
        let _budget = self.reserve_pithos(working_set(source.blob_size)).await?;
        let operator = self.operator_from_location(source)?;
        let path = source.get_storage_path()?;
        let idle = self.transfer_idle_timeout();
        let stored = StoredBytes {
            operator: operator.clone(),
            path: path.clone(),
            idle,
        };
        let options = OpenOptions::default()
            .with_limits(open_limits(source.blob_size))
            .with_access_keys(keys)
            .with_expected_metadata_digest(layout.metadata_digest);
        let archive = AsyncArchive::open(stored, options, Some(layout.stored_size)).await;
        let archive =
            archive.map_err(|error| BlobError::IntegrityCheckFailed(error.to_string()))?;
        let view = archive.view();
        let directory = read_range(&operator, &path, view.directory_range(), idle).await?;
        let recipient = PublicKey::from_raw(plan.public_key).map_err(|error| {
            BlobError::WriteError(format!("invalid bucket public key: {error}"))
        })?;
        let replacement = view.replace_grants(&directory, vec![recipient]);
        let replacement =
            replacement.map_err(|error| BlobError::IntegrityCheckFailed(error.to_string()))?;
        let header = Bytes::copy_from_slice(&replacement.header());
        let blocks = replacement.copy_range();
        if header.len() as u64 != blocks.start || blocks.end > layout.stored_size {
            let message = "the granted archive does not fit the stored copy";
            return Err(BlobError::IntegrityCheckFailed(message.to_string()));
        }
        let old = OpenDalReader::new(&operator, &path, layout.stored_size, idle)
            .await
            .map_err(|error| BlobError::OperatorCreationFailed(error.to_string()))?;
        let granted = PithosLayout {
            stored_size: replacement.archive_len(),
            metadata_digest: replacement.metadata_digest(),
            storage_generation: plan.storage_generation,
        };
        let mut sent = source.clone();
        sent.format = StoredFormat::pithos(granted, plan.key);
        sent.hashes = HashMap::new();
        let reader = RegrantReader {
            header,
            blocks,
            old,
            directory: Bytes::copy_from_slice(replacement.directory()),
            size: replacement.archive_len(),
            _lease: lease,
        };
        Ok((reader, sent))
    }
}
