//! Grants a sealed copy to another bucket key while it is sent: the new header, the unchanged
//! blocks and a new directory, read by offset for a bao transfer. No block is decrypted.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::rewrite::StoredBytes;
use super::{Share, open_limits, working_set};
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
    _budget: Share,
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
            _budget,
        };
        Ok((reader, sent))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::compute::{SecretBytes, SharedSecret};
    use aruna_core::effects::BlobEffect;
    use aruna_core::events::BlobEvent;
    use aruna_core::stream::BackendStream;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::blob::{ArchiveKey, Backend, BackendConfig, ResolvedBackend};
    use aruna_core::structs::storage::encryption::{
        BlockCipher, BlockKeys, BucketKeyRef, public_key_of,
    };
    use bao_tree::ByteRanges;
    use bao_tree::io::fsm::{CreateOutboard, encode_ranges_validated};
    use bao_tree::io::outboard::PreOrderOutboard;
    use bao_tree::io::round_up_to_chunks;
    use iroh_io::AsyncStreamWriter;
    use std::time::Duration;
    use tokio::sync::oneshot;
    use ulid::Ulid;

    struct StalledWriter {
        started: Option<oneshot::Sender<()>>,
        release: Option<oneshot::Receiver<()>>,
    }

    impl AsyncStreamWriter for StalledWriter {
        async fn write(&mut self, _data: &[u8]) -> io::Result<()> {
            if let Some(started) = self.started.take() {
                started.send(()).unwrap();
                self.release.take().unwrap().await.unwrap();
            }
            Ok(())
        }

        async fn sync(&mut self) -> io::Result<()> {
            Ok(())
        }

        async fn write_bytes(&mut self, data: Bytes) -> io::Result<()> {
            self.write(&data).await
        }
    }

    #[tokio::test]
    async fn regrant_keeps_budget() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("blobs");
        std::fs::create_dir(&root).unwrap();
        let storage = aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let net = aruna_net::NetHandle::new(
            aruna_net::NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                discovery_method: aruna_net::DiscoveryMethod::None,
                relay_method: aruna_net::RelayMethod::None,
                ..Default::default()
            },
            storage.clone(),
        )
        .await
        .unwrap();
        let handle = BlobHandler::new(
            BackendConfig {
                backend_type: Backend::FileSystem,
                root: root.to_str().unwrap().to_string(),
                service_config: HashMap::new(),
                bucket_prefix: Some("test-".to_string()),
                max_bucket_size: None,
                multipart_bucket: Some("parts".to_string()),
                timeouts: Default::default(),
            },
            storage,
            net.clone(),
        )
        .await
        .unwrap();
        let handler = &handle.handler;
        let private = SharedSecret::new(SecretBytes::new(vec![5; 32]));
        let key = BucketKeyRef::new(Ulid::generate(), 1);
        let plan = SealPlan {
            key,
            public_key: public_key_of(private.bytes()).unwrap(),
            cipher: BlockCipher::default(),
            block_keys: BlockKeys::default(),
            storage_generation: 1,
        };
        let data = Bytes::from_static(b"retained while the granted archive transfer waits");
        let stream = BackendStream::new(futures::stream::iter([Ok(data)]));
        let written = handler
            .write_blob(
                "bucket",
                "object",
                ResolvedBackend::node_default().with_encryption(Some(plan)),
                UserId::new(Ulid::generate(), RealmId::from_bytes([1; 32])),
                stream,
            )
            .await;
        let BlobEvent::WriteFinished { location } = written else {
            panic!("archive write failed: {written:?}");
        };
        let BlobEvent::KeyPrepared { ticket } = handler.unlock_effect(BlobEffect::PrepareKey {
            key,
            public_key: plan.public_key,
            private_key: private,
            duration: None,
            max: None,
        }) else {
            panic!("key preparation failed");
        };
        assert!(matches!(
            handler.unlock_effect(BlobEffect::ActivateKey { ticket }),
            BlobEvent::KeyActivated { .. }
        ));
        let BlobEvent::ReadAdmitted { lease } =
            handler.admit_read(key, ArchiveKey::of(&location)).await
        else {
            panic!("read admission failed");
        };
        let target = SecretBytes::new(vec![6; 32]);
        let plan = SealPlan {
            key: BucketKeyRef::new(Ulid::generate(), 1),
            public_key: public_key_of(&target).unwrap(),
            ..plan
        };
        let budget = handler.pithos_budget.clone();
        let capacity = budget.available_permits();
        let charged = working_set(location.blob_size).div_ceil(1 << 20) as usize;
        let (mut reader, sent) = handler
            .regrant_reader(&location, lease, &plan)
            .await
            .unwrap();
        assert_eq!(budget.available_permits(), capacity - charged);
        let mut outboard =
            PreOrderOutboard::<BytesMut>::create(&mut reader, crate::blob::BAO_BLOCK_SIZE)
                .await
                .unwrap();
        assert_eq!(budget.available_permits(), capacity - charged);
        let (started, waiting) = oneshot::channel();
        let (release, resume) = oneshot::channel();
        let mut writer = StalledWriter {
            started: Some(started),
            release: Some(resume),
        };
        let ranges = round_up_to_chunks(&ByteRanges::from(0..sent.stored_size()));
        let transfer = tokio::spawn(async move {
            encode_ranges_validated(reader, &mut outboard, &ranges, &mut writer)
                .await
                .unwrap();
        });
        tokio::time::timeout(Duration::from_secs(60), waiting)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(budget.available_permits(), capacity - charged);
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(60), transfer)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(budget.available_permits(), capacity);
        net.shutdown().await;
    }
}
