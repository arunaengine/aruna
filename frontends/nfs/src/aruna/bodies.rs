//! Prepared S3 writes of the NFS backend: the body of a publication or committed move read
//! from the effective file image in bounded chunks, and the routing and quota of the write.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::task::Poll;

use aruna_core::bucket_read::GetBucketOperation;
use aruna_core::stream::{BackendStream, ByteStream};
use aruna_core::structs::storage::vfs::{NfsMountBinding, NfsMountTarget};
use aruna_ops_data::s3::object::put::{PutObjectConfig, PutObjectInput};
use aruna_ops_data::vfs::io::{ReadInput, VfsReadOperation};
use aruna_ops_data::vfs::{CHUNK_BYTES, VfsError};
use aruna_ops_identity::document_repository::GetConfigOperation;
use aruna_ops_identity::groups::routing::{BucketRules, SnapshotOperation};
use aruna_runner::driver::{DriverContext, drive, node_routing};
use bytes::Bytes;
use tokio::sync::mpsc;

use super::ArunaNfsBackend;
use crate::types::NfsStat4;

/// Chunks a body producer reads ahead of the writer that consumes them.
const READ_AHEAD: usize = 2;

/// Reads one file image chunk by chunk; each chunk is admitted and read on its own.
struct ChunkReader {
    context: Arc<DriverContext>,
    read: ReadInput,
    size: u64,
}

impl ChunkReader {
    async fn run(mut self, sender: mpsc::Sender<Result<Bytes, VfsError>>) {
        let mut offset = 0;
        while offset < self.size {
            let length = (self.size - offset).min(CHUNK_BYTES);
            self.read.offset = offset;
            self.read.length = u32::try_from(length).unwrap_or(u32::MAX);
            let read = VfsReadOperation::new(self.read.clone());
            let chunk = match drive(read, &self.context).await {
                Ok(chunk) => chunk,
                Err(error) => {
                    let _ = sender.send(Err(error)).await;
                    return;
                }
            };
            let short = (chunk.data.len() as u64) < length;
            offset += chunk.data.len() as u64;
            if chunk.data.is_empty() || sender.send(Ok(chunk.data.into())).await.is_err() {
                return;
            }
            if short || chunk.eof {
                return;
            }
        }
    }
}

/// The reader before the writer first polls, then the chunks it sends.
enum Feed {
    Waiting(Option<Box<ChunkReader>>),
    Reading(mpsc::Receiver<Result<Bytes, VfsError>>),
}

/// The bytes of `read`'s file image up to `size`, read in chunks of at most [`CHUNK_BYTES`]
/// only once the write consumes them, so no image is ever held in memory as a whole.
pub(super) fn image_body(context: Arc<DriverContext>, read: ReadInput, size: u64) -> ByteStream {
    let reader = Box::new(ChunkReader {
        context,
        read,
        size,
    });
    let mut feed = Feed::Waiting(Some(reader));
    let stream = futures_util::stream::poll_fn(move |task| {
        if let Feed::Waiting(reader) = &mut feed {
            let (sender, receiver) = mpsc::channel(READ_AHEAD);
            if let Some(reader) = reader.take() {
                tokio::spawn(reader.run(sender));
            }
            feed = Feed::Reading(receiver);
        }
        match &mut feed {
            Feed::Reading(receiver) => receiver.poll_recv(task),
            Feed::Waiting(_) => Poll::Ready(None),
        }
    });
    BackendStream::new(stream)
}

/// The bucket of a bucket or folder binding.
pub(super) fn bucket_name(binding: &NfsMountBinding) -> Result<&str, NfsStat4> {
    match &binding.target {
        NfsMountTarget::Bucket { bucket, .. } | NfsMountTarget::Folder { bucket, .. } => {
            Ok(bucket.as_str())
        }
        NfsMountTarget::JobInputView { .. } => Err(NfsStat4::ReadOnly),
    }
}

impl ArunaNfsBackend {
    /// An ordinary write of `body` to `key` as the binding's principal, with the realm quota
    /// ceiling and the bucket's routing; the operation that runs it admits it again.
    pub(super) async fn put_config(
        &self,
        binding: &NfsMountBinding,
        key: &str,
        body: ByteStream,
        (size, exists): (u64, bool),
    ) -> Result<PutObjectConfig, NfsStat4> {
        let bucket = bucket_name(binding)?;
        let info = drive(GetBucketOperation::new(bucket.to_string()), &self.context)
            .await
            .map_err(|_| NfsStat4::Stale)?;
        let realm = drive(GetConfigOperation::new(self.realm_id), &self.context)
            .await
            .map_err(|_| NfsStat4::Delay)?;
        let rules = BucketRules::of(&info);
        let routing = SnapshotOperation::new(&node_routing(&self.context), info.group_id, rules);
        let routing = drive(routing, &self.context)
            .await
            .map_err(|_| NfsStat4::Delay)?;
        Ok(PutObjectConfig {
            user_id: binding.owner,
            group_id: info.group_id,
            realm_id: self.realm_id,
            node_id: self.node_id,
            request: PutObjectInput {
                bucket: bucket.to_string(),
                key: key.to_string(),
                content_length: Some(size),
                body: Some(body),
            },
            expected_checksums: Vec::new(),
            checksum_type: None,
            exists,
            version_source: None,
            preassigned_version_id: None,
            quota_ceiling: realm.quota.effective_group_ceiling(&info.group_id),
            routing,
        })
    }

    /// The zero-length write of a directory marker `d/`.
    pub(super) async fn marker_put(
        &self,
        binding: &NfsMountBinding,
        directory: &str,
    ) -> Result<PutObjectConfig, NfsStat4> {
        let marker = aruna_ops_data::vfs::path::marker_key(directory);
        self.put_config(binding, &marker, BackendStream::empty(), (0, false))
            .await
    }
}
