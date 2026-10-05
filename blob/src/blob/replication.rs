//! Replicates blobs between nodes over verified bao streams and quarantines corrupt copies.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::BlobHandler;
use super::backend::rebuild_backend_path;
use super::control_plane::{
    parse_replication_init, read_replication_message, send_replication_message, validate_init_ack,
};
use super::frames::SliceReader;
use crate::bao_tree::{BaoReadWriter, OpenDalWriter, RecvStreamWrapper, SendStreamWrapper};
use crate::error::BlobLibError;
use crate::messages::{MessageType, ReplicationMessage};
use aruna_core::effects::StorageEffect;
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::BLOB_QUARANTINE_KEYSPACE;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::execution::source_access::ResolvedSourceAccess;
use aruna_core::structs::storage::blob::{
    BackendLocation, BackendRef, BlobQuarantineRecord, ResolvedBackend,
};
use aruna_core::structs::storage::encryption::{ReadLease, SealPlan};
use aruna_core::structs::storage::format::{Compression, StoredFormat, StoredLayout};
use aruna_core::time::unix_timestamp_millis;
use bao_tree::io::fsm::{CreateOutboard, decode_ranges, encode_ranges_validated};
use bao_tree::io::outboard::PreOrderOutboard;
use bao_tree::io::round_up_to_chunks;
use bao_tree::{BaoTree, ByteRanges};
use bytes::BytesMut;
use std::collections::HashMap;
use tracing::{debug, warn};
use ulid::Ulid;

use super::BAO_BLOCK_SIZE;

impl BlobHandler {
    /// Persists durable evidence that a stored copy failed verification (§8.2)
    /// before the error returns, so a corrupt copy is tracked, not just logged.
    /// Best-effort: a failed write is logged, never masking the original error.
    pub(super) async fn quarantine_corrupt_blob(
        &self,
        blake3: [u8; 32],
        backend: &BackendRef,
        reason: &str,
    ) {
        let record = BlobQuarantineRecord::new(
            blake3,
            backend.clone(),
            reason.to_string(),
            unix_timestamp_millis(),
        );
        let value = match record.to_bytes() {
            Ok(value) => value,
            Err(error) => {
                warn!(%error, "failed to encode blob quarantine record");
                return;
            }
        };
        let event = self
            .storage
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_QUARANTINE_KEYSPACE.to_string(),
                key: record.key().into(),
                value: value.into(),
                txn_id: None,
            })
            .await;
        if let Event::Storage(StorageEvent::Error { error }) = event {
            warn!(%error, reason, "failed to persist blob quarantine record");
        } else {
            warn!(reason, "Quarantined a blob that failed verification");
        }
    }

    pub async fn serve_read(
        &self,
        stream_id: Ulid,
        location: BackendLocation,
        expected_blake3: [u8; 32],
    ) -> BlobEvent {
        if location.get_blake3() != Some(expected_blake3.as_slice()) {
            return BlobEvent::Error(BlobError::IntegrityCheckFailed(
                "bao read location hash mismatch".to_string(),
            ));
        }
        // Framed copies are decoded, so the peer always receives original bytes.
        let reader = match self.slice_reader(&location).await {
            Ok(reader) => reader,
            Err(error) => return BlobEvent::Error(error),
        };
        self.serve_from(stream_id, location, expected_blake3, reader)
            .await
    }

    /// Serves the plaintext of a copy of an encrypting bucket to an authorized reader. The lease
    /// keeps the key and the archive in use until the transfer ends.
    pub async fn serve_sealed_read(
        &self,
        stream_id: Ulid,
        location: BackendLocation,
        expected_blake3: [u8; 32],
        lease: ReadLease,
    ) -> BlobEvent {
        if location.get_blake3() != Some(expected_blake3.as_slice()) {
            return BlobEvent::Error(BlobError::IntegrityCheckFailed(
                "bao read location hash mismatch".to_string(),
            ));
        }
        // A plain copy of an encrypting bucket keeps its bucket lease until the transfer ends.
        let (reader, _lease) = match location.format.layout {
            StoredLayout::Pithos(_) => match self.sealed_reader(&location, lease).await {
                Ok(reader) => (SliceReader::Sealed(reader), None),
                Err(error) => return BlobEvent::Error(error),
            },
            _ => match self.slice_reader(&location).await {
                Ok(reader) => (reader, Some(lease)),
                Err(error) => return BlobEvent::Error(error),
            },
        };
        self.serve_from(stream_id, location, expected_blake3, reader)
            .await
    }

    async fn serve_from(
        &self,
        stream_id: Ulid,
        location: BackendLocation,
        expected_blake3: [u8; 32],
        mut reader: SliceReader,
    ) -> BlobEvent {
        let mut outboard =
            match PreOrderOutboard::<BytesMut>::create(&mut reader, BAO_BLOCK_SIZE).await {
                Ok(outboard) if outboard.root.as_bytes() == &expected_blake3 => outboard,
                Ok(_) => {
                    self.quarantine_corrupt_blob(
                        expected_blake3,
                        &location.backend,
                        "bao read source hash mismatch",
                    )
                    .await;
                    return BlobEvent::Error(BlobError::IntegrityCheckFailed(
                        "bao read source hash mismatch".to_string(),
                    ));
                }
                Err(error) => {
                    return BlobEvent::Error(BlobError::OutboardCreationFailed(error.to_string()));
                }
            };
        self.stream_encoding(stream_id, reader, &mut outboard, location.blob_size)
            .await
    }
    /// Streams one bao encoding to the peer and tears the connection down,
    /// whatever the encoder answered.
    async fn stream_encoding<R: iroh_io::AsyncSliceReader>(
        &self,
        stream_id: Ulid,
        reader: R,
        outboard: &mut PreOrderOutboard<BytesMut>,
        size: u64,
    ) -> BlobEvent {
        let stream = match self.connection_handle(stream_id).await {
            Ok(stream) => stream,
            Err(event) => return event,
        };
        let mut stream = stream.lock().await;
        let ranges = round_up_to_chunks(&ByteRanges::from(0..size));
        let mut sender = SendStreamWrapper::new(&mut stream.0, self.transfer_idle_timeout());
        let result = encode_ranges_validated(reader, outboard, &ranges, &mut sender).await;
        _ = stream.0.finish();
        _ = stream.1.stop(0u32.into());
        drop(stream);
        self.connections.lock().await.remove(&stream_id);

        match result {
            Ok(()) => BlobEvent::ReadServed { stream_id },
            Err(error) => BlobEvent::Error(BlobError::ReplicationFailed(error.to_string())),
        }
    }

    /// Serves one version this node never materialized: the bytes come from the
    /// owner's own file. The file is refused unless it still carries the
    /// observed fingerprint and hashes to the requester's identity.
    pub async fn serve_source_read(
        &self,
        stream_id: Ulid,
        access: ResolvedSourceAccess,
        size: u64,
        expected_blake3: [u8; 32],
        fingerprint: String,
    ) -> BlobEvent {
        let (reader, mut outboard, file, current) =
            match verified_source(&access, size, expected_blake3, &fingerprint).await {
                Ok(verified) => verified,
                Err(event) => return event,
            };
        let served = self
            .stream_encoding(stream_id, reader, &mut outboard, size)
            .await;
        // The encoder validates every chunk against the outboard, so a file
        // rewritten mid-stream already fails; this only names the cause.
        if crate::fs_source::current_fingerprint(&file).await != Some(current) {
            return BlobEvent::Error(BlobError::IntegrityCheckFailed(
                "the offered file changed while it was served".to_string(),
            ));
        }
        served
    }

    pub async fn receive_read(
        &self,
        stream_id: Ulid,
        size: u64,
        expected_blake3: [u8; 32],
    ) -> BlobEvent {
        let Some(connection) = self.connections.lock().await.remove(&stream_id) else {
            return BlobEvent::Error(BlobError::ReplicationRejected(
                "Stream not available".to_string(),
            ));
        };
        let (writer, reader) = tokio::io::duplex(64 * 1024);
        let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();
        let transfer_timeout = self.transfer_idle_timeout();

        tokio::spawn(async move {
            let result: Result<(), BlobError> = async {
                let mut stream = connection.stream.lock().await;
                let receiver = RecvStreamWrapper::new(&mut stream.1, transfer_timeout);
                let mut writer = BaoReadWriter::new(writer);
                let mut outboard = PreOrderOutboard {
                    tree: BaoTree::new(size, BAO_BLOCK_SIZE),
                    root: expected_blake3.into(),
                    data: BytesMut::new(),
                };
                let ranges = round_up_to_chunks(&ByteRanges::from(0..size));
                decode_ranges(receiver, ranges, &mut writer, &mut outboard)
                    .await
                    .map_err(|error| BlobError::ReplicationFailed(error.to_string()))?;
                writer
                    .finish(size, expected_blake3)
                    .map_err(|error| BlobError::IntegrityCheckFailed(error.to_string()))?;
                _ = stream.0.finish();
                _ = stream.1.stop(0u32.into());
                Ok(())
            }
            .await;
            _ = completion_tx.send(result);
        });

        let blob = BackendStream::new(tokio_util::io::ReaderStream::new(reader)).on_success_async(
            move || async move {
                completion_rx
                    .await
                    .map_err(|_| StreamError(Box::new(BlobError::ChannelClosed)))?
                    .map_err(|error| StreamError(Box::new(error)))
            },
        );
        BlobEvent::ReadFinished {
            blob,
            stream_size: size,
        }
    }

    pub async fn replicate_blob(
        &self,
        replication_id: Ulid,
        stream_id: Ulid,
        location: BackendLocation,
        keep_alive: bool,
    ) -> BlobEvent {
        let reader = match self.slice_reader(&location).await {
            Ok(reader) => reader,
            Err(err) => return BlobEvent::Error(err),
        };
        let ids = (replication_id, stream_id);
        let size = location.blob_size;
        match self
            .send_replica(ids, location.clone(), reader, size, keep_alive)
            .await
        {
            Ok(()) => BlobEvent::ReplicationFinished { location },
            Err(event) => event,
        }
    }

    /// Sends a copy of an encrypting bucket under `lease`. With `regrant` a sealed copy is granted
    /// to that key and its stored bytes are sent; otherwise its plaintext is sent.
    pub async fn replicate_leased(
        &self,
        (replication_id, stream_id): (Ulid, Ulid),
        location: BackendLocation,
        lease: ReadLease,
        regrant: Option<SealPlan>,
    ) -> BlobEvent {
        let ids = (replication_id, stream_id);
        let sealed = matches!(location.format.layout, StoredLayout::Pithos(_));
        let sent = match (regrant, sealed) {
            (Some(plan), true) => match self.regrant_reader(&location, lease, &plan).await {
                Ok((reader, sent)) => {
                    let size = sent.stored_size();
                    self.send_replica(ids, sent, reader, size, true).await
                }
                Err(error) => Err(BlobEvent::Error(error)),
            },
            (Some(_), false) => {
                let message = "only a sealed copy is granted to another key";
                Err(BlobEvent::Error(BlobError::ReadError(message.to_string())))
            }
            (None, true) => match self.sealed_reader(&location, lease).await {
                Ok(reader) => {
                    let size = location.blob_size;
                    let plain = plain_sent(&location);
                    let reader = SliceReader::Sealed(reader);
                    self.send_replica(ids, plain, reader, size, true).await
                }
                Err(error) => Err(BlobEvent::Error(error)),
            },
            // A plain copy of an encrypting bucket keeps its bucket lease until the transfer ends.
            (None, false) => match self.slice_reader(&location).await {
                Ok(reader) => {
                    let _lease = lease;
                    let size = location.blob_size;
                    let plain = plain_sent(&location);
                    self.send_replica(ids, plain, reader, size, true).await
                }
                Err(error) => Err(BlobEvent::Error(error)),
            },
        };
        match sent {
            Ok(()) => BlobEvent::ReplicationFinished { location },
            Err(event) => event,
        }
    }

    /// Announces `sent` with the bao root of `size` bytes of `reader`, then streams them.
    async fn send_replica<R: iroh_io::AsyncSliceReader>(
        &self,
        (replication_id, stream_id): (Ulid, Ulid),
        sent: BackendLocation,
        mut reader: R,
        size: u64,
        keep_alive: bool,
    ) -> Result<(), BlobEvent> {
        let mut outboard = PreOrderOutboard::<BytesMut>::create(&mut reader, BAO_BLOCK_SIZE)
            .await
            .map_err(|err| BlobEvent::Error(BlobError::OutboardCreationFailed(err.to_string())))?;

        let stream = self.connection_handle(stream_id).await?;
        let mut stream = stream.lock().await;

        let replication_init = ReplicationMessage {
            id: replication_id,
            msg_type: MessageType::BaoTreeInfo {
                location: sent,
                root: outboard.root,
            },
        };
        let sx = &mut stream.0;
        send_replication_message(
            sx,
            replication_init,
            self.io_timeout(),
            "sending replication tree info",
        )
        .await?;

        let rx = &mut stream.1;
        let msg = read_replication_message(
            rx,
            self.io_timeout(),
            "waiting for replication tree info acknowledgement",
        )
        .await?;
        validate_init_ack(msg, replication_id).map_err(BlobEvent::Error)?;

        let sx = &mut stream.0;
        let mut sx_wrapper = SendStreamWrapper::new(sx, self.transfer_idle_timeout());
        let ranges = ByteRanges::from(0..size);
        let ranges = round_up_to_chunks(&ranges);
        debug!("Chunk Ranges: {:#?}", ranges.boundaries());

        if let Err(err) =
            encode_ranges_validated(reader, &mut outboard, &ranges, &mut sx_wrapper).await
        {
            return Err(BlobEvent::Error(BlobError::ReplicationFailed(
                err.to_string(),
            )));
        }

        if !keep_alive {
            _ = stream.0.finish();
            _ = stream.1.stop(0u32.into());
        }
        drop(stream);
        if !keep_alive {
            self.connections.lock().await.remove(&stream_id);
        }
        Ok(())
    }

    pub async fn handle_incoming_replication(
        &self,
        replication_id: Option<Ulid>,
        stream_id: Ulid,
        resolved: ResolvedBackend,
        keep_alive: bool,
    ) -> BlobEvent {
        let (_replication_id, root, mut location) = {
            let stream = match self.connection_handle(stream_id).await {
                Ok(stream) => stream,
                Err(event) => return event,
            };
            let mut stream = stream.lock().await;

            match read_replication_message(
                &mut stream.1,
                self.io_timeout(),
                "waiting for incoming replication tree info",
            )
            .await
            {
                Ok(msg) => {
                    let (replication_id, root, location) =
                        match parse_replication_init(msg, replication_id) {
                            Ok(parsed) => parsed,
                            Err(err) => return BlobEvent::Error(err),
                        };

                    if let Err(event) = send_replication_message(
                        &mut stream.0,
                        ReplicationMessage::new(replication_id, MessageType::BaoTreeReceived),
                        self.io_timeout(),
                        "sending replication tree info acknowledgement",
                    )
                    .await
                    {
                        return event;
                    };
                    (replication_id, root, location)
                }
                Err(event) => return event,
            }
        };

        // A received archive is stored as it is, and only when sealed to the key asked for.
        let sealed = matches!(location.format.layout, StoredLayout::Pithos(_));
        if sealed && location.format.bucket_key() != resolved.encryption.map(|plan| plan.key) {
            let message = "the archive is not sealed to this bucket key";
            return BlobEvent::Error(BlobError::ReplicationRejected(message.to_string()));
        }
        let size = match sealed {
            true => location.stored_size(),
            false => location.blob_size,
        };
        // Reserve and record the destination before the replica is written.
        let backend_root = match self.registry.config_for(&resolved.backend) {
            Ok(config) => config.root.clone(),
            Err(err) => return BlobEvent::Error(err),
        };
        let ulid = Ulid::generate();
        location.backend = resolved.backend.clone();
        location.storage_class = resolved.storage_class.clone();
        location.root = backend_root;
        location.backend_path = match rebuild_backend_path(&location.backend_path, ulid) {
            Ok(path) => path,
            Err(err) => return BlobEvent::Error(BlobError::ConversionError(err)),
        };
        location.ulid = ulid;
        // The sender's plain format describes its own copy; this node stores with its own setting.
        if !sealed {
            location.format = StoredFormat::default();
        }
        let Some(mut reservation) = self.hold_reservation(location.ulid) else {
            return BlobEvent::Error(BlobError::ReplicationFailed(
                "too many active blob reservations".to_string(),
            ));
        };
        let mut location = match self.reserve_bucket(&resolved.backend, &location).await {
            Ok(location) => location,
            Err(err) => return BlobEvent::Error(err),
        };

        let operator = match self.operator_from_location(&location) {
            Ok(op) => op,
            Err(err) => {
                _ = self.release_reservation(&location).await;
                return BlobEvent::Error(err);
            }
        };

        let stream = match self.connection_handle(stream_id).await {
            Ok(stream) => stream,
            Err(event) => {
                _ = self.release_reservation(&location).await;
                return event;
            }
        };
        // Plaintext for an encrypting bucket is sealed with its plan, like any new write.
        if let (false, Some(plan)) = (sealed, resolved.encryption) {
            let received = (stream, root);
            let sealing = (plan, resolved.compression);
            let written = self.receive_sealing(received, location, operator, sealing);
            let event = Box::pin(written).await;
            if matches!(&event, BlobEvent::ReplicationFinished { .. })
                || matches!(&event, BlobEvent::Error(BlobError::WriteCleanup { .. }))
            {
                reservation.retain();
            }
            if !keep_alive {
                _ = self.close_connection(stream_id).await;
            }
            return event;
        }
        let mut stream = stream.lock().await;
        let rx = &mut stream.1;
        let rx_wrapper = RecvStreamWrapper::new(rx, self.transfer_idle_timeout());
        let storage_path = match location.get_storage_path() {
            Ok(storage_path) => storage_path,
            Err(e) => {
                _ = self.release_reservation(&location).await;
                return BlobEvent::Error(e);
            }
        };
        let mut writer = match OpenDalWriter::new(
            &operator,
            &storage_path,
            self.transfer_idle_timeout(),
            self.io_timeout(),
        )
        .await
        {
            Ok(writer) if sealed => writer.encoded(Compression::Off),
            Ok(writer) => writer.encoded(resolved.compression),
            Err(BlobLibError::IoError(error)) if error.kind() == std::io::ErrorKind::TimedOut => {
                reservation.retain();
                return BlobEvent::Error(BlobError::WriteCleanup {
                    location,
                    message: error.to_string(),
                });
            }
            Err(err) => {
                _ = self.release_reservation(&location).await;
                return BlobEvent::Error(BlobError::OperatorCreationFailed(err.to_string()));
            }
        };
        let mut ob = PreOrderOutboard {
            tree: BaoTree::new(size, BAO_BLOCK_SIZE),
            root,
            data: BytesMut::new(),
        };
        let byte_ranges = ByteRanges::from(0..size);
        let chunk_ranges = round_up_to_chunks(&byte_ranges);

        debug!("Try to decode chunks received from bidi stream");
        let decode_result = decode_ranges(rx_wrapper, chunk_ranges, &mut writer, &mut ob).await;
        drop(stream);

        let event = match decode_result {
            Err(err) => match writer.abort().await {
                Ok(()) => {
                    _ = self.release_reservation(&location).await;
                    BlobEvent::Error(BlobError::ReplicationFailed(err.to_string()))
                }
                Err(cleanup) => {
                    reservation.retain();
                    BlobEvent::Error(BlobError::WriteCleanup {
                        location,
                        message: format!("{err}; {cleanup}"),
                    })
                }
            },
            Ok(()) => {
                let hashes = writer.hasher.to_map();
                let actual_blake3 = writer.hasher.finalize().blake3;
                match writer.finalize().await.map(|layout| {
                    if let Some(layout) = layout {
                        location.format.layout = StoredLayout::Frames(Box::new(layout));
                    }
                }) {
                    Err(error) => {
                        reservation.retain();
                        BlobEvent::Error(BlobError::WriteCleanup {
                            location,
                            message: error.to_string(),
                        })
                    }
                    Ok(()) => {
                        debug!("Decoded all chunks and wrote them into the backend");
                        if actual_blake3 != root {
                            let mismatch = "replicated content hash mismatch";
                            match self.delete_path(&operator, &storage_path).await {
                                Ok(()) => {
                                    _ = self.release_reservation(&location).await;
                                    BlobEvent::Error(BlobError::IntegrityCheckFailed(
                                        mismatch.to_string(),
                                    ))
                                }
                                Err(error) => {
                                    reservation.retain();
                                    BlobEvent::Error(BlobError::WriteCleanup {
                                        location,
                                        message: format!("{mismatch}; {error}"),
                                    })
                                }
                            }
                        } else {
                            // Hashes of archive bytes are a transfer check, never an identity.
                            location.hashes = if sealed { HashMap::new() } else { hashes };
                            reservation.retain();
                            match self.finalize_reservation(&location).await {
                                Ok(()) => BlobEvent::ReplicationFinished { location },
                                Err(error) => BlobEvent::Error(BlobError::WriteCleanup {
                                    location,
                                    message: error.to_string(),
                                }),
                            }
                        }
                    }
                }
            }
        };

        if !keep_alive {
            _ = self.close_connection(stream_id).await;
        }
        event
    }

    /// Decodes plaintext checked against `root` and writes it sealed with `plan`, reusing the
    /// write path of new objects. The written content hash must equal `root`.
    async fn receive_sealing(
        &self,
        (stream, root): (super::SharedBiStream, blake3::Hash),
        location: BackendLocation,
        operator: opendal::Operator,
        (plan, compression): (SealPlan, Compression),
    ) -> BlobEvent {
        let size = location.blob_size;
        let (writer, reader) = tokio::io::duplex(64 * 1024);
        let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();
        let idle = self.transfer_idle_timeout();
        tokio::spawn(async move {
            let result: Result<(), BlobError> = async {
                let mut stream = stream.lock().await;
                let receiver = RecvStreamWrapper::new(&mut stream.1, idle);
                let mut writer = BaoReadWriter::new(writer);
                let mut outboard = PreOrderOutboard {
                    tree: BaoTree::new(size, BAO_BLOCK_SIZE),
                    root,
                    data: BytesMut::new(),
                };
                let ranges = round_up_to_chunks(&ByteRanges::from(0..size));
                decode_ranges(receiver, ranges, &mut writer, &mut outboard)
                    .await
                    .map_err(|error| BlobError::ReplicationFailed(error.to_string()))?;
                writer
                    .finish(size, *root.as_bytes())
                    .map_err(|error| BlobError::IntegrityCheckFailed(error.to_string()))
            }
            .await;
            _ = completion_tx.send(result);
        });
        let blob = BackendStream::new(tokio_util::io::ReaderStream::new(reader)).on_success_async(
            move || async move {
                completion_rx
                    .await
                    .map_err(|_| StreamError(Box::new(BlobError::ChannelClosed)))?
                    .map_err(|error| StreamError(Box::new(error)))
            },
        );
        let seal = (Some(plan), None);
        let written = self.write_encoded(
            location.clone(),
            operator,
            blob,
            compression,
            seal,
            Some(size),
        );
        match Box::pin(written).await {
            BlobEvent::WriteFinished { location }
                if location.get_blake3() == Some(root.as_bytes().as_slice()) =>
            {
                match self.finalize_reservation(&location).await {
                    Ok(()) => BlobEvent::ReplicationFinished { location },
                    Err(error) => BlobEvent::Error(BlobError::WriteCleanup {
                        location,
                        message: error.to_string(),
                    }),
                }
            }
            BlobEvent::WriteFinished { location } => BlobEvent::Error(BlobError::WriteCleanup {
                location,
                message: "replicated content hash mismatch".to_string(),
            }),
            BlobEvent::Error(error @ BlobError::WriteCleanup { .. }) => BlobEvent::Error(error),
            other => {
                _ = self.release_reservation(&location).await;
                other
            }
        }
    }
}

/// The location a plaintext transfer announces: the receiver stores the bytes with its own format.
fn plain_sent(location: &BackendLocation) -> BackendLocation {
    let mut plain = location.clone();
    plain.format = StoredFormat::default();
    plain
}

/// Resolves one observation to a reader whose bytes provably carry the named
/// identity. Every refusal here happens before a single byte is offered.
async fn verified_source(
    access: &ResolvedSourceAccess,
    size: u64,
    expected_blake3: [u8; 32],
    fingerprint: &str,
) -> Result<
    (
        crate::bao_tree::LocalFileReader,
        PreOrderOutboard<BytesMut>,
        tokio::fs::File,
        String,
    ),
    BlobEvent,
> {
    // Only a local directory is ever streamed this way: another connector kind
    // carrying an offered root must never reach the filesystem here.
    if !crate::fs_source::is_local_access(access) {
        return Err(BlobEvent::Error(BlobError::ReadError(
            "only a local directory is served from its source".to_string(),
        )));
    }
    let (file, current) = crate::fs_source::stable_source(access)
        .await
        .map_err(|error| BlobEvent::Error(BlobError::ReadError(error.to_string())))?;
    if fingerprint != current {
        return Err(BlobEvent::Error(BlobError::IntegrityCheckFailed(
            "the offered file changed since it was observed".to_string(),
        )));
    }
    let observed = file
        .try_clone()
        .await
        .map_err(|error| BlobEvent::Error(BlobError::ReadError(error.to_string())))?;
    let mut reader = crate::bao_tree::LocalFileReader::from_file(file, size);
    let outboard = match PreOrderOutboard::<BytesMut>::create(&mut reader, BAO_BLOCK_SIZE).await {
        Ok(outboard) if outboard.root.as_bytes() == &expected_blake3 => outboard,
        Ok(_) => {
            return Err(BlobEvent::Error(BlobError::IntegrityCheckFailed(
                "the offered file does not carry the expected hash".to_string(),
            )));
        }
        Err(error) => {
            return Err(BlobEvent::Error(BlobError::OutboardCreationFailed(
                error.to_string(),
            )));
        }
    };
    Ok((reader, outboard, observed, current))
}

#[cfg(test)]
mod tests {
    use super::verified_source;
    use aruna_core::errors::BlobError;
    use aruna_core::events::BlobEvent;
    use aruna_core::structs::execution::offered_directory::{
        FileStat, OFFERED_DIRECTORY_ROOT, weak_fingerprint,
    };
    use aruna_core::structs::execution::source_access::ResolvedSourceAccess;
    use aruna_core::structs::execution::source_connector::SourceConnectorKind;
    use std::collections::HashMap;
    use std::path::Path;

    fn access(root: &Path, path: &str, kind: SourceConnectorKind) -> ResolvedSourceAccess {
        ResolvedSourceAccess::OpenDal {
            kind,
            config: HashMap::from([(
                OFFERED_DIRECTORY_ROOT.to_string(),
                root.to_string_lossy().to_string(),
            )]),
            path: path.to_string(),
            version: None,
        }
    }

    async fn fingerprint_of(path: &Path) -> String {
        let metadata = tokio::fs::metadata(path).await.unwrap();
        weak_fingerprint(&FileStat::from_metadata(&metadata))
    }

    #[tokio::test]
    async fn serves_verified_source() {
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("note.txt");
        tokio::fs::write(&file, b"hello").await.unwrap();
        let hash = *blake3::hash(b"hello").as_bytes();
        let fingerprint = fingerprint_of(&file).await;
        assert!(
            verified_source(
                &access(root.path(), "note.txt", SourceConnectorKind::LocalDirectory),
                5,
                hash,
                &fingerprint,
            )
            .await
            .is_ok()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn source_handle_pinned() {
        use iroh_io::AsyncSliceReader;
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let path = root.path().join("file");
        std::fs::write(&path, b"inside").unwrap();
        std::fs::write(outside.path().join("file"), b"secret").unwrap();
        let (mut reader, _, _, _) = verified_source(
            &access(root.path(), "file", SourceConnectorKind::LocalDirectory),
            6,
            *blake3::hash(b"inside").as_bytes(),
            &fingerprint_of(&path).await,
        )
        .await
        .unwrap();
        std::fs::rename(&path, root.path().join("saved")).unwrap();
        std::os::unix::fs::symlink(outside.path().join("file"), &path).unwrap();
        assert_eq!(
            reader.read_exact_at(0, 6).await.unwrap().as_ref(),
            b"inside"
        );
    }

    // Only a local directory is streamed from its source; another connector
    // kind carrying an offered root must never reach the filesystem.
    #[tokio::test]
    async fn refuses_foreign_kind() {
        let root = tempfile::tempdir().unwrap();
        tokio::fs::write(root.path().join("note.txt"), b"hello")
            .await
            .unwrap();
        let refused = verified_source(
            &access(root.path(), "note.txt", SourceConnectorKind::S3),
            5,
            *blake3::hash(b"hello").as_bytes(),
            "5-0",
        )
        .await;
        assert!(matches!(
            refused,
            Err(BlobEvent::Error(BlobError::ReadError(_)))
        ));
    }

    #[tokio::test]
    async fn refuses_changed_file() {
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("note.txt");
        tokio::fs::write(&file, b"hello").await.unwrap();
        let refused = verified_source(
            &access(root.path(), "note.txt", SourceConnectorKind::LocalDirectory),
            5,
            *blake3::hash(b"hello").as_bytes(),
            "5-deadbeef",
        )
        .await;
        assert!(matches!(
            refused,
            Err(BlobEvent::Error(BlobError::IntegrityCheckFailed(_)))
        ));
    }

    // The requester names an identity; a file that does not carry it is never
    // streamed under it.
    #[tokio::test]
    async fn refuses_wrong_hash() {
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("note.txt");
        tokio::fs::write(&file, b"hello").await.unwrap();
        let fingerprint = fingerprint_of(&file).await;
        let refused = verified_source(
            &access(root.path(), "note.txt", SourceConnectorKind::LocalDirectory),
            5,
            [9u8; 32],
            &fingerprint,
        )
        .await;
        assert!(matches!(
            refused,
            Err(BlobEvent::Error(BlobError::IntegrityCheckFailed(_)))
        ));
    }

    #[tokio::test]
    async fn refuses_escaping_link() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        tokio::fs::write(outside.path().join("secret"), b"hello")
            .await
            .unwrap();
        std::os::unix::fs::symlink(outside.path().join("secret"), root.path().join("link"))
            .unwrap();
        let refused = verified_source(
            &access(root.path(), "link", SourceConnectorKind::LocalDirectory),
            5,
            *blake3::hash(b"hello").as_bytes(),
            "5-0",
        )
        .await;
        assert!(matches!(
            refused,
            Err(BlobEvent::Error(BlobError::ReadError(_)))
        ));
    }
}
