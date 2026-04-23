use super::BlobHandler;
use super::backend::rebuild_backend_path;
use super::control_plane::{
    parse_replication_init, read_replication_message_with_timeout,
    send_replication_message_with_timeout, validate_replication_init_ack,
};
use crate::bao_tree::{OpenDalReader, OpenDalWriter, RecvStreamWrapper, SendStreamWrapper};
use crate::messages::{MessageType, ReplicationMessage};
use aruna_core::errors::BlobError;
use aruna_core::events::BlobEvent;
use aruna_core::structs::BackendLocation;
use bao_tree::io::fsm::{CreateOutboard, decode_ranges, encode_ranges_validated};
use bao_tree::io::outboard::PreOrderOutboard;
use bao_tree::io::round_up_to_chunks;
use bao_tree::{BaoTree, ByteRanges};
use bytes::BytesMut;
use futures::AsyncWriteExt;
use tracing::debug;
use ulid::Ulid;

use super::BAO_BLOCK_SIZE;

impl BlobHandler {
    pub async fn replicate_blob(
        &mut self,
        replication_id: Ulid,
        stream_id: Ulid,
        location: BackendLocation,
        keep_alive: bool,
    ) -> BlobEvent {
        let operator = match self.operator_from_location(&location) {
            Ok(op) => op,
            Err(err) => {
                return BlobEvent::Error(BlobError::OperatorCreationFailed(err.to_string()));
            }
        };

        let storage_path = match location.get_storage_path() {
            Ok(storage_path) => storage_path,
            Err(e) => return BlobEvent::Error(e),
        };

        let mut reader = match OpenDalReader::new(
            &operator,
            &storage_path,
            location.blob_size,
            self.transfer_idle_timeout(),
        )
        .await
        {
            Ok(reader) => reader,
            Err(err) => {
                return BlobEvent::Error(BlobError::OperatorCreationFailed(err.to_string()));
            }
        };
        let mut outboard =
            match PreOrderOutboard::<BytesMut>::create(&mut reader, BAO_BLOCK_SIZE).await {
                Ok(outboard) => outboard,
                Err(err) => {
                    return BlobEvent::Error(BlobError::OutboardCreationFailed(err.to_string()));
                }
            };

        let mut connections = self.connections.lock().await;
        let Some((sx, rx)) = connections.get_mut(&stream_id) else {
            return BlobEvent::Error(BlobError::ReplicationRejected(
                "Stream not available".to_string(),
            ));
        };

        let replication_init = ReplicationMessage {
            id: replication_id,
            msg_type: MessageType::BaoTreeInfo {
                location: location.clone(),
                root: outboard.root,
            },
        };
        if let Err(event) = send_replication_message_with_timeout(
            sx,
            replication_init,
            self.control_plane_io_timeout(),
            "sending replication tree info",
        )
        .await
        {
            return event;
        }

        match read_replication_message_with_timeout(
            rx,
            self.control_plane_io_timeout(),
            "waiting for replication tree info acknowledgement",
        )
        .await
        {
            Ok(msg) => {
                if let Err(err) = validate_replication_init_ack(msg, replication_id) {
                    return BlobEvent::Error(err);
                }
            }
            Err(err) => return err,
        }

        let mut sx_wrapper = SendStreamWrapper::new(sx, self.transfer_idle_timeout());
        let ranges = ByteRanges::from(0..location.blob_size);
        let ranges = round_up_to_chunks(&ranges);
        debug!("Chunk Ranges: {:#?}", ranges.boundaries());

        if let Err(err) =
            encode_ranges_validated(reader, &mut outboard, &ranges, &mut sx_wrapper).await
        {
            return BlobEvent::Error(BlobError::ReplicationFailed(err.to_string()));
        }

        if !keep_alive {
            _ = sx.finish();
            _ = rx.stop(0u32.into());
        }
        BlobEvent::ReplicationFinished { location }
    }

    pub async fn handle_incoming_replication(
        &mut self,
        replication_id: Option<Ulid>,
        stream_id: Ulid,
        keep_alive: bool,
    ) -> BlobEvent {
        let (_replication_id, root, mut location) = {
            let mut connections = self.connections.lock().await;
            let Some((sx, rx)) = connections.get_mut(&stream_id) else {
                return BlobEvent::Error(BlobError::ReplicationRejected(
                    "Stream not available".to_string(),
                ));
            };

            match read_replication_message_with_timeout(
                rx,
                self.control_plane_io_timeout(),
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

                    if let Err(event) = send_replication_message_with_timeout(
                        sx,
                        ReplicationMessage::new(replication_id, MessageType::BaoTreeInfoReceived),
                        self.control_plane_io_timeout(),
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

        let backend_bucket = match self.eval_backend_bucket().await {
            Ok(bucket) => bucket,
            Err(err) => return BlobEvent::Error(err),
        };
        let ulid = Ulid::new();
        location.root = self.backend_config.root.clone();
        location.storage_bucket = backend_bucket.clone();
        location.backend_path = match rebuild_backend_path(&location.backend_path, ulid) {
            Ok(path) => path,
            Err(err) => return BlobEvent::Error(BlobError::ConversionError(err)),
        };
        location.ulid = ulid;

        let operator = match self.operator_from_location(&location) {
            Ok(op) => op,
            Err(err) => {
                return BlobEvent::Error(BlobError::OperatorCreationFailed(err.to_string()));
            }
        };

        let mut connections = self.connections.lock().await;
        let Some((_, rx)) = connections.get_mut(&stream_id) else {
            return BlobEvent::Error(BlobError::ReplicationRejected(
                "Stream not available".to_string(),
            ));
        };
        let rx_wrapper = RecvStreamWrapper::new(rx, self.transfer_idle_timeout());
        let storage_path = match location.get_storage_path() {
            Ok(storage_path) => storage_path,
            Err(e) => return BlobEvent::Error(e),
        };
        let mut writer = match OpenDalWriter::new(
            &operator,
            &storage_path,
            location.blob_size,
            self.transfer_idle_timeout(),
        )
        .await
        {
            Ok(writer) => writer,
            Err(err) => {
                return BlobEvent::Error(BlobError::OperatorCreationFailed(err.to_string()));
            }
        };
        let mut ob = PreOrderOutboard {
            tree: BaoTree::new(location.blob_size, BAO_BLOCK_SIZE),
            root,
            data: BytesMut::new(),
        };
        let byte_ranges = ByteRanges::from(0..location.blob_size);
        let chunk_ranges = round_up_to_chunks(&byte_ranges);

        debug!("Try to decode chunks received from bidi stream");
        if let Err(err) = decode_ranges(rx_wrapper, chunk_ranges, &mut writer, &mut ob).await {
            return BlobEvent::Error(BlobError::ReplicationFailed(err.to_string()));
        }
        _ = writer.writer.close().await;
        debug!("Decoded all chunks and wrote them into the backend");

        let hashes = writer.hasher.to_map();
        let actual_blake3 = writer.hasher.finalize().blake3;
        if actual_blake3 != root {
            let _ = operator.delete(&storage_path).await;
            return BlobEvent::Error(BlobError::IntegrityCheckFailed(
                "replicated content hash mismatch".to_string(),
            ));
        }

        location.hashes = hashes;
        if let Err(err) = self.increment_bucket_load(&location.storage_bucket).await {
            BlobEvent::Error(err)
        } else {
            if !keep_alive {
                let mut connections = self.connections.lock().await;
                if let Some((sx, rx)) = connections.get_mut(&stream_id) {
                    _ = sx.finish();
                    _ = rx.stop(0u32.into());
                }
            }
            BlobEvent::ReplicationFinished { location }
        }
    }
}
