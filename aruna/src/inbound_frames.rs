//! Reads one metadata request frame from a peer stream and writes its answer back, each step
//! bounded in time. The protocol decides what a frame may hold; this file only moves bytes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use aruna_core::metadata::MetadataError;
use aruna_core::metadata::protocol::{
    MAX_MESSAGE_SIZE, METADATA_IO_TIMEOUT, MetadataTransportMessage, POSTCARD_VARIANT_BYTES,
    check_header, check_variant, decode_frame, encode_frame, transport_message_kind, variant_read,
};
use aruna_core::telemetry::record_elapsed_ms;
use aruna_net::streams::BiStream;
use aruna_operations::metadata::audit::AUDIT_DEADLINE_SECS;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::{Instant, timeout, timeout_at};
use tracing::Span;

/// A body is written in chunks of this many bytes.
const BODY_CHUNK_BYTES: usize = 64 * 1024;

/// One request read from an inbound metadata stream.
pub(crate) struct InboundRequest {
    pub(crate) message: MetadataTransportMessage,
    /// Held until the answer is written, so frames in flight stay within the budget.
    pub(crate) budget: OwnedSemaphorePermit,
    /// An audit request keeps one deadline for every step after it was read.
    pub(crate) audit: Option<Instant>,
}

fn backend(error: impl ToString) -> MetadataError {
    MetadataError::Backend(error.to_string())
}

/// One frame no larger than `max_size`, read under the shared byte `budget`, which the payload
/// holds until the answer is written.
pub(crate) async fn read_frame<R>(
    reader: &mut R,
    max_size: usize,
    budget: &Arc<Semaphore>,
) -> Result<(MetadataTransportMessage, OwnedSemaphorePermit), String>
where
    R: AsyncRead + Unpin + ?Sized,
{
    let mut header = [0u8; 5];
    reader
        .read_exact(&mut header)
        .await
        .map_err(|error| error.to_string())?;
    let class = header[0];
    let length = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
    check_header(class, length, max_size)?;
    let permits =
        u32::try_from(length).map_err(|_| "metadata frame length is unsupported".to_string())?;
    let permit = (budget.clone())
        .try_acquire_many_owned(permits)
        .map_err(|_| "metadata inbound frame budget unavailable".to_string())?;
    let mut prefix = [0u8; POSTCARD_VARIANT_BYTES];
    let variant_end = length.min(POSTCARD_VARIANT_BYTES);
    let mut read = 0;
    while read < variant_end && (read == 0 || !variant_read(&prefix[..read])) {
        reader
            .read_exact(&mut prefix[read..read + 1])
            .await
            .map_err(|error| error.to_string())?;
        read += 1;
    }
    if read > 0 {
        check_variant(&prefix[..read], length)?;
    }
    let mut bytes = vec![0u8; length];
    bytes[..read].copy_from_slice(&prefix[..read]);
    reader
        .read_exact(&mut bytes[read..])
        .await
        .map_err(|error| error.to_string())?;
    Ok((decode_frame(class, &bytes)?, permit))
}

/// One frame of at most the message size, read within the I/O wait.
async fn read_within<R>(
    reader: &mut R,
    budget: &Arc<Semaphore>,
) -> Result<(MetadataTransportMessage, OwnedSemaphorePermit), MetadataError>
where
    R: AsyncRead + Unpin + ?Sized,
{
    let read = read_frame(reader, MAX_MESSAGE_SIZE, budget);
    timeout(METADATA_IO_TIMEOUT, read)
        .await
        .map_err(|_| backend("timed out waiting for metadata message"))?
        .map_err(MetadataError::Backend)
}

/// Reads the request of an inbound metadata stream within the I/O wait.
pub(crate) async fn read_request(
    stream: &mut BiStream,
    budget: &Arc<Semaphore>,
) -> Result<InboundRequest, MetadataError> {
    let audit_deadline = Instant::now() + Duration::from_secs(AUDIT_DEADLINE_SECS);
    let (message, budget) = read_within(&mut stream.1, budget).await?;
    Span::current().record("request", transport_message_kind(&message));
    let audit = matches!(message, MetadataTransportMessage::ForwardAuditPage { .. });
    Ok(InboundRequest {
        message,
        budget,
        audit: audit.then_some(audit_deadline),
    })
}

// The deadline of one step: the audit deadline, or a fresh I/O wait.
fn step_deadline(audit: Option<Instant>) -> Instant {
    audit.unwrap_or_else(|| Instant::now() + METADATA_IO_TIMEOUT)
}

// Finishes the answer unless the deadline passed, then stops reading the request.
fn close(stream: &mut BiStream, deadline: Instant) {
    if Instant::now() < deadline {
        let _ = stream.0.finish();
    }
    let _ = stream.1.stop(0u32.into());
}

/// Drains the request, writes the answer and any body, and closes the stream.
pub(crate) async fn write_answer(
    mut stream: BiStream,
    audit: Option<Instant>,
    response: &MetadataTransportMessage,
    body: Option<Vec<u8>>,
) -> Result<(), MetadataError> {
    let span = Span::current();
    span.record("response", transport_message_kind(response));
    let drain_started = std::time::Instant::now();
    let drained = timeout_at(step_deadline(audit), stream.1.read_to_end(1))
        .await
        .map_err(|_| backend("timed out draining metadata request stream"))
        .and_then(|read| read.map(|_| ()).map_err(backend));
    if let Err(error) = drained {
        if let Some(deadline) = audit {
            close(&mut stream, deadline);
        }
        return Err(error);
    }
    record_elapsed_ms(&span, "drain_ms", drain_started);
    let write_started = std::time::Instant::now();
    let frame = encode_frame(response).map_err(MetadataError::Backend)?;
    let written = timeout_at(step_deadline(audit), async {
        stream.0.write_all(&frame).await?;
        stream.0.flush().await
    })
    .await
    .map_err(|_| backend("timed out writing metadata answer"))
    .and_then(|write| write.map_err(backend));
    let mut result = written;
    if result.is_ok()
        && let Some(body) = body
    {
        result = timeout_at(step_deadline(audit), async {
            for chunk in body.chunks(BODY_CHUNK_BYTES) {
                stream.0.write_all(chunk).await?;
            }
            Ok::<(), std::io::Error>(())
        })
        .await
        .map_err(|_| backend("timed out writing metadata answer body"))
        .and_then(|write| write.map_err(backend));
    }
    record_elapsed_ms(&span, "write_ms", write_started);
    close(&mut stream, step_deadline(audit));
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::audit::{AuditPageEntry, AuditPageResponse};
    use aruna_core::metadata::protocol::{
        AUDIT_FRAME, AUDIT_REQUEST_VARIANT, INBOUND_FRAME_BYTES, STANDARD_FRAME, audit_frame_cap,
    };
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::metadata_registry::{
        MetadataAuditOperation, MetadataAuditRecord,
    };
    use aruna_core::user::id::UserId;
    use ulid::Ulid;

    async fn read_capped(
        reader: &mut tokio::io::DuplexStream,
        cap: usize,
    ) -> Result<MetadataTransportMessage, String> {
        let budget = Arc::new(Semaphore::new(MAX_MESSAGE_SIZE));
        let read = read_frame(reader, cap, &budget).await;
        read.map(|(message, _)| message)
    }

    #[tokio::test]
    async fn audit_frame_rejects() {
        let (mut writer, mut reader) = tokio::io::duplex(16);
        let length = (audit_frame_cap() + 1) as u32;
        writer.write_all(&[AUDIT_FRAME]).await.unwrap();
        writer.write_all(&length.to_be_bytes()).await.unwrap();

        let error = read_capped(&mut reader, audit_frame_cap())
            .await
            .unwrap_err();
        assert_eq!(error, "metadata frame exceeds maximum size");
    }

    #[tokio::test]
    async fn audit_rejects_early() {
        let (mut writer, mut reader) = tokio::io::duplex(16);
        let length = (audit_frame_cap() + 1) as u32;
        writer.write_all(&[AUDIT_FRAME]).await.unwrap();
        writer.write_all(&length.to_be_bytes()).await.unwrap();
        let budget = Arc::new(Semaphore::new(1));

        let error = read_frame(&mut reader, MAX_MESSAGE_SIZE, &budget)
            .await
            .unwrap_err();
        assert_eq!(error, "metadata audit frame exceeds maximum size");
        assert_eq!(budget.available_permits(), 1);
    }

    #[tokio::test]
    async fn audit_wire_cap() {
        let (mut writer, mut reader) = tokio::io::duplex(16);
        let length = (audit_frame_cap() + 1) as u32;
        writer.write_all(&[STANDARD_FRAME]).await.unwrap();
        writer.write_all(&length.to_be_bytes()).await.unwrap();
        writer
            .write_all(&[AUDIT_REQUEST_VARIANT as u8])
            .await
            .unwrap();
        let budget = Arc::new(Semaphore::new(INBOUND_FRAME_BYTES));

        let error = read_frame(&mut reader, MAX_MESSAGE_SIZE, &budget)
            .await
            .unwrap_err();
        assert_eq!(error, "metadata audit frame exceeds maximum size");
        assert_eq!(budget.available_permits(), INBOUND_FRAME_BYTES);
    }

    #[tokio::test]
    async fn audit_wire_varint() {
        let (mut writer, mut reader) = tokio::io::duplex(16);
        let length = (audit_frame_cap() + 1) as u32;
        writer.write_all(&[STANDARD_FRAME]).await.unwrap();
        writer.write_all(&length.to_be_bytes()).await.unwrap();
        writer.write_all(&[0x9f, 0]).await.unwrap();
        let budget = Arc::new(Semaphore::new(INBOUND_FRAME_BYTES));

        let error = read_frame(&mut reader, MAX_MESSAGE_SIZE, &budget)
            .await
            .unwrap_err();
        assert_eq!(error, "metadata audit frame exceeds maximum size");
        assert_eq!(budget.available_permits(), INBOUND_FRAME_BYTES);
    }

    #[tokio::test]
    async fn audit_variant_limit() {
        for prefix in [
            vec![0x80; POSTCARD_VARIANT_BYTES],
            vec![0x9f, 0x80, 0x80, 0x80, 0x80, 0],
        ] {
            let (mut writer, mut reader) = tokio::io::duplex(16);
            let length = (audit_frame_cap() + 1) as u32;
            writer.write_all(&[STANDARD_FRAME]).await.unwrap();
            writer.write_all(&length.to_be_bytes()).await.unwrap();
            writer.write_all(&prefix).await.unwrap();
            let budget = Arc::new(Semaphore::new(INBOUND_FRAME_BYTES));

            let error = read_frame(&mut reader, MAX_MESSAGE_SIZE, &budget)
                .await
                .unwrap_err();
            assert_eq!(error, "metadata frame variant is invalid");
            assert_eq!(budget.available_permits(), INBOUND_FRAME_BYTES);
        }
    }

    #[tokio::test]
    async fn budget_releases() {
        let message = MetadataTransportMessage::Reject(String::new());
        let bytes = postcard::to_allocvec(&message).unwrap();
        let (mut writer, mut reader) = tokio::io::duplex(bytes.len() + 5);
        writer.write_all(&[STANDARD_FRAME]).await.unwrap();
        writer
            .write_all(&(bytes.len() as u32).to_be_bytes())
            .await
            .unwrap();
        writer.write_all(&bytes).await.unwrap();
        let budget = Arc::new(Semaphore::new(32));

        let (decoded, permit) = read_frame(&mut reader, MAX_MESSAGE_SIZE, &budget)
            .await
            .unwrap();
        assert_eq!(decoded, message);
        assert_eq!(budget.available_permits(), 32 - bytes.len());
        drop(permit);
        assert_eq!(budget.available_permits(), 32);
    }

    #[tokio::test]
    async fn budget_rejects() {
        let (mut writer, mut reader) = tokio::io::duplex(16);
        writer
            .write_all(&[STANDARD_FRAME, 0, 0, 0, 8])
            .await
            .unwrap();
        let budget = Arc::new(Semaphore::new(4));

        let error = read_frame(&mut reader, MAX_MESSAGE_SIZE, &budget)
            .await
            .unwrap_err();
        assert_eq!(error, "metadata inbound frame budget unavailable");
        assert_eq!(budget.available_permits(), 4);
    }

    #[tokio::test]
    async fn budget_cancels() {
        let (mut writer, reader) = tokio::io::duplex(16);
        writer
            .write_all(&[STANDARD_FRAME, 0, 0, 0, 8])
            .await
            .unwrap();
        let budget = Arc::new(Semaphore::new(8));
        let task_budget = budget.clone();
        let task = tokio::spawn(async move {
            let mut reader = reader;
            read_frame(&mut reader, MAX_MESSAGE_SIZE, &task_budget).await
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            while budget.available_permits() == 8 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        task.abort();
        let _ = task.await;
        assert_eq!(budget.available_permits(), 8);
    }

    #[tokio::test]
    async fn audit_frame_reads() {
        let message = MetadataTransportMessage::ForwardedAuditPage {
            result: Ok(AuditPageResponse {
                records: Vec::new(),
                next_start_after: None,
            }),
        };
        let bytes = postcard::to_allocvec(&message).unwrap();
        assert!(bytes.len() <= audit_frame_cap());
        let (mut writer, mut reader) = tokio::io::duplex(bytes.len() + 5);
        writer.write_all(&[AUDIT_FRAME]).await.unwrap();
        writer
            .write_all(&(bytes.len() as u32).to_be_bytes())
            .await
            .unwrap();
        writer.write_all(&bytes).await.unwrap();

        assert_eq!(
            read_capped(&mut reader, audit_frame_cap()).await.unwrap(),
            message
        );
    }

    #[tokio::test]
    async fn audit_cap_decoded() {
        let realm_id = RealmId([1u8; 32]);
        let message = MetadataTransportMessage::ForwardedAuditPage {
            result: Ok(AuditPageResponse {
                records: vec![AuditPageEntry {
                    key: vec![0u8; aruna_core::audit::AUDIT_KEY_BYTES],
                    record: MetadataAuditRecord {
                        realm_id,
                        group_id: Ulid::from_bytes([2u8; 16]),
                        document_id: Ulid::from_bytes([3u8; 16]),
                        graph_iri: "x".repeat(audit_frame_cap()),
                        user_id: UserId::local(Ulid::from_bytes([4u8; 16]), realm_id),
                        node_id: iroh::SecretKey::from_bytes(&[5u8; 32]).public(),
                        operation: MetadataAuditOperation::Create,
                        occurred_at_ms: 0,
                        details: None,
                    },
                }],
                next_start_after: None,
            }),
        };
        let bytes = postcard::to_allocvec(&message).unwrap();
        assert!(bytes.len() > audit_frame_cap());
        let (mut writer, mut reader) = tokio::io::duplex(bytes.len() + 5);
        writer.write_all(&[STANDARD_FRAME]).await.unwrap();
        writer
            .write_all(&(bytes.len() as u32).to_be_bytes())
            .await
            .unwrap();
        writer.write_all(&bytes).await.unwrap();

        assert_eq!(
            read_capped(&mut reader, MAX_MESSAGE_SIZE)
                .await
                .unwrap_err(),
            "metadata audit frame exceeds maximum size"
        );
    }

    #[tokio::test]
    async fn frame_class_matches() {
        let message = MetadataTransportMessage::ForwardedAuditPage {
            result: Ok(AuditPageResponse {
                records: Vec::new(),
                next_start_after: None,
            }),
        };
        let bytes = postcard::to_allocvec(&message).unwrap();
        let (mut writer, mut reader) = tokio::io::duplex(bytes.len() + 5);
        writer.write_all(&[STANDARD_FRAME]).await.unwrap();
        writer
            .write_all(&(bytes.len() as u32).to_be_bytes())
            .await
            .unwrap();
        writer.write_all(&bytes).await.unwrap();

        assert_eq!(
            read_capped(&mut reader, MAX_MESSAGE_SIZE)
                .await
                .unwrap_err(),
            "metadata frame class does not match message"
        );
    }

    async fn assert_timeout(frame: &[u8], held: bool) {
        let (mut writer, reader) = tokio::io::duplex(16);
        writer.write_all(frame).await.unwrap();
        let budget = Arc::new(Semaphore::new(8));
        let task_budget = budget.clone();
        let task = tokio::spawn(async move {
            let mut reader = reader;
            read_within(&mut reader, &task_budget).await
        });
        tokio::task::yield_now().await;
        assert_eq!(budget.available_permits(), if held { 0 } else { 8 });

        tokio::time::advance(METADATA_IO_TIMEOUT).await;
        let error = task.await.unwrap().unwrap_err();
        assert!(matches!(
            error,
            MetadataError::Backend(message)
                if message == "timed out waiting for metadata message"
        ));
        assert_eq!(budget.available_permits(), 8);
    }

    #[tokio::test(start_paused = true)]
    async fn header_timeout() {
        assert_timeout(&[0], false).await;
    }

    #[tokio::test(start_paused = true)]
    async fn body_timeout() {
        assert_timeout(&[0, 0, 0, 0, 8], true).await;
    }
}
