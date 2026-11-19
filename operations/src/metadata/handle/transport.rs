//! Frames metadata messages on streams and sends remote requests like query, search and sync.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::metadata::message_kind::transport_message_kind;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use aruna_core::NodeId;
use aruna_core::alpn::Alpn;
use aruna_core::metadata::{MetadataError, MetadataQueryResults, MetadataSearchHit};
use aruna_core::telemetry::record_elapsed_ms;
use aruna_core::types::GroupId;
use aruna_net::NetHandle;
use aruna_net::streams::{BiStream, RecvStream};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::time::{timeout, timeout_at};
use tracing::{Span, field};
use ulid::Ulid;

use super::effects::record_query_counts;
use super::frames::{
    read_message, read_message_budget, read_message_cap, write_encoded_message, write_message,
};
use super::{
    METADATA_CHUNK_SIZE, METADATA_ENVELOPE_BYTES, METADATA_IO_TIMEOUT, MetadataHandle,
    MetadataInner, MetadataRequestError, PAGE_ANSWER_TIMEOUT,
};
use crate::auth::request_policy::PolicyRequestExtras;
use crate::metadata::protocol::{
    MetadataReadError, MetadataTransportMessage, decode_message, encode_message, frame_class,
    response_cap,
};
use crate::s3::bucket::search::BucketPage;
use crate::s3::object::search::{ObjectKeyMatch, SearchNodePage};
use aruna_core::metadata::AuthToken;
use aruna_core::structs::storage::release::{HolderAnswer, HolderAsk};
use aruna_core::telemetry::record_error;

#[tracing::instrument(
    name = "metadata.remote.request",
    level = "debug",
    skip(net_handle, message),
    fields(
        peer = ?node_id,
        request = transport_message_kind(&message),
        response = field::Empty,
        open_stream_ms = field::Empty,
        write_ms = field::Empty,
        finish_ms = field::Empty,
        read_ms = field::Empty,
        close_ms = field::Empty,
        elapsed_ms = field::Empty,
    )
)]
pub(super) async fn send_request(
    net_handle: &NetHandle,
    node_id: NodeId,
    message: MetadataTransportMessage,
) -> Result<MetadataTransportMessage, MetadataRequestError> {
    let span = Span::current();
    let total_started = Instant::now();
    let max_response_size = response_cap(&message);
    // A member answers a reference page only after its one commit, which takes longer.
    let answer_timeout = match &message {
        MetadataTransportMessage::ReferencePage { .. } => PAGE_ANSWER_TIMEOUT,
        _ => METADATA_IO_TIMEOUT,
    };

    let bytes = encode_message(&message)
        .map_err(MetadataError::Backend)
        .map_err(MetadataRequestError::definitely_not_sent)?;

    let open_started = Instant::now();
    let mut stream = timeout(
        METADATA_IO_TIMEOUT,
        net_handle.open_stream(node_id, Alpn::Metadata),
    )
    .await
    .map_err(|_| MetadataError::Backend("timed out opening metadata stream".to_string()))
    .and_then(|result| result.map_err(|error| MetadataError::Backend(error.to_string())))
    .map_err(MetadataRequestError::definitely_not_sent)?;
    record_elapsed_ms(&span, "open_stream_ms", open_started);

    let write_started = Instant::now();
    write_framed_message(&mut stream, frame_class(&message), &bytes)
        .await
        .map_err(|error| with_close(&stream, error))
        .map_err(MetadataRequestError::possibly_sent)?;
    record_elapsed_ms(&span, "write_ms", write_started);

    let finish_started = Instant::now();
    stream
        .0
        .finish()
        .map_err(|error| with_close(&stream, MetadataError::Backend(error.to_string())))
        .map_err(MetadataRequestError::possibly_sent)?;
    record_elapsed_ms(&span, "finish_ms", finish_started);

    let read_started = Instant::now();
    let response = read_transport_cap(&mut stream, max_response_size, answer_timeout)
        .await
        .map_err(|error| with_close(&stream, error))
        .map_err(MetadataRequestError::possibly_sent)?;
    record_elapsed_ms(&span, "read_ms", read_started);

    let close_started = Instant::now();
    close_stream(&mut stream).await;
    record_elapsed_ms(&span, "close_ms", close_started);
    record_elapsed_ms(&span, "elapsed_ms", total_started);
    span.record("response", transport_message_kind(&response));
    Ok(response)
}

pub(super) async fn send_export_request(
    inner: &MetadataInner,
    node_id: NodeId,
    message: MetadataTransportMessage,
) -> Result<
    Result<super::super::api::export_types::ExportMetadataResult, MetadataReadError>,
    MetadataRequestError,
> {
    let metadata_bytes = match &message {
        MetadataTransportMessage::ForwardExportDocument { metadata_bytes, .. } => *metadata_bytes,
        // The channel carries one Profile document; the holder caps the body.
        MetadataTransportMessage::ForwardExportProfile { .. } => u64::MAX,
        _ => {
            return Err(MetadataRequestError::definitely_not_sent(
                MetadataError::InvalidInput("expected a metadata export request".to_string()),
            ));
        }
    };
    let bytes = encode_message(&message)
        .map_err(MetadataError::Backend)
        .map_err(MetadataRequestError::definitely_not_sent)?;
    let net_handle = inner
        .net_handle
        .clone()
        .ok_or_else(|| MetadataRequestError::definitely_not_sent(MetadataError::HandleMissing))?;
    let mut stream = timeout(
        METADATA_IO_TIMEOUT,
        net_handle.open_stream(node_id, Alpn::Metadata),
    )
    .await
    .map_err(|_| MetadataError::Backend("timed out opening metadata stream".to_string()))
    .and_then(|result| result.map_err(|error| MetadataError::Backend(error.to_string())))
    .map_err(MetadataRequestError::definitely_not_sent)?;
    write_framed_message(&mut stream, frame_class(&message), &bytes)
        .await
        .map_err(MetadataRequestError::possibly_sent)?;
    stream
        .0
        .finish()
        .map_err(|error| MetadataError::Backend(error.to_string()))
        .map_err(MetadataRequestError::possibly_sent)?;
    let response = read_transport_message(&mut stream)
        .await
        .map_err(MetadataRequestError::possibly_sent)?;
    let result = match response {
        MetadataTransportMessage::ForwardedExport { result: Err(error) } => Err(error),
        MetadataTransportMessage::ForwardedExport { result: Ok(length) } => {
            if length > metadata_body_limit(metadata_bytes) {
                return Err(MetadataRequestError::possibly_sent(MetadataError::Backend(
                    "metadata export body exceeds the protocol limit".to_string(),
                )));
            }
            let bytes = read_stream_body(&mut stream, length)
                .await
                .map_err(MetadataRequestError::possibly_sent)?;
            postcard::from_bytes(&bytes)
                .map_err(|error| MetadataError::Backend(error.to_string()))
                .map_err(MetadataRequestError::possibly_sent)
                .map(Ok)?
        }
        response => {
            return Err(MetadataRequestError::possibly_sent(MetadataError::Backend(
                format!(
                    "unexpected metadata export response: {}",
                    transport_message_kind(&response)
                ),
            )));
        }
    };
    close_stream(&mut stream).await;
    Ok(result)
}

pub(super) async fn write_transport_message(
    stream: &mut BiStream,
    message: &MetadataTransportMessage,
) -> Result<(), MetadataError> {
    let result: Result<Result<(), String>, tokio::time::error::Elapsed> =
        timeout(METADATA_IO_TIMEOUT, write_message(stream, message)).await;
    result
        .map_err(|_| MetadataError::Backend("timed out writing metadata message".to_string()))?
        .map_err(MetadataError::Backend)
}

/// A lost stream with the peer's close reason, which the stream error itself omits.
fn with_close(stream: &BiStream, error: MetadataError) -> MetadataError {
    match stream.close_reason() {
        Some(reason) => MetadataError::Backend(format!("{error}: {reason}")),
        None => error,
    }
}

async fn write_framed_message(
    stream: &mut BiStream,
    class: u8,
    bytes: &[u8],
) -> Result<(), MetadataError> {
    let result: Result<Result<(), String>, tokio::time::error::Elapsed> = timeout(
        METADATA_IO_TIMEOUT,
        write_encoded_message(stream, class, bytes),
    )
    .await;
    result
        .map_err(|_| MetadataError::Backend("timed out writing metadata message".to_string()))?
        .map_err(MetadataError::Backend)
}

async fn read_transport_message(
    stream: &mut BiStream,
) -> Result<MetadataTransportMessage, MetadataError> {
    let result: Result<Result<MetadataTransportMessage, String>, tokio::time::error::Elapsed> =
        timeout(METADATA_IO_TIMEOUT, read_message(stream)).await;
    result
        .map_err(|_| MetadataError::Backend("timed out waiting for metadata message".to_string()))?
        .map_err(MetadataError::Backend)
}

pub(super) async fn read_budget<R>(
    reader: &mut R,
    budget: &Arc<tokio::sync::Semaphore>,
) -> Result<(MetadataTransportMessage, tokio::sync::OwnedSemaphorePermit), MetadataError>
where
    R: AsyncRead + Unpin + ?Sized,
{
    timeout(
        METADATA_IO_TIMEOUT,
        read_message_budget(reader, super::super::protocol::MAX_MESSAGE_SIZE, budget),
    )
    .await
    .map_err(|_| MetadataError::Backend("timed out waiting for metadata message".to_string()))?
    .map_err(MetadataError::Backend)
}

async fn read_transport_cap(
    stream: &mut BiStream,
    max_size: usize,
    wait: Duration,
) -> Result<MetadataTransportMessage, MetadataError> {
    let result: Result<Result<MetadataTransportMessage, String>, tokio::time::error::Elapsed> =
        timeout(wait, read_message_cap(&mut stream.1, max_size)).await;
    result
        .map_err(|_| MetadataError::Backend("timed out waiting for metadata message".to_string()))?
        .map_err(MetadataError::Backend)
}

pub(super) async fn write_stream_body(
    stream: &mut BiStream,
    bytes: &[u8],
) -> Result<(), MetadataError> {
    timeout(METADATA_IO_TIMEOUT, async {
        for chunk in bytes.chunks(METADATA_CHUNK_SIZE) {
            stream
                .0
                .write_all(chunk)
                .await
                .map_err(|error| MetadataError::Backend(error.to_string()))?;
        }
        Ok::<(), MetadataError>(())
    })
    .await
    .map_err(|_| MetadataError::Backend("timed out writing metadata body".to_string()))?
}

pub(super) fn metadata_body_limit(metadata_bytes: u64) -> u64 {
    metadata_bytes.saturating_add(METADATA_ENVELOPE_BYTES)
}

async fn read_stream_body(stream: &mut BiStream, length: u64) -> Result<Vec<u8>, MetadataError> {
    let length = usize::try_from(length)
        .map_err(|_| MetadataError::Backend("metadata body length is unsupported".to_string()))?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|_| MetadataError::Backend("metadata body allocation failed".to_string()))?;
    bytes.resize(length, 0);
    timeout(METADATA_IO_TIMEOUT, async {
        for chunk in bytes.chunks_mut(METADATA_CHUNK_SIZE) {
            stream
                .1
                .read_exact(chunk)
                .await
                .map_err(|error| MetadataError::Backend(error.to_string()))?;
        }
        Ok::<Vec<u8>, MetadataError>(bytes)
    })
    .await
    .map_err(|_| MetadataError::Backend("timed out reading metadata body".to_string()))?
}

pub(super) async fn close_stream(stream: &mut BiStream) {
    let _ = stream.0.finish();
    let _ = stream.1.stop(0u32.into());
}

pub(super) fn close_stream_at(stream: &mut BiStream, deadline: tokio::time::Instant) {
    if tokio::time::Instant::now() < deadline {
        let _ = stream.0.finish();
    }
    let _ = stream.1.stop(0u32.into());
}

pub(super) async fn write_message_at(
    stream: &mut BiStream,
    message: &MetadataTransportMessage,
    deadline: tokio::time::Instant,
) -> Result<(), MetadataError> {
    timeout_at(deadline, write_message(stream, message))
        .await
        .map_err(|_| MetadataError::Backend("timed out writing metadata message".to_string()))?
        .map_err(MetadataError::Backend)
}

pub(super) async fn write_body_at(
    stream: &mut BiStream,
    bytes: &[u8],
    deadline: tokio::time::Instant,
) -> Result<(), MetadataError> {
    timeout_at(deadline, async {
        for chunk in bytes.chunks(METADATA_CHUNK_SIZE) {
            stream
                .0
                .write_all(chunk)
                .await
                .map_err(|error| MetadataError::Backend(error.to_string()))?;
        }
        Ok::<(), MetadataError>(())
    })
    .await
    .map_err(|_| MetadataError::Backend("timed out writing metadata body".to_string()))?
}

pub(super) async fn drain_stream_at(
    reader: &mut RecvStream,
    deadline: tokio::time::Instant,
) -> Result<(), MetadataError> {
    timeout_at(deadline, reader.read_to_end(1))
        .await
        .map_err(|_| {
            MetadataError::Backend("timed out draining metadata request stream".to_string())
        })?
        .map(|_| ())
        .map_err(|error| MetadataError::Backend(error.to_string()))
}

pub(super) async fn drain_request_stream(stream: &mut BiStream) -> Result<(), MetadataError> {
    timeout(METADATA_IO_TIMEOUT, stream.1.read_to_end(1))
        .await
        .map_err(|_| {
            MetadataError::Backend("timed out draining metadata request stream".to_string())
        })?
        .map(|_| ())
        .map_err(|error| MetadataError::Backend(error.to_string()))
}

impl MetadataHandle {
    #[tracing::instrument(
        name = "metadata.forward.remote",
        level = "debug",
        skip(self, message),
        fields(
            peer = ?node_id,
            request = transport_message_kind(&message),
            elapsed_ms = field::Empty,
        )
    )]
    pub(crate) async fn request_forwarded_write(
        &self,
        node_id: NodeId,
        message: MetadataTransportMessage,
    ) -> Result<MetadataTransportMessage, MetadataRequestError> {
        let started = Instant::now();
        let span = Span::current();
        let result = send_remote_request(&self.inner, &span, node_id, message).await;
        record_elapsed_ms(&span, "elapsed_ms", started);
        if let Err(error) = &result {
            record_error(&span, &error.to_string());
        }
        result
    }

    /// Sends one request an operation encoded and returns the encoded answer.
    pub(crate) async fn request_frame(
        &self,
        node_id: NodeId,
        frame: &[u8],
    ) -> Result<Vec<u8>, MetadataRequestError> {
        let message = decode_message(frame)
            .map_err(|error| MetadataError::InvalidInput(error.to_string()))
            .map_err(MetadataRequestError::definitely_not_sent)?;
        let answer = match message {
            // An export answers with a header and a separate body, read here as one answer.
            MetadataTransportMessage::ForwardExportDocument { .. }
            | MetadataTransportMessage::ForwardExportProfile { .. } => {
                let result = send_export_request(&self.inner, node_id, message).await?;
                MetadataTransportMessage::ExportAnswer {
                    result: result.map(Box::new),
                }
            }
            message => self.request_forwarded_write(node_id, message).await?,
        };
        #[cfg(feature = "test-hooks")]
        if matches!(answer, MetadataTransportMessage::SyncMirrorCreated) && self.take_dropped_ack()
        {
            return Err(MetadataRequestError::possibly_sent(MetadataError::Backend(
                "sync mirror request timed out".to_string(),
            )));
        }
        encode_message(&answer)
            .map_err(MetadataError::Backend)
            .map_err(MetadataRequestError::possibly_sent)
    }
}

async fn send_remote_request(
    inner: &MetadataInner,
    span: &Span,
    node_id: NodeId,
    message: MetadataTransportMessage,
) -> Result<MetadataTransportMessage, MetadataRequestError> {
    let Some(net_handle) = inner.net_handle.clone() else {
        record_error(span, "metadata net handle missing");
        return Err(MetadataRequestError::definitely_not_sent(
            MetadataError::HandleMissing,
        ));
    };

    send_request(&net_handle, node_id, message).await
}

impl MetadataHandle {
    #[tracing::instrument(
        name = "metadata.query.remote",
        level = "debug",
        skip(self, auth_token, sparql),
        fields(
            peer = ?node_id,
            query_len = sparql.len() as u64,
            graph_filter_count = graph_iris.as_ref().map_or(0, Vec::len) as u64,
            elapsed_ms = field::Empty,
            result = field::Empty,
            row_count = field::Empty,
            triple_count = field::Empty,
        )
        )]
    pub(crate) async fn query_remote_graphs(
        &self,
        node_id: NodeId,
        auth_token: Option<AuthToken>,
        graph_iris: Option<Vec<String>>,
        sparql: String,
    ) -> Result<MetadataQueryResults, MetadataReadError> {
        let started = Instant::now();
        let span = Span::current();
        let result = match send_remote_request(
            &self.inner,
            &span,
            node_id,
            MetadataTransportMessage::QueryGraphs {
                auth_token,
                graph_iris,
                sparql,
            },
        )
        .await
        .map_err(|_| MetadataReadError::Unavailable)?
        {
            MetadataTransportMessage::QueryResults { result } => result,
            _ => Err(MetadataReadError::Unavailable),
        };
        record_elapsed_ms(&span, "elapsed_ms", started);
        match &result {
            Ok(results) => {
                span.record("result", results.kind());
                record_query_counts(&span, results);
            }
            Err(error) => record_error(&span, &format!("{error:?}")),
        }
        result
    }

    #[tracing::instrument(
        name = "metadata.search.remote",
        level = "debug",
        skip(self, auth_token, query),
        fields(
            peer = ?node_id,
            query_len = query.len() as u64,
            limit = limit as u64,
            graph_filter_count = graph_iris.as_ref().map_or(0, Vec::len) as u64,
            elapsed_ms = field::Empty,
            result = field::Empty,
            hit_count = field::Empty,
        )
    )]
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn search_remote_graphs(
        &self,
        node_id: NodeId,
        auth_token: Option<AuthToken>,
        graph_iris: Option<Vec<String>>,
        query: String,
        limit: usize,
        group_id: Option<GroupId>,
    ) -> Result<Vec<MetadataSearchHit>, MetadataReadError> {
        self.search_remote(
            node_id, auth_token, graph_iris, query, limit, group_id, None,
        )
        .await
    }

    /// Asks one node which readable documents its reverse index lists for the identifier.
    pub(crate) async fn request_data_links(
        &self,
        node_id: NodeId,
        auth_token: Option<AuthToken>,
        request: crate::metadata::api::links::types::LinkNodeRequest,
    ) -> Result<crate::metadata::api::links::types::LinkNodePage, MetadataReadError> {
        match send_remote_request(
            &self.inner,
            &Span::current(),
            node_id,
            MetadataTransportMessage::DataLinks {
                auth_token,
                request: Box::new(request),
            },
        )
        .await
        .map_err(|_| MetadataReadError::Unavailable)?
        {
            MetadataTransportMessage::DataLinkResults { result } => result.map(|page| *page),
            _ => Err(MetadataReadError::Unavailable),
        }
    }

    pub(crate) async fn request_set_preview(
        &self,
        node_id: NodeId,
        auth_token: Option<AuthToken>,
        (set_id, limit): (Ulid, usize),
    ) -> Result<crate::metadata::api::links::types::SetPreviewPage, MetadataReadError> {
        let request = MetadataTransportMessage::SetPreview {
            auth_token,
            set_id,
            limit,
        };
        match send_remote_request(&self.inner, &Span::current(), node_id, request)
            .await
            .map_err(|_| MetadataReadError::Unavailable)?
        {
            MetadataTransportMessage::SetPreviewResults { result } => result.map(|page| *page),
            _ => Err(MetadataReadError::Unavailable),
        }
    }

    /// Hands the management node `node_id` one chunk of a plugin artifact to keep; `true` once
    /// it holds the whole, checked artifact.
    pub(crate) async fn request_store_artifact(
        &self,
        node_id: NodeId,
        chunk: aruna_core::plugins::Chunk,
    ) -> Result<bool, MetadataReadError> {
        let request = MetadataTransportMessage::StoreArtifact { chunk };
        match send_remote_request(&self.inner, &Span::current(), node_id, request)
            .await
            .map_err(|_| MetadataReadError::Unavailable)?
        {
            MetadataTransportMessage::ArtifactStored { result } => result,
            _ => Err(MetadataReadError::Unavailable),
        }
    }

    pub(crate) async fn request_identifier_lookup(
        &self,
        node_id: NodeId,
        auth_token: Option<AuthToken>,
        kind: aruna_core::structs::secondary_id::SecondaryIdKind,
        value: String,
        endpoint: Option<String>,
    ) -> Result<Vec<crate::metadata::secondary_ids::IdentifierMatch>, MetadataReadError> {
        let span = Span::current();
        match send_remote_request(
            &self.inner,
            &span,
            node_id,
            MetadataTransportMessage::LookupIdentifier {
                auth_token,
                kind,
                value,
                endpoint,
            },
        )
        .await
        .map_err(|_| MetadataReadError::Unavailable)?
        {
            MetadataTransportMessage::IdentifierMatches { result } => result,
            _ => Err(MetadataReadError::Unavailable),
        }
    }

    /// Asks a range coordinator about releases or release files in its ranges.
    pub async fn request_release_holder(
        &self,
        node_id: NodeId,
        auth_token: Option<AuthToken>,
        bucket: String,
        ask: HolderAsk,
    ) -> Result<HolderAnswer, MetadataReadError> {
        let span = Span::current();
        match send_remote_request(
            &self.inner,
            &span,
            node_id,
            MetadataTransportMessage::ReleaseHolder {
                auth_token,
                bucket,
                ask,
            },
        )
        .await
        .map_err(|_| MetadataReadError::Unavailable)?
        {
            MetadataTransportMessage::ReleaseHolderResults { result } => result,
            _ => Err(MetadataReadError::Unavailable),
        }
    }

    #[tracing::instrument(
        name = "metadata.bucket_search.remote",
        level = "debug",
        skip(self, auth_token, query),
        fields(
            peer = ?node_id,
            query_len = query.len() as u64,
            limit = limit as u64,
            elapsed_ms = field::Empty,
            result = field::Empty,
            hit_count = field::Empty,
        )
    )]
    pub(crate) async fn request_bucket_search(
        &self,
        node_id: NodeId,
        auth_token: Option<AuthToken>,
        (query, key, extras, limit): (String, Option<String>, Option<PolicyRequestExtras>, usize),
        (group_id, database): (Option<GroupId>, bool),
        start_after: Option<Vec<u8>>,
    ) -> Result<BucketPage, MetadataReadError> {
        let started = Instant::now();
        let span = Span::current();
        let result = match send_remote_request(
            &self.inner,
            &span,
            node_id,
            MetadataTransportMessage::SearchBuckets {
                auth_token,
                query,
                key,
                extras,
                limit,
                group_id,
                start_after,
                database,
            },
        )
        .await
        .map_err(|_| MetadataReadError::Unavailable)?
        {
            MetadataTransportMessage::BucketSearchResults { result } => result,
            _ => Err(MetadataReadError::Unavailable),
        };
        record_elapsed_ms(&span, "elapsed_ms", started);
        match &result {
            Ok(page) => {
                span.record("result", "ok");
                span.record("hit_count", page.hits.len() as u64);
            }
            Err(error) => record_error(&span, &format!("{error:?}")),
        }
        result
    }

    #[tracing::instrument(
        name = "metadata.object_search.remote",
        level = "debug",
        skip(self, auth_token, query, start_after),
        fields(
            peer = ?node_id,
            query_len = query.len() as u64,
            limit = limit as u64,
            elapsed_ms = field::Empty,
            result = field::Empty,
            hit_count = field::Empty,
        )
    )]
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn request_object_search(
        &self,
        node_id: NodeId,
        auth_token: Option<AuthToken>,
        query: String,
        key_match: ObjectKeyMatch,
        bucket: Option<String>,
        limit: usize,
        start_after: Option<Vec<u8>>,
        as_of: SystemTime,
    ) -> Result<SearchNodePage, MetadataReadError> {
        let started = Instant::now();
        let span = Span::current();
        let result = match send_remote_request(
            &self.inner,
            &span,
            node_id,
            MetadataTransportMessage::SearchObjects {
                auth_token,
                query,
                key_match,
                bucket,
                limit,
                start_after,
                as_of,
            },
        )
        .await
        .map_err(|_| MetadataReadError::Unavailable)?
        {
            MetadataTransportMessage::ObjectSearchResults { result } => result,
            _ => Err(MetadataReadError::Unavailable),
        };
        record_elapsed_ms(&span, "elapsed_ms", started);
        match &result {
            Ok(page) => {
                span.record("result", "ok");
                span.record("hit_count", page.hits.len() as u64);
            }
            Err(error) => record_error(&span, &format!("{error:?}")),
        }
        result
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn search_remote_filtered(
        &self,
        node_id: NodeId,
        auth_token: Option<AuthToken>,
        graph_iris: Option<Vec<String>>,
        query: String,
        limit: usize,
        predicate_iri: String,
        object_iri: String,
        group_id: Option<GroupId>,
    ) -> Result<Vec<MetadataSearchHit>, MetadataReadError> {
        self.search_remote(
            node_id,
            auth_token,
            graph_iris,
            query,
            limit,
            group_id,
            Some((predicate_iri, object_iri)),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn search_remote(
        &self,
        node_id: NodeId,
        auth_token: Option<AuthToken>,
        graph_iris: Option<Vec<String>>,
        query: String,
        limit: usize,
        group_id: Option<GroupId>,
        iri_filter: Option<(String, String)>,
    ) -> Result<Vec<MetadataSearchHit>, MetadataReadError> {
        let started = Instant::now();
        let span = Span::current();
        let message = match iri_filter {
            Some((predicate_iri, object_iri)) => MetadataTransportMessage::FilteredSearchGraphs {
                auth_token,
                graph_iris,
                query,
                limit,
                predicate_iri,
                object_iri,
                group_id,
            },
            None => MetadataTransportMessage::SearchGraphs {
                auth_token,
                graph_iris,
                query,
                limit,
                group_id,
            },
        };
        let result = match send_remote_request(&self.inner, &span, node_id, message)
            .await
            .map_err(|_| MetadataReadError::Unavailable)?
        {
            MetadataTransportMessage::SearchResults { result } => result,
            _ => Err(MetadataReadError::Unavailable),
        };
        record_elapsed_ms(&span, "elapsed_ms", started);
        match &result {
            Ok(hits) => {
                span.record("result", "ok");
                span.record("hit_count", hits.len() as u64);
            }
            Err(error) => record_error(&span, &format!("{error:?}")),
        }
        result
    }

    pub(crate) async fn request_remote_preflight(
        &self,
        node_id: NodeId,
        auth_token: Option<AuthToken>,
        request: super::super::api::preflight_types::ReferenceNodeRequest,
    ) -> Result<super::super::api::preflight_types::ReferenceNodeExecution, MetadataReadError> {
        match send_remote_request(
            &self.inner,
            &Span::current(),
            node_id,
            MetadataTransportMessage::ReferencePreflight {
                auth_token,
                request: Box::new(request),
            },
        )
        .await
        .map_err(|_| MetadataReadError::Unavailable)?
        {
            MetadataTransportMessage::ReferencePreflightResults { result } => {
                result.map(|result| *result)
            }
            _ => Err(MetadataReadError::Unavailable),
        }
    }
}
