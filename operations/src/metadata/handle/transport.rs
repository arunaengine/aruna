use std::sync::Arc;
use std::time::{Instant, SystemTime};

use aruna_core::NodeId;
use aruna_core::alpn::Alpn;
use aruna_core::metadata::{MetadataError, MetadataQueryResults, MetadataSearchHit};
use aruna_core::structs::SyncRelationship;
use aruna_core::telemetry::record_elapsed_ms;
use aruna_core::types::GroupId;
use aruna_net::NetHandle;
use aruna_net::streams::{BiStream, RecvStream};
use tokio::io::AsyncRead;
use tokio::time::{timeout, timeout_at};
use tracing::{Span, field};
use ulid::Ulid;

use super::effects::{record_error, record_query_counts};
use super::{
    METADATA_CHUNK_SIZE, METADATA_ENVELOPE_BYTES, METADATA_IO_TIMEOUT, MetadataHandle,
    MetadataInner, SYNC_MIRROR_REQUEST_TIMEOUT,
};
use crate::auth::request_policy::PolicyRequestExtras;
use crate::metadata::protocol::{
    MetadataAuthToken, MetadataReadError, MetadataTransportMessage, encode_message, frame_class,
    read_message, read_message_budget, read_message_cap, response_cap, write_encoded_message,
    write_message,
};
use crate::s3::search_buckets::BucketSearchHit;
use crate::s3::search_objects::{ObjectKeyMatch, ObjectSearchNodePage};

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
        .map_err(MetadataRequestError::possibly_sent)?;
    record_elapsed_ms(&span, "write_ms", write_started);

    let finish_started = Instant::now();
    stream
        .0
        .finish()
        .map_err(|error| MetadataError::Backend(error.to_string()))
        .map_err(MetadataRequestError::possibly_sent)?;
    record_elapsed_ms(&span, "finish_ms", finish_started);

    let read_started = Instant::now();
    let response = read_transport_cap(&mut stream, max_response_size)
        .await
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
    Result<super::super::api::ExportMetadataRoCrateResult, MetadataReadError>,
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
) -> Result<MetadataTransportMessage, MetadataError> {
    let result: Result<Result<MetadataTransportMessage, String>, tokio::time::error::Elapsed> =
        timeout(
            METADATA_IO_TIMEOUT,
            read_message_cap(&mut stream.1, max_size),
        )
        .await;
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

pub(crate) fn transport_message_kind(message: &MetadataTransportMessage) -> &'static str {
    match message {
        MetadataTransportMessage::QueryGraphs { .. } => "query_graphs",
        MetadataTransportMessage::QueryResults { .. } => "query_results",
        MetadataTransportMessage::SearchGraphs { .. } => "search_graphs",
        MetadataTransportMessage::FilteredSearchGraphs { .. } => "filtered_search_graphs",
        MetadataTransportMessage::SearchResults { .. } => "search_results",
        MetadataTransportMessage::SearchBuckets { .. } => "search_buckets",
        MetadataTransportMessage::BucketSearchResults { .. } => "bucket_search_results",
        MetadataTransportMessage::SearchObjects { .. } => "search_objects",
        MetadataTransportMessage::ObjectSearchResults { .. } => "object_search_results",
        MetadataTransportMessage::CreateSyncMirror { .. } => "create_sync_mirror",
        MetadataTransportMessage::DeleteSyncMirror { .. } => "delete_sync_mirror",
        MetadataTransportMessage::SyncMirrorCreated => "sync_mirror_created",
        MetadataTransportMessage::SyncMirrorDeleted => "sync_mirror_deleted",
        MetadataTransportMessage::ForwardCreateDocument { .. } => "forward_create_document",
        MetadataTransportMessage::ForwardUpdateDocument { .. } => "forward_update_document",
        MetadataTransportMessage::ForwardDeleteDocument { .. } => "forward_delete_document",
        MetadataTransportMessage::ForwardReadDocument { .. } => "forward_read_document",
        MetadataTransportMessage::ForwardedRecord { .. } => "forwarded_record",
        MetadataTransportMessage::ForwardedRead { .. } => "forwarded_read",
        MetadataTransportMessage::ForwardPathLookup { .. } => "forward_path_lookup",
        MetadataTransportMessage::ForwardedPathLookup { .. } => "forwarded_path_lookup",
        MetadataTransportMessage::ForwardPathResolution { .. } => "forward_path_resolution",
        MetadataTransportMessage::ForwardedPathResolution { .. } => "forwarded_path_resolution",
        MetadataTransportMessage::ForwardedWriteDenied { .. } => "forwarded_write_denied",
        MetadataTransportMessage::ForwardedWriteNotFound => "forwarded_write_not_found",
        MetadataTransportMessage::ForwardedWriteUnavailable => "forwarded_write_unavailable",
        MetadataTransportMessage::ForwardedDelete => "forwarded_delete",
        MetadataTransportMessage::ForwardExportDocument { .. } => "forward_export_document",
        MetadataTransportMessage::ForwardExportProfile { .. } => "forward_export_profile",
        MetadataTransportMessage::ForwardedExport { .. } => "forwarded_export",
        MetadataTransportMessage::QueryDocument { .. } => "query_document",
        MetadataTransportMessage::DocumentQueryResults { .. } => "document_query_results",
        MetadataTransportMessage::Reject(_) => "reject",
        MetadataTransportMessage::ForwardedUpdateInvalidInput { .. } => {
            "forwarded_update_invalid_input"
        }
        MetadataTransportMessage::ForwardAuditPage { .. } => "forward_audit_page",
        MetadataTransportMessage::ForwardedAuditPage { .. } => "forwarded_audit_page",
        MetadataTransportMessage::ForwardTokenRevocation { .. } => "forward_token_revocation",
        MetadataTransportMessage::ForwardedTokenRevoked => "forwarded_token_revoked",
        MetadataTransportMessage::ForwardedTokenRevocationCapacity => {
            "forwarded_token_revocation_capacity"
        }
        MetadataTransportMessage::ForwardedMetadataHistoryCapacity => {
            "forwarded_metadata_history_capacity"
        }
        MetadataTransportMessage::ForwardPersistentId { .. } => "forward_persistent_id",
        MetadataTransportMessage::ForwardedPersistentId { .. } => "forwarded_persistent_id",
        MetadataTransportMessage::ForwardPlacementPolicy { .. } => "forward_placement_policy",
        MetadataTransportMessage::ForwardedPlacementPolicy { .. } => "forwarded_placement_policy",
        MetadataTransportMessage::ForwardCreatePlacementPolicy { .. } => {
            "forward_create_placement_policy"
        }
        MetadataTransportMessage::ForwardedPlacementPolicyCreated { .. } => {
            "forwarded_placement_policy_created"
        }
        MetadataTransportMessage::ForwardJobRecord { .. } => "forward_job_record",
        MetadataTransportMessage::ForwardedJobRecord { .. } => "forwarded_job_record",
        MetadataTransportMessage::ForwardJobRecordPage { .. } => "forward_job_record_page",
        MetadataTransportMessage::ForwardedJobRecordPage { .. } => "forwarded_job_record_page",
        MetadataTransportMessage::ForwardLaunchOffer { .. } => "forward_launch_offer",
        MetadataTransportMessage::ForwardedLaunchOffer { .. } => "forwarded_launch_offer",
        MetadataTransportMessage::ForwardJobSubmission { .. } => "forward_job_submission",
        MetadataTransportMessage::ForwardedJobSubmission { .. } => "forwarded_job_submission",
        MetadataTransportMessage::ForwardedProfileValidation { .. } => {
            "forwarded_profile_validation"
        }
        MetadataTransportMessage::ForwardProfileValidationStatus { .. } => {
            "forward_profile_validation_status"
        }
        MetadataTransportMessage::ForwardedProfileValidationStatus { .. } => {
            "forwarded_profile_validation_status"
        }
        MetadataTransportMessage::ReferencePreflight { .. } => "reference_preflight",
        MetadataTransportMessage::ReferencePreflightResults { .. } => "reference_preflight_results",
        MetadataTransportMessage::ForwardAdminEvent { .. } => "forward_admin_event",
        MetadataTransportMessage::ForwardedAdminEventQueued => "forwarded_admin_event_queued",
        MetadataTransportMessage::ForwardGroupCreate { .. } => "forward_group_create",
        MetadataTransportMessage::ForwardedGroupCreated { .. } => "forwarded_group_created",
        MetadataTransportMessage::ForwardSyncPull { .. } => "forward_sync_pull",
        MetadataTransportMessage::ForwardedSyncPull { .. } => "forwarded_sync_pull",
        MetadataTransportMessage::ForwardListVersions { .. } => "forward_list_versions",
        MetadataTransportMessage::ForwardedVersions { .. } => "forwarded_versions",
        MetadataTransportMessage::ForwardCreateBucket { .. } => "forward_create_bucket",
        MetadataTransportMessage::ForwardedBucketCreated { .. } => "forwarded_bucket_created",
        MetadataTransportMessage::FetchRealmDocuments { .. } => "fetch_realm_documents",
        MetadataTransportMessage::FetchedRealmDocuments { .. } => "fetched_realm_documents",
        MetadataTransportMessage::FetchGraphState { .. } => "fetch_graph_state",
        MetadataTransportMessage::FetchedGraphState { .. } => "fetched_graph_state",
        MetadataTransportMessage::ForwardApplyBatch { .. } => "forward_apply_batch",
        MetadataTransportMessage::ForwardedApplyBatch { .. } => "forwarded_apply_batch",
        MetadataTransportMessage::ForwardedGroupCreateConflict { .. } => {
            "forwarded_group_create_conflict"
        }
    }
}

impl MetadataHandle {
    pub(crate) async fn request_export(
        &self,
        node_id: NodeId,
        message: MetadataTransportMessage,
    ) -> Result<
        Result<super::super::api::ExportMetadataRoCrateResult, MetadataReadError>,
        MetadataRequestError,
    > {
        send_export_request(&self.inner, node_id, message).await
    }

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
    pub async fn query_remote_graphs(
        &self,
        node_id: NodeId,
        auth_token: Option<MetadataAuthToken>,
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

    pub(crate) async fn request_document_query(
        &self,
        node_id: NodeId,
        auth_token: Option<MetadataAuthToken>,
        config_digest: [u8; 32],
        document_id: Ulid,
        sparql: String,
    ) -> Result<MetadataQueryResults, MetadataReadError> {
        match send_remote_request(
            &self.inner,
            &Span::current(),
            node_id,
            MetadataTransportMessage::QueryDocument {
                auth_token,
                config_digest,
                document_id,
                sparql,
            },
        )
        .await
        .map_err(|_| MetadataReadError::Unavailable)?
        {
            MetadataTransportMessage::DocumentQueryResults { result } => result,
            _ => Err(MetadataReadError::Unavailable),
        }
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
    pub async fn search_remote_graphs(
        &self,
        node_id: NodeId,
        auth_token: Option<MetadataAuthToken>,
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
    pub async fn request_bucket_search(
        &self,
        node_id: NodeId,
        auth_token: Option<MetadataAuthToken>,
        query: String,
        limit: usize,
    ) -> Result<Vec<BucketSearchHit>, MetadataReadError> {
        let started = Instant::now();
        let span = Span::current();
        let result = match send_remote_request(
            &self.inner,
            &span,
            node_id,
            MetadataTransportMessage::SearchBuckets {
                auth_token,
                query,
                limit,
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
            Ok(hits) => {
                span.record("result", "ok");
                span.record("hit_count", hits.len() as u64);
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
    pub async fn request_object_search(
        &self,
        node_id: NodeId,
        auth_token: Option<MetadataAuthToken>,
        query: String,
        key_match: ObjectKeyMatch,
        bucket: Option<String>,
        limit: usize,
        start_after: Option<Vec<u8>>,
        as_of: SystemTime,
    ) -> Result<ObjectSearchNodePage, MetadataReadError> {
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

    pub async fn request_sync_create(
        &self,
        node_id: NodeId,
        auth_token: Option<MetadataAuthToken>,
        source_group_id: GroupId,
        relationship: SyncRelationship,
        extras: PolicyRequestExtras,
    ) -> Result<(), MetadataError> {
        match with_sync_timeout(send_remote_request(
            &self.inner,
            &Span::current(),
            node_id,
            MetadataTransportMessage::CreateSyncMirror {
                auth_token,
                source_group_id,
                relationship: Box::new(relationship),
                extras,
            },
        ))
        .await?
        {
            MetadataTransportMessage::SyncMirrorCreated => Ok(()),
            MetadataTransportMessage::Reject(error) => Err(MetadataError::Backend(error)),
            other => Err(MetadataError::Backend(format!(
                "unexpected sync mirror response: {other:?}"
            ))),
        }
    }

    pub async fn request_sync_delete(
        &self,
        node_id: NodeId,
        auth_token: Option<MetadataAuthToken>,
        relationship: SyncRelationship,
        extras: PolicyRequestExtras,
    ) -> Result<(), MetadataError> {
        match with_sync_timeout(send_remote_request(
            &self.inner,
            &Span::current(),
            node_id,
            MetadataTransportMessage::DeleteSyncMirror {
                auth_token,
                relationship: Box::new(relationship),
                extras,
            },
        ))
        .await?
        {
            MetadataTransportMessage::SyncMirrorDeleted => Ok(()),
            MetadataTransportMessage::Reject(error) => Err(MetadataError::Backend(error)),
            other => Err(MetadataError::Backend(format!(
                "unexpected sync mirror response: {other:?}"
            ))),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn search_remote_filtered(
        &self,
        node_id: NodeId,
        auth_token: Option<MetadataAuthToken>,
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
        auth_token: Option<MetadataAuthToken>,
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

    pub async fn request_remote_preflight(
        &self,
        node_id: NodeId,
        auth_token: Option<MetadataAuthToken>,
        request: super::super::api::MetadataReferencePreflightNodeRequest,
    ) -> Result<super::super::api::MetadataReferencePreflightNodeExecution, MetadataReadError> {
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

pub(super) async fn with_sync_timeout<T>(
    request: impl std::future::Future<Output = Result<T, MetadataRequestError>>,
) -> Result<T, MetadataError> {
    timeout(SYNC_MIRROR_REQUEST_TIMEOUT, request)
        .await
        .map_err(|_| MetadataError::Backend("sync mirror request timed out".to_string()))?
        .map_err(MetadataRequestError::into_metadata_error)
}
