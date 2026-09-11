use super::*;

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
    write_encoded_transport_message(&mut stream, frame_class(&message), &bytes)
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
) -> Result<Result<super::super::api::ExportMetadataRoCrateResult, MetadataReadError>, MetadataRequestError>
{
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
    write_encoded_transport_message(&mut stream, frame_class(&message), &bytes)
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
