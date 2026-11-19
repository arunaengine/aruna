//! Serves inbound metadata stream requests from peers: query, search, sync and mirror delete.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::{Duration, Instant};

use aruna_core::NodeId;
use aruna_core::metadata::MetadataError;
use aruna_core::telemetry::record_elapsed_ms;
use aruna_net::streams::BiStream;
use tracing::Span;

use super::MetadataHandle;
use super::transport::{
    close_stream, close_stream_at, drain_request_stream, drain_stream_at, read_budget,
    write_body_at, write_message_at, write_stream_body, write_transport_message,
};
use crate::driver::node_routing;
use crate::driver::{DriverContext, drive};
use crate::forward::transport::reject;
use crate::metadata::message_kind::transport_message_kind;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::metadata::serve::{ServeNode, ServeRequestOperation, needs_view};

impl MetadataHandle {
    /// Reads one request frame from an inbound metadata stream under the shared frame budget
    /// and names it on the current span. Returns the deadline an audit request keeps.
    pub(crate) async fn read_request(
        &self,
        stream: &mut BiStream,
    ) -> Result<InboundRequest, MetadataError> {
        let audit_deadline = tokio::time::Instant::now()
            + Duration::from_secs(super::super::audit::AUDIT_DEADLINE_SECS);
        let (message, budget) = read_budget(&mut stream.1, &self.inner.inbound_frame_bytes).await?;
        Span::current().record("request", transport_message_kind(&message));
        let audit = matches!(&message, MetadataTransportMessage::ForwardAuditPage { .. });
        Ok(InboundRequest {
            message,
            budget,
            audit_deadline,
            audit,
        })
    }

    /// Answers one metadata request from `peer`; requests of other domains are rejected.
    pub(crate) async fn answer_request(
        &self,
        context: &Arc<DriverContext>,
        peer: NodeId,
        metadata_bytes: u64,
        _audit_deadline: tokio::time::Instant,
        message: MetadataTransportMessage,
    ) -> (MetadataTransportMessage, Option<Vec<u8>>) {
        let view = needs_view(&message);
        let serving = ServeNode {
            local: context.node_and_realm(),
            graph: context.graph_handle.is_some(),
            indexed: context.storage_handle.range_indexes().is_some(),
            metadata_bytes,
            here: view.then(|| context.here()),
            routing: view.then(|| node_routing(context)),
        };
        let serve = ServeRequestOperation::new(peer, serving, message);
        match drive(serve, context.as_ref()).await {
            Ok(reply) => reply,
            Err(error) => (reject(error.to_string()), None),
        }
    }
}

/// One request read from an inbound metadata stream.
pub(crate) struct InboundRequest {
    pub(crate) message: MetadataTransportMessage,
    /// Held until the answer is written, so frames in flight stay within the budget.
    pub(crate) budget: tokio::sync::OwnedSemaphorePermit,
    pub(crate) audit_deadline: tokio::time::Instant,
    pub(crate) audit: bool,
}

impl MetadataHandle {
    /// Drains the request, writes the answer and closes the stream. An audit request keeps
    /// the audit deadline for every step.
    pub(crate) async fn write_answer(
        mut stream: BiStream,
        audit_deadline: Option<tokio::time::Instant>,
        response: &MetadataTransportMessage,
        response_body: Option<Vec<u8>>,
    ) -> Result<(), MetadataError> {
        let span = Span::current();
        span.record("response", transport_message_kind(response));
        let drain_started = Instant::now();
        let drain_result = match audit_deadline {
            Some(deadline) => drain_stream_at(&mut stream.1, deadline).await,
            None => drain_request_stream(&mut stream).await,
        };
        if let Err(error) = drain_result {
            if let Some(deadline) = audit_deadline {
                close_stream_at(&mut stream, deadline);
            }
            return Err(error);
        }
        record_elapsed_ms(&span, "drain_ms", drain_started);

        let write_started = Instant::now();
        let response_written = match audit_deadline {
            Some(deadline) => write_message_at(&mut stream, response, deadline)
                .await
                .is_ok(),
            None => write_transport_message(&mut stream, response).await.is_ok(),
        };
        if response_written && let Some(body) = response_body {
            let _ = match audit_deadline {
                Some(deadline) => write_body_at(&mut stream, &body, deadline).await,
                None => write_stream_body(&mut stream, &body).await,
            };
        }
        record_elapsed_ms(&span, "write_ms", write_started);
        match audit_deadline {
            Some(deadline) => close_stream_at(&mut stream, deadline),
            None => close_stream(&mut stream).await,
        }
        Ok(())
    }
}
