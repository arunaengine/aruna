use std::sync::Arc;

use crate::driver::{DriverContext, drive};
use crate::incoming_automerge::IncomingAutomergeOperation;
use crate::incoming_gossip::IncomingGossipOperation;
use crate::replication::incoming_bao::IncomingBaoOperation;
use crate::telemetry::extract_trace_context;
use aruna_core::alpn::Alpn;
use aruna_core::automerge::AutomergeAnnouncementEnvelope;
use aruna_core::id::{NodeId, TopicId};
use aruna_net::InboundEventHandler;
use aruna_net::streams::BiStream;
use async_trait::async_trait;
use opentelemetry::trace::TraceContextExt;
use tracing::{Instrument, error, field, info_span, trace, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

#[derive(Debug)]
struct OperationsInboundHandler {
    context: Arc<DriverContext>,
}

impl OperationsInboundHandler {
    fn new(context: Arc<DriverContext>) -> Self {
        Self { context }
    }
}

pub fn initialize_net_incoming(context: Arc<DriverContext>) {
    let Some(net_handle) = context.net_handle.clone() else {
        warn!("Cannot initialize inbound handling without net handle");
        return;
    };

    net_handle.set_inbound_handler(Arc::new(OperationsInboundHandler::new(context)));
}

#[async_trait]
impl InboundEventHandler for OperationsInboundHandler {
    async fn handle_gossip_message(&self, topic: TopicId, sender: NodeId, data: Vec<u8>) {
        let envelope = postcard::from_bytes::<AutomergeAnnouncementEnvelope>(&data).ok();
        let span = info_span!(
            "gossip.receive",
            topic = %topic,
            sender = %sender,
            message_id = ?envelope.as_ref().map(|envelope| envelope.message_id),
            trace_id = field::Empty,
        );
        if let Some(envelope) = envelope.as_ref() {
            let _ = span.set_parent(extract_trace_context(&envelope.trace_context));
        }
        let trace_id = span.context().span().span_context().trace_id().to_string();
        span.record("trace_id", field::display(trace_id));

        async move {
            trace!(
                event = "gossip.received",
                topic = %topic,
                sender = %sender,
                message_id = ?envelope.as_ref().map(|envelope| envelope.message_id),
                "Received inbound gossip message"
            );

            let op = IncomingGossipOperation::new(topic, sender, data);
            if let Err(err) = drive(op, self.context.as_ref()).await {
                error!(error = ?err, "Failed to process inbound gossip event");
            }
        }
        .instrument(span)
        .await;
    }

    async fn handle_incoming_stream(&self, alpn: Alpn, stream: BiStream, node_id: NodeId) {
        let span = info_span!("net.incoming_stream", peer = %node_id, alpn = ?alpn);

        async move {
            trace!(event = "stream.received", peer = %node_id, alpn = ?alpn, "Received inbound stream");
            match alpn {
                Alpn::Bao => {
                    if let Some(mut blob_handle) = self.context.blob_handle.clone() {
                        let stream_id = blob_handle.store_connection(stream).await;
                        let op = IncomingBaoOperation::new(stream_id, node_id);
                        if let Err(err) = drive(op, self.context.as_ref()).await {
                            error!(error = ?err, "Failed to process inbound bao stream");
                        }
                    } else {
                        error!("Cannot handle incoming bao stream without blob handle");
                        return;
                    };
                }
                Alpn::Automerge => {
                    let Some(automerge_handle) = self.context.automerge_handle.clone() else {
                        warn!(node_id = %node_id, "Dropping inbound automerge stream without automerge handle");
                        return;
                    };
                    let Some(net_handle) = self.context.net_handle.as_ref() else {
                        warn!(node_id = %node_id, "Dropping inbound automerge stream without net handle");
                        return;
                    };
                    let sync_id = automerge_handle.register_inbound_stream(stream, node_id).await;
                    let op = IncomingAutomergeOperation::new(sync_id, node_id, net_handle.node_id());
                    if let Err(err) = drive(op, self.context.as_ref()).await {
                        error!(error = ?err, "Failed to process inbound automerge stream event");
                    }
                }
                Alpn::Dht | Alpn::Gossip => {
                    warn!(
                        node_id = %node_id,
                        "Ignoring inbound stream for non-stream ALPN"
                    );
                }
            }
        }
        .instrument(span)
        .await;
    }
}
