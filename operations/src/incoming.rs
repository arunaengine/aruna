use std::sync::Arc;

use crate::driver::{DriverContext, drive};
use crate::incoming_automerge::IncomingAutomergeOperation;
use crate::incoming_gossip::IncomingGossipOperation;
use crate::replication::incoming_version_replication::IncomingVersionReplicationOperation;
use crate::replication::protocol::VersionReplicationMessage;
use aruna_core::alpn::Alpn;
use aruna_core::effects::BlobEffect;
use aruna_core::events::{BlobEvent, Event};
use aruna_core::gossip::TopicMessage;
use aruna_core::id::{NodeId, TopicId};
use aruna_net::InboundEventHandler;
use aruna_net::streams::BiStream;
use async_trait::async_trait;
use tracing::{Instrument, error, field, info_span, trace, warn};

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
        let message = postcard::from_bytes::<TopicMessage>(&data).ok();
        let span = info_span!(
            "gossip.receive",
            topic = %topic,
            sender = %sender,
            message_id = ?message.as_ref().map(|message| message.message_id),
            trace_id = field::Empty,
        );
        if let Some(message) = message.as_ref()
            && let Some(trace_id) = message.trace_id.as_ref()
        {
            span.record("trace_id", field::display(trace_id));
        }

        async move {
            trace!(
                event = "gossip.received",
                topic = %topic,
                sender = %sender,
                message_id = ?message.as_ref().map(|message| message.message_id),
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
                        let Some(net_handle) = self.context.net_handle.as_ref() else {
                            error!(peer = %node_id, "Cannot handle incoming bao stream without net handle");
                            return;
                        };
                        let first_event = blob_handle
                            .send_blob_effect(BlobEffect::ReadMessage { stream_id })
                            .await;

                        match first_event {
                            Event::Blob(BlobEvent::MessageReceived { payload, .. }) => {
                                match VersionReplicationMessage::from_bytes(&payload) {
                                    Ok(VersionReplicationMessage::VersionManifest(manifest)) => {
                                        let op = IncomingVersionReplicationOperation::new(
                                            stream_id,
                                            net_handle.node_id(),
                                            *net_handle.realm_id(),
                                            manifest,
                                        );
                                        if let Err(err) = drive(op, self.context.as_ref()).await {
                                            error!(error = ?err, "Failed to process inbound version replication stream");
                                        }
                                    }
                                    _ => {
                                        error!(
                                            peer = %node_id,
                                            stream_id = %stream_id,
                                            "Unsupported inbound bao payload; legacy raw bao replication is no longer supported"
                                        );
                                        let close_event = blob_handle
                                            .send_blob_effect(BlobEffect::CloseConnection { stream_id })
                                            .await;
                                        if let Event::Blob(BlobEvent::Error(err)) = close_event {
                                            error!(error = ?err, "Failed to close unsupported inbound bao stream");
                                        }
                                    }
                                }
                            }
                            Event::Blob(BlobEvent::Error(err)) => {
                                error!(error = ?err, "Failed to read initial inbound bao payload");
                            }
                            other => {
                                error!(event = ?other, "Unexpected first event for inbound bao stream");
                            }
                        }
                    } else {
                        error!("Cannot handle incoming bao stream without blob handle");
                    }
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
                Alpn::Metadata => {
                    let Some(metadata_handle) = self.context.metadata_handle.clone() else {
                        warn!(node_id = %node_id, "Dropping inbound metadata stream without metadata handle");
                        return;
                    };
                    if let Err(err) = metadata_handle.handle_inbound_stream(stream, node_id).await {
                        error!(error = ?err, "Failed to process inbound metadata stream");
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
