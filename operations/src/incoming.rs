use std::sync::Arc;
use std::time::Duration;

use crate::driver::{DriverContext, drive};
use crate::metadata::MetadataHandle;
use crate::process_placements::{PlacementConfig, ProcessPlacementsOperation};
use crate::replication::incoming_version_replication::IncomingVersionReplicationOperation;
use crate::replication::protocol::VersionReplicationMessage;
use aruna_core::alpn::Alpn;
use aruna_core::effects::BlobEffect;
use aruna_core::events::{BlobEvent, Event};
use aruna_core::id::NodeId;
use aruna_net::InboundEventHandler;
use aruna_net::streams::BiStream;
use async_trait::async_trait;
use tokio::time::sleep;
use tracing::{Instrument, debug, error, info_span, trace, warn};

const METADATA_IROKLE_MAINTENANCE_ATTEMPTS: usize = 3;
const METADATA_IROKLE_MAINTENANCE_RETRY_AFTER: Duration = Duration::from_millis(500);
const METADATA_IROKLE_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(5);

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
    let metadata_handle = context.metadata_handle.clone();

    net_handle.set_inbound_handler(Arc::new(OperationsInboundHandler::new(context)));
    if let Some(metadata_handle) = metadata_handle {
        schedule_periodic_metadata_irokle_maintenance(metadata_handle);
    }
}

#[async_trait]
impl InboundEventHandler for OperationsInboundHandler {
    #[tracing::instrument(
        name = "operations.inbound.stream",
        level = "debug",
        skip(self, stream),
        fields(peer = %node_id, alpn = ?alpn)
    )]
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
                                        debug!(
                                            peer = %node_id,
                                            stream_id = %stream_id,
                                            bucket = %manifest.bucket,
                                            key = %manifest.key,
                                            version_id = %manifest.version_id,
                                            kind = ?manifest.kind,
                                            "Received inbound version replication manifest"
                                        );
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
                Alpn::Irokle => {
                    let Some(net_handle) = self.context.net_handle.as_ref() else {
                        warn!(node_id = %node_id, "Dropping inbound irokle stream without net handle");
                        return;
                    };
                    match net_handle.handle_irokle_stream(stream, node_id).await {
                        Ok(applied) => {
                            debug!(node_id = %node_id, applied, "Reconciled inbound Irokle document events");
                            if applied > 0 {
                                let operation = ProcessPlacementsOperation::new(
                                    PlacementConfig {
                                        realm_id: *net_handle.realm_id(),
                                        local_node_id: net_handle.node_id(),
                                    },
                                );
                                if let Err(error) = drive(operation, self.context.as_ref()).await {
                                    error!(error = ?error, "Failed to process pending topic placements after Irokle reconciliation");
                                }
                            }
                            if let Some(metadata_handle) = self.context.metadata_handle.as_ref() {
                                run_metadata_irokle_maintenance(metadata_handle, "inbound", 0)
                                    .await;
                                schedule_metadata_irokle_maintenance(metadata_handle.clone());
                            }
                        }
                        Err(err) => error!(error = ?err, "Failed to process inbound irokle stream"),
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
                Alpn::Dht => {
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

fn schedule_periodic_metadata_irokle_maintenance(metadata_handle: MetadataHandle) {
    tokio::spawn(async move {
        let mut cycle = 0usize;
        loop {
            sleep(METADATA_IROKLE_MAINTENANCE_INTERVAL).await;
            cycle = cycle.saturating_add(1);
            run_metadata_irokle_maintenance(&metadata_handle, "periodic", cycle).await;
        }
    });
}

fn schedule_metadata_irokle_maintenance(metadata_handle: MetadataHandle) {
    tokio::spawn(async move {
        for attempt in 1..=METADATA_IROKLE_MAINTENANCE_ATTEMPTS {
            sleep(METADATA_IROKLE_MAINTENANCE_RETRY_AFTER).await;
            run_metadata_irokle_maintenance(&metadata_handle, "delayed", attempt).await;
        }
    });
}

async fn run_metadata_irokle_maintenance(
    metadata_handle: &MetadataHandle,
    source: &'static str,
    attempt: usize,
) {
    if let Err(error) = metadata_handle.reconcile_irokle().await {
        warn!(
            source,
            attempt,
            error = ?error,
            "Craqle Irokle reconciliation failed"
        );
    }
    match metadata_handle.prune_deleted_graphs().await {
        Ok(pruned) if pruned > 0 => {
            debug!(source, attempt, pruned, "Metadata graph prune completed")
        }
        Ok(_) => {}
        Err(error) => warn!(
            source,
            attempt,
            error = ?error,
            "Metadata graph prune failed"
        ),
    }
}
