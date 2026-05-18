use aruna_core::NodeId;
use aruna_core::alpn::Alpn;
use iroh::Endpoint;
use iroh::endpoint::Connection;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span, field, info_span, trace, warn};

use crate::error::{NetError, Result};
use crate::telemetry::{
    duration_ms, record_duration_ms, warn_if_slow_iroh_phase, warn_if_slow_iroh_request,
};

const STREAM_IO_TIMEOUT: Duration = Duration::from_secs(10);

pub use iroh::endpoint::{RecvStream, SendStream};

pub type BiStream = (SendStream, RecvStream);

pub struct StreamsService {
    endpoint: Endpoint,
    #[allow(dead_code)]
    shutdown: CancellationToken,
}

impl StreamsService {
    pub fn new(endpoint: Endpoint, shutdown: CancellationToken) -> Self {
        Self { endpoint, shutdown }
    }

    #[tracing::instrument(
        name = "iroh.stream.open.request",
        level = "debug",
        skip(self),
        fields(peer = %node_id, alpn = %alpn)
    )]
    pub async fn open(&self, node_id: NodeId, alpn: Alpn) -> Result<BiStream> {
        let endpoint = self.endpoint.clone();
        let span = info_span!(
            "iroh.stream.open",
            "otel.kind" = "client",
            "otel.status_code" = field::Empty,
            "otel.status_description" = field::Empty,
            "network.transport" = "quic",
            "iroh.connect_ms" = field::Empty,
            "iroh.open_bi_ms" = field::Empty,
            "iroh.total_ms" = field::Empty,
            "iroh.selected_address" = field::Empty,
            "iroh.rtt_ms" = field::Empty,
            peer = %node_id,
            alpn = %alpn,
        );

        async move {
            let span = Span::current();
            let total_started = Instant::now();

            let connect_started = Instant::now();
            let conn = match tokio::time::timeout(
                STREAM_IO_TIMEOUT,
                endpoint.connect(node_id, alpn.as_bytes()),
            )
            .await
            {
                Ok(Ok(conn)) => {
                    let elapsed = connect_started.elapsed();
                    record_duration_ms(&span, "iroh.connect_ms", elapsed);
                    warn_if_slow_iroh_phase("stream.open", "connect", elapsed);
                    trace!(
                        event = "iroh.stream.open_phase",
                        peer = %node_id,
                        alpn = %alpn,
                        iroh_phase = "connect",
                        duration_ms = duration_ms(elapsed),
                        "Completed Iroh stream open phase"
                    );
                    conn
                }
                Ok(Err(error)) => {
                    let elapsed = connect_started.elapsed();
                    record_duration_ms(&span, "iroh.connect_ms", elapsed);
                    span.record("otel.status_code", "ERROR");
                    span.record("otel.status_description", field::display(error.to_string()));
                    warn!(
                        event = "iroh.stream.connect_failed",
                        peer = %node_id,
                        alpn = %alpn,
                        duration_ms = duration_ms(elapsed),
                        error = %error,
                        "Iroh stream connect failed"
                    );
                    return Err(NetError::Connection(error.to_string()));
                }
                Err(error) => {
                    let elapsed = connect_started.elapsed();
                    record_duration_ms(&span, "iroh.connect_ms", elapsed);
                    span.record("otel.status_code", "ERROR");
                    span.record("otel.status_description", field::display(error.to_string()));
                    warn!(
                        event = "iroh.stream.connect_timeout",
                        peer = %node_id,
                        alpn = %alpn,
                        duration_ms = duration_ms(elapsed),
                        timeout_ms = duration_ms(STREAM_IO_TIMEOUT),
                        error = %error,
                        "Iroh stream connect timed out"
                    );
                    return Err(NetError::Connection(error.to_string()));
                }
            };

            record_selected_path(&span, &conn);

            let open_started = Instant::now();
            let stream = match tokio::time::timeout(STREAM_IO_TIMEOUT, conn.open_bi()).await {
                Ok(Ok(stream)) => {
                    let elapsed = open_started.elapsed();
                    record_duration_ms(&span, "iroh.open_bi_ms", elapsed);
                    warn_if_slow_iroh_phase("stream.open", "open_bi", elapsed);
                    trace!(
                        event = "iroh.stream.open_phase",
                        peer = %node_id,
                        alpn = %alpn,
                        iroh_phase = "open_bi",
                        duration_ms = duration_ms(elapsed),
                        "Completed Iroh stream open phase"
                    );
                    stream
                }
                Ok(Err(error)) => {
                    let elapsed = open_started.elapsed();
                    record_duration_ms(&span, "iroh.open_bi_ms", elapsed);
                    span.record("otel.status_code", "ERROR");
                    span.record("otel.status_description", field::display(error.to_string()));
                    warn!(
                        event = "iroh.stream.open_bi_failed",
                        peer = %node_id,
                        alpn = %alpn,
                        duration_ms = duration_ms(elapsed),
                        error = %error,
                        "Iroh bidirectional stream open failed"
                    );
                    return Err(NetError::Stream(error.to_string()));
                }
                Err(error) => {
                    let elapsed = open_started.elapsed();
                    record_duration_ms(&span, "iroh.open_bi_ms", elapsed);
                    span.record("otel.status_code", "ERROR");
                    span.record("otel.status_description", field::display(error.to_string()));
                    warn!(
                        event = "iroh.stream.open_bi_timeout",
                        peer = %node_id,
                        alpn = %alpn,
                        duration_ms = duration_ms(elapsed),
                        timeout_ms = duration_ms(STREAM_IO_TIMEOUT),
                        error = %error,
                        "Iroh bidirectional stream open timed out"
                    );
                    return Err(NetError::Stream(error.to_string()));
                }
            };

            let total_elapsed = total_started.elapsed();
            record_duration_ms(&span, "iroh.total_ms", total_elapsed);
            warn_if_slow_iroh_request("stream.open", total_elapsed);
            span.record("otel.status_code", "OK");
            trace!(
                event = "iroh.stream.open_completed",
                peer = %node_id,
                alpn = %alpn,
                duration_ms = duration_ms(total_elapsed),
                "Opened Iroh stream"
            );
            Ok(stream)
        }
        .instrument(span)
        .await
    }
}

fn record_selected_path(span: &Span, conn: &Connection) {
    let paths = conn.paths();
    let Some(path) = paths.iter().find(|path| path.is_selected()) else {
        return;
    };

    span.record(
        "iroh.selected_address",
        field::display(format!("{:?}", path.remote_addr())),
    );
    span.record("iroh.rtt_ms", duration_ms(path.rtt()));
}

impl std::fmt::Debug for StreamsService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamsService").finish()
    }
}

#[tracing::instrument(
    name = "iroh.stream.accept_loop",
    level = "debug",
    skip(endpoint, dht_handler, gossip_handler, stream_handler, shutdown)
)]
pub async fn run_accept_loop(
    endpoint: Endpoint,
    dht_handler: mpsc::Sender<(Connection, SendStream, RecvStream, NodeId)>,
    gossip_handler: mpsc::Sender<(Connection, NodeId)>,
    stream_handler: mpsc::Sender<(Alpn, SendStream, RecvStream, NodeId)>,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            incoming = endpoint.accept() => {
                let Some(incoming) = incoming else { break };

                let dht_handler = dht_handler.clone();
                let gossip_handler = gossip_handler.clone();
                let stream_handler = stream_handler.clone();

                tokio::spawn(async move {
                    let accepting = match incoming.accept() {
                        Ok(accepting) => accepting,
                        Err(_) => return,
                    };

                    let conn = match tokio::time::timeout(STREAM_IO_TIMEOUT, accepting).await {
                        Ok(Ok(conn)) => conn,
                        Ok(Err(err)) => {
                            warn!(error = %err, "Failed to accept incoming Iroh connection");
                            return;
                        }
                        Err(err) => {
                            warn!(
                                error = %err,
                                timeout_ms = duration_ms(STREAM_IO_TIMEOUT),
                                "Timed out accepting incoming Iroh connection"
                            );
                            return;
                        }
                    };

                    let alpn_bytes = conn.alpn().to_vec();
                    let peer_id = conn.remote_id();

                    match Alpn::from_bytes(&alpn_bytes) {
                        Some(Alpn::Dht) => {
                            let (send, recv) = match tokio::time::timeout(
                                STREAM_IO_TIMEOUT,
                                conn.accept_bi(),
                            )
                            .await
                            {
                                Ok(Ok(streams)) => streams,
                                Ok(Err(err)) => {
                                    warn!(node_id = %peer_id, error = %err, "Failed to accept inbound DHT stream");
                                    return;
                                }
                                Err(err) => {
                                    warn!(
                                        node_id = %peer_id,
                                        error = %err,
                                        timeout_ms = duration_ms(STREAM_IO_TIMEOUT),
                                        "Timed out accepting inbound DHT stream"
                                    );
                                    return;
                                }
                            };
                            if let Err(err) = dht_handler.send((conn, send, recv, peer_id)).await {
                                warn!(
                                    node_id = %peer_id,
                                    error = %err,
                                    "Failed to forward inbound DHT stream"
                                );
                            }
                        }
                        Some(Alpn::Gossip) => {
                            if let Err(err) = gossip_handler.send((conn, peer_id)).await {
                                warn!(
                                    node_id = %peer_id,
                                    error = %err,
                                    "Failed to forward inbound gossip connection"
                                );
                            }
                        }
                        Some(alpn @ (Alpn::Bao | Alpn::Automerge | Alpn::Metadata)) => {
                            let (send, recv) = match tokio::time::timeout(
                                STREAM_IO_TIMEOUT,
                                conn.accept_bi(),
                            )
                            .await
                            {
                                Ok(Ok(streams)) => streams,
                                Ok(Err(err)) => {
                                    warn!(node_id = %peer_id, error = %err, "Failed to accept inbound app stream");
                                    return;
                                }
                                Err(err) => {
                                    warn!(
                                        node_id = %peer_id,
                                        error = %err,
                                        timeout_ms = duration_ms(STREAM_IO_TIMEOUT),
                                        "Timed out accepting inbound app stream"
                                    );
                                    return;
                                }
                            };
                            if let Err(err) = stream_handler.send((alpn, send, recv, peer_id)).await {
                                warn!(
                                    node_id = %peer_id,
                                    error = %err,
                                    "Failed to forward inbound app stream"
                                );
                            }
                        }
                        None => {
                            warn!(
                                "Dropping incoming connection with unknown ALPN: {:?}",
                                alpn_bytes
                            );
                        }
                    }
                });
            }
        }
    }
}
