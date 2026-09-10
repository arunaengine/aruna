use aruna_core::structs::{BlobState, NetState, RequestSummaryState, Status};

use crate::driver::DriverContext;

pub struct NodeObservabilityStatus {
    pub network: Option<NetState>,
    pub blob: Option<BlobState>,
    pub database: DatabaseObservabilityStatus,
}

pub struct DatabaseObservabilityStatus {
    pub status: Status,
    pub requests: RequestSummaryState,
}

pub async fn load_node_observability_status(context: &DriverContext) -> NodeObservabilityStatus {
    let network = match &context.net_handle {
        Some(net) => Some(net.get_status().await),
        None => None,
    };
    let blob = match &context.blob_handle {
        Some(blob) => Some(blob.get_status().await),
        None => None,
    };
    let metrics = context.storage_handle.snapshot_metrics();

    NodeObservabilityStatus {
        network,
        blob,
        database: DatabaseObservabilityStatus {
            status: if metrics.channel_closed {
                Status::Unavailable
            } else {
                Status::Available
            },
            requests: RequestSummaryState {
                total: metrics.requests_total,
                failures: metrics.failed_total,
                last_error: metrics.last_error,
            },
        },
    }
}
