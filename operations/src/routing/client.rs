use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::alpn::Alpn;
use aruna_net::NetHandle;
use aruna_net::streams::BiStream;
use tokio::time::timeout;

use crate::routing::incoming::close_stream;
use crate::routing::protocol::{
    HolderProxyRequest, HolderProxyResponse, read_holder_proxy_response, write_holder_proxy_request,
};

pub const HOLDER_PROXY_IO_TIMEOUT: Duration = Duration::from_secs(10);

/// A send failure split at the retry-safety boundary. `Connect` means the
/// request provably never reached the holder, so any call may retry the next
/// one. `Io` means request bytes may already have been delivered — a mutation
/// retried elsewhere after this could be applied twice.
#[derive(Debug)]
pub enum HolderProxySendError {
    Connect(String),
    Io(String),
}

/// Sends one holder-proxy request to `holder` and returns its response. The
/// error kind tells the dispatch loop whether falling through to the next
/// holder is safe for a mutation.
pub async fn send_holder_proxy_request(
    net_handle: &NetHandle,
    holder: NodeId,
    request: HolderProxyRequest,
) -> Result<HolderProxyResponse, HolderProxySendError> {
    let mut stream = net_handle
        .open_stream(holder, Alpn::HolderProxy)
        .await
        .map_err(|error| HolderProxySendError::Connect(error.to_string()))?;
    write_message(&mut stream, &request)
        .await
        .map_err(HolderProxySendError::Io)?;
    stream
        .0
        .finish()
        .map_err(|error| HolderProxySendError::Io(error.to_string()))?;
    let response = read_response(&mut stream)
        .await
        .map_err(HolderProxySendError::Io)?;
    close_stream(&mut stream).await;
    Ok(response)
}

async fn write_message(stream: &mut BiStream, request: &HolderProxyRequest) -> Result<(), String> {
    timeout(
        HOLDER_PROXY_IO_TIMEOUT,
        write_holder_proxy_request(stream, request),
    )
    .await
    .map_err(|_| "timed out writing holder proxy request".to_string())?
}

async fn read_response(stream: &mut BiStream) -> Result<HolderProxyResponse, String> {
    timeout(HOLDER_PROXY_IO_TIMEOUT, read_holder_proxy_response(stream))
        .await
        .map_err(|_| "timed out waiting for holder proxy response".to_string())?
}
