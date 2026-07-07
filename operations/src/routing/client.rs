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

/// Sends one holder-proxy request to `holder` and returns its response. A
/// connect or I/O failure surfaces as `Err` so the dispatch loop can fall
/// through to the next holder in rank order.
pub async fn send_holder_proxy_request(
    net_handle: &NetHandle,
    holder: NodeId,
    request: HolderProxyRequest,
) -> Result<HolderProxyResponse, String> {
    let mut stream = net_handle
        .open_stream(holder, Alpn::HolderProxy)
        .await
        .map_err(|error| error.to_string())?;
    write_message(&mut stream, &request).await?;
    stream.0.finish().map_err(|error| error.to_string())?;
    let response = read_response(&mut stream).await?;
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
