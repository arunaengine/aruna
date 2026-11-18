//! Answers a peer's native reference stream: reads its request, prepares the answer and streams
//! the bytes back.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_blob::native::{NATIVE_IO_TIMEOUT, read_frame, write_frame};
use aruna_core::NodeId;
use aruna_core::native_reference::{
    NativeAuthority, NativeReferenceRequest, NativeReferenceResponse,
};
use aruna_net::streams::{BiStream, SendStream};
use futures_util::StreamExt;
use tokio::io::AsyncWriteExt;
use tokio::time::timeout;
use tracing::warn;

use crate::driver::{DriverContext, drive};
use crate::staging::native_source::PrepareReferenceOperation;

pub(crate) async fn handle_native_stream(
    context: &DriverContext,
    mut stream: BiStream,
    peer: NodeId,
) {
    let frame = read_frame::<NativeReferenceRequest>(&mut stream.1);
    let request = match timeout(NATIVE_IO_TIMEOUT, frame).await {
        Ok(Ok(request)) => request,
        Ok(Err(error)) => {
            warn!(%peer, %error, "Failed to read native reference request");
            return;
        }
        Err(_) => {
            warn!(%peer, "Timed out reading native reference request");
            return;
        }
    };
    let relationship = matches!(request.authority, NativeAuthority::Relationship(_));
    let prepare = PrepareReferenceOperation::new(context.node_and_realm(), peer, request);
    match drive(prepare, context).await {
        Ok(mut prepared) => {
            // Only a caller read names the group; a sync relationship knows its own bucket.
            let response = match prepared.group_id.filter(|_| !relationship) {
                None => NativeReferenceResponse::Content(prepared.metadata),
                Some(group_id) => NativeReferenceResponse::Described {
                    metadata: prepared.metadata,
                    group_id,
                },
            };
            if write_response(&mut stream.0, &response).await.is_err() {
                return;
            }
            if let Some(body) = prepared.body.as_mut() {
                while let Some(chunk) = body.next().await {
                    let chunk = match chunk {
                        Ok(chunk) => chunk,
                        Err(error) => {
                            warn!(%peer, %error, "Failed to read native reference source");
                            stream.0.reset(1u32.into()).ok();
                            return;
                        }
                    };
                    let written = timeout(NATIVE_IO_TIMEOUT, stream.0.write_all(&chunk));
                    if !matches!(written.await, Ok(Ok(()))) {
                        return;
                    }
                }
            }
        }
        Err(reject) => {
            let response = NativeReferenceResponse::Rejected(reject);
            if write_response(&mut stream.0, &response).await.is_err() {
                return;
            }
        }
    }
    stream.0.finish().ok();
}

async fn write_response(
    send: &mut SendStream,
    response: &NativeReferenceResponse,
) -> Result<(), String> {
    timeout(NATIVE_IO_TIMEOUT, write_frame(send, response))
        .await
        .map_err(|_| "native response timed out".to_string())?
}
