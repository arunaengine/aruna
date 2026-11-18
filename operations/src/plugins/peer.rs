//! Asks another node over the metadata transport from a plugin operation.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult};

use crate::forward::fetch::{peer_call, peer_reply};
use crate::metadata::protocol::{MetadataTransportMessage, encode_message};

/// Upper bound of one plugin request to another node.
const PEER_TIMEOUT: Duration = Duration::from_secs(15);

/// The call that sends `message` to `node`; `None` when it cannot be encoded.
pub(crate) fn ask_peer(node: NodeId, message: &MetadataTransportMessage) -> Option<IoCall> {
    let frame = encode_message(message).ok()?;
    Some(peer_call(node, frame, PEER_TIMEOUT))
}

/// What `node` answered to [`ask_peer`]; `None` when it did not answer.
pub(crate) fn peer_answer(
    node: NodeId,
    result: IoResult,
) -> Result<Option<MetadataTransportMessage>, IoError> {
    Ok(peer_reply(node, result)?.ok())
}
