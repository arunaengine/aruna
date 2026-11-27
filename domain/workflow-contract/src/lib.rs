//! The channel between an Aruna node and its workflow coordinator: frame bounds, messages, host
//! calls, preflight requests and the Aruna settings of composed workflow steps.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod encoding;
pub mod frame;
pub mod host;
pub mod message;
pub mod preflight;
pub mod settings;

pub use frame::{FrameError, decode_frame_len, decode_message, encode_frame, encode_message};
pub use frame::{read_frame, write_frame};
pub use host::{HostRpcRequest, HostRpcResult};
pub use message::{CoordinatorMode, CoordinatorToHost, HostToCoordinator, LiveWorkflowEvent};
pub use preflight::{PreflightKind, PreflightResult};
pub use settings::ArunaStepSettings;

use serde::{Deserialize, Serialize};

/// The version of this protocol; `Hello` and `HelloAck` must name the same one.
pub const PROTOCOL_VERSION: u32 = 1;
/// The largest JSON payload of one frame, without its four-byte length prefix.
pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
/// The largest number of file bytes that one data frame carries, so that chunks stay far below
/// `MAX_FRAME_BYTES` after base64 encoding.
pub const DATA_CHUNK_BYTES: usize = 256 * 1024;

/// Why a request across the channel failed, in a form both sides can act on.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ContractError {
    #[error("protocol version {found} is not supported; this side speaks {expected}")]
    Version { expected: u32, found: u32 },
    #[error("invalid request: {message}")]
    Invalid { message: String },
    /// The workflow, its values or its settings do not pass a check.
    #[error("{message}")]
    Workflow { message: String },
    #[error("not supported: {message}")]
    Unsupported { message: String },
    #[error("not found: {message}")]
    NotFound { message: String },
    /// The host refused a call, like a read without permission.
    #[error("denied: {message}")]
    Denied { message: String },
    #[error("cancelled: {reason}")]
    Cancelled { reason: String },
    /// A fault of the side that answers; another attempt may work.
    #[error("internal failure: {message}")]
    Internal { message: String },
}

impl ContractError {
    pub fn invalid(message: impl Into<String>) -> ContractError {
        ContractError::Invalid {
            message: message.into(),
        }
    }

    pub fn workflow(message: impl Into<String>) -> ContractError {
        ContractError::Workflow {
            message: message.into(),
        }
    }

    pub fn internal(message: impl Into<String>) -> ContractError {
        ContractError::Internal {
            message: message.into(),
        }
    }

    pub fn unsupported(message: impl Into<String>) -> ContractError {
        ContractError::Unsupported {
            message: message.into(),
        }
    }
}
