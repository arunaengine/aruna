//! Defines the error type shared by the replication operations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::errors::ConversionError;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum ReplicationError {
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Connection to receiving node is missing")]
    ConnectionMissing,
    #[error("Replication rejected: {0}")]
    ReplicationRejected(String),
    #[error("Replication failed")]
    ReplicationFailed,
}
