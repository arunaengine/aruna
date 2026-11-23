//! Defines the storage crate's error type for fjall, conversion and channel failures.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::errors::ConversionError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageLibError {
    #[error(transparent)]
    FjallError(#[from] fjall::Error),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("Crossfire error: {0}")]
    CrossfireRecvError(#[from] crossfire::RecvError),
    #[error("Crossfire send error")]
    CrossfireSendError,
}
