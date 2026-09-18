//! Defines the crate's internal error type wrapping io, postcard, opendal and client errors.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use thiserror::Error;

#[derive(Debug, Error)]
pub enum BlobLibError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    PostcardError(#[from] postcard::Error),
    #[error(transparent)]
    OpenDalError(#[from] opendal::Error),
    #[error(transparent)]
    ClientError(#[from] reqwest::Error),
}
