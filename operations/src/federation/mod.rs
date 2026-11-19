//! Groups the federation operations: registry publication, native login and exports.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod access;
pub mod grant;
#[cfg(test)]
mod grant_tests;
pub mod import;
pub mod link;
pub mod login;
pub mod publish;
pub mod records;
pub mod registration;
pub mod totals;
pub mod transfer;

/// Converts call failures into the storage, unexpected and unfinished variants of an error.
macro_rules! io_failure {
    ($error:ty) => {
        impl From<aruna_core::calls::IoError> for $error {
            fn from(error: aruna_core::calls::IoError) -> Self {
                use aruna_core::calls::IoError;
                use aruna_core::errors::StorageError;
                match error {
                    IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
                    IoError::Storage(error) => Self::Storage(error),
                    IoError::Unexpected { .. } | IoError::Codec(_) => Self::UnexpectedEvent,
                    IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished => {
                        Self::NotFinished
                    }
                }
            }
        }
    };
}
pub(crate) use io_failure;
