//! Why a change of a stored job was refused or failed.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::execution::job::JobRecordError;
use aruna_core::structs::execution::job::JobTransitionError;
use thiserror::Error;

#[derive(Debug, PartialEq, Error)]
pub enum JobMutationError {
    #[error("job not found")]
    NotFound,
    #[error("job claim token mismatch")]
    TokenMismatch,
    #[error("attempt intent conflicts with the committed lineage")]
    IntentConflict,
    #[error("attempt epoch exhausted")]
    EpochExhausted,
    #[error("controller generation exhausted")]
    GenerationExhausted,
    #[error("attempt control record is missing")]
    MissingControl,
    #[error("job report is frozen")]
    ReportFrozen,
    #[error("attempt control epoch mismatch")]
    EpochMismatch,
    #[error("execution output record digest conflicts with the stored digest")]
    OutputRecordConflict,
    #[error(transparent)]
    IllegalTransition(#[from] JobTransitionError),
    #[error("execution outputs are not proven durable: {0}")]
    OutputsUnproven(#[from] JobRecordError),
    #[error("{0}")]
    Storage(String),
    /// The write transaction lost to a concurrent one; a fresh run may succeed.
    #[error("job storage transaction conflicted")]
    Conflict,
}

impl From<aruna_core::calls::IoError> for JobMutationError {
    fn from(error: aruna_core::calls::IoError) -> Self {
        match error {
            aruna_core::calls::IoError::Conflict => Self::Conflict,
            error => Self::Storage(error.to_string()),
        }
    }
}
