//! The calls an operation makes to one executor backend of this node, and their answers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{BackendError, ExecutorKind, FenceContext, TombstoneEvidence, TombstoneSpec};

/// One call to the backend of `kind` about the fenced attempt.
#[derive(Debug, PartialEq)]
pub struct ComputeEffect {
    pub kind: ExecutorKind,
    pub fence: FenceContext,
    pub request: ComputeRequest,
}

/// What a call asks of the backend.
#[derive(Debug, PartialEq)]
pub enum ComputeRequest {
    Fence,
    /// Deletes the external object; only after its terminal evidence is durable.
    Cleanup,
    Tombstone(TombstoneSpec),
}

/// The answer to one [`ComputeEffect`].
#[derive(Debug, PartialEq)]
pub enum ComputeAnswer {
    /// This node has no compute plane.
    NoCompute,
    /// This node has no backend of that kind.
    NoBackend,
    Done(Result<(), BackendError>),
    Tombstone(Result<TombstoneEvidence, BackendError>),
}
