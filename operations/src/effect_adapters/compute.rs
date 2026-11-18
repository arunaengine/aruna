//! Carries out one compute call on the executor backend the registry holds for its kind.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::compute::effect::{ComputeAnswer, ComputeEffect, ComputeRequest};

use crate::driver::DriverContext;

pub(super) async fn dispatch_compute(
    effect: ComputeEffect,
    context: &DriverContext,
) -> ComputeAnswer {
    let Some(registry) = context.compute_handle.as_ref() else {
        return ComputeAnswer::NoCompute;
    };
    let Some(backend) = registry.get(&effect.kind) else {
        return ComputeAnswer::NoBackend;
    };
    let fence = &effect.fence;
    match effect.request {
        ComputeRequest::Fence => ComputeAnswer::Done(backend.fence(fence).await),
        ComputeRequest::Cleanup => ComputeAnswer::Done(backend.cleanup(fence).await),
        ComputeRequest::Tombstone(spec) => {
            ComputeAnswer::Tombstone(backend.tombstone(fence, &spec).await)
        }
    }
}
