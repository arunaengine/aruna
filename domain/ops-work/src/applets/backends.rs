//! Lists what the backends of this node declare about kernel sandboxes and public work, which
//! work started for applet visitors and guests needs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoResult, Next};
use aruna_core::compute::effect::{ComputeAnswer, ComputeEffect};
use aruna_core::operation::Operation;

use super::AppletError;
use crate::jobs::workflow::compute::{compute_answer, compute_call};

/// One backend: whether it declares a kernel sandbox, and whether it takes public work.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sandboxing {
    pub backend: String,
    pub sandbox: bool,
    pub public_work: bool,
}

/// Whether work for the public may run on this node: some backend, and every one of them
/// sandboxed and open to public work.
pub fn public_ready(backends: &[Sandboxing]) -> bool {
    !backends.is_empty() && (backends.iter()).all(|backend| backend.sandbox && backend.public_work)
}

/// Answers every backend of this node in name order; a node without compute has none.
#[derive(Debug, Default, PartialEq)]
pub struct SandboxOperation;

impl Operation for SandboxOperation {
    type Output = Vec<Sandboxing>;
    type Error = AppletError;

    fn first(&mut self) -> Result<Next<Vec<Sandboxing>>, AppletError> {
        Ok(Next::Call(compute_call(ComputeEffect::Offers)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<Sandboxing>>, AppletError> {
        let ComputeAnswer::Offers(offers) = compute_answer(result)? else {
            return Ok(Next::Done(Vec::new()));
        };
        let backends = (offers.iter())
            .map(|offer| Sandboxing {
                backend: offer.name().to_string(),
                sandbox: offer.caps().declared.sandbox,
                public_work: offer.caps().declared.public_work,
            })
            .collect();
        Ok(Next::Done(backends))
    }

    fn expected_error(error: &AppletError) -> bool {
        !matches!(error, AppletError::Storage(_))
    }
}
