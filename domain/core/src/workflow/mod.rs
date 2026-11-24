//! Calls to the workflow coordinators of this node: preflight requests, run sessions and the
//! digests that key stored preflight results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod effect;
pub mod key;

pub use effect::{WorkflowAnswer, WorkflowEffect, WorkflowError, WorkflowHost};
pub use key::source_digest;
