//! Writes, gathers and publishes this node's info document through its flow operations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#[cfg(test)]
mod flow_tests;
pub mod flows;
pub mod inputs;
pub(crate) mod publish;
pub(crate) mod write;
