//! Runs this node's document sync outbox drain: publishes the drained sub-batches, syncs each
//! with its peers and finishes it once its records may go.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod finish;
pub mod publish;
pub mod sync;
