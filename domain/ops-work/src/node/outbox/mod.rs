//! Runs this node's document sync outbox drain from the timer: one rotation invocation drains
//! pages, relays what this node cannot publish, and publishes, syncs and finishes the rest.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod drain_page;
pub mod finish_batch;
pub mod publish_batches;
pub mod relay_records;
pub mod rotation_run;
pub mod sync_batch;
