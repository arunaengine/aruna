//! Groups the replication modules: job queue, inbound and outbound transfer, and the sync
//! relationships with their mirror repair.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod bao_read;
pub mod bootstrap;
pub mod catch_up;
pub mod drain;
pub mod drain_timer;
mod error;
pub mod incoming;
pub mod live_queue;
pub mod locations;
pub mod managed_read;
pub mod members;
pub mod mirror_repair;
mod obligations;
pub mod page;
pub mod parking;
pub mod plaintext;
pub mod queue;
mod retire;
pub mod source_rules;
pub mod sync_relationship;
pub mod version_replication;
