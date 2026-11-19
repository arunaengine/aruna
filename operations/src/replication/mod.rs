//! Groups the replication modules: wire protocol, job queue, inbound and outbound transfer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod bao_read;
pub(crate) mod bootstrap;
pub mod catch_up;
pub mod drain;
mod error;
pub mod incoming;
pub mod live_queue;
pub mod locations;
pub(crate) mod managed_read;
pub mod members;
mod obligations;
pub mod page;
pub mod parking;
pub mod plaintext;
pub mod protocol;
pub mod queue;
mod retire;
pub(crate) mod source_rules;
pub(crate) mod version_replication;
