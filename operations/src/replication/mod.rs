//! Groups the replication modules: wire protocol, job queue, inbound and outbound transfer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod bao_read;
pub mod bootstrap;
pub mod drain;
mod error;
pub mod incoming;
pub mod locations;
pub mod members;
mod obligations;
pub mod page;
pub mod parking;
pub mod plaintext;
pub mod protocol;
pub mod queue;
mod retire;
pub mod version_replication;
