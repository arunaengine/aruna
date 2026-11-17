//! Groups the replication modules: wire protocol, job queue, inbound and outbound transfer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod bao_read;
pub mod bootstrap;
pub(crate) mod dht_registration;
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
pub mod records;
pub mod version_replication;
