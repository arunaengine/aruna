//! Groups the DHT parts: routing, protocol, state machine, driver and storage.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod constants;
pub mod driver;
pub mod handle;
pub mod kbucket;
pub mod protocol;
pub mod rpc;
pub mod state;
pub mod storage;

pub use handle::DhtHandle;
