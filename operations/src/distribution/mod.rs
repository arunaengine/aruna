//! Publishes distributed bucket records: the set's publisher validates requests against its
//! head and commits each signed revision with its outbox row.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod accept_delegation;
pub mod admit;
pub mod counts;
pub mod delegate;
pub(crate) mod fence_row;
pub mod guard;
pub mod handoff;
pub mod held;
pub mod imports;
#[cfg(test)]
mod index_feed_tests;
pub mod inventory;
pub mod keep;
pub mod nonce;
pub mod publish;
pub mod rebuild;
pub mod refill;
pub mod resolve;
pub mod roots;
pub mod route_query;
pub mod routing;
pub mod source_plan;
pub mod split;
pub mod submit;
#[cfg(any(test, feature = "test-hooks"))]
pub mod test_record;
#[cfg(any(test, feature = "test-hooks"))]
pub mod test_store;
pub mod ticket;
pub mod uploads;
pub mod version_key;
pub mod versions;
