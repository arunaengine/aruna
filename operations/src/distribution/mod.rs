//! Publishes distributed bucket records: the set's publisher validates requests against its
//! head and commits each signed revision with its outbox row.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod accept_delegation;
pub mod admit;
pub mod balance;
pub mod change;
pub mod charged_ranges;
pub mod counts;
pub mod create;
pub mod delegate;
pub mod feed_walks;
mod fence_row;
pub mod forward;
pub mod guard;
pub mod handoff;
pub mod held;
pub mod imports;
#[cfg(test)]
mod index_feed_tests;
pub mod inventory;
pub mod keep;
pub mod lifecycle;
pub mod nonce;
pub mod publish;
pub mod pull;
pub(crate) mod purge_steps;
pub mod rebuild;
pub mod refill;
mod refresh;
pub mod replica_shares;
pub mod resolve;
pub mod roots;
mod route_query;
pub mod routing;
pub mod sharding;
pub mod source_keep;
pub mod source_plan;
pub mod split;
pub mod status;
mod status_view;
pub mod submit;
#[cfg(test)]
pub(crate) mod test_record;
#[cfg(any(test, feature = "test-hooks"))]
pub mod test_store;
pub mod ticket;
pub mod uploads;
pub mod usage;
pub mod version_key;
pub mod versions;
