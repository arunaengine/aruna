//! Groups the shared test fixtures: assistant, datasets, distributed buckets and their members,
//! execution and realm states, routes, storage routing and users. The metadata fixtures are public
//! under `test-hooks`.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#[cfg(test)]
pub(crate) mod assistant;
#[cfg(test)]
pub(crate) mod datasets;
#[cfg(test)]
pub(crate) mod distributed;
#[cfg(test)]
pub(crate) mod execution;
#[cfg(test)]
pub(crate) mod members;
pub mod metadata_fixtures;
#[cfg(test)]
pub(crate) mod realms;
#[cfg(test)]
pub(crate) mod routes;
#[cfg(test)]
pub(crate) mod storage_routing;
#[cfg(test)]
pub(crate) mod users;
