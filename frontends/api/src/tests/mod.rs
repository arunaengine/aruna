//! Groups the shared test fixtures of assistant, datasets, distributed buckets, execution, realm,
//! routes, storage routing and users; the metadata fixtures are public under `test-hooks`.
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
