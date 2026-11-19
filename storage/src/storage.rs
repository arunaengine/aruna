//! Ties the storage submodules together; the handle, worker and owner types are its public modules.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod handle;
#[cfg(debug_assertions)]
mod ledger;
pub mod lifecycle;
pub mod metrics;
pub mod owner;
pub mod persistence;
mod policies;
mod preimage;
mod rebuild;
mod records;
pub mod sealing;
mod shutdown;
mod telemetry;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_config;
#[cfg(test)]
mod tests_counters;
#[cfg(test)]
mod tests_persistence;
#[cfg(test)]
mod tests_pool;
#[cfg(test)]
mod tests_range;
#[cfg(test)]
mod tests_rebuild;
mod transactions;
mod vault;
pub mod worker;

