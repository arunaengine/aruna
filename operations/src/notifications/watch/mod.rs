//! Groups watch authorization, event emission, expansion, interest and subscriptions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod authorization;
pub mod check;
pub mod digest;
pub mod emit;
pub mod expand;
pub mod expansion;
pub mod interest;
pub mod listing;
pub mod origin;
pub mod paths;
pub mod records;
pub mod remove;
pub mod store;
#[cfg(test)]
mod subscriptions;
pub mod table;
