//! Declares the routed metadata read and write modules.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub(crate) mod holder_reads;
pub(crate) mod profile_export;
pub mod read;
pub mod route_create;
pub mod write;

#[cfg(test)]
mod tests;
