//! Groups the staging source operations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod check_source;
pub mod descriptor;
pub mod head_source;
pub mod list_source;
pub mod native_source;
pub mod offered_directory;
pub mod page;
pub mod read_source;
pub mod reference;
pub mod root_record;
pub mod snapshot;

#[cfg(test)]
pub(crate) mod tests;
