//! Groups the federation operations: registry publication, native login and exports.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod access;
pub mod grant;
#[cfg(test)]
mod grant_flows;
pub mod import;
pub mod link;
pub mod login;
pub mod publish;
pub mod records;
pub(crate) mod registration;
pub mod totals;
pub mod transfer;
