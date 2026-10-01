//! Groups the user operations for lookup, search, updates, subject index and the vault.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod account_status;
pub mod get_oidc;
pub mod get_user;
pub mod list_users;
pub mod oidc_user;
pub mod read_document;
pub mod resolve_users;
pub mod search_users;
pub mod service_account;
pub mod subject_index;
pub mod update_user;
mod vault;

pub use vault::{read as vault_read, route as vault_route, write as vault_write};
