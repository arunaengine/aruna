//! Service accounts: user records without a login that one group owns and administers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod check;
pub mod create;

use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::GroupId;

/// Group administrators manage the group's service accounts.
pub fn group_admin_path(realm_id: RealmId, group_id: GroupId) -> String {
    format!("/{realm_id}/g/{group_id}/admin")
}
