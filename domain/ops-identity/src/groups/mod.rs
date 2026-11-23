//! Owns the group operations and decodes stored group authorization records.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod add_member;
pub mod add_role;
pub mod admin_check;
pub mod backends;
pub mod create_group;
pub mod default_rule;
pub mod deletion;
pub mod fence;
pub mod forward;
pub mod join_request;
pub mod list_groups;
pub mod list_requests;
pub mod remove_member;
pub mod remove_role;
pub mod routing;
pub mod search_groups;
pub mod set_policies;
pub mod storage_routing;
pub mod update_group;

use aruna_core::errors::ConversionError;
use aruna_core::structs::identity::group::GroupAuthorizationDocument;
use byteview::ByteView;

/// Reads the group authorization, its admin reducer state and the realm config.
pub fn member_reads(
    group_id: aruna_core::types::GroupId,
    realm_id: aruna_core::structs::identity::realm::RealmId,
) -> aruna_core::calls::IoCall {
    let target = aruna_core::admin_documents::AdminDocumentTarget::Group { group_id };
    aruna_core::calls::IoCall::Read(vec![
        (
            aruna_core::keyspaces::AUTH_KEYSPACE.to_string(),
            ByteView::from(group_id.to_bytes()),
        ),
        (
            aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE.to_string(),
            aruna_core::storage_entries::reducer_state_key(&target),
        ),
        (
            aruna_core::keyspaces::REALM_CONFIG_KEYSPACE.to_string(),
            ByteView::from(*realm_id.as_bytes()),
        ),
    ])
}

/// Presence refreshes, node-info heartbeats, placement timers and synced realm config write while
/// a group change is in flight; a caller must not see their SSI conflict as a failure.
pub const CONFLICT_RETRIES: u8 = 5;

pub fn parse_auth_record(
    value: Option<ByteView>,
) -> Result<Option<GroupAuthorizationDocument>, ConversionError> {
    value
        .as_ref()
        .map(|value| GroupAuthorizationDocument::from_bytes(value.as_ref()))
        .transpose()
}
