//! Owns the group operations and decodes stored group authorization records.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod add_member;
pub mod add_role;
pub mod backends;
pub mod create_group;
pub mod fence;
pub mod forward;
pub mod get_group;
pub mod join_request;
pub mod list_groups;
pub mod list_requests;
pub mod remove_member;
pub mod remove_role;
pub mod search_groups;
pub mod set_policies;
pub mod storage_routing;
pub mod update_group;

use aruna_core::errors::ConversionError;
use aruna_core::structs::identity::group::GroupAuthorizationDocument;
use byteview::ByteView;

pub(crate) fn parse_auth_record(
    value: Option<ByteView>,
) -> Result<Option<GroupAuthorizationDocument>, ConversionError> {
    value
        .as_ref()
        .map(|value| GroupAuthorizationDocument::from_bytes(value.as_ref()))
        .transpose()
}
