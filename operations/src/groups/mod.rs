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
use aruna_core::events::{Event, StorageEvent};
use aruna_core::structs::identity::group::GroupAuthorizationDocument;
use aruna_core::types::Value;
use byteview::ByteView;

/// Presence refreshes, node-info heartbeats, placement timers and synced realm config write while
/// a group change is in flight; a caller must not see their SSI conflict as a failure.
pub(crate) const CONFLICT_RETRIES: u8 = 5;

pub(crate) fn parse_auth_record(
    value: Option<ByteView>,
) -> Result<Option<GroupAuthorizationDocument>, ConversionError> {
    value
        .as_ref()
        .map(|value| GroupAuthorizationDocument::from_bytes(value.as_ref()))
        .transpose()
}

/// The auth document, admin state and realm config values a member change reads in one batch;
/// any other event is refused with what was expected and the event's label.
pub(crate) fn member_reads(
    event: Event,
) -> Result<[Option<Value>; 3], (&'static str, &'static str)> {
    let got = event.label();
    let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
        return Err(("Event::Storage(StorageEvent::BatchReadResult)", got));
    };
    let Ok([(_, auth_doc), (_, reducer_state), (_, realm_config)]) = <[_; 3]>::try_from(values)
    else {
        let expected = "Event::Storage(StorageEvent::BatchReadResult) with auth doc, admin state, \
            and realm config values";
        return Err((expected, got));
    };
    Ok([auth_doc, reducer_state, realm_config])
}
