//! Groups the onboarding operations for enrollment secrets and node bootstrap.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod consume_secret;
pub mod create_secret;
pub mod delete_secret;
pub mod expiry_page;
pub mod finalize_bootstrap;
pub mod inspect_secret;
pub mod issue_ticket;
pub mod list_secrets;
pub mod live_page;
pub mod prune_secret;
pub mod read_secret;
pub mod recover_admin;
pub mod reserve_secret;
pub mod secret_state;

/// Reads the secret record and the secret state of `enrollment_id`.
pub(crate) fn secret_reads(enrollment_id: ulid::Ulid) -> aruna_core::calls::IoCall {
    use aruna_core::keyspaces::ONBOARDING_KEYSPACE;
    aruna_core::calls::IoCall::Read(vec![
        (
            ONBOARDING_KEYSPACE.to_string(),
            create_secret::secret_record_key(enrollment_id),
        ),
        (
            ONBOARDING_KEYSPACE.to_string(),
            secret_state::secret_state_key(enrollment_id),
        ),
    ])
}
