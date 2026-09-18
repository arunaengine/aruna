//! Owns the device-local plane of a user node: the offline publish queue and node controls.
//! A queued intent becomes realm state only when the drain forwards it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod backlog;
pub mod compute;
pub mod delete_draft;
pub mod drain;
pub mod edit;
pub mod enqueue_draft;
pub mod forward;
pub mod inspect_draft;
pub mod list_drafts;
pub mod publish_queue;
pub mod realm_documents;
pub mod refresh;
pub mod remove_node;
pub mod replica;
pub mod selection;
pub mod sync;
pub mod sync_status;
#[cfg(test)]
mod tests;
pub mod wipe;
