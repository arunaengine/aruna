//! Owns the device-local plane of a user node: replicas, drafts, synced folders and the offline
//! publish queue, whose intents become realm state only when the drain forwards them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod backlog;
pub mod delete_draft;
pub mod enqueue_draft;
pub mod inspect_draft;
pub mod list_drafts;
pub mod publish_queue;
pub mod replica;
pub mod replica_store;
pub mod retry;
pub mod sync;
pub mod sync_status;
#[cfg(test)]
mod tests;
