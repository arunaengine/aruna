//! Groups the sync modules for announces, the outbox, placement, relationships and repair.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod announce;
pub mod document_outbox;
pub(crate) mod evicted;
pub mod mirror_repair;
pub(crate) mod native_serve;
pub mod outbox_record;
pub mod reconcile;
pub mod replicate_documents;
pub mod shard_placement;
pub mod sync_quarantine;
pub mod sync_relationship;
