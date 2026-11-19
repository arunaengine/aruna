//! Owns the persisted and wire records, grouped into domain modules.
//! Callers name a record through its domain, such as structs::storage::blob::BlobVersion.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod execution;
pub mod identity;
pub mod placement;
pub mod storage;

pub mod assistant_chat;
pub mod assistant_provider;
pub mod central_assistant;
pub mod checksum;
pub mod info;
pub mod path_claim;
pub mod persistent_id;
pub mod secondary_id;
pub mod sync_quarantine;
pub mod sync_relationship;
pub mod synced_folder;

