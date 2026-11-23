//! Owns a device as a node of its realm: eviction, the wipe check, its copy of the realm
//! documents, the note of its exchange with the realm and the device row scan.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod realm_documents;
pub mod remove_node;
pub mod scan_rows;
pub mod sync_note;
pub mod wipe;
