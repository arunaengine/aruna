//! Groups the storage records: backends, blobs, versions, uploads, usage and routing rules.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod abe;
pub mod abe_access;
pub mod backends;
pub mod blob;
pub mod cleanup;
pub mod data_identity;
pub mod dataset_location;
pub mod delete_audit;
pub mod encryption;
pub mod format;
pub mod group_backend;
pub mod holders;
pub mod key_audit;
pub mod metadata_registry;
pub mod multipart;
pub mod node_info;
pub mod replication;
pub mod routing;
pub mod storage_purge;
pub mod transition;
pub mod usage;
