//! Groups the metadata modules and starts the background warmup of the metadata caches.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod after_sync;
pub mod api;
pub mod audit;
pub mod audit_entry;
pub(crate) mod builtin;
pub(crate) mod checkpoint;
pub(crate) mod checkpoint_window;
pub mod create_document;
pub mod create_entry;
pub mod data_links;
pub mod delete_document;
pub mod deletion_guard;
pub mod device;
pub mod device_pull;
pub mod forward;
pub mod get_document;
pub mod global_entries;
pub(crate) mod graph_deleted;
pub mod id_mapping;
pub mod identifier_lookup;
pub mod iri_index;
pub mod list_documents;
pub(crate) mod live_records;
pub mod local_query;
pub mod materialization_queue;
pub(crate) mod object_version;
pub mod persistent_id;
pub mod pid_read;
pub(crate) mod pid_transition;
pub mod profile;
pub mod projection;
pub mod provenance;
pub mod prune_jobs;
pub mod prune_queue;
pub mod public_preview;
mod query_cache;
pub(crate) mod raw_events;
pub mod raw_revision;
pub(crate) mod read_peer;
pub mod realm_counts;
pub mod record_reads;
pub mod registry_cache;
pub mod repository;
pub mod revision_read;
mod search_cursor;
pub mod secondary_ids;
pub mod serve;
pub(crate) mod span_timing;
pub mod stats;
pub mod text_search;
pub mod timestamp_sweep;
pub mod topic_sync;
pub mod update_document;
pub mod update_entry;
pub(crate) mod updated_page;
pub mod visibility_index;
pub(crate) mod visible_scope;

pub use builtin::builtin_shapes;

#[cfg(test)]
mod store_tests;
