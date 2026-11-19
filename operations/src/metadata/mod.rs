//! Groups the metadata modules and starts the background warmup of the metadata caches.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub(crate) mod after_sync;
pub mod api;
pub mod audit;
pub mod audit_entry;
pub(crate) mod builtin;
pub(crate) mod call_results;
pub(crate) mod checkpoint;
pub(crate) mod checkpoint_window;
pub mod contact;
pub mod create_document;
pub mod create_entry;
pub(crate) mod data_links;
pub mod delete_document;
pub mod device_pull;
pub mod forward;
pub(crate) mod frames;
pub mod get_document;
pub mod global_entries;
pub(crate) mod graph_deleted;
pub(crate) mod handle;
pub mod id_mapping;
pub mod identifier_lookup;
mod iri_index;
pub mod list_documents;
pub(crate) mod live_records;
pub mod materialization_queue;
pub(crate) mod message_kind;
pub mod mirror_request;
pub(crate) mod object_version;
pub mod persistent_id;
pub mod pid_read;
pub(crate) mod pid_transition;
pub mod profile;
pub mod projection;
pub(crate) mod protocol;
pub mod provenance;
pub(crate) mod prune_jobs;
pub mod prune_queue;
pub mod public_preview;
mod query_cache;
mod queue_storage;
pub(crate) mod raw_events;
pub mod raw_revision;
pub mod realm_counts;
pub mod record_reads;
pub mod repository;
pub mod revision_read;
mod search_cursor;
pub mod secondary_ids;
pub mod stats;
pub(crate) mod timestamp_sweep;
pub mod update_document;
pub mod update_entry;
pub(crate) mod updated_page;
pub mod visibility_index;
pub mod write_peer;

/// The phase-time and identity samples a metadata operation stamps records
/// with. Production keeps the defaults and samples per phase; tests replace the
/// source so fixed inputs produce byte-identical records.
#[derive(Clone, Copy, Debug)]
pub(crate) struct MetadataPhaseSource {
    now_ms: fn() -> u64,
    next_id: fn() -> ulid::Ulid,
}

impl PartialEq for MetadataPhaseSource {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::fn_addr_eq(self.now_ms, other.now_ms)
            && std::ptr::fn_addr_eq(self.next_id, other.next_id)
    }
}

impl Eq for MetadataPhaseSource {}

impl Default for MetadataPhaseSource {
    fn default() -> Self {
        Self {
            now_ms: aruna_core::time::unix_timestamp_millis,
            next_id: ulid::Ulid::generate,
        }
    }
}

impl MetadataPhaseSource {
    /// A source with explicit samplers, for tests that fix a trace.
    #[cfg(test)]
    pub(crate) fn fixed(now_ms: fn() -> u64, next_id: fn() -> ulid::Ulid) -> Self {
        Self { now_ms, next_id }
    }

    /// The current phase's wall clock in milliseconds.
    pub(crate) fn now_ms(self) -> u64 {
        (self.now_ms)()
    }

    /// A fresh identity for one record this phase mints.
    pub(crate) fn next_id(self) -> ulid::Ulid {
        (self.next_id)()
    }
}

pub use builtin::builtin_shapes;
pub use contact::{PEER_CONTACT_WINDOW, PeerContacts};
pub(crate) use handle::search_local_filtered;
pub use handle::{
    MetadataHandle, MetadataHandleOptions, export_rocrate_jsonld, list_cached_group,
    list_cached_records, query_authorized_local, search_authorized_local, send_metadata_effect,
    warm_caches,
};
pub(crate) use message_kind::transport_message_kind;
pub use protocol::{
    MetadataPathWinner, MetadataReadError, PersistentIdOutcome, PersistentIdRequest,
    PersistentIdResolution,
};
