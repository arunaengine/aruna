//! Storage and routing records: backends, blobs, versions, uploads, usage,
//! and the routing rules that select them.

pub mod backends;
pub mod blob;
pub mod cleanup;
pub mod delete_audit;
pub mod group_backend;
pub mod metadata_registry;
pub mod multipart;
pub mod node_info;
pub mod replication;
pub mod routing;
pub mod storage_purge;
pub mod usage;
