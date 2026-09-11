use super::*;

use super::export::raw_identity_matches;
use super::fanout::ensure_supported_query_form;
use super::path::{
    reduce_path_response, sanitize_path_winner, select_forward_peers, validate_path_resolution,
};
use super::read::ensure_permission;

use std::collections::BTreeMap;

use aruna_core::UserId;
use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE};
use aruna_core::metadata::MetadataCreateEventPayload;
use aruna_core::storage_entries::{
    metadata_create_event_and_pending_projection_write_entries,
    metadata_document_lifecycle_write_entry, metadata_graph_lifecycle_write_entry,
};
use aruna_core::structs::{
    Actor, Group, GroupAuthorizationDocument, PlacementRef, RealmAuthorizationDocument,
    RealmNodeKind, Role,
};
use aruna_core::structured_id::{BucketId, PlacementHandle};
use aruna_core::types::{Key, RoleId};
use aruna_storage::storage;
use byteview::ByteView;
use tempfile::{TempDir, tempdir};

use crate::metadata::MetadataHandle;

mod export;
mod fanout;
mod list;
mod path;
mod preflight;
mod query;
mod search;

const TEST_REALM_ID: RealmId = RealmId([7u8; 32]);

struct MetadataTest {
    context: DriverContext,
    _storage_dir: TempDir,
    _metadata_dir: TempDir,
}

fn metadata_test() -> MetadataTest {
    let storage_dir = tempdir().expect("storage dir");
    let metadata_dir = tempdir().expect("metadata dir");
    let storage_handle =
        storage::FjallStorage::open(storage_dir.path().to_str().expect("storage path"))
            .expect("storage opens");
    let metadata_handle = MetadataHandle::new(
        metadata_dir.path(),
        iroh::SecretKey::from_bytes(&[7u8; 32]).public(),
        storage_handle.clone(),
        None,
        None,
        None,
    )
    .expect("metadata handle");
    MetadataTest {
        context: DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: Some(metadata_handle),
            task_handle: None,
            compute_handle: None,
        },
        _storage_dir: storage_dir,
        _metadata_dir: metadata_dir,
    }
}

fn public_record(group_id: GroupId, document_id: Ulid) -> MetadataRegistryRecord {
    let event_id = Ulid::generate();
    let document_id = MetaResourceId::from_parts(
        document_id.timestamp_ms(),
        PlacementHandle::new(1).unwrap(),
        BucketId::new(0).unwrap(),
        document_id.0 as u64 & ((1_u64 << 48) - 1),
    )
    .unwrap()
    .as_ulid();
    let document_path = format!("datasets/cached/{document_id}");
    MetadataRegistryRecord {
        realm_id: TEST_REALM_ID,
        group_id,
        document_id,
        document_path: document_path.clone(),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        public: true,
        permission_path: MetadataRegistryRecord::permission_path_for(
            &TEST_REALM_ID,
            group_id,
            &document_path,
            document_id,
        ),
        placement: PlacementRef::NIL,
        holder_node_ids: Vec::new(),
        created_at_ms: 1,
        updated_at_ms: 1,
        establishing_event_id: event_id,
        last_event_id: event_id,
    }
}
