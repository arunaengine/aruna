use super::*;

use super::export::raw_identity_matches;
use super::fanout::ensure_supported_query_form;
use super::path::{
    reduce_path_response, sanitize_path_winner, select_forward_peers, validate_path_resolution,
};
use super::read::ensure_permission;

use std::collections::BTreeMap;

use aruna_core::UserId;
use aruna_core::identifiers::{BucketId, PlacementHandle};
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

mod fixtures;
use fixtures::*;
