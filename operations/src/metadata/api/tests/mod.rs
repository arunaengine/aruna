use super::*;

use super::export::raw_identity_matches;
use super::fanout::ensure_query_form;
use super::path::{
    reduce_path_response, sanitize_path_winner, select_forward_peers, validate_path_resolution,
};
use super::read::ensure_permission;

use std::collections::BTreeMap;

use aruna_core::UserId;
use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE};
use aruna_core::metadata::MetadataEventPayload;
use aruna_core::storage_entries::{
    create_projection_entries, document_lifecycle_entry, graph_lifecycle_entry,
};
use aruna_core::structs::identity::auth::{Actor, Role};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::{RealmAuthorizationDocument, RealmNodeKind};
use aruna_core::types::{Key, RoleId};
use aruna_storage::storage;
use byteview::ByteView;
use tempfile::tempdir;

mod export;
mod fanout;
mod list;
mod path;
mod preflight;
mod query;
mod search;

use crate::tests::metadata_api::*;
