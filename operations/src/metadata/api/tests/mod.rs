//! Declares the metadata API test modules and the imports and helpers they share.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::list::{GroupRecordsOperation, ListVisibleOperation, PendingRecordsOperation};
use super::list_types::check_policy_limit;
use super::*;
use crate::driver::DriverContext;
use crate::metadata::handle::QUERY_MAX_BYTES;

use super::distributed_types::{
    MetadataQueryForm, aggregate_query_results, query_form, query_select_limit,
};
use super::export::raw_identity_matches;
use super::export_types::{
    ExportMetadataRequest, ExportMetadataResult, GetVisibleRequest, RoCrateExportView,
};
use super::fanout::ensure_query_form;
use super::fanout_types::{forwarded_bearer, query_union_safe};
use super::list_types::{ListVisibleRequest, ListVisibleResult, MetadataListOrder};
use super::path::LocalPathOperation;
use super::path::{reduce_path_response, select_forward_peers, validate_path_resolution};
use super::path_types::MetadataLookupResult;
use super::path_types::deduplicate_fanout_nodes;
use super::path_types::sanitize_path_winner;
use super::preflight_types::{
    MetadataIndexState, MetadataNodeFreshness, MetadataResolvedTarget, MetadataStorageOperation,
    ReferenceRequest, ReferenceTarget,
};
use super::readable::PathPermissionOperation;
use super::search_types::SearchQueryRequest;
use crate::driver::drive;
use aruna_core::effects::StorageEffect;
use aruna_core::events::StorageEvent;

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
use byteview::ByteView;

mod export;
mod fanout;
mod list;
mod path;
mod preflight;
mod query;
mod search;

use crate::tests::metadata_api::*;
