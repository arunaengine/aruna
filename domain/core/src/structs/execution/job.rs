//! Owns the job records module and re-exports its family, identity, input and local parts.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::str::FromStr;

use byteview::ByteView;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use crate::compute::ExecutionTargetId;
use crate::compute::runtimes::{SESSION_RUNTIME_TAG, SESSION_TAG, SESSION_TAG_NOTEBOOK};
use crate::errors::ConversionError;
use crate::id::NodeId;
use crate::structs::checksum::ChecksumAlgorithm;
use crate::structs::execution::harvest::HarvestJobSpec;
use crate::structs::execution::notification::invert_timestamp_ms;
use crate::structs::execution::staging::StagingStrategy;
use crate::structs::identity::auth::AuthContext;
use crate::structs::identity::realm::RealmId;
use crate::structs::persistent_id::MintPersistentSpec;
use crate::structs::placement::policy::PlacementPolicyRef;
use crate::structs::placement::record::PlacementRef;
use crate::structs::storage::blob::{BackendLocation, HiddenBlobKey};
use crate::structs::storage::storage_purge::{
    StoragePurgeResult, StoragePurgeScope, StoragePurgeSpec,
};
use crate::structured_id::{
    BucketId, FieldError, JobId as RoutableJobId, PlacementHandle, StructuredId,
};
use crate::types::{GroupId, Key};
use crate::user::id::UserId;

pub mod family;
pub mod identity;
pub mod input;
pub mod key_wait;
pub mod launch;
pub mod local;
pub mod membership;
pub mod pages;
pub mod prune;
pub mod record_keys;
pub mod record_rows;
pub mod rows;

#[cfg(test)]
mod tests;
