use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::str::FromStr;

use byteview::ByteView;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use crate::NodeId;
use crate::UserId;
use crate::compute::ExecutionTargetId;
use crate::compute::runtimes::{SESSION_RUNTIME_TAG, SESSION_TAG, SESSION_TAG_NOTEBOOK};
use crate::errors::ConversionError;
use crate::structs::execution::notification::invert_timestamp_ms;
use crate::structs::identity::auth::AuthContext;
use crate::structs::storage::blob::{BackendLocation, HiddenBlobKey};
use crate::structs::execution::harvest::HarvestJobSpec;
use crate::structs::MintPersistentSpec;
use crate::structs::placement::placement_policy::PlacementPolicyRef;
use crate::structs::placement::placement_record::PlacementRef;
use crate::structs::identity::realm::RealmId;
use crate::structs::execution::staging::StagingStrategy;
use crate::structs::storage::storage_purge::{
    StoragePurgeResult, StoragePurgeScope, StoragePurgeSpec,
};
use crate::structured_id::{
    BucketId, FieldError, JobId as RoutableJobId, PlacementHandle, StructuredId,
};
use crate::types::{GroupId, Key};

mod family;
mod identity;
mod input;
mod local;

pub use family::*;
pub use identity::*;
pub use input::*;
pub use local::*;

#[cfg(test)]
mod tests;
