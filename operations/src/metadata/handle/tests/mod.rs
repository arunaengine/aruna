use super::lifecycle::registry_records_for_group;
use super::search::{GraphVisibilityScope, LifecycleVisibility, registry_record_for_graph};
use super::search::{
    HitDescribe, ScopeAuthorizer, describe_hits_parallel, filter_candidate_records,
    select_visible_records,
};
use super::*;
use aruna_core::auth::bearer_token_hash;
use aruna_core::keys::generate_signing_key;
use aruna_core::metadata::MetadataApplyRoCrateRequest;
use aruna_core::storage_entries::metadata_graph_lifecycle_key;
use aruna_core::structs::{
    ArunaArn, PathRestriction, PlacementRef, RealmNodeKind, SyncMode, SyncState,
    SyncStatusSnapshot, TokenRevocation,
};
use aruna_storage::FjallStorage;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::Serialize;
use tempfile::{TempDir, tempdir};
use tokio::io::AsyncWriteExt;

const ROCRATE_12: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/rocrate/roundtrip-1.2.json"
));
const ROCRATE_13: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/rocrate/roundtrip-1.3.json"
));

mod auth;
mod effect;
mod lifecycle;
mod query;
mod search;
mod sync;
mod visibility;
