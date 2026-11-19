//! Owns the metadata handle: its caches, peer state and node keys, beside the graph store.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::path::Path;
use std::sync::Arc;
#[cfg(feature = "test-hooks")]
use std::sync::atomic::AtomicBool;

use aruna_core::structs::identity::realm::RealmId;
use aruna_net::NetHandle;
use aruna_storage::StorageHandle;
use aruna_storage::key_index::RangeIndexes;

use super::protocol::METADATA_IO_TIMEOUT;
use crate::auth::bearer_token::{
    DecodeTokenOperation, IssuerKeyCache, IssuerKeys, TokenChecks, claims_auth,
};
use crate::auth::error::ArunaBearerError;
use crate::driver::{DriverContext, drive};
use crate::metadata::write_peer::forwarded_checks;
use aruna_core::structs::identity::auth::AuthContext;

mod effects;
mod forward_serve;
pub(crate) mod frames;
pub use self::effects::{export_rocrate_jsonld, send_metadata_effect};
pub use self::pull_serve::{accept_pull, pull_status};
mod ingress;
mod peer_auth;
mod pull_serve;
mod transport;

const METADATA_CHUNK_SIZE: usize = 64 * 1024;

/// The directory below the metadata storage path that holds the range search indexes.
const RANGE_INDEX_DIR: &str = "range-indexes";

#[derive(Clone)]
pub struct MetadataHandle {
    inner: Arc<MetadataInner>,
}

#[derive(Clone, Debug, Default)]
pub struct MetadataHandleOptions {}

struct MetadataInner {
    storage_handle: StorageHandle,
    auth_validation: AuthValidationState,
    net_handle: Option<NetHandle>,
    inbound_frame_bytes: Arc<tokio::sync::Semaphore>,
    sync_pulls: self::pull_serve::PullRegistry,
    #[cfg(feature = "test-hooks")]
    repair_hold: AtomicBool,
}

#[derive(Clone)]
struct AuthValidationState {
    storage_handle: StorageHandle,
    /// Realm this node serves; without one there is no replicated revocation
    /// set to consult, and this node serves no remote metadata peers either.
    realm_id: Option<RealmId>,
    issuer_keys: Arc<IssuerKeyCache>,
}

impl AuthValidationState {
    fn new(storage_handle: StorageHandle, realm_id: Option<RealmId>) -> Self {
        Self {
            storage_handle,
            realm_id,
            issuer_keys: Arc::new(IssuerKeyCache::new()),
        }
    }

    /// Verifies `token`. Without realm state this node holds no revocation authority at all;
    /// once it has any, the token's own issuing realm answers.
    async fn validate(&self, token: &str) -> Result<AuthContext, ArunaBearerError> {
        let checks = TokenChecks {
            keys: IssuerKeys::Cached(self.issuer_keys.clone()),
            ..forwarded_checks(self.realm_id, false)
        };
        let context = DriverContext::storage_only(self.storage_handle.clone());
        claims_auth(drive(DecodeTokenOperation::new(token, checks), &context).await?)
    }
}

impl std::fmt::Debug for MetadataHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataHandle").finish_non_exhaustive()
    }
}

impl MetadataHandle {
    /// A handle whose range indexes live under `path`.
    pub fn new(
        path: impl AsRef<Path>,
        storage_handle: StorageHandle,
        net_handle: Option<NetHandle>,
        _options: MetadataHandleOptions,
    ) -> Self {
        // Range indexes sit beside the metadata store, so only a node with metadata keeps them.
        storage_handle
            .install_range_indexes(RangeIndexes::new(path.as_ref().join(RANGE_INDEX_DIR)));
        Self {
            inner: Arc::new(MetadataInner {
                auth_validation: AuthValidationState::new(
                    storage_handle.clone(),
                    net_handle.as_ref().map(|net| *net.realm_id()),
                ),
                storage_handle,
                net_handle,
                inbound_frame_bytes: Arc::new(tokio::sync::Semaphore::new(
                    super::protocol::INBOUND_FRAME_BYTES,
                )),
                sync_pulls: Default::default(),
                #[cfg(feature = "test-hooks")]
                repair_hold: AtomicBool::new(false),
            }),
        }
    }

    /// Test hook: sync mirror repair on this node waits until `release_repair`.
    #[cfg(feature = "test-hooks")]
    pub fn hold_repair(&self) {
        self.inner
            .repair_hold
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(feature = "test-hooks")]
    pub fn release_repair(&self) {
        self.inner
            .repair_hold
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(feature = "test-hooks")]
    pub(crate) fn repair_held(&self) -> bool {
        self.inner
            .repair_hold
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// The device pulls this node is running.
    pub(crate) fn sync_pulls(&self) -> &self::pull_serve::PullRegistry {
        &self.inner.sync_pulls
    }
}

#[cfg(test)]
mod tests;
