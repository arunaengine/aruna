use std::time::{Duration, Instant};

use aruna_core::structs::{RealmConfigDocument, RealmId};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::get_realm_config::GetRealmConfigOperation;
use tokio::sync::RwLock;

/// Short TTL for the realm-config read-through cache on the quota write path.
/// Every PUT/CMU/staging write resolves the group ceiling and the active node
/// set from the realm config; without a cache each drives a fresh
/// `GetRealmConfigOperation`. Enforcement lag after a quota change is bounded by
/// this TTL, well inside the snapshot-staleness budget the grace headroom covers.
pub const QUOTA_CONFIG_CACHE_TTL: Duration = Duration::from_secs(2);

/// Read-through cache of the realm config for the quota write path.
#[derive(Debug, Default)]
pub struct QuotaConfigCache {
    inner: RwLock<Option<(Instant, RealmConfigDocument)>>,
}

impl QuotaConfigCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the realm config, serving the cached copy while it is within the
    /// TTL and otherwise loading and caching a fresh one.
    pub async fn get(
        &self,
        ctx: &DriverContext,
        realm_id: RealmId,
    ) -> Result<RealmConfigDocument, String> {
        if let Some((fetched_at, config)) = self.inner.read().await.as_ref()
            && fetched_at.elapsed() < QUOTA_CONFIG_CACHE_TTL
        {
            return Ok(config.clone());
        }
        let config = drive(GetRealmConfigOperation::new(realm_id), ctx)
            .await
            .map_err(|err| err.to_string())?;
        *self.inner.write().await = Some((Instant::now(), config.clone()));
        Ok(config)
    }

    /// Drops the cached entry so the next read reloads. Called locally when this
    /// node applies a realm quota change.
    pub async fn invalidate(&self) {
        *self.inner.write().await = None;
    }
}
