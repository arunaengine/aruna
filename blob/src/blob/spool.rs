//! Durable spool budget charges of transient hidden spools.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::BlobHandler;
use super::backend::build_hidden_path;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::handle::Handle as _;
use aruna_core::keyspaces::SPOOL_CHARGE_KEYSPACE;
use byteview::ByteView;
use tokio::runtime::Handle;
use tokio::sync::OwnedSemaphorePermit;
use ulid::Ulid;

const CHARGE_PAGE: usize = 256;

/// The charge a hidden spool path belongs to: the path without its `_<ulid>` suffix.
pub(super) fn charge_key(backend_path: &str) -> Option<&str> {
    backend_path.rsplit_once('_').map(|(prefix, _)| prefix)
}

/// Settles a charge whose spool did not finish: released once no spool bytes remain.
pub(super) struct ChargeSettle {
    pub(super) handler: BlobHandler,
    pub(super) namespace: Ulid,
    pub(super) charge: String,
    pub(super) armed: bool,
}

impl Drop for ChargeSettle {
    fn drop(&mut self) {
        self.handler.mark_live(&self.charge, false);
        if !self.armed {
            return;
        }
        let Ok(runtime) = Handle::try_current() else {
            tracing::warn!("transient spool charge kept until its spool is swept");
            return;
        };
        let handler = self.handler.clone();
        let (namespace, charge) = (self.namespace, std::mem::take(&mut self.charge));
        runtime.spawn(async move { handler.settle_charge(namespace, &charge).await });
    }
}

impl BlobHandler {
    /// Persists `units` MiB charged to the spools of `charge` and holds `permit` for them.
    pub(super) async fn hold_charge(
        &self,
        (namespace, name): (Ulid, &str),
        units: u32,
        permit: OwnedSemaphorePermit,
    ) -> Result<String, BlobError> {
        let path = build_hidden_path(namespace, name, Ulid::nil())?;
        let Some(charge) = charge_key(&path).map(str::to_string) else {
            return Err(BlobError::WriteError(
                "spool path has no charge".to_string(),
            ));
        };
        let value = postcard::to_allocvec(&units).map_err(|error| {
            BlobError::WriteError(format!("spool charge does not encode: {error}"))
        })?;
        let write = StorageEffect::Write {
            key_space: SPOOL_CHARGE_KEYSPACE.to_string(),
            key: ByteView::from(charge.as_bytes().to_vec()),
            value: ByteView::from(value),
            txn_id: None,
        };
        match self.storage.send_effect(Effect::Storage(write)).await {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => {
                return Err(BlobError::WriteError(format!(
                    "spool charge was not stored: {other:?}"
                )));
            }
        }
        if let Ok(mut held) = self.transient_spools.lock() {
            held.insert(charge.clone(), permit);
        }
        Ok(charge)
    }

    /// Marks the spools of `charge` in use, so the sweep neither lists nor deletes them.
    pub(super) fn mark_live(&self, charge: &str, live: bool) {
        if let Ok(mut spools) = self.live_spools.lock() {
            match live {
                true => spools.insert(charge.to_string()),
                false => spools.remove(charge),
            };
        }
    }

    /// Whether the transient spool at `backend_path` is being written or read.
    pub(super) fn spool_live(&self, backend_path: &str) -> bool {
        charge_key(backend_path).is_some_and(|charge| {
            self.live_spools
                .lock()
                .is_ok_and(|spools| spools.contains(charge))
        })
    }

    /// Leaves spools in use out of a hidden listing, so the sweep never reclaims them.
    pub(super) fn hide_live(&self, event: BlobEvent) -> BlobEvent {
        match event {
            BlobEvent::HiddenListed {
                mut entries,
                next_cursor,
            } => {
                entries.retain(|entry| !self.spool_live(&entry.key.backend_path));
                BlobEvent::HiddenListed {
                    entries,
                    next_cursor,
                }
            }
            other => other,
        }
    }

    /// Whether `charge` is still held by this node.
    pub(super) fn charged(&self, charge: &str) -> bool {
        self.transient_spools
            .lock()
            .is_ok_and(|held| held.contains_key(charge))
    }

    /// Deletes the stored charge, then frees its budget.
    pub(super) async fn release_charge(&self, charge: &str) -> Result<(), BlobError> {
        let delete = StorageEffect::Delete {
            key_space: SPOOL_CHARGE_KEYSPACE.to_string(),
            key: ByteView::from(charge.as_bytes().to_vec()),
            txn_id: None,
        };
        match self.storage.send_effect(Effect::Storage(delete)).await {
            Event::Storage(StorageEvent::DeleteResult { .. }) => {}
            other => {
                return Err(BlobError::DeleteError(format!(
                    "spool charge was not deleted: {other:?}"
                )));
            }
        }
        if let Ok(mut held) = self.transient_spools.lock() {
            held.remove(charge);
        }
        Ok(())
    }

    /// Releases `charge` when no spool object or reservation of it is left; else the sweep's
    /// deletion releases it later.
    pub(super) async fn settle_charge(&self, namespace: Ulid, charge: &str) {
        let result = match self.spool_remains(namespace, charge).await {
            Ok(false) => self.release_charge(charge).await,
            Ok(true) => Ok(()),
            Err(error) => Err(error),
        };
        if let Err(error) = result {
            tracing::warn!(%error, "transient spool charge kept until its spool is swept");
        }
    }

    /// Whether any hidden object or reservation of `charge` still exists.
    async fn spool_remains(&self, namespace: Ulid, charge: &str) -> Result<bool, BlobError> {
        let mut cursor = None;
        loop {
            let (entries, next) = match self.list_hidden_blobs(Some(namespace), cursor).await {
                BlobEvent::HiddenListed {
                    entries,
                    next_cursor,
                } => (entries, next_cursor),
                BlobEvent::Error(error) => return Err(error),
                other => return Err(BlobError::ListError(format!("{other:?}"))),
            };
            if entries
                .iter()
                .any(|entry| charge_key(&entry.key.backend_path) == Some(charge))
            {
                return Ok(true);
            }
            match next {
                Some(next) => cursor = Some(next),
                None => return Ok(false),
            }
        }
    }

    /// Takes the stored charges of spools a previous run left behind out of the budget, and
    /// drops charges whose spools are gone.
    pub(super) async fn restore_charges(&self) -> Result<(), BlobError> {
        let mut start = None;
        loop {
            let iter = StorageEffect::Iter {
                key_space: SPOOL_CHARGE_KEYSPACE.to_string(),
                prefix: None,
                start: start.take().map(IterStart::After),
                limit: CHARGE_PAGE,
                txn_id: None,
            };
            let (rows, next) = match self.storage.send_effect(Effect::Storage(iter)).await {
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) => (values, next_start_after),
                other => return Err(BlobError::ReadError(format!("{other:?}"))),
            };
            for (key, value) in rows {
                self.restore_charge(&key, &value).await?;
            }
            match next {
                Some(next) => start = Some(next),
                None => return Ok(()),
            }
        }
    }

    async fn restore_charge(&self, key: &[u8], value: &[u8]) -> Result<(), BlobError> {
        let charge = String::from_utf8_lossy(key).into_owned();
        let namespace = charge
            .split('/')
            .nth(1)
            .and_then(|namespace| Ulid::from_string(namespace).ok());
        let units = postcard::from_bytes::<u32>(value).ok();
        let (Some(namespace), Some(units)) = (namespace, units) else {
            tracing::warn!(charge, "dropping malformed transient spool charge");
            return self.release_charge(&charge).await;
        };
        // A spool that cannot be checked keeps its charge until the sweep deletes it.
        if !self.spool_remains(namespace, &charge).await.unwrap_or(true) {
            return self.release_charge(&charge).await;
        }
        match self.spool_budget.clone().try_acquire_many_owned(units) {
            Ok(permit) => {
                if let Ok(mut held) = self.transient_spools.lock() {
                    held.insert(charge, permit);
                }
            }
            Err(_) => tracing::warn!(charge, "transient spool charge exceeds the spool budget"),
        }
        Ok(())
    }
}
