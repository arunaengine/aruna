//! Decides deliveries from this node's view of a user's notification settings and asks a
//! management node to open the sealed secret.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::compute::SecretBytes;
use aruna_core::document::DocumentTarget;
use aruna_core::structs::execution::job::JobError;
use aruna_core::structs::execution::notification::NotificationRecord;
use aruna_core::structs::execution::setting::SettingChannel;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
use aruna_outbound::Backoffs;
use thiserror::Error;
use tracing::warn;

use crate::driver::{DriverContext, drive};
use crate::metadata::protocol::MetadataReadError;
use crate::notifications::setting_rows::ReadSettingOperation;

/// The first delay after no management node opened a secret; it doubles up to `BACKOFF_CAP_MS`.
pub(crate) const BACKOFF_START_MS: u64 = 30_000;
const BACKOFF_CAP_MS: u64 = 30 * 60_000;

/// The current setting with its secret opened when it is on.
#[derive(Clone, PartialEq, Eq)]
pub struct SettingWinner {
    pub bytes: Vec<u8>,
    pub secret: Option<SecretBytes>,
}

impl std::fmt::Debug for SettingWinner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SettingWinner")
            .field("bytes_len", &self.bytes.len())
            .field("has_secret", &self.secret.is_some())
            .finish()
    }
}

/// Opens the sealed secret of a setting on a management node, locally or over the metadata
/// transport.
#[async_trait::async_trait]
pub trait SettingPeers: Send + Sync {
    async fn open(
        &self,
        node: NodeId,
        target: DocumentTarget,
        bytes: Vec<u8>,
    ) -> Result<SecretBytes, MetadataReadError>;
}

/// Opens on this node when it is the asked one, else over the metadata transport.
pub struct MetadataPeers<'a> {
    pub ctx: &'a DriverContext,
    pub local: NodeId,
}

#[async_trait::async_trait]
impl SettingPeers for MetadataPeers<'_> {
    async fn open(
        &self,
        node: NodeId,
        target: DocumentTarget,
        bytes: Vec<u8>,
    ) -> Result<SecretBytes, MetadataReadError> {
        let metadata = self.ctx.metadata_handle.as_ref();
        if node == self.local {
            let key = metadata.and_then(|metadata| metadata.realm_seal_key());
            let served =
                crate::realm_vault::serve_open(self.ctx, key.as_deref(), self.local, target, bytes);
            return Box::pin(served).await;
        }
        let metadata = metadata.ok_or(MetadataReadError::Unavailable)?;
        metadata.request_open(node, target, bytes).await
    }
}

/// The management nodes of `config`, in config order.
pub fn managers(config: &RealmConfigDocument) -> Vec<NodeId> {
    (config.nodes.iter())
        .filter(|node| node.kind == RealmNodeKind::Management)
        .filter_map(|node| node.node_id.parse::<NodeId>().ok())
        .collect()
}

/// The next delay while no management node opens a secret: it starts at `BACKOFF_START_MS` and
/// doubles up to `BACKOFF_CAP_MS`.
fn next_delay(previous_ms: Option<u64>) -> u64 {
    previous_ms.map_or(BACKOFF_START_MS, |delay| {
        delay.saturating_mul(2).min(BACKOFF_CAP_MS)
    })
}

/// What a delivery attempt does.
#[derive(Debug)]
pub enum Delivery {
    /// The notice expired, or the current setting on this node is off or missing.
    Skip,
    /// Try again after the delay without spending an attempt.
    Wait(JobError, Duration),
    /// Send with the current setting and its opened secret.
    Send(SettingWinner),
}

/// Decides one delivery from this node's current setting. When it is on, the management nodes
/// are asked in config order to open its secret and the first answer is used. `managers` is
/// `None` when the realm config could not be read, which counts as no answer.
pub async fn delivery_setting(
    ctx: &DriverContext,
    peers: &dyn SettingPeers,
    backoffs: &Backoffs,
    record: &NotificationRecord,
    channel: SettingChannel,
    managers: Option<&[NodeId]>,
    now_ms: u64,
) -> Delivery {
    let expires_ms = record.expires_at_ms();
    if now_ms >= expires_ms {
        return Delivery::Skip;
    }
    let owner = record.recipient;
    let wait = |until_ms: u64, message: &str| {
        let delay = until_ms.min(expires_ms).saturating_sub(now_ms);
        Delivery::Wait(JobError::retryable(message), Duration::from_millis(delay))
    };
    let current = match drive(ReadSettingOperation::new(channel, owner), ctx).await {
        Ok(Some(bytes)) => bytes,
        Ok(None) => return Delivery::Skip,
        Err(error) => {
            warn!(%error, "Reading the notification setting failed");
            return wait(
                backoffs.fail(owner, channel, now_ms, next_delay),
                "the setting read failed",
            );
        }
    };
    if let Some(until_ms) = backoffs.until(owner, channel, now_ms) {
        return wait(until_ms, "waiting for a management node");
    }
    let (Ok(head), Some(managers)) = (channel.head(&current), managers) else {
        let until_ms = backoffs.fail(owner, channel, now_ms, next_delay);
        return wait(until_ms, "the realm configuration cannot be read now");
    };
    let target = channel.target(owner, head.id);
    for node in managers {
        match peers.open(*node, target.clone(), current.clone()).await {
            Ok(secret) => {
                backoffs.clear(owner, channel);
                return Delivery::Send(SettingWinner {
                    bytes: current,
                    secret: Some(secret),
                });
            }
            Err(error) => warn!(%node, ?error, "A management node did not open a setting"),
        }
    }
    let until_ms = backoffs.fail(owner, channel, now_ms, next_delay);
    wait(until_ms, "no management node opened the setting secret")
}

#[cfg(test)]
#[path = "settings_tests.rs"]
mod tests;
