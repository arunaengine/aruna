//! Asks a realm node to pull one device version and polls the pull until it ends.
//! A pull is judged by the bytes it receives, never by how long it takes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::future::Future;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::metadata::AuthToken;
use aruna_core::structs::{SyncPull, SyncPullAck, SyncPullStatus, SyncRefusal};
use tracing::warn;

use crate::driver::DriverContext;
use crate::metadata::protocol::MetadataTransportMessage;

/// Delay before the first poll; it doubles up to [`PULL_POLL_AFTER`], so a small file settles
/// within a few quick polls.
const FIRST_POLL_AFTER: Duration = Duration::from_millis(50);

/// Delay between two polls of a running pull.
pub const PULL_POLL_AFTER: Duration = Duration::from_secs(1);

/// Polls in a row at the full delay a running pull may report no new bytes before it counts as
/// stalled.
pub const STALL_POLLS: u32 = 10;

/// Asks `node_id` to pull one device version, then polls until the pull is done or refused.
/// `progress` hears every new byte count. A pull that keeps receiving bytes runs as long as it
/// needs; one that stops, or a node that cannot be reached, ends as `Unavailable`.
pub(crate) async fn forward_pull<F, Fut>(
    context: &DriverContext,
    node_id: NodeId,
    auth_token: AuthToken,
    pull: SyncPull,
    mut progress: F,
) -> Result<SyncPullAck, SyncRefusal>
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = ()>,
{
    let request = || MetadataTransportMessage::ForwardSyncPull {
        auth_token: auth_token.clone(),
        pull: Box::new(pull.clone()),
    };
    let mut status = exchange(context, node_id, request())
        .await
        .map_err(|reason| {
            warn!(node = %node_id, reason = %reason, "Could not ask the realm node for a pull");
            SyncRefusal::Unavailable
        })?;
    let mut watch = Progress::default();
    loop {
        let (bytes_done, total, ask_again) = match status {
            SyncPullStatus::Done(ack) => return Ok(ack),
            SyncPullStatus::Refused(refusal) => return Err(refusal),
            SyncPullStatus::Pending { bytes_done, total } => (bytes_done, total, false),
            // The node lost the pull, as after a restart; it is asked again.
            SyncPullStatus::Unknown => (watch.seen, u64::MAX, true),
        };
        if bytes_done != watch.seen {
            progress(bytes_done).await;
        }
        if watch.stalled(bytes_done, total) {
            warn!(node = %node_id, bytes_done, "A synced-folder pull stopped receiving bytes");
            return Err(SyncRefusal::Unavailable);
        }
        tokio::time::sleep(watch.wait).await;
        let message = match ask_again {
            true => request(),
            false => MetadataTransportMessage::ForwardPullStatus {
                auth_token: auth_token.clone(),
                pull: Box::new(pull.clone()),
            },
        };
        // A lost answer is one more poll without new bytes.
        let pending = SyncPullStatus::Pending {
            bytes_done: watch.seen,
            total: u64::MAX,
        };
        status = exchange(context, node_id, message).await.unwrap_or(pending);
    }
}

/// Sends one pull request or poll; any answer but a pull status is a failed exchange.
async fn exchange(
    context: &DriverContext,
    node_id: NodeId,
    message: MetadataTransportMessage,
) -> Result<SyncPullStatus, String> {
    let Some(metadata) = context.metadata_handle.as_ref() else {
        return Err("the metadata handle is missing".to_string());
    };
    match metadata.request_forwarded_write(node_id, message).await {
        Ok(MetadataTransportMessage::ForwardedSyncPull { status }) => Ok(status),
        Ok(other) => Err(crate::metadata::transport_message_kind(&other).to_string()),
        Err(error) => Err(error.to_string()),
    }
}

/// What the polls of one pull have seen so far.
#[derive(Debug, PartialEq)]
struct Progress {
    seen: u64,
    stalls: u32,
    wait: Duration,
}

impl Default for Progress {
    fn default() -> Self {
        Self {
            seen: 0,
            stalls: 0,
            wait: FIRST_POLL_AFTER,
        }
    }
}

impl Progress {
    /// Takes one answer and sets the next delay; true once the pull has stalled. Every byte in
    /// means the node is committing, which is progress too.
    fn stalled(&mut self, bytes_done: u64, total: u64) -> bool {
        let moved = bytes_done != self.seen || bytes_done >= total;
        let ramping = self.wait < PULL_POLL_AFTER;
        self.seen = bytes_done;
        self.wait = self.wait.saturating_mul(2).min(PULL_POLL_AFTER);
        self.stalls = match (moved, ramping) {
            (true, _) => 0,
            (false, true) => self.stalls,
            (false, false) => self.stalls.saturating_add(1),
        };
        self.stalls >= STALL_POLLS
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A watch past its quick first polls.
    fn settled() -> Progress {
        let mut watch = Progress::default();
        while watch.wait < PULL_POLL_AFTER {
            watch.stalled(0, 100);
        }
        watch
    }

    #[test]
    fn follows_moving_pull() {
        // A slow pull that keeps receiving bytes is polled on for as long as it moves.
        let mut watch = settled();
        for bytes_done in 1..=3 * u64::from(STALL_POLLS) {
            assert!(!watch.stalled(bytes_done, 1_000));
        }
        assert_eq!(watch.wait, PULL_POLL_AFTER);
    }

    #[test]
    fn stops_stalled_pull() {
        // Only polls at the full delay that see no new bytes count toward a stall.
        let mut watch = Progress::default();
        let mut quick = 0;
        while watch.wait < PULL_POLL_AFTER {
            assert!(!watch.stalled(0, 100));
            quick += 1;
        }
        assert!(quick > 0);
        for _ in 1..STALL_POLLS {
            assert!(!watch.stalled(0, 100));
        }
        assert!(watch.stalled(0, 100));
    }

    #[test]
    fn waits_for_commit() {
        // A node that has every byte is committing and is never stalled.
        let mut watch = settled();
        for _ in 0..3 * STALL_POLLS {
            assert!(!watch.stalled(100, 100));
        }
    }
}
