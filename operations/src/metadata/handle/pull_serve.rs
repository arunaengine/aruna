//! Serves a device's sync requests on this node: admits a pull, runs it in the background
//! while the device polls, and lists heads. The decisions are operations; this file only
//! drives them and keeps the pulls in flight.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use aruna_core::id::NodeId;
use aruna_core::metadata::AuthToken;
use aruna_core::structs::{SyncPull, SyncPullAck, SyncPullStatus, SyncRefusal};
use aruna_core::types::Key;
use tracing::warn;

use crate::driver::{DriverContext, drive, node_routing};
use crate::metadata::device_pull::{
    Admission, AdmitPullOperation, CommitPullOperation, ListHeadsOperation, PullCallerOperation,
    ReceiptOperation, ServingNode, SetListing, receipt_of, refusal_kind, refused,
};
use crate::metadata::protocol::MetadataTransportMessage;

/// Pulls this node is running, by receipt key. The registry lives as long as the process, so a
/// restart forgets a running pull and the device asks for it again.
#[derive(Debug, Default)]
pub(crate) struct PullRegistry(Mutex<HashMap<Key, PullTask>>);

#[derive(Debug)]
enum PullTask {
    Running {
        bytes_done: Arc<AtomicU64>,
        total: u64,
    },
    /// Kept until the device asks once, so it learns why the pull ended.
    Refused(SyncRefusal),
}

impl PullRegistry {
    fn pulls(&self) -> std::sync::MutexGuard<'_, HashMap<Key, PullTask>> {
        self.0.lock().unwrap_or_else(|error| error.into_inner())
    }

    /// Claims `key` for a new run, or answers with the run that already holds it.
    fn claim(&self, key: Key, total: u64) -> Result<Arc<AtomicU64>, SyncPullStatus> {
        let mut pulls = self.pulls();
        if let Some(PullTask::Running { bytes_done, total }) = pulls.get(&key) {
            let bytes_done = bytes_done.load(Ordering::Relaxed);
            return Err(SyncPullStatus::Pending {
                bytes_done,
                total: *total,
            });
        }
        let bytes_done = Arc::new(AtomicU64::new(0));
        let running = PullTask::Running {
            bytes_done: bytes_done.clone(),
            total,
        };
        pulls.insert(key, running);
        Ok(bytes_done)
    }

    /// Ends a run. A commit is answered by its receipt from now on, a refusal by its entry.
    fn finish(&self, key: Key, result: &Result<SyncPullAck, SyncRefusal>) {
        let mut pulls = self.pulls();
        match result {
            Ok(_) => pulls.remove(&key),
            Err(refusal) => pulls.insert(key, PullTask::Refused(refusal.clone())),
        };
    }

    /// The run `key` names. A refusal is handed out once.
    fn status(&self, key: &Key) -> Option<SyncPullStatus> {
        let mut pulls = self.pulls();
        if let PullTask::Running { bytes_done, total } = pulls.get(key)? {
            let bytes_done = bytes_done.load(Ordering::Relaxed);
            return Some(SyncPullStatus::Pending {
                bytes_done,
                total: *total,
            });
        }
        match pulls.remove(key) {
            Some(PullTask::Refused(refusal)) => Some(SyncPullStatus::Refused(refusal)),
            _ => None,
        }
    }
}

/// This node and its realm; a pull cannot be attributed without them.
fn serving(context: &DriverContext) -> Result<ServingNode, SyncRefusal> {
    let net = context
        .net_handle
        .as_ref()
        .ok_or(SyncRefusal::Unavailable)?;
    Ok((net.node_id(), *net.realm_id()))
}

/// Serves one forwarded sync pull.
pub(crate) async fn serve_sync_pull(
    context: &Arc<DriverContext>,
    peer: NodeId,
    message: MetadataTransportMessage,
) -> MetadataTransportMessage {
    let MetadataTransportMessage::ForwardSyncPull { auth_token, pull } = message else {
        return MetadataTransportMessage::Reject("unexpected metadata control message".to_string());
    };
    let status = accept_pull(context, peer, auth_token, *pull).await;
    MetadataTransportMessage::ForwardedSyncPull { status }
}

/// Serves one poll of an accepted sync pull.
pub(crate) async fn serve_pull_status(
    context: &Arc<DriverContext>,
    peer: NodeId,
    message: MetadataTransportMessage,
) -> MetadataTransportMessage {
    let MetadataTransportMessage::ForwardPullStatus { auth_token, pull } = message else {
        return MetadataTransportMessage::Reject("unexpected metadata control message".to_string());
    };
    let status = pull_status(context, peer, auth_token, &pull).await;
    MetadataTransportMessage::ForwardedSyncPull { status }
}

/// Starts pulling one device version of `peer` and answers at once: a replay with its
/// receipt, a refusal with its reason, otherwise the running pull, which the device polls.
pub async fn accept_pull(
    context: &Arc<DriverContext>,
    peer: NodeId,
    auth_token: AuthToken,
    request: SyncPull,
) -> SyncPullStatus {
    let node = match serving(context) {
        Ok(node) => node,
        Err(refusal) => return SyncPullStatus::Refused(refused(peer, &request, refusal)),
    };
    let caller = PullCallerOperation::new((peer, node.1), auth_token, request.clone());
    let auth = match drive(caller, context.as_ref()).await {
        Ok(auth) => auth,
        Err(refusal) => return SyncPullStatus::Refused(refused(peer, &request, refusal)),
    };
    let admit = AdmitPullOperation::new(node.0, auth.clone(), request.clone());
    let bucket = match drive(admit, context.as_ref()).await {
        Ok(Admission::Replayed(ack)) => return SyncPullStatus::Done(ack),
        Ok(Admission::Admitted(bucket)) => bucket,
        Err(refusal) => return SyncPullStatus::Refused(refused(peer, &request, refusal)),
    };
    let (Some(metadata), Ok(key)) = (context.metadata_handle.as_ref(), receipt_of(&request)) else {
        let refusal = refused(peer, &request, SyncRefusal::Unavailable);
        return SyncPullStatus::Refused(refusal);
    };
    let bytes_done = match metadata.sync_pulls().claim(key.clone(), request.size) {
        Ok(bytes_done) => bytes_done,
        Err(running) => return running,
    };
    let total = request.size;
    let context = context.clone();
    tokio::spawn(async move {
        let bucket_name = request.target_bucket.clone();
        let commit = CommitPullOperation::new(
            (node.0, node_routing(&context)),
            (auth, bucket),
            request,
            bytes_done,
        );
        let result = drive(commit, context.as_ref()).await;
        if let Err(refusal) = &result {
            warn!(peer = %peer, bucket = %bucket_name, refusal = refusal_kind(refusal), "A synced-folder pull failed");
        }
        if let Some(metadata) = context.metadata_handle.as_ref() {
            metadata.sync_pulls().finish(key, &result);
        }
    });
    SyncPullStatus::Pending {
        bytes_done: 0,
        total,
    }
}

/// Where the pull of one device version of `peer` stands on this node.
pub async fn pull_status(
    context: &Arc<DriverContext>,
    peer: NodeId,
    auth_token: AuthToken,
    request: &SyncPull,
) -> SyncPullStatus {
    let realm_id = match serving(context) {
        Ok((_, realm_id)) => realm_id,
        Err(refusal) => return SyncPullStatus::Refused(refusal),
    };
    let caller = PullCallerOperation::new((peer, realm_id), auth_token, request.clone());
    let checked = drive(caller, context.as_ref()).await;
    let key = match checked.and_then(|_| receipt_of(request)) {
        Ok(key) => key,
        Err(refusal) => return SyncPullStatus::Refused(refusal),
    };
    let pulls = context.metadata_handle.as_ref().map(|m| m.sync_pulls());
    if let Some(status) = pulls.and_then(|pulls| pulls.status(&key)) {
        return status;
    }
    match drive(ReceiptOperation::new(request), context.as_ref()).await {
        Ok(Some(version_id)) => SyncPullStatus::Done(SyncPullAck {
            version_id,
            already_applied: true,
        }),
        _ => SyncPullStatus::Unknown,
    }
}

/// Serves one bounded listing of the current heads under a bucket prefix.
pub(crate) async fn serve_list_versions(
    context: &Arc<DriverContext>,
    peer: NodeId,
    message: MetadataTransportMessage,
) -> MetadataTransportMessage {
    let MetadataTransportMessage::ForwardListVersions {
        auth_token,
        bucket,
        prefix,
        cursor,
        limit,
    } = message
    else {
        return MetadataTransportMessage::Reject("unexpected metadata control message".to_string());
    };
    let listing = SetListing {
        bucket: bucket.clone(),
        prefix,
        cursor,
        limit,
    };
    let result = match serving(context) {
        Ok(node) => {
            let list = ListHeadsOperation::new(peer, node, auth_token, listing);
            drive(list, context.as_ref()).await
        }
        Err(refusal) => Err(refusal),
    };
    if let Err(refusal) = &result {
        warn!(peer = %peer, bucket = %bucket, refusal = refusal_kind(refusal), "Refused a synced-folder listing");
    }
    MetadataTransportMessage::ForwardedVersions { result }
}
