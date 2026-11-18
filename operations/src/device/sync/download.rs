//! Fetches planned remote versions from the folder's realm node and writes each one under its
//! guard; a failed fetch is recorded on the entry's base row.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::SYNC_BASE_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{done, read_row, value};
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_core::structs::{EntryState, SyncBase, SyncedFolder};
use aruna_core::time::unix_timestamp_millis;
use bytes::Bytes;
use thiserror::Error;
use tracing::{debug, warn};
use ulid::Ulid;

use crate::replication::bao_read::{BaoReadError, BaoReadOutput};
use crate::replication::managed_read::ManagedReadOperation;
use crate::replication::protocol::{BaoReadRefusal, BaoReadRequest, BaoReadTarget};

use super::materialize::{
    MaterializeEntryOperation, MaterializeError, MaterializeInput, MaterializeOutcome,
};
use super::reconcile::Download;
use super::repository::{base_entry, base_key};

pub(super) type Blob = BackendStream<Result<Bytes, StreamError>>;

/// Why a remote version could not be read; the text is shown on the entry.
#[derive(Debug, Error, PartialEq)]
#[error("{0}")]
pub struct FetchError(String);

impl From<IoError> for FetchError {
    fn from(error: IoError) -> Self {
        Self(error.to_string())
    }
}

/// Reads one remote version from the folder's realm node. A member keeping only the record of
/// a distributed version names a holder of its bytes, which is asked once.
#[derive(Debug, PartialEq)]
pub(super) struct FetchRemoteOperation {
    realm_id: RealmId,
    node_id: NodeId,
    owner: aruna_core::UserId,
    bucket: String,
    key: String,
    version: Ulid,
    request: Option<BaoReadRequest>,
    redirected: bool,
}

impl FetchRemoteOperation {
    pub(super) fn new(
        realm_id: RealmId,
        folder: &SyncedFolder,
        relative: &str,
        version: Ulid,
    ) -> Self {
        Self {
            realm_id,
            node_id: folder.remote.node_id,
            owner: folder.created_by,
            bucket: folder.remote.bucket.clone(),
            key: folder.remote.remote_key(relative),
            version,
            request: None,
            redirected: false,
        }
    }
}

impl Operation for FetchRemoteOperation {
    type Output = Blob;
    type Error = FetchError;

    fn first(&mut self) -> Result<Next<Blob>, FetchError> {
        let source = VersionedObjectArn::new(
            self.realm_id,
            self.node_id,
            self.bucket.clone(),
            self.key.clone(),
            self.version,
        )
        .map_err(|error| FetchError(error.to_string()))?;
        let request = BaoReadRequest {
            auth_context: AuthContext {
                user_id: self.owner,
                realm_id: self.realm_id,
                path_restrictions: None,
                session: None,
            },
            realm_id: self.realm_id,
            target: BaoReadTarget::ExactVersion(source),
            expected_blake3: None,
            metadata_only: false,
            destination: None,
            known_refs: Vec::new(),
            range: None,
        };
        self.request = Some(request.clone());
        let read = ManagedReadOperation::new(self.node_id, request);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Blob>, FetchError> {
        match result.child::<BaoReadOutput, BaoReadError>()? {
            Ok(BaoReadOutput::Stream { blob, .. }) => Ok(Next::Done(blob)),
            Ok(BaoReadOutput::Metadata { .. }) => Err(FetchError(
                "the remote version returned no bytes".to_string(),
            )),
            Err(BaoReadError::Refused(BaoReadRefusal::HeldBy { holder, set_id }))
                if !self.redirected =>
            {
                self.redirected = true;
                let request = self.request.take().ok_or(IoError::Unfinished)?;
                let held = BaoReadRequest {
                    target: BaoReadTarget::Version {
                        set_id,
                        version_id: self.version,
                    },
                    ..request
                };
                let read = ManagedReadOperation::new(holder, held);
                Ok(Next::Call(IoCall::child(read)))
            }
            Err(error) => {
                debug!(key = %self.key, %error, "Could not read the remote version");
                Err(FetchError(error.to_string()))
            }
        }
    }
}

/// What the pending call of applying the downloads answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DownloadStep {
    Fetch,
    Write,
    Mark,
}

/// Fetches every planned remote version and writes it under its guard. Answers how many
/// entries reached the disk.
#[derive(Debug, PartialEq)]
pub struct ApplyDownloadsOperation {
    realm_id: RealmId,
    folder: SyncedFolder,
    downloads: VecDeque<Download>,
    current: Option<Download>,
    step: DownloadStep,
    applied: usize,
}

type DownloadTurn = Result<Next<usize>, IoError>;

impl ApplyDownloadsOperation {
    pub fn new(realm_id: RealmId, folder: SyncedFolder, downloads: Vec<Download>) -> Self {
        Self {
            realm_id,
            folder,
            downloads: downloads.into(),
            current: None,
            step: DownloadStep::Fetch,
            applied: 0,
        }
    }

    fn next_download(&mut self) -> DownloadTurn {
        let Some(download) = self.downloads.pop_front() else {
            return Ok(Next::Done(self.applied));
        };
        let fetch = FetchRemoteOperation::new(
            self.realm_id,
            &self.folder,
            &download.relative,
            download.remote_version,
        );
        self.current = Some(download);
        self.step = DownloadStep::Fetch;
        Ok(Next::Call(IoCall::child(fetch)))
    }

    fn write(&mut self, blob: Blob) -> DownloadTurn {
        let download = self.current.take().ok_or(IoError::Unfinished)?;
        let write = MaterializeEntryOperation::new(MaterializeInput {
            folder: self.folder.clone(),
            relative: download.relative.clone(),
            remote_version: download.remote_version,
            guard: download.guard,
            conflicted: download.conflicted,
            local_fingerprint: (download.local.as_ref()).and_then(|side| side.fingerprint.clone()),
            local: download.local,
            remote: None,
            synced: download.synced,
            audit: None,
            blob,
        });
        self.step = DownloadStep::Write;
        Ok(Next::Call(IoCall::child(write)))
    }

    fn mark(&mut self, message: String) -> DownloadTurn {
        let download = self.current.take().ok_or(IoError::Unfinished)?;
        let relative = download.relative;
        warn!(%relative, %message, "Could not fetch the folder entry");
        self.step = DownloadStep::Mark;
        let mark = MarkFailedOperation::new(self.folder.folder_id, relative, message);
        Ok(Next::Call(IoCall::child(mark)))
    }

    fn written(&mut self, outcome: Result<MaterializeOutcome, MaterializeError>) {
        match outcome {
            Ok(MaterializeOutcome::Written | MaterializeOutcome::Copied { .. }) => {
                self.applied += 1;
            }
            Ok(MaterializeOutcome::Refused { reason }) => {
                debug!(?reason, "Kept the local file");
            }
            Ok(MaterializeOutcome::Failed { message }) => {
                warn!(%message, "Could not write the folder entry");
            }
            Err(error) => warn!(%error, "Could not write the folder entry"),
        }
    }
}

impl Operation for ApplyDownloadsOperation {
    type Output = usize;
    type Error = IoError;

    fn first(&mut self) -> DownloadTurn {
        self.next_download()
    }

    fn next(&mut self, result: IoResult) -> DownloadTurn {
        match self.step {
            DownloadStep::Fetch => match result.child::<Blob, FetchError>()? {
                Ok(blob) => self.write(blob),
                Err(error) => self.mark(error.to_string()),
            },
            DownloadStep::Write => {
                let outcome = result.child::<MaterializeOutcome, MaterializeError>()?;
                self.written(outcome);
                self.next_download()
            }
            DownloadStep::Mark => {
                if let Err(error) = result.child::<(), IoError>()? {
                    warn!(%error, "Could not store the failed download row");
                }
                self.next_download()
            }
        }
    }
}

/// Records a failed fetch on the entry's base row, when the entry has one.
#[derive(Debug, PartialEq)]
struct MarkFailedOperation {
    folder_id: Ulid,
    relative: String,
    message: String,
    writing: bool,
}

impl MarkFailedOperation {
    fn new(folder_id: Ulid, relative: String, message: String) -> Self {
        Self {
            folder_id,
            relative,
            message,
            writing: false,
        }
    }
}

impl Operation for MarkFailedOperation {
    type Output = ();
    type Error = IoError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<()>, IoError> {
        let key = base_key(self.folder_id, &self.relative);
        Ok(Next::Call(read_row(SYNC_BASE_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        if self.writing {
            done(result)?;
            return Ok(Next::Done(()));
        }
        let Some(bytes) = value(result)? else {
            warn!(relative = %self.relative, "The failed download has no row");
            return Ok(Next::Done(()));
        };
        let codec = |error: aruna_core::errors::ConversionError| IoError::Codec(error.to_string());
        let base = SyncBase::from_bytes(&bytes).map_err(codec)?;
        let base = SyncBase {
            entry: EntryState::Error {
                reason: self.message.clone(),
            },
            synced_at_ms: unix_timestamp_millis(),
            ..base
        };
        let row = base_entry(self.folder_id, &self.relative, &base).map_err(codec)?;
        self.writing = true;
        Ok(Next::Call(IoCall::Write(vec![row])))
    }
}

#[cfg(test)]
#[path = "download_tests.rs"]
mod tests;
