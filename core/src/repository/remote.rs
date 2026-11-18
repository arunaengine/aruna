//! Declares the repository and provider calls the blob backend makes for operations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::net::IpAddr;
use std::ops::Range;
use std::sync::Arc;

use futures::future::BoxFuture;
use serde_json::Value;
use thiserror::Error;
use ulid::Ulid;
use url::Url;

use super::ImportMode;
use crate::UserId;
use crate::credential_encryption::CredentialEncryptionKey;
use crate::id::NodeId;
use crate::structs::checksum::Hasher;
use crate::structs::execution::job::{ArtifactRef, RoCrateLimits};
use crate::structs::execution::source_access::SourceMetadata;
use crate::structs::identity::realm::RealmId;
use crate::structs::storage::blob::BackendLocation;

/// What an operation needs to know about this node; the caller reads it before the run.
#[derive(Clone, Debug, PartialEq)]
pub struct NodeScope {
    pub node: Option<(NodeId, RealmId)>,
    pub credential_key: Option<CredentialEncryptionKey>,
    pub has_blob: bool,
    pub has_tasks: bool,
}

impl NodeScope {
    /// This node, or `fallback` on a node without a net transport.
    pub fn node_or(&self, fallback: NodeId) -> NodeId {
        self.node.map_or(fallback, |(node, _)| node)
    }

    /// The realm this node serves, or `fallback` on a node without a net transport.
    pub fn realm_or(&self, fallback: RealmId) -> RealmId {
        self.node.map_or(fallback, |(_, realm)| realm)
    }
}

/// Why a running job must stop.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StopReason {
    /// The user cancelled the job.
    Cancelled,
    /// The node shuts down; the job runs again later.
    Interrupted,
}

/// Where a job's stop comes from; the job's runtime provides it.
pub trait StopSignal: Send + Sync {
    /// Why the job must stop now, a cancel before a shutdown.
    fn reason(&self) -> Option<StopReason>;
    /// Completes once the job must stop.
    fn stopped(&self) -> BoxFuture<'_, StopReason>;
}

/// The stop signal of one job, carried by the backend calls that end when the job stops.
#[derive(Clone)]
pub struct JobStop(Arc<dyn StopSignal>);

impl JobStop {
    pub fn new(signal: impl StopSignal + 'static) -> Self {
        Self(Arc::new(signal))
    }

    pub fn reason(&self) -> Option<StopReason> {
        self.0.reason()
    }

    pub fn stopped(&self) -> BoxFuture<'_, StopReason> {
        self.0.stopped()
    }
}

impl std::fmt::Debug for JobStop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("JobStop").field(&self.reason()).finish()
    }
}

/// Two stops are equal while they say the same; the signals themselves have no identity.
impl PartialEq for JobStop {
    fn eq(&self, other: &Self) -> bool {
        self.reason() == other.reason()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum InvenioError {
    #[error("invalid repository URL or cross-origin link")]
    InvalidUrl,
    #[error("repository request refused by egress policy")]
    Egress,
    #[error("repository transport failed")]
    Transport,
    #[error("repository returned HTTP {0}")]
    Status(u16),
    #[error("repository response exceeds the metadata limit")]
    Limit,
    #[error("repository returned invalid JSON")]
    Json,
    #[error("repository redirected too often")]
    Redirects,
    #[error("repository tokens require HTTPS outside loopback")]
    InsecureToken,
}

/// A checked repository endpoint with the token and response limit of its calls.
#[derive(Clone, Eq, PartialEq)]
pub struct InvenioAccess {
    endpoint: Url,
    token: Option<String>,
    metadata_limit: u64,
}

/// Shows neither the token nor the endpoint path.
impl std::fmt::Debug for InvenioAccess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InvenioAccess")
            .field("host", &self.endpoint.host_str())
            .field("has_token", &self.token.is_some())
            .field("metadata_limit", &self.metadata_limit)
            .finish()
    }
}

impl InvenioAccess {
    pub fn new(
        endpoint: &str,
        token: Option<String>,
        metadata_limit: u64,
    ) -> Result<Self, InvenioError> {
        let mut endpoint = Url::parse(endpoint).map_err(|_| InvenioError::InvalidUrl)?;
        if !matches!(endpoint.scheme(), "https" | "http")
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(InvenioError::InvalidUrl);
        }
        if token.is_some() && !secure_transport(&endpoint) {
            return Err(InvenioError::InsecureToken);
        }
        let path = format!("{}/", endpoint.path().trim_end_matches('/'));
        endpoint.set_path(&path);
        Ok(Self {
            endpoint,
            token,
            metadata_limit,
        })
    }

    pub fn endpoint(&self) -> &str {
        self.endpoint.as_str()
    }

    pub fn endpoint_url(&self) -> &Url {
        &self.endpoint
    }

    pub fn token(&self) -> Option<&str> {
        self.token.as_deref()
    }

    pub fn metadata_limit(&self) -> u64 {
        self.metadata_limit
    }

    pub fn url(&self, segments: &[&str]) -> Result<Url, InvenioError> {
        let mut url = self.endpoint.clone();
        url.path_segments_mut()
            .map_err(|_| InvenioError::InvalidUrl)?
            .pop_if_empty()
            .extend(segments);
        Ok(url)
    }

    /// A link the repository returned, accepted only below its own API root.
    pub fn link(&self, link: &str) -> Result<Url, InvenioError> {
        let url = self
            .endpoint
            .join(link)
            .map_err(|_| InvenioError::InvalidUrl)?;
        if url.origin() != self.endpoint.origin()
            || !url.path().starts_with(self.endpoint.path())
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
        {
            return Err(InvenioError::InvalidUrl);
        }
        Ok(url)
    }
}

fn secure_transport(url: &Url) -> bool {
    let host = url.host_str().unwrap_or_default();
    url.scheme() == "https"
        || host.eq_ignore_ascii_case("localhost")
        || host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvenioMethod {
    Get,
    Post,
    Put,
}

/// Everything an import archive holds: the crate metadata and each version's record and files.
#[derive(Clone, Debug, PartialEq)]
pub struct ArchiveSpool {
    pub access: InvenioAccess,
    pub namespace: Ulid,
    pub created_by: UserId,
    pub metadata: String,
    pub records: Vec<(Value, Value)>,
    pub limits: RoCrateLimits,
    pub mode: ImportMode,
}

/// One call to a repository, a provider or a stored artifact.
#[derive(Clone, Debug, PartialEq)]
pub enum RemoteCall {
    /// Answers `Done` unless the job must stop.
    Checkpoint,
    /// One JSON request; with `if_match` an update of that revision.
    InvenioJson {
        access: InvenioAccess,
        method: InvenioMethod,
        url: Url,
        body: Option<Value>,
        if_match: Option<u64>,
    },
    /// Deletes a repository resource; a missing one counts as deleted.
    InvenioDelete {
        access: InvenioAccess,
        url: Url,
    },
    InvenioHead {
        access: InvenioAccess,
        url: Url,
    },
    /// Streams a stored range to `url` and answers the digests of the bytes sent.
    InvenioUpload {
        access: InvenioAccess,
        url: Url,
        size: u64,
        source: BackendLocation,
        range: Range<u64>,
    },
    /// Hashes a stored range that may hold at most `limit` bytes.
    HashRange {
        source: BackendLocation,
        range: Range<u64>,
        limit: u64,
    },
    /// The bytes of a stored range.
    ReadRange {
        source: BackendLocation,
        range: Range<u64>,
    },
    /// Writes the import archive into hidden storage while its files download.
    SpoolArchive(Box<ArchiveSpool>),
    /// Fetches one OAI-PMH response as text.
    OaiFetch {
        url: Url,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct RemoteRequest {
    /// When set, the call ends as soon as the job must stop, and does not start after that.
    pub stop: Option<JobStop>,
    pub call: RemoteCall,
}

/// The digests of a file and its size.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileDigests {
    pub hashes: HashMap<String, Vec<u8>>,
    pub crc32: [u8; 4],
    pub blake3: [u8; 32],
    pub size: u64,
}

impl FileDigests {
    pub fn of(hasher: &Hasher, size: u64) -> Self {
        let digests = hasher.finalize();
        Self {
            hashes: hasher.to_map(),
            crc32: digests.crc32,
            blake3: *digests.blake3.as_bytes(),
            size,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum RemoteAnswer {
    Done,
    Json(Value),
    Metadata(SourceMetadata),
    Digests(FileDigests),
    Bytes(Vec<u8>),
    Artifact(ArtifactRef),
    Text(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum RemoteFailure {
    #[error("the job must stop: {0:?}")]
    Stopped(StopReason),
    #[error(transparent)]
    Invenio(InvenioError),
    #[error("{0}")]
    Permanent(String),
    #[error("{0}")]
    Retryable(String),
}
