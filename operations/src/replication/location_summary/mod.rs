//! Answers "which copy of a version does a node hold": locally, from a peer,
//! and from the replication queue.

mod local;
mod queued;
mod relationships;
mod remote;

use aruna_core::errors::{BlobError, ConversionError};
use thiserror::Error;

pub use local::{LocalSummary, LocationSummaryOperation};
pub use queued::{QueuedReplicaNodesOperation, QueuedReplicas};
pub use relationships::{RelationshipReplicaNodesOperation, ReplicaTarget};
pub use remote::RemoteLocationSummaryOperation;

#[derive(Debug, Error, PartialEq)]
pub enum LocationSummaryError {
    #[error(transparent)]
    ManagedCopy(#[from] crate::blob::managed_copy::ManagedCopyError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Blob(#[from] BlobError),
    #[error("peer is not a member of the realm")]
    PeerDenied,
    #[error("read access denied")]
    Denied,
    #[error("bucket not found")]
    BucketNotFound,
    #[error("unexpected event in state {state}: {event}")]
    Unexpected { state: &'static str, event: String },
    #[error("peer did not answer before the request deadline")]
    Aborted,
}

#[cfg(test)]
mod tests;
