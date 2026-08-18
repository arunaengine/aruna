//! Cross-node staging of one sealed input version.
//!
//! The target may not hold the bytes an execution needs. It then reads them
//! from a legal holder through the managed-copy handshake, which challenges a
//! policy-unaware request, teaches the refs, and retries only once this node's
//! own destination subject complies. The bytes land through the ordinary
//! policy-gated workspace write, verified against the sealed hash.

use aruna_core::stream::BackendStream;
use aruna_core::structs::{
    AuthContext, InputSelection, InputSource, JobError, JobRecord, VersionedObjectArn,
};
use aruna_core::types::NodeId;
use tracing::{debug, warn};
use ulid::Ulid;

use crate::blob_holders::GetBlobHoldersOperation;
use crate::driver::{DriverContext, drive};
use crate::replication::bao_read::{BaoReadError, BaoReadOutput, managed_read};
use crate::replication::protocol::{BaoReadRequest, BaoReadTarget};

/// The staged bytes of one input plus the size the caller writes with.
pub struct StagedInput {
    pub blob: BackendStream<Result<bytes::Bytes, aruna_core::stream::StreamError>>,
    pub size: u64,
}

/// Reads one exact input version from a legal holder. Holders are tried in
/// canonical order and a missing or corrupt source moves to the next one
/// without ever changing the sealed input.
pub async fn stage_remote_input(
    context: &DriverContext,
    record: &JobRecord,
    input: &InputSelection,
    version: Ulid,
    blake3: [u8; 32],
) -> Result<StagedInput, JobError> {
    let InputSource::S3 { bucket, key, .. } = &input.source;
    let net = context
        .net_handle
        .as_ref()
        .ok_or_else(|| JobError::permanent("remote staging needs a net handle"))?;
    let realm_id = *net.realm_id();
    let holders = drive(
        GetBlobHoldersOperation::new(blake3, realm_id, net.node_id()),
        context,
    )
    .await
    .map_err(|error| JobError::retryable(format!("input holder lookup failed: {error}")))?;
    if holders.is_empty() {
        return Err(JobError::retryable(format!(
            "no known holder for input {bucket}/{key}"
        )));
    }
    let request = BaoReadRequest {
        auth_context: AuthContext {
            user_id: record.created_by,
            realm_id,
            path_restrictions: None,
        },
        realm_id,
        target: BaoReadTarget::ExactVersion(VersionedObjectArn {
            realm_id,
            node_id: net.node_id(),
            bucket: bucket.clone(),
            key: key.clone(),
            version,
        }),
        expected_blake3: Some(blake3),
        metadata_only: false,
        // The managed read fills in this node's own destination subject, so the
        // source evaluates placement against where the bytes would land.
        destination: None,
        known_refs: Vec::new(),
    };
    let mut last: Option<BaoReadError> = None;
    for holder in holders {
        match managed_read(context, holder, request.clone()).await {
            Ok(BaoReadOutput::Stream { blob, size, .. }) => {
                debug!(peer = %holder, "Input staged from a remote holder");
                return Ok(StagedInput { blob, size });
            }
            Ok(BaoReadOutput::Metadata { .. }) => continue,
            Err(error) => {
                warn!(peer = %holder, error = %error, "Remote input read failed");
                last = Some(error);
            }
        }
    }
    Err(stage_error(bucket, key, last))
}

/// A denied or policy-blocked read fails this attempt permanently; anything
/// else may still succeed from another holder later.
fn stage_error(bucket: &str, key: &str, error: Option<BaoReadError>) -> JobError {
    let message = match &error {
        Some(error) => format!("staging {bucket}/{key} failed: {error}"),
        None => format!("staging {bucket}/{key} found no usable holder"),
    };
    match error {
        Some(BaoReadError::PolicyRequired { .. }) | Some(BaoReadError::NoDestination) => {
            JobError::permanent(message)
        }
        _ => JobError::retryable(message),
    }
}

/// Holders of one input's bytes, for callers that only need the node list.
pub async fn input_holders(
    context: &DriverContext,
    blake3: [u8; 32],
) -> Result<Vec<NodeId>, JobError> {
    let net = context
        .net_handle
        .as_ref()
        .ok_or_else(|| JobError::permanent("holder lookup needs a net handle"))?;
    drive(
        GetBlobHoldersOperation::new(blake3, *net.realm_id(), net.node_id()),
        context,
    )
    .await
    .map_err(|error| JobError::retryable(format!("input holder lookup failed: {error}")))
}
