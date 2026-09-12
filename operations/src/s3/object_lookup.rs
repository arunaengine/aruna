//! Shared version-location lookup steps for the S3 object read operations: the
//! managed-copy gate, the blob location read and the multipart summary read.

use crate::blob::managed_copy::{
    CopyRequest, ManagedCopyError, serve_reads, split_serve_reads, validate_registration,
};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::S3_MULTIPART_OBJECT_METADATA_KEYSPACE;
use aruna_core::structs::{
    BackendLocation, BackendRef, BlobLocationKey, ManagedCopyKey, MultipartObjectMetadataKey,
    MultipartObjectSummary, PlacementPolicyRef, VersionKey,
};
use aruna_core::types::NodeId;
use ulid::Ulid;

#[derive(Debug, PartialEq)]
pub(crate) enum LookupError {
    Conversion(ConversionError),
    Managed(ManagedCopyError),
    InvalidEvent(Event),
    Missing,
}

pub(crate) struct CopyLookup {
    pub copy_key: ManagedCopyKey,
    pub location_key: BlobLocationKey,
    pub effect: Effect,
}

pub(crate) fn begin_copy_check(
    bucket: &str,
    key: &str,
    version_id: Ulid,
    blob_hash: [u8; 32],
    backend: BackendRef,
    txn_id: Option<Ulid>,
) -> Result<CopyLookup, ManagedCopyError> {
    let copy_key = ManagedCopyKey::new(VersionKey::new(bucket, key, version_id), backend.clone());
    let effect = serve_reads(&copy_key, txn_id)?;
    Ok(CopyLookup {
        copy_key,
        location_key: BlobLocationKey::new(blob_hash, backend),
        effect,
    })
}

/// Which node a registration must name: the reader's local node, or the node of
/// the subject row read alongside the registration.
pub(crate) enum ExpectedNode {
    Exact(NodeId),
    Subject,
}

pub(crate) fn finish_copy_check(
    event: Event,
    pending_copy: &mut Option<ManagedCopyKey>,
    pending_location: &mut Option<BlobLocationKey>,
    refs: &[PlacementPolicyRef],
    node_id: ExpectedNode,
) -> Result<BlobLocationKey, LookupError> {
    let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
        return Err(LookupError::InvalidEvent(event));
    };
    let (copy, subject) = split_serve_reads(values).map_err(LookupError::Managed)?;
    let (Some(copy_key), Some(location_key)) = (pending_copy.take(), pending_location.take())
    else {
        return Err(LookupError::Missing);
    };
    let node_id = match node_id {
        ExpectedNode::Exact(node_id) => Some(node_id),
        ExpectedNode::Subject => Some(subject.subject.node_id),
    };
    validate_registration(
        copy.as_deref(),
        &CopyRequest {
            key: &copy_key,
            node_id,
            blake3: Some(location_key.blake3_hash),
            refs,
            subject_generation: Some(subject.subject.generation),
        },
    )
    .map_err(LookupError::Managed)?;
    Ok(location_key)
}

pub(crate) fn location_from_read(event: Event) -> Result<Option<BackendLocation>, LookupError> {
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
        return Err(LookupError::InvalidEvent(event));
    };
    match value {
        Some(value) => Ok(Some(
            BackendLocation::from_bytes(value.as_ref()).map_err(LookupError::Conversion)?,
        )),
        None => Ok(None),
    }
}

pub(crate) fn multipart_summary_read(
    version_id: Ulid,
    txn_id: Option<Ulid>,
) -> Result<Effect, ConversionError> {
    let key = MultipartObjectMetadataKey::summary(version_id).to_bytes()?;
    Ok(Effect::Storage(StorageEffect::Read {
        key_space: S3_MULTIPART_OBJECT_METADATA_KEYSPACE.to_string(),
        key: key.into(),
        txn_id,
    }))
}

pub(crate) fn summary_from_read(
    event: Event,
) -> Result<Option<MultipartObjectSummary>, LookupError> {
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
        return Err(LookupError::InvalidEvent(event));
    };
    value
        .map(|value| {
            MultipartObjectSummary::from_bytes(value.as_ref()).map_err(LookupError::Conversion)
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::MultipartChecksumType;

    #[test]
    fn summary_decode_fails() {
        let summary = MultipartObjectSummary {
            checksum_type: MultipartChecksumType::Composite,
            part_count: 3,
            composite_hashes: Default::default(),
        };
        let valid = Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: Some(summary.to_bytes().unwrap().into()),
        });
        assert_eq!(summary_from_read(valid), Ok(Some(summary)));

        let corrupt = Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: Some(vec![0xff].into()),
        });
        assert!(matches!(
            summary_from_read(corrupt),
            Err(LookupError::Conversion(_))
        ));
    }
}
