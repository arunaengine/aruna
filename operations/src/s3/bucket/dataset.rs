//! Reads the current metadata of a local bucket and the description its dataset gives it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, NetEvent};
use aruna_core::keyspaces::{
    BUCKET_CREATION_KEYSPACE, BUCKET_METADATA_KEYSPACE, DISTRIBUTION_KEYSPACE,
};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::rows::{event, values};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{BucketInfo, incarnation};
use aruna_core::structs::storage::bucket_metadata::{BucketMetadata, StoredMetadata};
use aruna_core::structs::storage::dataset_description::{
    DatasetDescription, DatasetSummaryRequest, DescriptionError, describe,
};
use aruna_core::structs::storage::distribution::DistributionView;
use aruna_core::types::{Key, Value};
use thiserror::Error;
use ulid::Ulid;

use crate::metadata::api::MetadataApiError;
use crate::metadata::forward::read::{summary_answer, summary_export};
use aruna_core::metadata::api::ExportMetadataResult;

/// Why the metadata of a bucket could not be read.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error("bucket metadata read failed: {0}")]
pub struct BucketMetadataError(String);

impl From<IoError> for BucketMetadataError {
    fn from(error: IoError) -> Self {
        Self(error.to_string())
    }
}

impl From<ConversionError> for BucketMetadataError {
    fn from(error: ConversionError) -> Self {
        Self(error.to_string())
    }
}

/// The current metadata of a local bucket: its distributed set's settings, or the row of this
/// bucket's current incarnation. `None` when there is none.
#[derive(Debug, PartialEq)]
pub struct BucketMetadataOperation {
    bucket: String,
    set_id: Option<Ulid>,
}

impl BucketMetadataOperation {
    pub fn new(bucket: &str, info: &BucketInfo) -> Self {
        Self {
            bucket: bucket.to_string(),
            set_id: info.distribution.as_ref().map(|binding| binding.set_id),
        }
    }
}

impl Operation for BucketMetadataOperation {
    type Output = Option<BucketMetadata>;
    type Error = BucketMetadataError;

    fn first(&mut self) -> Result<Next<Self::Output>, Self::Error> {
        let reads = match self.set_id {
            Some(set_id) => {
                let key = Key::from(set_id.to_bytes().to_vec());
                vec![(DISTRIBUTION_KEYSPACE.to_string(), key)]
            }
            None => {
                let key = Key::from(self.bucket.as_bytes().to_vec());
                vec![
                    (BUCKET_METADATA_KEYSPACE.to_string(), key.clone()),
                    (BUCKET_CREATION_KEYSPACE.to_string(), key),
                ]
            }
        };
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, Self::Error> {
        let metadata = if self.set_id.is_some() {
            let [row] = values::<1>(result)?;
            let view = row
                .map(|row| DistributionView::from_bytes(&row))
                .transpose()?;
            view.and_then(|view| view.snapshot().config.settings.metadata.clone())
                .map(|metadata| *metadata)
        } else {
            let [row, creation] = values::<2>(result)?;
            current_row(creation.as_ref(), row.as_ref())?
        };
        Ok(Next::Done(metadata))
    }
}

/// The metadata a stored row gives the bucket's current incarnation.
pub(super) fn current_row(
    creation: Option<&Value>,
    row: Option<&Value>,
) -> Result<Option<BucketMetadata>, ConversionError> {
    let Some(row) = row else {
        return Ok(None);
    };
    let stored = StoredMetadata::from_bytes(row.as_ref())?;
    let current = incarnation(creation.map(|value| value.as_ref()));
    Ok((stored.incarnation == current).then_some(stored.metadata))
}

/// What the dataset says about a bucket, read from its holders with the caller's own access,
/// so a reader never sees a description it may not read.
#[derive(Debug, PartialEq)]
pub struct DatasetSummaryOperation {
    request: Option<DatasetSummaryRequest>,
}

impl DatasetSummaryOperation {
    pub fn new(
        realm_id: RealmId,
        dataset: Ulid,
        (auth, token): (Option<&AuthContext>, Option<AuthToken>),
    ) -> Self {
        let auth = auth.cloned();
        let request = DatasetSummaryRequest {
            realm_id,
            dataset,
            auth,
            token,
        };
        Self {
            request: Some(request),
        }
    }
}

impl Operation for DatasetSummaryOperation {
    type Output = DatasetDescription;
    type Error = DescriptionError;

    fn first(&mut self) -> Result<Next<DatasetDescription>, DescriptionError> {
        let local = Effect::Net(NetEffect::LocalNode);
        Ok(Next::Call(IoCall::Effect(Box::new(local))))
    }

    // The summary is read from the dataset's holders with the caller's own access.
    fn next(&mut self, result: IoResult) -> Result<Next<DatasetDescription>, DescriptionError> {
        if let Some(request) = self.request.take() {
            let Ok(Event::Net(NetEvent::LocalNode(local_node))) = event(result) else {
                return Err(DescriptionError::Unavailable);
            };
            let export = summary_export(local_node, request);
            return Ok(Next::Call(IoCall::child(export)));
        }
        let export = (result.child::<ExportMetadataResult, MetadataApiError>())
            .map_err(|_| DescriptionError::Unavailable)?;
        let summary = summary_answer(export)?;
        let described = describe(&summary).map_err(DescriptionError::Invalid)?;
        Ok(Next::Done(described))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stored(incarnation: u128) -> Value {
        let metadata = BucketMetadata::default();
        let row = StoredMetadata {
            incarnation,
            metadata,
        };
        Value::from(row.to_bytes().unwrap())
    }

    fn local() -> BucketMetadataOperation {
        BucketMetadataOperation {
            bucket: "db".to_string(),
            set_id: None,
        }
    }

    fn summary() -> DatasetSummaryOperation {
        DatasetSummaryOperation::new(
            RealmId::from_bytes([1; 32]),
            Ulid::from_parts(1, 1),
            (None, None),
        )
    }

    // A summary walked past its local node lookup to the export it reads.
    fn exporting() -> DatasetSummaryOperation {
        let mut operation = summary();
        let local = Effect::Net(NetEffect::LocalNode);
        assert_eq!(
            operation.first(),
            Ok(Next::Call(IoCall::Effect(Box::new(local))))
        );
        let node = IoResult::Event(Box::new(Event::Net(NetEvent::LocalNode(None))));
        assert!(matches!(
            operation.next(node),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        operation
    }

    fn exported(export: Result<ExportMetadataResult, MetadataApiError>) -> IoResult {
        IoResult::Child(aruna_core::calls::ChildOutcome::new(export))
    }

    // A local bucket reads its row and its creation marker in one call.
    #[test]
    fn local_reads_both() {
        let Ok(Next::Call(IoCall::Read(reads))) = local().first() else {
            panic!("expected one read");
        };
        let spaces: Vec<_> = reads.iter().map(|(space, _)| space.as_str()).collect();
        assert_eq!(spaces, [BUCKET_METADATA_KEYSPACE, BUCKET_CREATION_KEYSPACE]);
    }

    // A row of an earlier incarnation of the bucket name gives no metadata.
    #[test]
    fn stale_row_hidden() {
        let creation = Value::from(7u128.to_be_bytes().to_vec());
        let read = |row: u128| {
            let rows = vec![Some(stored(row)), Some(creation.clone())];
            local().next(IoResult::Values(rows))
        };
        assert_eq!(read(6), Ok(Next::Done(None)));
        assert_eq!(read(7), Ok(Next::Done(Some(BucketMetadata::default()))));
    }

    // A distributed bucket reads its set's record, and a missing record gives no metadata.
    #[test]
    fn set_reads_record() {
        let set_id = Ulid::from_parts(2, 2);
        let mut operation = BucketMetadataOperation {
            bucket: "db".to_string(),
            set_id: Some(set_id),
        };
        let row = (
            DISTRIBUTION_KEYSPACE.to_string(),
            Key::from(set_id.to_bytes().to_vec()),
        );
        assert_eq!(operation.first(), Ok(Next::Call(IoCall::Read(vec![row]))));
        assert_eq!(
            operation.next(IoResult::Values(vec![None])),
            Ok(Next::Done(None))
        );
    }

    // The summary is read once with the caller's access, and its root must be a Dataset.
    #[test]
    fn summary_needs_dataset() {
        let summary = ExportMetadataResult::Summary {
            record: crate::metadata::visible_scope::call_tests::record(1),
            jsonld: "{}".to_string(),
        };
        let refused = exporting().next(exported(Ok(summary)));
        assert!(matches!(refused, Err(DescriptionError::Invalid(_))));
    }

    // A refused export leaves the dataset unreadable; any other failure leaves it unavailable.
    #[test]
    fn summary_keeps_refusal() {
        let refused = exporting().next(exported(Err(MetadataApiError::Forbidden)));
        assert_eq!(refused, Err(DescriptionError::Unreadable));
        let failed = exporting().next(exported(Err(MetadataApiError::ServiceUnavailable)));
        assert_eq!(failed, Err(DescriptionError::Unavailable));
        assert_eq!(
            exporting().next(IoResult::Done),
            Err(DescriptionError::Unavailable)
        );
    }
}
