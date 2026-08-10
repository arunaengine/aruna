use aruna_core::errors::StorageError;
use aruna_core::metadata::{MetadataError, MetadataValidationViolation};

use crate::create_metadata_document::CreateMetadataDocumentError;
use crate::delete_metadata_document::DeleteMetadataDocumentError;
use crate::metadata::forward::MetadataWriteError;
use crate::update_metadata_document::UpdateMetadataDocumentError;

/// What a job should do about a failed metadata write.
///
/// Shared by every job that writes through the metadata seam so one error means
/// one thing everywhere: a document the backend will never accept is reported,
/// never retried, while an overloaded or unreachable node is always retried.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetadataFailure {
    /// The document itself is rejected; retrying cannot change the outcome.
    Validation(Vec<MetadataValidationViolation>),
    Permanent(String),
    Retryable(String),
}

pub fn classify_metadata(error: MetadataWriteError) -> MetadataFailure {
    match error {
        MetadataWriteError::Create(CreateMetadataDocumentError::MetadataError(
            MetadataError::Validation(violations),
        ))
        | MetadataWriteError::Update(UpdateMetadataDocumentError::MetadataError(
            MetadataError::Validation(violations),
        ))
        | MetadataWriteError::Delete(DeleteMetadataDocumentError::MetadataError(
            MetadataError::Validation(violations),
        )) => MetadataFailure::Validation(violations),
        error if metadata_is_transient(&error) => MetadataFailure::Retryable(error.to_string()),
        error => MetadataFailure::Permanent(error.to_string()),
    }
}

/// Authorization, validation and invariant breaches are the document's fault
/// and stay permanent. Capacity, transport and commit-ambiguity failures are
/// the node's, and every metadata write is fenced by an idempotency check, so
/// replaying an ambiguous commit is safe.
pub fn metadata_is_transient(error: &MetadataWriteError) -> bool {
    match error {
        MetadataWriteError::Unauthorized
        | MetadataWriteError::Forbidden
        | MetadataWriteError::NotFound => false,
        MetadataWriteError::Undeliverable(_) => true,
        MetadataWriteError::Create(error) => match error {
            CreateMetadataDocumentError::StorageError(error) => storage_is_transient(error),
            CreateMetadataDocumentError::MetadataError(error) => metadata_error_transient(error),
            CreateMetadataDocumentError::ClockHealth(_)
            | CreateMetadataDocumentError::TopicAnnouncement(_)
            | CreateMetadataDocumentError::OriginHoldsNoBucket
            | CreateMetadataDocumentError::PlacementBindingUnavailable(_)
            | CreateMetadataDocumentError::PlacementBinding(_) => true,
            _ => false,
        },
        MetadataWriteError::Update(error) => match error {
            UpdateMetadataDocumentError::StorageError(error) => storage_is_transient(error),
            UpdateMetadataDocumentError::MetadataError(error) => metadata_error_transient(error),
            UpdateMetadataDocumentError::TopicAnnouncement(_) => true,
            _ => false,
        },
        MetadataWriteError::Delete(error) => match error {
            DeleteMetadataDocumentError::StorageError(error) => storage_is_transient(error),
            DeleteMetadataDocumentError::MetadataError(error) => metadata_error_transient(error),
            DeleteMetadataDocumentError::SyncDelete(_) => true,
            _ => false,
        },
    }
}

fn storage_is_transient(error: &StorageError) -> bool {
    !matches!(
        error,
        StorageError::KeyNotFound | StorageError::InvalidEffect
    )
}

fn metadata_error_transient(error: &MetadataError) -> bool {
    match error {
        MetadataError::Validation(_)
        | MetadataError::InvalidInput(_)
        | MetadataError::InvalidEffect
        | MetadataError::GraphNotFound => false,
        MetadataError::Storage(error) => storage_is_transient(error),
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn violation() -> Vec<MetadataValidationViolation> {
        vec![MetadataValidationViolation {
            code: "missing_root".to_string(),
            message: "no root".to_string(),
            pointer: "/@graph".to_string(),
            entity_id: None,
        }]
    }

    fn create(error: CreateMetadataDocumentError) -> MetadataWriteError {
        MetadataWriteError::Create(error)
    }

    fn update(error: UpdateMetadataDocumentError) -> MetadataWriteError {
        MetadataWriteError::Update(error)
    }

    fn delete(error: DeleteMetadataDocumentError) -> MetadataWriteError {
        MetadataWriteError::Delete(error)
    }

    #[test]
    fn validation_is_reported_not_retried() {
        for error in [
            create(CreateMetadataDocumentError::MetadataError(
                MetadataError::Validation(violation()),
            )),
            update(UpdateMetadataDocumentError::MetadataError(
                MetadataError::Validation(violation()),
            )),
            delete(DeleteMetadataDocumentError::MetadataError(
                MetadataError::Validation(violation()),
            )),
        ] {
            assert_eq!(
                classify_metadata(error),
                MetadataFailure::Validation(violation())
            );
        }
    }

    #[test]
    fn every_write_variant_is_classified() {
        let permanent: Vec<MetadataWriteError> = vec![
            MetadataWriteError::Unauthorized,
            MetadataWriteError::Forbidden,
            MetadataWriteError::NotFound,
            create(CreateMetadataDocumentError::DocumentAlreadyExists),
            create(CreateMetadataDocumentError::MissingTransaction),
            create(CreateMetadataDocumentError::NotFinished),
            create(CreateMetadataDocumentError::RawLimit),
            create(CreateMetadataDocumentError::UnexpectedEvent {
                state: "s".to_string(),
                expected: "e",
                got: "g".to_string(),
            }),
            create(CreateMetadataDocumentError::MetadataError(
                MetadataError::InvalidInput("bad".to_string()),
            )),
            create(CreateMetadataDocumentError::StorageError(
                StorageError::KeyNotFound,
            )),
            update(UpdateMetadataDocumentError::DocumentNotFound),
            update(UpdateMetadataDocumentError::MissingTransaction),
            update(UpdateMetadataDocumentError::NotFinished),
            update(UpdateMetadataDocumentError::RawLimit),
            update(UpdateMetadataDocumentError::UnexpectedEvent {
                state: "s".to_string(),
                expected: "e",
                got: "g".to_string(),
            }),
            update(UpdateMetadataDocumentError::MetadataError(
                MetadataError::GraphNotFound,
            )),
            delete(DeleteMetadataDocumentError::DocumentNotFound),
            delete(DeleteMetadataDocumentError::MissingTransaction),
            delete(DeleteMetadataDocumentError::UnexpectedEvent {
                state: "s".to_string(),
                expected: "e",
                got: "g".to_string(),
            }),
        ];
        for error in permanent {
            let label = error.to_string();
            assert!(
                matches!(classify_metadata(error), MetadataFailure::Permanent(_)),
                "expected permanent: {label}"
            );
        }

        let retryable: Vec<MetadataWriteError> = vec![
            MetadataWriteError::Undeliverable("no holder".to_string()),
            create(CreateMetadataDocumentError::StorageError(
                StorageError::TransactionConflict,
            )),
            create(CreateMetadataDocumentError::StorageError(
                StorageError::QueueFull,
            )),
            create(CreateMetadataDocumentError::StorageError(
                StorageError::Timeout,
            )),
            create(CreateMetadataDocumentError::StorageError(
                StorageError::ChannelClosed,
            )),
            create(CreateMetadataDocumentError::StorageError(
                StorageError::CommitFailed,
            )),
            create(CreateMetadataDocumentError::StorageError(
                StorageError::Sealed,
            )),
            create(CreateMetadataDocumentError::TopicAnnouncement(
                "no topic".to_string(),
            )),
            create(CreateMetadataDocumentError::OriginHoldsNoBucket),
            create(CreateMetadataDocumentError::PlacementBindingUnavailable(
                "no binding".to_string(),
            )),
            create(CreateMetadataDocumentError::MetadataError(
                MetadataError::ChannelClosed,
            )),
            create(CreateMetadataDocumentError::MetadataError(
                MetadataError::HandleMissing,
            )),
            create(CreateMetadataDocumentError::MetadataError(
                MetadataError::Persist("disk".to_string()),
            )),
            create(CreateMetadataDocumentError::MetadataError(
                MetadataError::Backend("busy".to_string()),
            )),
            create(CreateMetadataDocumentError::MetadataError(
                MetadataError::TaskJoin("panic".to_string()),
            )),
            update(UpdateMetadataDocumentError::StorageError(
                StorageError::TransactionConflict,
            )),
            update(UpdateMetadataDocumentError::StorageError(
                StorageError::QueueFull,
            )),
            update(UpdateMetadataDocumentError::TopicAnnouncement(
                "no topic".to_string(),
            )),
            update(UpdateMetadataDocumentError::MetadataError(
                MetadataError::Backend("busy".to_string()),
            )),
            delete(DeleteMetadataDocumentError::StorageError(
                StorageError::TransactionConflict,
            )),
            delete(DeleteMetadataDocumentError::SyncDelete("peer".to_string())),
            delete(DeleteMetadataDocumentError::MetadataError(
                MetadataError::Persist("disk".to_string()),
            )),
        ];
        for error in retryable {
            let label = error.to_string();
            assert!(
                matches!(classify_metadata(error), MetadataFailure::Retryable(_)),
                "expected retryable: {label}"
            );
        }
    }

    #[test]
    fn conversion_errors_stay_permanent() {
        let error = create(CreateMetadataDocumentError::ConversionError(
            postcard::Error::SerdeSerCustom.into(),
        ));
        assert!(matches!(
            classify_metadata(error),
            MetadataFailure::Permanent(_)
        ));
    }
}
