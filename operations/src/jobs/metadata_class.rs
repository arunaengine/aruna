use aruna_core::errors::StorageError;
use aruna_core::metadata::{MetadataError, MetadataValidationViolation};
use aruna_core::structs::BindingError;

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
            | CreateMetadataDocumentError::PlacementBindingUnavailable(_) => true,
            CreateMetadataDocumentError::PlacementBinding(error) => binding_is_transient(error),
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

/// A binding set that is merely incomplete here still converges, so an unknown
/// handle or strategy waits for replication. Divergent tuples and a bucket
/// outside the strategy's range are settled facts of the immutable binding set:
/// no retry can resolve them, so burning a backoff schedule on them only hides
/// the fault.
fn binding_is_transient(error: &BindingError) -> bool {
    match error {
        BindingError::Unknown(_) | BindingError::UnknownStrategy(_) => true,
        BindingError::Conflicted(_) | BindingError::BucketOutOfRange(_) => false,
    }
}

fn storage_is_transient(error: &StorageError) -> bool {
    !matches!(
        error,
        StorageError::KeyNotFound | StorageError::InvalidEffect
    )
}

/// A missing graph is a durable registry row whose materialization has not
/// caught up, which the read API already reports as service unavailable. Only a
/// rejected document stays permanent.
fn metadata_error_transient(error: &MetadataError) -> bool {
    match error {
        MetadataError::ProfileValidation(findings) => {
            !findings.is_empty()
                && findings.iter().all(|finding| {
                    matches!(
                        finding.code.as_str(),
                        "profile_unavailable" | "validator_unavailable"
                    )
                })
        }
        MetadataError::Validation(_)
        | MetadataError::InvalidInput(_)
        | MetadataError::InvalidEffect => false,
        MetadataError::Storage(error) => storage_is_transient(error),
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::metadata::{
        MetadataProfileValidationCompleteness, MetadataProfileValidationFinding,
        MetadataProfileValidationSeverity,
    };

    fn handle() -> aruna_core::structured_id::PlacementHandle {
        aruna_core::structured_id::PlacementHandle::new(1).expect("handle")
    }

    fn violation() -> Vec<MetadataValidationViolation> {
        vec![MetadataValidationViolation {
            code: "missing_root".to_string(),
            message: "no root".to_string(),
            pointer: "/@graph".to_string(),
            entity_id: None,
        }]
    }

    fn profile_finding(code: &str) -> MetadataProfileValidationFinding {
        MetadataProfileValidationFinding {
            code: code.to_string(),
            severity: MetadataProfileValidationSeverity::Violation,
            focus_node: None,
            path: None,
            rule: code.to_string(),
            message: code.to_string(),
            profile_revision: None,
            completeness: MetadataProfileValidationCompleteness::Incomplete,
        }
    }

    #[test]
    fn profile_gate_is_permanent_except_for_unavailable_dependencies() {
        let permanent = create(CreateMetadataDocumentError::MetadataError(
            MetadataError::ProfileValidation(vec![profile_finding("unsupported_constraint")]),
        ));
        assert!(matches!(
            classify_metadata(permanent),
            MetadataFailure::Permanent(_)
        ));

        let retryable = create(CreateMetadataDocumentError::MetadataError(
            MetadataError::ProfileValidation(vec![profile_finding("validator_unavailable")]),
        ));
        assert!(matches!(
            classify_metadata(retryable),
            MetadataFailure::Retryable(_)
        ));
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

    // a validation failure is reported, not retried
    #[test]
    fn validation_never_retries() {
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
    fn write_variants_classified() {
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
            delete(DeleteMetadataDocumentError::DocumentNotFound),
            delete(DeleteMetadataDocumentError::MissingTransaction),
            delete(DeleteMetadataDocumentError::UnexpectedEvent {
                state: "s".to_string(),
                expected: "e",
                got: "g".to_string(),
            }),
            // Settled facts of the immutable binding set; a retry replays them.
            create(CreateMetadataDocumentError::PlacementBinding(
                BindingError::Conflicted(handle()),
            )),
            create(CreateMetadataDocumentError::PlacementBinding(
                BindingError::BucketOutOfRange(
                    aruna_core::structured_id::BucketId::new(9)
                        .expect("bucket")
                        .in_strategy_range(4)
                        .expect_err("out of range"),
                ),
            )),
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
            // An incomplete local binding set still converges by replication.
            create(CreateMetadataDocumentError::PlacementBinding(
                BindingError::Unknown(handle()),
            )),
            create(CreateMetadataDocumentError::PlacementBinding(
                BindingError::UnknownStrategy(ulid::Ulid::nil()),
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
            create(CreateMetadataDocumentError::MetadataError(
                MetadataError::GraphNotFound,
            )),
            update(UpdateMetadataDocumentError::MetadataError(
                MetadataError::GraphNotFound,
            )),
            delete(DeleteMetadataDocumentError::MetadataError(
                MetadataError::GraphNotFound,
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

    /// A lagging graph must never retire a source record: the read API answers
    /// the same condition with service unavailable.
    #[test]
    fn missing_graph_unavailable() {
        assert!(matches!(
            classify_metadata(update(UpdateMetadataDocumentError::MetadataError(
                MetadataError::GraphNotFound
            ))),
            MetadataFailure::Retryable(_)
        ));
        assert!(matches!(
            classify_metadata(update(UpdateMetadataDocumentError::MetadataError(
                MetadataError::InvalidInput("not an iri".to_string())
            ))),
            MetadataFailure::Permanent(_)
        ));
    }

    #[test]
    fn conversions_stay_permanent() {
        let error = create(CreateMetadataDocumentError::ConversionError(
            postcard::Error::SerdeSerCustom.into(),
        ));
        assert!(matches!(
            classify_metadata(error),
            MetadataFailure::Permanent(_)
        ));
    }
}
