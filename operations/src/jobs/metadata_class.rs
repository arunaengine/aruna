//! Sorts a failed metadata write into a validation, permanent or retryable job outcome.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::errors::StorageError;
use aruna_core::metadata::{MetadataError, MetadataValidationViolation};
use aruna_core::structs::placement::binding_directory::BindingError;

use crate::forward::transport::MetadataWriteError;
use crate::metadata::create_document::CreateDocumentError;
use crate::metadata::delete_document::DeleteDocumentError;
use crate::metadata::update_document::UpdateDocumentError;

/// What a job should do about a failed metadata write, shared by every job that
/// writes through the metadata seam: a document the backend will never accept
/// is reported, never retried; an overloaded or unreachable node is retried.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetadataFailure {
    /// The document itself is rejected; retrying cannot change the outcome.
    Validation(Vec<MetadataValidationViolation>),
    Permanent(String),
    Retryable(String),
}

pub fn classify_metadata(error: MetadataWriteError) -> MetadataFailure {
    match error {
        MetadataWriteError::Create(CreateDocumentError::MetadataError(
            MetadataError::Validation(violations),
        ))
        | MetadataWriteError::Update(UpdateDocumentError::MetadataError(
            MetadataError::Validation(violations),
        ))
        | MetadataWriteError::Delete(DeleteDocumentError::MetadataError(
            MetadataError::Validation(violations),
        )) => MetadataFailure::Validation(violations),
        error if metadata_is_transient(&error) => MetadataFailure::Retryable(error.to_string()),
        error => MetadataFailure::Permanent(error.to_string()),
    }
}

/// Authorization, validation and invariant breaches are the document's fault
/// and stay permanent. Capacity, transport and commit-ambiguity failures are the
/// node's, and every write is fenced by an idempotency check, so replay is safe.
pub fn metadata_is_transient(error: &MetadataWriteError) -> bool {
    match error {
        MetadataWriteError::Unauthorized
        | MetadataWriteError::Forbidden
        | MetadataWriteError::NotFound => false,
        MetadataWriteError::Undeliverable(_) => true,
        MetadataWriteError::Create(error) => match error {
            CreateDocumentError::StorageError(error) => storage_is_transient(error),
            CreateDocumentError::MetadataError(error) => metadata_error_transient(error),
            CreateDocumentError::ClockHealth(_)
            | CreateDocumentError::TopicAnnouncement(_)
            | CreateDocumentError::HoldsNoBucket
            | CreateDocumentError::PlacementBindingUnavailable(_) => true,
            CreateDocumentError::PlacementBinding(error) => binding_is_transient(error),
            _ => false,
        },
        MetadataWriteError::Update(error) => match error {
            UpdateDocumentError::StorageError(error) => storage_is_transient(error),
            UpdateDocumentError::MetadataError(error) => metadata_error_transient(error),
            UpdateDocumentError::TopicAnnouncement(_) => true,
            _ => false,
        },
        MetadataWriteError::Delete(error) => match error {
            DeleteDocumentError::StorageError(error) => storage_is_transient(error),
            DeleteDocumentError::MetadataError(error) => metadata_error_transient(error),
            DeleteDocumentError::SyncDelete(_) => true,
            _ => false,
        },
    }
}

/// An incomplete binding set still converges, so an unknown handle or strategy
/// waits for replication. Divergent tuples and out-of-range buckets are settled
/// values: retry cannot resolve them and only hides the fault.
fn binding_is_transient(error: &BindingError) -> bool {
    match error {
        BindingError::Unknown(_) | BindingError::UnknownStrategy(_) => true,
        BindingError::Conflicted(_) | BindingError::OutOfRange(_) => false,
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
mod pure_tests {
    use super::*;
    use aruna_core::metadata::{
        ProfileValidationCompleteness, ProfileValidationFinding, ProfileValidationSeverity,
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

    fn profile_finding(code: &str) -> ProfileValidationFinding {
        ProfileValidationFinding {
            code: code.to_string(),
            severity: ProfileValidationSeverity::Violation,
            focus_node: None,
            path: None,
            rule: code.to_string(),
            message: code.to_string(),
            profile_revision: None,
            completeness: ProfileValidationCompleteness::Incomplete,
        }
    }

    #[test]
    fn profile_gate_exceptions() {
        for code in ["unsupported_constraint", "validation_limit"] {
            let permanent = create(CreateDocumentError::MetadataError(
                MetadataError::ProfileValidation(vec![profile_finding(code)]),
            ));
            assert!(matches!(
                classify_metadata(permanent),
                MetadataFailure::Permanent(_)
            ));
        }

        let retryable = create(CreateDocumentError::MetadataError(
            MetadataError::ProfileValidation(vec![profile_finding("validator_unavailable")]),
        ));
        assert!(matches!(
            classify_metadata(retryable),
            MetadataFailure::Retryable(_)
        ));
    }

    fn create(error: CreateDocumentError) -> MetadataWriteError {
        MetadataWriteError::Create(error)
    }

    fn update(error: UpdateDocumentError) -> MetadataWriteError {
        MetadataWriteError::Update(error)
    }

    fn delete(error: DeleteDocumentError) -> MetadataWriteError {
        MetadataWriteError::Delete(error)
    }

    // a validation failure is reported, not retried
    #[test]
    fn validation_never_retries() {
        for error in [
            create(CreateDocumentError::MetadataError(
                MetadataError::Validation(violation()),
            )),
            update(UpdateDocumentError::MetadataError(
                MetadataError::Validation(violation()),
            )),
            delete(DeleteDocumentError::MetadataError(
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
            create(CreateDocumentError::DocumentAlreadyExists),
            create(CreateDocumentError::MissingTransaction),
            create(CreateDocumentError::NotFinished),
            create(CreateDocumentError::RawLimit),
            create(CreateDocumentError::UnexpectedEvent {
                state: "s".to_string(),
                expected: "e",
                got: "g".to_string(),
            }),
            create(CreateDocumentError::MetadataError(
                MetadataError::InvalidInput("bad".to_string()),
            )),
            create(CreateDocumentError::StorageError(StorageError::KeyNotFound)),
            update(UpdateDocumentError::DocumentNotFound),
            update(UpdateDocumentError::MissingTransaction),
            update(UpdateDocumentError::NotFinished),
            update(UpdateDocumentError::RawLimit),
            update(UpdateDocumentError::UnexpectedEvent {
                state: "s".to_string(),
                expected: "e",
                got: "g".to_string(),
            }),
            delete(DeleteDocumentError::DocumentNotFound),
            delete(DeleteDocumentError::MissingTransaction),
            delete(DeleteDocumentError::UnexpectedEvent {
                state: "s".to_string(),
                expected: "e",
                got: "g".to_string(),
            }),
            // Settled values of the immutable binding set; a retry replays them.
            create(CreateDocumentError::PlacementBinding(
                BindingError::Conflicted(handle()),
            )),
            create(CreateDocumentError::PlacementBinding(
                BindingError::OutOfRange(
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
            create(CreateDocumentError::StorageError(
                StorageError::TransactionConflict,
            )),
            create(CreateDocumentError::StorageError(StorageError::QueueFull)),
            create(CreateDocumentError::StorageError(StorageError::Timeout)),
            create(CreateDocumentError::StorageError(
                StorageError::ChannelClosed,
            )),
            create(CreateDocumentError::StorageError(
                StorageError::CommitFailed,
            )),
            create(CreateDocumentError::StorageError(StorageError::Closed)),
            create(CreateDocumentError::TopicAnnouncement(
                "no topic".to_string(),
            )),
            create(CreateDocumentError::HoldsNoBucket),
            create(CreateDocumentError::PlacementBindingUnavailable(
                "no binding".to_string(),
            )),
            // An incomplete local binding set still converges by replication.
            create(CreateDocumentError::PlacementBinding(
                BindingError::Unknown(handle()),
            )),
            create(CreateDocumentError::PlacementBinding(
                BindingError::UnknownStrategy(ulid::Ulid::nil()),
            )),
            create(CreateDocumentError::MetadataError(
                MetadataError::ChannelClosed,
            )),
            create(CreateDocumentError::MetadataError(
                MetadataError::HandleMissing,
            )),
            create(CreateDocumentError::MetadataError(MetadataError::Persist(
                "disk".to_string(),
            ))),
            create(CreateDocumentError::MetadataError(MetadataError::Backend(
                "busy".to_string(),
            ))),
            create(CreateDocumentError::MetadataError(MetadataError::TaskJoin(
                "panic".to_string(),
            ))),
            update(UpdateDocumentError::StorageError(
                StorageError::TransactionConflict,
            )),
            update(UpdateDocumentError::StorageError(StorageError::QueueFull)),
            update(UpdateDocumentError::TopicAnnouncement(
                "no topic".to_string(),
            )),
            update(UpdateDocumentError::MetadataError(MetadataError::Backend(
                "busy".to_string(),
            ))),
            delete(DeleteDocumentError::StorageError(
                StorageError::TransactionConflict,
            )),
            delete(DeleteDocumentError::SyncDelete("peer".to_string())),
            delete(DeleteDocumentError::MetadataError(MetadataError::Persist(
                "disk".to_string(),
            ))),
            create(CreateDocumentError::MetadataError(
                MetadataError::GraphNotFound,
            )),
            update(UpdateDocumentError::MetadataError(
                MetadataError::GraphNotFound,
            )),
            delete(DeleteDocumentError::MetadataError(
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
            classify_metadata(update(UpdateDocumentError::MetadataError(
                MetadataError::GraphNotFound
            ))),
            MetadataFailure::Retryable(_)
        ));
        assert!(matches!(
            classify_metadata(update(UpdateDocumentError::MetadataError(
                MetadataError::InvalidInput("not an iri".to_string())
            ))),
            MetadataFailure::Permanent(_)
        ));
    }

    #[test]
    fn conversions_stay_permanent() {
        let error = create(CreateDocumentError::ConversionError(
            postcard::Error::SerdeSerCustom.into(),
        ));
        assert!(matches!(
            classify_metadata(error),
            MetadataFailure::Permanent(_)
        ));
    }
}
