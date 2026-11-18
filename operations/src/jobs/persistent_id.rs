//! Runs the jobs that mint a persistent identifier and register repository identifiers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{JobError, JobResultPayload};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::secondary_id::RegisterIdentifiersSpec;
use aruna_core::structs::{MintPersistentSpec, PersistentIdFailure, PersistentIdMapping};
use ulid::Ulid;

use crate::jobs::executor::JobRunOutcome;
use crate::metadata::api::MetadataApiError;
use crate::metadata::persistent_id::forward::{PidNode, PidRouteOperation, outcome_mapping};
use crate::metadata::protocol::{PersistentIdOutcome, PersistentIdRequest};

/// How long identifier registration waits for a missing mapping without using attempts.
const IDENTIFIER_WAIT_MS: u64 = 600_000;

type JobNext = Result<Next<JobRunOutcome>, IoError>;

/// The mapping a routed PID request answered with, and whether the request changed it.
fn routed_mapping(
    result: IoResult,
) -> Result<Result<(PersistentIdMapping, bool), MetadataApiError>, IoError> {
    let outcome = result.child::<PersistentIdOutcome, MetadataApiError>()?;
    Ok(outcome.and_then(outcome_mapping))
}

/// Registers a w3id PID for a document. The mint is a compare-and-set on the document's
/// authority, so a lost race or post-withdrawal run reports the authoritative mapping with
/// `newly_minted: false`; routing never mints locally.
#[derive(Debug, PartialEq)]
pub struct MintPidOperation {
    spec: MintPersistentSpec,
    node: PidNode,
    final_attempt: bool,
    now_ms: u64,
    token: AuthToken,
    /// What the job answers once its failure is recorded on the mapping.
    recording: Option<Box<JobRunOutcome>>,
}

impl MintPidOperation {
    pub fn new(
        spec: &MintPersistentSpec,
        (realm_id, local): (RealmId, Option<NodeId>),
        final_attempt: bool,
        now_ms: u64,
    ) -> Self {
        // The submitting route requires an unrestricted realm token, so the internal
        // principal the authority re-checks carries no path restrictions to drop.
        let token = AuthToken::internal(AuthContext {
            user_id: spec.minted_by,
            realm_id,
            path_restrictions: None,
            session: None,
        });
        Self {
            spec: spec.clone(),
            node: (realm_id, local),
            final_attempt,
            now_ms,
            token,
            recording: None,
        }
    }

    fn route(&self, request: PersistentIdRequest) -> IoCall {
        let caller = (Some(self.token.clone()), None);
        IoCall::child(PidRouteOperation::new(
            self.node,
            self.spec.document_id,
            request,
            caller,
        ))
    }

    fn record_failure(
        &mut self,
        error: &MetadataApiError,
        retryable: bool,
        outcome: JobRunOutcome,
    ) -> JobNext {
        let failure = PersistentIdFailure {
            message: error.to_string(),
            retryable,
            recorded_at_ms: self.now_ms,
        };
        self.recording = Some(Box::new(outcome));
        let call = self.route(PersistentIdRequest::Fail { failure });
        Ok(Next::Call(call))
    }
}

impl Operation for MintPidOperation {
    type Output = JobRunOutcome;
    type Error = IoError;

    fn first(&mut self) -> JobNext {
        let request = PersistentIdRequest::Mint {
            minted_by: self.spec.minted_by,
            minted_at_ms: self.now_ms,
        };
        Ok(Next::Call(self.route(request)))
    }

    fn next(&mut self, result: IoResult) -> JobNext {
        // Recording the failure is best effort; the job's own answer stands either way.
        if let Some(outcome) = self.recording.take() {
            let _ = result.child::<PersistentIdOutcome, MetadataApiError>()?;
            return Ok(Next::Done(*outcome));
        }
        match routed_mapping(result)? {
            Ok((mapping, newly_minted)) if mapping.is_active() || mapping.is_retired() => Ok(
                Next::Done(JobRunOutcome::Succeeded(JobResultPayload::PersistentId {
                    pid: mapping.pid,
                    newly_minted,
                })),
            ),
            Ok(_) | Err(MetadataApiError::NotFound) => Ok(Next::Done(JobRunOutcome::Deferred(
                JobError::retryable("persistent id mint is waiting for metadata projection"),
            ))),
            Err(error @ (MetadataApiError::Unauthorized | MetadataApiError::Forbidden)) => {
                let failed = JobError::permanent(format!("persistent id mint: {error}"));
                self.record_failure(&error, false, JobRunOutcome::Failed(failed))
            }
            Err(error) => {
                let failed = JobRunOutcome::Failed(JobError::retryable(format!(
                    "persistent id mint: {error}"
                )));
                match self.final_attempt {
                    true => self.record_failure(&error, true, failed),
                    false => Ok(Next::Done(failed)),
                }
            }
        }
    }
}

/// Adds repository identifiers through the document's authority. An unreachable authority
/// defers the job without using an attempt, so registration waits for it instead of failing.
#[derive(Debug, PartialEq)]
pub struct RegisterIdentifiersOperation {
    spec: RegisterIdentifiersSpec,
    local: Option<NodeId>,
    queued_at_ms: u64,
    now_ms: u64,
    document_id: Ulid,
}

impl RegisterIdentifiersOperation {
    pub fn new(
        spec: &RegisterIdentifiersSpec,
        local: Option<NodeId>,
        queued_at_ms: u64,
        now_ms: u64,
    ) -> Self {
        Self {
            spec: spec.clone(),
            local,
            queued_at_ms,
            now_ms,
            document_id: spec.document_id,
        }
    }
}

impl Operation for RegisterIdentifiersOperation {
    type Output = JobRunOutcome;
    type Error = IoError;

    fn first(&mut self) -> JobNext {
        let request = PersistentIdRequest::AddIdentifiers {
            identifiers: self.spec.identifiers.clone(),
            occurred_at_ms: self.now_ms,
        };
        let node = (self.spec.auth_context.realm_id, self.local);
        let caller = (None, Some(self.spec.auth_context.clone()));
        let add = PidRouteOperation::new(node, self.document_id, request, caller);
        Ok(Next::Call(IoCall::child(add)))
    }

    fn next(&mut self, result: IoResult) -> JobNext {
        let waited_ms = self.now_ms.saturating_sub(self.queued_at_ms);
        Ok(Next::Done(match routed_mapping(result)? {
            Ok((_, changed)) => JobRunOutcome::Succeeded(JobResultPayload::Identifiers { changed }),
            // The authority may not have the mapping yet; after the wait, attempts bound the
            // retries.
            Err(MetadataApiError::NotFound) if waited_ms < IDENTIFIER_WAIT_MS => {
                JobRunOutcome::Deferred(JobError::retryable(
                    "registering identifiers is waiting for the persistent id mapping",
                ))
            }
            Err(error @ MetadataApiError::NotFound) => JobRunOutcome::Failed(JobError::retryable(
                format!("registering identifiers: {error}"),
            )),
            Err(error @ (MetadataApiError::Forbidden | MetadataApiError::Unauthorized)) => {
                let document_id = self.document_id;
                tracing::warn!(%document_id, %error, "identifiers not registered");
                JobRunOutcome::Failed(JobError::permanent(format!(
                    "registering identifiers: {error}"
                )))
            }
            Err(error) => JobRunOutcome::Deferred(JobError::retryable(format!(
                "registering identifiers: {error}"
            ))),
        }))
    }
}

#[cfg(test)]
mod tests {
    use aruna_core::UserId;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::execution::job::JobId;
    use aruna_core::structs::{PersistentIdRevision, PersistentIdStatus};

    use super::*;

    const REALM: RealmId = RealmId([1u8; 32]);

    fn user() -> UserId {
        UserId::local(Ulid::from_bytes([2u8; 16]), REALM)
    }

    fn spec() -> MintPersistentSpec {
        MintPersistentSpec {
            document_id: Ulid::from_bytes([3u8; 16]),
            minted_by: user(),
        }
    }

    fn mint(final_attempt: bool) -> MintPidOperation {
        MintPidOperation::new(&spec(), (REALM, None), final_attempt, 5)
    }

    fn register() -> RegisterIdentifiersOperation {
        let spec = RegisterIdentifiersSpec {
            document_id: Ulid::from_bytes([3u8; 16]),
            identifiers: Vec::new(),
            auth_context: AuthContext {
                user_id: user(),
                realm_id: REALM,
                path_restrictions: None,
                session: None,
            },
        };
        RegisterIdentifiersOperation::new(&spec, None, 1_000, 2_000)
    }

    fn mapping(status: PersistentIdStatus) -> PersistentIdOutcome {
        let mut mapping = PersistentIdMapping::requested(
            Ulid::from_bytes([3u8; 16]),
            false,
            user(),
            JobId::from_bytes([7u8; 16]),
            true,
            "documents/one".to_string(),
            PersistentIdRevision {
                event_id: Ulid::from_bytes([4u8; 16]),
                actor: iroh::SecretKey::from_bytes(&[5u8; 32]).public(),
                occurred_at_ms: 1,
            },
        );
        mapping.status = status;
        PersistentIdOutcome::Mapping {
            mapping: Box::new(mapping),
            changed: true,
        }
    }

    fn answer(result: Result<PersistentIdOutcome, MetadataApiError>) -> IoResult {
        IoResult::Child(ChildOutcome::new(result))
    }

    fn is_call(next: &JobNext) -> bool {
        matches!(next, Ok(Next::Call(IoCall::Child(_))))
    }

    // An active mapping is the job's result, whether this run minted it or not.
    #[test]
    fn active_mapping_succeeds() {
        let mut job = mint(false);
        assert!(is_call(&job.first()));
        let done = job.next(answer(Ok(mapping(PersistentIdStatus::Active))));
        assert!(matches!(
            done,
            Ok(Next::Done(JobRunOutcome::Succeeded(
                JobResultPayload::PersistentId {
                    newly_minted: true,
                    ..
                }
            )))
        ));
    }

    // A document the authority cannot see yet defers the job without using an attempt.
    #[test]
    fn missing_document_defers() {
        let done = mint(false).next(answer(Err(MetadataApiError::NotFound)));
        assert!(matches!(done, Ok(Next::Done(JobRunOutcome::Deferred(_)))));
    }

    // A refused mint records the failure on the mapping before the job fails for good.
    #[test]
    fn refusal_recorded_first() {
        let mut job = mint(false);
        assert!(is_call(&job.next(answer(Err(MetadataApiError::Forbidden)))));
        let recorded = answer(Err(MetadataApiError::ServiceUnavailable));
        let Ok(Next::Done(JobRunOutcome::Failed(error))) = job.next(recorded) else {
            panic!("expected the failed job");
        };
        assert_eq!(
            error,
            JobError::permanent(format!(
                "persistent id mint: {}",
                MetadataApiError::Forbidden
            ))
        );
    }

    // A retryable failure is recorded only on the last attempt.
    #[test]
    fn retry_recorded_last() {
        let failure = || answer(Err(MetadataApiError::ServiceUnavailable));
        let done = mint(false).next(failure());
        assert!(matches!(done, Ok(Next::Done(JobRunOutcome::Failed(_)))));
        assert!(is_call(&mint(true).next(failure())));
    }

    // Registration reports whether the mapping changed.
    #[test]
    fn identifiers_report_change() {
        let mut job = register();
        assert!(is_call(&job.first()));
        let done = job.next(answer(Ok(mapping(PersistentIdStatus::Active))));
        assert!(matches!(
            done,
            Ok(Next::Done(JobRunOutcome::Succeeded(
                JobResultPayload::Identifiers { changed: true }
            )))
        ));
    }

    // A missing mapping defers during the wait, and afterwards uses attempts.
    #[test]
    fn missing_mapping_waits() {
        let missing = || answer(Err(MetadataApiError::NotFound));
        let done = register().next(missing());
        assert!(matches!(done, Ok(Next::Done(JobRunOutcome::Deferred(_)))));
        let mut late = register();
        late.now_ms = late.queued_at_ms + IDENTIFIER_WAIT_MS;
        let done = late.next(missing());
        assert!(matches!(done, Ok(Next::Done(JobRunOutcome::Failed(_)))));
    }

    // A refusal is final, and an unreachable authority only defers.
    #[test]
    fn authority_answers_mapped() {
        let done = register().next(answer(Err(MetadataApiError::Forbidden)));
        let Ok(Next::Done(JobRunOutcome::Failed(error))) = done else {
            panic!("expected the failed job");
        };
        assert_eq!(
            error,
            JobError::permanent(format!(
                "registering identifiers: {}",
                MetadataApiError::Forbidden
            ))
        );
        let done = register().next(answer(Err(MetadataApiError::ServiceUnavailable)));
        assert!(matches!(done, Ok(Next::Done(JobRunOutcome::Deferred(_)))));
    }
}
