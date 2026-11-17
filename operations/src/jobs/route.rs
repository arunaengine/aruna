//! Routes one job control request to the node that owns or answers for the job.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, JobControlEffect, NetEffect};
use aruna_core::events::{Event, JobControlEvent, NetEvent};
use aruna_core::id::NodeId;
use aruna_core::jobs::{JobRequest, JobResponse};
use aruna_core::operation::Operation;
use aruna_core::rows::values;
use aruna_core::structs::execution::job::JobId;
use aruna_core::structs::identity::realm::{JobOwnerError, RealmConfigDocument, RealmId};
use thiserror::Error;

/// Where a routed job-control request must be served.
#[derive(Debug, PartialEq)]
pub(crate) enum JobRouteOutcome {
    /// The serving node is the owner; the caller reads its local store.
    Local,
    /// The owner answered over the wire.
    Remote(JobResponse),
}

type Step = Result<Next<JobRouteOutcome>, JobRouteError>;

/// Routes one job-control request to the node that can answer it. An external
/// job names its responder from the family projection; every other job still
/// derives the immutable owner from its JobId. A remote request needs a token.
#[derive(Debug, PartialEq)]
pub(crate) struct JobRouteOperation {
    realm_id: RealmId,
    local_node: NodeId,
    job_id: JobId,
    request: Option<JobRequest>,
    /// Responder the caller resolved from the family projection, if any.
    responder: Option<NodeId>,
    /// Whether the request went to a remote owner, so the next result is its answer.
    sent: bool,
}

impl JobRouteOperation {
    pub(crate) fn new(
        realm_id: RealmId,
        local_node: NodeId,
        job_id: JobId,
        request: Option<JobRequest>,
    ) -> Self {
        Self {
            realm_id,
            local_node,
            job_id,
            request,
            responder: None,
            sent: false,
        }
    }

    /// Answers through the family's responder instead of the id-derived owner.
    pub(crate) fn with_responder(mut self, responder: Option<NodeId>) -> Self {
        self.responder = responder;
        self
    }

    fn send(&mut self, node: NodeId) -> Step {
        if node == self.local_node {
            return Ok(Next::Done(JobRouteOutcome::Local));
        }
        let request = self.request.take().ok_or(JobRouteError::Unauthorized)?;
        self.sent = true;
        let effect = JobControlEffect {
            owner: node,
            request,
        };
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Net(
            NetEffect::JobControl(Box::new(effect)),
        )))))
    }

    fn route(&mut self, result: IoResult) -> Step {
        let [value] = values::<1>(result)?;
        let value = value
            .ok_or_else(|| JobRouteError::Unavailable("realm config unavailable".to_string()))?;
        let config = RealmConfigDocument::from_bytes(&value).map_err(|error| {
            JobRouteError::Unavailable(format!("realm config undecodable: {error}"))
        })?;
        match config.job_owner(self.job_id) {
            // Only a provably invalid id is absence; unsynced state is 503.
            Err(JobOwnerError::NotJobControl) => Err(JobRouteError::NotFound),
            Err(JobOwnerError::Unavailable(message)) => Err(JobRouteError::Unavailable(message)),
            Ok(owner) => self.send(owner),
        }
    }
}

/// The remote owner's answer to a sent request.
fn answer(result: IoResult) -> Step {
    let event = match result {
        IoResult::Event(event) => *event,
        other => {
            let got = other.label();
            return Err(JobRouteError::Unavailable(format!(
                "unexpected result while awaiting job response: {got}"
            )));
        }
    };
    match event {
        Event::Net(NetEvent::JobControl(JobControlEvent::Response(response))) => {
            Ok(Next::Done(JobRouteOutcome::Remote(*response)))
        }
        Event::Net(NetEvent::JobControl(JobControlEvent::Unavailable(message))) => Err(
            JobRouteError::Unavailable(format!("job owner unreachable: {message}")),
        ),
        other => Err(JobRouteError::Unavailable(format!(
            "unexpected event while awaiting job response: {}",
            other.label()
        ))),
    }
}

impl Operation for JobRouteOperation {
    type Output = JobRouteOutcome;
    type Error = JobRouteError;

    fn first(&mut self) -> Step {
        if let Some(responder) = self.responder {
            return self.send(responder);
        }
        let config = DocumentTarget::RealmConfig {
            realm_id: self.realm_id,
        };
        Ok(Next::Call(IoCall::Read(vec![(
            config.storage_keyspace().to_string(),
            config.storage_key(),
        )])))
    }

    fn next(&mut self, result: IoResult) -> Step {
        match self.sent {
            true => answer(result),
            false => self.route(result),
        }
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(
            error,
            JobRouteError::NotFound | JobRouteError::Unauthorized | JobRouteError::Forbidden
        )
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum JobRouteError {
    #[error("job request is unauthorized")]
    Unauthorized,
    #[error("job request is forbidden")]
    Forbidden,
    #[error("job not found")]
    NotFound,
    #[error("job owner unavailable: {0}")]
    Unavailable(String),
    #[error("job operation failed: {0}")]
    Internal(String),
}

impl From<aruna_core::calls::IoError> for JobRouteError {
    fn from(error: aruna_core::calls::IoError) -> Self {
        match error {
            aruna_core::calls::IoError::Storage(error) => Self::Unavailable(error.to_string()),
            error => Self::Unavailable(error.to_string()),
        }
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::jobs::{JobKind, JobResponse, JobStatusView};
    use aruna_core::metadata::AuthToken;
    use aruna_core::structs::execution::job::{JobProgress, JobState, WorkspaceMode};
    use aruna_core::structs::identity::auth::AuthContext;
    use aruna_core::structs::placement::record::{
        DocumentClass, FIRST_GRANTABLE_HANDLE, HANDLE_RANGE_SIZE, HandleRange, PlacementBinding,
        PlacementScope,
    };
    use aruna_core::structured_id::{BucketId, PlacementHandle};
    use ulid::Ulid;

    // A result that is not the config read is refused.
    #[test]
    fn rejects_unfit_result() {
        let mut route = JobRouteOperation::new(
            RealmId([1u8; 32]),
            node(1),
            JobId::from_bytes([1u8; 16]),
            None,
        );
        assert!(matches!(route.first(), Ok(Next::Call(IoCall::Read(reads))) if reads.len() == 1));
        assert!(matches!(
            route.next(IoResult::Done),
            Err(JobRouteError::Unavailable(_))
        ));
    }

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn user(realm_id: RealmId) -> UserId {
        UserId::new(Ulid::from_bytes([2u8; 16]), realm_id)
    }

    /// Realm config binding job-control to `owner`, plus a JobId it owns.
    fn owned_job(realm_id: RealmId, owner: NodeId) -> (Vec<u8>, JobId) {
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        config.seed_default_placement();
        let range_id = Ulid::from_bytes([9; 16]);
        config.placement_handle_ranges.push(HandleRange {
            range_id,
            owner,
            start: FIRST_GRANTABLE_HANDLE,
            end: FIRST_GRANTABLE_HANDLE + HANDLE_RANGE_SIZE,
        });
        let handle = PlacementHandle::new(FIRST_GRANTABLE_HANDLE).unwrap();
        config.placement_bindings.push(PlacementBinding {
            handle,
            scope: PlacementScope::Realm(realm_id),
            document_class: DocumentClass::JobControl,
            strategy_id: config.default_strategy_id.unwrap(),
            allocator_range_id: Some(range_id),
            allocated_by: Some(owner),
            allocated_at_ms: Some(1),
        });
        let job_id = JobId::from_parts(1, handle, BucketId::new(0).unwrap(), 9).unwrap();
        (postcard::to_allocvec(&config).unwrap(), job_id)
    }

    fn status_view(job_id: JobId, owner: UserId) -> JobStatusView {
        JobStatusView {
            job_id,
            created_by: owner,
            kind: JobKind::Execution,
            state: JobState::Succeeded,
            attempts: 1,
            cancel_requested: false,
            created_at_ms: 1,
            updated_at_ms: 2,
            finished_at_ms: Some(2),
            progress: JobProgress::new("items"),
            last_error: None,
            result: None,
            workspace_bucket: None,
            workspace_mode: WorkspaceMode::None,
            locally_exhausted: false,
            session_runtime: None,
            awaiting_keys: Vec::new(),
        }
    }

    fn token(realm_id: RealmId) -> AuthToken {
        AuthToken::internal(AuthContext {
            user_id: user(realm_id),
            realm_id,
            path_restrictions: None,
            session: None,
        })
    }

    fn config_read(bytes: Vec<u8>) -> IoResult {
        IoResult::Values(vec![Some(bytes.into())])
    }

    fn event(event: Event) -> IoResult {
        IoResult::Event(Box::new(event))
    }

    // The remote path makes exactly one JobControl call, then the response
    // becomes the operation output.
    #[test]
    fn remote_routes_effect() {
        let realm_id = RealmId([1u8; 32]);
        let (config, job_id) = owned_job(realm_id, node(7));
        let request = JobRequest::Status {
            auth_token: token(realm_id),
            job_id,
        };
        let mut op = JobRouteOperation::new(realm_id, node(3), job_id, Some(request));
        assert!(matches!(op.first(), Ok(Next::Call(IoCall::Read(_)))));

        let Ok(Next::Call(IoCall::Effect(effect))) = op.next(config_read(config)) else {
            panic!("remote owner must make one job-control call");
        };
        let Effect::Net(NetEffect::JobControl(job_control)) = *effect else {
            panic!("remote owner must make one job-control call");
        };
        assert_eq!(job_control.owner, node(7));

        let response = JobResponse::Status {
            job: status_view(job_id, user(realm_id)),
            run_crate: None,
        };
        let answered = Event::Net(NetEvent::JobControl(JobControlEvent::Response(Box::new(
            response.clone(),
        ))));
        assert_eq!(
            op.next(event(answered)),
            Ok(Next::Done(JobRouteOutcome::Remote(response)))
        );
    }

    // An owner that matches the serving node resolves locally with no call.
    #[test]
    fn local_owner_resolves() {
        let realm_id = RealmId([1u8; 32]);
        let owner = node(5);
        let (config, job_id) = owned_job(realm_id, owner);
        let mut op = JobRouteOperation::new(realm_id, owner, job_id, None);
        op.first().expect("config read");
        assert_eq!(
            op.next(config_read(config)),
            Ok(Next::Done(JobRouteOutcome::Local))
        );
    }

    // An unreachable owner is Unavailable (503), never a false 404.
    #[test]
    fn unreachable_owner_unavailable() {
        let realm_id = RealmId([1u8; 32]);
        let (config, job_id) = owned_job(realm_id, node(7));
        let request = JobRequest::Cancel {
            auth_token: token(realm_id),
            job_id,
        };
        let mut op = JobRouteOperation::new(realm_id, node(3), job_id, Some(request));
        op.first().expect("config read");
        op.next(config_read(config)).expect("job-control call");
        let unreachable = Event::Net(NetEvent::JobControl(JobControlEvent::Unavailable(
            "stream open timed out".to_string(),
        )));
        assert!(matches!(
            op.next(event(unreachable)),
            Err(JobRouteError::Unavailable(message)) if message.contains("job owner unreachable")
        ));
    }

    // A remote owner reached without a token is Unauthorized before any call.
    #[test]
    fn remote_without_token() {
        let realm_id = RealmId([1u8; 32]);
        let (config, job_id) = owned_job(realm_id, node(7));
        let mut op = JobRouteOperation::new(realm_id, node(3), job_id, None);
        op.first().expect("config read");
        assert_eq!(
            op.next(config_read(config)),
            Err(JobRouteError::Unauthorized)
        );
    }
}
