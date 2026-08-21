use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{Effect, JobControlEffect, NetEffect};
use aruna_core::events::{Event, JobControlEvent, NetEvent, StorageEvent};
use aruna_core::jobs::{JobRequest, JobResponse};
use aruna_core::operation::Operation;
use aruna_core::structs::{JobId, JobOwnerError, RealmConfigDocument, RealmId};
use aruna_core::types::{Effects, NodeId};
use smallvec::smallvec;

use super::protocol::JobRouteError;
use crate::document_repository::read_effect;

/// Where a routed job-control request must be served.
#[derive(Debug, PartialEq)]
pub(crate) enum JobRouteOutcome {
    /// The serving node is the owner; the caller reads its local store.
    Local,
    /// The owner answered over the wire.
    Remote(JobResponse),
}

#[derive(Debug, PartialEq)]
enum RouteState {
    LoadConfig,
    AwaitResponse,
    Done,
}

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
    state: RouteState,
    output: Option<Result<JobRouteOutcome, JobRouteError>>,
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
            state: RouteState::LoadConfig,
            output: None,
        }
    }

    /// Answers through the family's responder instead of the id-derived owner.
    pub(crate) fn with_responder(mut self, responder: Option<NodeId>) -> Self {
        self.responder = responder;
        self
    }

    fn send(&mut self, node: NodeId) -> Effects {
        if node == self.local_node {
            return self.finish(Ok(JobRouteOutcome::Local));
        }
        match self.request.take() {
            Some(request) => {
                self.state = RouteState::AwaitResponse;
                smallvec![Effect::Net(NetEffect::JobControl(Box::new(
                    JobControlEffect {
                        owner: node,
                        request
                    }
                )))]
            }
            None => self.finish(Err(JobRouteError::Unauthorized)),
        }
    }

    fn finish(&mut self, result: Result<JobRouteOutcome, JobRouteError>) -> Effects {
        self.state = RouteState::Done;
        self.output = Some(result);
        smallvec![]
    }

    fn route(&mut self, config: RealmConfigDocument) -> Effects {
        match config.job_owner(self.job_id) {
            // Only a provably invalid id is absence; unsynced state is 503.
            Err(JobOwnerError::NotJobControl) => self.finish(Err(JobRouteError::NotFound)),
            Err(JobOwnerError::Unavailable(message)) => {
                self.finish(Err(JobRouteError::Unavailable(message)))
            }
            Ok(owner) => self.send(owner),
        }
    }
}

impl Operation for JobRouteOperation {
    type Output = JobRouteOutcome;
    type Error = JobRouteError;

    fn start(&mut self) -> Effects {
        if let Some(responder) = self.responder {
            return self.send(responder);
        }
        smallvec![read_effect(
            &DocumentSyncTarget::RealmConfig {
                realm_id: self.realm_id,
            },
            None,
        )]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            RouteState::LoadConfig => match event {
                Event::Storage(StorageEvent::ReadResult {
                    value: Some(value), ..
                }) => match RealmConfigDocument::from_bytes(&value) {
                    Ok(config) => self.route(config),
                    Err(error) => self.finish(Err(JobRouteError::Unavailable(format!(
                        "realm config undecodable: {error}"
                    )))),
                },
                Event::Storage(StorageEvent::ReadResult { value: None, .. }) => self.finish(Err(
                    JobRouteError::Unavailable("realm config unavailable".to_string()),
                )),
                Event::Storage(StorageEvent::Error { error }) => {
                    self.finish(Err(JobRouteError::Unavailable(error.to_string())))
                }
                other => self.finish(Err(JobRouteError::Unavailable(format!(
                    "unexpected event while loading realm config: {other:?}"
                )))),
            },
            RouteState::AwaitResponse => match event {
                Event::Net(NetEvent::JobControl(JobControlEvent::Response(response))) => {
                    self.finish(Ok(JobRouteOutcome::Remote(*response)))
                }
                Event::Net(NetEvent::JobControl(JobControlEvent::Unavailable(message))) => self
                    .finish(Err(JobRouteError::Unavailable(format!(
                        "job owner unreachable: {message}"
                    )))),
                other => self.finish(Err(JobRouteError::Unavailable(format!(
                    "unexpected event while awaiting job response: {other:?}"
                )))),
            },
            RouteState::Done => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, RouteState::Done)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(JobRouteError::Unavailable(
            "job routing finished without output".to_string(),
        )))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(
            error,
            JobRouteError::NotFound | JobRouteError::Unauthorized | JobRouteError::Forbidden
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::effects::StorageEffect;
    use aruna_core::jobs::{JobKind, JobResponse, JobStatusView};
    use aruna_core::metadata::MetadataAuthToken;
    use aruna_core::structs::{
        AuthContext, DocumentClass, FIRST_GRANTABLE_HANDLE, HANDLE_RANGE_SIZE, HandleRange,
        JobProgress, JobState, PlacementBinding, PlacementScope, WorkspaceMode,
    };
    use aruna_core::structured_id::{BucketId, PlacementHandle};
    use aruna_core::types::UserId;
    use ulid::Ulid;

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
        }
    }

    fn token(realm_id: RealmId) -> MetadataAuthToken {
        MetadataAuthToken::internal(AuthContext {
            user_id: user(realm_id),
            realm_id,
            path_restrictions: None,
        })
    }

    fn config_read(bytes: Vec<u8>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: Some(bytes.into()),
        })
    }

    // The remote path emits exactly one JobControl effect, then the response
    // becomes the operation output; no I/O runs inside start/step.
    #[test]
    fn remote_routes_effect() {
        let realm_id = RealmId([1u8; 32]);
        let (config, job_id) = owned_job(realm_id, node(7));
        let request = JobRequest::Status {
            auth_token: token(realm_id),
            job_id,
        };
        let mut op = JobRouteOperation::new(realm_id, node(3), job_id, Some(request));
        let start = op.start();
        assert!(matches!(
            start.as_slice(),
            [Effect::Storage(StorageEffect::Read { .. })]
        ));

        let effects = op.step(config_read(config));
        let [Effect::Net(NetEffect::JobControl(job_control))] = effects.as_slice() else {
            panic!("remote owner must emit one job-control effect");
        };
        assert_eq!(job_control.owner, node(7));

        let response = JobResponse::Status {
            job: status_view(job_id, user(realm_id)),
            run_crate: None,
        };
        let done = op.step(Event::Net(NetEvent::JobControl(JobControlEvent::Response(
            Box::new(response.clone()),
        ))));
        assert!(done.is_empty());
        assert!(op.is_complete());
        assert_eq!(op.finalize(), Ok(JobRouteOutcome::Remote(response)));
    }

    // An owner that matches the serving node resolves locally with no effect.
    #[test]
    fn local_owner_resolves() {
        let realm_id = RealmId([1u8; 32]);
        let owner = node(5);
        let (config, job_id) = owned_job(realm_id, owner);
        let mut op = JobRouteOperation::new(realm_id, owner, job_id, None);
        op.start();
        let effects = op.step(config_read(config));
        assert!(effects.is_empty());
        assert_eq!(op.finalize(), Ok(JobRouteOutcome::Local));
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
        op.start();
        op.step(config_read(config));
        op.step(Event::Net(NetEvent::JobControl(
            JobControlEvent::Unavailable("stream open timed out".to_string()),
        )));
        assert!(matches!(
            op.finalize(),
            Err(JobRouteError::Unavailable(message)) if message.contains("job owner unreachable")
        ));
    }

    // A remote owner reached without a token is Unauthorized before any effect.
    #[test]
    fn remote_without_token() {
        let realm_id = RealmId([1u8; 32]);
        let (config, job_id) = owned_job(realm_id, node(7));
        let mut op = JobRouteOperation::new(realm_id, node(3), job_id, None);
        op.start();
        let effects = op.step(config_read(config));
        assert!(effects.is_empty());
        assert_eq!(op.finalize(), Err(JobRouteError::Unauthorized));
    }
}
