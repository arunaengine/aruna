use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use aruna_core::admin_documents::AdminDocumentTarget;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::join_request::{JoinDecisionKind, JoinRequestState};
use aruna_core::keyspaces::ADMIN_DOCUMENT_STATE_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::reducer::decode_admin_document_reducer_state;
use aruna_core::storage_entries::admin_document_reducer_state_key;
use aruna_core::structs::{AuthContext, Permission};
use aruna_core::types::{Effects, Key, Value};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, PartialEq)]
pub struct ListJoinRequestsInput {
    pub auth: AuthContext,
    pub group_id: Option<Ulid>,
    pub pending_only: bool,
    pub start_after: Option<(Ulid, Ulid)>,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct JoinRequestsPage {
    pub requests: Vec<JoinRequestState>,
    pub next_start_after: Option<(Ulid, Ulid)>,
}

#[derive(Debug, Error, PartialEq)]
pub enum ListJoinRequestsError {
    #[error("not authorized to list membership requests")]
    Unauthorized,
    #[error("unexpected event while listing membership requests")]
    UnexpectedEvent,
    #[error("membership request listing did not finish")]
    NotFinished,
    #[error(transparent)]
    Authorization(#[from] AuthorizationError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum State {
    Init,
    Auth,
    Read,
    Done,
    Failed,
}

#[derive(Debug, PartialEq)]
pub struct ListJoinRequestsOperation {
    input: ListJoinRequestsInput,
    state: State,
    requests: Vec<JoinRequestState>,
    output: Option<Result<JoinRequestsPage, ListJoinRequestsError>>,
}

impl ListJoinRequestsOperation {
    pub fn new(mut input: ListJoinRequestsInput) -> Self {
        input.limit = input.limit.clamp(1, 100);
        Self {
            input,
            state: State::Init,
            requests: Vec::new(),
            output: None,
        }
    }

    fn fail(&mut self, error: ListJoinRequestsError) -> Effects {
        self.state = State::Failed;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn read(&mut self, after: Option<Key>) -> Effects {
        self.state = State::Read;
        let prefix = self.input.group_id.map_or_else(
            || Key::from(vec![b'g']),
            |group_id| admin_document_reducer_state_key(&AdminDocumentTarget::Group { group_id }),
        );
        let start = after.map(IterStart::After).or_else(|| {
            self.input.start_after.map(|(group_id, _)| {
                IterStart::At(admin_document_reducer_state_key(
                    &AdminDocumentTarget::Group { group_id },
                ))
            })
        });
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: ADMIN_DOCUMENT_STATE_KEYSPACE.into(),
            prefix: Some(prefix),
            start,
            limit: 64,
            txn_id: None,
        })]
    }

    fn collect(
        &mut self,
        values: Vec<(Key, Value)>,
        next: Option<Key>,
    ) -> Result<Effects, ListJoinRequestsError> {
        for (key, value) in values {
            let state =
                decode_admin_document_reducer_state(&value).map_err(ConversionError::from)?;
            let AdminDocumentTarget::Group { group_id } = state.target else {
                return Err(ListJoinRequestsError::UnexpectedEvent);
            };
            if key != admin_document_reducer_state_key(&state.target) {
                return Err(ListJoinRequestsError::UnexpectedEvent);
            }
            for entry in state.join_requests() {
                if entry.request.user_id.realm_id != self.input.auth.realm_id
                    || (self.input.group_id.is_none()
                        && entry.request.user_id != self.input.auth.user_id)
                    || (self.input.pending_only && entry.decision.is_some())
                    || entry
                        .decision
                        .as_ref()
                        .is_some_and(|decision| decision.kind == JoinDecisionKind::Withdrawn)
                    || self
                        .input
                        .start_after
                        .is_some_and(|cursor| (group_id, entry.request.request_id) <= cursor)
                {
                    continue;
                }
                self.requests.push(entry);
                if self.requests.len() > self.input.limit {
                    self.requests.truncate(self.input.limit);
                    let last = self
                        .requests
                        .last()
                        .map(|entry| (entry.request.group_id, entry.request.request_id));
                    return Ok(self.finish(last));
                }
            }
        }
        Ok(match next {
            Some(key) => self.read(Some(key)),
            None => self.finish(None),
        })
    }

    fn finish(&mut self, next_start_after: Option<(Ulid, Ulid)>) -> Effects {
        self.state = State::Done;
        self.output = Some(Ok(JoinRequestsPage {
            requests: std::mem::take(&mut self.requests),
            next_start_after,
        }));
        smallvec![]
    }
}

impl Operation for ListJoinRequestsOperation {
    type Output = JoinRequestsPage;
    type Error = ListJoinRequestsError;
    fn start(&mut self) -> Effects {
        if self.state != State::Init {
            return self.fail(ListJoinRequestsError::UnexpectedEvent);
        }
        if self.input.auth.path_restrictions.is_some()
            || self.input.auth.user_id.is_nil()
            || self.input.auth.user_id.realm_id != self.input.auth.realm_id
        {
            return self.fail(ListJoinRequestsError::Unauthorized);
        }
        if let Some(group_id) = self.input.group_id {
            self.state = State::Auth;
            return smallvec![Effect::SubOperation(boxed_suboperation(
                CheckPermissionsOperation::new(CheckPermissionsConfig {
                    auth_context: self.input.auth.clone(),
                    path: format!("/{}/g/{group_id}/admin/users/**", self.input.auth.realm_id),
                    required_permission: Permission::WRITE,
                }),
                |allowed| Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed })
            ))];
        }
        self.read(None)
    }
    fn step(&mut self, event: Event) -> Effects {
        match (self.state, event) {
            (_, Event::Storage(StorageEvent::Error { error })) => self.fail(error.into()),
            (
                State::Auth,
                Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }),
            ) => match allowed {
                Ok(true) => self.read(None),
                Ok(false) => self.fail(ListJoinRequestsError::Unauthorized),
                Err(error) => self.fail(error.into()),
            },
            (
                State::Read,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => match self.collect(values, next_start_after) {
                Ok(effects) => effects,
                Err(error) => self.fail(error),
            },
            _ => self.fail(ListJoinRequestsError::UnexpectedEvent),
        }
    }
    fn is_complete(&self) -> bool {
        matches!(self.state, State::Done | State::Failed)
    }
    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(ListJoinRequestsError::NotFinished)?
    }
    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
