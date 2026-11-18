//! Reads and checks a link's record lineage in an Invenio repository.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::repository::invenio::{record_id, validate_id};
use aruna_core::repository::remote::InvenioError;
use aruna_core::repository::{
    LinkFailure, LinkReview, LinkTarget, PullCheck, RemoteState, RepositoryDestination,
    RepositoryLink, RepositoryRecord,
};
use aruna_core::rows::value;
use aruna_core::structs::execution::harvest::RepositoryConnectorKind;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use serde_json::Value;

use super::export::{file_list, record_from};
use super::{Session, json, json_reply, optional_json};
use crate::jobs::repository::connector::{ConnectOperation, ConnectRequest};
use crate::jobs::repository::link_queue::{PushRevisionOperation, Revision};
use crate::jobs::repository::link_store::{parse_secret, secret_row};
use crate::jobs::repository::links::LinkError;
use crate::jobs::repository::push::creator_auth;
use crate::jobs::repository::{NodeScope, TransferError, not_supported};

type Turn<T> = Result<Next<T>, TransferError>;

fn session_of(result: IoResult) -> Result<Session, TransferError> {
    result.child::<Session, TransferError>()?
}

fn changed() -> TransferError {
    TransferError::Refused(LinkFailure::RemoteChanged)
}

/// A read whose missing target means the lineage moved outside Aruna.
fn lineage_read(result: IoResult) -> Result<Value, TransferError> {
    match json_reply(result)? {
        Ok(value) => Ok(value),
        Err(InvenioError::Status(404 | 410)) => Err(changed()),
        Err(error) => Err(error.into()),
    }
}

#[derive(Debug, PartialEq)]
enum LineageStep {
    Connect,
    Draft(Session),
    Published,
    Parent(String),
}

/// Fails as remote changed when a link's lineage moved outside Aruna; answers the published
/// version a parent-only link continues.
#[derive(Debug, PartialEq)]
pub(crate) struct CheckLineageOperation {
    kind: RepositoryConnectorKind,
    scope: NodeScope,
    auth: AuthContext,
    limit: u64,
    destination: RepositoryDestination,
    target: LinkTarget,
    step: LineageStep,
}

impl CheckLineageOperation {
    pub(crate) fn new(
        kind: RepositoryConnectorKind,
        scope: NodeScope,
        auth: AuthContext,
        limit: u64,
        destination: RepositoryDestination,
        target: LinkTarget,
    ) -> Self {
        Self {
            kind,
            scope,
            auth,
            limit,
            destination,
            target,
            step: LineageStep::Connect,
        }
    }

    // The record's own version flag comes from the database, so no search index lag applies.
    fn check_target(&mut self, session: &Session) -> Turn<Option<String>> {
        if let Some(published) = &self.target.published_id {
            validate_id(published)?;
            let url = session.url(&["records", published])?;
            self.step = LineageStep::Published;
            return Ok(Next::Call(session.get(url)));
        }
        let parent = self
            .target
            .parent_id
            .as_ref()
            .filter(|_| self.destination.draft_id.is_none());
        let Some(parent) = parent else {
            return Ok(Next::Done(None));
        };
        validate_id(parent)?;
        let mut url = session.url(&["records"])?;
        url.query_pairs_mut()
            .append_pair("q", &format!("parent.id:{parent}"))
            .append_pair("size", "1");
        self.step = LineageStep::Parent(parent.clone());
        Ok(Next::Call(session.get(url)))
    }
}

impl Operation for CheckLineageOperation {
    type Output = Option<String>;
    type Error = TransferError;

    fn first(&mut self) -> Turn<Option<String>> {
        if self.kind != RepositoryConnectorKind::Invenio {
            return Err(not_supported("links"));
        }
        Ok(Next::Call(IoCall::child(ConnectOperation::new(
            ConnectRequest {
                scope: self.scope.clone(),
                auth: self.auth.clone(),
                group_id: self.destination.group_id,
                connector_id: self.destination.connector_id,
                permission: Permission::WRITE,
                limit: self.limit,
                credential: self.destination.credential.clone(),
                stop: None,
            },
        ))))
    }

    fn next(&mut self, result: IoResult) -> Turn<Option<String>> {
        match std::mem::replace(&mut self.step, LineageStep::Published) {
            LineageStep::Connect => {
                let session = session_of(result)?;
                let Some(draft) = self.destination.draft_id.clone() else {
                    return self.check_target(&session);
                };
                validate_id(&draft)?;
                let url = session.url(&["records", &draft, "draft"])?;
                let call = session.get(url);
                self.step = LineageStep::Draft(session);
                Ok(Next::Call(call))
            }
            LineageStep::Draft(session) => {
                let current = lineage_read(result)?;
                // Another revision than this link's last write means someone edited the draft.
                if self
                    .target
                    .revision_id
                    .is_some_and(|revision| current["revision_id"].as_u64() != Some(revision))
                {
                    return Err(changed());
                }
                self.check_target(&session)
            }
            LineageStep::Published => {
                let record = lineage_read(result)?;
                if record["versions"]["is_latest"] != true {
                    return Err(changed());
                }
                Ok(Next::Done(None))
            }
            LineageStep::Parent(parent) => {
                let page = lineage_read(result)?;
                let hit = &page["hits"]["hits"][0];
                match hit["id"].as_str() {
                    Some(id) if hit["parent"]["id"] == parent.as_str() => {
                        validate_id(id)?;
                        Ok(Next::Done(Some(id.to_string())))
                    }
                    _ => Err(changed()),
                }
            }
        }
    }
}

/// Opens the connector with the link's own sealed token.
#[derive(Debug, PartialEq)]
struct LinkSessionOperation {
    scope: NodeScope,
    link: RepositoryLink,
    connecting: bool,
}

impl LinkSessionOperation {
    fn new(scope: NodeScope, link: RepositoryLink) -> Self {
        Self {
            scope,
            link,
            connecting: false,
        }
    }
}

impl Operation for LinkSessionOperation {
    type Output = Session;
    type Error = TransferError;

    fn first(&mut self) -> Turn<Session> {
        Ok(Next::Call(IoCall::Read(vec![secret_row(self.link.link_id)])))
    }

    fn next(&mut self, result: IoResult) -> Turn<Session> {
        if self.connecting {
            return Ok(Next::Done(session_of(result)?));
        }
        let link = &self.link;
        let credential = parse_secret(value(result)?)
            .map_err(|error| TransferError::Retryable(error.to_string()))?
            .filter(|secret| secret.link_id == Some(link.link_id) && secret.endpoint == link.endpoint)
            .ok_or_else(|| TransferError::Permanent("the link token was removed".into()))?;
        self.connecting = true;
        Ok(Next::Call(IoCall::child(ConnectOperation::new(
            ConnectRequest {
                auth: creator_auth(&self.scope, link),
                scope: self.scope.clone(),
                group_id: link.group_id,
                connector_id: link.connector_id,
                permission: Permission::WRITE,
                limit: link.limits.metadata_bytes,
                credential: Some(credential),
                stop: None,
            },
        ))))
    }
}

fn review_of(record: &Value) -> Option<LinkReview> {
    match record["parent"]["review"]["status"].as_str()? {
        "created" | "submitted" => Some(LinkReview::Pending),
        "accepted" => Some(LinkReview::Accepted),
        "declined" | "cancelled" | "expired" => Some(LinkReview::Declined),
        _ => None,
    }
}

fn files_url(session: &Session, id: &str, published: bool) -> Result<url::Url, TransferError> {
    match published {
        true => session.url(&["records", id, "files"]),
        false => session.url(&["records", id, "draft", "files"]),
    }
}

#[derive(Debug, PartialEq)]
enum ReviewStep {
    Connect,
    Draft(Session),
    Record(Session),
    Files(RemoteState),
}

/// The repository's answer to a pending review; `None` while the community still decides.
#[derive(Debug, PartialEq)]
pub(crate) struct ReviewStateOperation {
    scope: NodeScope,
    link: RepositoryLink,
    step: ReviewStep,
}

impl ReviewStateOperation {
    pub(crate) fn new(scope: NodeScope, link: RepositoryLink) -> Self {
        Self {
            scope,
            link,
            step: ReviewStep::Connect,
        }
    }

    fn draft_id(&self) -> Result<String, TransferError> {
        self.link
            .remote
            .draft_id
            .clone()
            .ok_or_else(|| TransferError::Permanent("the link holds no draft".into()))
    }
}

impl Operation for ReviewStateOperation {
    type Output = Option<RemoteState>;
    type Error = TransferError;

    fn first(&mut self) -> Turn<Option<RemoteState>> {
        if self.link.kind != RepositoryConnectorKind::Invenio {
            return Err(not_supported("reviews"));
        }
        let Some(draft_id) = self.link.remote.draft_id.as_deref() else {
            return Ok(Next::Done(None));
        };
        validate_id(draft_id)?;
        Ok(Next::Call(IoCall::child(LinkSessionOperation::new(
            self.scope.clone(),
            self.link.clone(),
        ))))
    }

    fn next(&mut self, result: IoResult) -> Turn<Option<RemoteState>> {
        let draft_id = self.draft_id()?;
        match std::mem::replace(&mut self.step, ReviewStep::Connect) {
            ReviewStep::Connect => {
                let session = session_of(result)?;
                let call = session.get(session.url(&["records", &draft_id, "draft"])?);
                self.step = ReviewStep::Draft(session);
                Ok(Next::Call(call))
            }
            ReviewStep::Draft(session) => {
                let Some(draft) = optional_json(result)? else {
                    // An accepted review publishes the draft, which then leaves the draft
                    // endpoint.
                    let call = session.get(session.url(&["records", &draft_id])?);
                    self.step = ReviewStep::Record(session);
                    return Ok(Next::Call(call));
                };
                let review = review_of(&draft).unwrap_or(LinkReview::Declined);
                if review == LinkReview::Pending {
                    return Ok(Next::Done(None));
                }
                let state = RemoteState {
                    draft: Some(record_from(&session, &draft)?),
                    latest: None,
                    review,
                    files: Vec::new(),
                };
                let call = session.get(files_url(&session, &draft_id, false)?);
                self.step = ReviewStep::Files(state);
                Ok(Next::Call(call))
            }
            ReviewStep::Record(session) => {
                let record = optional_json(result)?.ok_or_else(changed)?;
                let state = RemoteState {
                    draft: None,
                    latest: Some(record_from(&session, &record)?),
                    review: LinkReview::Accepted,
                    files: Vec::new(),
                };
                let call = session.get(files_url(&session, &draft_id, true)?);
                self.step = ReviewStep::Files(state);
                Ok(Next::Call(call))
            }
            ReviewStep::Files(mut state) => {
                state.files = file_list(&json(result)?)?;
                Ok(Next::Done(Some(state)))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum StateStep {
    Connect,
    Draft(Session),
    Versions(Session, Option<Value>),
    Files(Option<RepositoryRecord>, Option<RepositoryRecord>, LinkReview),
}

/// The repository's current draft and latest published version, for accepting remote edits.
#[derive(Debug, PartialEq)]
pub struct RemoteStateOperation {
    scope: NodeScope,
    link: RepositoryLink,
    step: StateStep,
}

impl RemoteStateOperation {
    pub fn new(scope: NodeScope, link: RepositoryLink) -> Self {
        Self {
            scope,
            link,
            step: StateStep::Connect,
        }
    }

    fn versions(&mut self, session: Session, draft: Option<Value>) -> Turn<RemoteState> {
        let anchor = self
            .link
            .remote
            .record_id
            .as_deref()
            .or(self.link.remote.draft_id.as_deref());
        let Some(anchor) = anchor else {
            return self.files(session, draft, None);
        };
        validate_id(anchor)?;
        let mut url = session.url(&["records", anchor, "versions"])?;
        url.query_pairs_mut()
            .append_pair("size", "1")
            .append_pair("sort", "version");
        let call = session.get(url);
        self.step = StateStep::Versions(session, draft);
        Ok(Next::Call(call))
    }

    fn files(
        &mut self,
        session: Session,
        draft: Option<Value>,
        latest: Option<RepositoryRecord>,
    ) -> Turn<RemoteState> {
        let pending = self.link.remote.review == LinkReview::Pending;
        let review = match &draft {
            Some(draft) => review_of(draft).unwrap_or(LinkReview::None),
            None if pending && latest.is_some() => LinkReview::Accepted,
            None if pending => LinkReview::None,
            None => self.link.remote.review,
        };
        let draft = draft
            .map(|draft| record_from(&session, &draft))
            .transpose()?;
        let url = match (&draft, &latest) {
            (Some(draft), _) => files_url(&session, &draft.id, false)?,
            (None, Some(latest)) => files_url(&session, &latest.id, true)?,
            (None, None) => {
                return Ok(Next::Done(RemoteState {
                    draft,
                    latest,
                    review,
                    files: Vec::new(),
                }));
            }
        };
        self.step = StateStep::Files(draft, latest, review);
        Ok(Next::Call(session.get(url)))
    }
}

impl Operation for RemoteStateOperation {
    type Output = RemoteState;
    type Error = TransferError;

    fn first(&mut self) -> Turn<RemoteState> {
        if self.link.kind != RepositoryConnectorKind::Invenio {
            return Err(not_supported("links"));
        }
        Ok(Next::Call(IoCall::child(LinkSessionOperation::new(
            self.scope.clone(),
            self.link.clone(),
        ))))
    }

    fn next(&mut self, result: IoResult) -> Turn<RemoteState> {
        match std::mem::replace(&mut self.step, StateStep::Connect) {
            StateStep::Connect => {
                let session = session_of(result)?;
                let Some(id) = self.link.remote.draft_id.clone() else {
                    return self.versions(session, None);
                };
                validate_id(&id)?;
                let call = session.get(session.url(&["records", &id, "draft"])?);
                self.step = StateStep::Draft(session);
                Ok(Next::Call(call))
            }
            StateStep::Draft(session) => {
                let draft = optional_json(result)?;
                self.versions(session, draft)
            }
            StateStep::Versions(session, draft) => {
                let page = optional_json(result)?.unwrap_or(Value::Null);
                let hit = &page["hits"]["hits"][0];
                let latest = hit
                    .is_object()
                    .then(|| record_from(&session, hit))
                    .transpose()?;
                self.files(session, draft, latest)
            }
            StateStep::Files(draft, latest, review) => Ok(Next::Done(RemoteState {
                draft,
                latest,
                review,
                files: file_list(&json(result)?)?,
            })),
        }
    }
}

#[derive(Debug, PartialEq)]
enum LatestStep {
    Revision,
    Connect(Option<ulid::Ulid>),
    Record(Session, Option<ulid::Ulid>),
    Versions(Option<ulid::Ulid>),
}

/// The latest version of a pull link's record lineage. The held record's own version flag comes
/// from the database; only a newer version needs the version listing.
#[derive(Debug, PartialEq)]
pub(crate) struct LatestVersionOperation {
    scope: NodeScope,
    link: RepositoryLink,
    step: LatestStep,
}

impl LatestVersionOperation {
    pub(crate) fn new(scope: NodeScope, link: RepositoryLink) -> Self {
        Self {
            scope,
            link,
            step: LatestStep::Revision,
        }
    }

    fn held(&self) -> Result<&str, TransferError> {
        self.link
            .remote
            .record_id
            .as_deref()
            .ok_or_else(|| TransferError::Permanent("the link holds no record".into()))
    }

    fn found(latest: &Value, local: Option<ulid::Ulid>) -> Turn<PullCheck> {
        let latest_id = record_id(latest)?.to_string();
        let revision = latest["revision_id"]
            .as_u64()
            .ok_or_else(|| TransferError::Permanent("missing record revision".into()))?;
        Ok(Next::Done(PullCheck::Found {
            latest_id,
            revision,
            local,
        }))
    }
}

/// A read whose missing record means the source is gone.
fn source_read(result: IoResult) -> Result<Value, TransferError> {
    match json_reply(result)? {
        Ok(value) => Ok(value),
        Err(InvenioError::Status(404 | 410)) => {
            Err(TransferError::Refused(LinkFailure::SourceUnavailable))
        }
        Err(error) => Err(error.into()),
    }
}

impl Operation for LatestVersionOperation {
    type Output = PullCheck;
    type Error = TransferError;

    fn first(&mut self) -> Turn<PullCheck> {
        if self.link.kind != RepositoryConnectorKind::Invenio {
            return Err(not_supported("pull links"));
        }
        validate_id(self.held()?)?;
        Ok(Next::Call(IoCall::child(PushRevisionOperation::new(
            self.link.document_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Turn<PullCheck> {
        match std::mem::replace(&mut self.step, LatestStep::Revision) {
            LatestStep::Revision => {
                let local = result
                    .child::<Option<Revision>, LinkError>()?
                    .ok()
                    .flatten()
                    .map(|(event_id, _)| event_id);
                self.step = LatestStep::Connect(local);
                Ok(Next::Call(IoCall::child(ConnectOperation::new(
                    ConnectRequest {
                        auth: creator_auth(&self.scope, &self.link),
                        scope: self.scope.clone(),
                        group_id: self.link.group_id,
                        connector_id: self.link.connector_id,
                        permission: Permission::READ,
                        limit: self.link.limits.metadata_bytes,
                        credential: None,
                        stop: None,
                    },
                ))))
            }
            LatestStep::Connect(local) => {
                let session = session_of(result)?;
                let call = session.get(session.url(&["records", self.held()?])?);
                self.step = LatestStep::Record(session, local);
                Ok(Next::Call(call))
            }
            LatestStep::Record(session, local) => {
                let record = source_read(result)?;
                if record["versions"]["is_latest"] == true {
                    return Self::found(&record, local);
                }
                let mut url = session.url(&["records", self.held()?, "versions"])?;
                url.query_pairs_mut()
                    .append_pair("size", "1")
                    .append_pair("sort", "version");
                self.step = LatestStep::Versions(local);
                Ok(Next::Call(session.get(url)))
            }
            LatestStep::Versions(local) => {
                let page = source_read(result)?;
                Self::found(&page["hits"]["hits"][0], local)
            }
        }
    }
}

#[cfg(test)]
#[path = "remote_tests.rs"]
mod tests;
