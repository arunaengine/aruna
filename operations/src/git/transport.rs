//! Serves Git over HTTP from this holder's repository cache.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::git::{GitEffect, GitEvent, GitRequest};
use aruna_core::git_state::Projection;
use aruna_core::operation::{HoldScope, Operation};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use tracing::warn;

use super::access::Repository;
use super::calls::{git_call, git_event, push_call};
use super::{GitError, GitNode, pending, repository, snapshot};

/// What the pending call of serving a request answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ServeStep {
    Repository,
    Cache,
    Serve,
    Pending,
}

/// Serves Git over HTTP after bringing the local cache up to date from the records. Reads need
/// a current cache but no hold; pushes keep the repository until their record is out.
pub fn transport(node: &GitNode, auth: &AuthContext, request: GitRequest) -> TransportOperation {
    let write = request.action == "git-receive-pack" || request.query == "service=git-receive-pack";
    TransportOperation {
        node: node.clone(),
        auth: auth.clone(),
        write,
        request: Some(request),
        step: ServeStep::Repository,
        document: None,
        served: None,
    }
}

#[derive(Debug, PartialEq)]
pub struct TransportOperation {
    node: GitNode,
    auth: AuthContext,
    write: bool,
    request: Option<GitRequest>,
    step: ServeStep,
    document: Option<MetadataRegistryRecord>,
    served: Option<GitEvent>,
}

type ServeTurn = Result<Next<GitEvent>, GitError>;

impl TransportOperation {
    fn request(&self) -> Result<&GitRequest, IoError> {
        self.request.as_ref().ok_or(IoError::Unfinished)
    }

    fn serve(&mut self) -> ServeTurn {
        let request = self.request.take().ok_or(IoError::Unfinished)?;
        let actor = self.auth.user_id;
        self.step = ServeStep::Serve;
        Ok(Next::Call(match self.write {
            true => push_call(request, actor),
            false => git_call(GitEffect::Http(Box::new(request)), actor),
        }))
    }

    fn finish(&mut self) -> ServeTurn {
        Ok(Next::Done(self.served.take().ok_or(IoError::Unfinished)?))
    }
}

impl Operation for TransportOperation {
    type Output = GitEvent;
    type Error = GitError;

    fn hold_scope(&self) -> Option<HoldScope> {
        let document_id = self.request.as_ref()?.repository.document_id;
        self.write.then_some(HoldScope::GitRepository(document_id))
    }

    fn first(&mut self) -> ServeTurn {
        let permission = match self.write {
            true => Permission::WRITE,
            false => Permission::READ,
        };
        let id = self.request()?.repository.document_id;
        let read = repository(self.node.node(), &self.auth, id, permission);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> ServeTurn {
        match self.step {
            ServeStep::Repository => {
                let (document, repository) = result.child::<Repository, GitError>()??;
                if repository != self.request()?.repository {
                    return Err(GitError::Conflict);
                }
                let cache = match self.write {
                    true => IoCall::child(snapshot::refresh(&self.node, &document)),
                    false => IoCall::child(snapshot::read_view(&self.node, &document)),
                };
                self.document = Some(document);
                self.step = ServeStep::Cache;
                Ok(Next::Call(cache))
            }
            ServeStep::Cache => {
                match self.write {
                    true => drop(result.child::<Projection, GitError>()??),
                    false => drop(result.child::<(Projection, bool), GitError>()??),
                }
                self.serve()
            }
            ServeStep::Serve => {
                self.served = Some(git_event(result)?);
                if !self.write {
                    return self.finish();
                }
                // A recorded push to main brings metadata; it applies now, or on a later refresh.
                let document = self.document.as_ref().ok_or(IoError::Unfinished)?;
                let apply = pending::apply(&self.node, document);
                self.step = ServeStep::Pending;
                Ok(Next::Call(IoCall::child(apply)))
            }
            ServeStep::Pending => {
                if let Err(error) = result.child::<(), GitError>()? {
                    let document = self.document.as_ref().ok_or(IoError::Unfinished)?;
                    let document_id = document.document_id;
                    warn!(%document_id, %error, "Pushed metadata waits");
                }
                self.finish()
            }
        }
    }
}

#[cfg(test)]
#[path = "transport_tests.rs"]
mod tests;
