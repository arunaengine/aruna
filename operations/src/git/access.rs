//! Authorizes access to a dataset's Git repository on this document holder.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::git::GitRepository;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::dataset_location::default_bucket;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use ulid::Ulid;

use super::GitError;
use super::calls::node_of;
use super::publish::HoldersOperation;
use crate::auth::error::AuthorizeError;
use crate::auth::request_authorization::AuthorizeOperation;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::driver::DriverContext;
use crate::metadata::record_reads::DocumentRecordOperation;
use crate::metadata::repository::{parse_lifecycle_read, read_lifecycle_effect};
use crate::storage_read::StorageReadError;

type Document = MetadataRegistryRecord;

/// What the pending call of checking a document answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AccessStep {
    Record,
    Authorize,
    Lifecycle,
}

/// The document, once `auth` holds `permission` on it and it is not deleted.
pub fn document(auth: &AuthContext, id: Ulid, permission: Permission) -> DocumentAccessOperation {
    DocumentAccessOperation {
        auth: auth.clone(),
        id,
        permission,
        step: AccessStep::Record,
        record: None,
    }
}

#[derive(Debug, PartialEq)]
pub struct DocumentAccessOperation {
    auth: AuthContext,
    id: Ulid,
    permission: Permission,
    step: AccessStep,
    record: Option<Document>,
}

type AccessTurn = Result<Next<Document>, GitError>;

impl Operation for DocumentAccessOperation {
    type Output = Document;
    type Error = GitError;

    fn first(&mut self) -> AccessTurn {
        let read = DocumentRecordOperation::new(self.id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> AccessTurn {
        match self.step {
            AccessStep::Record => {
                let record = result.child::<Option<Document>, StorageReadError>()?;
                let record = record.map_err(|_| GitError::Unavailable)?;
                let record = record.ok_or(GitError::NotFound)?;
                let authorize = AuthorizeOperation::new(
                    record.realm_id,
                    &self.auth,
                    &record.permission_path,
                    &self.permission,
                    PolicyRequestExtras::rest(),
                );
                self.record = Some(record);
                self.step = AccessStep::Authorize;
                Ok(Next::Call(IoCall::child(authorize)))
            }
            AccessStep::Authorize => {
                result.child::<(), AuthorizeError>()??;
                let record = self.record.as_ref().ok_or(IoError::Unfinished)?;
                let read = read_lifecycle_effect(&record.graph_iri, None);
                self.step = AccessStep::Lifecycle;
                Ok(Next::Call(IoCall::from(read)))
            }
            AccessStep::Lifecycle => {
                let lifecycle = parse_lifecycle_read(result.event()?);
                let lifecycle = lifecycle.map_err(|_| GitError::Unavailable)?;
                if lifecycle.is_some_and(|state| state.is_deleted()) {
                    return Err(GitError::NotFound);
                }
                let record = self.record.take().ok_or(IoError::Unfinished)?;
                Ok(Next::Done(record))
            }
        }
    }
}

/// The document's repository on this holder; new LFS content goes to the node's dataset bucket.
pub fn repository(
    context: &DriverContext,
    auth: &AuthContext,
    id: Ulid,
    permission: Permission,
) -> RepositoryOperation {
    RepositoryOperation::new(node_of(context), auth, id, permission)
}

#[derive(Debug, PartialEq)]
pub struct RepositoryOperation {
    node: Option<NodeId>,
    access: Option<DocumentAccessOperation>,
    document: Option<Document>,
}

pub(super) type Repository = (Document, GitRepository);

impl RepositoryOperation {
    pub(super) fn new(
        node: Option<NodeId>,
        auth: &AuthContext,
        id: Ulid,
        permission: Permission,
    ) -> Self {
        Self {
            node,
            access: Some(document(auth, id, permission)),
            document: None,
        }
    }
}

impl Operation for RepositoryOperation {
    type Output = Repository;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<Repository>, GitError> {
        let access = self.access.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Repository>, GitError> {
        let Some(document) = self.document.take() else {
            let document = result.child::<Document, GitError>()??;
            let holders = HoldersOperation::new(self.node, &document);
            self.document = Some(document);
            return Ok(Next::Call(IoCall::child(holders)));
        };
        result.child::<Vec<NodeId>, GitError>()??;
        let repository = GitRepository {
            document_id: document.document_id,
            group_id: document.group_id,
            bucket: default_bucket(document.group_id),
        };
        Ok(Next::Done((document, repository)))
    }
}

#[cfg(test)]
#[path = "access_tests.rs"]
mod tests;
