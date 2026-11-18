//! Writes recoverable native object references for repository files without copying their bytes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::time::SystemTime;

use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::events::Event;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::repository::invenio::{
    REFERENCE_FILE, REFERENCE_GROUP, REFERENCE_RECORD, validate_id,
};
use aruna_core::repository::remote::RemoteCall;
use aruna_core::structs::execution::harvest::RepositoryConnectorKind;
use aruna_core::structs::execution::job::{ImportRoCrateSource, ImportRoCrateSpec};
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::execution::source_connector::{SourceConnector, SourceConnectorKind};
use aruna_core::structs::execution::staging::StagingStrategy;
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::storage::blob::BucketInfo;
use serde_json::Value;
use ulid::Ulid;

use super::{Session, finished, head_metadata, invalid};
use crate::connectors::descriptor::build_source_binding;
use crate::jobs::repository::connector::{ConnectOperation, ConnectRequest};
use crate::jobs::repository::link_store::time_of;
use crate::jobs::repository::{JobScope, NodeScope, TransferError, not_supported, remote_call};
use crate::staging::reference::{ReferenceWrite, WriteReferenceOperation};

#[derive(Debug, PartialEq)]
enum ReferenceStep {
    Connect,
    Head(Session),
    Checkpoint(Session, SourceMetadata),
    Write(SourceMetadata),
}

/// Writes a reference object whose bytes stay in the repository.
#[derive(Debug, PartialEq)]
pub(crate) struct WriteRepositoryReference {
    kind: RepositoryConnectorKind,
    scope: NodeScope,
    job: JobScope,
    spec: ImportRoCrateSpec,
    bucket: BucketInfo,
    key: String,
    version_id: Ulid,
    descriptor: Value,
    operation_id: Ulid,
    now_ms: u64,
    step: ReferenceStep,
}

impl WriteRepositoryReference {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        kind: RepositoryConnectorKind,
        scope: NodeScope,
        job: JobScope,
        spec: ImportRoCrateSpec,
        bucket: BucketInfo,
        key: String,
        version_id: Ulid,
        descriptor: Value,
        operation_id: Ulid,
    ) -> Self {
        Self {
            kind,
            scope,
            job,
            spec,
            bucket,
            key,
            version_id,
            descriptor,
            operation_id,
            now_ms: 0,
            step: ReferenceStep::Connect,
        }
    }

    fn connector(&self) -> Result<(Ulid, Ulid), TransferError> {
        let ImportRoCrateSource::Repository {
            group_id,
            connector_id,
            ..
        } = &self.spec.source
        else {
            return Err(invalid("reference requires repository source"));
        };
        Ok((*group_id, *connector_id))
    }

    fn file(&self) -> Result<(&str, &str), TransferError> {
        let id = self.descriptor["record_id"]
            .as_str()
            .ok_or_else(|| invalid("missing reference record"))?;
        validate_id(id)?;
        let name = self.descriptor["file"]["key"]
            .as_str()
            .ok_or_else(|| invalid("missing reference key"))?;
        Ok((id, name))
    }

    fn write(&self, session: &Session, metadata: &SourceMetadata) -> Result<IoCall, TransferError> {
        let (group_id, connector_id) = self.connector()?;
        let (id, name) = self.file()?;
        // The binding freezes the API root and file; reads resolve the token by connector id.
        let connector = SourceConnector::new(
            connector_id,
            group_id,
            String::new(),
            SourceConnectorKind::Invenio,
            HashMap::from([
                ("endpoint".to_string(), session.endpoint().to_string()),
                (REFERENCE_GROUP.to_string(), group_id.to_string()),
                (REFERENCE_RECORD.to_string(), id.to_string()),
                (REFERENCE_FILE.to_string(), name.to_string()),
            ]),
            SystemTime::UNIX_EPOCH,
            SystemTime::UNIX_EPOCH,
            self.spec.auth_context.user_id,
        );
        let source = build_source_binding(
            StagingStrategy::Reference,
            &connector,
            metadata,
            "content".into(),
            Some(self.job.owner_node),
            Some(connector_id),
        );
        let operation = WriteReferenceOperation::new(
            ReferenceWrite {
                preassigned_version_id: Some(self.version_id),
                group_id,
                user_id: self.spec.auth_context.user_id,
                realm_id: self.spec.auth_context.realm_id,
                node_id: self.job.owner_node,
                bucket: self.spec.target.bucket.clone(),
                key: self.key.clone(),
                expected_bucket: Some(self.bucket.clone()),
                version_source: source,
                metadata: metadata.clone(),
                inherited_policies: Vec::new(),
                connector_guard: None,
                restrictions: self.spec.auth_context.path_restrictions.clone(),
                mirror_guards: Vec::new(),
                job_id: Some(self.job.job_id.as_ulid()),
            },
            time_of(self.now_ms),
            self.operation_id,
        );
        Ok(IoCall::Child(boxed_suboperation(operation, |result| {
            let result = result.map(|_| ()).map_err(|error| error.to_string());
            Event::Child(ChildOutcome::new(result))
        })))
    }
}

impl Operation for WriteRepositoryReference {
    type Output = SourceMetadata;
    type Error = TransferError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<SourceMetadata>, TransferError> {
        if self.kind != RepositoryConnectorKind::Invenio {
            return Err(not_supported("references"));
        }
        let (group_id, connector_id) = self.connector()?;
        if self.bucket.group_id != group_id {
            return Err(invalid("reference target and connector must share a group"));
        }
        Ok(Next::Call(IoCall::child(ConnectOperation::new(
            ConnectRequest {
                scope: self.scope.clone(),
                auth: self.spec.auth_context.clone(),
                group_id,
                connector_id,
                permission: Permission::READ,
                limit: self.spec.limits.metadata_bytes,
                credential: None,
                stop: Some(self.job.stop.clone()),
            },
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<SourceMetadata>, TransferError> {
        match std::mem::replace(&mut self.step, ReferenceStep::Connect) {
            ReferenceStep::Connect => {
                let session = result.child::<Session, TransferError>()??;
                let (id, name) = self.file()?;
                let url = session.url(&["records", id, "files", name, "content"])?;
                let call = session.head(url);
                self.step = ReferenceStep::Head(session);
                Ok(Next::Call(call))
            }
            ReferenceStep::Head(session) => {
                let metadata = head_metadata(result)?;
                if self.descriptor["file"]["size"].as_u64() != Some(metadata.content_length) {
                    return Err(invalid("repository reference size changed"));
                }
                self.step = ReferenceStep::Checkpoint(session, metadata);
                Ok(Next::Call(remote_call(
                    RemoteCall::Checkpoint,
                    Some(self.job.stop.clone()),
                )))
            }
            ReferenceStep::Checkpoint(session, metadata) => {
                finished(result)?;
                let call = self.write(&session, &metadata)?;
                self.step = ReferenceStep::Write(metadata);
                Ok(Next::Call(call))
            }
            ReferenceStep::Write(metadata) => {
                result
                    .child::<(), String>()?
                    .map_err(TransferError::Retryable)?;
                Ok(Next::Done(metadata))
            }
        }
    }
}

#[cfg(test)]
#[path = "reference_tests.rs"]
mod tests;
