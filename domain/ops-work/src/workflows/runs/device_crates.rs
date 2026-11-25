//! Publishes a workflow run's RO-Crate to the realm and reads local device run crates.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::errors::{AuthorizationError, StorageReadError};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::WORKFLOW_CRATE_KEYSPACE;
use aruna_core::metadata::AuthToken;
use aruna_core::metadata::api::CreateDocumentPayload;
use aruna_core::operation::Operation;
use aruna_core::rows::read_row;
use aruna_core::structs::execution::job::identity::JobId;
use aruna_core::structs::execution::job::input::JobPayload;
use aruna_core::structs::execution::job::local::{JobRecord, OutputObject, RunCrateStatus};
use aruna_core::structs::execution::workflow_run::{RunFile, WorkflowRunRecord, run_key};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::structured_id::{MetaResourceId, StructuredId};
use aruna_core::user::id::UserId;
use aruna_ops_data::metadata::create_document::{
    CreateDocumentConfig, CreateDocumentError, CreateDocumentOperation, CreateDocumentResult,
};
use aruna_ops_data::metadata::create_entry::DocumentMintOperation;
use aruna_ops_data::metadata::forward::route_create::RouteCreateOperation;
use aruna_ops_data::metadata::forward::write::MetadataWriteError;
use aruna_ops_data::metadata::record_reads::DocumentRecordOperation;
use aruna_ops_identity::auth::check_permissions::{
    CheckPermissionsConfig, CheckPermissionsOperation,
};
use serde_json::Value;
use ulid::Ulid;

use super::provenance::{BuildCrateOperation, ROOT, rename, status};
use super::{RunChange, change_call, changed, read_run, run_answer};
use crate::jobs::metadata_class::metadata_is_transient;
use crate::jobs::store::mutation_error::JobMutationError;
use crate::jobs::store::row_calls;
use crate::jobs::workflow::publish_run::{PublishRunError, local_run_crate};
use crate::workflows::CatalogError;

pub(super) fn unavailable(error: impl std::fmt::Display) -> PublishRunError {
    PublishRunError::Unavailable(error.to_string())
}

#[derive(Debug, PartialEq)]
enum CrateWriteStep {
    Registry,
    Allowed,
    Build,
    Create,
    Record(RunCrateStatus),
}

/// Writes one workflow run's RO-Crate under a previously minted document id and records the
/// provenance outcome on the run.
#[derive(Debug, PartialEq)]
pub(super) struct WriteWorkflowCrate {
    record: Box<WorkflowRunRecord>,
    copies: Vec<OutputObject>,
    node_id: NodeId,
    realm_id: RealmId,
    auth: AuthContext,
    doc_id: Ulid,
    step: CrateWriteStep,
}

impl WriteWorkflowCrate {
    pub(super) fn new(
        record: WorkflowRunRecord,
        copies: Vec<OutputObject>,
        (node_id, realm_id): (NodeId, RealmId),
        auth: AuthContext,
        doc_id: Ulid,
    ) -> Self {
        Self {
            record: Box::new(record),
            copies,
            node_id,
            realm_id,
            auth,
            doc_id,
            step: CrateWriteStep::Registry,
        }
    }

    fn actor(&self) -> Actor {
        Actor {
            node_id: self.node_id,
            user_id: self.auth.user_id,
            realm_id: self.realm_id,
        }
    }

    fn document_path(&self) -> String {
        format!("runs/{}", self.record.run_id)
    }

    fn check_permission(&mut self) -> Result<Next<RunCrateStatus>, PublishRunError> {
        let check = CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: self.auth.clone(),
            path: MetadataRegistryRecord::permission_path_for(
                &self.realm_id,
                self.record.group_id,
                &self.document_path(),
                self.doc_id,
            ),
            required_permission: Permission::WRITE,
        });
        self.step = CrateWriteStep::Allowed;
        Ok(Next::Call(IoCall::child(check)))
    }

    fn published_record(&self) -> WorkflowRunRecord {
        let mut published = (*self.record).clone();
        published.outputs = self
            .copies
            .iter()
            .map(|copy| RunFile {
                name: copy.container_path.clone(),
                bucket: Some(copy.bucket.clone()),
                key: Some(copy.key.clone()),
                version_id: Some(copy.version_id.to_string()),
                node_id: Some(copy.node_id),
                size: Some(copy.size),
                digest: copy.digest.clone(),
            })
            .collect();
        published
    }

    fn create_crate(&mut self, mut jsonld: Value) -> Result<Next<RunCrateStatus>, PublishRunError> {
        if let Some(Value::Array(graph)) = jsonld.get_mut("@graph") {
            rename(
                graph,
                ROOT,
                &MetadataRegistryRecord::graph_iri_for(self.doc_id),
            );
        }
        let public = (self.record.output.as_ref())
            .and_then(|output| output.dataset_json.as_deref())
            .and_then(|text| serde_json::from_str::<Value>(text).ok())
            .and_then(|dataset| dataset["public"].as_bool())
            .unwrap_or(false);
        let create = RouteCreateOperation::new(
            CreateDocumentOperation::new_generated_id(CreateDocumentConfig {
                actor: self.actor(),
                group_id: self.record.group_id,
                document_id: self.doc_id,
                document_path: self.document_path(),
                public,
                payload: CreateDocumentPayload::RoCrate {
                    jsonld: jsonld.to_string(),
                },
            }),
            Some(self.node_id),
            Some(AuthToken::internal(self.auth.clone())),
        );
        self.step = CrateWriteStep::Create;
        Ok(Next::Call(IoCall::child(create)))
    }

    fn record_outcome(
        &mut self,
        label: &str,
        doc_id: Option<Ulid>,
        outcome: RunCrateStatus,
    ) -> Result<Next<RunCrateStatus>, PublishRunError> {
        let change = RunChange::Provenance {
            status: label.to_owned(),
            document_id: doc_id,
        };
        self.step = CrateWriteStep::Record(outcome);
        Ok(Next::Call(change_call(self.record.run_id, vec![change])))
    }
}

impl Operation for WriteWorkflowCrate {
    type Output = RunCrateStatus;
    type Error = PublishRunError;

    fn first(&mut self) -> Result<Next<RunCrateStatus>, PublishRunError> {
        Ok(Next::Call(IoCall::child(DocumentRecordOperation::new(
            self.doc_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<RunCrateStatus>, PublishRunError> {
        match std::mem::replace(&mut self.step, CrateWriteStep::Registry) {
            CrateWriteStep::Registry => {
                let found = result
                    .child::<Option<MetadataRegistryRecord>, StorageReadError>()?
                    .map_err(unavailable)?;
                match found {
                    Some(_) => self.record_outcome(
                        status::WRITTEN,
                        Some(self.doc_id),
                        RunCrateStatus::Written {
                            resource: self.doc_id.to_string(),
                        },
                    ),
                    None => self.check_permission(),
                }
            }
            CrateWriteStep::Allowed => match result.child::<bool, AuthorizationError>()? {
                Ok(true) => {
                    let published = self.published_record();
                    self.step = CrateWriteStep::Build;
                    Ok(Next::Call(IoCall::child(BuildCrateOperation::new(
                        published,
                    ))))
                }
                Ok(false)
                | Err(
                    AuthorizationError::InvalidRealmId
                    | AuthorizationError::InvalidGroupId
                    | AuthorizationError::GroupNotFound
                    | AuthorizationError::DocNotFound,
                ) => self.record_outcome(
                    status::DENIED,
                    None,
                    RunCrateStatus::Denied {
                        message: "metadata write access denied".to_string(),
                    },
                ),
                Err(error) => Err(PublishRunError::Crate(error.to_string())),
            },
            CrateWriteStep::Build => {
                let jsonld = result
                    .child::<Value, CatalogError>()?
                    .map_err(|error| PublishRunError::Crate(error.to_string()))?;
                self.create_crate(jsonld)
            }
            CrateWriteStep::Create => {
                match result.child::<CreateDocumentResult, MetadataWriteError>()? {
                    Ok(created) => {
                        let doc_id = created.record.document_id;
                        self.record_outcome(
                            status::WRITTEN,
                            Some(doc_id),
                            RunCrateStatus::Written {
                                resource: doc_id.to_string(),
                            },
                        )
                    }
                    Err(error) if metadata_is_transient(&error) => {
                        Err(PublishRunError::Crate(error.to_string()))
                    }
                    Err(MetadataWriteError::Forbidden | MetadataWriteError::Unauthorized) => self
                        .record_outcome(
                            status::DENIED,
                            None,
                            RunCrateStatus::Denied {
                                message: "metadata write access denied".to_string(),
                            },
                        ),
                    Err(error) => self.record_outcome(
                        status::FAILED,
                        None,
                        RunCrateStatus::Failed {
                            message: error.to_string(),
                        },
                    ),
                }
            }
            CrateWriteStep::Record(outcome) => {
                changed(result).map_err(unavailable)?;
                Ok(Next::Done(outcome))
            }
        }
    }
}

pub(super) fn mint_crate_call(actor: Actor, record: &WorkflowRunRecord) -> IoCall {
    IoCall::child(DocumentMintOperation::job(
        actor,
        record.group_id,
        format!("runs/{}", record.run_id),
    ))
}

pub(super) fn minted_doc_id(result: IoResult) -> Result<Ulid, PublishRunError> {
    let id = result
        .child::<MetaResourceId, CreateDocumentError>()?
        .map_err(|error| PublishRunError::Crate(error.to_string()))?;
    Ok(id.as_ulid())
}

pub(super) fn workflow_outputs(
    record: &WorkflowRunRecord,
) -> Result<Vec<OutputObject>, PublishRunError> {
    record
        .outputs
        .iter()
        .map(|file| {
            let bucket = file
                .bucket
                .clone()
                .ok_or_else(|| unavailable("workflow output is missing its bucket"))?;
            let key = file
                .key
                .clone()
                .ok_or_else(|| unavailable("workflow output is missing its key"))?;
            let version_id = file
                .version_id
                .as_deref()
                .and_then(|value| value.parse::<Ulid>().ok())
                .ok_or_else(|| unavailable("workflow output is missing its version"))?;
            Ok(OutputObject {
                node_id: file.node_id.unwrap_or(record.node_id),
                bucket,
                key,
                version_id,
                execution_id: record.run_id,
                container_path: file.name.clone(),
                size: file.size.unwrap_or_default(),
                digest: file.digest.clone(),
            })
        })
        .collect()
}

#[derive(Debug, PartialEq)]
enum CrateStep {
    Execution(JobId),
    Job,
    Workflow,
    Stored,
}

/// Reads the local RO-Crate for one of the owner's local execution or workflow runs.
pub fn local_device_crate(id: Ulid, owner: UserId) -> DeviceCrateOperation {
    DeviceCrateOperation {
        id,
        owner,
        step: CrateStep::Stored,
    }
}

#[derive(Debug, PartialEq)]
pub struct DeviceCrateOperation {
    id: Ulid,
    owner: UserId,
    step: CrateStep,
}

impl Operation for DeviceCrateOperation {
    type Output = Value;
    type Error = PublishRunError;

    fn first(&mut self) -> Result<Next<Value>, PublishRunError> {
        match JobId::try_from_bytes(self.id.to_bytes()) {
            Ok(job_id) => {
                self.step = CrateStep::Execution(job_id);
                Ok(Next::Call(IoCall::child(local_run_crate(
                    job_id, self.owner,
                ))))
            }
            Err(_) => {
                self.step = CrateStep::Workflow;
                Ok(Next::Call(read_run(self.id)))
            }
        }
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Value>, PublishRunError> {
        match std::mem::replace(&mut self.step, CrateStep::Stored) {
            CrateStep::Execution(job_id) => match result.child::<Value, PublishRunError>()? {
                Ok(jsonld) => Ok(Next::Done(jsonld)),
                Err(PublishRunError::NotFound) => {
                    self.step = CrateStep::Job;
                    Ok(Next::Call(IoCall::child(row_calls::read_job_record(
                        job_id,
                    ))))
                }
                Err(error) => Err(error),
            },
            CrateStep::Job => {
                let job = result
                    .child::<Option<JobRecord>, JobMutationError>()?
                    .map_err(unavailable)?;
                let run_id = match job {
                    Some(JobRecord {
                        payload: JobPayload::WorkflowRun(spec),
                        ..
                    }) => spec.run_id,
                    None => self.id,
                    Some(_) => return Err(PublishRunError::NotFound),
                };
                self.step = CrateStep::Workflow;
                Ok(Next::Call(read_run(run_id)))
            }
            CrateStep::Workflow => {
                let record = run_answer(result)
                    .map_err(unavailable)?
                    .ok_or(PublishRunError::NotFound)?;
                if !record.local_only || record.created_by != self.owner {
                    return Err(PublishRunError::NotFound);
                }
                self.step = CrateStep::Stored;
                Ok(Next::Call(read_row(
                    WORKFLOW_CRATE_KEYSPACE,
                    run_key(record.run_id),
                )))
            }
            CrateStep::Stored => {
                let bytes = result.value()?.ok_or(PublishRunError::NotFound)?;
                let jsonld = serde_json::from_slice(&bytes).map_err(unavailable)?;
                Ok(Next::Done(jsonld))
            }
        }
    }
}
