//! Writes the RO-Crate document that records a finished execution job.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use aruna_core::errors::AuthorizationError;
use aruna_core::id::NodeId;
use aruna_core::metadata::CRATE_PROFILE_IRI;
use aruna_core::structs::ORIGIN_DEVICE_TAG;
use aruna_core::structs::execution::job::{
    CapturedInput, ExecutionSpec, InputContent, InputSource, JobError, JobId, JobPayload,
    JobRecord, JobResultPayload, JobState, LaunchRecord, LocalCrate, RunCrateStatus, RunLink,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::key_content_type;
use aruna_core::structs::storage::data::identity::{
    CONTENT_URL, DataEntity, ObjectLocation, content_id, normalized_id, parse_hash,
};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_core::{MetaResourceId, StructuredId};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};

use crate::metadata::provenance::{RELEASE_INCOMPLETE, RELEASE_MEMBERSHIP};
use serde_json::json;
use ulid::Ulid;

/// Process Run Crate profile identifiers (0.5).
const CRATE_CONTEXT: &str = "https://w3id.org/ro/crate/1.2/context";
const WORKFLOW_RUN_CONTEXT: &str = "https://w3id.org/ro/terms/workflow-run/context";
const CRATE_PROFILE: &str = "https://w3id.org/ro/crate/1.2";
const TERMS: &str = "https://w3id.org/aruna/terms/";
const WORKSPACE_PROPERTY: &str = "https://w3id.org/aruna/terms/workspace-bucket";
const LINK_PROPERTY: &str = "https://w3id.org/aruna/terms/link";

use super::super::executor::JobRunOutcome;
use super::super::records::pages::ServingNode;
use super::super::store::JobMutationError;
use super::super::store::row_calls::{
    put_crate_outcome, put_crate_status, put_local_crate, read_crate_status, read_job_record,
    read_launch_record, read_run_link,
};
use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::forward::transport::MetadataWriteError;
use crate::metadata::create_document::{
    CreateDocumentConfig, CreateDocumentError, CreateDocumentOperation, CreateDocumentPayload,
    CreateDocumentResult,
};
use crate::metadata::create_entry::DocumentMintOperation;
use crate::metadata::forward::route_create::RouteCreateOperation;
use crate::metadata::record_reads::DocumentRecordOperation;
use crate::notifications::watch::origin::EmitEventOperation;
use crate::realm::mutate_placement::node_kind;
use crate::replication::locations::{LocationSummaryError, RemoteLocationOperation};
use crate::replication::protocol::{LocationSummary, LocationSummaryRequest};
use crate::s3::object::head::{
    HeadObjectError, HeadObjectInput, HeadObjectOperation, HeadObjectResult,
};
use crate::storage_read::StorageReadError;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use tracing::warn;

/// How a run-crate job ended.
#[derive(Debug, PartialEq)]
pub enum CrateRun {
    /// The outcome is on record, under this resource name.
    Recorded(String),
    /// A dependency is not there yet; the job runs again without spending an attempt.
    NotReady(JobError),
}

/// How the job runtime takes the end of a run-crate job.
pub fn crate_outcome(result: Result<CrateRun, JobError>) -> JobRunOutcome {
    match result {
        Ok(CrateRun::Recorded(resource)) => {
            JobRunOutcome::Succeeded(JobResultPayload::RunCrate { resource })
        }
        Ok(CrateRun::NotReady(error)) => JobRunOutcome::Deferred(error),
        Err(error) => JobRunOutcome::Failed(error),
    }
}

fn retry(context: &str, error: impl std::fmt::Display) -> JobError {
    JobError::retryable(format!("{context}: {error}"))
}

fn is_transient(error: &MetadataWriteError) -> bool {
    crate::jobs::metadata_class::metadata_is_transient(error)
}

#[derive(Debug, PartialEq)]
enum WriteStep {
    Status,
    Parent {
        saved_id: Option<Ulid>,
    },
    Config {
        saved_id: Option<Ulid>,
        parent: Box<JobRecord>,
    },
    Launch {
        parent: Box<JobRecord>,
    },
    Local,
    Record,
}

/// Runs the follow-on run-crate obligation of a finished execution job. A failure records a
/// durable status and never fails the parent; this job succeeds once the outcome is recorded,
/// so it is not retried forever on a permanent error.
#[derive(Debug, PartialEq)]
pub struct WriteCrateOperation {
    for_job: JobId,
    serving: ServingNode,
    step: WriteStep,
}

type WriteNext = Result<Next<CrateRun>, JobError>;

fn recorded(resource: impl Into<String>) -> WriteNext {
    Ok(Next::Done(CrateRun::Recorded(resource.into())))
}

fn execution_spec(record: &JobRecord) -> Option<&ExecutionSpec> {
    match &record.payload {
        JobPayload::Execution(spec) => Some(spec),
        _ => None,
    }
}

impl WriteCrateOperation {
    pub fn new(for_job: JobId, serving: ServingNode) -> Self {
        Self {
            for_job,
            serving,
            step: WriteStep::Status,
        }
    }

    fn call(&mut self, step: WriteStep, call: IoCall) -> WriteNext {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn record(&mut self, parent: &JobRecord, saved_id: Option<Ulid>) -> WriteNext {
        let Some(spec) = execution_spec(parent) else {
            return recorded("not-execution");
        };
        let record = RecordCrateOperation::new(self.for_job, parent, spec, saved_id, self.serving);
        self.call(WriteStep::Record, IoCall::child(record))
    }

    /// A device run stays local: its crate is written when the owner publishes it. A node the
    /// stored realm configuration does not name is undecided, so no crate leaves it yet.
    fn configured(
        &mut self,
        config: Result<RealmConfigDocument, GetConfigError>,
        saved_id: Option<Ulid>,
        parent: Box<JobRecord>,
    ) -> WriteNext {
        let config = match config {
            Ok(config) => config,
            Err(error) => {
                let reason = format!("run crate needs the realm configuration: {error}");
                return Ok(Next::Done(CrateRun::NotReady(JobError::retryable(reason))));
            }
        };
        let node = self.serving.map(|(node, _)| node);
        let Some(kind) = node.and_then(|node| node_kind(&config, node)) else {
            let reason = "the realm configuration does not name this node yet";
            return Ok(Next::Done(CrateRun::NotReady(JobError::retryable(reason))));
        };
        if kind.owner().is_none() {
            return self.record(&parent, saved_id);
        }
        if saved_id.is_some() {
            return recorded(RunCrateStatus::Local.name());
        }
        let launch = read_launch_record(&parent);
        self.call(WriteStep::Launch { parent }, IoCall::child(launch))
    }

    /// Keeps the crate of a device run here with the `Local` status; it names this device's own
    /// versions and leaves the device only through publication.
    fn keep_local(&mut self, parent: &JobRecord, launch: Option<LaunchRecord>) -> WriteNext {
        let Some(spec) = execution_spec(parent) else {
            return recorded("not-execution");
        };
        let realm_id = self
            .serving
            .map_or(parent.created_by.realm_id, |(_, realm_id)| realm_id);
        let jsonld = build_crate_jsonld(
            parent,
            spec,
            (realm_id, None),
            &BTreeMap::new(),
            launch.as_ref(),
        );
        let local = LocalCrate {
            owner: parent.created_by,
            jsonld,
        };
        let put = put_local_crate(parent.job_id, &local);
        self.call(WriteStep::Local, IoCall::child(put))
    }
}

impl Operation for WriteCrateOperation {
    type Output = CrateRun;
    type Error = JobError;

    fn first(&mut self) -> WriteNext {
        // A re-driven crate job returns a recorded outcome without recreating it.
        Ok(Next::Call(IoCall::child(read_crate_status(self.for_job))))
    }

    fn next(&mut self, result: IoResult) -> WriteNext {
        const LOCAL_FAILED: &str = "local run crate write failed";
        match std::mem::replace(&mut self.step, WriteStep::Record) {
            WriteStep::Status => {
                let status = result
                    .child::<Option<RunCrateStatus>, JobMutationError>()?
                    .map_err(|error| retry("run crate read status failed", error))?;
                let saved_id = match status {
                    Some(RunCrateStatus::Written { resource }) => return recorded(resource),
                    Some(RunCrateStatus::Pending) | None => None,
                    Some(RunCrateStatus::Minted { document_id }) => Some(document_id),
                    Some(status) => return recorded(status.name()),
                };
                let read = read_job_record(self.for_job);
                self.call(WriteStep::Parent { saved_id }, IoCall::child(read))
            }
            WriteStep::Parent { saved_id } => {
                let parent = result
                    .child::<Option<JobRecord>, JobMutationError>()?
                    .map_err(|error| retry("run crate read parent failed", error))?;
                // A parent pruned before the crate ran leaves nothing to write.
                let Some(parent) = parent else {
                    return recorded("parent-gone");
                };
                if execution_spec(&parent).is_none() {
                    return recorded("not-execution");
                }
                // Without a net handle nothing can be forwarded; the write fails retryably.
                let Some((_, realm_id)) = self.serving else {
                    return self.record(&parent, saved_id);
                };
                let parent = Box::new(parent);
                let step = WriteStep::Config { saved_id, parent };
                self.call(step, IoCall::child(GetConfigOperation::new(realm_id)))
            }
            WriteStep::Config { saved_id, parent } => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                self.configured(config, saved_id, parent)
            }
            WriteStep::Launch { parent } => {
                let launch = result
                    .child::<Option<LaunchRecord>, JobMutationError>()?
                    .map_err(|error| retry(LOCAL_FAILED, error))?;
                self.keep_local(&parent, launch)
            }
            WriteStep::Local => {
                result
                    .child::<(), JobMutationError>()?
                    .map_err(|error| retry(LOCAL_FAILED, error))?;
                recorded(RunCrateStatus::Local.name())
            }
            WriteStep::Record => {
                let resource = result.child::<String, JobError>()??;
                recorded(resource)
            }
        }
    }
}

/// One input whose provenance the crate records.
#[derive(Debug, PartialEq)]
struct CrateInput {
    dest_key: String,
    bucket: String,
    key: String,
    captured: CapturedInput,
}

#[derive(Debug, PartialEq)]
enum RecordStep {
    Mint,
    PutMinted,
    Registry,
    Allowed,
    Device(String),
    Launch,
    Create,
    Emitted {
        status: RunCrateStatus,
        document_id: Ulid,
    },
    Link {
        status: RunCrateStatus,
        run_crate: Option<Ulid>,
    },
    Outcome(String),
}

type RecordNext = Result<Next<String>, JobError>;

/// Mints, authorizes and writes the crate of one finished execution, then records the outcome.
/// The output names the recorded resource.
#[derive(Debug, PartialEq)]
pub struct RecordCrateOperation {
    for_job: JobId,
    parent: Box<JobRecord>,
    spec: Box<ExecutionSpec>,
    saved_id: Option<Ulid>,
    serving: ServingNode,
    document_id: Ulid,
    inputs: VecDeque<CrateInput>,
    devices: BTreeMap<String, String>,
    /// The version a remote holder is asked about; `None` while a local head runs.
    expect_remote: Option<Ulid>,
    step: RecordStep,
}

impl RecordCrateOperation {
    pub fn new(
        for_job: JobId,
        parent: &JobRecord,
        spec: &ExecutionSpec,
        saved_id: Option<Ulid>,
        serving: ServingNode,
    ) -> Self {
        let inputs = (spec.inputs.iter())
            .filter_map(|input| {
                let InputSource::S3 { bucket, key, .. } = &input.source;
                let captured = (parent.captured_inputs.iter())
                    .find(|captured| captured.destination_key == input.dest_key)?;
                Some(CrateInput {
                    dest_key: input.dest_key.clone(),
                    bucket: bucket.clone(),
                    key: key.clone(),
                    captured: captured.clone(),
                })
            })
            .collect();
        Self {
            for_job,
            parent: Box::new(parent.clone()),
            spec: Box::new(spec.clone()),
            saved_id,
            serving,
            document_id: saved_id.unwrap_or_else(Ulid::nil),
            inputs,
            devices: BTreeMap::new(),
            expect_remote: None,
            step: RecordStep::Mint,
        }
    }

    fn place(&self) -> Result<(NodeId, RealmId), JobError> {
        self.serving
            .ok_or_else(|| JobError::retryable("run crate needs a net handle"))
    }

    fn document_path(&self) -> String {
        format!("runs/{}", self.for_job)
    }

    fn auth(&self, realm_id: RealmId) -> AuthContext {
        AuthContext {
            user_id: self.parent.created_by,
            realm_id,
            path_restrictions: None,
            session: None,
        }
    }

    fn actor(&self) -> Result<Actor, JobError> {
        let (node_id, realm_id) = self.place()?;
        Ok(Actor {
            node_id,
            user_id: self.parent.created_by,
            realm_id,
        })
    }

    fn call(&mut self, step: RecordStep, call: IoCall) -> RecordNext {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn check(&mut self) -> RecordNext {
        let (_, realm_id) = self.place()?;
        let check = CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: self.auth(realm_id),
            path: MetadataRegistryRecord::permission_path_for(
                &realm_id,
                self.spec.group_id,
                &self.document_path(),
                self.document_id,
            ),
            required_permission: Permission::WRITE,
        });
        self.call(RecordStep::Allowed, IoCall::child(check))
    }

    /// Asks where the next input came from. A version on another node is asked of that node as
    /// the job owner, under the owner's own READ.
    fn next_device(&mut self) -> RecordNext {
        let (node_id, realm_id) = self.place()?;
        let Some(input) = self.inputs.pop_front() else {
            let launch = read_launch_record(&self.parent);
            return self.call(RecordStep::Launch, IoCall::child(launch));
        };
        let version_id = input.captured.version_id;
        let call = match input.captured.source_node_id == node_id {
            true => IoCall::child(HeadObjectOperation::new(HeadObjectInput {
                bucket: input.bucket,
                key: input.key,
                version_id: Some(version_id),
            })),
            false => IoCall::child(RemoteLocationOperation::new(
                input.captured.source_node_id,
                LocationSummaryRequest {
                    realm_id,
                    bucket: input.bucket,
                    key: input.key,
                    version_id: Some(version_id),
                    auth_context: self.auth(realm_id),
                },
            )),
        };
        let remote = input.captured.source_node_id != node_id;
        self.step = RecordStep::Device(input.dest_key);
        self.expect_remote = remote.then_some(version_id);
        Ok(Next::Call(call))
    }

    /// The device one input version was published from. A version that is gone or not readable
    /// names no device; any other read failure retries the crate.
    fn device_of(&self, result: IoResult) -> Result<Option<String>, JobError> {
        const FAILED: &str = "run crate input provenance read failed";
        let Some(version_id) = self.expect_remote else {
            return match result.child::<HeadObjectResult, HeadObjectError>()? {
                Ok(head) => Ok(head.metadata.get(ORIGIN_DEVICE_TAG).cloned()),
                Err(
                    HeadObjectError::NoSuchKey
                    | HeadObjectError::NoSuchVersion
                    | HeadObjectError::DeleteMarker,
                ) => Ok(None),
                Err(error) => Err(retry(FAILED, error)),
            };
        };
        match result.child::<LocationSummary, LocationSummaryError>()? {
            Ok(summary) if summary.version_id == Some(version_id) => Ok(summary.origin_device),
            Ok(_) | Err(LocationSummaryError::Denied | LocationSummaryError::BucketNotFound) => {
                Ok(None)
            }
            Err(error) => Err(retry(FAILED, error)),
        }
    }

    fn create(&mut self, launch: Option<LaunchRecord>) -> RecordNext {
        let (node_id, realm_id) = self.place()?;
        let jsonld = build_crate_jsonld(
            &self.parent,
            &self.spec,
            (realm_id, Some(self.document_id)),
            &self.devices,
            launch.as_ref(),
        );
        let create = RouteCreateOperation::new(
            CreateDocumentOperation::new_generated_id(CreateDocumentConfig {
                actor: self.actor()?,
                group_id: self.spec.group_id,
                document_id: self.document_id,
                document_path: self.document_path(),
                public: false,
                payload: CreateDocumentPayload::RoCrate { jsonld },
            }),
            Some(node_id),
            Some(AuthToken::internal(self.auth(realm_id))),
        );
        self.call(RecordStep::Create, IoCall::child(create))
    }

    /// Announces the created crate document, then records it as the run's written crate.
    fn written(&mut self, record: MetadataRegistryRecord, event_id: Ulid) -> RecordNext {
        let (_, realm_id) = self.place()?;
        let emit = EmitEventOperation::metadata_created(
            realm_id,
            self.parent.created_by,
            record.group_id,
            record.document_id,
            &record.document_path,
            event_id,
        );
        let step = RecordStep::Emitted {
            status: RunCrateStatus::Written {
                resource: record.document_id.to_string(),
            },
            document_id: record.document_id,
        };
        self.call(step, IoCall::child(emit))
    }

    /// Records the final status with the run link; a link the receipt wrote keeps its family.
    fn finish(&mut self, status: RunCrateStatus, run_crate: Option<Ulid>) -> RecordNext {
        let read = read_run_link(self.for_job);
        let step = RecordStep::Link { status, run_crate };
        self.call(step, IoCall::child(read))
    }

    fn put_outcome(
        &mut self,
        known: Option<RunLink>,
        status: RunCrateStatus,
        run_crate: Option<Ulid>,
    ) -> RecordNext {
        let link = RunLink {
            family: known.as_ref().and_then(|link| link.family),
            logical_job_id: known.and_then(|link| link.logical_job_id),
            group_id: self.spec.group_id,
            created_by: self.parent.created_by,
            run_crate,
        };
        let resource = match &status {
            RunCrateStatus::Written { resource } => resource.clone(),
            other => other.name().to_string(),
        };
        let put = put_crate_outcome(self.for_job, &status, &link);
        self.call(RecordStep::Outcome(resource), IoCall::child(put))
    }
}

impl Operation for RecordCrateOperation {
    type Output = String;
    type Error = JobError;

    fn first(&mut self) -> RecordNext {
        let actor = self.actor()?;
        // A re-driven job whose create already committed records it instead of importing again.
        if let Some(document_id) = self.saved_id {
            let read = DocumentRecordOperation::new(document_id);
            return self.call(RecordStep::Registry, IoCall::child(read));
        }
        let mint = DocumentMintOperation::job(actor, self.spec.group_id, self.document_path());
        self.call(RecordStep::Mint, IoCall::child(mint))
    }

    fn next(&mut self, result: IoResult) -> RecordNext {
        const STATUS_FAILED: &str = "run crate status write failed";
        match std::mem::replace(&mut self.step, RecordStep::Mint) {
            RecordStep::Mint => {
                let id = result
                    .child::<MetaResourceId, CreateDocumentError>()?
                    .map_err(|error| retry("run crate mint document id failed", error))?;
                self.document_id = id.as_ulid();
                // The first minted id is kept before the create, so retries survive policy
                // changes.
                let status = RunCrateStatus::Minted {
                    document_id: self.document_id,
                };
                let put = put_crate_status(self.for_job, &status);
                self.call(RecordStep::PutMinted, IoCall::child(put))
            }
            RecordStep::PutMinted => {
                result
                    .child::<(), JobMutationError>()?
                    .map_err(|error| retry(STATUS_FAILED, error))?;
                self.check()
            }
            RecordStep::Registry => {
                let record = result
                    .child::<Option<MetadataRegistryRecord>, StorageReadError>()?
                    .map_err(|error| retry("run crate registry read failed", error))?;
                match record {
                    Some(record) => {
                        let event_id = record.establishing_event_id;
                        self.written(record, event_id)
                    }
                    None => self.check(),
                }
            }
            RecordStep::Allowed => {
                let denied = match result.child::<bool, AuthorizationError>()? {
                    Ok(allowed) => !allowed,
                    Err(
                        AuthorizationError::InvalidRealmId
                        | AuthorizationError::InvalidGroupId
                        | AuthorizationError::GroupNotFound
                        | AuthorizationError::DocNotFound,
                    ) => true,
                    Err(error) => return Err(retry("run crate authorization failed", error)),
                };
                if denied {
                    let status = RunCrateStatus::Denied {
                        message: "metadata write access denied".to_string(),
                    };
                    return self.finish(status, None);
                }
                self.next_device()
            }
            RecordStep::Device(dest_key) => {
                if let Some(device) = self.device_of(result)? {
                    self.devices.insert(dest_key, device);
                }
                self.next_device()
            }
            RecordStep::Launch => {
                let launch = result
                    .child::<Option<LaunchRecord>, JobMutationError>()?
                    .map_err(|error| retry("run crate launch record read failed", error))?;
                self.create(launch)
            }
            RecordStep::Create => match result
                .child::<CreateDocumentResult, MetadataWriteError>()?
            {
                Ok(created) => self.written(created.record, created.event_id),
                // Transient errors retry. An invalid crate is permanent: it is recorded and
                // the obligation discharged, so it is not retried forever.
                Err(error) if is_transient(&error) => Err(retry("run crate write failed", error)),
                Err(error) => {
                    let status = RunCrateStatus::Failed {
                        message: error.to_string(),
                    };
                    self.finish(status, None)
                }
            },
            RecordStep::Emitted {
                status,
                document_id,
            } => {
                if let Err(error) = result.child::<(), IoError>()? {
                    warn!(job_id = %self.for_job, %error, "Run crate watch event failed");
                }
                self.finish(status, Some(document_id))
            }
            RecordStep::Link { status, run_crate } => {
                let known = result
                    .child::<Option<RunLink>, JobMutationError>()?
                    .map_err(|error| retry(STATUS_FAILED, error))?;
                self.put_outcome(known, status, run_crate)
            }
            RecordStep::Outcome(resource) => {
                result
                    .child::<(), JobMutationError>()?
                    .map_err(|error| retry(STATUS_FAILED, error))?;
                Ok(Next::Done(resource))
            }
        }
    }
}

/// The contextual entity naming the node and site an execution ran on.
const SITE_ID: &str = "#execution-site";

fn env_entity(name: &str) -> String {
    format!("#env-{}", utf8_percent_encode(name, NON_ALPHANUMERIC))
}

/// Granted limits and the working directory as PropertyValues; unbounded dimensions are omitted.
fn launch_properties(launch: &LaunchRecord) -> Vec<serde_json::Value> {
    let resources = &launch.resources;
    let cores = resources
        .nano_cpus
        .map(|nanos| (nanos as f64 / 1e9).to_string());
    let values = [
        ("cpu-cores", "CPU cores", cores),
        (
            "ram-bytes",
            "Memory bytes",
            resources.ram_bytes.map(|v| v.to_string()),
        ),
        (
            "disk-bytes",
            "Disk bytes",
            resources.disk_bytes.map(|v| v.to_string()),
        ),
        (
            "workspace-bytes",
            "Workspace bytes",
            resources.workspace_bytes.map(|v| v.to_string()),
        ),
        (
            "walltime-ms",
            "Walltime milliseconds",
            resources.walltime_ms.map(|v| v.to_string()),
        ),
        (
            "working-directory",
            "Working directory",
            launch.workdir.clone(),
        ),
    ];
    values
        .into_iter()
        .filter_map(|(term, name, value)| {
            value.map(|value| {
                json!({
                    "@id": format!("#{term}"),
                    "@type": "PropertyValue",
                    "name": name,
                    "propertyID": format!("{TERMS}{term}"),
                    "value": value
                })
            })
        })
        .collect()
}

/// The contextual entity id of one device an input came from.
fn group_entity(group_id: Ulid) -> String {
    format!("#group-{group_id}")
}

/// What admission captured about an input beyond its bytes, as entities next to the file `id`:
/// a public source's pin, the bucket's metadata then, the releases that pin the version, and
/// whether that membership was checked everywhere.
fn capture_entities(
    id: &str,
    captured: &CapturedInput,
    file: &mut serde_json::Value,
) -> Vec<serde_json::Value> {
    let mut entities = Vec::new();
    let mut properties: Vec<serde_json::Value> = match file["additionalProperty"].take() {
        serde_json::Value::Null => Vec::new(),
        serde_json::Value::Array(values) => values,
        value => vec![value],
    };
    let mut property = |term: &str, name: &str, value: String| {
        let property_id = format!("{id}#{term}");
        properties.push(json!({ "@id": property_id.clone() }));
        json!({ "@id": property_id, "@type": "PropertyValue", "name": name, "value": value })
    };
    // A private source stays unnamed, its path included.
    if let Some(pin) = (captured.source_pin.as_ref()).filter(|pin| pin.url.is_some()) {
        let etag = match &captured.content {
            InputContent::Reference { etag, .. } => etag.clone(),
            InputContent::Blake3(_) => None,
        };
        let pinned = [
            ("source-path", "Source path", Some(pin.path.clone())),
            (
                "source-version",
                "Source version",
                pin.source_version.clone(),
            ),
            ("source-etag", "Source ETag", etag),
        ];
        for (term, name, value) in pinned {
            entities.extend(value.map(|value| property(term, name, value)));
        }
    }
    // The one of the version's equal sources that delivered the bytes, when anyone may read it.
    if let Some(url) = (captured.served_by.as_ref()).and_then(|pin| pin.url.clone()) {
        entities.push(property("source-served-by", "Served by", url));
    }
    if !captured.releases_complete {
        let mut marker = property(
            "release-membership",
            "Release membership",
            "incomplete".into(),
        );
        marker["propertyID"] = json!(RELEASE_MEMBERSHIP);
        marker["description"] = json!(RELEASE_INCOMPLETE);
        entities.push(marker);
    }
    let mut part_of = Vec::new();
    if let Some(metadata) = &captured.database {
        let described = &metadata.described;
        // Each captured description is its own snapshot, based on the dataset it was read from.
        let snapshot = serde_json::to_value(metadata).unwrap_or_default();
        let digest = blake3::hash(snapshot.to_string().as_bytes()).to_hex();
        let database_id = format!("#bucket-metadata-{}", &digest[..16]);
        let title = described.title.as_deref().unwrap_or("Bucket metadata");
        let mut database = json!({
            "@id": database_id.clone(),
            "@type": "Dataset",
            "name": format!("{title} (at admission)")
        });
        for (name, value) in [
            ("version", &described.version),
            ("license", &described.license),
            ("description", &described.description),
        ] {
            if let Some(value) = value {
                database[name] = json!(value);
            }
        }
        let links: Vec<_> = (described.identifiers.iter())
            .map(|identifier| json!(identifier.url))
            .collect();
        if !links.is_empty() {
            database["identifier"] = json!(links);
        }
        if let Some(dataset) = metadata.dataset {
            database["isBasedOn"] =
                json!({ "@id": MetadataRegistryRecord::graph_iri_for(dataset) });
        }
        part_of.push(json!({ "@id": database_id }));
        entities.push(database);
    }
    for release in &captured.releases {
        let release_id = release.crate_version.to_id();
        let upstream: Vec<_> = (release.upstream.iter())
            .map(|identifier| json!({ "@id": identifier.url }))
            .collect();
        let mut entity = json!({
            "@id": release_id.clone(),
            "@type": "Dataset",
            "name": format!("Release {}", release.label),
            "version": release.label
        });
        if !upstream.is_empty() {
            entity["isBasedOn"] = json!(upstream);
        }
        part_of.push(json!({ "@id": release_id }));
        entities.push(entity);
    }
    if !part_of.is_empty() {
        file["isPartOf"] = json!(part_of);
    }
    match properties.len() {
        0 => {}
        1 => file["additionalProperty"] = properties.remove(0),
        _ => file["additionalProperty"] = json!(properties),
    }
    entities
}

fn device_entity(device: &str) -> String {
    format!("#device-{device}")
}

/// RO-Crate 1.2 JSON-LD conforming to the Process Run Crate 0.5 profile. No
/// secrets are included (spec 16.10).
fn build_crate_jsonld(
    record: &JobRecord,
    spec: &ExecutionSpec,
    (realm_id, document_id): (RealmId, Option<Ulid>),
    devices: &BTreeMap<String, String>,
    launch: Option<&LaunchRecord>,
) -> String {
    // A local crate has no realm document yet, so its root is the crate folder itself.
    let root = document_id.map_or_else(
        || "./".to_string(),
        |document_id| format!("https://w3id.org/aruna/{document_id}"),
    );
    let action_id = format!("#run-{}", record.job_id);
    let agent_id = format!("#agent-{}", record.created_by);
    let software_id = format!("#software-{}", record.job_id);
    let container_id = format!("#container-{}", record.job_id);
    let image = record
        .attempt_intent
        .as_ref()
        .map(|intent| intent.pinned_image.as_str())
        .unwrap_or(spec.image.as_str());
    let (repository, pinned_tag, digest) = split_image(image);
    let (_, submitted_tag, _) = split_image(&spec.image);
    let (registry, image_name) = split_registry(&repository);
    let application_name = image_name.rsplit('/').next().unwrap_or(image_name.as_str());
    let run_name = spec
        .name
        .as_deref()
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| format!("Run {application_name}"));
    let run_description = spec
        .description
        .as_deref()
        .map(str::trim)
        .filter(|description| !description.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| format!("Container execution using {application_name}"));

    let (exit_code, result_workspace, outputs) = match &record.result {
        Some(JobResultPayload::Execution {
            exit_code,
            workspace_bucket,
            outputs,
            ..
        }) => (*exit_code, workspace_bucket.clone(), outputs.clone()),
        _ => (None, None, Vec::new()),
    };
    let workspace = record.workspace_bucket.clone().or(result_workspace);
    let action_status = if record.state == JobState::Succeeded {
        "http://schema.org/CompletedActionStatus"
    } else {
        "http://schema.org/FailedActionStatus"
    };
    let command = command_line(spec);

    // Inputs are the action `object` and outputs its `result`, named by content where known.
    let mut used = BTreeSet::new();
    // The exact version id joins the location as a second `contentUrl`, so readers can follow it.
    let exact = |node_id, location: &ObjectLocation, version_id| {
        VersionedObjectArn::new(
            realm_id,
            node_id,
            &location.bucket,
            &location.key,
            version_id,
        )
        .ok()
        .map(|arn| arn.to_w3id())
    };
    let mut entity = |location: ObjectLocation,
                      hash: Option<[u8; 32]>,
                      (version, by_version): (Option<String>, bool),
                      file: serde_json::Value| {
        // Without a hash, or when another input has the same bytes, a captured input's exact
        // version names it, so two inputs never share an id or fall back to a bucket path.
        let id = match (hash, &version) {
            (Some(hash), Some(version))
                if by_version
                    && used.contains(&content_id(hash))
                    && used.insert(version.clone()) =>
            {
                version.clone()
            }
            (Some(hash), _) => normalized_id(hash, &location, &mut used),
            (None, Some(version)) if by_version => version.clone(),
            _ => location.to_url(),
        };
        let mut file = file.as_object().cloned().unwrap_or_default();
        DataEntity {
            id: id.clone(),
            location,
            local_path: None,
            size: None,
            encoding_format: None,
        }
        .apply(&mut file);
        if let Some(version) = version
            && let Some(url) = file.remove(CONTENT_URL)
        {
            file.insert(CONTENT_URL.into(), json!([url, version]));
        }
        (id, serde_json::Value::Object(file))
    };
    let mut used_devices = BTreeSet::new();
    let mut source_groups = BTreeSet::new();
    let mut link_properties = Vec::new();
    let mut context_entities = BTreeMap::new();
    let mut object_ids = Vec::with_capacity(spec.inputs.len());
    let mut part_ids = Vec::new();
    let mut files = Vec::new();
    for input in &spec.inputs {
        let InputSource::S3 { bucket, key, .. } = &input.source;
        let name = input.name.clone().unwrap_or_else(|| input.dest_key.clone());
        let captured = record
            .captured_inputs
            .iter()
            .find(|captured| captured.destination_key == input.dest_key);
        let mut file = json!({ "@type": "File", "name": name });
        if let Some(captured) = captured {
            file["contentSize"] = json!(captured.bytes.to_string());
            // The group whose bucket held the input, and whether anyone may read it.
            file["sourceOrganization"] = json!({ "@id": group_entity(captured.source_group_id) });
            source_groups.insert(captured.source_group_id);
            if captured.public {
                file["conditionsOfAccess"] = json!("public");
            }
            // The run read the target; the link it named stays visible next to it.
            if let Some(link) = &captured.link {
                let id = format!("#link-{}", link_properties.len());
                file["additionalProperty"] = json!({ "@id": id });
                link_properties.push(json!({
                    "@id": id,
                    "@type": "PropertyValue",
                    "name": "Reached through link",
                    "propertyID": LINK_PROPERTY,
                    "value": link.to_w3id()
                }));
            }
        }
        // Only the device a version came from is named, never the local path it had there.
        if let Some(device) = devices.get(&input.dest_key) {
            file["isBasedOn"] = json!({ "@id": device_entity(device) });
            used_devices.insert(device.clone());
        } else if let Some(url) = captured
            .and_then(|captured| captured.source_pin.as_ref())
            .and_then(|pin| pin.url.as_ref())
        {
            // A reference names the public source it reads; a private one stays unnamed.
            file["isBasedOn"] = json!({ "@id": url });
        }
        let location = ObjectLocation {
            bucket: bucket.clone(),
            key: key.clone(),
        };
        let version = captured
            .and_then(|captured| exact(captured.source_node_id, &location, captured.version_id));
        let hash = captured.and_then(|captured| captured.content.blake3());
        let (id, mut file) = entity(location, hash, (version, captured.is_some()), file);
        if let Some(captured) = captured {
            for context_entity in capture_entities(&id, captured, &mut file) {
                let context_id = context_entity["@id"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string();
                context_entities.entry(context_id).or_insert(context_entity);
            }
            // The bucket metadata and releases an input is part of are datasets the root lists.
            for part in file["isPartOf"].as_array().into_iter().flatten() {
                if let Some(part) = part["@id"].as_str()
                    && !part_ids.iter().any(|id| id == part)
                {
                    part_ids.push(part.to_string());
                }
            }
        }
        object_ids.push(json!({ "@id": id.clone() }));
        files.push(file);
        part_ids.push(id);
    }

    let mut result_ids = Vec::with_capacity(outputs.len());
    for output in &outputs {
        let file = json!({
            "@type": "File",
            "name": output.key.clone(),
            "contentSize": output.size.to_string(),
            "encodingFormat": key_content_type(&output.key)
        });
        let location = ObjectLocation {
            bucket: output.bucket.clone(),
            key: output.key.clone(),
        };
        let hash = output.digest.as_deref().and_then(parse_hash);
        let version = exact(output.node_id, &location, output.version_id);
        let (id, file) = entity(location, hash, (version, false), file);
        result_ids.push(json!({ "@id": id.clone() }));
        files.push(file);
        part_ids.push(id);
    }
    let has_part: Vec<serde_json::Value> = part_ids
        .iter()
        .map(|id| json!({ "@id": id.clone() }))
        .collect();

    let workspace_property = workspace.as_deref().map(|bucket| {
        json!({
            "@id": "#workspace-bucket",
            "@type": "PropertyValue",
            "name": "Workspace bucket",
            "propertyID": WORKSPACE_PROPERTY,
            "value": bucket
        })
    });
    let mut properties: Vec<serde_json::Value> = workspace_property.into_iter().collect();
    properties.extend(launch.map(launch_properties).unwrap_or_default());

    let mut action = json!({
        "@id": action_id.clone(),
        "@type": "CreateAction",
        "name": run_name.clone(),
        "agent": {"@id": agent_id.clone()},
        "instrument": {"@id": software_id.clone()},
        "containerImage": {"@id": container_id.clone()},
        "object": object_ids,
        "result": result_ids,
        "actionStatus": {"@id": action_status}
    });
    if let Some(map) = action.as_object_mut() {
        if !properties.is_empty() {
            let ids: Vec<_> = properties
                .iter()
                .map(|property| json!({ "@id": property["@id"].clone() }))
                .collect();
            map.insert("additionalProperty".to_string(), json!(ids));
        }
        if let Some(launch) = launch {
            map.insert("location".to_string(), json!({ "@id": SITE_ID }));
            let environment: Vec<_> = launch
                .env
                .keys()
                .map(|name| json!({ "@id": env_entity(name) }))
                .collect();
            if !environment.is_empty() {
                map.insert("environment".to_string(), json!(environment));
            }
        }
        if !command.is_empty() {
            map.insert("description".to_string(), json!(command));
        }
        if let Some(started) = record.started_at_ms {
            map.insert("startTime".to_string(), json!(rfc3339(started)));
        }
        if let Some(finished) = record.finished_at_ms {
            map.insert("endTime".to_string(), json!(rfc3339(finished)));
        }
        if let Some(code) = exit_code.filter(|code| *code != 0) {
            map.insert("error".to_string(), json!(format!("exit code {code}")));
        }
    }

    let mut software = json!({
        "@id": software_id,
        "@type": "SoftwareApplication",
        "name": application_name,
        "identifier": image
    });
    let image_url = image_url(&registry, &image_name);
    if let Some(map) = software.as_object_mut()
        && let Some(version) = submitted_tag
            .as_ref()
            .or(pinned_tag.as_ref())
            .or(digest.as_ref())
    {
        map.insert("softwareVersion".to_string(), json!(version));
    }

    let mut container = json!({
        "@id": container_id,
        "@type": "https://w3id.org/ro/terms/workflow-run#ContainerImage",
        "additionalType": {"@id": "https://w3id.org/ro/terms/workflow-run#DockerImage"},
        "identifier": image,
        "registry": registry,
        "name": image_name
    });
    if let Some(map) = container.as_object_mut() {
        if let Some(tag) = submitted_tag.as_ref().or(pinned_tag.as_ref()) {
            map.insert("tag".to_string(), json!(tag));
        }
        if let Some(sha256) = digest
            .as_deref()
            .and_then(|value| value.strip_prefix("sha256:"))
        {
            map.insert("sha256".to_string(), json!(sha256));
        }
        if let Some(url) = image_url {
            map.insert("url".to_string(), json!(url));
        }
    }

    let mut graph = vec![
        json!({
            "@id": "ro-crate-metadata.json",
            "@type": "CreativeWork",
            "conformsTo": {"@id": CRATE_PROFILE},
            "about": {"@id": root.clone()}
        }),
        json!({
            "@id": root,
            "@type": "Dataset",
            "name": run_name,
            "description": run_description,
            "datePublished": rfc3339(record.created_at_ms),
            "license": {"@id": "https://creativecommons.org/licenses/by/4.0/"},
            "conformsTo": {"@id": CRATE_PROFILE_IRI},
            "hasPart": has_part,
            "mentions": {"@id": action_id}
        }),
        action,
        json!({
            "@id": agent_id,
            "@type": "Person",
            "identifier": record.created_by.to_string()
        }),
        software,
        container,
        json!({
            "@id": CRATE_PROFILE_IRI,
            "@type": ["CreativeWork", "http://www.w3.org/ns/dx/prof/Profile"],
            "name": "Process Run Crate",
            "version": "0.5"
        }),
    ];
    graph.extend(files);
    graph.extend(used_devices.iter().map(|device| {
        json!({
            "@id": device_entity(device),
            "@type": "IndividualProduct",
            "name": "Aruna device",
            "identifier": device
        })
    }));
    graph.extend(source_groups.iter().map(|group_id| {
        json!({
            "@id": group_entity(*group_id),
            "@type": "Organization",
            "name": "Aruna group",
            "identifier": group_id.to_string()
        })
    }));
    graph.extend(link_properties);
    graph.extend(context_entities.into_values());
    graph.extend(properties);
    if let Some(launch) = launch {
        let mut site = json!({
            "@id": SITE_ID,
            "@type": "Place",
            "identifier": launch.node_id.to_string(),
            "additionalType": launch.executor_kind
        });
        if !launch.location.is_empty() {
            site["name"] = json!(launch.location);
        }
        graph.push(site);
        graph.extend(launch.env.iter().map(|(name, value)| {
            json!({
                "@id": env_entity(name),
                "@type": "PropertyValue",
                "name": name,
                "value": value
            })
        }));
    }

    json!({
        "@context": [
            CRATE_CONTEXT,
            WORKFLOW_RUN_CONTEXT,
            {
                "containerImage": "https://w3id.org/ro/terms/workflow-run#containerImage",
                "registry": "https://w3id.org/ro/terms/workflow-run#registry",
                "tag": "https://w3id.org/ro/terms/workflow-run#tag",
                "sha256": "https://w3id.org/ro/terms/workflow-run#sha256",
                "environment": "https://w3id.org/ro/terms/workflow-run#environment"
            }
        ],
        "@graph": graph
    })
    .to_string()
}

fn command_line(spec: &ExecutionSpec) -> String {
    spec.entrypoint
        .iter()
        .flatten()
        .chain(spec.command.iter())
        .map(|argument| quote_argument(argument.as_str()))
        .collect::<Vec<_>>()
        .join(" ")
}

fn quote_argument(argument: &str) -> String {
    if !argument.is_empty()
        && argument
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"%+,-./:=@_".contains(&byte))
    {
        return argument.to_string();
    }
    format!("'{}'", argument.replace('\'', "'\"'\"'"))
}

fn split_image(image: &str) -> (String, Option<String>, Option<String>) {
    let (name, digest) = image
        .split_once('@')
        .map_or((image, None), |(name, digest)| (name, Some(digest)));
    if let Some((repository, tag)) = name.rsplit_once(':')
        && !tag.contains('/')
    {
        return (
            repository.to_string(),
            Some(tag.to_string()),
            digest.map(str::to_string),
        );
    }
    (name.to_string(), None, digest.map(str::to_string))
}

fn split_registry(repository: &str) -> (String, String) {
    if let Some((registry, name)) = repository.split_once('/')
        && (registry.contains('.') || registry.contains(':') || registry == "localhost")
    {
        return (registry.to_string(), name.to_string());
    }
    let name = if repository.contains('/') {
        repository.to_string()
    } else {
        format!("library/{repository}")
    };
    ("docker.io".to_string(), name)
}

fn image_url(registry: &str, name: &str) -> Option<String> {
    let valid_name = name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || b"-._/".contains(&byte));
    if name.is_empty() || !valid_name {
        return None;
    }
    match registry {
        "docker.io" | "index.docker.io" | "registry-1.docker.io" => {
            Some(format!("https://hub.docker.com/r/{name}"))
        }
        "ghcr.io" => Some(format!("https://ghcr.io/{name}")),
        _ => None,
    }
}

fn rfc3339(ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(ms as i64)
        .map(|timestamp| timestamp.to_rfc3339())
        .unwrap_or_default()
}

#[cfg(test)]
#[path = "crate_run_tests.rs"]
mod run_tests;

#[cfg(test)]
mod pure_tests {
    use super::*;
    use crate::metadata::create_document::CreateDocumentError;
    use aruna_core::metadata::{
        MetadataError, ProfileValidationCompleteness, ProfileValidationFinding,
        ProfileValidationSeverity,
    };

    fn profile_rejection(code: &str) -> MetadataWriteError {
        MetadataWriteError::Create(CreateDocumentError::MetadataError(
            MetadataError::ProfileValidation(vec![ProfileValidationFinding {
                code: code.to_string(),
                severity: ProfileValidationSeverity::Violation,
                focus_node: None,
                path: None,
                rule: "http://purl.org/dc/terms/conformsTo".to_string(),
                message: String::new(),
                profile_revision: None,
                completeness: ProfileValidationCompleteness::Complete,
            }]),
        ))
    }

    #[test]
    fn profile_rejections_settle() {
        // A Profile verdict is permanent; only an unavailable evaluator retries.
        assert!(!is_transient(&profile_rejection("profile_not_registered")));
        assert!(is_transient(&profile_rejection("validator_unavailable")));
    }

    use aruna_core::UserId;
    use aruna_core::structs::execution::job::{
        AttemptIntent, CapturedInput, InputMode, InputSelection, OutputObject,
    };
    use aruna_core::structs::identity::realm::RealmId;
    use serde_json::Value;

    pub(super) fn execution_record() -> (JobRecord, ExecutionSpec) {
        // Succeeded run with one S3 input and one produced output.
        let realm = RealmId([1; 32]);
        let user = UserId::new(Ulid::from_bytes([2; 16]), realm);
        let node = iroh::SecretKey::from_bytes(&[3; 32]).public();
        let spec = ExecutionSpec {
            group_id: Ulid::from_bytes([4; 16]),
            name: Some("Variant calling".to_string()),
            description: Some("Call variants for sample one".to_string()),
            tags: Default::default(),
            image: "docker.io/aruna/tool:2.0".to_string(),
            entrypoint: Some(vec!["/usr/bin/python".to_string()]),
            command: vec![
                "/work/script.py".to_string(),
                "--label".to_string(),
                "sample one".to_string(),
            ],
            workdir: None,
            env: Default::default(),
            resources: Default::default(),
            executor_constraint: None,
            inputs: vec![InputSelection {
                source: InputSource::S3 {
                    bucket: "src".to_string(),
                    key: "in.txt".to_string(),
                    version_id: None,
                },
                source_node_id: None,
                dest_key: "inputs/in.txt".to_string(),
                mode: InputMode::Snapshot,
                container_path: None,
                name: Some("Input sample".to_string()),
                description: None,
            }],
            file_outputs: Vec::new(),
            workspace_outputs: Vec::new(),
            output_prefixes: Vec::new(),
            collision_policy: Default::default(),
        };
        let job_id = JobId::from_bytes([5; 16]);
        let mut record = JobRecord::new(
            job_id,
            JobPayload::Execution(spec.clone()),
            user,
            node,
            1_000,
            1_000,
            None,
        );
        record.state = JobState::Succeeded;
        record.started_at_ms = Some(2_000);
        record.finished_at_ms = Some(3_000);
        record.attempt_intent = Some(AttemptIntent {
            attempt_no: 2,
            external_name: "aruna-task-a2".to_string(),
            executor_kind: "kubernetes".to_string(),
            pinned_image: format!("docker.io/aruna/tool@sha256:{}", "a".repeat(64)),
            attempt_epoch: 7,
        });
        record.workspace_bucket = Some("workspace-sample-one".to_string());
        record.result = Some(JobResultPayload::Execution {
            exit_code: Some(0),
            workspace_bucket: Some("workspace-sample-one".to_string()),
            outputs: vec![OutputObject {
                node_id: node,
                bucket: "src".to_string(),
                key: "out.txt".to_string(),
                version_id: Ulid::from_bytes([11; 16]),
                execution_id: Ulid::from_bytes([12; 16]),
                container_path: "/work/out.txt".to_string(),
                size: 7,
                digest: None,
            }],
            stdout: String::new(),
            stderr: String::new(),
            output_digest: None,
        });
        (record, spec)
    }

    fn parse(record: &JobRecord, spec: &ExecutionSpec) -> serde_json::Value {
        let doc = Ulid::from_bytes(record.job_id.to_bytes());
        serde_json::from_str(&build_crate_jsonld(
            record,
            spec,
            (record.created_by.realm_id, Some(doc)),
            &BTreeMap::new(),
            None,
        ))
        .unwrap()
    }

    pub(super) fn entity_id(value: &Value) -> Option<&str> {
        value
            .as_str()
            .or_else(|| value.get("@id").and_then(Value::as_str))
    }

    #[test]
    fn crate_declares_profiles() {
        // The descriptor declares 1.2; the root declares the exact run profile.
        let (record, spec) = execution_record();
        let value = parse(&record, &spec);
        assert_eq!(value["@context"][0], CRATE_CONTEXT);
        assert_eq!(value["@context"][1], WORKFLOW_RUN_CONTEXT);
        assert_eq!(
            value["@context"][2]["containerImage"],
            "https://w3id.org/ro/terms/workflow-run#containerImage"
        );
        let graph = value["@graph"].as_array().unwrap();
        let by_id = |id: &str| graph.iter().find(|e| e["@id"] == id).expect("entity");
        let descriptor = by_id("ro-crate-metadata.json");
        assert_eq!(descriptor["conformsTo"]["@id"], CRATE_PROFILE);
        let root_id = descriptor["about"]["@id"].as_str().unwrap().to_string();
        let root = by_id(&root_id);
        assert_eq!(root["conformsTo"]["@id"], CRATE_PROFILE_IRI);
        let profile = by_id(CRATE_PROFILE_IRI);
        assert_eq!(profile["@type"][0], "CreativeWork");
        assert_eq!(profile["@type"][1], "http://www.w3.org/ns/dx/prof/Profile");
        assert_eq!(profile["name"], "Process Run Crate");
        assert!(!graph.iter().any(|entity| entity["@id"] == CRATE_PROFILE));
        assert_eq!(root["mentions"]["@id"], format!("#run-{}", record.job_id));
    }

    #[test]
    fn action_wires_provenance() {
        let (record, spec) = execution_record();
        let value = parse(&record, &spec);
        let graph = value["@graph"].as_array().unwrap();
        let by_id = |id: &str| graph.iter().find(|e| e["@id"] == id).expect("entity");
        let action = by_id(&format!("#run-{}", record.job_id));
        assert_eq!(action["@type"], "CreateAction");
        assert_eq!(action["name"], "Variant calling");
        assert_eq!(
            action["description"],
            "/usr/bin/python /work/script.py --label 'sample one'"
        );
        assert!(action.get("command").is_none());
        let instrument = action["instrument"]["@id"].as_str().unwrap();
        assert_eq!(instrument, format!("#software-{}", record.job_id));
        let software = by_id(instrument);
        assert_eq!(software["@type"], "SoftwareApplication");
        assert_eq!(software["name"], "tool");
        assert_eq!(
            software["identifier"],
            format!("docker.io/aruna/tool@sha256:{}", "a".repeat(64))
        );
        assert_eq!(software["softwareVersion"], "2.0");
        let container = by_id(action["containerImage"]["@id"].as_str().unwrap());
        assert_eq!(
            container["@type"],
            "https://w3id.org/ro/terms/workflow-run#ContainerImage"
        );
        assert_eq!(container["registry"], "docker.io");
        assert_eq!(container["name"], "aruna/tool");
        assert_eq!(container["tag"], "2.0");
        assert_eq!(container["sha256"], "a".repeat(64));
        assert_eq!(container["url"], "https://hub.docker.com/r/aruna/tool");
        assert_eq!(
            image_url("ghcr.io", "astral-sh/uv").as_deref(),
            Some("https://ghcr.io/astral-sh/uv")
        );
        assert_eq!(action["object"][0]["@id"], "s3://src/in.txt");
        assert_eq!(action["result"][0]["@id"], "s3://src/out.txt");
        assert_eq!(
            action["actionStatus"]["@id"],
            "http://schema.org/CompletedActionStatus"
        );
        assert!(action["startTime"].is_string());
        assert!(action["endTime"].is_string());
        let agent = by_id(action["agent"]["@id"].as_str().unwrap());
        assert_eq!(agent["@type"], "Person");
        let input = by_id("s3://src/in.txt");
        assert_eq!(input["name"], "Input sample");
        let output = by_id("s3://src/out.txt");
        assert_eq!(output["name"], "out.txt");
        assert_eq!(output["contentSize"], "7");
        assert_eq!(output["encodingFormat"], "text/plain; charset=utf-8");
        let workspace = by_id("#workspace-bucket");
        assert_eq!(action["additionalProperty"][0]["@id"], "#workspace-bucket");
        assert_eq!(workspace["propertyID"], WORKSPACE_PROPERTY);
        assert_eq!(workspace["value"], "workspace-sample-one");
        let descriptor = by_id("ro-crate-metadata.json");
        let root = by_id(descriptor["about"]["@id"].as_str().unwrap());
        assert_eq!(root["name"], "Variant calling");
        assert_eq!(root["description"], "Call variants for sample one");
    }

    #[test]
    fn names_source_group() {
        // An input from another group's public bucket names that group and its open access.
        let (mut record, spec) = execution_record();
        let group = Ulid::from_bytes([21; 16]);
        record.captured_inputs.push(CapturedInput {
            destination_key: "inputs/in.txt".to_string(),
            source_node_id: record.owner_node_id,
            version_id: Ulid::from_bytes([13; 16]),
            content: aruna_core::structs::execution::job::InputContent::Blake3([9; 32]),
            bytes: 3,
            policies: Vec::new(),
            source_group_id: group,
            public: true,
            link: Some(VersionedObjectArn {
                realm_id: record.created_by.realm_id,
                node_id: record.owner_node_id,
                bucket: "work".to_string(),
                key: "db/bakta.db".to_string(),
                version: Ulid::from_bytes([14; 16]),
            }),
            source_pin: None,
            database: None,
            releases: Vec::new(),
            releases_complete: true,
            grant: None,
            served_by: None,
        });
        let value = parse(&record, &spec);
        let graph = value["@graph"].as_array().unwrap();
        let input = graph
            .iter()
            .find(|entity| entity["name"] == "Input sample")
            .expect("input entity");
        let organization = format!("#group-{group}");
        assert_eq!(input["sourceOrganization"]["@id"], organization.as_str());
        assert_eq!(input["conditionsOfAccess"], "public");
        let entity = graph
            .iter()
            .find(|entity| entity["@id"] == organization.as_str())
            .expect("group entity");
        assert_eq!(entity["@type"], "Organization");
        assert_eq!(entity["identifier"], group.to_string());
        let link = graph
            .iter()
            .find(|entity| entity["@id"] == input["additionalProperty"]["@id"])
            .expect("link property");
        assert_eq!(link["propertyID"], LINK_PROPERTY);
        assert!(
            link["value"]
                .as_str()
                .is_some_and(|value| value.contains("work"))
        );
    }

    #[test]
    fn names_captured_context() {
        // Admission's capture names a public source's pin, the bucket's metadata then and the
        // releases that pin the input; unchecked membership is marked on the file.
        use aruna_core::structs::identity::identifier::{Identifier, IdentifierTarget};
        use aruna_core::structs::storage::bucket_metadata::{CapturedDatabase, ReleaseRef};
        use aruna_core::structs::storage::dataset_description::DatasetDescription;
        use aruna_core::structs::storage::origin::{OriginVersion, SourcePin};
        let (mut record, spec) = execution_record();
        let doi = Identifier {
            name: "DOI".to_string(),
            value: "10.5281/zenodo.1".to_string(),
            url: "https://doi.org/10.5281/zenodo.1".to_string(),
            target: IdentifierTarget::Key,
        };
        let crate_version = OriginVersion::Node(VersionedObjectArn {
            realm_id: record.created_by.realm_id,
            node_id: record.owner_node_id,
            bucket: "bakta-db".to_string(),
            key: "db/release.json".to_string(),
            version: Ulid::from_bytes([15; 16]),
        });
        let captured = |releases_complete| CapturedInput {
            destination_key: "inputs/in.txt".to_string(),
            source_node_id: record.owner_node_id,
            version_id: Ulid::from_bytes([13; 16]),
            content: InputContent::Reference {
                etag: Some("\"abc\"".to_string()),
                range_bound: true,
            },
            bytes: 3,
            policies: Vec::new(),
            source_group_id: Ulid::from_bytes([21; 16]),
            public: false,
            link: None,
            source_pin: Some(SourcePin {
                url: Some("https://example.org/db/in.txt".to_string()),
                path: "db/in.txt".to_string(),
                source_version: Some("v6".to_string()),
            }),
            served_by: None,
            database: Some(CapturedDatabase {
                database: true,
                described: DatasetDescription {
                    title: Some("Bakta".to_string()),
                    version: Some("6.0".to_string()),
                    identifiers: vec![doi.clone()],
                    ..DatasetDescription::default()
                },
                dataset: Some(Ulid::from_bytes([16; 16])),
                ..CapturedDatabase::default()
            }),
            releases: vec![ReleaseRef {
                crate_version: crate_version.clone(),
                label: "6.0".to_string(),
                upstream: vec![doi.clone()],
            }],
            releases_complete,
            grant: None,
        };
        record.captured_inputs.push(captured(true));
        let value = parse(&record, &spec);
        let graph = value["@graph"].as_array().unwrap();
        let find = |id: &serde_json::Value| graph.iter().find(|entity| entity["@id"] == *id);
        let input = graph
            .iter()
            .find(|entity| entity["name"] == "Input sample")
            .expect("input entity");
        let properties = input["additionalProperty"]
            .as_array()
            .expect("pin properties");
        let values: Vec<_> = (properties.iter())
            .map(|property| find(&property["@id"]).unwrap()["value"].clone())
            .collect();
        assert_eq!(values, [json!("db/in.txt"), json!("v6"), json!("\"abc\"")]);
        // The source that delivered the bytes is named beside the pin, a public one only.
        let mut served = captured(true);
        served.served_by = Some(SourcePin {
            url: Some("https://mirror.example.org/db/in.txt".to_string()),
            path: "db/in.txt".to_string(),
            source_version: None,
        });
        let mut private = captured(true);
        private.served_by = Some(SourcePin {
            url: None,
            path: "secret/in.txt".to_string(),
            source_version: None,
        });
        let mut named = record.clone();
        named.captured_inputs = vec![served];
        let rendered = parse(&named, &spec).to_string();
        assert!(rendered.contains("https://mirror.example.org/db/in.txt"));
        named.captured_inputs = vec![private];
        let rendered = parse(&named, &spec).to_string();
        assert!(rendered.contains("db/in.txt") && !rendered.contains("secret/in.txt"));
        let part_of = input["isPartOf"].as_array().expect("part of");
        let database = find(&part_of[0]["@id"]).expect("metadata snapshot");
        let dataset = MetadataRegistryRecord::graph_iri_for(Ulid::from_bytes([16; 16]));
        assert_eq!(database["isBasedOn"]["@id"], dataset.as_str());
        assert_eq!(database["name"], "Bakta (at admission)");
        assert_eq!(database["version"], "6.0");
        assert_eq!(database["identifier"][0], doi.url.as_str());
        let release = find(&part_of[1]["@id"]).expect("release");
        assert_eq!(release["@id"], crate_version.to_id().as_str());
        assert_eq!(release["version"], "6.0");
        assert_eq!(release["isBasedOn"][0]["@id"], doi.url.as_str());
        // The metadata snapshot and the release must reach the root, or the crate is refused.
        let checked = craqle::validate_rocrate_jsonld(&value.to_string());
        assert!(checked.is_ok(), "{checked:?}");

        // Membership that was not checked everywhere is marked; a private source stays unnamed.
        let mut unchecked = captured(false);
        (unchecked.source_pin.as_mut().unwrap()).url = None;
        record.captured_inputs = vec![unchecked];
        let value = parse(&record, &spec);
        let graph = value["@graph"].as_array().unwrap();
        let input = graph
            .iter()
            .find(|entity| entity["name"] == "Input sample")
            .expect("input entity");
        let marker = graph
            .iter()
            .find(|entity| entity["@id"] == input["additionalProperty"]["@id"])
            .expect("membership marker");
        assert_eq!(marker["propertyID"], RELEASE_MEMBERSHIP);
        assert_eq!(marker["value"], "incomplete");
        assert!(!value.to_string().contains("db/in.txt"));
    }

    #[test]
    fn references_stay_apart() {
        // Two references with one bucket and key on two nodes are two files, each with its
        // own source version.
        use aruna_core::structs::storage::origin::SourcePin;
        let (mut record, mut spec) = execution_record();
        let mut second = spec.inputs[0].clone();
        second.dest_key = "inputs/in2.txt".to_string();
        second.name = Some("Second sample".to_string());
        spec.inputs.push(second);
        let other_node = iroh::SecretKey::from_bytes(&[33; 32]).public();
        for (destination, node, seed, source_version) in [
            ("inputs/in.txt", record.owner_node_id, 13, "v1"),
            ("inputs/in2.txt", other_node, 14, "v2"),
        ] {
            record.captured_inputs.push(CapturedInput {
                destination_key: destination.to_string(),
                source_node_id: node,
                version_id: Ulid::from_bytes([seed; 16]),
                content: InputContent::Reference {
                    etag: Some(format!("\"{source_version}\"")),
                    range_bound: true,
                },
                bytes: 3,
                policies: Vec::new(),
                source_group_id: Ulid::from_bytes([21; 16]),
                public: false,
                link: None,
                source_pin: Some(SourcePin {
                    url: Some("https://example.org/in.txt".to_string()),
                    path: "in.txt".to_string(),
                    source_version: Some(source_version.to_string()),
                }),
                database: None,
                releases: Vec::new(),
                releases_complete: true,
                grant: None,
                served_by: None,
            });
        }
        let value = parse(&record, &spec);
        let graph = value["@graph"].as_array().unwrap();
        let version_of = |name: &str| {
            let file = graph
                .iter()
                .find(|entity| entity["name"] == name)
                .expect("input entity");
            let property = (file["additionalProperty"].as_array().unwrap().iter())
                .map(|property| graph.iter().find(|entity| entity["@id"] == property["@id"]))
                .find(|entity| entity.is_some_and(|entity| entity["name"] == "Source version"))
                .flatten()
                .expect("source version");
            (file["@id"].clone(), property["value"].clone())
        };
        let (first_id, first) = version_of("Input sample");
        let (second_id, second) = version_of("Second sample");
        assert_ne!(first_id, second_id);
        assert_eq!((first, second), (json!("v1"), json!("v2")));
    }

    #[test]
    fn snapshots_stay_apart() {
        // Two captures of one dataset with different descriptions stay two snapshots of it.
        use aruna_core::structs::storage::bucket_metadata::CapturedDatabase;
        use aruna_core::structs::storage::dataset_description::DatasetDescription;
        let (mut record, mut spec) = execution_record();
        let mut second = spec.inputs[0].clone();
        second.dest_key = "inputs/in2.txt".to_string();
        second.name = Some("Second sample".to_string());
        spec.inputs.push(second);
        let dataset = Ulid::from_bytes([16; 16]);
        for (destination, seed, title) in [
            ("inputs/in.txt", 13, "Bakta 5"),
            ("inputs/in2.txt", 14, "Bakta 6"),
        ] {
            record.captured_inputs.push(CapturedInput {
                destination_key: destination.to_string(),
                source_node_id: record.owner_node_id,
                version_id: Ulid::from_bytes([seed; 16]),
                content: InputContent::Blake3([seed; 32]),
                bytes: 3,
                policies: Vec::new(),
                source_group_id: Ulid::from_bytes([21; 16]),
                public: false,
                link: None,
                source_pin: None,
                served_by: None,
                database: Some(CapturedDatabase {
                    database: true,
                    described: DatasetDescription {
                        title: Some(title.to_string()),
                        ..DatasetDescription::default()
                    },
                    dataset: Some(dataset),
                    ..CapturedDatabase::default()
                }),
                releases: Vec::new(),
                releases_complete: true,
                grant: None,
            });
        }
        let value = parse(&record, &spec);
        let graph = value["@graph"].as_array().unwrap();
        let snapshot_of = |name: &str| {
            let file = graph
                .iter()
                .find(|entity| entity["name"] == name)
                .expect("input entity");
            let id = &file["isPartOf"][0]["@id"];
            graph
                .iter()
                .find(|entity| entity["@id"] == *id)
                .expect("snapshot")
                .clone()
        };
        let (first, second) = (snapshot_of("Input sample"), snapshot_of("Second sample"));
        assert_ne!(first["@id"], second["@id"]);
        assert_eq!(first["name"], "Bakta 5 (at admission)");
        assert_eq!(second["name"], "Bakta 6 (at admission)");
        let source = MetadataRegistryRecord::graph_iri_for(dataset);
        assert_eq!(first["isBasedOn"]["@id"], source.as_str());
        assert_eq!(second["isBasedOn"]["@id"], source.as_str());
    }

    #[test]
    fn hides_pin_credentials() {
        // Credentials in a connector endpoint never reach a pin or the run crate.
        use aruna_core::structs::execution::source_connector::SourceConnectorKind;
        use aruna_core::structs::execution::staging::PortableSourceDescriptor;
        use aruna_core::structs::storage::origin::SourcePin;
        let endpoint = "https://reader:hunter2@data.example.org/api?token=t0ken&sig=s1g";
        let mut descriptor = PortableSourceDescriptor {
            kind: SourceConnectorKind::S3,
            public_config: std::collections::HashMap::from([
                ("endpoint".to_string(), endpoint.to_string()),
                ("skip_signature".to_string(), "true".to_string()),
            ]),
            source_path: "in.txt".to_string(),
            version_selector: None,
            namespace: None,
            capabilities: Vec::new(),
            origin_node_id: None,
            root: None,
        };
        let source_pin = Some(SourcePin::of(&descriptor));
        descriptor.kind = SourceConnectorKind::Http;
        let served_by = Some(SourcePin::served(&descriptor));
        let (mut record, spec) = execution_record();
        record.captured_inputs.push(CapturedInput {
            destination_key: "inputs/in.txt".to_string(),
            source_node_id: record.owner_node_id,
            version_id: Ulid::from_bytes([13; 16]),
            content: aruna_core::structs::execution::job::InputContent::Blake3([9; 32]),
            bytes: 3,
            policies: Vec::new(),
            source_group_id: Default::default(),
            public: false,
            link: None,
            source_pin,
            database: None,
            releases: Vec::new(),
            releases_complete: false,
            grant: None,
            served_by,
        });
        let rendered = parse(&record, &spec).to_string();
        assert!(rendered.contains("https://data.example.org/api/in.txt"));
        assert!(rendered.contains("\"https://data.example.org\""));
        for secret in ["reader", "hunter2", "t0ken", "s1g", "token"] {
            assert!(!rendered.contains(secret), "{secret} leaked: {rendered}");
        }
    }

    #[test]
    fn names_by_content() {
        // Known digests become content addresses; the same content twice keeps one address,
        // and each entity keeps its own exact version as a second `contentUrl`.
        let (mut record, spec) = execution_record();
        record.captured_inputs.push(CapturedInput {
            destination_key: "inputs/in.txt".to_string(),
            source_node_id: record.owner_node_id,
            version_id: Ulid::from_bytes([13; 16]),
            content: aruna_core::structs::execution::job::InputContent::Blake3([9; 32]),
            bytes: 3,
            policies: Vec::new(),
            source_group_id: Default::default(),
            public: false,
            link: None,
            source_pin: None,
            database: None,
            releases: Vec::new(),
            releases_complete: false,
            grant: None,
            served_by: None,
        });
        if let Some(JobResultPayload::Execution { outputs, .. }) = &mut record.result {
            outputs[0].digest = Some(hex::encode([9; 32]));
        }
        let value = parse(&record, &spec);
        let graph = value["@graph"].as_array().unwrap();
        let by_url = |url: &str| {
            graph
                .iter()
                .find(|entity| entity["contentUrl"][0] == url)
                .expect("entity")
        };
        let arn = |version: u8| {
            VersionedObjectArn::new(
                record.created_by.realm_id,
                record.owner_node_id,
                "src",
                if version == 13 { "in.txt" } else { "out.txt" },
                Ulid::from_bytes([version; 16]),
            )
            .unwrap()
            .to_w3id()
        };
        let content = aruna_core::structs::storage::data::identity::content_id([9; 32]);
        let input = by_url("s3://src/in.txt");
        assert_eq!(input["@id"], content);
        assert_eq!(input["contentSize"], "3");
        let output = by_url("s3://src/out.txt");
        assert_eq!(output["@id"], "s3://src/out.txt");
        assert_eq!(input["contentUrl"][1], arn(13));
        assert_eq!(output["contentUrl"][1], arn(11));
        let action = graph.iter().find(|e| e["@type"] == "CreateAction").unwrap();
        assert_eq!(action["object"][0]["@id"], content);
        assert!(craqle::validate_rocrate_jsonld(&value.to_string()).is_ok());
    }

    #[test]
    fn same_bytes_versioned() {
        // A second input with the same bytes, here reached through a link, is named by its
        // exact version, never by a bucket path another crate could use for other data.
        let (mut record, mut spec) = execution_record();
        let mut second = spec.inputs[0].clone();
        second.dest_key = "inputs/linked.txt".to_string();
        second.name = Some("Linked sample".to_string());
        spec.inputs.push(second);
        let link = VersionedObjectArn::new(
            record.created_by.realm_id,
            record.owner_node_id,
            "work",
            "links/in.txt",
            Ulid::from_bytes([15; 16]),
        )
        .unwrap();
        for (destination, link) in [("inputs/in.txt", None), ("inputs/linked.txt", Some(link))] {
            record.captured_inputs.push(CapturedInput {
                destination_key: destination.to_string(),
                source_node_id: record.owner_node_id,
                version_id: Ulid::from_bytes([13; 16]),
                content: InputContent::Blake3([9; 32]),
                bytes: 3,
                policies: Vec::new(),
                source_group_id: Default::default(),
                public: false,
                link,
                source_pin: None,
                database: None,
                releases: Vec::new(),
                releases_complete: false,
                grant: None,
                served_by: None,
            });
        }
        let value = parse(&record, &spec);
        let graph = value["@graph"].as_array().unwrap();
        let id_of = |name: &str| {
            let file = graph.iter().find(|entity| entity["name"] == name);
            file.expect("input entity")["@id"].clone()
        };
        let exact = VersionedObjectArn::new(
            record.created_by.realm_id,
            record.owner_node_id,
            "src",
            "in.txt",
            Ulid::from_bytes([13; 16]),
        )
        .unwrap()
        .to_w3id();
        let content = aruna_core::structs::storage::data::identity::content_id([9; 32]);
        assert_eq!(id_of("Input sample"), content);
        assert_eq!(id_of("Linked sample"), exact);
        assert!(
            !graph
                .iter()
                .any(|entity| entity["@id"] == "s3://src/in.txt")
        );
        assert!(craqle::validate_rocrate_jsonld(&value.to_string()).is_ok());
    }

    #[test]
    fn uncaptured_keeps_location() {
        // An input without a captured version keeps its location as its only `contentUrl`.
        let (record, spec) = execution_record();
        let value = parse(&record, &spec);
        let graph = value["@graph"].as_array().unwrap();
        let input = graph
            .iter()
            .find(|entity| entity["name"] == "Input sample")
            .expect("input entity");
        assert_eq!(input["contentUrl"], "s3://src/in.txt");
    }

    #[test]
    fn command_quotes_arguments() {
        let (_, mut spec) = execution_record();
        spec.entrypoint = Some(vec!["/bin/echo".to_string()]);
        spec.command = vec![String::new(), "it's ready".to_string()];
        assert_eq!(command_line(&spec), "/bin/echo '' 'it'\"'\"'s ready'");
    }

    #[test]
    fn failed_run_status() {
        // A nonzero exit yields FailedActionStatus and an error string.
        let (mut record, spec) = execution_record();
        record.state = JobState::Failed;
        if let Some(JobResultPayload::Execution { exit_code, .. }) = &mut record.result {
            *exit_code = Some(2);
        }
        let value = parse(&record, &spec);
        let graph = value["@graph"].as_array().unwrap();
        let action = graph
            .iter()
            .find(|e| e["@type"] == "CreateAction")
            .expect("action");
        assert_eq!(
            action["actionStatus"]["@id"],
            "http://schema.org/FailedActionStatus"
        );
        assert_eq!(action["error"], "exit code 2");
    }
}

/// Storage/profile boundary coverage for the crate generator; kept out of the
/// pure selection because it opens temporary Craqle and SHACL stores.
#[cfg(test)]
mod tests {
    use super::pure_tests::{entity_id, execution_record};
    use super::*;
    use serde_json::Value;
    use ulid::Ulid;

    #[test]
    fn crate_survives_roundtrip() {
        // Storage must retain exact profile and workflow-run terms.
        let (record, spec) = execution_record();
        let document_id = Ulid::from_bytes(record.job_id.to_bytes());
        let root_id = format!("https://w3id.org/aruna/{document_id}");
        let jsonld = build_crate_jsonld(
            &record,
            &spec,
            (record.created_by.realm_id, Some(document_id)),
            &BTreeMap::new(),
            None,
        );
        let directory = tempfile::tempdir().unwrap();
        let node = craqle::CraqleNode::open(directory.path()).unwrap();
        let graph_id = craqle::GraphId::new(&root_id);
        node.apply_rocrate_document_checked_with_policy(
            &craqle::AllowAllAuthorizer,
            graph_id.clone(),
            &jsonld,
            craqle::GraphPolicy::default(),
        )
        .unwrap();

        let exported = node
            .export_rocrate(&craqle::AllowAllAuthorizer, &graph_id)
            .unwrap();
        let value: Value = serde_json::from_str(&exported).unwrap();
        let graph = value["@graph"].as_array().unwrap();
        let by_id = |id: &str| {
            graph
                .iter()
                .find(|entity| entity["@id"] == id)
                .expect("entity")
        };
        let has_type = |entity: &Value, expected: &str| match &entity["@type"] {
            Value::String(value) => value == expected,
            Value::Array(values) => values.iter().any(|value| value == expected),
            _ => false,
        };
        let root = by_id(&root_id);
        assert_eq!(entity_id(&root["conformsTo"]), Some(CRATE_PROFILE_IRI));
        let action = graph
            .iter()
            .find(|entity| has_type(entity, "CreateAction"))
            .expect("action");
        assert_eq!(
            action["description"],
            "/usr/bin/python /work/script.py --label 'sample one'"
        );
        assert_eq!(
            entity_id(&action["additionalProperty"]),
            Some("#workspace-bucket")
        );
        let container = by_id(entity_id(&action["containerImage"]).unwrap());
        assert!(has_type(
            container,
            "https://w3id.org/ro/terms/workflow-run#ContainerImage"
        ));
        assert_eq!(container["registry"], "docker.io");
        assert_eq!(container["sha256"], "a".repeat(64));
        let profile = by_id(CRATE_PROFILE_IRI);
        assert!(has_type(profile, "http://www.w3.org/ns/dx/prof/Profile"));
        assert!(graph.iter().any(|entity| {
            entity["propertyID"] == WORKSPACE_PROPERTY && entity["value"] == "workspace-sample-one"
        }));
    }

    fn device_id() -> String {
        iroh::SecretKey::from_bytes(&[8; 32]).public().to_string()
    }

    #[test]
    fn names_input_device() {
        // An input published from a device points at one entity naming only that device.
        let (record, spec) = execution_record();
        let devices = BTreeMap::from([("inputs/in.txt".to_string(), device_id())]);
        let document_id = Ulid::from_bytes(record.job_id.to_bytes());
        let jsonld: Value = serde_json::from_str(&build_crate_jsonld(
            &record,
            &spec,
            (record.created_by.realm_id, Some(document_id)),
            &devices,
            None,
        ))
        .unwrap();
        let graph = jsonld["@graph"].as_array().unwrap();
        let input = graph
            .iter()
            .find(|entity| entity["name"] == "Input sample")
            .expect("the input file is described");
        let device = format!("#device-{}", device_id());
        assert_eq!(entity_id(&input["isBasedOn"]), Some(device.as_str()));
        let entity = graph
            .iter()
            .find(|entity| entity["@id"] == device.as_str())
            .expect("the device is a contextual entity");
        assert_eq!(entity["@type"], "IndividualProduct");
        assert_eq!(entity["identifier"], device_id().as_str());
        // An input from no device names none.
        let plain = build_crate_jsonld(
            &record,
            &spec,
            (record.created_by.realm_id, Some(document_id)),
            &BTreeMap::new(),
            None,
        );
        assert!(!plain.contains("IndividualProduct"));
    }

    fn launch() -> LaunchRecord {
        LaunchRecord {
            node_id: iroh::SecretKey::from_bytes(&[9; 32]).public(),
            executor_kind: "kubernetes".to_string(),
            location: "giessen".to_string(),
            resources: aruna_core::compute::GrantedResources {
                nano_cpus: Some(1_500_000_000),
                ram_bytes: Some(9 << 30),
                disk_bytes: None,
                workspace_bytes: Some(10 << 30),
                walltime_ms: Some(86_400_000),
            },
            workdir: Some("/work".to_string()),
            env: BTreeMap::from([
                ("API_TOKEN".to_string(), "hidden".to_string()),
                ("THREADS".to_string(), "8".to_string()),
            ]),
            started_at_ms: Some(1),
        }
    }

    #[test]
    fn names_execution_site() {
        // The executing node, its site, the granted limits and the environment are recorded.
        let (record, spec) = execution_record();
        let document_id = Ulid::from_bytes(record.job_id.to_bytes());
        let launch = launch();
        let jsonld: Value = serde_json::from_str(&build_crate_jsonld(
            &record,
            &spec,
            (record.created_by.realm_id, Some(document_id)),
            &BTreeMap::new(),
            Some(&launch),
        ))
        .unwrap();
        let graph = jsonld["@graph"].as_array().unwrap();
        let find = |id: &str| graph.iter().find(|entity| entity["@id"] == id).unwrap();
        let action = graph
            .iter()
            .find(|entity| entity["@type"] == "CreateAction")
            .unwrap();
        assert_eq!(entity_id(&action["location"]), Some(SITE_ID));
        let site = find(SITE_ID);
        assert_eq!(site["name"], "giessen");
        assert_eq!(site["identifier"], launch.node_id.to_string());
        assert_eq!(site["additionalType"], "kubernetes");
        assert_eq!(find("#cpu-cores")["value"], "1.5");
        assert_eq!(find("#ram-bytes")["value"], (9u64 << 30).to_string());
        assert_eq!(find("#working-directory")["value"], "/work");
        assert!(graph.iter().all(|entity| entity["@id"] != "#disk-bytes"));
        let properties: Vec<_> = action["additionalProperty"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(entity_id)
            .collect();
        assert!(properties.contains(&"#workspace-bucket"));
        assert!(properties.contains(&"#walltime-ms"));
        assert_eq!(find("#env-API%5FTOKEN")["value"], "hidden");
        assert_eq!(find("#env-THREADS")["value"], "8");
        assert_eq!(action["environment"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn meets_builtin_profile() {
        // The built-in shapes must accept what this generator writes.
        use crate::metadata::builtin::{BUILTIN_REVISION, builtin_shapes};
        use aruna_core::profile_shapes::ProfileShapes;
        use aruna_graph::ProfileShaclEngine;

        let (record, spec) = execution_record();
        // An input from a device carries its provenance entity, which the shapes must accept.
        let devices = BTreeMap::from([("inputs/in.txt".to_string(), device_id())]);
        let document_id = Ulid::from_bytes(record.job_id.to_bytes());
        let jsonld = build_crate_jsonld(
            &record,
            &spec,
            (record.created_by.realm_id, Some(document_id)),
            &devices,
            Some(&launch()),
        );
        let directory = tempfile::tempdir().unwrap();
        let engine = ProfileShaclEngine::open(directory.path()).unwrap();
        let report = engine
            .evaluate(
                &ProfileShapes {
                    graph_iri: format!("{CRATE_PROFILE_IRI}#shapes/{BUILTIN_REVISION}"),
                    sources: builtin_shapes(CRATE_PROFILE_IRI)
                        .unwrap()
                        .iter()
                        .map(|shapes| shapes.to_string())
                        .collect(),
                },
                &jsonld,
            )
            .unwrap();
        let blocking: Vec<_> = report
            .results
            .iter()
            .filter(|result| !result.severity.0.contains("Warning"))
            .collect();
        assert!(blocking.is_empty(), "{blocking:#?}");
        assert!(report.structural.is_empty(), "{:#?}", report.structural);
    }
}
