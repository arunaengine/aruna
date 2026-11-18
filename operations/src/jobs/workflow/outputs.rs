//! Exports the file outputs of a finished attempt into their S3 destinations, each under the
//! version reserved for it before the first write.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::effect::{ComputeAnswer, ComputeRequest, OutputBody, OutputFailure};
use aruna_core::compute::{ExecutorKind, FenceContext, has_wildcard};
use aruna_core::effects::Effect;
use aruna_core::errors::AuthorizationError;
use aruna_core::events::Event;
use aruna_core::id::NodeId;
use aruna_core::jobs::{JobEffect, JobEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{
    AttemptControl, ExecutionSpec, JobError, JobRecord, MAX_EXECUTION_OUTPUTS, OutputDestination,
    OutputObject, OutputSelection,
};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::placement::policy::PlacementPolicyRef;
use aruna_core::structs::storage::blob::{BucketInfo, object_permission_path};
use aruna_core::structs::storage::distribution::hint::SetHint;
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::origin::VersionOrigin;
use aruna_core::structs::storage::replication::ReplicationFailure;
use aruna_core::structs::storage::replication_queue::{
    ReplicateScopeInput, ReplicateScopeTarget, ReplicationMode,
};
use aruna_core::structs::storage::routing::{NodeRouting, RoutingSnapshot};
use tracing::debug;
use ulid::Ulid;

use super::compute::{attempt_call, compute_answer};
use super::credential::RunIdentity;
use super::workspace::{
    authorization_error, bucket_lookup_error, capture_cancelled, destination_of,
    distinct_selections, expand_selection, gate_error, gate_stopped, output_metadata,
    output_object, output_read_error, output_replication_error, output_stage_bucket,
    put_object_error, routing_error,
};
use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::groups::routing::{BucketRules, QuotaOperation, RoutingInputsError, SnapshotOperation};
use crate::jobs::store::JobMutationError;
use crate::jobs::store::job_calls::reserve_output_commits;
use crate::placement::policy::{GateContext, GateContextError, GateOperation};
use crate::replication::live_queue::{CompletePutInput, CompletePutOperation};
use crate::replication::managed_read::LocalUserOperation;
use crate::replication::source_rules::SourceAuthOperation;
use crate::replication::version_replication::{
    ReplicateScopeError, ReplicateScopeOperation, ReplicateScopeResult, SourceAuthorization,
    SourceAuthorizationError,
};
use crate::s3::bucket::create::{CreateBucketError, CreateBucketOperation};
use crate::s3::bucket::delete::{DeleteBucketError, DeleteBucketOperation};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::s3::object::delete::{
    DeleteObjectError, DeleteObjectInput, DeleteObjectOperation, DeleteObjectResult,
};
use crate::s3::object::put::{
    PutObjectConfig, PutObjectError, PutObjectInput, PutObjectOperation, PutObjectResult,
};

/// What an output capture works with.
#[derive(Clone, Debug, PartialEq)]
pub struct CaptureScope {
    pub kind: ExecutorKind,
    pub fence: FenceContext,
    pub spec: Arc<ExecutionSpec>,
    pub record: Arc<JobRecord>,
    pub run: RunIdentity,
    pub routing: NodeRouting,
    /// A job stop ends the capture. The cancel path captures what finished before the cancel,
    /// so it runs without this.
    pub until_stop: bool,
}

impl CaptureScope {
    fn auth(&self) -> AuthContext {
        AuthContext {
            user_id: self.run.user_id,
            realm_id: self.run.realm_id,
            path_restrictions: None,
            session: None,
        }
    }
}

fn no_backend() -> JobError {
    JobError::retryable("the attempt's backend is unavailable")
}

#[derive(Debug, PartialEq)]
enum PutStep {
    Stage,
    Bucket,
    Allowed,
    Config,
    Fetch,
    Routing,
    Gate(Box<RoutingSnapshot>),
    Put,
    Source,
    Quota(Box<SourceAuthorization>),
    Replicate,
    DeleteCopy,
    DeleteStage,
    Announce,
}

type PutNext = Result<Next<OutputObject>, JobError>;

/// Streams one declared output into its destination under its reserved version. The write
/// fences itself on that id, so a replayed capture resolves to the version it already created.
/// An output for another node is written to a staging bucket here and copied there.
#[derive(Debug, PartialEq)]
pub struct PutOutputOperation {
    scope: Arc<CaptureScope>,
    output: OutputSelection,
    destination_node: NodeId,
    version_id: Ulid,
    execution_id: Ulid,
    inherited: Vec<PlacementPolicyRef>,
    write_bucket: String,
    write_key: String,
    quota_ceiling: Option<u64>,
    body: Option<OutputBody>,
    size: u64,
    failure: OutputFailure,
    result: Option<PutObjectResult>,
    now_ms: u64,
    step: PutStep,
}

impl PutOutputOperation {
    fn new(
        scope: Arc<CaptureScope>,
        output: OutputSelection,
        destination_node: NodeId,
        (version_id, execution_id): (Ulid, Ulid),
        inherited: Vec<PlacementPolicyRef>,
    ) -> Self {
        Self {
            scope,
            output,
            destination_node,
            version_id,
            execution_id,
            inherited,
            write_bucket: String::new(),
            write_key: String::new(),
            quota_ceiling: None,
            body: None,
            size: 0,
            failure: OutputFailure::default(),
            result: None,
            now_ms: 0,
            step: PutStep::Stage,
        }
    }

    fn remote(&self) -> bool {
        self.destination_node != self.scope.run.node_id
    }

    fn call(&mut self, step: PutStep, call: IoCall) -> PutNext {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn destination(&self) -> (&str, &str) {
        let OutputDestination::S3 { bucket, key } = &self.output.destination;
        (bucket, key)
    }

    fn read_config(&mut self) -> PutNext {
        let read = GetConfigOperation::new(self.scope.run.realm_id);
        self.call(PutStep::Config, IoCall::child(read))
    }

    fn fetch(&mut self) -> PutNext {
        let scope = self.scope.clone();
        let request = ComputeRequest::FetchOutput {
            path: self.output.container_path.clone(),
            until_stop: scope.until_stop,
        };
        let call = attempt_call(&scope.kind, &scope.fence, request);
        self.call(PutStep::Fetch, call)
    }

    fn put(&mut self, routing: RoutingSnapshot, gate: Option<GateContext>) -> PutNext {
        let body = self.body.take().ok_or(IoError::Unfinished)?;
        let scope = self.scope.clone();
        let (_, key) = self.destination();
        let metadata = output_metadata(key);
        let mut operation = PutObjectOperation::new(PutObjectConfig {
            user_id: scope.run.user_id,
            group_id: scope.run.group_id,
            realm_id: scope.run.realm_id,
            node_id: scope.run.node_id,
            request: PutObjectInput {
                bucket: self.write_bucket.clone(),
                key: self.write_key.clone(),
                content_length: Some(self.size),
                body: Some(body.bytes),
            },
            expected_checksums: Vec::new(),
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: Some(self.version_id),
            quota_ceiling: self.quota_ceiling,
            routing,
        })
        .with_inherited_policies(self.inherited.clone())
        .with_metadata(metadata)
        .with_version_origin(Some(VersionOrigin::Job {
            run_id: Ulid::from_bytes(scope.run.job_id.to_bytes()),
            started_by: Some(scope.run.user_id).filter(|user| !user.is_nil()),
        }));
        if let Some(gate) = gate {
            operation = operation.with_gate(gate);
        }
        self.call(PutStep::Put, IoCall::child(operation))
    }

    fn finish(&mut self) -> PutNext {
        let result = self.result.take().ok_or(IoError::Unfinished)?;
        let (bucket, key) = self.destination();
        Ok(Next::Done(output_object(
            &self.output,
            self.destination_node,
            bucket,
            key,
            &result,
            self.execution_id,
        )))
    }

    fn put_done(&mut self, result: PutObjectResult) -> PutNext {
        let version_id = result.version_id;
        let replayed = result.replayed;
        self.result = Some(result);
        let scope = self.scope.clone();
        if self.remote() {
            let read = SourceAuthOperation::new(
                scope.auth(),
                scope.run.group_id,
                scope.record.owner_node_id,
            );
            return self.call(PutStep::Source, IoCall::child(read));
        }
        // A retried capture hands back a committed version; its write was already announced.
        if replayed {
            return self.finish();
        }
        let announce = CompletePutOperation::new(CompletePutInput {
            realm_id: scope.run.realm_id,
            node_id: scope.run.node_id,
            auth: scope.auth(),
            group_id: scope.run.group_id,
            bucket: self.write_bucket.clone(),
            key: self.write_key.clone(),
            version_id,
            size_bytes: self.size,
        });
        self.call(PutStep::Announce, IoCall::child(announce))
    }

    fn replicate(&mut self, source: SourceAuthorization, routing: NodeRouting) -> PutNext {
        let scope = self.scope.clone();
        let version_id = self.result.as_ref().ok_or(IoError::Unfinished)?.version_id;
        let (bucket, key) = self.destination();
        let (bucket, key) = (bucket.to_string(), key.to_string());
        let auth = scope.auth();
        let replicate = ReplicateScopeOperation::new(ReplicateScopeInput {
            bucket: self.write_bucket.clone(),
            target: ReplicateScopeTarget::Version {
                key: self.write_key.clone(),
                version_id,
            },
            target_node_id: self.destination_node,
            auth_context: auth.clone(),
            replicate_delete_markers: false,
            mode: ReplicationMode::OnDemand,
        })
        .with_routing(routing)
        .with_source_authorization(source)
        .with_destination(bucket, key, scope.record.owner_node_id, auth);
        self.call(PutStep::Replicate, IoCall::child(replicate))
    }

    fn delete_copy(&mut self) -> PutNext {
        let scope = self.scope.clone();
        let version_id = self.result.as_ref().ok_or(IoError::Unfinished)?.version_id;
        let delete = DeleteObjectOperation::new(DeleteObjectInput {
            bucket: self.write_bucket.clone(),
            key: self.write_key.clone(),
            version_id: Some(version_id),
            group_id: scope.run.group_id,
            realm_id: scope.run.realm_id,
            node_id: scope.record.owner_node_id,
            deleted_by: scope.run.user_id,
        });
        self.call(PutStep::DeleteCopy, IoCall::child(delete))
    }
}

impl Operation for PutOutputOperation {
    type Output = OutputObject;
    type Error = JobError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> PutNext {
        let scope = self.scope.clone();
        if self.remote() {
            self.write_bucket = output_stage_bucket(&scope.record);
            self.write_key = self.version_id.to_string();
            let info = BucketInfo {
                group_id: scope.run.group_id,
                created_at: SystemTime::UNIX_EPOCH + Duration::from_millis(self.now_ms),
                created_by: scope.run.user_id,
                cors_configuration: None,
                storage_routing: Vec::new(),
                placement_policies: Vec::new(),
                placement_policy_generation: 0,
                compression: Compression::Off,
                distribution: None,
            };
            let create = CreateBucketOperation::new(self.write_bucket.clone(), info);
            return self.call(PutStep::Stage, IoCall::child(create));
        }
        let (bucket, key) = self.destination();
        let (bucket, key) = (bucket.to_string(), key.to_string());
        self.write_bucket = bucket.clone();
        self.write_key = key;
        let read = GetBucketOperation::new(bucket);
        self.call(PutStep::Bucket, IoCall::child(read))
    }

    fn next(&mut self, result: IoResult) -> PutNext {
        let scope = self.scope.clone();
        match std::mem::replace(&mut self.step, PutStep::Announce) {
            PutStep::Stage => match result.child::<BucketInfo, CreateBucketError>()? {
                Ok(_) | Err(CreateBucketError::BucketAlreadyExists) => self.read_config(),
                Err(error) => Err(JobError::retryable(format!(
                    "output staging bucket create failed: {error}"
                ))),
            },
            PutStep::Bucket => {
                let info = result
                    .child::<BucketInfo, GetBucketError>()?
                    .map_err(|error| bucket_lookup_error("output", error))?;
                if info.group_id != scope.run.group_id {
                    return Err(JobError::permanent(
                        "output bucket is outside the execution group",
                    ));
                }
                let (bucket, key) = self.destination();
                let check = CheckPermissionsOperation::new(CheckPermissionsConfig {
                    auth_context: scope.auth(),
                    path: object_permission_path(
                        scope.run.realm_id,
                        scope.run.group_id,
                        scope.run.node_id,
                        bucket,
                        key,
                    ),
                    required_permission: Permission::WRITE,
                });
                self.call(PutStep::Allowed, IoCall::child(check))
            }
            PutStep::Allowed => {
                let allowed = result
                    .child::<bool, AuthorizationError>()?
                    .map_err(|error| authorization_error("output", error))?;
                if !allowed {
                    let (bucket, key) = self.destination();
                    return Err(JobError::permanent(format!(
                        "output {bucket}/{key} access denied"
                    )));
                }
                self.read_config()
            }
            PutStep::Config => {
                let config = result
                    .child::<RealmConfigDocument, GetConfigError>()?
                    .map_err(|error| {
                        JobError::retryable(format!("output quota lookup failed: {error}"))
                    })?;
                self.quota_ceiling = config.quota.effective_group_ceiling(&scope.run.group_id);
                self.fetch()
            }
            PutStep::Fetch => {
                let body = match compute_answer(result)? {
                    ComputeAnswer::Output(body) => {
                        body.map_err(|error| output_read_error(&error))?
                    }
                    ComputeAnswer::Stopped => return Err(capture_cancelled()),
                    _ => return Err(no_backend()),
                };
                self.size = body.size;
                self.failure = body.failure.clone();
                self.body = Some(body);
                let routing = SnapshotOperation::new(
                    &scope.routing,
                    scope.run.group_id,
                    BucketRules::Named(self.write_bucket.clone()),
                );
                self.call(PutStep::Routing, IoCall::child(routing))
            }
            PutStep::Routing => {
                let routing = result
                    .child::<RoutingSnapshot, RoutingInputsError>()?
                    .map_err(|error| routing_error("output write", error))?;
                let gate = GateOperation::new(scope.run.realm_id, self.now_ms);
                self.call(PutStep::Gate(Box::new(routing)), IoCall::child(gate))
            }
            PutStep::Gate(routing) => {
                let gate = result
                    .child::<Option<GateContext>, GateContextError>()?
                    .map_err(|error| gate_error("output write", error))?;
                if gate.as_ref().is_some_and(|gate| !gate.admitting) {
                    return Err(gate_stopped("output write"));
                }
                self.put(*routing, gate)
            }
            PutStep::Put => {
                // A failure caused by the container-side stream keeps its own classification.
                let put = result.child::<PutObjectResult, PutObjectError>()?;
                let put = put.map_err(|error| match self.failure.take() {
                    Some(backend) => output_read_error(&backend),
                    None => put_object_error("output write", error),
                })?;
                self.put_done(put)
            }
            PutStep::Source => {
                let source = result
                    .child::<SourceAuthorization, SourceAuthorizationError>()?
                    .map_err(|error| match error {
                        SourceAuthorizationError::Denied => {
                            JobError::permanent("output staging read access denied")
                        }
                        SourceAuthorizationError::Unavailable(error) => JobError::retryable(
                            format!("output staging authorization failed: {error}"),
                        ),
                    })?;
                let quota = QuotaOperation::new(scope.routing.clone());
                self.call(PutStep::Quota(Box::new(source)), IoCall::child(quota))
            }
            PutStep::Quota(source) => {
                let routing = result
                    .child::<NodeRouting, RoutingInputsError>()?
                    .map_err(|error| routing_error("output copy", error))?;
                self.replicate(*source, routing)
            }
            PutStep::Replicate => {
                let copied = result
                    .child::<ReplicateScopeResult, ReplicateScopeError>()?
                    .map_err(|error| {
                        output_replication_error(error.failure(), error.to_string())
                    })?;
                if copied.failed > 0 || copied.replicated == 0 && copied.skipped == 0 {
                    return Err(output_replication_error(
                        copied.failure.unwrap_or(ReplicationFailure::Other),
                        copied
                            .last_error
                            .unwrap_or_else(|| "output copy made no progress".to_string()),
                    ));
                }
                self.delete_copy()
            }
            PutStep::DeleteCopy => match result.child::<DeleteObjectResult, DeleteObjectError>()? {
                Ok(_) | Err(DeleteObjectError::NoSuchVersion) => {
                    let delete = DeleteBucketOperation::new(self.write_bucket.clone());
                    self.call(PutStep::DeleteStage, IoCall::child(delete))
                }
                Err(error) => Err(JobError::retryable(format!(
                    "output staging delete failed: {error}"
                ))),
            },
            PutStep::DeleteStage => match result.child::<(), DeleteBucketError>()? {
                Ok(()) | Err(DeleteBucketError::NotFound) => self.finish(),
                Err(error) => Err(JobError::retryable(format!(
                    "output staging bucket delete failed: {error}"
                ))),
            },
            PutStep::Announce => {
                // Live replication and watchers are best effort after a committed put.
                let _ = result.child::<(), IoError>()?;
                self.finish()
            }
        }
    }
}

/// The first input the job holds no captured form of.
pub(super) fn uncaptured_input<'a>(spec: &'a ExecutionSpec, record: &JobRecord) -> Option<&'a str> {
    (spec.inputs.iter())
        .find(|input| {
            !(record.captured_inputs.iter())
                .any(|captured| captured.destination_key == input.dest_key)
        })
        .map(|input| input.dest_key.as_str())
}

/// Union of the refs the captured inputs carry, so an output is never less constrained than what
/// produced it, also on a node that holds no copy of an input.
pub(super) fn input_refs(
    spec: &ExecutionSpec,
    record: &JobRecord,
) -> Result<Vec<PlacementPolicyRef>, JobError> {
    let refs: Vec<PlacementPolicyRef> = (record.captured_inputs.iter())
        .filter(|captured| {
            (spec.inputs.iter()).any(|input| input.dest_key == captured.destination_key)
        })
        .flat_map(|captured| captured.policies.iter().copied())
        .collect();
    PlacementPolicyRef::canonical_set(&refs)
        .map_err(|error| JobError::permanent(format!("input policy refs invalid: {error}")))
}

#[derive(Debug, PartialEq)]
enum CaptureStep {
    Resolve(OutputSelection),
    Hint(String),
    Reserve,
    Role,
    Stops,
    Put,
}

type CaptureNext = Result<Next<Vec<OutputObject>>, JobError>;

/// Exports every declared file output under one write-ahead commit reservation, so an
/// interrupted capture replays into the same versions.
#[derive(Debug, PartialEq)]
pub struct CaptureOutputsOperation {
    scope: Arc<CaptureScope>,
    declared: VecDeque<OutputSelection>,
    selections: Vec<OutputSelection>,
    destinations: Vec<(NodeId, String, String)>,
    buckets: VecDeque<String>,
    hints: BTreeMap<(NodeId, String), SetHint>,
    control: Option<AttemptControl>,
    inherited: Vec<PlacementPolicyRef>,
    queue: VecDeque<(OutputSelection, (NodeId, String, String))>,
    outputs: Vec<OutputObject>,
    step: CaptureStep,
}

impl CaptureOutputsOperation {
    pub fn new(scope: CaptureScope) -> Self {
        let declared = scope.spec.file_outputs.iter().cloned().collect();
        Self {
            scope: Arc::new(scope),
            declared,
            selections: Vec::new(),
            destinations: Vec::new(),
            buckets: VecDeque::new(),
            hints: BTreeMap::new(),
            control: None,
            inherited: Vec::new(),
            queue: VecDeque::new(),
            outputs: Vec::new(),
            step: CaptureStep::Reserve,
        }
    }

    fn call(&mut self, step: CaptureStep, call: IoCall) -> CaptureNext {
        self.step = step;
        Ok(Next::Call(call))
    }

    /// Expands the next declared output. A wildcard path is expanded against the terminal
    /// attempt; a literal path is uploaded as declared and fails the job when it is missing.
    fn resolve_next(&mut self) -> CaptureNext {
        while let Some(output) = self.declared.pop_front() {
            if !has_wildcard(&output.container_path) {
                self.selections.push(output);
                continue;
            }
            let scope = self.scope.clone();
            let request = ComputeRequest::ListOutputs {
                pattern: output.container_path.clone(),
                until_stop: scope.until_stop,
            };
            let call = attempt_call(&scope.kind, &scope.fence, request);
            return self.call(CaptureStep::Resolve(output), call);
        }
        self.plan()
    }

    fn plan(&mut self) -> CaptureNext {
        let selections = distinct_selections(std::mem::take(&mut self.selections))?;
        if selections.len() > MAX_EXECUTION_OUTPUTS {
            return Err(JobError::permanent(format!(
                "output manifest exceeds {MAX_EXECUTION_OUTPUTS} objects"
            )));
        }
        self.destinations = selections
            .iter()
            .map(destination_of)
            .collect::<Result<_, _>>()?;
        self.selections = selections;
        let node_id = self.scope.run.node_id;
        let local: BTreeSet<String> = (self.destinations.iter())
            .filter(|(node, ..)| *node == node_id)
            .map(|(_, bucket, _)| bucket.clone())
            .collect();
        self.buckets = local.into_iter().collect();
        self.next_hint()
    }

    /// Reads the set hint of the next local destination bucket. A remote destination has no
    /// hint here, so a write of its reserved id into a distributed bucket is refused.
    fn next_hint(&mut self) -> CaptureNext {
        let Some(bucket) = self.buckets.pop_front() else {
            let reserve = reserve_output_commits(
                self.scope.run.job_id,
                self.destinations.clone(),
                std::mem::take(&mut self.hints),
            );
            return self.call(CaptureStep::Reserve, IoCall::child(reserve));
        };
        let read = GetBucketOperation::new(bucket.clone());
        self.call(CaptureStep::Hint(bucket), IoCall::child(read))
    }

    fn reserved(&mut self, control: AttemptControl) -> CaptureNext {
        if control.attempt_epoch != self.scope.fence.attempt_epoch {
            return Err(JobError::retryable(
                "attempt fence moved during output capture",
            ));
        }
        let selections = std::mem::take(&mut self.selections);
        let destinations = std::mem::take(&mut self.destinations);
        self.queue = selections.into_iter().zip(destinations).collect();
        self.control = Some(control);
        self.policies()
    }

    /// A device captures no inputs and may never read governed content, so its inputs carry no
    /// refs; a realm node must not guess them.
    fn policies(&mut self) -> CaptureNext {
        let scope = self.scope.clone();
        if uncaptured_input(&scope.spec, &scope.record).is_some() {
            let role = LocalUserOperation::new(scope.run.realm_id);
            return self.call(CaptureStep::Role, IoCall::child(role));
        }
        self.inherit()
    }

    fn inherit(&mut self) -> CaptureNext {
        self.inherited = input_refs(&self.scope.spec, &self.scope.record)?;
        self.next_output()
    }

    fn next_output(&mut self) -> CaptureNext {
        if self.queue.is_empty() {
            return Ok(Next::Done(std::mem::take(&mut self.outputs)));
        }
        if self.scope.until_stop {
            let stops = IoCall::Effect(Box::new(Effect::Job(JobEffect::Stops)));
            return self.call(CaptureStep::Stops, stops);
        }
        self.put_next()
    }

    fn put_next(&mut self) -> CaptureNext {
        let control = self.control.as_ref().ok_or(IoError::Unfinished)?;
        let execution_id = control.execution_id;
        let Some((output, (node, bucket, key))) = self.queue.pop_front() else {
            return Ok(Next::Done(std::mem::take(&mut self.outputs)));
        };
        let version_id = (control.output_commits.iter())
            .find(|commit| commit.node_id == node && commit.bucket == bucket && commit.key == key)
            .map(|commit| commit.version_id)
            .ok_or_else(|| JobError::retryable("output commit reservation lost"))?;
        let put = PutOutputOperation::new(
            self.scope.clone(),
            output,
            node,
            (version_id, execution_id),
            self.inherited.clone(),
        );
        self.call(CaptureStep::Put, IoCall::child(put))
    }
}

impl Operation for CaptureOutputsOperation {
    type Output = Vec<OutputObject>;
    type Error = JobError;

    fn first(&mut self) -> CaptureNext {
        self.resolve_next()
    }

    fn next(&mut self, result: IoResult) -> CaptureNext {
        match std::mem::replace(&mut self.step, CaptureStep::Put) {
            CaptureStep::Resolve(output) => {
                let mut matched = match compute_answer(result)? {
                    ComputeAnswer::Outputs(matched) => {
                        matched.map_err(|error| output_read_error(&error))?
                    }
                    ComputeAnswer::Stopped => return Err(capture_cancelled()),
                    _ => return Err(no_backend()),
                };
                matched.sort();
                matched.dedup();
                debug!(
                    pattern = %output.container_path,
                    matched = matched.len(),
                    "Expanded wildcard output"
                );
                let inputs = &self.scope.spec.inputs;
                let expanded = expand_selection(&output, matched, inputs)?;
                self.selections.extend(expanded);
                self.resolve_next()
            }
            CaptureStep::Hint(bucket) => match result.child::<BucketInfo, GetBucketError>()? {
                Ok(info) => {
                    if let Some(binding) = info.distribution {
                        let key = (self.scope.run.node_id, bucket);
                        self.hints.insert(key, binding.hint());
                    }
                    self.next_hint()
                }
                Err(GetBucketError::NotFound) => self.next_hint(),
                Err(error) => Err(JobError::retryable(format!(
                    "output bucket {bucket} could not be read: {error}"
                ))),
            },
            CaptureStep::Reserve => {
                let control = result
                    .child::<AttemptControl, JobMutationError>()?
                    .map_err(|error| {
                        JobError::retryable(format!("output commit reservation failed: {error}"))
                    })?;
                self.reserved(control)
            }
            CaptureStep::Role => {
                if !result.child::<bool, IoError>()?.unwrap_or(false) {
                    let missing = uncaptured_input(&self.scope.spec, &self.scope.record);
                    let key = missing.unwrap_or_default();
                    return Err(JobError::permanent(format!(
                        "input `{key}` has no captured inputs to inherit refs from"
                    )));
                }
                self.inherit()
            }
            CaptureStep::Stops => match result.event()? {
                Event::Job(JobEvent::Stops(stops)) if stops.cancelled => Err(capture_cancelled()),
                _ => self.put_next(),
            },
            CaptureStep::Put => {
                let output = result.child::<OutputObject, JobError>()??;
                self.outputs.push(output);
                self.next_output()
            }
        }
    }
}

#[cfg(test)]
#[path = "outputs_tests.rs"]
mod tests;
