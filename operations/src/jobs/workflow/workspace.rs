use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::Path;
use std::time::{Duration, SystemTime};

use aruna_compute::ExecutorBackend;
use aruna_core::compute::{
    BackendError, FenceContext, MAX_OUTPUT_MATCHES, MAX_TRANSFER_BYTES, S3Mount, TaskInput,
    has_wildcard, output_suffix,
};
use aruna_core::errors::{AuthorizationError, StorageError};
use aruna_core::stream::BackendStream;
use aruna_core::structs::{
    AttemptControl, AuthContext, BackendLocation, BucketInfo, CollisionPolicy, ExecutionSpec,
    InputMode, InputSelection, InputSource, JobError, JobInputFact, JobRecord,
    MAX_EXECUTION_OUTPUTS, OutputDestination, OutputObject, OutputSelection, PathRestriction,
    Permission, PlacementPolicyRef, UserAccess, VersionedObjectArn, WorkspaceMode,
    blob_bucket_permission_path, blob_group_permission_path, blob_object_permission_path,
    ensure_confined_relative_path, workspace_credential_id,
};
use aruna_core::types::NodeId;
use futures_util::StreamExt;
use std::sync::Arc;
use ulid::Ulid;

use super::DEFAULT_WALLTIME;
use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::driver::{
    DriverContext, GateContextError, RoutingInputsError, drive, gate_context, now_ms,
    quota_marked_routing, routing_snapshot,
};
use crate::get_realm_config::GetRealmConfigOperation;
use crate::jobs::lifecycle::stage::stage_error;
use crate::jobs::store::reserve_output_commits;
use crate::replication::bao_read::{BaoReadError, BaoReadOutput, local_is_user, managed_read};
use crate::replication::protocol::{
    BaoReadRefusal, BaoReadRequest, BaoReadTarget, ReplicationMode,
};
use crate::replication::version_replication::{
    ReplicateScopeInput, ReplicateScopeOperation, ReplicateScopeTarget, SourceAuthorization,
    SourceAuthorizationError,
};
use crate::s3::create_bucket::{CreateBucketError, CreateBucketOperation};
use crate::s3::create_user_access::{CreateUserAccessConfig, CreateUserAccessOperation};
use crate::s3::delete_bucket::{DeleteBucketError, DeleteBucketOperation};
use crate::s3::delete_object::{DeleteObjectError, DeleteObjectInput, DeleteObjectOperation};
use crate::s3::get_bucket_info::{GetBucketInfoError, GetBucketInfoOperation};
use crate::s3::get_object::{GetObjectError, GetObjectInput, GetObjectOperation};
use crate::s3::get_user_access::{GetUserAccessError, GetUserAccessOperation};
use crate::s3::head_object::{HeadObjectError, HeadObjectInput, HeadObjectOperation};
use crate::s3::list_objects_v2::{ListObjectsV2Input, ListObjectsV2Operation};
use crate::s3::put_object::{
    PutObjectConfig, PutObjectError, PutObjectInput, PutObjectOperation, PutObjectResult,
};

/// Credential lifetime past the walltime so a slow finalize still authorizes.
const CREDENTIAL_SLACK: Duration = Duration::from_secs(6 * 60 * 60);

/// Minted workspace S3 credential handed to the container.
pub struct WorkspaceCredential {
    pub access_key: String,
    pub secret: String,
}

pub async fn ensure_group_write(
    context: &DriverContext,
    spec: &ExecutionSpec,
    record: &JobRecord,
    node_id: NodeId,
) -> Result<(), JobError> {
    let allowed = Box::pin(drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: AuthContext {
                user_id: record.created_by,
                realm_id: record.created_by.realm_id,
                path_restrictions: None,
                session: None,
            },
            path: blob_group_permission_path(record.created_by.realm_id, spec.group_id, node_id),
            required_permission: Permission::WRITE,
        }),
        context,
    ))
    .await
    .map_err(|error| match error {
        AuthorizationError::InvalidRealmId
        | AuthorizationError::InvalidGroupId
        | AuthorizationError::GroupNotFound
        | AuthorizationError::AuthDocNotFound => {
            JobError::permanent("workspace write access denied")
        }
        other => JobError::retryable(format!("workspace authorization failed: {other}")),
    })?;
    if allowed {
        Ok(())
    } else {
        Err(JobError::permanent("workspace write access denied"))
    }
}

/// Create the durable run bucket `ws-{jobid}` under the job's group. Idempotent
/// across attempts: an already-existing bucket is accepted.
pub async fn ensure_workspace_bucket(
    context: &DriverContext,
    spec: &ExecutionSpec,
    record: &JobRecord,
    node_id: NodeId,
    bucket: &str,
) -> Result<(), JobError> {
    if record.workspace_mode == WorkspaceMode::Existing {
        let info = Box::pin(drive(
            GetBucketInfoOperation::new(bucket.to_string()),
            context,
        ))
        .await
        .and_then(|result| result.transpose())
        .map_err(|error| bucket_lookup_error("workspace", error))?
        .ok_or_else(|| JobError::permanent("existing workspace bucket not found"))?;
        if info.group_id != spec.group_id {
            return Err(JobError::permanent(
                "existing workspace bucket is outside the execution group",
            ));
        }
        let allowed = Box::pin(drive(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: AuthContext {
                    user_id: record.created_by,
                    realm_id: record.created_by.realm_id,
                    path_restrictions: None,
                    session: None,
                },
                path: blob_bucket_permission_path(
                    record.created_by.realm_id,
                    spec.group_id,
                    node_id,
                    bucket,
                ),
                required_permission: Permission::WRITE,
            }),
            context,
        ))
        .await
        .map_err(|error| authorization_error("workspace", error))?;
        return if allowed {
            Ok(())
        } else {
            Err(JobError::permanent("workspace write access denied"))
        };
    }
    let bucket_info = BucketInfo {
        group_id: spec.group_id,
        created_at: SystemTime::now(),
        created_by: record.created_by,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
    };
    match Box::pin(drive(
        CreateBucketOperation::new(bucket.to_string(), bucket_info),
        context,
    ))
    .await
    {
        Ok(_) => Ok(()),
        Err(CreateBucketError::BucketAlreadyExists) => Ok(()),
        Err(error) => Err(JobError::retryable(format!(
            "workspace bucket create failed: {error}"
        ))),
    }
}

/// Mint a path-restricted `UserAccess` confined to the workspace bucket, issued by
/// the serving node so the container's SigV4 requests authorize locally.
pub async fn mint_workspace_credential(
    context: &DriverContext,
    spec: &ExecutionSpec,
    record: &JobRecord,
    node_id: NodeId,
    bucket: &str,
) -> Result<WorkspaceCredential, JobError> {
    ensure_group_write(context, spec, record, node_id).await?;
    let realm_id = record.created_by.realm_id;
    // WRITE on the bucket and its subtree also satisfies READ without matching siblings.
    let bucket_path = blob_bucket_permission_path(realm_id, spec.group_id, node_id, bucket);
    let restrictions = vec![
        PathRestriction {
            pattern: bucket_path.clone(),
            permission: Permission::WRITE,
        },
        PathRestriction {
            pattern: format!("{bucket_path}/**"),
            permission: Permission::WRITE,
        },
    ];
    mint_credential(context, spec, record, node_id, restrictions).await
}

/// Mint one read-only credential restricted to the input buckets mounted by a job.
pub async fn mint_input_credential(
    context: &DriverContext,
    spec: &ExecutionSpec,
    record: &JobRecord,
    node_id: NodeId,
    buckets: &BTreeSet<String>,
) -> Result<WorkspaceCredential, JobError> {
    let realm_id = record.created_by.realm_id;
    let restrictions = buckets
        .iter()
        .flat_map(|bucket| {
            let path = blob_bucket_permission_path(realm_id, spec.group_id, node_id, bucket);
            [
                PathRestriction {
                    pattern: path.clone(),
                    permission: Permission::READ,
                },
                PathRestriction {
                    pattern: format!("{path}/**"),
                    permission: Permission::READ,
                },
            ]
        })
        .collect();
    mint_credential(context, spec, record, node_id, restrictions).await
}

async fn mint_credential(
    context: &DriverContext,
    spec: &ExecutionSpec,
    record: &JobRecord,
    node_id: NodeId,
    restrictions: Vec<PathRestriction>,
) -> Result<WorkspaceCredential, JobError> {
    // Reject an oversized mount set permanently before any attempt intent so a
    // job cannot loop retrying a credential the evaluator would reject.
    if let Err(error) = aruna_core::permission_path::validate_restriction_limits(&restrictions) {
        return Err(JobError::permanent(format!(
            "workspace credential restrictions invalid: {error}"
        )));
    }
    let key_id = workspace_credential_id(record.job_id);
    let access_key = UserAccess::build_access_key(&key_id).map_err(|error| {
        JobError::permanent(format!("workspace credential key failed: {error}"))
    })?;
    // The credential is issued and consumed on this node, so its secret seals
    // and unseals with this node's issuer-local key.
    let seal_key = context
        .net_handle
        .as_ref()
        .map(|net| net.credential_seal_key())
        .ok_or_else(|| JobError::permanent("workspace credential needs a net handle"))?;
    match Box::pin(drive(
        GetUserAccessOperation::new(access_key.clone()),
        context,
    ))
    .await
    {
        Ok(Some(Ok(access))) => {
            let matches_job = access.access_key == access_key
                && access.user_identity == record.created_by
                && access.group_id == spec.group_id
                && access.issued_by == *node_id.as_bytes()
                && access.path_restrictions == Some(restrictions.clone());
            if !matches_job || access.is_revoked() {
                return Err(JobError::permanent("workspace credential is invalid"));
            }
            if !access.is_expired(SystemTime::now()) {
                let secret = access.open_secret(&seal_key).map_err(|error| {
                    JobError::permanent(format!("workspace credential unseal failed: {error}"))
                })?;
                return Ok(WorkspaceCredential {
                    access_key: access.access_key,
                    secret,
                });
            }
            if record.attempt_intent.is_some() {
                return Err(JobError::permanent("workspace credential expired"));
            }
        }
        Ok(None)
        | Ok(Some(Err(GetUserAccessError::NotFound)))
        | Err(GetUserAccessError::NotFound) => {}
        Ok(Some(Err(error))) | Err(error) => {
            return Err(JobError::retryable(format!(
                "workspace credential lookup failed: {error}"
            )));
        }
    }
    let walltime = spec
        .resources
        .max_walltime_ms
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_WALLTIME);
    let expiry = SystemTime::now() + walltime + CREDENTIAL_SLACK;
    let (_, secret, access) = Box::pin(drive(
        CreateUserAccessOperation::new_with_key(
            CreateUserAccessConfig {
                user_identity: record.created_by,
                group_id: spec.group_id,
                expiry,
                path_restrictions: Some(restrictions),
                issued_by: *node_id.as_bytes(),
            },
            key_id,
            seal_key,
        ),
        context,
    ))
    .await
    .map_err(|error| JobError::retryable(format!("workspace credential mint failed: {error}")))?
    .map_err(|error| JobError::retryable(format!("workspace credential mint failed: {error}")))?;
    Ok(WorkspaceCredential {
        access_key: access.access_key,
        secret: secret.expose().to_string(),
    })
}

/// Validate mounted inputs and resolve the exact S3 objects exposed to the task.
pub async fn prepare_mounts(
    context: &DriverContext,
    spec: &ExecutionSpec,
    record: &JobRecord,
    node_id: NodeId,
) -> Result<Vec<S3Mount>, JobError> {
    let mut mounts = Vec::with_capacity(spec.inputs.len());
    for input in &spec.inputs {
        if input.mode != InputMode::Mount {
            return Err(JobError::permanent("mounted job contains a snapshot input"));
        }
        let InputSource::S3 {
            bucket,
            key,
            version_id,
        } = &input.source;
        if version_id.is_some() {
            return Err(JobError::permanent(
                "mounted input versions are not supported",
            ));
        }
        ensure_confined_relative_path(Path::new(key))
            .map_err(|error| JobError::permanent(format!("invalid mounted input key: {error}")))?;
        let path = input
            .container_path
            .clone()
            .ok_or_else(|| JobError::permanent("mounted input has no container path"))?;
        let bucket_info = Box::pin(drive(GetBucketInfoOperation::new(bucket.clone()), context))
            .await
            .and_then(|result| result.transpose())
            .map_err(|error| bucket_lookup_error("input", error))?
            .ok_or_else(|| JobError::permanent(format!("input bucket {bucket} not found")))?;
        if bucket_info.group_id != spec.group_id {
            return Err(JobError::permanent(
                "input bucket is outside the execution group",
            ));
        }
        let allowed = Box::pin(drive(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: AuthContext {
                    user_id: record.created_by,
                    realm_id: record.created_by.realm_id,
                    path_restrictions: None,
                    session: None,
                },
                path: blob_object_permission_path(
                    record.created_by.realm_id,
                    spec.group_id,
                    node_id,
                    bucket,
                    key,
                ),
                required_permission: Permission::READ,
            }),
            context,
        ))
        .await
        .map_err(|error| authorization_error("input", error))?;
        if !allowed {
            return Err(JobError::permanent(format!(
                "input {bucket}/{key} access denied"
            )));
        }
        match Box::pin(drive(
            HeadObjectOperation::new(HeadObjectInput {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: None,
            }),
            context,
        ))
        .await
        .and_then(|result| result.transpose())
        {
            Ok(Some(_)) => {}
            Ok(None)
            | Err(
                HeadObjectError::NoSuchKey
                | HeadObjectError::NoSuchVersion
                | HeadObjectError::DeleteMarker,
            ) => {
                return Err(JobError::permanent(format!(
                    "input {bucket}/{key} not found"
                )));
            }
            Err(error) => {
                return Err(JobError::retryable(format!("input lookup failed: {error}")));
            }
        }
        mounts.push(S3Mount {
            bucket: bucket.clone(),
            key: key.clone(),
            path,
        });
    }
    Ok(mounts)
}

/// Snapshot each declared input into the workspace bucket by copying resolved
/// bytes (internal GetObject -> PutObject). v1 supports internal S3 sources only.
pub async fn stage_inputs(
    context: &DriverContext,
    spec: &ExecutionSpec,
    record: &JobRecord,
    bucket: &str,
    node_id: NodeId,
) -> Result<(), JobError> {
    for input in &spec.inputs {
        Box::pin(stage_one_input(
            context, spec, record, bucket, node_id, input,
        ))
        .await?;
    }
    Ok(())
}

/// Open one un-consumed stream per staged input; bytes flow only when the
/// backend uploads them, so peak memory stays bounded by a chunk.
pub async fn load_inputs(
    context: &DriverContext,
    spec: &ExecutionSpec,
    record: &JobRecord,
    bucket: &str,
    node_id: NodeId,
) -> Result<Vec<TaskInput>, JobError> {
    let mut files = Vec::new();
    let mut total_bytes = 0u64;
    for input in &spec.inputs {
        let Some(path) = input.container_path.clone() else {
            continue;
        };
        let get = Box::pin(drive(
            GetObjectOperation::new(GetObjectInput {
                bucket: bucket.to_string(),
                key: input.dest_key.clone(),
                version_id: None,
                range: None,
                group_id: spec.group_id,
                user_identity: record.created_by,
                node_id,
            }),
            context,
        ))
        .await
        .and_then(|result| result.transpose())
        .map_err(staged_input_error)?
        .ok_or_else(|| JobError::permanent(format!("staged input {} missing", input.dest_key)))?;
        let size = get
            .location
            .as_ref()
            .map(|location| location.blob_size)
            .or_else(|| {
                get.source_metadata
                    .as_ref()
                    .map(|metadata| metadata.content_length)
            })
            .ok_or_else(|| {
                JobError::retryable(format!("staged input {} has no size", input.dest_key))
            })?;
        total_bytes = total_bytes
            .checked_add(size)
            .filter(|total| *total <= MAX_TRANSFER_BYTES)
            .ok_or_else(|| JobError::permanent("staged inputs exceed transfer limit"))?;
        let stream = get.blob.map(|chunk| chunk.map_err(std::io::Error::other));
        files.push(TaskInput::from_workspace(
            path,
            input.dest_key.clone(),
            size,
            Box::pin(stream),
        ));
    }
    Ok(files)
}

/// Export every declared file output under one write-ahead commit reservation, so
/// an interrupted capture replays into the same versions instead of new ones.
pub async fn capture_outputs(
    context: &DriverContext,
    backend: &Arc<dyn ExecutorBackend>,
    fence: &FenceContext,
    spec: &ExecutionSpec,
    record: &JobRecord,
    node_id: NodeId,
) -> Result<Vec<OutputObject>, JobError> {
    let mut selections = Vec::with_capacity(spec.file_outputs.len());
    for declared in &spec.file_outputs {
        selections.extend(resolve_output(backend, fence, declared).await?);
    }
    if selections.len() > MAX_EXECUTION_OUTPUTS {
        return Err(JobError::permanent(format!(
            "output manifest exceeds {MAX_EXECUTION_OUTPUTS} objects"
        )));
    }
    let destinations: Vec<(NodeId, String, String)> = selections
        .iter()
        .map(destination_of)
        .collect::<Result<_, _>>()?;
    let control = reserve_output_commits(&context.storage_handle, record.job_id, &destinations)
        .await
        .map_err(|error| {
            JobError::retryable(format!("output commit reservation failed: {error}"))
        })?;
    if control.attempt_epoch != fence.attempt_epoch {
        return Err(JobError::retryable(
            "attempt fence moved during output capture",
        ));
    }
    let reserved: HashMap<(NodeId, &str, &str), Ulid> = control
        .output_commits
        .iter()
        .map(|commit| {
            (
                (commit.node_id, commit.bucket.as_str(), commit.key.as_str()),
                commit.version_id,
            )
        })
        .collect();
    // Plan section 10: an output inherits the union of its inputs' refs and the
    // destination default, so a result can never be less constrained than what
    // produced it.
    let inherited = Box::pin(staged_input_policies(context, spec, record)).await?;
    let mut outputs = Vec::with_capacity(selections.len());
    for (selection, (destination_node_id, bucket, key)) in selections.iter().zip(&destinations) {
        let Some(version_id) = reserved
            .get(&(*destination_node_id, bucket.as_str(), key.as_str()))
            .copied()
        else {
            return Err(JobError::retryable("output commit reservation lost"));
        };
        outputs.push(
            Box::pin(put_file_output(
                context,
                backend,
                fence,
                spec,
                record,
                node_id,
                selection,
                version_id,
                control.execution_id,
                &inherited,
            ))
            .await?,
        );
    }
    Ok(outputs)
}

/// Union of the refs every staged input carries. Read from the workspace copies
/// themselves, so a restarted capture inherits exactly what staging sealed.
async fn staged_input_policies(
    context: &DriverContext,
    spec: &ExecutionSpec,
    record: &JobRecord,
) -> Result<Vec<PlacementPolicyRef>, JobError> {
    let bucket = super::job_bucket(record);
    if bucket.is_empty() {
        return Ok(Vec::new());
    }
    let mut refs = Vec::new();
    for input in &spec.inputs {
        let head = Box::pin(drive(
            HeadObjectOperation::new(HeadObjectInput {
                bucket: bucket.clone(),
                key: input.dest_key.clone(),
                version_id: None,
            }),
            context,
        ))
        .await
        .and_then(|result| result.transpose());
        match head {
            Ok(Some(result)) => refs.extend(result.source_policies),
            Ok(None)
            | Err(
                HeadObjectError::NoSuchKey
                | HeadObjectError::NoSuchVersion
                | HeadObjectError::DeleteMarker,
            ) => {}
            Err(error) => {
                return Err(JobError::retryable(format!(
                    "input policy lookup failed: {error}"
                )));
            }
        }
    }
    PlacementPolicyRef::canonical_set(&refs)
        .map_err(|error| JobError::permanent(format!("input policy refs invalid: {error}")))
}

fn destination_of(selection: &OutputSelection) -> Result<(NodeId, String, String), JobError> {
    let OutputDestination::S3 { bucket, key } = &selection.destination;
    let node_id = selection
        .destination_node_id
        .ok_or_else(|| JobError::permanent("output destination endpoint is missing"))?;
    Ok((node_id, bucket.clone(), key.clone()))
}

/// Resolve one declared output into the concrete files to upload. A wildcard
/// path is expanded against the terminal attempt; a literal path is uploaded as
/// declared and still fails the job when it is missing.
async fn resolve_output(
    backend: &Arc<dyn ExecutorBackend>,
    fence: &FenceContext,
    output: &OutputSelection,
) -> Result<Vec<OutputSelection>, JobError> {
    if !has_wildcard(&output.container_path) {
        return Ok(vec![output.clone()]);
    }
    let mut matched = backend
        .list_outputs(fence, &output.container_path)
        .await
        .map_err(|error| output_read_error(&error))?;
    matched.sort();
    matched.dedup();
    tracing::debug!(
        pattern = %output.container_path,
        matched = matched.len(),
        "Expanded wildcard output"
    );
    expand_selection(output, matched)
}

/// One selection per matched file, keyed by the match with `path_prefix` stripped.
/// Zero matches captures nothing, which the spec allows for a selection pattern.
fn expand_selection(
    output: &OutputSelection,
    matched: Vec<String>,
) -> Result<Vec<OutputSelection>, JobError> {
    let prefix = output.path_prefix.as_deref().ok_or_else(|| {
        JobError::permanent(format!(
            "output `{}` contains wildcards without a path_prefix",
            output.container_path
        ))
    })?;
    if matched.len() > MAX_OUTPUT_MATCHES {
        return Err(JobError::permanent(format!(
            "output `{}` matches more than {MAX_OUTPUT_MATCHES} files",
            output.container_path
        )));
    }
    let OutputDestination::S3 { bucket, key } = &output.destination;
    matched
        .into_iter()
        .map(|path| {
            let suffix = output_suffix(&path, prefix).ok_or_else(|| {
                JobError::permanent(format!("output `{path}` is outside path_prefix `{prefix}`"))
            })?;
            Ok(OutputSelection {
                container_path: path,
                path_prefix: None,
                destination_node_id: output.destination_node_id,
                destination: OutputDestination::S3 {
                    bucket: bucket.clone(),
                    key: format!("{}/{suffix}", key.trim_end_matches('/')),
                },
                name: output.name.clone(),
                description: output.description.clone(),
            })
        })
        .collect()
}

/// Stream one declared container output into its S3 destination under its
/// reserved VersionId. The write fences itself on that id, so a replayed capture
/// resolves to the version it already created instead of writing a second one.
#[allow(clippy::too_many_arguments)]
async fn put_file_output(
    context: &DriverContext,
    backend: &Arc<dyn ExecutorBackend>,
    fence: &FenceContext,
    spec: &ExecutionSpec,
    record: &JobRecord,
    node_id: NodeId,
    output: &OutputSelection,
    version_id: Ulid,
    execution_id: Ulid,
    inherited: &[PlacementPolicyRef],
) -> Result<OutputObject, JobError> {
    let OutputDestination::S3 { bucket, key } = &output.destination;
    let destination_node_id = output
        .destination_node_id
        .ok_or_else(|| JobError::permanent("output destination endpoint is missing"))?;
    let remote = destination_node_id != node_id;
    let (write_bucket, write_key) = if remote {
        let stage_bucket = output_stage_bucket(record);
        Box::pin(ensure_output_stage(context, spec, record, &stage_bucket)).await?;
        (stage_bucket, version_id.to_string())
    } else {
        let bucket_info = Box::pin(drive(GetBucketInfoOperation::new(bucket.clone()), context))
            .await
            .and_then(|result| result.transpose())
            .map_err(|error| bucket_lookup_error("output", error))?
            .ok_or_else(|| JobError::permanent(format!("output bucket {bucket} not found")))?;
        if bucket_info.group_id != spec.group_id {
            return Err(JobError::permanent(
                "output bucket is outside the execution group",
            ));
        }
        let allowed = Box::pin(drive(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: AuthContext {
                    user_id: record.created_by,
                    realm_id: record.created_by.realm_id,
                    path_restrictions: None,
                    session: None,
                },
                path: blob_object_permission_path(
                    record.created_by.realm_id,
                    spec.group_id,
                    node_id,
                    bucket,
                    key,
                ),
                required_permission: Permission::WRITE,
            }),
            context,
        ))
        .await
        .map_err(|error| authorization_error("output", error))?;
        if !allowed {
            return Err(JobError::permanent(format!(
                "output {bucket}/{key} access denied"
            )));
        }
        (bucket.clone(), key.clone())
    };

    let realm_config = Box::pin(drive(
        GetRealmConfigOperation::new(record.created_by.realm_id),
        context,
    ))
    .await
    .map_err(|error| JobError::retryable(format!("output quota lookup failed: {error}")))?;
    let quota_ceiling = realm_config.quota.effective_group_ceiling(&spec.group_id);

    let fetched = backend
        .fetch_output(fence, &output.container_path)
        .await
        .map_err(|error| output_read_error(&error))?;
    let size = fetched.size;
    let stream_error = Arc::new(std::sync::Mutex::new(None));
    let body_error = stream_error.clone();
    let body = BackendStream::new(fetched.chunks.map(move |chunk| {
        chunk.map_err(|error| {
            if let Ok(mut slot) = body_error.lock() {
                *slot = Some(error.clone());
            }
            std::io::Error::other(error)
        })
    }));
    let routing = routing_snapshot(context, spec.group_id, &write_bucket)
        .await
        .map_err(|error| routing_error("output write", error))?;
    let gate = gate_context(context, record.created_by.realm_id, now_ms())
        .await
        .map_err(|error| gate_error("output write", error))?;
    if gate.as_ref().is_some_and(|gate| !gate.admitting) {
        return Err(gate_stopped("output write"));
    }
    let mut operation = PutObjectOperation::new(PutObjectConfig {
        user_id: record.created_by,
        group_id: spec.group_id,
        realm_id: record.created_by.realm_id,
        node_id,
        request: PutObjectInput {
            bucket: write_bucket.clone(),
            key: write_key.clone(),
            content_length: Some(size),
            body: Some(body),
        },
        expected_checksums: Vec::new(),
        checksum_type: None,
        exists: false,
        version_source: None,
        preassigned_version_id: Some(version_id),
        quota_ceiling,
        routing,
    })
    .with_inherited_policies(inherited.to_vec());
    if let Some(gate) = gate {
        operation = operation.with_gate(gate);
    }
    let result = Box::pin(drive(operation, context))
        .await
        .and_then(|result| result.transpose())
        // A failure caused by the container-side stream keeps its own
        // retryable/permanent classification instead of the put's.
        .map_err(
            |error| match stream_error.lock().ok().and_then(|mut e| e.take()) {
                Some(backend_error) => output_read_error(&backend_error),
                None => put_object_error("output write", error),
            },
        )?
        .ok_or_else(|| JobError::retryable("output write returned no version"))?;
    if remote {
        Box::pin(replicate_output(
            context,
            spec,
            record,
            &write_bucket,
            &write_key,
            result.version_id,
            output,
        ))
        .await?;
        Box::pin(cleanup_output_stage(
            context,
            spec,
            record,
            &write_bucket,
            &write_key,
            result.version_id,
        ))
        .await?;
    }
    Ok(output_object(
        output,
        destination_node_id,
        bucket,
        key,
        &result,
        execution_id,
    ))
}

fn output_stage_bucket(record: &JobRecord) -> String {
    format!("out-{}", record.job_id.to_string().to_lowercase())
}

async fn ensure_output_stage(
    context: &DriverContext,
    spec: &ExecutionSpec,
    record: &JobRecord,
    bucket: &str,
) -> Result<(), JobError> {
    let info = BucketInfo {
        group_id: spec.group_id,
        created_at: SystemTime::now(),
        created_by: record.created_by,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
    };
    match Box::pin(drive(
        CreateBucketOperation::new(bucket.to_string(), info),
        context,
    ))
    .await
    {
        Ok(_) | Err(CreateBucketError::BucketAlreadyExists) => Ok(()),
        Err(error) => Err(JobError::retryable(format!(
            "output staging bucket create failed: {error}"
        ))),
    }
}

async fn replicate_output(
    context: &DriverContext,
    spec: &ExecutionSpec,
    record: &JobRecord,
    source_bucket: &str,
    source_key: &str,
    version_id: Ulid,
    output: &OutputSelection,
) -> Result<(), JobError> {
    let destination_node_id = output
        .destination_node_id
        .ok_or_else(|| JobError::permanent("output destination endpoint is missing"))?;
    let OutputDestination::S3 { bucket, key } = &output.destination;
    let auth = AuthContext {
        user_id: record.created_by,
        realm_id: record.created_by.realm_id,
        path_restrictions: None,
        session: None,
    };
    let source =
        SourceAuthorization::load(context, auth.clone(), spec.group_id, record.owner_node_id)
            .await
            .map_err(|error| match error {
                SourceAuthorizationError::Denied => {
                    JobError::permanent("output staging read access denied")
                }
                SourceAuthorizationError::Unavailable(error) => {
                    JobError::retryable(format!("output staging authorization failed: {error}"))
                }
            })?;
    let routing = quota_marked_routing(context)
        .await
        .map_err(|error| routing_error("output copy", error))?;
    let operation = ReplicateScopeOperation::new(ReplicateScopeInput {
        bucket: source_bucket.to_string(),
        target: ReplicateScopeTarget::Version {
            key: source_key.to_string(),
            version_id,
        },
        target_node_id: destination_node_id,
        auth_context: auth.clone(),
        replicate_delete_markers: false,
        mode: ReplicationMode::OnDemand,
    })
    .with_routing(routing)
    .with_source_authorization(source)
    .with_destination(bucket.clone(), key.clone(), record.owner_node_id, auth);
    let result = Box::pin(drive(operation, context))
        .await
        .and_then(|result| result.transpose())
        .map_err(|error| output_replication_error(error.to_string()))?
        .ok_or_else(|| JobError::retryable("output copy returned no result"))?;
    if result.failed > 0 || result.replicated == 0 && result.skipped == 0 {
        return Err(output_replication_error(
            result
                .last_error
                .unwrap_or_else(|| "output copy made no progress".to_string()),
        ));
    }
    Ok(())
}

async fn cleanup_output_stage(
    context: &DriverContext,
    spec: &ExecutionSpec,
    record: &JobRecord,
    bucket: &str,
    key: &str,
    version_id: Ulid,
) -> Result<(), JobError> {
    match Box::pin(drive(
        DeleteObjectOperation::new(DeleteObjectInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: Some(version_id),
            group_id: spec.group_id,
            realm_id: record.created_by.realm_id,
            node_id: record.owner_node_id,
            deleted_by: record.created_by,
        }),
        context,
    ))
    .await
    .and_then(|result| result.transpose())
    {
        Ok(Some(_)) | Err(DeleteObjectError::NoSuchVersion) => {}
        Ok(None) => {
            return Err(JobError::retryable(
                "output staging delete returned no result",
            ));
        }
        Err(error) => {
            return Err(JobError::retryable(format!(
                "output staging delete failed: {error}"
            )));
        }
    }
    match Box::pin(drive(
        DeleteBucketOperation::new(bucket.to_string()),
        context,
    ))
    .await
    .and_then(|result| result.transpose())
    {
        Ok(Some(())) | Err(DeleteBucketError::NotFound) => Ok(()),
        Ok(None) => Err(JobError::retryable(
            "output staging bucket delete returned no result",
        )),
        Err(error) => Err(JobError::retryable(format!(
            "output staging bucket delete failed: {error}"
        ))),
    }
}

fn output_replication_error(message: String) -> JobError {
    if message.contains("access denied") || message.contains("writer_access_denied") {
        JobError::permanent(format!("output copy failed: {message}"))
    } else {
        JobError::retryable(format!("output copy failed: {message}"))
    }
}

fn output_read_error(error: &BackendError) -> JobError {
    let message = format!("container output read failed: {error}");
    if error.retryable() {
        JobError::retryable(message)
    } else {
        JobError::permanent(message)
    }
}

/// Exact identity of one written output: the version this write created and the
/// physical execution that produced it, both read back from the stored version.
fn output_object(
    output: &OutputSelection,
    node_id: NodeId,
    bucket: &str,
    key: &str,
    result: &PutObjectResult,
    execution_id: Ulid,
) -> OutputObject {
    OutputObject {
        node_id,
        bucket: bucket.to_string(),
        key: key.to_string(),
        version_id: result.version_id,
        execution_id,
        container_path: output.container_path.clone(),
        size: result.location.blob_size,
        digest: result.location.get_blake3().map(hex_encode),
    }
}

/// One input's bytes plus the facts the workspace write needs, from the local
/// copy or from a remote holder.
struct StagedSource {
    blob: BackendStream<Result<bytes::Bytes, aruna_core::stream::StreamError>>,
    location: Option<BackendLocation>,
    size: Option<u64>,
    source_policies: Vec<PlacementPolicyRef>,
}

impl StagedSource {
    fn from_local(get: crate::s3::get_object::GetObjectResult) -> Self {
        Self {
            blob: get.blob,
            location: get.location,
            size: get
                .source_metadata
                .as_ref()
                .map(|metadata| metadata.content_length),
            source_policies: get.source_policies,
        }
    }
}

/// Stages one exact version from a legal holder. The sealed version and hash
/// are resolved first, so a remote read can never substitute other bytes.
async fn remote_source(
    context: &DriverContext,
    record: &JobRecord,
    input: &InputSelection,
    fact: &JobInputFact,
) -> Result<StagedSource, JobError> {
    let version = match &input.source {
        InputSource::S3 { version_id, .. } => version_id
            .as_deref()
            .map(Ulid::from_string)
            .transpose()
            .map_err(|_| JobError::permanent("input version is invalid".to_string()))?,
    };
    if version != Some(fact.version_id) || input.source_node_id != Some(fact.source_node_id) {
        return Err(JobError::permanent(
            "sealed remote input facts do not match the physical input".to_string(),
        ));
    }
    let staged = crate::jobs::lifecycle::stage::stage_remote_input(
        context,
        record,
        input,
        fact.version_id,
        fact.blake3,
    )
    .await?;
    if staged.size != fact.bytes {
        return Err(JobError::permanent(
            "staged input size differs from the sealed fact".to_string(),
        ));
    }
    Ok(StagedSource {
        blob: staged.blob,
        location: None,
        size: Some(fact.bytes),
        source_policies: fact.policies.clone(),
    })
}

/// One exact realm version a device fetches for itself. It plans nothing, so
/// the request's own version is the only authority, and the bytes land in the
/// run's own workspace as a local copy, never as a reference.
async fn device_source(
    context: &DriverContext,
    record: &JobRecord,
    input: &InputSelection,
    source: NodeId,
) -> Result<StagedSource, JobError> {
    let InputSource::S3 {
        bucket,
        key,
        version_id,
    } = &input.source;
    let version = version_id
        .as_deref()
        .map(Ulid::from_string)
        .transpose()
        .map_err(|_| JobError::permanent(format!("input version is invalid for {bucket}/{key}")))?
        .ok_or_else(|| {
            JobError::permanent(format!("realm input {bucket}/{key} names no exact version"))
        })?;
    let realm_id = record.created_by.realm_id;
    let request = BaoReadRequest {
        auth_context: AuthContext {
            user_id: record.created_by,
            realm_id,
            path_restrictions: None,
            session: None,
        },
        realm_id,
        target: BaoReadTarget::ExactVersion(VersionedObjectArn {
            realm_id,
            node_id: source,
            bucket: bucket.clone(),
            key: key.clone(),
            version,
        }),
        expected_blake3: None,
        metadata_only: false,
        // A device is never a managed-copy destination; the read is the
        // owner-bound device read, which refuses governed content outright.
        destination: None,
        known_refs: Vec::new(),
    };
    match managed_read(context, source, request).await {
        Ok(BaoReadOutput::Stream { blob, size, .. }) => Ok(StagedSource {
            blob,
            location: None,
            size: Some(size),
            source_policies: Vec::new(),
        }),
        Ok(BaoReadOutput::Metadata { .. }) => Err(JobError::retryable(format!(
            "staging {bucket}/{key} received no bytes"
        ))),
        Err(error) => Err(device_read_error(bucket, key, error)),
    }
}

/// A read outcome no retry can change: the version is not there, is not the one
/// named, is not the caller's to read, or is governed data a device may never
/// hold. Everything else stays retryable, because transport is.
fn device_read_error(bucket: &str, key: &str, error: BaoReadError) -> JobError {
    match &error {
        BaoReadError::GovernedUnavailable
        | BaoReadError::Refused(
            BaoReadRefusal::NotFound
            | BaoReadRefusal::InvalidTarget
            | BaoReadRefusal::HashMismatch
            | BaoReadRefusal::ReadDenied,
        ) => JobError::permanent(format!("staging {bucket}/{key} failed: {error}")),
        _ => stage_error(bucket, key, Some(error)),
    }
}

async fn stage_one_input(
    context: &DriverContext,
    spec: &ExecutionSpec,
    record: &JobRecord,
    bucket: &str,
    node_id: NodeId,
    input: &InputSelection,
) -> Result<(), JobError> {
    let InputSource::S3 {
        bucket: src_bucket,
        key: src_key,
        version_id,
    } = &input.source;
    let version = version_id
        .as_deref()
        .map(Ulid::from_string)
        .transpose()
        .map_err(|_| {
            JobError::permanent(format!(
                "invalid input version_id for {src_bucket}/{src_key}"
            ))
        })?;
    let staged = if let Some(source) = input.source_node_id.filter(|source| *source != node_id) {
        // A forwarded plan has already validated the source at its ingress
        // endpoint; only the explicit exact-version staging path is local here.
        match record
            .input_facts
            .iter()
            .find(|fact| fact.destination_key == input.dest_key)
        {
            Some(fact) => Box::pin(remote_source(context, record, input, fact)).await?,
            None => {
                if !local_is_user(context, record.created_by.realm_id).await {
                    return Err(JobError::permanent(
                        "sealed input facts are missing".to_string(),
                    ));
                }
                Box::pin(device_source(context, record, input, source)).await?
            }
        }
    } else {
        let bucket_info = Box::pin(drive(
            GetBucketInfoOperation::new(src_bucket.clone()),
            context,
        ))
        .await
        .and_then(|result| result.transpose())
        .map_err(|error| bucket_lookup_error("input", error))?
        .ok_or_else(|| JobError::permanent(format!("input bucket {src_bucket} not found")))?;
        let allowed = Box::pin(drive(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: AuthContext {
                    user_id: record.created_by,
                    realm_id: record.created_by.realm_id,
                    path_restrictions: None,
                    session: None,
                },
                path: blob_object_permission_path(
                    record.created_by.realm_id,
                    bucket_info.group_id,
                    node_id,
                    src_bucket,
                    src_key,
                ),
                required_permission: Permission::READ,
            }),
            context,
        ))
        .await
        .map_err(|error| authorization_error("input", error))?;
        if !allowed {
            return Err(JobError::permanent(format!(
                "input {src_bucket}/{src_key} access denied"
            )));
        }
        let local = Box::pin(drive(
            GetObjectOperation::new(GetObjectInput {
                bucket: src_bucket.clone(),
                key: src_key.clone(),
                version_id: version,
                range: None,
                group_id: bucket_info.group_id,
                user_identity: record.created_by,
                node_id,
            }),
            context,
        ))
        .await
        .and_then(|result| result.transpose());
        // A target that holds no copy stages the exact sealed version from a legal
        // holder through the managed-copy handshake instead of failing the launch.
        match local {
            Ok(Some(get)) => StagedSource::from_local(get),
            Ok(None) => {
                let fact = record
                    .input_facts
                    .iter()
                    .find(|fact| fact.destination_key == input.dest_key)
                    .ok_or_else(|| {
                        JobError::permanent("sealed input facts are missing".to_string())
                    })?;
                Box::pin(remote_source(context, record, input, fact)).await?
            }
            Err(error) => {
                let Some(fact) = record
                    .input_facts
                    .iter()
                    .find(|fact| fact.destination_key == input.dest_key)
                else {
                    return Err(source_input_error(error));
                };
                match Box::pin(remote_source(context, record, input, fact)).await {
                    Ok(staged) => staged,
                    Err(_) => return Err(source_input_error(error)),
                }
            }
        }
    };
    let get = staged;

    let destination = match Box::pin(drive(
        HeadObjectOperation::new(HeadObjectInput {
            bucket: bucket.to_string(),
            key: input.dest_key.clone(),
            version_id: None,
        }),
        context,
    ))
    .await
    .and_then(|result| result.transpose())
    {
        Ok(Some(result)) => result.location,
        Ok(None)
        | Err(
            HeadObjectError::NoSuchKey
            | HeadObjectError::NoSuchVersion
            | HeadObjectError::DeleteMarker,
        ) => None,
        Err(error) => {
            return Err(JobError::retryable(format!(
                "input destination lookup failed: {error}"
            )));
        }
    };
    if staged_content_matches(
        get.location.as_ref().map(blob_identity),
        destination.as_ref().map(blob_identity),
    ) {
        return Ok(());
    }
    if destination.is_some() {
        match spec.collision_policy {
            CollisionPolicy::Reject => {
                return Err(JobError::permanent(format!(
                    "composition key conflict on {}",
                    input.dest_key
                )));
            }
            CollisionPolicy::KeepExisting => return Ok(()),
            CollisionPolicy::Replace => {}
        }
    }

    let content_length = get
        .location
        .as_ref()
        .map(|location| location.blob_size)
        .or(get.size);
    let realm_config = Box::pin(drive(
        GetRealmConfigOperation::new(record.created_by.realm_id),
        context,
    ))
    .await
    .map_err(|error| JobError::retryable(format!("quota lookup failed: {error}")))?;
    let quota_ceiling = realm_config.quota.effective_group_ceiling(&spec.group_id);
    let routing = routing_snapshot(context, spec.group_id, bucket)
        .await
        .map_err(|error| routing_error("input stage", error))?;
    let gate = gate_context(context, record.created_by.realm_id, now_ms())
        .await
        .map_err(|error| gate_error("input stage", error))?;
    if gate.as_ref().is_some_and(|gate| !gate.admitting) {
        return Err(gate_stopped("input stage"));
    }
    // The staged copy carries the source version's refs: staging can only
    // tighten what the workspace bucket already requires.
    let mut operation = PutObjectOperation::new(PutObjectConfig {
        user_id: record.created_by,
        group_id: spec.group_id,
        realm_id: record.created_by.realm_id,
        node_id,
        request: PutObjectInput {
            bucket: bucket.to_string(),
            key: input.dest_key.clone(),
            content_length,
            body: Some(get.blob),
        },
        expected_checksums: Vec::new(),
        checksum_type: None,
        exists: false,
        version_source: None,
        preassigned_version_id: None,
        quota_ceiling,
        routing,
    })
    .with_inherited_policies(get.source_policies.clone());
    if let Some(gate) = gate {
        operation = operation.with_gate(gate);
    }
    Box::pin(drive(operation, context))
        .await
        .and_then(|result| result.transpose())
        .map_err(|error| put_object_error("input stage", error))?;
    Ok(())
}

fn bucket_lookup_error(scope: &str, error: GetBucketInfoError) -> JobError {
    let message = format!("{scope} bucket lookup failed: {error}");
    if matches!(&error, GetBucketInfoError::StorageError(error) if storage_retryable(error)) {
        JobError::retryable(message)
    } else {
        JobError::permanent(message)
    }
}

/// Only transient source drift earns another attempt: a historical observation
/// the source dropped and an exhausted binding both need an explicit rebind.
fn get_input_retryable(error: &GetObjectError) -> bool {
    match error {
        GetObjectError::StorageError(error) => storage_retryable(error),
        GetObjectError::ReferenceSourceChanged => true,
        GetObjectError::HistoricalReferenceUnavailable
        | GetObjectError::ReferenceAdvanceExhausted => false,
        _ => false,
    }
}

fn staged_input_error(error: GetObjectError) -> JobError {
    let message = format!("staged input read failed: {error}");
    if get_input_retryable(&error) {
        JobError::retryable(message)
    } else {
        JobError::permanent(message)
    }
}

fn source_input_error(error: GetObjectError) -> JobError {
    let message = format!("input read failed: {error}");
    if get_input_retryable(&error) {
        JobError::retryable(message)
    } else {
        JobError::permanent(message)
    }
}

fn authorization_error(scope: &str, error: AuthorizationError) -> JobError {
    let message = format!("{scope} authorization failed: {error}");
    if matches!(&error, AuthorizationError::StorageError(error) if storage_retryable(error)) {
        JobError::retryable(message)
    } else {
        JobError::permanent(message)
    }
}

fn put_object_error(scope: &str, error: PutObjectError) -> JobError {
    let message = format!("{scope} failed: {error}");
    if matches!(&error, PutObjectError::StorageError(error) if storage_retryable(error)) {
        JobError::retryable(message)
    } else {
        JobError::permanent(message)
    }
}

/// A node mid-transition admits nothing governed. That ends on its own, so the
/// attempt parks and retries instead of failing the job.
fn gate_error(scope: &str, error: GateContextError) -> JobError {
    JobError::retryable(format!("{scope} destination unavailable: {error}"))
}

fn gate_stopped(scope: &str) -> JobError {
    JobError::retryable(format!(
        "{scope} destination is not admitting governed data"
    ))
}

fn routing_error(scope: &str, error: RoutingInputsError) -> JobError {
    let message = format!("{scope} routing lookup failed: {error}");
    if error.storage().is_some_and(storage_retryable) {
        JobError::retryable(message)
    } else {
        JobError::permanent(message)
    }
}

fn storage_retryable(error: &StorageError) -> bool {
    matches!(
        error,
        StorageError::TransactionConflict
            | StorageError::CleanupCapacity
            | StorageError::ReadError(_)
            | StorageError::WriteError(_)
            | StorageError::DeleteError
            | StorageError::PersistError(_)
            | StorageError::ChannelClosed
            | StorageError::QueueFull
            | StorageError::Timeout
    )
}

fn blob_identity(location: &BackendLocation) -> (u64, Option<&[u8]>) {
    (location.blob_size, location.get_blake3())
}

fn staged_content_matches(
    source: Option<(u64, Option<&[u8]>)>,
    destination: Option<(u64, Option<&[u8]>)>,
) -> bool {
    let (Some(source), Some(destination)) = (source, destination) else {
        return false;
    };
    source.0 == destination.0
        && matches!(
            (source.1, destination.1),
            (Some(source), Some(destination)) if source == destination
        )
}

/// Attribute this execution's outputs under the declared prefixes. A listed key
/// counts only when this execution durably reserved its VersionId before
/// writing: the current head may belong to a duplicate execution or to an
/// unrelated later write, and stamping it here would forge provenance.
pub async fn collect_outputs(
    context: &DriverContext,
    spec: &ExecutionSpec,
    bucket: &str,
    control: &AttemptControl,
) -> Result<Vec<OutputObject>, JobError> {
    if spec.file_outputs.is_empty() && !spec.output_prefixes.is_empty() {
        return Err(JobError::permanent(
            "output prefixes require declared file outputs",
        ));
    }
    if spec.output_prefixes.is_empty() {
        return Ok(Vec::new());
    }
    let node_id = context
        .net_handle
        .as_ref()
        .map(|net| net.node_id())
        .ok_or_else(|| JobError::retryable("output inventory needs a node identity"))?;
    let mut outputs = Vec::new();
    let mut keys = HashSet::new();
    let mut foreign = 0usize;
    for prefix in &spec.output_prefixes {
        let mut continuation = None;
        loop {
            let result = Box::pin(drive(
                ListObjectsV2Operation::new(ListObjectsV2Input {
                    bucket: bucket.to_string(),
                    group_id: spec.group_id,
                    continuation_token: continuation.clone(),
                    max_keys: None,
                    prefix: Some(prefix.clone()),
                    delimiter: None,
                    start_after: None,
                }),
                context,
            ))
            .await
            .and_then(|result| result.transpose())
            .map_err(|error| JobError::retryable(format!("output inventory failed: {error}")))?;
            let Some(result) = result else { break };
            for object in result.objects {
                let key = object.head.key;
                let Some(version_id) = reserved_version(control, node_id, bucket, &key) else {
                    foreign += 1;
                    continue;
                };
                let location = Box::pin(head_version(context, bucket, &key, version_id))
                    .await?
                    .ok_or_else(|| {
                        JobError::permanent(format!(
                            "reserved output {bucket}/{key} version {version_id} is absent"
                        ))
                    })?;
                let (size, digest) = match location {
                    Some(location) => (location.blob_size, location.get_blake3().map(hex_encode)),
                    None => (0, None),
                };
                insert_output(
                    &mut outputs,
                    &mut keys,
                    OutputObject {
                        node_id,
                        bucket: bucket.to_string(),
                        key,
                        version_id,
                        execution_id: control.execution_id,
                        container_path: String::new(),
                        size,
                        digest,
                    },
                )?;
            }
            match result.continuation_token {
                Some(token) => continuation = Some(token),
                None => break,
            }
        }
    }
    if foreign > 0 {
        return Err(JobError::permanent(format!(
            "output prefix inventory contains {foreign} unreserved objects"
        )));
    }
    Ok(outputs)
}

/// Version this execution reserved for `bucket`/`key` before writing, or `None`
/// when the object under that key was produced by another writer. This is the
/// only thing that attributes a workspace object to an execution.
fn reserved_version(
    control: &AttemptControl,
    node_id: NodeId,
    bucket: &str,
    key: &str,
) -> Option<Ulid> {
    control
        .output_commits
        .iter()
        .find(|commit| commit.node_id == node_id && commit.bucket == bucket && commit.key == key)
        .map(|commit| commit.version_id)
}

/// Stored location of one reserved output version, or `None` when that exact
/// version does not exist. The head is read by VersionId, never by current
/// head, so a concurrent write cannot substitute its own bytes here.
async fn head_version(
    context: &DriverContext,
    bucket: &str,
    key: &str,
    version_id: Ulid,
) -> Result<Option<Option<BackendLocation>>, JobError> {
    match Box::pin(drive(
        HeadObjectOperation::new(HeadObjectInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: Some(version_id),
        }),
        context,
    ))
    .await
    .and_then(|result| result.transpose())
    {
        Ok(Some(result)) => match result.version_id {
            Some(found) if found == version_id => Ok(Some(result.location)),
            _ => Err(JobError::permanent(format!(
                "output {bucket}/{key} does not carry reserved version {version_id}"
            ))),
        },
        Ok(None)
        | Err(
            HeadObjectError::NoSuchKey
            | HeadObjectError::NoSuchVersion
            | HeadObjectError::DeleteMarker,
        ) => Ok(None),
        Err(error) => Err(JobError::retryable(format!(
            "output version lookup failed: {error}"
        ))),
    }
}

/// Fold the exported manifest into the inventoried one under the same keyed limit
/// inventory enforces. A retried finalize inventories objects a previous export already
/// wrote; the export row wins, because it names a container path inventory cannot know.
pub(super) fn merge_outputs(
    inventoried: Vec<OutputObject>,
    captured: Vec<OutputObject>,
) -> Result<Vec<OutputObject>, JobError> {
    let exported: HashSet<(NodeId, &str, &str)> = captured
        .iter()
        .map(|output| (output.node_id, output.bucket.as_str(), output.key.as_str()))
        .collect();
    let retained: Vec<OutputObject> = inventoried
        .into_iter()
        .filter(|output| {
            !exported.contains(&(output.node_id, output.bucket.as_str(), output.key.as_str()))
        })
        .collect();

    let mut outputs = Vec::new();
    let mut keys = HashSet::new();
    for output in retained.into_iter().chain(captured) {
        insert_output(&mut outputs, &mut keys, output)?;
    }
    Ok(outputs)
}

fn insert_output(
    outputs: &mut Vec<OutputObject>,
    keys: &mut HashSet<(NodeId, String, String)>,
    output: OutputObject,
) -> Result<(), JobError> {
    if !keys.insert((output.node_id, output.bucket.clone(), output.key.clone())) {
        return Ok(());
    }
    if outputs.len() >= MAX_EXECUTION_OUTPUTS {
        return Err(JobError::permanent(format!(
            "output manifest exceeds {MAX_EXECUTION_OUTPUTS} objects"
        )));
    }
    outputs.push(output);
    Ok(())
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::keyspaces::{
        AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE, USER_ACCESS_KEYSPACE,
    };
    use aruna_core::structs::{
        Actor, Group, GroupAuthorizationDocument, JobErrorKind, JobId, JobPayload,
        OutputCommitIntent, RealmAuthorizationDocument, RealmConfigDocument, RealmId,
    };
    use aruna_storage::FjallStorage;
    use tempfile::tempdir;

    use super::*;

    const HASH_A: [u8; 32] = [1; 32];
    const HASH_B: [u8; 32] = [2; 32];

    fn spec(output_prefixes: Vec<String>) -> ExecutionSpec {
        ExecutionSpec {
            group_id: Ulid::from_bytes([2; 16]),
            name: None,
            description: None,
            tags: Default::default(),
            image: "alpine".to_string(),
            entrypoint: None,
            command: Vec::new(),
            workdir: None,
            env: Default::default(),
            resources: Default::default(),
            executor_constraint: None,
            inputs: Vec::new(),
            file_outputs: Vec::new(),
            workspace_outputs: Vec::new(),
            output_prefixes,
            collision_policy: Default::default(),
        }
    }

    fn output(key: &str) -> OutputObject {
        OutputObject {
            node_id: iroh::SecretKey::from_bytes(&[4u8; 32]).public(),
            bucket: "workspace".to_string(),
            key: key.to_string(),
            version_id: Ulid::generate(),
            execution_id: Ulid::from_bytes([9; 16]),
            container_path: key.to_string(),
            size: 0,
            digest: None,
        }
    }

    // Both input mappings must retry only transient drift: a job that waits on a
    // rebind or a dropped observation would burn its whole attempt budget.
    #[test]
    fn device_read_fails_fast() {
        // A governed or missing realm input must not burn every attempt.
        for error in [
            BaoReadError::GovernedUnavailable,
            BaoReadError::Refused(BaoReadRefusal::NotFound),
            BaoReadError::Refused(BaoReadRefusal::InvalidTarget),
            BaoReadError::Refused(BaoReadRefusal::ReadDenied),
        ] {
            assert_eq!(
                device_read_error("src", "reads.fastq", error).kind,
                JobErrorKind::Permanent
            );
        }
        assert_eq!(
            device_read_error(
                "src",
                "reads.fastq",
                BaoReadError::Refused(BaoReadRefusal::BackendFailure)
            )
            .kind,
            JobErrorKind::Retryable
        );
    }

    #[test]
    fn classifies_input_errors() {
        for map in [staged_input_error, source_input_error] {
            assert_eq!(
                map(GetObjectError::ReferenceSourceChanged).kind,
                JobErrorKind::Retryable
            );
            assert_eq!(
                map(GetObjectError::HistoricalReferenceUnavailable).kind,
                JobErrorKind::Permanent
            );
            assert_eq!(
                map(GetObjectError::ReferenceAdvanceExhausted).kind,
                JobErrorKind::Permanent
            );
        }
    }

    // Transient fjall faults now carry their source; classification must key on
    // the variant, not on the payload.
    #[test]
    fn classifies_storage_retries() {
        for error in [
            StorageError::ReadError("io".to_string()),
            StorageError::WriteError("io".to_string()),
            StorageError::DeleteError,
            StorageError::TransactionConflict,
            // Capacity conditions prove no commit, so they stay infrastructure errors.
            StorageError::CleanupCapacity,
            StorageError::QueueFull,
        ] {
            assert!(storage_retryable(&error));
        }

        for error in [
            StorageError::KeyspaceError("missing".to_string()),
            StorageError::KeyNotFound,
        ] {
            assert!(!storage_retryable(&error));
        }
    }

    #[test]
    fn matching_stage_skips() {
        assert!(staged_content_matches(
            Some((5, Some(&HASH_A))),
            Some((5, Some(&HASH_A)))
        ));
    }

    #[test]
    fn changed_stage_writes() {
        assert!(!staged_content_matches(
            Some((5, Some(&HASH_A))),
            Some((6, Some(&HASH_A)))
        ));
        assert!(!staged_content_matches(
            Some((5, Some(&HASH_A))),
            Some((5, Some(&HASH_B)))
        ));
    }

    #[test]
    fn unknown_stage_writes() {
        assert!(!staged_content_matches(Some((5, Some(&HASH_A))), None));
        assert!(!staged_content_matches(
            Some((5, Some(&HASH_A))),
            Some((5, None))
        ));
        assert!(!staged_content_matches(
            Some((5, None)),
            Some((5, Some(&HASH_A)))
        ));
    }

    #[tokio::test]
    async fn empty_outputs_remain() {
        let (storage_handle, _receivers) = aruna_storage::StorageHandle::new();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        assert!(
            collect_outputs(
                &context,
                &spec(Vec::new()),
                "workspace",
                &control(Vec::new())
            )
            .await
            .unwrap()
            .is_empty()
        );
    }

    #[tokio::test]
    async fn prefix_only_rejected() {
        let (storage_handle, _receivers) = aruna_storage::StorageHandle::new();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let error = collect_outputs(
            &context,
            &spec(vec!["reports/".to_string()]),
            "workspace",
            &control(Vec::new()),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind, JobErrorKind::Permanent);
        assert_eq!(
            error.message,
            "output prefixes require declared file outputs"
        );
    }

    fn control(output_commits: Vec<OutputCommitIntent>) -> AttemptControl {
        AttemptControl {
            attempt_epoch: 1,
            execution_id: Ulid::from_bytes([9; 16]),
            controller_generation: 1,
            bound_token: None,
            tombstone_ref: None,
            output_commits,
            output_record: None,
        }
    }

    #[test]
    fn attributes_reserved_only() {
        // A duplicate execution's head under the same key, an unrelated later
        // write, and another bucket all stay unattributed.
        let version = Ulid::from_bytes([5; 16]);
        let node_id = iroh::SecretKey::from_bytes(&[4u8; 32]).public();
        let reserved = control(vec![OutputCommitIntent {
            node_id,
            bucket: "workspace".to_string(),
            key: "reports/a.txt".to_string(),
            version_id: version,
        }]);

        assert_eq!(
            reserved_version(&reserved, node_id, "workspace", "reports/a.txt"),
            Some(version)
        );
        assert_eq!(
            reserved_version(&reserved, node_id, "workspace", "reports/b.txt"),
            None
        );
        assert_eq!(
            reserved_version(&reserved, node_id, "other", "reports/a.txt"),
            None
        );
        assert_eq!(
            reserved_version(&control(Vec::new()), node_id, "workspace", "reports/a.txt"),
            None
        );
    }

    #[test]
    fn duplicates_keep_versions() {
        // Two executions writing one key reserve two versions, so neither can
        // be attributed to the other.
        let node_id = iroh::SecretKey::from_bytes(&[4u8; 32]).public();
        let destinations = vec![(
            node_id,
            "workspace".to_string(),
            "reports/a.txt".to_string(),
        )];
        let mut first = control(Vec::new());
        let mut second = control(Vec::new());
        second.execution_id = Ulid::from_bytes([10; 16]);
        first.reserve_outputs(&destinations, Ulid::generate);
        second.reserve_outputs(&destinations, Ulid::generate);

        let left = reserved_version(&first, node_id, "workspace", "reports/a.txt").unwrap();
        let right = reserved_version(&second, node_id, "workspace", "reports/a.txt").unwrap();
        assert_ne!(left, right);
    }

    struct CredentialFixture {
        context: DriverContext,
        net: aruna_net::NetHandle,
        spec: ExecutionSpec,
        record: JobRecord,
        node_id: NodeId,
        bucket: String,
        _dir: tempfile::TempDir,
    }

    async fn credential_fixture() -> CredentialFixture {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let realm_id = RealmId([1; 32]);
        let user_id = UserId::local(Ulid::from_bytes([2; 16]), realm_id);
        let node_id = iroh::SecretKey::from_bytes(&[3; 32]).public();
        let spec = spec(Vec::new());
        let actor = Actor {
            node_id,
            user_id,
            realm_id,
        };
        let realm_doc = RealmAuthorizationDocument::new_default_realm_doc(realm_id);
        let group_doc =
            GroupAuthorizationDocument::new_default_group_doc(user_id, realm_id, spec.group_id);
        // Policy loading fails closed without the realm config and group record.
        let realm_config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        let group = Group {
            display_name: "workspace".to_string(),
            group_id: spec.group_id,
            realm_id,
            roles: group_doc.roles.keys().copied().collect(),
            owner: user_id,
        };
        for (key_space, key, value) in [
            (
                AUTH_KEYSPACE,
                realm_id.as_bytes().to_vec(),
                realm_doc.to_bytes(&actor).unwrap(),
            ),
            (
                AUTH_KEYSPACE,
                spec.group_id.to_bytes().to_vec(),
                group_doc.to_bytes(&actor).unwrap(),
            ),
            (
                REALM_CONFIG_KEYSPACE,
                realm_id.as_bytes().to_vec(),
                realm_config.to_bytes(&actor).unwrap(),
            ),
            (
                GROUP_KEYSPACE,
                spec.group_id.to_bytes().to_vec(),
                group.to_bytes(&actor).unwrap(),
            ),
        ] {
            let _ = storage
                .send_storage_effect(StorageEffect::Write {
                    key_space: key_space.to_string(),
                    key: key.into(),
                    value: value.into(),
                    txn_id: None,
                })
                .await;
        }
        let net = aruna_net::NetHandle::new(
            aruna_net::NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                realm_id,
                discovery_method: aruna_net::DiscoveryMethod::None,
                relay_method: aruna_net::RelayMethod::None,
                ..aruna_net::NetConfig::default()
            },
            storage.clone(),
        )
        .await
        .unwrap();
        let context = DriverContext {
            storage_handle: storage,
            net_handle: Some(net.clone()),
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let job_id = JobId::from_bytes([4; 16]);
        let record = JobRecord::new(
            job_id,
            JobPayload::Execution(spec.clone()),
            user_id,
            node_id,
            1,
            1,
            None,
        );
        let bucket = JobRecord::workspace_bucket_name(job_id);
        CredentialFixture {
            context,
            net,
            spec,
            record,
            node_id,
            bucket,
            _dir: dir,
        }
    }

    #[tokio::test]
    async fn device_checks_grant() {
        // The cached group documents are the whole authority on a device too:
        // the owner binding does not stand in for a grant the owner lacks.
        let CredentialFixture {
            context,
            net,
            spec,
            record,
            node_id,
            _dir,
            ..
        } = credential_fixture().await;
        let realm_id = record.created_by.realm_id;
        let owner = record.created_by;
        let ungranted = UserId::local(Ulid::from_bytes([9; 16]), realm_id);
        let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        config.ensure_node(node_id, aruna_core::structs::RealmNodeKind::User { owner });
        let actor = Actor {
            node_id,
            user_id: owner,
            realm_id,
        };
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: REALM_CONFIG_KEYSPACE.to_string(),
                key: realm_id.as_bytes().to_vec().into(),
                value: config.to_bytes(&actor).unwrap().into(),
                txn_id: None,
            })
            .await;

        assert!(
            ensure_group_write(&context, &spec, &record, node_id)
                .await
                .is_ok()
        );
        let foreign = JobRecord::new(
            record.job_id,
            JobPayload::Execution(spec.clone()),
            ungranted,
            node_id,
            1,
            1,
            None,
        );
        assert!(
            ensure_group_write(&context, &spec, &foreign, node_id)
                .await
                .is_err()
        );
        net.shutdown().await;
    }

    #[tokio::test]
    async fn credential_reuses_secret() {
        let CredentialFixture {
            context,
            net,
            spec,
            record,
            node_id,
            bucket,
            _dir,
        } = credential_fixture().await;
        let realm_id = record.created_by.realm_id;

        let first = mint_workspace_credential(&context, &spec, &record, node_id, &bucket)
            .await
            .unwrap();
        let second = mint_workspace_credential(&context, &spec, &record, node_id, &bucket)
            .await
            .unwrap();

        assert_eq!(second.access_key, first.access_key);
        assert_eq!(second.secret, first.secret);

        let access = Box::pin(drive(
            GetUserAccessOperation::new(first.access_key.clone()),
            &context,
        ))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        let mut expired = access.clone();
        expired.expiry = SystemTime::UNIX_EPOCH;
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: USER_ACCESS_KEYSPACE.to_string(),
                key: expired.access_key.as_bytes().into(),
                value: expired.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
        let renewed = mint_workspace_credential(&context, &spec, &record, node_id, &bucket)
            .await
            .unwrap();
        assert_eq!(renewed.access_key, first.access_key);
        let renewed_access = Box::pin(drive(
            GetUserAccessOperation::new(first.access_key.clone()),
            &context,
        ))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        assert!(!renewed_access.is_expired(SystemTime::now()));
        let restrictions = renewed_access.path_restrictions.unwrap();
        let bucket_path = blob_bucket_permission_path(realm_id, spec.group_id, node_id, &bucket);
        let permits = |path: &str| {
            restrictions.iter().any(|restriction| {
                globset::Glob::new(&restriction.pattern)
                    .unwrap()
                    .compile_matcher()
                    .is_match(path)
            })
        };
        assert!(permits(&bucket_path));
        assert!(permits(&format!("{bucket_path}/object")));
        assert!(!permits(&format!("{bucket_path}-sibling")));
        net.shutdown().await;
    }

    #[tokio::test]
    async fn rejects_foreign_seal() {
        // A secret sealed by another node's key must refuse to be reused rather
        // than hand the container an unusable or wrong secret.
        let CredentialFixture {
            context,
            net,
            spec,
            record,
            node_id,
            bucket,
            _dir,
        } = credential_fixture().await;

        let minted = mint_workspace_credential(&context, &spec, &record, node_id, &bucket)
            .await
            .unwrap();
        let mut access = Box::pin(drive(
            GetUserAccessOperation::new(minted.access_key.clone()),
            &context,
        ))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        access
            .seal_secret(
                &aruna_core::credential_seal::CredentialSealKey::random(),
                "foreign",
            )
            .unwrap();
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: USER_ACCESS_KEYSPACE.to_string(),
                key: access.access_key.as_bytes().into(),
                value: access.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;

        let Err(error) =
            mint_workspace_credential(&context, &spec, &record, node_id, &bucket).await
        else {
            panic!("a foreign seal must not yield a credential")
        };

        assert_eq!(error.kind, aruna_core::structs::JobErrorKind::Permanent);
        assert!(
            error
                .message
                .starts_with("workspace credential unseal failed")
        );
        net.shutdown().await;
    }

    #[tokio::test]
    async fn rejects_oversized_mounts() {
        // Fifty patterns are the issuance cap, so twenty-five mounted buckets
        // still pass and the twenty-sixth must fail permanently.
        let (storage_handle, _receivers) = aruna_storage::StorageHandle::new();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let realm_id = RealmId([1; 32]);
        let user_id = UserId::local(Ulid::from_bytes([2; 16]), realm_id);
        let node_id = iroh::SecretKey::from_bytes(&[3; 32]).public();
        let spec = spec(Vec::new());
        let record = JobRecord::new(
            JobId::from_bytes([4; 16]),
            JobPayload::Execution(spec.clone()),
            user_id,
            node_id,
            1,
            1,
            None,
        );
        let buckets = |count: usize| {
            (0..count)
                .map(|index| format!("bucket-{index}"))
                .collect::<BTreeSet<String>>()
        };

        // Without a net handle the accepted set fails later, past the limit check.
        let Err(within) =
            mint_input_credential(&context, &spec, &record, node_id, &buckets(25)).await
        else {
            panic!("a credential cannot be sealed without a net handle")
        };
        assert_eq!(within.message, "workspace credential needs a net handle");

        let Err(over) =
            mint_input_credential(&context, &spec, &record, node_id, &buckets(26)).await
        else {
            panic!("an over-limit mount set must not yield a credential")
        };

        assert_eq!(over.kind, aruna_core::structs::JobErrorKind::Permanent);
        assert!(
            over.message
                .starts_with("workspace credential restrictions invalid")
        );
    }

    #[test]
    fn overlap_deduplicates() {
        let mut outputs = Vec::new();
        let mut keys = HashSet::new();
        insert_output(&mut outputs, &mut keys, output("result")).unwrap();
        insert_output(&mut outputs, &mut keys, output("result")).unwrap();
        assert_eq!(outputs.len(), 1);
    }

    #[test]
    fn merge_deduplicates() {
        // A retried finalize inventories the object its own export already wrote;
        // the manifest must name it once, with the export's container path.
        let mut inventoried = output("result");
        inventoried.container_path.clear();
        let captured = output("result");

        let outputs = merge_outputs(vec![inventoried], vec![captured.clone()]).unwrap();

        assert_eq!(outputs, vec![captured]);
    }

    #[test]
    fn merge_keeps_order() {
        let inventoried = vec![output("a"), output("b")];
        let captured = vec![output("c")];

        let outputs = merge_outputs(inventoried, captured).unwrap();

        let keys: Vec<_> = outputs.iter().map(|output| output.key.as_str()).collect();
        assert_eq!(keys, vec!["a", "b", "c"]);
    }

    #[test]
    fn merge_enforces_limit() {
        let inventoried: Vec<_> = (0..MAX_EXECUTION_OUTPUTS)
            .map(|index| output(&index.to_string()))
            .collect();

        // A duplicate is absorbed, so a full inventory still merges.
        let merged = merge_outputs(inventoried.clone(), vec![output("0")]).unwrap();
        assert_eq!(merged.len(), MAX_EXECUTION_OUTPUTS);

        let error = merge_outputs(inventoried, vec![output("overflow")]).unwrap_err();
        assert_eq!(error.kind, aruna_core::structs::JobErrorKind::Permanent);
    }

    fn wildcard_output(pattern: &str) -> OutputSelection {
        OutputSelection {
            container_path: pattern.to_string(),
            path_prefix: Some("/out".to_string()),
            destination_node_id: Some(iroh::SecretKey::from_bytes(&[4u8; 32]).public()),
            destination: OutputDestination::S3 {
                bucket: "dest".to_string(),
                key: "results/".to_string(),
            },
            name: Some("reports".to_string()),
            description: None,
        }
    }

    fn expanded_keys(output: &OutputSelection, matched: Vec<String>) -> Vec<String> {
        expand_selection(output, matched)
            .unwrap()
            .iter()
            .map(|output| {
                let OutputDestination::S3 { key, .. } = &output.destination;
                key.clone()
            })
            .collect()
    }

    #[test]
    fn expands_matched_keys() {
        let output = wildcard_output("/out/*.txt");
        let matched = vec!["/out/a.txt".to_string(), "/out/b.txt".to_string()];

        let expanded = expand_selection(&output, matched).unwrap();

        assert_eq!(expanded[1].container_path, "/out/b.txt");
        assert_eq!(expanded[1].name.as_deref(), Some("reports"));
        assert!(expanded[1].path_prefix.is_none());
        // The suffix below path_prefix keeps every nested component.
        assert_eq!(
            expanded_keys(&output, vec!["/out/a.txt".to_string()]),
            vec!["results/a.txt"]
        );
        assert_eq!(
            expanded_keys(
                &wildcard_output("/out/*/*/*.txt"),
                vec!["/out/sub/deep/b.txt".to_string()]
            ),
            vec!["results/sub/deep/b.txt"]
        );
    }

    #[test]
    fn empty_expansion_captures() {
        // Zero matches is a legitimate empty selection, not a job failure.
        assert!(
            expand_selection(&wildcard_output("/out/*.txt"), Vec::new())
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn rejects_foreign_match() {
        let output = wildcard_output("/out/*.txt");
        let error = expand_selection(&output, vec!["/other/a.txt".to_string()]).unwrap_err();
        assert_eq!(error.kind, aruna_core::structs::JobErrorKind::Permanent);

        let mut output = output;
        output.path_prefix = None;
        let error = expand_selection(&output, vec!["/out/a.txt".to_string()]).unwrap_err();
        assert_eq!(error.kind, aruna_core::structs::JobErrorKind::Permanent);
    }

    #[test]
    fn output_limit_errors() {
        let mut outputs = Vec::new();
        let mut keys = HashSet::new();
        for index in 0..MAX_EXECUTION_OUTPUTS {
            insert_output(&mut outputs, &mut keys, output(&index.to_string())).unwrap();
        }
        insert_output(&mut outputs, &mut keys, output("0")).unwrap();
        let error = insert_output(&mut outputs, &mut keys, output("overflow")).unwrap_err();
        assert_eq!(error.kind, aruna_core::structs::JobErrorKind::Permanent);
        assert_eq!(outputs.len(), MAX_EXECUTION_OUTPUTS);
    }
}
