use std::collections::HashSet;
use std::time::{Duration, SystemTime};

use aruna_compute::backend::ExecutorBackend;
use aruna_core::compute::{AttemptRef, BackendError, MAX_TRANSFER_BYTES, TaskInput};
use aruna_core::errors::{AuthorizationError, StorageError};
use aruna_core::stream::BackendStream;
use aruna_core::structs::{
    AuthContext, BackendLocation, BucketInfo, ExecutionSpec, InputSelection, InputSource, JobError,
    JobRecord, OutputDestination, OutputObject, OutputSelection, PathRestriction, Permission,
    UserAccess, blob_bucket_permission_path, blob_group_permission_path,
    blob_object_permission_path, workspace_credential_id,
};
use aruna_core::types::NodeId;
use futures_util::StreamExt;
use std::sync::Arc;
use ulid::Ulid;

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::driver::{DriverContext, drive};
use crate::get_realm_config::GetRealmConfigOperation;
use crate::s3::create_bucket::{CreateBucketError, CreateBucketOperation};
use crate::s3::create_user_access::{CreateUserAccessConfig, CreateUserAccessOperation};
use crate::s3::get_bucket_info::{GetBucketInfoError, GetBucketInfoOperation};
use crate::s3::get_object::{GetObjectError, GetObjectInput, GetObjectOperation};
use crate::s3::get_user_access::{GetUserAccessError, GetUserAccessOperation};
use crate::s3::head_object::{HeadObjectError, HeadObjectInput, HeadObjectOperation};
use crate::s3::list_objects_v2::{ListObjectsV2Input, ListObjectsV2Operation};
use crate::s3::put_object::{PutObjectConfig, PutObjectError, PutObjectInput, PutObjectOperation};

/// Credential lifetime past the walltime so a slow finalize still authorizes.
const CREDENTIAL_SLACK: Duration = Duration::from_secs(6 * 60 * 60);
const MAX_OUTPUT_MANIFEST_OBJECTS: usize = 10_000;

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
    let allowed = drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: AuthContext {
                user_id: record.created_by,
                realm_id: record.created_by.realm_id,
                path_restrictions: None,
            },
            path: blob_group_permission_path(record.created_by.realm_id, spec.group_id, node_id),
            required_permission: Permission::WRITE,
        }),
        context,
    )
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
    bucket: &str,
) -> Result<(), JobError> {
    let bucket_info = BucketInfo {
        group_id: spec.group_id,
        created_at: SystemTime::now(),
        created_by: record.created_by,
        cors_configuration: None,
    };
    match drive(
        CreateBucketOperation::new(bucket.to_string(), bucket_info),
        context,
    )
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
    let key_id = workspace_credential_id(record.job_id);
    let access_key =
        UserAccess::build_access_key(&record.created_by, &key_id).map_err(|error| {
            JobError::permanent(format!("workspace credential key failed: {error}"))
        })?;
    match drive(GetUserAccessOperation::new(access_key.clone()), context).await {
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
                return Ok(WorkspaceCredential {
                    access_key: access.access_key,
                    secret: access.secret,
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
        .unwrap_or(Duration::from_secs(24 * 60 * 60));
    let expiry = SystemTime::now() + walltime + CREDENTIAL_SLACK;
    let (_, access) = drive(
        CreateUserAccessOperation::new_with_key(
            CreateUserAccessConfig {
                user_identity: record.created_by,
                group_id: spec.group_id,
                expiry,
                path_restrictions: Some(restrictions),
                issued_by: *node_id.as_bytes(),
            },
            key_id,
        ),
        context,
    )
    .await
    .map_err(|error| JobError::retryable(format!("workspace credential mint failed: {error}")))?
    .map_err(|error| JobError::retryable(format!("workspace credential mint failed: {error}")))?;
    Ok(WorkspaceCredential {
        access_key: access.access_key,
        secret: access.secret,
    })
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
        stage_one_input(context, spec, record, bucket, node_id, input).await?;
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
) -> Result<Vec<TaskInput>, JobError> {
    let mut files = Vec::new();
    let mut total_bytes = 0u64;
    for input in &spec.inputs {
        let Some(path) = input.container_path.clone() else {
            continue;
        };
        let get = drive(
            GetObjectOperation::new(GetObjectInput {
                bucket: bucket.to_string(),
                key: input.dest_key.clone(),
                version_id: None,
                range: None,
                group_id: spec.group_id,
                user_identity: record.created_by,
            }),
            context,
        )
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
        files.push(TaskInput::from_stream(path, size, Box::pin(stream)));
    }
    Ok(files)
}

pub async fn capture_outputs(
    context: &DriverContext,
    backend: &Arc<dyn ExecutorBackend>,
    attempt: &AttemptRef,
    spec: &ExecutionSpec,
    record: &JobRecord,
    node_id: NodeId,
) -> Result<Vec<OutputObject>, JobError> {
    let mut outputs = Vec::with_capacity(spec.file_outputs.len());
    for output in &spec.file_outputs {
        outputs
            .push(put_file_output(context, backend, attempt, spec, record, node_id, output).await?);
    }
    Ok(outputs)
}

/// Stream one declared container output into its S3 destination. When the
/// destination already holds identical content (a retried finalize), the write
/// is skipped after a streamed hash pass.
#[allow(clippy::too_many_arguments)]
async fn put_file_output(
    context: &DriverContext,
    backend: &Arc<dyn ExecutorBackend>,
    attempt: &AttemptRef,
    spec: &ExecutionSpec,
    record: &JobRecord,
    node_id: NodeId,
    output: &OutputSelection,
) -> Result<OutputObject, JobError> {
    let OutputDestination::S3 { bucket, key } = &output.destination;
    let bucket_info = drive(GetBucketInfoOperation::new(bucket.clone()), context)
        .await
        .and_then(|result| result.transpose())
        .map_err(|error| bucket_lookup_error("output", error))?
        .ok_or_else(|| JobError::permanent(format!("output bucket {bucket} not found")))?;
    if bucket_info.group_id != spec.group_id {
        return Err(JobError::permanent(
            "output bucket is outside the execution group",
        ));
    }
    let allowed = drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: AuthContext {
                user_id: record.created_by,
                realm_id: record.created_by.realm_id,
                path_restrictions: None,
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
    )
    .await
    .map_err(|error| authorization_error("output", error))?;
    if !allowed {
        return Err(JobError::permanent(format!(
            "output {bucket}/{key} access denied"
        )));
    }

    let existing = match drive(
        HeadObjectOperation::new(HeadObjectInput {
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: None,
        }),
        context,
    )
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
                "output destination lookup failed: {error}"
            )));
        }
    };
    if let Some(location) = &existing {
        let (size, digest) = hash_output(backend, attempt, &output.container_path).await?;
        if location.blob_size == size && location.get_blake3() == Some(digest.as_bytes().as_slice())
        {
            return Ok(output_object(output, bucket, key, size, &digest));
        }
    }

    let realm_config = drive(
        GetRealmConfigOperation::new(record.created_by.realm_id),
        context,
    )
    .await
    .map_err(|error| JobError::retryable(format!("output quota lookup failed: {error}")))?;
    let quota_ceiling = realm_config.quota.effective_group_ceiling(&spec.group_id);

    let fetched = backend
        .fetch_output(attempt, &output.container_path)
        .await
        .map_err(|error| output_read_error(&error))?;
    let size = fetched.size;
    let hasher = Arc::new(std::sync::Mutex::new(blake3::Hasher::new()));
    let stream_error = Arc::new(std::sync::Mutex::new(None));
    let body_hasher = hasher.clone();
    let body_error = stream_error.clone();
    let body = BackendStream::new(fetched.chunks.map(move |chunk| match chunk {
        Ok(chunk) => {
            if let Ok(mut hasher) = body_hasher.lock() {
                hasher.update(&chunk);
            }
            Ok(chunk)
        }
        Err(error) => {
            if let Ok(mut slot) = body_error.lock() {
                *slot = Some(error.clone());
            }
            Err(std::io::Error::other(error))
        }
    }));
    drive(
        PutObjectOperation::new(PutObjectConfig {
            user_id: record.created_by,
            group_id: spec.group_id,
            realm_id: record.created_by.realm_id,
            node_id,
            request: PutObjectInput {
                bucket: bucket.clone(),
                key: key.clone(),
                content_length: Some(size),
                body: Some(body),
            },
            expected_checksums: Vec::new(),
            checksum_type: None,
            exists: false,
            version_source: None,
            quota_ceiling,
        }),
        context,
    )
    .await
    .and_then(|result| result.transpose())
    // A failure caused by the container-side stream keeps its own
    // retryable/permanent classification instead of the put's.
    .map_err(
        |error| match stream_error.lock().ok().and_then(|mut e| e.take()) {
            Some(backend_error) => output_read_error(&backend_error),
            None => put_object_error("output write", error),
        },
    )?;
    let digest = hasher
        .lock()
        .map(|hasher| hasher.finalize())
        .map_err(|_| JobError::retryable("output digest lost"))?;
    Ok(output_object(output, bucket, key, size, &digest))
}

/// Streamed size + blake3 of one container output, for the idempotent-retry check.
async fn hash_output(
    backend: &Arc<dyn ExecutorBackend>,
    attempt: &AttemptRef,
    path: &str,
) -> Result<(u64, blake3::Hash), JobError> {
    let fetched = backend
        .fetch_output(attempt, path)
        .await
        .map_err(|error| output_read_error(&error))?;
    let mut chunks = fetched.chunks;
    let mut hasher = blake3::Hasher::new();
    let mut size = 0u64;
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.map_err(|error| output_read_error(&error))?;
        size += chunk.len() as u64;
        hasher.update(&chunk);
    }
    Ok((size, hasher.finalize()))
}

fn output_read_error(error: &BackendError) -> JobError {
    let message = format!("container output read failed: {error}");
    if error.retryable() {
        JobError::retryable(message)
    } else {
        JobError::permanent(message)
    }
}

fn output_object(
    output: &OutputSelection,
    bucket: &str,
    key: &str,
    size: u64,
    digest: &blake3::Hash,
) -> OutputObject {
    OutputObject {
        bucket: bucket.to_string(),
        key: key.to_string(),
        container_path: output.container_path.clone(),
        size,
        digest: Some(digest.to_hex().to_string()),
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
    let bucket_info = drive(GetBucketInfoOperation::new(src_bucket.clone()), context)
        .await
        .and_then(|result| result.transpose())
        .map_err(|error| bucket_lookup_error("input", error))?
        .ok_or_else(|| JobError::permanent(format!("input bucket {src_bucket} not found")))?;
    let allowed = drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: AuthContext {
                user_id: record.created_by,
                realm_id: record.created_by.realm_id,
                path_restrictions: None,
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
    )
    .await
    .map_err(|error| authorization_error("input", error))?;
    if !allowed {
        return Err(JobError::permanent(format!(
            "input {src_bucket}/{src_key} access denied"
        )));
    }
    let get = drive(
        GetObjectOperation::new(GetObjectInput {
            bucket: src_bucket.clone(),
            key: src_key.clone(),
            version_id: version,
            range: None,
            group_id: spec.group_id,
            user_identity: record.created_by,
        }),
        context,
    )
    .await
    .and_then(|result| result.transpose())
    .map_err(source_input_error)?
    .ok_or_else(|| JobError::permanent(format!("input {src_bucket}/{src_key} not found")))?;

    let destination = match drive(
        HeadObjectOperation::new(HeadObjectInput {
            bucket: bucket.to_string(),
            key: input.dest_key.clone(),
            version_id: None,
        }),
        context,
    )
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

    let content_length = get.location.as_ref().map(|location| location.blob_size);
    let realm_config = drive(
        GetRealmConfigOperation::new(record.created_by.realm_id),
        context,
    )
    .await
    .map_err(|error| JobError::retryable(format!("quota lookup failed: {error}")))?;
    let quota_ceiling = realm_config.quota.effective_group_ceiling(&spec.group_id);
    drive(
        PutObjectOperation::new(PutObjectConfig {
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
            quota_ceiling,
        }),
        context,
    )
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

fn staged_input_error(error: GetObjectError) -> JobError {
    let message = format!("staged input read failed: {error}");
    if matches!(&error, GetObjectError::StorageError(error) if storage_retryable(error)) {
        JobError::retryable(message)
    } else {
        JobError::permanent(message)
    }
}

fn source_input_error(error: GetObjectError) -> JobError {
    let message = format!("input read failed: {error}");
    if matches!(&error, GetObjectError::StorageError(error) if storage_retryable(error)) {
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

fn storage_retryable(error: &StorageError) -> bool {
    matches!(
        error,
        StorageError::TransactionConflict
            | StorageError::ReadError
            | StorageError::WriteError
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

/// Inventory the declared output prefixes in the workspace at completion. Missing
/// prefixes contribute nothing; no declarations produce an empty manifest.
pub async fn collect_outputs(
    context: &DriverContext,
    spec: &ExecutionSpec,
    bucket: &str,
) -> Result<Vec<OutputObject>, JobError> {
    if spec.output_prefixes.is_empty() {
        return Ok(Vec::new());
    }
    let mut outputs = Vec::new();
    let mut keys = HashSet::new();
    for prefix in &spec.output_prefixes {
        let mut continuation = None;
        loop {
            let result = drive(
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
            )
            .await
            .and_then(|result| result.transpose())
            .map_err(|error| JobError::retryable(format!("output inventory failed: {error}")))?;
            let Some(result) = result else { break };
            for object in result.objects {
                let (size, digest) = match object.location {
                    Some(location) => (location.blob_size, location.get_blake3().map(hex_encode)),
                    None => (0, None),
                };
                insert_output(
                    &mut outputs,
                    &mut keys,
                    OutputObject {
                        bucket: bucket.to_string(),
                        key: object.head.key,
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
    Ok(outputs)
}

fn insert_output(
    outputs: &mut Vec<OutputObject>,
    keys: &mut HashSet<(String, String)>,
    output: OutputObject,
) -> Result<(), JobError> {
    if !keys.insert((output.bucket.clone(), output.key.clone())) {
        return Ok(());
    }
    if outputs.len() >= MAX_OUTPUT_MANIFEST_OBJECTS {
        return Err(JobError::permanent(format!(
            "output manifest exceeds {MAX_OUTPUT_MANIFEST_OBJECTS} objects"
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
    use aruna_core::keyspaces::AUTH_KEYSPACE;
    use aruna_core::structs::{
        Actor, GroupAuthorizationDocument, JobId, JobPayload, RealmAuthorizationDocument, RealmId,
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
            output_prefixes,
        }
    }

    fn output(key: &str) -> OutputObject {
        OutputObject {
            bucket: "workspace".to_string(),
            key: key.to_string(),
            container_path: key.to_string(),
            size: 0,
            digest: None,
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
        let (storage_handle, _receiver) = aruna_storage::StorageHandle::new();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        assert!(
            collect_outputs(&context, &spec(Vec::new()), "workspace")
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn credential_reuses_secret() {
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
        for (key, value) in [
            (
                realm_id.as_bytes().to_vec(),
                realm_doc.to_bytes(&actor).unwrap(),
            ),
            (
                spec.group_id.to_bytes().to_vec(),
                group_doc.to_bytes(&actor).unwrap(),
            ),
        ] {
            let _ = storage
                .send_storage_effect(StorageEffect::Write {
                    key_space: AUTH_KEYSPACE.to_string(),
                    key: key.into(),
                    value: value.into(),
                    txn_id: None,
                })
                .await;
        }
        let context = DriverContext {
            storage_handle: storage,
            net_handle: None,
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

        let first = mint_workspace_credential(&context, &spec, &record, node_id, &bucket)
            .await
            .unwrap();
        let second = mint_workspace_credential(&context, &spec, &record, node_id, &bucket)
            .await
            .unwrap();

        assert_eq!(second.access_key, first.access_key);
        assert_eq!(second.secret, first.secret);

        let access = drive(
            GetUserAccessOperation::new(first.access_key.clone()),
            &context,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        let restrictions = access.path_restrictions.unwrap();
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
    fn output_limit_errors() {
        let mut outputs = Vec::new();
        let mut keys = HashSet::new();
        for index in 0..MAX_OUTPUT_MANIFEST_OBJECTS {
            insert_output(&mut outputs, &mut keys, output(&index.to_string())).unwrap();
        }
        insert_output(&mut outputs, &mut keys, output("0")).unwrap();
        let error = insert_output(&mut outputs, &mut keys, output("overflow")).unwrap_err();
        assert_eq!(error.kind, aruna_core::structs::JobErrorKind::Permanent);
        assert_eq!(outputs.len(), MAX_OUTPUT_MANIFEST_OBJECTS);
    }
}
