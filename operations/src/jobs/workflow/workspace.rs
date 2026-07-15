use std::time::{Duration, SystemTime};

use aruna_core::structs::{
    AuthContext, BucketInfo, ExecutionSpec, InputSelection, InputSource, JobError, JobRecord,
    OutputObject, PathRestriction, Permission, blob_bucket_permission_path,
    blob_object_permission_path,
};
use aruna_core::types::NodeId;
use ulid::Ulid;

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::driver::{DriverContext, drive};
use crate::get_realm_config::GetRealmConfigOperation;
use crate::s3::create_bucket::{CreateBucketError, CreateBucketOperation};
use crate::s3::create_user_access::{CreateUserAccessConfig, CreateUserAccessOperation};
use crate::s3::get_bucket_info::GetBucketInfoOperation;
use crate::s3::get_object::{GetObjectInput, GetObjectOperation};
use crate::s3::list_objects_v2::{ListObjectsV2Input, ListObjectsV2Operation};
use crate::s3::put_object::{PutObjectConfig, PutObjectInput, PutObjectOperation};

/// Credential lifetime past the walltime so a slow finalize still authorizes.
const CREDENTIAL_SLACK: Duration = Duration::from_secs(6 * 60 * 60);
const MAX_OUTPUT_MANIFEST_OBJECTS: usize = 10_000;

/// Minted workspace S3 credential handed to the container.
pub struct WorkspaceCredential {
    pub access_key: String,
    pub secret: String,
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
    let realm_id = record.created_by.realm_id;
    // A single WRITE restriction over the bucket subtree; WRITE also satisfies READ.
    let pattern = format!(
        "{}/**",
        blob_bucket_permission_path(realm_id, spec.group_id, node_id, bucket)
    );
    let walltime = spec
        .resources
        .max_walltime_ms
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_secs(24 * 60 * 60));
    let expiry = SystemTime::now() + walltime + CREDENTIAL_SLACK;
    let (_, access) = drive(
        CreateUserAccessOperation::new(CreateUserAccessConfig {
            user_identity: record.created_by,
            group_id: spec.group_id,
            expiry,
            path_restrictions: Some(vec![PathRestriction {
                pattern,
                permission: Permission::WRITE,
            }]),
            issued_by: *node_id.as_bytes(),
        }),
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
        .map_err(|error| JobError::permanent(format!("input bucket lookup failed: {error}")))?
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
    .map_err(|error| JobError::permanent(format!("input authorization failed: {error}")))?;
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
    .map_err(|error| JobError::permanent(format!("input read failed: {error}")))?
    .ok_or_else(|| JobError::permanent(format!("input {src_bucket}/{src_key} not found")))?;

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
    .map_err(|error| JobError::retryable(format!("input stage failed: {error}")))?;
    Ok(())
}

/// Inventory the declared output prefixes in the workspace at completion. Missing
/// prefixes contribute nothing; a listing failure is retryable.
pub async fn collect_outputs(
    context: &DriverContext,
    spec: &ExecutionSpec,
    bucket: &str,
) -> Result<Vec<OutputObject>, JobError> {
    let mut outputs = Vec::new();
    let prefixes: Vec<Option<String>> = if spec.output_prefixes.is_empty() {
        vec![None]
    } else {
        spec.output_prefixes.iter().cloned().map(Some).collect()
    };
    for prefix in prefixes {
        let mut continuation = None;
        loop {
            let result = drive(
                ListObjectsV2Operation::new(ListObjectsV2Input {
                    bucket: bucket.to_string(),
                    group_id: spec.group_id,
                    continuation_token: continuation.clone(),
                    max_keys: None,
                    prefix: prefix.clone(),
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
                if outputs.len() >= MAX_OUTPUT_MANIFEST_OBJECTS {
                    return Ok(outputs);
                }
                let (size, digest) = match object.location {
                    Some(location) => (location.blob_size, location.get_blake3().map(hex_encode)),
                    None => (0, None),
                };
                outputs.push(OutputObject {
                    key: object.head.key,
                    size,
                    digest,
                });
            }
            match result.continuation_token {
                Some(token) => continuation = Some(token),
                None => break,
            }
        }
    }
    Ok(outputs)
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}
