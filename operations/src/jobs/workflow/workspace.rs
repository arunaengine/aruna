use std::collections::HashSet;
use std::time::{Duration, SystemTime};

use aruna_core::errors::AuthorizationError;
use aruna_core::structs::{
    AuthContext, BackendLocation, BucketInfo, ExecutionSpec, InputSelection, InputSource, JobError,
    JobRecord, OutputObject, PathRestriction, Permission, blob_bucket_permission_path,
    blob_group_permission_path, blob_object_permission_path,
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
use crate::s3::head_object::{HeadObjectError, HeadObjectInput, HeadObjectOperation};
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
    .map_err(|error| JobError::retryable(format!("input stage failed: {error}")))?;
    Ok(())
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
                        key: object.head.key,
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
    keys: &mut HashSet<String>,
    output: OutputObject,
) -> Result<(), JobError> {
    if !keys.insert(output.key.clone()) {
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
            env: Default::default(),
            resources: Default::default(),
            executor_constraint: None,
            inputs: Vec::new(),
            output_prefixes,
        }
    }

    fn output(key: &str) -> OutputObject {
        OutputObject {
            key: key.to_string(),
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
