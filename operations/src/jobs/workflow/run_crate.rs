use aruna_core::errors::AuthorizationError;
use aruna_core::structs::{
    Actor, AuthContext, ExecutionSpec, InputSource, JobError, JobId, JobPayload, JobRecord,
    JobResultPayload, JobState, MetadataRegistryRecord, Permission, RunCrateStatus,
};
use serde_json::json;
use ulid::Ulid;

/// Workflow Run RO-Crate "Process Run Crate" profile identifiers (0.5).
const CRATE_CONTEXT: &str = "https://w3id.org/ro/crate/1.2/context";
const WORKFLOW_RUN_CONTEXT: &str = "https://w3id.org/ro/terms/workflow-run/context";
const CRATE_PROFILE: &str = "https://w3id.org/ro/crate/1.2";
const PROCESS_PROFILE: &str = "https://w3id.org/ro/wfrun/process/0.5";

use super::super::executor::{JobContext, JobRunOutcome};
use super::super::store::{put_run_crate_status, read_job_record, read_run_crate_status};
use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentError, CreateMetadataDocumentOperation,
    CreateMetadataDocumentPayload,
};
use crate::driver::drive;
use crate::metadata::MetadataAuthToken;
use crate::metadata::forward::{MetadataWriteError, create_metadata_document_routed};

/// Run the follow-on run-crate obligation for a finished execution job. A failure
/// records a durable status and never fails the parent; this internal job succeeds
/// once the outcome is recorded so it is not retried forever on a permanent error.
pub async fn run_write_run_crate(ctx: &JobContext, for_job: JobId) -> JobRunOutcome {
    let context = ctx.driver.as_ref();
    let storage = &context.storage_handle;

    // A re-driven crate job returns a previously recorded outcome without recreating it.
    match read_run_crate_status(storage, for_job).await {
        Ok(Some(RunCrateStatus::Written { resource })) => {
            return JobRunOutcome::Succeeded(JobResultPayload::RunCrate { resource });
        }
        Ok(Some(RunCrateStatus::Pending)) | Ok(None) => {}
        Ok(Some(status)) => {
            return JobRunOutcome::Succeeded(JobResultPayload::RunCrate {
                resource: status.name().to_string(),
            });
        }
        Err(error) => {
            return JobRunOutcome::Failed(JobError::retryable(format!(
                "run crate read status failed: {error}"
            )));
        }
    }

    let parent = match read_job_record(storage, for_job, None).await {
        Ok(Some(record)) => record,
        Ok(None) => {
            // Parent pruned before the crate ran; nothing to write.
            return JobRunOutcome::Succeeded(JobResultPayload::RunCrate {
                resource: "parent-gone".to_string(),
            });
        }
        Err(error) => {
            return JobRunOutcome::Failed(JobError::retryable(format!(
                "run crate read parent failed: {error}"
            )));
        }
    };

    let JobPayload::Execution(spec) = &parent.payload else {
        return JobRunOutcome::Succeeded(JobResultPayload::RunCrate {
            resource: "not-execution".to_string(),
        });
    };

    let Some(net_handle) = context.net_handle.as_ref() else {
        return JobRunOutcome::Failed(JobError::retryable("run crate needs a net handle"));
    };
    let document_id = Ulid::from_bytes(for_job.to_bytes());
    let document_path = format!("runs/{for_job}");
    let node_id = net_handle.node_id();
    let actor = Actor {
        node_id,
        user_id: parent.created_by,
        realm_id: parent.created_by.realm_id,
    };
    let denied = match drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: AuthContext {
                user_id: parent.created_by,
                realm_id: parent.created_by.realm_id,
                path_restrictions: None,
            },
            path: MetadataRegistryRecord::permission_path_for(
                &parent.created_by.realm_id,
                spec.group_id,
                &document_path,
                document_id,
            ),
            required_permission: Permission::WRITE,
        }),
        context,
    )
    .await
    {
        Ok(allowed) => !allowed,
        Err(
            AuthorizationError::InvalidRealmId
            | AuthorizationError::InvalidGroupId
            | AuthorizationError::GroupNotFound
            | AuthorizationError::AuthDocNotFound,
        ) => true,
        Err(error) => {
            return JobRunOutcome::Failed(JobError::retryable(format!(
                "run crate authorization failed: {error}"
            )));
        }
    };
    if denied {
        let status = RunCrateStatus::Denied {
            message: "metadata write access denied".to_string(),
        };
        if let Err(error) = put_run_crate_status(storage, for_job, &status).await {
            return JobRunOutcome::Failed(JobError::retryable(format!(
                "run crate status write failed: {error}"
            )));
        }
        return JobRunOutcome::Succeeded(JobResultPayload::RunCrate {
            resource: status.name().to_string(),
        });
    }

    let jsonld = build_run_crate_jsonld(&parent, spec, document_id);

    let (status, resource) = match create_metadata_document_routed(
        CreateMetadataDocumentOperation::new_for_generated_document_id(
            CreateMetadataDocumentConfig {
                actor,
                group_id: spec.group_id,
                document_id,
                document_path,
                public: false,
                payload: CreateMetadataDocumentPayload::RoCrate { jsonld },
            },
        ),
        ctx.driver.clone(),
        Some(MetadataAuthToken::internal(
            parent.created_by,
            parent.created_by.realm_id,
        )),
    )
    .await
    {
        Ok(result) => {
            let resource = result.record.document_id.to_string();
            (
                RunCrateStatus::Written {
                    resource: resource.clone(),
                },
                resource,
            )
        }
        // An invalid crate is permanent: record it and discharge the
        // obligation so it is not retried forever. Transient errors retry.
        Err(error) => {
            if is_transient(&error) {
                return JobRunOutcome::Failed(JobError::retryable(format!(
                    "run crate write failed: {error}"
                )));
            }
            let status = RunCrateStatus::Failed {
                message: error.to_string(),
            };
            let resource = status.name().to_string();
            (status, resource)
        }
    };
    if let Err(error) = put_run_crate_status(storage, for_job, &status).await {
        return JobRunOutcome::Failed(JobError::retryable(format!(
            "run crate status write failed: {error}"
        )));
    }
    JobRunOutcome::Succeeded(JobResultPayload::RunCrate { resource })
}

fn is_transient(error: &MetadataWriteError) -> bool {
    matches!(
        error,
        MetadataWriteError::Create(
            CreateMetadataDocumentError::StorageError(_)
                | CreateMetadataDocumentError::MetadataError(_)
        ) | MetadataWriteError::Undeliverable(_)
    )
}

/// RO-Crate 1.2 JSON-LD conforming to the Workflow Run RO-Crate "Process Run
/// Crate" profile: a metadata descriptor, the run `Dataset` (conforming to the
/// crate and process profiles and mentioning the action), a `CreateAction`
/// binding agent/instrument/object/result/status/times, the instrument
/// `SoftwareApplication`, and the input/output `File` entities. No secrets
/// (spec 16.10).
fn build_run_crate_jsonld(record: &JobRecord, spec: &ExecutionSpec, document_id: Ulid) -> String {
    let root = format!("https://w3id.org/aruna/{document_id}");
    let action_id = format!("#run-{}", record.job_id);
    let agent_id = format!("#agent-{}", record.created_by);
    let workspace = record
        .workspace_bucket
        .clone()
        .unwrap_or_else(|| JobRecord::workspace_bucket_name(record.job_id));

    let (exit_code, outputs) = match &record.result {
        Some(JobResultPayload::Execution {
            exit_code, outputs, ..
        }) => (*exit_code, outputs.clone()),
        _ => (None, Vec::new()),
    };
    let action_status = if record.state == JobState::Succeeded {
        "http://schema.org/CompletedActionStatus"
    } else {
        "http://schema.org/FailedActionStatus"
    };

    let mut command = spec.entrypoint.clone().unwrap_or_default();
    command.extend(spec.command.iter().cloned());

    // Inputs are the action `object`, keyed by their S3 source URL.
    let mut object_ids = Vec::with_capacity(spec.inputs.len());
    let mut part_ids = Vec::new();
    let mut files = Vec::new();
    for input in &spec.inputs {
        let InputSource::S3 { bucket, key, .. } = &input.source;
        let id = format!("s3://{bucket}/{key}");
        let name = input.name.clone().unwrap_or_else(|| input.dest_key.clone());
        object_ids.push(json!({ "@id": id.clone() }));
        files.push(json!({ "@id": id.clone(), "@type": "File", "name": name }));
        part_ids.push(id);
    }

    // Outputs are the action `result`, keyed by their S3 destination URL.
    let mut result_ids = Vec::with_capacity(outputs.len());
    for output in &outputs {
        let id = format!("s3://{}/{}", output.bucket, output.key);
        result_ids.push(json!({ "@id": id.clone() }));
        files.push(json!({
            "@id": id.clone(),
            "@type": "File",
            "name": output.key.clone(),
            "contentSize": output.size.to_string()
        }));
        part_ids.push(id);
    }
    let has_part: Vec<serde_json::Value> = part_ids
        .iter()
        .map(|id| json!({ "@id": id.clone() }))
        .collect();

    let mut action = json!({
        "@id": action_id.clone(),
        "@type": "CreateAction",
        "name": format!("execution of {}", spec.image),
        "agent": {"@id": agent_id.clone()},
        "instrument": {"@id": spec.image.clone()},
        "object": object_ids,
        "result": result_ids,
        "actionStatus": {"@id": action_status},
        "command": command
    });
    if let Some(map) = action.as_object_mut() {
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

    let (image_name, image_version) = split_image(&spec.image);
    let mut software = json!({
        "@id": spec.image.clone(),
        "@type": "SoftwareApplication",
        "name": image_name
    });
    if let (Some(map), Some(version)) = (software.as_object_mut(), image_version) {
        map.insert("softwareVersion".to_string(), json!(version));
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
            "name": format!("Run {}", record.job_id),
            "description": format!(
                "Aruna execution run for job {} in workspace {}",
                record.job_id, workspace
            ),
            "datePublished": rfc3339(record.created_at_ms),
            "license": {"@id": "https://creativecommons.org/licenses/by/4.0/"},
            "conformsTo": [{"@id": CRATE_PROFILE}, {"@id": PROCESS_PROFILE}],
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
    ];
    graph.extend(files);

    json!({
        "@context": [CRATE_CONTEXT, WORKFLOW_RUN_CONTEXT],
        "@graph": graph
    })
    .to_string()
}

/// Split a container image reference into a `SoftwareApplication` name and
/// optional version: a digest after `@`, otherwise a trailing `:tag` that holds
/// no path separator (so a `host:port/image` registry prefix is not a version).
fn split_image(image: &str) -> (String, Option<String>) {
    if let Some((name, digest)) = image.split_once('@') {
        return (name.to_string(), Some(digest.to_string()));
    }
    if let Some((name, tag)) = image.rsplit_once(':')
        && !tag.contains('/')
    {
        return (name.to_string(), Some(tag.to_string()));
    }
    (image.to_string(), None)
}

fn rfc3339(ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(ms as i64)
        .map(|timestamp| timestamp.to_rfc3339())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::structs::{InputMode, InputSelection, OutputObject, RealmId};

    fn execution_record() -> (JobRecord, ExecutionSpec) {
        // Succeeded run with one S3 input and one produced output.
        let realm = RealmId([1; 32]);
        let user = UserId::new(Ulid::from_bytes([2; 16]), realm);
        let node = iroh::SecretKey::from_bytes(&[3; 32]).public();
        let spec = ExecutionSpec {
            group_id: Ulid::from_bytes([4; 16]),
            name: None,
            description: None,
            tags: Default::default(),
            image: "busybox:1.36".to_string(),
            entrypoint: Some(vec!["/bin/sh".to_string()]),
            command: vec!["-c".to_string(), "true".to_string()],
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
                dest_key: "inputs/in.txt".to_string(),
                mode: InputMode::Snapshot,
                container_path: None,
                name: None,
                description: None,
            }],
            file_outputs: Vec::new(),
            workspace_outputs: Vec::new(),
            output_prefixes: Vec::new(),
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
        record.result = Some(JobResultPayload::Execution {
            exit_code: Some(0),
            workspace_bucket: JobRecord::workspace_bucket_name(job_id),
            outputs: vec![OutputObject {
                bucket: "src".to_string(),
                key: "out.txt".to_string(),
                container_path: "/out.txt".to_string(),
                size: 7,
                digest: None,
            }],
            stdout: String::new(),
            stderr: String::new(),
        });
        (record, spec)
    }

    fn parse(record: &JobRecord, spec: &ExecutionSpec) -> serde_json::Value {
        let doc = Ulid::from_bytes(record.job_id.to_bytes());
        serde_json::from_str(&build_run_crate_jsonld(record, spec, doc)).unwrap()
    }

    #[test]
    fn crate_declares_profiles() {
        // Root Dataset must conform to both the crate and process profiles.
        let (record, spec) = execution_record();
        let value = parse(&record, &spec);
        assert_eq!(value["@context"][0], CRATE_CONTEXT);
        assert_eq!(value["@context"][1], WORKFLOW_RUN_CONTEXT);
        let graph = value["@graph"].as_array().unwrap();
        let by_id = |id: &str| graph.iter().find(|e| e["@id"] == id).expect("entity");
        let descriptor = by_id("ro-crate-metadata.json");
        assert_eq!(descriptor["conformsTo"]["@id"], CRATE_PROFILE);
        let root_id = descriptor["about"]["@id"].as_str().unwrap().to_string();
        let root = by_id(&root_id);
        let conforms: Vec<&str> = root["conformsTo"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["@id"].as_str().unwrap())
            .collect();
        assert!(conforms.contains(&CRATE_PROFILE));
        assert!(conforms.contains(&PROCESS_PROFILE));
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
        let instrument = action["instrument"]["@id"].as_str().unwrap();
        assert_eq!(instrument, "busybox:1.36");
        let software = by_id(instrument);
        assert_eq!(software["@type"], "SoftwareApplication");
        assert_eq!(software["name"], "busybox");
        assert_eq!(software["softwareVersion"], "1.36");
        assert_eq!(action["object"][0]["@id"], "s3://src/in.txt");
        assert_eq!(action["result"][0]["@id"], "s3://src/out.txt");
        assert_eq!(
            action["actionStatus"]["@id"],
            "http://schema.org/CompletedActionStatus"
        );
        assert_eq!(action["command"][0], "/bin/sh");
        assert!(action["startTime"].is_string());
        assert!(action["endTime"].is_string());
        let agent = by_id(action["agent"]["@id"].as_str().unwrap());
        assert_eq!(agent["@type"], "Person");
        assert_eq!(by_id("s3://src/in.txt")["@type"], "File");
        assert_eq!(by_id("s3://src/out.txt")["contentSize"], "7");
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
