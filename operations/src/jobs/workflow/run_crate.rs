use aruna_core::errors::AuthorizationError;
use aruna_core::structs::{
    Actor, AuthContext, ExecutionSpec, InputSource, JobError, JobId, JobPayload, JobRecord,
    JobResultPayload, JobState, MetadataRegistryRecord, Permission, RunCrateStatus,
};
use serde_json::json;
use ulid::Ulid;

/// Process Run Crate profile identifiers (0.5).
const CRATE_CONTEXT: &str = "https://w3id.org/ro/crate/1.2/context";
const WORKFLOW_RUN_CONTEXT: &str = "https://w3id.org/ro/terms/workflow-run/context";
const CRATE_PROFILE: &str = "https://w3id.org/ro/crate/1.2";
const PROCESS_PROFILE: &str = "https://w3id.org/ro/wfrun/process/0.5";
const WORKSPACE_PROPERTY: &str = "https://w3id.org/aruna/terms/workspace-bucket";

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

/// RO-Crate 1.2 JSON-LD conforming to the Process Run Crate 0.5 profile. No
/// secrets are included (spec 16.10).
fn build_run_crate_jsonld(record: &JobRecord, spec: &ExecutionSpec, document_id: Ulid) -> String {
    let root = format!("https://w3id.org/aruna/{document_id}");
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

    let workspace_property = workspace.as_deref().map(|bucket| {
        json!({
            "@id": "#workspace-bucket",
            "@type": "PropertyValue",
            "name": "Workspace bucket",
            "propertyID": WORKSPACE_PROPERTY,
            "value": bucket
        })
    });

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
        if workspace_property.is_some() {
            map.insert(
                "additionalProperty".to_string(),
                json!([{"@id": "#workspace-bucket"}]),
            );
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
            "conformsTo": {"@id": PROCESS_PROFILE},
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
            "@id": PROCESS_PROFILE,
            "@type": ["CreativeWork", "http://www.w3.org/ns/dx/prof/Profile"],
            "name": "Process Run Crate",
            "version": "0.5"
        }),
    ];
    graph.extend(files);
    if let Some(property) = workspace_property {
        graph.push(property);
    }

    json!({
        "@context": [
            CRATE_CONTEXT,
            WORKFLOW_RUN_CONTEXT,
            {
                "containerImage": "https://w3id.org/ro/terms/workflow-run#containerImage",
                "registry": "https://w3id.org/ro/terms/workflow-run#registry",
                "tag": "https://w3id.org/ro/terms/workflow-run#tag",
                "sha256": "https://w3id.org/ro/terms/workflow-run#sha256"
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
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::structs::{AttemptIntent, InputMode, InputSelection, OutputObject, RealmId};
    use serde_json::Value;

    fn execution_record() -> (JobRecord, ExecutionSpec) {
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
                dest_key: "inputs/in.txt".to_string(),
                mode: InputMode::Snapshot,
                container_path: None,
                name: Some("Input sample".to_string()),
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
                bucket: "src".to_string(),
                key: "out.txt".to_string(),
                container_path: "/work/out.txt".to_string(),
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

    fn entity_id(value: &Value) -> Option<&str> {
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
        assert_eq!(root["conformsTo"]["@id"], PROCESS_PROFILE);
        let profile = by_id(PROCESS_PROFILE);
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
    fn crate_survives_roundtrip() {
        // Storage must retain exact profile and workflow-run terms.
        let (record, spec) = execution_record();
        let document_id = Ulid::from_bytes(record.job_id.to_bytes());
        let root_id = format!("https://w3id.org/aruna/{document_id}");
        let jsonld = build_run_crate_jsonld(&record, &spec, document_id);
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
        assert_eq!(entity_id(&root["conformsTo"]), Some(PROCESS_PROFILE));
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
        let profile = by_id(PROCESS_PROFILE);
        assert!(has_type(profile, "http://www.w3.org/ns/dx/prof/Profile"));
        assert!(graph.iter().any(|entity| {
            entity["propertyID"] == WORKSPACE_PROPERTY && entity["value"] == "workspace-sample-one"
        }));
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
