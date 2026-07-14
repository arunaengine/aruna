use aruna_core::errors::AuthorizationError;
use aruna_core::structs::{
    Actor, AuthContext, ExecutionSpec, JobError, JobId, JobPayload, JobRecord, JobResultPayload,
    OutputObject, Permission, RunCrateStatus,
};
use serde_json::json;
use ulid::Ulid;

use super::super::executor::{JobContext, JobRunOutcome};
use super::super::store::{put_run_crate_status, read_job_record, read_run_crate_status};
use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentError, CreateMetadataDocumentOperation,
    CreateMetadataDocumentPayload,
};
use crate::driver::drive;

/// Run the follow-on run-crate obligation for a finished execution job. A failure
/// records a durable status and never fails the parent; this internal job succeeds
/// once the outcome is recorded so it is not retried forever on a permanent error.
pub async fn run_write_run_crate(ctx: &JobContext, for_job: JobId) -> JobRunOutcome {
    let context = ctx.driver.as_ref();
    let storage = &context.storage_handle;

    // A re-driven crate job must not mint a second document_id and write a duplicate
    // metadata document; the durable status is the idempotency record, not the dedup key.
    match read_run_crate_status(storage, for_job).await {
        Ok(Some(RunCrateStatus::Written { resource })) => {
            return JobRunOutcome::Succeeded(JobResultPayload::RunCrate { resource });
        }
        Ok(_) => {}
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
            path: format!(
                "/{}/g/{}/meta/**",
                parent.created_by.realm_id, spec.group_id
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
        let _ = put_run_crate_status(storage, for_job, &status).await;
        return JobRunOutcome::Succeeded(JobResultPayload::RunCrate {
            resource: status.name().to_string(),
        });
    }

    let document_id = Ulid::r#gen();
    let jsonld = build_run_crate_jsonld(&parent, spec, document_id);
    let document_path = format!("runs/{for_job}");

    match drive(
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
        context,
    )
    .await
    {
        Ok(result) => {
            let resource = result.record.document_id.to_string();
            let _ = put_run_crate_status(
                storage,
                for_job,
                &RunCrateStatus::Written {
                    resource: resource.clone(),
                },
            )
            .await;
            JobRunOutcome::Succeeded(JobResultPayload::RunCrate { resource })
        }
        // A denial or invalid crate is permanent: record it and discharge the
        // obligation so it is not retried forever. Transient errors retry.
        Err(error) => {
            if is_transient(&error) {
                return JobRunOutcome::Failed(JobError::retryable(format!(
                    "run crate write failed: {error}"
                )));
            }
            let status = if is_denial(&error) {
                RunCrateStatus::Denied {
                    message: error.to_string(),
                }
            } else {
                RunCrateStatus::Failed {
                    message: error.to_string(),
                }
            };
            let _ = put_run_crate_status(storage, for_job, &status).await;
            JobRunOutcome::Succeeded(JobResultPayload::RunCrate {
                resource: status.name().to_string(),
            })
        }
    }
}

fn is_transient(error: &CreateMetadataDocumentError) -> bool {
    matches!(
        error,
        CreateMetadataDocumentError::StorageError(_)
            | CreateMetadataDocumentError::MetadataError(_)
    )
}

fn is_denial(error: &CreateMetadataDocumentError) -> bool {
    // The create operation records the actor but does not itself enforce
    // permissions; a genuine denial surfaces from the metadata plane. Treat an
    // already-existing document as idempotent success handled by the caller.
    matches!(error, CreateMetadataDocumentError::DocumentAlreadyExists)
}

/// Hand-rolled RO-Crate JSON-LD (D12): a metadata descriptor, the run Dataset, and
/// a `CreateAction` binding agent/instrument/object/result. No secrets (spec 16.10).
fn build_run_crate_jsonld(record: &JobRecord, spec: &ExecutionSpec, document_id: Ulid) -> String {
    let root = format!("https://w3id.org/aruna/{document_id}");
    let workspace = record
        .workspace_bucket
        .clone()
        .unwrap_or_else(|| JobRecord::workspace_bucket_name(record.job_id));
    let (status, exit_code, outputs) = match &record.result {
        Some(JobResultPayload::Execution {
            exit_code, outputs, ..
        }) => (record.state.name(), *exit_code, outputs.clone()),
        _ => (record.state.name(), None, Vec::new()),
    };
    let action_status = if record.state.name() == "succeeded" {
        "CompletedActionStatus"
    } else {
        "FailedActionStatus"
    };

    let object_ids: Vec<serde_json::Value> = spec
        .inputs
        .iter()
        .map(|input| json!({ "@id": format!("workspace/{}", input.dest_key) }))
        .collect();
    let result_ids: Vec<serde_json::Value> = outputs
        .iter()
        .map(|output: &OutputObject| json!({ "@id": format!("workspace/{}", output.key) }))
        .collect();

    let mut graph = vec![
        json!({
            "@id": "ro-crate-metadata.json",
            "@type": "CreativeWork",
            "conformsTo": {"@id": "https://w3id.org/ro/crate/1.1"},
            "about": {"@id": root}
        }),
        json!({
            "@id": root,
            "@type": "Dataset",
            "name": format!("Run {}", record.job_id),
            "description": format!(
                "Aruna execution run for job {} in workspace {}",
                record.job_id, workspace
            ),
            "datePublished": "2026-01-01",
            "license": {"@id": "https://creativecommons.org/licenses/by/4.0/"},
            "mentions": {"@id": format!("#run-{}", record.job_id)}
        }),
        json!({
            "@id": format!("#run-{}", record.job_id),
            "@type": "CreateAction",
            "name": format!("execution of {}", spec.image),
            "agent": {"@id": format!("#agent-{}", record.created_by)},
            "instrument": spec.image,
            "object": object_ids,
            "result": result_ids,
            "actionStatus": action_status,
            "endTime": "2026-01-01",
            "error": exit_code
                .filter(|code| *code != 0)
                .map(|code| format!("exit code {code}"))
                .unwrap_or_default()
        }),
        json!({
            "@id": format!("#agent-{}", record.created_by),
            "@type": "Person",
            "identifier": record.created_by.to_string()
        }),
    ];
    for output in &outputs {
        graph.push(json!({
            "@id": format!("workspace/{}", output.key),
            "@type": "File",
            "name": output.key,
            "contentSize": output.size.to_string()
        }));
    }
    let _ = status;

    json!({
        "@context": "https://w3id.org/ro/crate/1.1/context",
        "@graph": graph
    })
    .to_string()
}
