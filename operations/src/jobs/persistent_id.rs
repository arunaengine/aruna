use aruna_core::structs::{
    AuthContext, JobError, JobResultPayload, MintPersistentIdSpec, PersistentIdFailure,
};
use aruna_core::time::unix_timestamp_millis;

use crate::metadata::MetadataAuthToken;
use crate::metadata::api::MetadataApiError;
use crate::metadata::forward::{fail_pid_routed, mint_pid_routed};

use crate::jobs::executor::{JobContext, JobRunOutcome};

/// Register a w3id PID for a document. The mint is a compare-and-set on the
/// document's authority, so a lost race or post-withdrawal run reports the
/// authoritative mapping with `newly_minted: false`; routing never mints locally.
pub async fn run_mint_pid(ctx: &JobContext, spec: &MintPersistentIdSpec) -> JobRunOutcome {
    let realm_id = spec.minted_by.realm_id;
    // The submitting route requires an unrestricted realm token, so the internal
    // principal the authority re-checks carries no path restrictions to drop.
    let auth_token = MetadataAuthToken::internal(AuthContext {
        user_id: spec.minted_by,
        realm_id,
        path_restrictions: None,
        session: None,
    });
    match mint_pid_routed(
        &ctx.driver,
        realm_id,
        spec.document_id,
        spec.minted_by,
        unix_timestamp_millis(),
        Some(auth_token.clone()),
    )
    .await
    {
        Ok((mapping, newly_minted)) if mapping.is_active() || mapping.is_retired() => {
            JobRunOutcome::Succeeded(JobResultPayload::PersistentId {
                pid: mapping.pid,
                newly_minted,
            })
        }
        Ok((_, _)) | Err(MetadataApiError::NotFound) => JobRunOutcome::Deferred(
            JobError::retryable("persistent id mint is waiting for metadata projection"),
        ),
        Err(error @ (MetadataApiError::Unauthorized | MetadataApiError::Forbidden)) => {
            record_failure(ctx, spec, &auth_token, error.to_string(), false).await;
            JobRunOutcome::Failed(JobError::permanent(format!("persistent id mint: {error}")))
        }
        Err(error) => {
            if ctx.final_attempt {
                record_failure(ctx, spec, &auth_token, error.to_string(), true).await;
            }
            JobRunOutcome::Failed(JobError::retryable(format!("persistent id mint: {error}")))
        }
    }
}

async fn record_failure(
    ctx: &JobContext,
    spec: &MintPersistentIdSpec,
    auth_token: &MetadataAuthToken,
    message: String,
    retryable: bool,
) {
    let _ = fail_pid_routed(
        &ctx.driver,
        spec.minted_by.realm_id,
        spec.document_id,
        PersistentIdFailure {
            message,
            retryable,
            recorded_at_ms: unix_timestamp_millis(),
        },
        auth_token.clone(),
    )
    .await;
}
