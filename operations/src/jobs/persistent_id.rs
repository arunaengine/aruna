use aruna_core::structs::{AuthContext, JobError, JobResultPayload, MintPersistentIdSpec};
use aruna_core::util::unix_timestamp_millis;

use crate::metadata::MetadataAuthToken;
use crate::metadata::api::MetadataApiError;
use crate::metadata::forward::mint_pid_routed;

use crate::jobs::executor::{JobContext, JobRunOutcome};

/// Register a w3id PID for a document. The mint is a compare-and-set on the
/// document's authority, so a job that lost the race — or ran after a withdrawal —
/// reports the authoritative mapping with `newly_minted: false` instead of
/// overwriting it. Runs from wherever the job was claimed and routes; it never
/// mints into the claiming node's own store.
pub async fn run_mint_pid(
    ctx: &JobContext,
    spec: &MintPersistentIdSpec,
) -> JobRunOutcome {
    let realm_id = spec.minted_by.realm_id;
    // The submitting route requires an unrestricted realm token, so the internal
    // principal the authority re-checks carries no path restrictions to drop.
    let auth_token = MetadataAuthToken::internal(AuthContext {
        user_id: spec.minted_by,
        realm_id,
        path_restrictions: None,
    });
    match mint_pid_routed(
        &ctx.driver,
        realm_id,
        spec.document_id,
        spec.minted_by,
        unix_timestamp_millis(),
        Some(auth_token),
    )
    .await
    {
        Ok((mapping, newly_minted)) => JobRunOutcome::Succeeded(JobResultPayload::PersistentId {
            pid: mapping.pid,
            newly_minted,
        }),
        Err(
            error @ (MetadataApiError::NotFound
            | MetadataApiError::Unauthorized
            | MetadataApiError::Forbidden),
        ) => JobRunOutcome::Failed(JobError::permanent(format!("persistent id mint: {error}"))),
        Err(error) => {
            JobRunOutcome::Failed(JobError::retryable(format!("persistent id mint: {error}")))
        }
    }
}
