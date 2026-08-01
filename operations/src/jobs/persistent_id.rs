use aruna_core::structs::{JobError, JobResultPayload, MintPersistentIdSpec};
use aruna_core::util::unix_timestamp_millis;

use crate::jobs::executor::{JobContext, JobRunOutcome};
use crate::persistent_id::mint_persistent_id;

/// Register a w3id PID for a document. Idempotent by document id: a re-mint
/// returns the same PID and reports `newly_minted: false`.
pub async fn run_mint_persistent_id(
    ctx: &JobContext,
    spec: &MintPersistentIdSpec,
) -> JobRunOutcome {
    match mint_persistent_id(
        &ctx.driver,
        spec.document_id,
        spec.minted_by,
        unix_timestamp_millis(),
    )
    .await
    {
        Ok((mapping, newly_minted)) => JobRunOutcome::Succeeded(JobResultPayload::PersistentId {
            pid: mapping.pid,
            newly_minted,
        }),
        Err(error) => {
            JobRunOutcome::Failed(JobError::retryable(format!("persistent id mint: {error}")))
        }
    }
}
