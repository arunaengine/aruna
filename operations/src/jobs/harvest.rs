use aruna_core::structs::{HarvestJobSpec, JobError};

use crate::jobs::executor::{JobContext, JobRunOutcome};

/// Run one harvest of a repository source.
///
/// Framework seam only: the harvester protocol that fetches and applies records
/// lands with the first harvester kind (OAI-PMH). Until then a submitted harvest
/// fails permanently rather than silently reporting success for zero work.
pub async fn run_harvest_job(_ctx: &JobContext, _spec: &HarvestJobSpec) -> JobRunOutcome {
    JobRunOutcome::Failed(JobError::permanent(
        "harvest protocol not yet available: no harvester kind is registered",
    ))
}
