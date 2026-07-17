use aruna_core::structs::JobRecord;
use aruna_storage::StorageHandle;

/// Reconciliation seam for lost external attempts. A lost lease or a node restart must
/// never blindly requeue an external attempt (that double-runs the container); the lease
/// sweep and restart recovery route such a job here instead. Stage 2 supplies the real
/// implementation (adopt the running attempt, commit its terminal outcome, or park the
/// job in `Indeterminate`); until then no reconciler is registered and the job is left
/// untouched for the next reconcile pass.
#[async_trait::async_trait]
pub trait ExternalReconciler: Send + Sync {
    async fn reconcile_lost_attempt(&self, storage: &StorageHandle, record: JobRecord);
}
