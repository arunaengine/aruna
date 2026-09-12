use aruna_core::structs::JobRecord;
use aruna_storage::StorageHandle;

/// Reconciliation seam for lost external attempts. A lost lease or node restart
/// must never blindly requeue one (that double-runs the container); the sweep and
/// recovery route it here. Until a reconciler is registered the job is untouched.
#[async_trait::async_trait]
pub trait ExternalReconciler: Send + Sync {
    async fn reconcile_lost_attempt(&self, storage: &StorageHandle, record: JobRecord);
}
