//! Cancellation-safe ownership for manually driven storage transactions: `finish`,
//! `unknown`, and `Drop` are the only ways ownership ends, each mapping to one
//! cleanup-registry transition. Cleanup registration itself stays in the handle.

use tracing::warn;
use ulid::Ulid;

use super::handle::StorageHandle;

/// Cancellation-safe owner for a manually driven storage transaction.
pub struct TransactionOwner {
    handle: StorageHandle,
    txn_id: Option<Ulid>,
}

impl TransactionOwner {
    pub fn new(handle: StorageHandle, txn_id: Ulid) -> Self {
        Self {
            handle,
            txn_id: Some(txn_id),
        }
    }

    pub fn id(&self) -> Option<Ulid> {
        self.txn_id
    }

    /// Releases ownership after storage proved the transaction terminal.
    pub fn finish(&mut self) {
        self.txn_id = None;
    }

    /// Retains an ambiguous commit without issuing an abort.
    pub fn unknown(&mut self) {
        if let Some(txn_id) = self.txn_id {
            if self.handle.retain_transaction(txn_id, true) {
                self.txn_id = None;
            } else {
                warn!(%txn_id, "Transaction cleanup handoff capacity reached");
            }
        }
    }
}

impl Drop for TransactionOwner {
    fn drop(&mut self) {
        if let Some(txn_id) = self.txn_id {
            if self.handle.retain_transaction(txn_id, false) {
                self.txn_id = None;
            } else {
                warn!(%txn_id, "Transaction cleanup handoff capacity reached");
            }
        }
    }
}
