//! Transaction-scoped connector reference scan shared by the delete and
//! replace operations. The stored state enums stay distinct; only the
//! transaction and page control flow lives here.

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::types::{Effects, Key, TxnId};
use smallvec::smallvec;
use ulid::Ulid;

use crate::connectors::repository::{
    StorageReadError, blob_version_references_connector, iter_connector_reference_versions_effect,
    parse_blob_version_iter,
};

/// Shared state transition an operation maps onto its own state enum.
pub(crate) enum ReferenceScanPhase {
    Scan,
    Abort,
    Error,
}

/// Transaction abort/commit and reference-scan flow shared by operations that
/// scan connector references before mutating their records.
pub(crate) trait ReferenceScan: Operation + Sized
where
    Self::Error: From<StorageReadError> + From<StorageError>,
{
    fn scan_connector_id(&self) -> Ulid;
    fn scan_txn_id(&mut self) -> &mut Option<TxnId>;
    fn scan_phase(&mut self, phase: ReferenceScanPhase);
    fn scan_error_output(&mut self, error: Self::Error);
    fn scan_referenced(&self) -> Self::Error;
    fn invalid_event(&self, expected: &'static str, received: Event) -> Self::Error;
    fn scan_done(&mut self) -> Effects;
    fn commit_done(&mut self) -> Effects;

    fn emit_error(&mut self, error: Self::Error) -> Effects {
        self.scan_phase(ReferenceScanPhase::Error);
        self.scan_error_output(error);
        smallvec![]
    }

    fn abort_with_error(&mut self, error: Self::Error) -> Effects {
        let Some(txn_id) = self.scan_txn_id().take() else {
            return self.emit_error(error);
        };

        self.scan_phase(ReferenceScanPhase::Abort);
        self.scan_error_output(error);
        smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
    }

    fn fail_or_abort(&mut self, error: Self::Error) -> Effects {
        if self.scan_txn_id().is_some() {
            self.abort_with_error(error)
        } else {
            self.emit_error(error)
        }
    }

    fn current_txn_id(&mut self) -> Result<TxnId, Effects> {
        match *self.scan_txn_id() {
            Some(txn_id) => Ok(txn_id),
            None => Err(self.emit_error(StorageError::TransactionNotFound.into())),
        }
    }

    fn scan_reference_versions(&mut self, start_after: Option<Key>) -> Effects {
        let txn_id = match self.current_txn_id() {
            Ok(txn_id) => txn_id,
            Err(effects) => return effects,
        };

        self.scan_phase(ReferenceScanPhase::Scan);
        smallvec![iter_connector_reference_versions_effect(
            start_after,
            Some(txn_id),
        )]
    }

    fn handle_reference_versions_scanned(&mut self, event: Event) -> Effects {
        let (versions, next_start_after) = match parse_blob_version_iter(event) {
            Ok(result) => result,
            Err(error) => return self.abort_with_error(error.into()),
        };

        if versions
            .iter()
            .any(|version| blob_version_references_connector(version, self.scan_connector_id()))
        {
            return self.abort_with_error(self.scan_referenced());
        }

        if let Some(start_after) = next_start_after {
            return self.scan_reference_versions(Some(start_after));
        }

        self.scan_done()
    }

    fn handle_transaction_committed(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                *self.scan_txn_id() = None;
                self.commit_done()
            }
            Event::Storage(StorageEvent::Error { error }) => {
                *self.scan_txn_id() = None;
                self.emit_error(error.into())
            }
            received => self.fail_or_abort(self.invalid_event(
                "Event::Storage(StorageEvent::TransactionCommitted)",
                received,
            )),
        }
    }

    fn handle_transaction_aborted(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionAborted { .. })
            | Event::Storage(StorageEvent::Error { .. }) => {
                self.scan_phase(ReferenceScanPhase::Error);
                smallvec![]
            }
            received => self.emit_error(
                self.invalid_event("Event::Storage(StorageEvent::TransactionAborted)", received),
            ),
        }
    }
}
