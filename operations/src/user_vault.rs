use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::USER_VAULT_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::{MAX_USER_VAULT_BYTES, UserVault};
use aruna_core::types::{Effects, Key, TxnId, UserId, Value};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;

pub const VAULT_CAP: &str = "a vault payload may hold at most 64 KiB";

fn vault_key(user_id: UserId) -> Key {
    ByteView::from(user_id.to_storage_key())
}

fn read_vault(user_id: UserId, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: USER_VAULT_KEYSPACE.to_string(),
        key: vault_key(user_id),
        txn_id,
    })
}

fn decode_vault(value: Option<Value>) -> Result<Option<UserVault>, VaultStoreError> {
    Ok(value
        .map(|bytes| UserVault::from_bytes(bytes.as_ref()))
        .transpose()?)
}

fn write_vault(vault: &UserVault, txn_id: TxnId) -> Result<Effect, ConversionError> {
    Ok(Effect::Storage(StorageEffect::Write {
        key_space: USER_VAULT_KEYSPACE.to_string(),
        key: vault_key(vault.user_id),
        value: vault.to_bytes()?.into(),
        txn_id: Some(txn_id),
    }))
}

fn abort_effects(txn_id: &mut Option<TxnId>) -> Effects {
    txn_id
        .take()
        .map_or_else(smallvec::SmallVec::new, |txn_id| {
            smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
        })
}

#[derive(Debug, Error, PartialEq)]
pub enum VaultStoreError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("{0}")]
    TooLarge(&'static str),
    #[error("vault changed in another browser")]
    Stale,
    #[error("vault operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

fn unexpected(
    state: &impl std::fmt::Debug,
    expected: &'static str,
    event: &Event,
) -> VaultStoreError {
    VaultStoreError::UnexpectedEvent {
        state: format!("{state:?}"),
        expected,
        got: format!("{event:?}"),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ReadVaultState {
    Init,
    ReadVault,
    Finish,
    Error,
}

/// Reads the vault of one user; `None` before the first save.
#[derive(Debug, PartialEq)]
pub struct ReadVaultOperation {
    user_id: UserId,
    state: ReadVaultState,
    output: Option<Result<Option<UserVault>, VaultStoreError>>,
}

impl ReadVaultOperation {
    pub fn new(user_id: UserId) -> Self {
        Self {
            user_id,
            state: ReadVaultState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: VaultStoreError) -> Effects {
        self.state = ReadVaultState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }
}

impl Operation for ReadVaultOperation {
    type Output = Option<UserVault>;
    type Error = VaultStoreError;

    fn start(&mut self) -> Effects {
        self.state = ReadVaultState::ReadVault;
        smallvec![read_vault(self.user_id, None)]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match self.state {
            ReadVaultState::Init => self.start(),
            ReadVaultState::ReadVault => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(unexpected(&self.state, "vault read", &event));
                };
                self.state = ReadVaultState::Finish;
                self.output = Some(decode_vault(value));
                smallvec![]
            }
            ReadVaultState::Finish | ReadVaultState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ReadVaultState::Finish | ReadVaultState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(VaultStoreError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum WriteVaultState {
    Init,
    StartTransaction,
    ReadVault,
    WriteVault,
    CommitTransaction,
    Finish,
    Error,
}

/// Stores the vault of one user. A write is refused when the caller read an
/// older revision than the node holds, so a second browser cannot silently
/// drop what the first one saved.
#[derive(Debug, PartialEq)]
pub struct WriteVaultOperation {
    user_id: UserId,
    payload: String,
    expected_revision: Option<u64>,
    now: u64,
    txn_id: Option<TxnId>,
    state: WriteVaultState,
    output: Option<Result<UserVault, VaultStoreError>>,
}

impl WriteVaultOperation {
    pub fn new(user_id: UserId, payload: String, expected_revision: Option<u64>, now: u64) -> Self {
        Self {
            user_id,
            payload,
            expected_revision,
            now,
            txn_id: None,
            state: WriteVaultState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: VaultStoreError) -> Effects {
        let cleanup = self.abort();
        self.state = WriteVaultState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn handle_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.fail(unexpected(&self.state, "transaction started", &event));
        };
        self.txn_id = Some(txn_id);
        self.state = WriteVaultState::ReadVault;
        smallvec![read_vault(self.user_id, Some(txn_id))]
    }

    fn handle_current(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.fail(unexpected(&self.state, "vault read", &event));
        };
        let held = match decode_vault(value) {
            Ok(current) => current.map_or(0, |vault| vault.revision),
            Err(error) => return self.fail(error),
        };
        // No expectation writes over whatever is there; an expectation must match.
        if let Some(expected) = self.expected_revision
            && expected != held
        {
            return self.fail(VaultStoreError::Stale);
        }
        let Some(txn_id) = self.txn_id else {
            return self.fail(StorageError::TransactionNotFound.into());
        };
        let vault = UserVault {
            user_id: self.user_id,
            payload: std::mem::take(&mut self.payload),
            revision: held.saturating_add(1),
            updated_at: self.now,
        };
        let effect = match write_vault(&vault, txn_id) {
            Ok(effect) => effect,
            Err(error) => return self.fail(error.into()),
        };
        self.state = WriteVaultState::WriteVault;
        self.output = Some(Ok(vault));
        smallvec![effect]
    }

    fn handle_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.fail(unexpected(&self.state, "vault write", &event));
        };
        let Some(txn_id) = self.txn_id else {
            return self.fail(StorageError::TransactionNotFound.into());
        };
        self.state = WriteVaultState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.fail(unexpected(&self.state, "transaction committed", &event));
        };
        self.txn_id = None;
        self.state = WriteVaultState::Finish;
        smallvec![]
    }
}

impl Operation for WriteVaultOperation {
    type Output = UserVault;
    type Error = VaultStoreError;

    fn start(&mut self) -> Effects {
        if self.payload.len() > MAX_USER_VAULT_BYTES {
            return self.fail(VaultStoreError::TooLarge(VAULT_CAP));
        }
        self.state = WriteVaultState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match self.state {
            WriteVaultState::Init => self.start(),
            WriteVaultState::StartTransaction => self.handle_started(event),
            WriteVaultState::ReadVault => self.handle_current(event),
            WriteVaultState::WriteVault => self.handle_written(event),
            WriteVaultState::CommitTransaction => self.handle_committed(event),
            WriteVaultState::Finish | WriteVaultState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, WriteVaultState::Finish | WriteVaultState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(VaultStoreError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        abort_effects(&mut self.txn_id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum DeleteVaultState {
    Init,
    StartTransaction,
    ReadVault,
    WriteVault,
    CommitTransaction,
    Finish,
    Error,
}

/// Empties the vault of one user but keeps its revision counting, so a browser
/// that still holds the deleted vault cannot overwrite a re-created one. An
/// absent or already emptied vault is left as it is.
#[derive(Debug, PartialEq)]
pub struct DeleteVaultOperation {
    user_id: UserId,
    now: u64,
    txn_id: Option<TxnId>,
    state: DeleteVaultState,
    output: Option<Result<(), VaultStoreError>>,
}

impl DeleteVaultOperation {
    pub fn new(user_id: UserId, now: u64) -> Self {
        Self {
            user_id,
            now,
            txn_id: None,
            state: DeleteVaultState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: VaultStoreError) -> Effects {
        let cleanup = self.abort();
        self.state = DeleteVaultState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn handle_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.fail(unexpected(&self.state, "transaction started", &event));
        };
        self.txn_id = Some(txn_id);
        self.state = DeleteVaultState::ReadVault;
        smallvec![read_vault(self.user_id, Some(txn_id))]
    }

    fn handle_current(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.fail(unexpected(&self.state, "vault read", &event));
        };
        let current = match decode_vault(value) {
            Ok(Some(vault)) if !vault.payload.is_empty() => vault,
            Ok(_) => {
                self.state = DeleteVaultState::Finish;
                self.output = Some(Ok(()));
                return self.abort();
            }
            Err(error) => return self.fail(error),
        };
        let Some(txn_id) = self.txn_id else {
            return self.fail(StorageError::TransactionNotFound.into());
        };
        let tombstone = UserVault {
            user_id: self.user_id,
            payload: String::new(),
            revision: current.revision.saturating_add(1),
            updated_at: self.now,
        };
        let effect = match write_vault(&tombstone, txn_id) {
            Ok(effect) => effect,
            Err(error) => return self.fail(error.into()),
        };
        self.state = DeleteVaultState::WriteVault;
        smallvec![effect]
    }

    fn handle_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.fail(unexpected(&self.state, "vault write", &event));
        };
        let Some(txn_id) = self.txn_id else {
            return self.fail(StorageError::TransactionNotFound.into());
        };
        self.state = DeleteVaultState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.fail(unexpected(&self.state, "transaction committed", &event));
        };
        self.txn_id = None;
        self.state = DeleteVaultState::Finish;
        self.output = Some(Ok(()));
        smallvec![]
    }
}

impl Operation for DeleteVaultOperation {
    type Output = ();
    type Error = VaultStoreError;

    fn start(&mut self) -> Effects {
        self.state = DeleteVaultState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match self.state {
            DeleteVaultState::Init => self.start(),
            DeleteVaultState::StartTransaction => self.handle_started(event),
            DeleteVaultState::ReadVault => self.handle_current(event),
            DeleteVaultState::WriteVault => self.handle_written(event),
            DeleteVaultState::CommitTransaction => self.handle_committed(event),
            DeleteVaultState::Finish | DeleteVaultState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            DeleteVaultState::Finish | DeleteVaultState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(VaultStoreError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        abort_effects(&mut self.txn_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::RealmId;
    use ulid::Ulid;

    fn user() -> UserId {
        UserId::local(Ulid::from_bytes([7; 16]), RealmId::from_bytes([3; 32]))
    }

    fn txn() -> Ulid {
        Ulid::from_bytes([6; 16])
    }

    fn vault(revision: u64) -> UserVault {
        UserVault {
            user_id: user(),
            payload: "sealed".to_string(),
            revision,
            updated_at: 10,
        }
    }

    fn started() -> Event {
        Event::Storage(StorageEvent::TransactionStarted { txn_id: txn() })
    }

    fn committed() -> Event {
        Event::Storage(StorageEvent::TransactionCommitted { txn_id: txn() })
    }

    fn vault_read(vault: Option<&UserVault>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: vault_key(user()),
            value: vault.map(|vault| vault.to_bytes().unwrap().into()),
        })
    }

    fn written() -> Event {
        Event::Storage(StorageEvent::WriteResult {
            key: vault_key(user()),
        })
    }

    fn tombstone(revision: u64) -> UserVault {
        UserVault {
            user_id: user(),
            payload: String::new(),
            revision,
            updated_at: 70,
        }
    }

    fn write_op(payload: &str, expected: Option<u64>) -> WriteVaultOperation {
        WriteVaultOperation::new(user(), payload.to_string(), expected, 50)
    }

    fn delete_op() -> DeleteVaultOperation {
        DeleteVaultOperation::new(user(), 70)
    }

    fn is_read(effects: &Effects) -> bool {
        matches!(
            effects.first(),
            Some(Effect::Storage(StorageEffect::Read { key_space, key, .. }))
                if key_space == USER_VAULT_KEYSPACE && *key == vault_key(user())
        )
    }

    fn is_write(effects: &Effects) -> bool {
        matches!(
            effects.first(),
            Some(Effect::Storage(StorageEffect::Write { .. }))
        )
    }

    fn is_abort(effects: &Effects) -> bool {
        matches!(
            effects.first(),
            Some(Effect::Storage(StorageEffect::AbortTransaction { .. }))
        )
    }

    #[test]
    fn reads_a_vault() {
        // Nothing before the first save; the stored record afterwards.
        let mut operation = ReadVaultOperation::new(user());
        assert!(is_read(&operation.start()));
        assert!(!operation.is_complete());
        assert!(operation.step(vault_read(None)).is_empty());
        assert!(operation.is_complete());
        assert_eq!(operation.finalize().unwrap(), None);

        let mut operation = ReadVaultOperation::new(user());
        operation.start();
        operation.step(vault_read(Some(&vault(3))));
        assert_eq!(operation.finalize().unwrap(), Some(vault(3)));

        // A deleted vault reads back as its tombstone, revision included.
        let mut operation = ReadVaultOperation::new(user());
        operation.start();
        operation.step(vault_read(Some(&tombstone(4))));
        assert_eq!(operation.finalize().unwrap(), Some(tombstone(4)));
    }

    #[test]
    fn creates_a_vault() {
        // The first save is revision 1, whether the caller expects 0 or nothing.
        for expected in [None, Some(0)] {
            let mut operation = write_op("sealed", expected);
            assert_eq!(operation.start().len(), 1);
            assert!(is_read(&operation.step(started())));
            let effects = operation.step(vault_read(None));
            let Some(Effect::Storage(StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id,
            })) = effects.first()
            else {
                panic!("expected the vault write, got {effects:?}");
            };
            assert_eq!(
                (key_space.as_str(), key, txn_id),
                (USER_VAULT_KEYSPACE, &vault_key(user()), &Some(txn()))
            );
            assert_eq!(
                UserVault::from_bytes(value.as_ref()).unwrap(),
                UserVault {
                    user_id: user(),
                    payload: "sealed".to_string(),
                    revision: 1,
                    updated_at: 50,
                }
            );
            assert_eq!(operation.step(written()).len(), 1);
            assert!(operation.step(committed()).is_empty());
            assert!(operation.is_complete());
            let saved = operation.finalize().unwrap();
            assert_eq!((saved.revision, saved.updated_at), (1, 50));
        }
    }

    #[test]
    fn replaces_with_revision() {
        // A matching expectation, or none, bumps the held revision.
        for expected in [None, Some(3)] {
            let mut operation = write_op("new", expected);
            operation.start();
            operation.step(started());
            assert!(is_write(&operation.step(vault_read(Some(&vault(3))))));
            operation.step(written());
            operation.step(committed());
            let saved = operation.finalize().unwrap();
            assert_eq!(
                (saved.payload.as_str(), saved.revision, saved.updated_at),
                ("new", 4, 50)
            );
        }
    }

    #[test]
    fn refuses_stale_revision() {
        // An older read stops before the write and releases the transaction.
        let mut operation = write_op("new", Some(2));
        operation.start();
        operation.step(started());
        assert!(is_abort(&operation.step(vault_read(Some(&vault(3))))));
        assert!(operation.is_complete());
        assert_eq!(operation.finalize().unwrap_err(), VaultStoreError::Stale);

        // Expecting a revision where nothing is stored is stale as well.
        let mut operation = write_op("new", Some(1));
        operation.start();
        operation.step(started());
        assert!(is_abort(&operation.step(vault_read(None))));
        assert_eq!(operation.finalize().unwrap_err(), VaultStoreError::Stale);
    }

    #[test]
    fn refuses_large_payload() {
        // Over the cap stops before any effect; exactly the cap goes on.
        let mut operation = write_op(&"x".repeat(MAX_USER_VAULT_BYTES + 1), None);
        assert!(operation.start().is_empty());
        assert!(operation.is_complete());
        assert_eq!(
            operation.finalize().unwrap_err(),
            VaultStoreError::TooLarge(VAULT_CAP)
        );

        let mut operation = write_op(&"x".repeat(MAX_USER_VAULT_BYTES), None);
        assert_eq!(operation.start().len(), 1);
    }

    #[test]
    fn writes_over_tombstone() {
        // A deleted vault keeps its revision for the check.
        let mut operation = write_op("new", Some(1));
        operation.start();
        operation.step(started());
        assert!(is_abort(&operation.step(vault_read(Some(&tombstone(2))))));
        assert_eq!(operation.finalize().unwrap_err(), VaultStoreError::Stale);

        for expected in [None, Some(2)] {
            let mut operation = write_op("new", expected);
            operation.start();
            operation.step(started());
            assert!(is_write(&operation.step(vault_read(Some(&tombstone(2))))));
            operation.step(written());
            operation.step(committed());
            assert_eq!(operation.finalize().unwrap().revision, 3);
        }
    }

    #[test]
    fn deletes_a_vault() {
        // The record stays as an empty tombstone with the next revision.
        let mut operation = delete_op();
        assert_eq!(operation.start().len(), 1);
        assert!(is_read(&operation.step(started())));
        let effects = operation.step(vault_read(Some(&vault(3))));
        let Some(Effect::Storage(StorageEffect::Write { value, txn_id, .. })) = effects.first()
        else {
            panic!("expected the tombstone write, got {effects:?}");
        };
        assert_eq!(txn_id, &Some(txn()));
        assert_eq!(UserVault::from_bytes(value.as_ref()).unwrap(), tombstone(4));
        assert_eq!(operation.step(written()).len(), 1);
        assert!(operation.step(committed()).is_empty());
        assert!(operation.is_complete());
        assert!(operation.finalize().is_ok());
    }

    #[test]
    fn skips_missing_vault() {
        // Nothing stored, or already emptied: the transaction is released without a write.
        for stored in [None, Some(tombstone(4))] {
            let mut operation = delete_op();
            operation.start();
            operation.step(started());
            assert!(is_abort(&operation.step(vault_read(stored.as_ref()))));
            assert!(operation.is_complete());
            assert!(operation.finalize().is_ok());
        }
    }

    fn expect_unexpected<O: Operation<Error = VaultStoreError>>(
        mut operation: O,
        events: Vec<Event>,
        wrong: Event,
        cleanup: usize,
    ) {
        operation.start();
        for event in events {
            operation.step(event);
        }
        assert_eq!(operation.step(wrong).len(), cleanup);
        assert!(operation.is_complete());
        assert!(matches!(
            operation.finalize().unwrap_err(),
            VaultStoreError::UnexpectedEvent { .. }
        ));
    }

    #[test]
    fn rejects_unexpected_events() {
        // Each state accepts one event kind; anything else fails and aborts the transaction.
        let stored = vault(3);
        let live = || vault_read(Some(&stored));

        expect_unexpected(ReadVaultOperation::new(user()), vec![], started(), 0);

        expect_unexpected(delete_op(), vec![], vault_read(None), 0);
        expect_unexpected(delete_op(), vec![started()], started(), 1);
        expect_unexpected(delete_op(), vec![started(), live()], vault_read(None), 1);
        expect_unexpected(
            delete_op(),
            vec![started(), live(), written()],
            written(),
            1,
        );

        expect_unexpected(write_op("x", None), vec![], vault_read(None), 0);
        expect_unexpected(write_op("x", None), vec![started()], started(), 1);
        expect_unexpected(
            write_op("x", None),
            vec![started(), live()],
            vault_read(None),
            1,
        );
        expect_unexpected(
            write_op("x", None),
            vec![started(), live(), written()],
            written(),
            1,
        );
    }

    #[test]
    fn aborts_on_error() {
        let error = || {
            Event::Storage(StorageEvent::Error {
                error: StorageError::TransactionConflict,
            })
        };
        let mut write = write_op("x", None);
        write.start();
        write.step(started());
        assert!(is_abort(&write.step(error())));
        assert!(write.is_complete());
        assert_eq!(
            write.finalize().unwrap_err(),
            VaultStoreError::Storage(StorageError::TransactionConflict)
        );

        let mut read = ReadVaultOperation::new(user());
        read.start();
        assert!(read.step(error()).is_empty());
        assert_eq!(
            read.finalize().unwrap_err(),
            VaultStoreError::Storage(StorageError::TransactionConflict)
        );

        let mut delete = delete_op();
        delete.start();
        delete.step(started());
        assert!(is_abort(&delete.step(error())));
        assert!(delete.is_complete());
        assert!(delete.finalize().is_err());
    }

    #[test]
    fn rejects_corrupt_records() {
        let corrupt = || {
            Event::Storage(StorageEvent::ReadResult {
                key: vault_key(user()),
                value: Some(vec![0xff; 3].into()),
            })
        };
        let mut read = ReadVaultOperation::new(user());
        read.start();
        read.step(corrupt());
        assert!(matches!(
            read.finalize().unwrap_err(),
            VaultStoreError::Conversion(_)
        ));

        let mut write = write_op("x", None);
        write.start();
        write.step(started());
        assert!(is_abort(&write.step(corrupt())));
        assert!(matches!(
            write.finalize().unwrap_err(),
            VaultStoreError::Conversion(_)
        ));

        let mut delete = delete_op();
        delete.start();
        delete.step(started());
        assert!(is_abort(&delete.step(corrupt())));
        assert!(matches!(
            delete.finalize().unwrap_err(),
            VaultStoreError::Conversion(_)
        ));
    }

    #[test]
    fn finalize_needs_completion() {
        // A step before start behaves like start; finalize before the end is refused.
        let mut write = write_op("x", None);
        assert!(!write.is_complete());
        assert_eq!(write.step(vault_read(None)).len(), 1);
        assert!(write.abort().is_empty());
        assert_eq!(write.finalize().unwrap_err(), VaultStoreError::NotFinished);

        let mut read = ReadVaultOperation::new(user());
        assert!(is_read(&read.step(started())));
        assert_eq!(read.finalize().unwrap_err(), VaultStoreError::NotFinished);

        let mut delete = delete_op();
        assert_eq!(delete.step(started()).len(), 1);
        assert_eq!(delete.finalize().unwrap_err(), VaultStoreError::NotFinished);
    }
}
