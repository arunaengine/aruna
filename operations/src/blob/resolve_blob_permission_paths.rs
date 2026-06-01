use crate::blob::blob_keyspace_helper::iter_hash_path_index_effect;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::HashPathIndexKey;
use aruna_core::types::Effects;
use smallvec::smallvec;
use std::collections::BTreeSet;
use thiserror::Error;
use ulid::Ulid;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ResolveBlobPermissionPathsState {
    Init,
    StartTransaction,
    ReadHashAliases,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ResolveBlobPermissionPathsError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("ResolveBlobPermissionPaths did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: ResolveBlobPermissionPathsState,
        expected: &'static str,
        got: String,
    },
}

#[derive(Debug, PartialEq)]
pub struct ResolveBlobPermissionPathsOperation {
    blake3_hash: [u8; 32],
    txn_id: Option<Ulid>,
    output: Option<Result<Vec<String>, ResolveBlobPermissionPathsError>>,
    state: ResolveBlobPermissionPathsState,
}

impl ResolveBlobPermissionPathsOperation {
    pub fn new(blake3_hash: [u8; 32]) -> Self {
        Self {
            blake3_hash,
            txn_id: None,
            output: None,
            state: ResolveBlobPermissionPathsState::Init,
        }
    }

    fn emit_read_hash_aliases(&mut self) -> Result<Effects, ResolveBlobPermissionPathsError> {
        Ok(smallvec![iter_hash_path_index_effect(
            &self.blake3_hash,
            self.txn_id,
        )?])
    }

    fn parse_hash_aliases(
        &mut self,
        values: Vec<(aruna_core::types::Key, aruna_core::types::Value)>,
    ) -> Result<Effects, ResolveBlobPermissionPathsError> {
        let permission_paths = values
            .into_iter()
            .map(|(key, _)| HashPathIndexKey::from_bytes(key.as_ref()))
            .map(|result| result.map(|key| key.permission_path()))
            .collect::<Result<BTreeSet<_>, _>>()?
            .into_iter()
            .collect();

        self.output = Some(Ok(permission_paths));

        let txn_id = self
            .txn_id
            .ok_or(ResolveBlobPermissionPathsError::NoTransactionFound)?;
        Ok(smallvec![Effect::Storage(
            StorageEffect::CommitTransaction { txn_id }
        )])
    }

    fn fail(&mut self, err: ResolveBlobPermissionPathsError) -> Effects {
        self.state = ResolveBlobPermissionPathsState::Error;
        self.output = Some(Err(err));
        smallvec![]
    }

    fn fail_with_cleanup(
        &mut self,
        err: ResolveBlobPermissionPathsError,
        cleanup_effects: Effects,
    ) -> Effects {
        self.state = ResolveBlobPermissionPathsState::Error;
        self.output = Some(Err(err));
        cleanup_effects
    }

    fn unexpected_event(
        &mut self,
        state: ResolveBlobPermissionPathsState,
        expected: &'static str,
        got: String,
    ) -> Effects {
        let cleanup_effects = self.abort();
        self.fail_with_cleanup(
            ResolveBlobPermissionPathsError::UnexpectedEvent {
                state,
                expected,
                got,
            },
            cleanup_effects,
        )
    }

    fn fail_on_storage_error(&mut self, event: Event) -> Result<Event, Effects> {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return Err(self.fail(error.into()));
        }

        Ok(event)
    }

    fn handle_start_transaction(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected_event(
                ResolveBlobPermissionPathsState::StartTransaction,
                "Event::Storage(StorageEvent::TransactionStarted)",
                got,
            );
        };

        self.state = ResolveBlobPermissionPathsState::ReadHashAliases;
        self.txn_id = Some(txn_id);
        match self.emit_read_hash_aliases() {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn handle_read_hash_aliases(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            return self.unexpected_event(
                ResolveBlobPermissionPathsState::ReadHashAliases,
                "Event::Storage(StorageEvent::IterResult)",
                got,
            );
        };

        match self.parse_hash_aliases(values) {
            Ok(effects) => {
                self.state = ResolveBlobPermissionPathsState::CommitTransaction;
                effects
            }
            Err(err) => self.fail(err),
        }
    }

    fn handle_commit_transaction(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.unexpected_event(
                ResolveBlobPermissionPathsState::CommitTransaction,
                "Event::Storage(StorageEvent::TransactionCommitted)",
                got,
            );
        };

        self.state = ResolveBlobPermissionPathsState::Finish;
        smallvec![]
    }
}

impl Operation for ResolveBlobPermissionPathsOperation {
    type Output = Vec<String>;
    type Error = ResolveBlobPermissionPathsError;

    fn start(&mut self) -> Effects {
        self.state = ResolveBlobPermissionPathsState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: true,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match self.fail_on_storage_error(event) {
            Ok(event) => event,
            Err(effects) => return effects,
        };

        match self.state {
            ResolveBlobPermissionPathsState::StartTransaction => {
                self.handle_start_transaction(event)
            }
            ResolveBlobPermissionPathsState::ReadHashAliases => {
                self.handle_read_hash_aliases(event)
            }
            ResolveBlobPermissionPathsState::CommitTransaction => {
                self.handle_commit_transaction(event)
            }
            ResolveBlobPermissionPathsState::Init
            | ResolveBlobPermissionPathsState::Finish
            | ResolveBlobPermissionPathsState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ResolveBlobPermissionPathsState::Finish | ResolveBlobPermissionPathsState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .ok_or(ResolveBlobPermissionPathsError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ResolveBlobPermissionPathsOperation, ResolveBlobPermissionPathsState};
    use crate::blob::blob_keyspace_helper::add_hash_path_index_effect;
    use crate::driver::{DriverContext, drive};
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::handle::Handle;
    use aruna_core::keyspaces::HASH_PATHS_INDEX_KEYSPACE;
    use aruna_core::operation::Operation;
    use aruna_core::structs::{HashPathIndexKey, RealmId};
    use aruna_storage::storage;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn alias_key(
        hash: [u8; 32],
        realm_id: RealmId,
        group_id: Ulid,
        node_id: aruna_core::NodeId,
        bucket: &str,
        key: &str,
    ) -> HashPathIndexKey {
        HashPathIndexKey::new(hash, realm_id, group_id, node_id, bucket, key)
    }

    #[test]
    fn start_reads_hash_aliases_in_hash_index_keyspace() {
        let mut op = ResolveBlobPermissionPathsOperation::new([7u8; 32]);

        let effects = op.start();
        assert_eq!(op.state, ResolveBlobPermissionPathsState::StartTransaction);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction {
                read: true
            })]
        ));

        let txn_id = Ulid::new();
        let effects = op.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        assert_eq!(op.state, ResolveBlobPermissionPathsState::ReadHashAliases);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Iter { key_space, txn_id: iter_txn_id, .. })]
                if key_space == HASH_PATHS_INDEX_KEYSPACE && *iter_txn_id == Some(txn_id)
        ));
    }

    #[test]
    fn read_hash_aliases_sorts_and_deduplicates_permission_paths() {
        let hash = [9u8; 32];
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let group_a = Ulid::from_bytes([2u8; 16]);
        let group_b = Ulid::from_bytes([3u8; 16]);
        let node_a = iroh::SecretKey::from_bytes(&[4u8; 32]).public();
        let node_b = iroh::SecretKey::from_bytes(&[5u8; 32]).public();
        let mut op = ResolveBlobPermissionPathsOperation::new(hash);

        op.start();
        let effects = op.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::new(),
        }));
        assert_eq!(op.state, ResolveBlobPermissionPathsState::ReadHashAliases);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Iter { .. })]
        ));

        let alias_a = alias_key(hash, realm_id, group_b, node_b, "bucket-b", "z.txt");
        let alias_b = alias_key(hash, realm_id, group_a, node_a, "bucket-a", "a.txt");
        let alias_b_dupe = alias_key(hash, realm_id, group_a, node_a, "bucket-a", "a.txt");

        let effects = op.step(Event::Storage(StorageEvent::IterResult {
            values: vec![
                (alias_a.to_bytes().unwrap().into(), Vec::<u8>::new().into()),
                (alias_b.to_bytes().unwrap().into(), Vec::<u8>::new().into()),
                (
                    alias_b_dupe.to_bytes().unwrap().into(),
                    Vec::<u8>::new().into(),
                ),
            ],
            next_start_after: None,
        }));

        assert_eq!(op.state, ResolveBlobPermissionPathsState::CommitTransaction);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { .. })]
        ));

        let effects = op.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: Ulid::new(),
        }));
        assert!(effects.is_empty());
        assert_eq!(op.state, ResolveBlobPermissionPathsState::Finish);
        assert_eq!(
            op.finalize().unwrap(),
            vec![alias_b.permission_path(), alias_a.permission_path()]
        );
    }

    #[tokio::test]
    async fn resolves_empty_hash_to_empty_permission_path_list() {
        let random_path = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(random_path.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        let result = drive(
            ResolveBlobPermissionPathsOperation::new([1u8; 32]),
            &context,
        )
        .await
        .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn resolves_existing_hash_aliases_to_permission_paths() {
        let random_path = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(random_path.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        let hash = [8u8; 32];
        let alias = HashPathIndexKey::new(
            hash,
            RealmId::from_bytes([1u8; 32]),
            Ulid::from_bytes([2u8; 16]),
            iroh::SecretKey::from_bytes(&[3u8; 32]).public(),
            "bucket",
            "path/file.txt",
        );
        let effect = add_hash_path_index_effect(
            &crate::blob::blob_keyspace_helper::HeadAliasContext::new(
                alias.realm_id,
                alias.group_id,
                alias.node_id,
                alias.bucket.clone(),
                alias.key.clone(),
            ),
            hash,
            None,
        )
        .unwrap();
        let event = context.storage_handle.send_effect(effect).await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));

        let result = drive(ResolveBlobPermissionPathsOperation::new(hash), &context)
            .await
            .unwrap();
        assert_eq!(result, vec![alias.permission_path()]);
    }
}
