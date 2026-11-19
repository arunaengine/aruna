//! Resolves one blob hash to the indexed paths a permission check can decide on.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::PATHS_INDEX_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::storage::blob::HashIndex;
use thiserror::Error;

pub const MAX_HASH_ALIASES: usize = 1024;

#[derive(Debug, Error, PartialEq)]
pub enum ResolvePathsError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("hash alias limit exceeded")]
    TooManyAliases,
    #[error("could not resolve the paths: {0}")]
    Io(IoError),
}

impl From<IoError> for ResolvePathsError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::StorageError(error),
            other => Self::Io(other),
        }
    }
}

/// Reads every alias of the hash in one page, so the answer is one consistent snapshot.
#[derive(Debug, PartialEq)]
pub struct ResolvePathsOperation {
    blake3_hash: [u8; 32],
}

impl ResolvePathsOperation {
    pub fn new(blake3_hash: [u8; 32]) -> Self {
        Self { blake3_hash }
    }
}

impl Operation for ResolvePathsOperation {
    type Output = Vec<HashIndex>;
    type Error = ResolvePathsError;

    fn first(&mut self) -> Result<Next<Vec<HashIndex>>, ResolvePathsError> {
        Ok(Next::Call(IoCall::Page {
            space: PATHS_INDEX_KEYSPACE.to_string(),
            prefix: HashIndex::hash_prefix(&self.blake3_hash)?.into(),
            after: None,
            limit: MAX_HASH_ALIASES.saturating_add(1),
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<HashIndex>>, ResolvePathsError> {
        let (values, next) = result.page()?;
        if next.is_some() || values.len() > MAX_HASH_ALIASES {
            return Err(ResolvePathsError::TooManyAliases);
        }
        let mut candidates = (values.into_iter())
            .map(|(key, _)| HashIndex::from_bytes(key.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        candidates
            .sort_by_cached_key(|candidate| (candidate.permission_path(), candidate.version_id));
        Ok(Next::Done(candidates))
    }
}

#[cfg(test)]
mod tests {
    use super::{MAX_HASH_ALIASES, ResolvePathsError, ResolvePathsOperation};
    use crate::blob::records::add_index_effect;
    use crate::driver::{DriverContext, drive};
    use aruna_core::calls::{IoCall, IoResult, Next};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::handle::Handle;
    use aruna_core::keyspaces::PATHS_INDEX_KEYSPACE;
    use aruna_core::operation::Operation;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::blob::HashIndex;
    use aruna_storage::storage;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn alias_key(
        hash: [u8; 32],
        version_id: Ulid,
        realm_id: RealmId,
        group_id: Ulid,
        node_id: aruna_core::NodeId,
        bucket: &str,
        key: &str,
    ) -> HashIndex {
        HashIndex::new(hash, version_id, realm_id, group_id, node_id, bucket, key)
    }

    #[test]
    fn starts_alias_reads() {
        let mut op = ResolvePathsOperation::new([7u8; 32]);
        assert_eq!(
            op.transaction(),
            aruna_core::operation::TransactionMode::None
        );
        assert!(matches!(
            op.first(),
            Ok(Next::Call(IoCall::Page { space, limit, .. }))
                if space == PATHS_INDEX_KEYSPACE && limit == MAX_HASH_ALIASES + 1
        ));
    }

    #[test]
    fn sorts_alias_candidates() {
        let hash = [9u8; 32];
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let group_a = Ulid::from_bytes([2u8; 16]);
        let group_b = Ulid::from_bytes([3u8; 16]);
        let node_a = iroh::SecretKey::from_bytes(&[4u8; 32]).public();
        let node_b = iroh::SecretKey::from_bytes(&[5u8; 32]).public();
        let mut op = ResolvePathsOperation::new(hash);

        let alias_a = alias_key(
            hash,
            Ulid::from_bytes([6u8; 16]),
            realm_id,
            group_b,
            node_b,
            "bucket-b",
            "z.txt",
        );
        let alias_b = alias_key(
            hash,
            Ulid::from_bytes([7u8; 16]),
            realm_id,
            group_a,
            node_a,
            "bucket-a",
            "a.txt",
        );
        let alias_b_dupe = alias_key(
            hash,
            Ulid::from_bytes([8u8; 16]),
            realm_id,
            group_a,
            node_a,
            "bucket-a",
            "a.txt",
        );

        let done = op.next(IoResult::Page {
            rows: vec![
                (alias_a.to_bytes().unwrap().into(), Vec::<u8>::new().into()),
                (alias_b.to_bytes().unwrap().into(), Vec::<u8>::new().into()),
                (
                    alias_b_dupe.to_bytes().unwrap().into(),
                    Vec::<u8>::new().into(),
                ),
            ],
            next: None,
        });

        assert_eq!(done, Ok(Next::Done(vec![alias_b, alias_b_dupe, alias_a])));
    }

    #[test]
    fn rejects_alias_overflow() {
        let mut op = ResolvePathsOperation::new([10u8; 32]);
        let rows = (0..=MAX_HASH_ALIASES)
            .map(|_| (Vec::<u8>::new().into(), Vec::<u8>::new().into()))
            .collect();
        let overflow = op.next(IoResult::Page { rows, next: None });
        assert_eq!(overflow, Err(ResolvePathsError::TooManyAliases));
        // A result that is no page fails instead of being taken as empty.
        assert!(matches!(
            op.next(IoResult::Done),
            Err(ResolvePathsError::Io(_))
        ));
    }

    #[tokio::test]
    async fn resolves_empty_hash() {
        let random_path = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(random_path.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };

        let result = drive(ResolvePathsOperation::new([1u8; 32]), &context)
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn resolves_existing_aliases() {
        let random_path = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(random_path.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };

        let hash = [8u8; 32];
        let alias = HashIndex::new(
            hash,
            Ulid::from_bytes([4u8; 16]),
            RealmId::from_bytes([1u8; 32]),
            Ulid::from_bytes([2u8; 16]),
            iroh::SecretKey::from_bytes(&[3u8; 32]).public(),
            "bucket",
            "path/file.txt",
        );
        let effect = add_index_effect(
            &crate::blob::records::HeadAliasContext::new(
                alias.realm_id,
                alias.group_id,
                alias.node_id,
                alias.bucket.clone(),
                alias.key.clone(),
            ),
            hash,
            alias.version_id,
        )
        .unwrap();
        let event = context.storage_handle.send_effect(effect).await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));

        let result = drive(ResolvePathsOperation::new(hash), &context)
            .await
            .unwrap();
        assert_eq!(result, vec![alias]);
    }
}
