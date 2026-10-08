//! Reads what holder resolution needs from outside one transaction: the users with WRITE on a
//! group's admin path, and each holder's public keys from the key directory.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::driver::{DriverContext, drive};
use crate::users::vault_read::{ReadVaultConfig, ReadVaultOperation};
use aruna_core::effects::{StorageEffect, VaultQuery};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::AUTH_KEYSPACE;
use aruna_core::structs::identity::group::GroupAuthorizationDocument;
use aruna_core::structs::identity::realm::{RealmAuthorizationDocument, RealmId};
use aruna_core::structs::identity::user::vault::VaultRecords;
use aruna_core::structs::placement::policy::document::group_admin_path;
use aruna_core::structs::storage::holders::{KeyLookup, admin_users};
use aruna_core::types::GroupId;
use aruna_core::{NodeId, UserId};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use thiserror::Error;

/// How long one key directory lookup may wait for the holders of a user's vault.
pub const KEY_LOOKUP_DEADLINE: Duration = Duration::from_secs(10);

#[derive(Debug, Error, PartialEq)]
pub enum HolderInputError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("an authorization document is missing")]
    MissingDocument,
}

/// Users with WRITE on the group admin path through the realm and group roles.
pub async fn read_admins(
    context: &DriverContext,
    realm_id: RealmId,
    group_id: GroupId,
) -> Result<BTreeSet<UserId>, HolderInputError> {
    let realm = read_document(context, realm_id.as_bytes().to_vec()).await?;
    let group = read_document(context, group_id.to_bytes().to_vec()).await?;
    let realm = RealmAuthorizationDocument::from_bytes(&realm)?;
    let group = GroupAuthorizationDocument::from_bytes(&group)?;
    let path = group_admin_path(realm_id, group_id);
    Ok(admin_users(
        realm.roles.values().chain(group.roles.values()),
        &path,
        realm_id,
    ))
}

/// The key directory answer for each user. Any failed lookup counts as unavailable, never as
/// a user without keys.
pub async fn lookup_keys(
    context: &DriverContext,
    node_id: NodeId,
    users: impl IntoIterator<Item = UserId>,
) -> BTreeMap<UserId, KeyLookup> {
    let mut lookups = BTreeMap::new();
    for user_id in users {
        let config = ReadVaultConfig {
            node_id,
            user_id,
            query: VaultQuery::Keys,
            deadline: KEY_LOOKUP_DEADLINE,
        };
        let lookup = match drive(ReadVaultOperation::new(config), context).await {
            Ok(VaultRecords::Keys(keys)) if keys.is_empty() => KeyLookup::Missing,
            Ok(VaultRecords::Keys(keys)) => KeyLookup::Keys(keys),
            Ok(VaultRecords::Heads(_)) | Err(_) => KeyLookup::Unavailable,
        };
        lookups.insert(user_id, lookup);
    }
    lookups
}

async fn read_document(context: &DriverContext, key: Vec<u8>) -> Result<Vec<u8>, HolderInputError> {
    let read = StorageEffect::Read {
        key_space: AUTH_KEYSPACE.to_string(),
        key: key.into(),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => Ok(value.to_vec()),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
            Err(HolderInputError::MissingDocument)
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        _ => Err(StorageError::ReadError("unexpected authorization read".to_string()).into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::s3::{test_context, test_storage};
    use aruna_core::structs::identity::auth::Actor;

    #[tokio::test]
    async fn admins_from_documents() {
        let (_dir, storage) = test_storage();
        let context = test_context(storage.clone());
        let realm_id = RealmId::from_bytes([3; 32]);
        let group_id = GroupId::from_bytes([4; 16]);
        let owner = UserId::new(ulid::Ulid::from_bytes([5; 16]), realm_id);
        let realm = RealmAuthorizationDocument {
            realm_id,
            roles: Default::default(),
            operation_restrictions: Default::default(),
        };
        let group = GroupAuthorizationDocument::default_group_doc(owner, realm_id, group_id);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[1; 32]).public(),
            user_id: owner,
            realm_id,
        };
        for (key, value) in [
            (
                realm_id.as_bytes().to_vec(),
                realm.to_bytes(&actor).unwrap(),
            ),
            (
                group_id.to_bytes().to_vec(),
                group.to_bytes(&actor).unwrap(),
            ),
        ] {
            storage
                .send_storage_effect(StorageEffect::Write {
                    key_space: AUTH_KEYSPACE.to_string(),
                    key: key.into(),
                    value: value.into(),
                    txn_id: None,
                })
                .await;
        }

        let admins = read_admins(&context, realm_id, group_id).await.unwrap();
        assert_eq!(admins, BTreeSet::from([owner]));
        let missing = read_admins(&context, realm_id, GroupId::from_bytes([6; 16])).await;
        assert_eq!(missing, Err(HolderInputError::MissingDocument));
    }

    #[tokio::test]
    async fn unreachable_directory_unavailable() {
        // Without a realm configuration no vault holder can be resolved: that is unknown
        // readiness, never a user without keys.
        let (_dir, storage) = test_storage();
        let context = test_context(storage);
        let node_id = iroh::SecretKey::from_bytes(&[1; 32]).public();
        let user = UserId::new(
            ulid::Ulid::from_bytes([7; 16]),
            RealmId::from_bytes([3; 32]),
        );

        let lookups = lookup_keys(&context, node_id, [user]).await;
        assert_eq!(lookups.get(&user), Some(&KeyLookup::Unavailable));
    }
}
