//! Binds a user's repository token to its issuing node and selected repository.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::credential_encryption::{CredentialEncryptionKey, EncryptedS3Secret};
use crate::user::id::UserId;

use super::RepositoryError;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RepositoryCredential {
    pub endpoint: String,
    fingerprint: [u8; 32],
    pub(crate) sealed: EncryptedS3Secret,
    /// The link whose stored token this is; bound into the encryption context.
    pub link_id: Option<Ulid>,
}

impl RepositoryCredential {
    /// Seals `token` under `key` with `nonce`, which must never repeat under that key.
    pub fn seal(
        (key, nonce): (&CredentialEncryptionKey, [u8; 24]),
        user: UserId,
        group: Ulid,
        connector: Ulid,
        endpoint: String,
        token: &str,
    ) -> Result<Self, RepositoryError> {
        Self::seal_link((key, nonce), user, group, connector, None, endpoint, token)
    }

    pub fn seal_link(
        (key, nonce): (&CredentialEncryptionKey, [u8; 24]),
        user: UserId,
        group: Ulid,
        connector: Ulid,
        link_id: Option<Ulid>,
        endpoint: String,
        token: &str,
    ) -> Result<Self, RepositoryError> {
        if token.is_empty()
            || token.len() > 16 * 1024
            || !token.bytes().all(|byte| byte.is_ascii_graphic())
        {
            return Err(RepositoryError(
                "a personal repository access token is required",
            ));
        }
        let aad = token_aad(user, group, connector, link_id, &endpoint);
        let sealed = EncryptedS3Secret::encrypt(key, token, &aad, nonce)
            .map_err(|_| RepositoryError("repository token encryption failed"))?;
        Ok(Self {
            endpoint,
            fingerprint: *blake3::hash(token.as_bytes()).as_bytes(),
            sealed,
            link_id,
        })
    }

    pub fn open(
        &self,
        key: &CredentialEncryptionKey,
        user: UserId,
        group: Ulid,
        connector: Ulid,
        endpoint: &str,
    ) -> Result<String, RepositoryError> {
        if endpoint != self.endpoint {
            return Err(RepositoryError("repository endpoint changed after login"));
        }
        self.sealed
            .open(
                key,
                &token_aad(user, group, connector, self.link_id, endpoint),
            )
            .map_err(|_| RepositoryError("repository login is not valid for this user and node"))
    }
}

/// A sealed token with the user, group, connector and endpoint it must open for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CredentialOpen {
    pub credential: RepositoryCredential,
    pub user: UserId,
    pub group: Ulid,
    pub connector: Ulid,
    pub endpoint: String,
}

impl CredentialOpen {
    /// Opens the token with the node's credential key.
    pub fn open(&self, key: &CredentialEncryptionKey) -> Result<String, RepositoryError> {
        let (user, group, connector) = (self.user, self.group, self.connector);
        let credential = &self.credential;
        credential.open(key, user, group, connector, &self.endpoint)
    }
}

fn token_aad(
    user: UserId,
    group: Ulid,
    connector: Ulid,
    link_id: Option<Ulid>,
    endpoint: &str,
) -> Vec<u8> {
    let mut aad = match link_id {
        Some(_) => b"aruna repository link credential\0".to_vec(),
        None => b"aruna repository credential\0".to_vec(),
    };
    aad.extend_from_slice(&user.to_bytes());
    aad.extend_from_slice(&group.to_bytes());
    aad.extend_from_slice(&connector.to_bytes());
    if let Some(link_id) = link_id {
        aad.extend_from_slice(&link_id.to_bytes());
    }
    aad.extend_from_slice(endpoint.as_bytes());
    aad
}
