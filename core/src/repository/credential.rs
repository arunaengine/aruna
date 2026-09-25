//! Binds a user's repository token to its issuing node and selected repository.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::UserId;
use crate::credential_encryption::{CredentialEncryptionKey, EncryptedS3Secret};

use super::InvenioError;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RepositoryCredential {
    pub endpoint: String,
    fingerprint: [u8; 32],
    pub(crate) sealed: EncryptedS3Secret,
    /// The link whose stored token this is; bound into the encryption context.
    pub link_id: Option<Ulid>,
}

impl RepositoryCredential {
    pub fn seal(
        key: &CredentialEncryptionKey,
        user: UserId,
        group: Ulid,
        connector: Ulid,
        endpoint: String,
        token: &str,
    ) -> Result<Self, InvenioError> {
        Self::seal_link(key, user, group, connector, None, endpoint, token)
    }

    pub fn seal_link(
        key: &CredentialEncryptionKey,
        user: UserId,
        group: Ulid,
        connector: Ulid,
        link_id: Option<Ulid>,
        endpoint: String,
        token: &str,
    ) -> Result<Self, InvenioError> {
        if token.is_empty()
            || token.len() > 16 * 1024
            || !token.bytes().all(|byte| byte.is_ascii_graphic())
        {
            return Err(InvenioError(
                "a personal repository access token is required",
            ));
        }
        let aad = token_aad(user, group, connector, link_id, &endpoint);
        let sealed = EncryptedS3Secret::encrypt(key, token, &aad)
            .map_err(|_| InvenioError("repository token encryption failed"))?;
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
    ) -> Result<String, InvenioError> {
        if endpoint != self.endpoint {
            return Err(InvenioError("repository endpoint changed after login"));
        }
        self.sealed
            .open(
                key,
                &token_aad(user, group, connector, self.link_id, endpoint),
            )
            .map_err(|_| InvenioError("repository login is not valid for this user and node"))
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
        Some(_) => b"aruna invenio link credential\0".to_vec(),
        None => b"aruna invenio credential\0".to_vec(),
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
