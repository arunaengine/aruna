//! Requests and sealed grants bound to recipients and current authority snapshots.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::abe::{AbeError, AbeParameters};
use crate::structs::identity::auth::PathRestriction;
use crate::{NodeId, UserId};
use aruna_kpabe::{Attribute, Policy, frame_context};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

pub const REQUEST_TTL: u64 = 30 * 24 * 60 * 60 * 1000;
pub const MAX_REQUESTS: usize = 64;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyScope {
    Exact(String),
    Subtree(String),
}

impl KeyScope {
    pub fn validate(&self) -> Result<(), AbeError> {
        let value = match self {
            Self::Exact(value) | Self::Subtree(value) => value,
        };
        if value.len() > aruna_kpabe::MAX_ATTRIBUTE_BYTES
            || value.contains('\0')
            || matches!(self, Self::Exact(value) if value.is_empty())
            || matches!(self, Self::Subtree(value) if !value.is_empty() && !value.ends_with('/'))
        {
            return Err(AbeError::Scope);
        }
        Ok(())
    }
    pub fn policy(&self, parameters: &AbeParameters, epochs: &[u64]) -> Result<Policy, AbeError> {
        self.validate()?;
        let attribute = match self {
            Self::Exact(key) => Attribute::Key(key.as_bytes().to_vec()),
            Self::Subtree(prefix) => Attribute::Prefix(prefix.as_bytes().to_vec()),
        };
        let alternatives = if matches!(self, Self::Subtree(prefix) if prefix.is_empty()) {
            Vec::new()
        } else {
            vec![attribute]
        };
        Ok(Policy::new(&parameters.domain()?, epochs, &alternatives)?)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyRequest {
    pub request_id: Ulid,
    pub requesting_user: UserId,
    pub recipient_user: UserId,
    pub recipient_record: Option<Ulid>,
    pub recipient_public: Option<[u8; 32]>,
    pub recipient_fingerprint: Option<[u8; 32]>,
    pub bucket: String,
    pub parameters: AbeParameters,
    pub scope: KeyScope,
    pub epochs: Vec<u64>,
    pub credential_id: Option<String>,
    pub restrictions: Option<Vec<PathRestriction>>,
    pub revisions: Vec<[u8; 32]>,
    pub created_at_ms: u64,
}

impl KeyRequest {
    pub fn prefix(&self) -> Vec<u8> {
        [
            &self.parameters.key.bucket_id.to_bytes()[..],
            &self.recipient_user.to_storage_key(),
        ]
        .concat()
    }
    pub fn key(&self) -> Vec<u8> {
        [self.prefix(), self.request_id.to_bytes().to_vec()].concat()
    }
    pub fn to_bytes(&self) -> Result<Vec<u8>, AbeError> {
        self.scope.policy(&self.parameters, &self.epochs)?;
        let bytes = postcard::to_allocvec(self).map_err(|_| AbeError::Context)?;
        if bytes.len() > aruna_kpabe::MAX_BYTES {
            return Err(AbeError::Limit);
        }
        Ok(bytes)
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, AbeError> {
        if bytes.len() > aruna_kpabe::MAX_BYTES {
            return Err(AbeError::Limit);
        }
        let value: Self = postcard::from_bytes(bytes).map_err(|_| AbeError::Context)?;
        value.to_bytes()?;
        Ok(value)
    }
    pub fn expired(&self, now: u64) -> bool {
        now.saturating_sub(self.created_at_ms) >= REQUEST_TTL
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyIssuer {
    User(UserId),
    Node(NodeId),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrantContext {
    pub request: KeyRequest,
    pub issuer: KeyIssuer,
}

impl GrantContext {
    pub fn bytes(&self) -> Result<Vec<u8>, AbeError> {
        let r = &self.request;
        let (tag, issuer) = match self.issuer {
            KeyIssuer::User(user) => (b"user".as_slice(), user.to_storage_key()),
            KeyIssuer::Node(node) => (b"node".as_slice(), node.as_bytes().to_vec()),
        };
        let epochs = postcard::to_allocvec(&r.epochs).map_err(|_| AbeError::Context)?;
        let scope = postcard::to_allocvec(&r.scope).map_err(|_| AbeError::Context)?;
        let restrictions = postcard::to_allocvec(&r.restrictions).map_err(|_| AbeError::Context)?;
        let revisions = r.revisions.concat();
        Ok(frame_context(&[
            super::abe::GRANT_PURPOSE,
            &r.request_id.to_bytes(),
            &r.requesting_user.to_storage_key(),
            &r.recipient_user.to_storage_key(),
            &r.recipient_record.ok_or(AbeError::Stale)?.to_bytes(),
            &r.recipient_public.ok_or(AbeError::Stale)?,
            &r.recipient_fingerprint.ok_or(AbeError::Stale)?,
            r.bucket.as_bytes(),
            &r.parameters.context()?,
            &r.parameters.fingerprint,
            &epochs,
            &scope,
            r.credential_id.as_deref().unwrap_or_default().as_bytes(),
            &restrictions,
            &revisions,
            &r.created_at_ms.to_be_bytes(),
            tag,
            &issuer,
        ])?)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyGrant {
    pub context: GrantContext,
    pub enc: [u8; 32],
    pub ciphertext: Vec<u8>,
}

impl KeyGrant {
    pub fn to_bytes(&self) -> Result<Vec<u8>, AbeError> {
        self.context.bytes()?;
        if self.ciphertext.len() < 16 || self.ciphertext.len() > aruna_kpabe::MAX_BYTES + 16 {
            return Err(AbeError::Limit);
        }
        let bytes = postcard::to_allocvec(self).map_err(|_| AbeError::Context)?;
        if bytes.len() > aruna_kpabe::MAX_BYTES + aruna_kpabe::MAX_CONTEXT_BYTES + 48 {
            return Err(AbeError::Limit);
        }
        Ok(bytes)
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, AbeError> {
        if bytes.len() > aruna_kpabe::MAX_BYTES + aruna_kpabe::MAX_CONTEXT_BYTES + 48 {
            return Err(AbeError::Limit);
        }
        let value: Self = postcard::from_bytes(bytes).map_err(|_| AbeError::Context)?;
        value.to_bytes()?;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::identity::realm::RealmId;
    use crate::structs::storage::encryption::BucketKeyRef;
    use base64::{Engine, engine::general_purpose::STANDARD};
    use serde_json::{Value, json};

    fn ulid(text: &str) -> Ulid {
        Ulid::from_string(text).unwrap()
    }

    fn user(text: &str, realm: u8) -> UserId {
        UserId::new(ulid(text), RealmId::from_bytes([realm; 32]))
    }

    /// The API proposal view: named fields, postcard record and associated data.
    fn view(context: &GrantContext) -> Value {
        let (r, p) = (&context.request, &context.request.parameters);
        let (kind, value) = match &r.scope {
            KeyScope::Exact(value) => ("exact", value),
            KeyScope::Subtree(value) => ("subtree", value),
        };
        let issuer = match context.issuer {
            KeyIssuer::User(user) => json!({"kind":"user","id":user.to_string()}),
            KeyIssuer::Node(node) => json!({"kind":"node","id":node.to_string()}),
        };
        json!({"fields":{"request_id":r.request_id.to_string(),
            "requesting_user":r.requesting_user.to_string(),
            "recipient_user":r.recipient_user.to_string(),
            "recipient_record":r.recipient_record.map(|v|v.to_string()),
            "recipient_public":r.recipient_public.map(|v|STANDARD.encode(v)),
            "recipient_fingerprint":r.recipient_fingerprint.map(|v|STANDARD.encode(v)),
            "bucket":r.bucket,"parameters":{"realm_id":p.realm_id.to_string(),
            "node_id":p.node_id.to_string(),"bucket_id":p.key.bucket_id.to_string(),
            "generation":p.key.generation,"fingerprint":STANDARD.encode(p.fingerprint),
            "parameters":STANDARD.encode(&p.parameters),"epoch":r.epochs[0],
            "context":STANDARD.encode(p.context().unwrap())},
            "scope":{"kind":kind,"value":value},"epochs":r.epochs,
            "credential_id":r.credential_id,"restrictions":r.restrictions,
            "revisions":r.revisions.iter().map(|v|STANDARD.encode(v)).collect::<Vec<_>>(),
            "created_at_ms":r.created_at_ms,"issuer":issuer},
            "record":STANDARD.encode(postcard::to_allocvec(context).unwrap()),
            "aad":STANDARD.encode(context.bytes().unwrap())})
    }

    fn fixture() -> Value {
        let node = iroh::SecretKey::from_bytes(&[4; 32]).public();
        let request = KeyRequest {
            request_id: ulid("01K6YQ8ZQ9V3X2N4M5P6R7S8T9"),
            requesting_user: user("01K6YQ8ZQ9V3X2N4M5P6R7S8TA", 1),
            recipient_user: user("01K6YQ8ZQ9V3X2N4M5P6R7S8TB", 1),
            recipient_record: Some(ulid("01K6YQ8ZQ9V3X2N4M5P6R7S8TC")),
            recipient_public: Some([5; 32]),
            recipient_fingerprint: Some([6; 32]),
            bucket: "reef".into(),
            parameters: AbeParameters {
                realm_id: RealmId::from_bytes([1; 32]),
                node_id: node,
                key: BucketKeyRef::new(ulid("01K6YQ8ZQ9V3X2N4M5P6R7S8TD"), 1),
                fingerprint: [7; 32],
                parameters: vec![9; 3],
            },
            scope: KeyScope::Exact("data/a.csv".into()),
            epochs: vec![1],
            credential_id: None,
            restrictions: None,
            revisions: vec![[8; 32]],
            created_at_ms: 1_700_000_000_000,
        };
        let mut wide = request.clone();
        wide.request_id = ulid("01K6YQ8ZQ9V3X2N4M5P6R7S8TE");
        wide.recipient_user = user("01K6YQ8ZQ9V3X2N4M5P6R7S8TF", 2);
        wide.parameters.key.generation = 300;
        wide.parameters.parameters = vec![10; 200];
        wide.scope = KeyScope::Subtree(format!("{}/", "é".repeat(70)));
        wide.epochs = vec![300, 70_000, 1 << 40];
        wide.revisions = vec![[11; 32], [12; 32]];
        wide.created_at_ms = 1_760_000_000_000;
        let mut root = request.clone();
        root.scope = KeyScope::Subtree(String::new());
        json!([
            view(&GrantContext {
                request,
                issuer: KeyIssuer::User(user("01K6YQ8ZQ9V3X2N4M5P6R7S8TG", 1)),
            }),
            view(&GrantContext {
                request: wide,
                issuer: KeyIssuer::Node(node),
            }),
            view(&GrantContext {
                request: root,
                issuer: KeyIssuer::User(user("01K6YQ8ZQ9V3X2N4M5P6R7S8TG", 3)),
            }),
        ])
    }

    #[test]
    fn grant_fixture() {
        let expected: Value =
            serde_json::from_str(include_str!("../../../tests/vectors/abe-grant.json")).unwrap();
        assert_eq!(fixture(), expected);
    }

    #[test]
    #[ignore = "writes deterministic fixture data for explicit regeneration"]
    fn write_fixture() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/vectors/abe-grant.json");
        std::fs::write(
            path,
            serde_json::to_string_pretty(&fixture()).unwrap() + "\n",
        )
        .unwrap();
    }
}
