//! Requests and sealed grants bound to recipients and current authority snapshots.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::abe::{AbeError, AbeParameters, GRANT_PURPOSE, ObjectEnvelope, check_object};
use super::encryption::{BucketEncryption, EncryptionMode};
use crate::compute::{SecretBytes, SharedSecret};
use crate::key_seal::{SealedSecret, open_sealed};
use crate::keyspaces::ABE_DUE_KEYSPACE;
use crate::structs::identity::auth::PathRestriction;
use crate::types::{Key, Value};
use crate::{NodeId, UserId};
use aruna_kpabe::{Attribute, Envelope, Policy, UserKey, frame_context};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

pub const REQUEST_TTL: u64 = 30 * 24 * 60 * 60 * 1000;
pub const MAX_REQUESTS: usize = 64;
/// Epochs one scoped key may admit.
pub const MAX_EPOCHS: usize = aruna_kpabe::MAX_EPOCHS;
/// Writes one enumerated key may name: the key rows beside its domain and its one epoch.
pub const MAX_WRITES: usize = aruna_kpabe::MAX_ROWS - 2;

/// Bucket names in rows of the group's encrypted bucket index.
pub fn indexed_buckets(rows: &[(Key, Value)]) -> Vec<String> {
    rows.iter()
        .filter_map(|(key, _)| std::str::from_utf8(key.get(16..)?).ok())
        .map(str::to_string)
        .collect()
}

/// Due rows, keyed by bucket id, for the buckets whose settings hold an active key, and the names
/// of the node managed ones. A blind write, so a raise that read the old marker conflicts.
pub fn due_rows(
    buckets: &[String],
    settings: Vec<(Key, Option<Value>)>,
    now: u64,
) -> (Vec<(String, Key, Value)>, Vec<String>) {
    let mut writes = Vec::new();
    let mut managed = Vec::new();
    for (bucket, (_, value)) in buckets.iter().zip(settings) {
        let Ok(settings) = BucketEncryption::from_row(value.as_deref()) else {
            continue;
        };
        let Some(key) = settings.active_key() else {
            continue;
        };
        let id = key.bucket_id.to_bytes().to_vec().into();
        let at = now.to_be_bytes().to_vec().into();
        writes.push((ABE_DUE_KEYSPACE.to_string(), id, at));
        if settings.mode == EncryptionMode::NodeManaged {
            managed.push(bucket.clone());
        }
    }
    (writes, managed)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyScope {
    Exact(String),
    Subtree(String),
    /// Exact writes with their object keys, all of one epoch; later writes are never covered.
    Writes(Vec<(String, Ulid)>),
}

impl KeyScope {
    pub fn validate(&self) -> Result<(), AbeError> {
        let value = match self {
            Self::Exact(value) | Self::Subtree(value) => value,
            Self::Writes(writes) => return check_writes(writes),
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
            // Each write keeps its own epoch, so one key names writes of one epoch only.
            Self::Writes(_) if epochs.len() != 1 => return Err(AbeError::Scope),
            Self::Writes(writes) => {
                let writes = writes.iter().map(|(_, id)| Attribute::Write(id.to_bytes()));
                let alternatives: Vec<_> = writes.collect();
                return Ok(Policy::new(&parameters.domain()?, epochs, &alternatives)?);
            }
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
        // A user issuer frames longer than a node, so every issuer can sign what is stored.
        let request = Self {
            recipient_record: Some(self.recipient_record.unwrap_or_default()),
            recipient_public: Some(self.recipient_public.unwrap_or_default()),
            recipient_fingerprint: Some(self.recipient_fingerprint.unwrap_or_default()),
            ..self.clone()
        };
        let issuer = KeyIssuer::User(self.recipient_user);
        if (GrantContext { request, issuer }).bytes().is_err() {
            return Err(AbeError::Limit);
        }
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
    /// The `token_grants` row of a credential's request: the access key prefix, then its key.
    pub fn token_key(&self) -> Option<Vec<u8>> {
        let access_key = self.credential_id.as_deref()?;
        Some([token_prefix(access_key), self.key()].concat())
    }
    /// Whether this scope's key may open the envelope of `key`.
    pub fn covers(&self, key: &str) -> bool {
        match &self.scope {
            KeyScope::Exact(exact) => exact == key,
            KeyScope::Subtree(prefix) => key.starts_with(prefix.as_str()),
            KeyScope::Writes(writes) => writes.iter().any(|(written, _)| written == key),
        }
    }
    /// Whether both bind everything but the epoch set alike, so their keys may be merged.
    pub fn same_context(&self, other: &Self) -> bool {
        let rebound = Self {
            request_id: other.request_id,
            epochs: other.epochs.clone(),
            created_at_ms: other.created_at_ms,
            ..self.clone()
        };
        rebound == *other
    }
}

fn check_writes(writes: &[(String, Ulid)]) -> Result<(), AbeError> {
    let mut ids: Vec<Ulid> = writes.iter().map(|(_, id)| *id).collect();
    ids.sort_unstable();
    ids.dedup();
    if writes.is_empty()
        || writes.len() > MAX_WRITES
        || ids.len() != writes.len()
        || writes.iter().any(|(key, _)| {
            key.is_empty() || key.contains('\0') || key.len() > aruna_kpabe::MAX_ATTRIBUTE_BYTES
        })
    {
        return Err(AbeError::Scope);
    }
    Ok(())
}

/// Scan prefix of one credential's `token_grants` rows; access keys are alphanumeric.
pub fn token_prefix(access_key: &str) -> Vec<u8> {
    [access_key.as_bytes(), &[0]].concat()
}

/// The recipient record, public key and fingerprint of the token key of `access_key`.
pub fn token_recipient(access_key: &str, public: [u8; 32]) -> Option<(Ulid, [u8; 32], [u8; 32])> {
    let record = Ulid::from_string(access_key).ok()?;
    Some((
        record,
        public,
        crate::vault_format::key_fingerprint(&public),
    ))
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
    /// Opens the scoped key with `token`, then the object key of `envelope`.
    /// A token that does not open this grant fails as `WrongKey`.
    pub fn open_object(
        &self,
        token: &SecretBytes,
        envelope: &ObjectEnvelope,
    ) -> Result<SharedSecret, AbeError> {
        let token: &[u8; 32] = token.expose().try_into().map_err(|_| AbeError::WrongKey)?;
        let parameters = self.context.request.parameters.public()?;
        let aad = self.context.bytes()?;
        let sealed = SealedSecret {
            enc: self.enc,
            ciphertext: self.ciphertext.clone(),
        };
        let opened = open_sealed(token, &sealed, GRANT_PURPOSE, &aad);
        let opened = opened.map_err(|_| AbeError::WrongKey)?;
        let transport = [&self.enc[..], &self.ciphertext].concat();
        let key = UserKey::open(&parameters, &transport, |_| Ok(opened))?;
        let cipher = Envelope::from_bytes(&parameters, &envelope.abe)?;
        let object = aruna_kpabe::open(&parameters, &key, &cipher, &envelope.context.bytes()?)?;
        let private = SharedSecret::new(SecretBytes::new(object.as_bytes().to_vec()));
        check_object(private.bytes(), envelope)?;
        Ok(private)
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
        let scope = match &r.scope {
            KeyScope::Exact(value) => json!({"kind":"exact","value":value}),
            KeyScope::Subtree(value) => json!({"kind":"subtree","value":value}),
            KeyScope::Writes(writes) => {
                let value: Vec<_> = (writes.iter())
                    .map(|(key, id)| json!({"key":key,"write_id":id.to_string()}))
                    .collect();
                json!({"kind":"writes","value":value})
            }
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
            "scope":scope,"epochs":r.epochs,
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
        let mut token = request.clone();
        token.request_id = ulid("01K6YQ8ZQ9V3X2N4M5P6R7S8TH");
        token.credential_id = Some("01K6YQ8ZQ9V3X2N4M5P6R7S8TJ".into());
        let mut writes = request.clone();
        writes.request_id = ulid("01K6YQ8ZQ9V3X2N4M5P6R7S8TK");
        writes.scope = KeyScope::Writes(vec![
            ("data/a.csv".into(), ulid("01K6YQ8ZQ9V3X2N4M5P6R7S8TM")),
            ("data/λ b.csv".into(), ulid("01K6YQ8ZQ9V3X2N4M5P6R7S8TN")),
        ]);
        writes.epochs = vec![3];
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
            view(&GrantContext {
                request: token,
                issuer: KeyIssuer::Node(node),
            }),
            view(&GrantContext {
                request: writes,
                issuer: KeyIssuer::User(user("01K6YQ8ZQ9V3X2N4M5P6R7S8TG", 1)),
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
    fn scope_grammar() {
        use super::super::abe::{EnvelopePlan, SysRng, create_envelope, create_parameters};
        use crate::structs::storage::encryption::public_key_of;
        let secret = SecretBytes::new(vec![9; 32]);
        let bucket_public = public_key_of(&secret).unwrap();
        let (node, other) = (iroh::SecretKey::from_bytes(&[4; 32]).public(), [5; 32]);
        let other = iroh::SecretKey::from_bytes(&other).public();
        let realm = RealmId::from_bytes([1; 32]);
        let bucket = |id: u8, node| {
            let key = BucketKeyRef::new(Ulid::from_bytes([id; 16]), 1);
            create_parameters(&secret, realm, node, key).unwrap()
        };
        let here = bucket(3, node);
        let (public, master) = (here.public().unwrap(), here.recompute(&secret).unwrap());
        // Whether a key for `scope` at epoch 1 opens the envelope of one write.
        let opens =
            |scope: &KeyScope,
             (parameters, object, write, epoch): (&AbeParameters, &str, u8, u64)| {
                let policy = scope.policy(&here, &[1]).unwrap();
                let key = aruna_kpabe::issue(&public, &master, &policy, &mut SysRng).unwrap();
                let plan = EnvelopePlan {
                    parameters: parameters.clone(),
                    epoch,
                    write_id: Ulid::from_bytes([write; 16]),
                    object_key: object.into(),
                    bucket_public,
                };
                let (envelope, _) = create_envelope(plan).unwrap();
                let context = envelope.context.bytes().unwrap();
                Envelope::from_bytes(&public, &envelope.abe)
                    .is_ok_and(|abe| aruna_kpabe::open(&public, &key, &abe, &context).is_ok())
            };
        let (sub, exact) = (
            |v: &str| KeyScope::Subtree(v.into()),
            |v: &str| KeyScope::Exact(v.into()),
        );
        let writes = KeyScope::Writes(vec![("w".into(), Ulid::from_bytes([1; 16]))]);
        for (scope, object, expected) in [
            (sub("foo/"), "foo/a", true),
            (sub("foo/"), "foobar/a", false),
            (sub(""), "x/y/z", true),
            (exact("foo"), "foo", true),
            (exact("foo"), "foo/a", false),
            (sub("a//"), "a//b", true),
            (sub("a//"), "a/b", false),
            (sub("a/"), "a//b", true),
            (exact("dir/"), "dir/", true),
            (exact("dir"), "dir/", false),
            (sub("λ/"), "λ/ü", true),
            (sub("λ/"), "Λ/ü", false),
            (sub("a*/"), "a*/b", true),
            (sub("a*/"), "ab/b", false),
            (exact("f?"), "fx", false),
        ] {
            assert_eq!(
                opens(&scope, (&here, object, 1, 1)),
                expected,
                "{scope:?} {object}"
            );
        }
        // A listed write opens only itself at its own epoch: no other or later write.
        assert!(opens(&writes, (&here, "w", 1, 1)));
        assert!(!opens(&writes, (&here, "w", 2, 1)));
        assert!(!opens(&writes, (&here, "w", 1, 2)));
        // A recreated bucket or the same bucket name on another node has other parameters.
        assert!(!opens(&sub(""), (&bucket(4, node), "a", 1, 1)));
        assert!(!opens(&sub(""), (&bucket(3, other), "a", 1, 1)));
        // Duplicate labels are refused.
        let id = Ulid::from_bytes([1; 16]);
        let twice = KeyScope::Writes(vec![("a".into(), id), ("b".into(), id)]);
        assert_eq!(twice.validate(), Err(AbeError::Scope));
        assert!(writes.policy(&here, &[1, 2]).is_err());
        assert!(sub("a/").policy(&here, &[1, 1]).is_err());
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
