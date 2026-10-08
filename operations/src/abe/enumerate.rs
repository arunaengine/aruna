//! Lists the current writes under a prefix that the recipient may read, grouped by their epoch.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::structs::storage::abe::ObjectEnvelope;
use aruna_core::structs::storage::blob::{BlobHeadKey, CurrentVersionPointer, VersionKey};
use std::collections::BTreeMap;

/// Current objects one enumeration scans under its prefix.
const HEADS: usize = 1024;

impl KeyOperation {
    /// Scans the current objects under `prefix` outside the transaction: a later write is never
    /// covered, so it need not conflict.
    pub(super) fn list_heads(&mut self, prefix: &str) -> Effects {
        let Ok(start) = BlobHeadKey::object_prefix(&self.bucket, prefix) else {
            return self.fail(AbeError::Scope);
        };
        self.state = State::Heads;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            prefix: Some(start.into()),
            start: None,
            limit: HEADS + 1,
            txn_id: None,
        })]
    }

    /// Keeps the objects the recipient may read; more than one grant may name is refused.
    pub(super) fn heads_read(&mut self, values: Vec<(Key, Value)>) -> Effects {
        let full = values.len() > HEADS;
        let mut heads = Vec::new();
        for (row, value) in values.into_iter().take(HEADS) {
            let head = BlobHeadKey::from_bytes(&row);
            let (Ok(head), Ok(pointer)) = (head, CurrentVersionPointer::from_bytes(&value)) else {
                return self.fail(KeyError::Storage);
            };
            heads.push((head.key, pointer.version_id));
        }
        let keys: Vec<&str> = heads.iter().map(|(key, _)| key.as_str()).collect();
        let readable = match self.readable(&keys) {
            Ok(readable) => readable,
            Err(error) => return self.fail(error),
        };
        let heads: Vec<_> = (heads.into_iter().zip(readable))
            .filter_map(|(head, ok)| ok.then_some(head))
            .collect();
        // A scan that did not reach the end may hide more readable files.
        if heads.len() > MAX_WRITES || full {
            let over = heads.len().saturating_sub(MAX_WRITES).max(1);
            return self.fail(KeyError::Bound(over));
        }
        let mut reads = Vec::new();
        for (key, version) in &heads {
            match VersionKey::new(&self.bucket, key, *version).to_bytes() {
                Ok(row) => reads.push((ABE_VERSION_KEYSPACE.to_string(), row.into())),
                Err(_) => return self.fail(KeyError::Storage),
            }
        }
        self.listed = heads;
        self.state = State::Writes;
        self.batch(reads)
    }

    /// Replaces each version id with its envelope id; versions without an envelope drop out.
    pub(super) fn writes_read(&mut self, values: Vec<(Key, Option<Value>)>) -> Effects {
        let listed = std::mem::take(&mut self.listed);
        for ((key, _), (_, id)) in listed.into_iter().zip(values) {
            if let Some(id) = id.and_then(|id| <[u8; 16]>::try_from(id.as_ref()).ok()) {
                self.listed.push((key, Ulid::from_bytes(id)));
            }
        }
        let reads = (self.listed.iter())
            .map(|(_, id)| {
                (
                    ABE_ENVELOPE_KEYSPACE.to_string(),
                    id.to_bytes().to_vec().into(),
                )
            })
            .collect();
        self.state = State::Envelopes;
        self.batch(reads)
    }

    /// Groups the writes of the current parameters by their envelope epoch, one scope each.
    pub(super) fn envelopes_read(&mut self, values: Vec<(Key, Option<Value>)>) -> Effects {
        let Some(parameters) = self.snapshot.as_ref().map(|s| s.parameters.clone()) else {
            return self.fail(KeyError::Missing);
        };
        let mut groups: BTreeMap<u64, Vec<(String, Ulid)>> = BTreeMap::new();
        for ((key, id), (_, value)) in std::mem::take(&mut self.listed).into_iter().zip(values) {
            let Some(Ok(envelope)) = value.map(|v| ObjectEnvelope::from_bytes(&v)) else {
                continue;
            };
            let context = &envelope.context;
            if context.object_key == key
                && context.write_id == id
                && context.parameters == parameters
            {
                groups.entry(context.epoch).or_default().push((key, id));
            }
        }
        for (epoch, writes) in groups {
            self.groups.push(epoch);
            self.scopes.push(KeyScope::Writes(writes));
        }
        if self.scopes.is_empty() {
            self.result = Some(KeyResult::Opened(Vec::new()));
            return self.flush();
        }
        self.read_keys()
    }

    fn batch(&mut self, reads: Vec<(String, Key)>) -> Effects {
        if reads.is_empty() {
            return self.envelopes_read(Vec::new());
        }
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None
        })]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::permission_rules::{CollectedRole, PermissionRules};
    use aruna_core::compute::SecretBytes;
    use aruna_core::request_policy::{PolicyKind, RequestPolicy};
    use aruna_core::structs::identity::auth::{Permission, Role};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::abe::{EnvelopePlan, create_envelope, create_parameters};
    use aruna_core::structs::storage::encryption::BucketKeyRef;
    use snapshot::Snapshot;

    /// A writes run for foo/ that reads foo/ but not foo/private/, with a CEL deny on foo/tmp/x.
    fn listing() -> KeyOperation {
        let realm_id = RealmId([1; 32]);
        let user = aruna_core::UserId::new(Ulid::from_bytes([5; 16]), realm_id);
        let node = iroh::SecretKey::from_bytes(&[7; 32]).public();
        let auth = AuthContext {
            user_id: user,
            realm_id,
            path_restrictions: None,
            session: None,
        };
        let action = KeyAction::Writes("foo/".into());
        let mut operation = KeyOperation::new("bucket".into(), auth, node, action, 1);
        let group_id = Ulid::from_bytes([8; 16]);
        let root = aruna_core::structs::storage::blob::bucket_permission_path(
            realm_id, group_id, node, "bucket",
        );
        let role = CollectedRole {
            role: Role {
                role_id: Ulid::from_bytes([2; 16]),
                name: "reader".into(),
                permissions: [
                    (format!("{root}/foo/**"), Permission::READ),
                    (format!("{root}/foo/private/**"), Permission::DENY),
                ]
                .into(),
                assigned_users: [user].into(),
            },
            direct: true,
            public: false,
        };
        let deny = RequestPolicy {
            policy_id: Ulid::from_bytes([9; 16]),
            name: "no-tmp".into(),
            kind: PolicyKind::Deny,
            when: None,
            expression: "path.endsWith('/foo/tmp/x')".into(),
            enabled: true,
        };
        let secret = SecretBytes::new(vec![9; 32]);
        let key = BucketKeyRef::new(Ulid::from_bytes([4; 16]), 1);
        operation.info = Some(BucketInfo {
            group_id,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: user,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Default::default(),
        });
        operation.snapshot = Some(Snapshot {
            parameters: create_parameters(&secret, realm_id, node, key).unwrap(),
            epoch: 3,
            revisions: Vec::new(),
            rules: PermissionRules::from_roles(vec![role], None).unwrap(),
            holder: false,
            holders: Default::default(),
            policies: true,
            due: false,
            checks: [Vec::new(), vec![deny]],
        });
        operation.state = State::Heads;
        operation
    }

    fn head(key: &str, version: u8) -> (Key, Value) {
        let row = BlobHeadKey::new("bucket", key).to_bytes().unwrap();
        let pointer = CurrentVersionPointer::new(Ulid::from_bytes([version; 16]));
        (row.into(), pointer.to_bytes().unwrap().into())
    }

    fn read_keys(effects: &Effects) -> usize {
        match effects.as_slice() {
            [Effect::Storage(StorageEffect::BatchRead { reads, .. })] => reads.len(),
            _ => panic!("one batch read: {effects:?}"),
        }
    }

    #[test]
    fn lists_readable_writes() {
        // DENY and the CEL policy drop their files; a file without an envelope drops out too.
        let mut operation = listing();
        let heads = ["foo/a", "foo/private/s", "foo/c", "foo/tmp/x"];
        let values = (heads.iter().enumerate())
            .map(|(index, key)| head(key, index as u8 + 1))
            .collect();
        let effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after: None,
        }));
        assert_eq!(read_keys(&effects), 2);
        let write = Ulid::from_bytes([11; 16]);
        let values = vec![
            (
                Key::from(Vec::new()),
                Some(write.to_bytes().to_vec().into()),
            ),
            (Key::from(Vec::new()), None),
        ];
        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult { values }));
        assert_eq!(read_keys(&effects), 1);
        let parameters = operation.snapshot.as_ref().unwrap().parameters.clone();
        let plan = EnvelopePlan {
            parameters,
            epoch: 2,
            write_id: write,
            object_key: "foo/a".into(),
            bucket_public: [3; 32],
        };
        let (envelope, _) = create_envelope(plan).unwrap();
        let values = vec![(
            Key::from(Vec::new()),
            Some(envelope.to_bytes().unwrap().into()),
        )];
        operation.step(Event::Storage(StorageEvent::BatchReadResult { values }));
        // One scope of the write's own epoch waits for the recipient's keys.
        assert_eq!(operation.state, State::Keys);
        let scope = KeyScope::Writes(vec![("foo/a".into(), write)]);
        assert_eq!(
            (operation.scopes.clone(), operation.groups.clone()),
            (vec![scope], vec![2])
        );
    }

    #[test]
    fn bound_refused() {
        let mut operation = listing();
        let values = (0..=MAX_WRITES)
            .map(|index| head(&format!("foo/{index}"), index as u8 + 1))
            .collect();
        let effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after: None,
        }));
        assert!(effects.is_empty());
        assert_eq!(operation.finalize(), Err(KeyError::Bound(1)));
    }

    #[test]
    fn scopes_rechecked() {
        // A write that is no longer readable fails the whole grant; a partly readable subtree
        // points to enumeration, an unreadable one is refused.
        let operation = listing();
        let id = Ulid::from_bytes([1; 16]);
        let denied = KeyScope::Writes(vec![("foo/a".into(), id), ("foo/tmp/x".into(), id)]);
        assert_eq!(operation.scope_allowed(&denied), Err(KeyError::Denied));
        let fine = KeyScope::Writes(vec![("foo/a".into(), id)]);
        assert_eq!(operation.scope_allowed(&fine), Ok(()));
        let mut continuing = listing();
        continuing.snapshot.as_mut().unwrap().policies = false;
        let scope = KeyScope::Subtree("foo/".into());
        assert_eq!(
            continuing.scope_allowed(&scope),
            Err(AbeError::Scope.into())
        );
        let scope = KeyScope::Subtree("bar/".into());
        assert_eq!(continuing.scope_allowed(&scope), Err(KeyError::Denied));
    }

    #[test]
    fn session_policy_checked() {
        // The caller's own session is evaluated; a holder publishing later cannot evaluate it.
        use aruna_core::structs::identity::auth::{SessionKind, SessionRef};
        let session = RequestPolicy {
            policy_id: Ulid::from_bytes([10; 16]),
            name: "no-assistant".into(),
            kind: PolicyKind::Deny,
            when: None,
            expression: "permission == 'read' && request.session.kind == 'assistant'".into(),
            enabled: true,
        };
        let id = Ulid::from_bytes([1; 16]);
        let scope = KeyScope::Writes(vec![("foo/a".into(), id)]);
        let mut operation = listing();
        operation.snapshot.as_mut().unwrap().checks[1].push(session);
        assert_eq!(operation.scope_allowed(&scope), Ok(()));
        operation.auth.session = Some(SessionRef {
            sid: Ulid::from_bytes([12; 16]).to_string(),
            kind: SessionKind::Assistant,
        });
        assert_eq!(operation.scope_allowed(&scope), Err(KeyError::Denied));
        let recipient = operation.auth.user_id;
        let parameters = operation.snapshot.as_ref().unwrap().parameters.clone();
        let request = KeyRequest {
            request_id: id,
            requesting_user: recipient,
            recipient_user: recipient,
            recipient_record: Some(id),
            recipient_public: Some([1; 32]),
            recipient_fingerprint: Some([2; 32]),
            bucket: "bucket".into(),
            parameters,
            scope: scope.clone(),
            epochs: vec![3],
            credential_id: None,
            restrictions: None,
            revisions: Vec::new(),
            created_at_ms: 0,
        };
        let issuer = KeyIssuer::User(recipient);
        let grant = KeyGrant {
            context: GrantContext { request, issuer },
            enc: [0; 32],
            ciphertext: vec![0; 16],
        };
        operation.action = KeyAction::Publish(grant);
        operation.auth.session = None;
        assert_eq!(operation.scope_allowed(&scope), Err(KeyError::Session));
    }
}
