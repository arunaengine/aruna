//! Reopens key requests for remaining readers after an epoch raise, one bounded page at a time.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::epoch::{parse_progress, progress};
use super::*;
use aruna_core::events::SubOperationEvent;
use aruna_core::operation::boxed_suboperation;
use aruna_core::structs::storage::blob::UserAccess;
use aruna_core::{NodeId, UserId};

#[derive(Clone, Copy, Debug, PartialEq)]
enum Step {
    Settings,
    Rows,
    Page,
    Access,
    Run,
    Start,
    Check,
    Save,
    Commit,
    Done,
}

/// Reopens member and token key requests at the current epoch for one page of grant or request
/// rows after a raise; the node issues them at once while it can. Returns whether pages remain,
/// and the stored cursor lets a later run resume after a crash.
#[derive(Debug, PartialEq)]
pub struct ReissueOperation {
    bucket: String,
    auth: AuthContext,
    node: NodeId,
    now: u64,
    page: usize,
    step: Step,
    bucket_id: Option<Ulid>,
    seen: Vec<u8>,
    next: Option<Vec<u8>>,
    users: Vec<(UserId, KeyScope, Option<Vec<PathRestriction>>)>,
    tokens: Vec<(String, [u8; 32])>,
    runs: Vec<KeyOperation>,
    txn: Option<TxnId>,
    output: Option<Result<bool, KeyError>>,
}
impl ReissueOperation {
    pub fn new(bucket: String, auth: AuthContext, node: NodeId, now: u64, page: usize) -> Self {
        Self {
            bucket,
            auth,
            node,
            now,
            page: page.max(1),
            step: Step::Settings,
            bucket_id: None,
            seen: Vec::new(),
            next: None,
            users: Vec::new(),
            tokens: Vec::new(),
            runs: Vec::new(),
            txn: None,
            output: None,
        }
    }
    fn finish(&mut self, result: Result<bool, KeyError>) -> Effects {
        self.step = Step::Done;
        self.output = Some(result);
        self.abort()
    }
    fn bucket_key(&self) -> Key {
        self.bucket_id
            .unwrap_or_default()
            .to_bytes()
            .to_vec()
            .into()
    }
    fn rows_read(&mut self, values: Vec<(Key, Option<Value>)>) -> Effects {
        let epoch = values.first().and_then(|(_, v)| v.as_deref());
        let epoch = epoch
            .and_then(|v| v.try_into().ok())
            .map(u64::from_be_bytes);
        let Some(seen) = values.get(1).and_then(|(_, v)| v.clone()) else {
            return self.finish(Ok(false));
        };
        let (Some(epoch), Some((at, requests, cursor))) = (epoch, parse_progress(&seen)) else {
            return self.finish(Err(AbeError::Context.into()));
        };
        self.seen = seen.to_vec();
        // A newer raise restarts from the first row.
        let (requests, cursor) = if at == epoch {
            (requests, cursor)
        } else {
            (false, Vec::new())
        };
        let space = if requests {
            ABE_REQUEST_KEYSPACE
        } else {
            ABE_GRANT_KEYSPACE
        };
        self.step = Step::Page;
        self.next = Some(progress(epoch, requests, &[]));
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: space.to_string(),
            prefix: Some(self.bucket_key()),
            start: (!cursor.is_empty()).then(|| IterStart::After(cursor.into())),
            limit: self.page,
            txn_id: None
        })]
    }
    fn page_read(&mut self, values: Vec<(Key, Value)>) -> Effects {
        let Some((epoch, requests, _)) = self.next.as_deref().and_then(parse_progress) else {
            return self.finish(Err(AbeError::Context.into()));
        };
        self.next = match values.last() {
            Some((key, _)) if values.len() >= self.page => Some(progress(epoch, requests, key)),
            _ if !requests => Some(progress(epoch, true, &[])),
            _ => None,
        };
        for (_, value) in &values {
            let request = KeyGrant::from_bytes(value)
                .map(|g| g.context.request)
                .or_else(|_| KeyRequest::from_bytes(value));
            let Ok(request) = request else {
                continue;
            };
            // An enumerated grant never covers later writes, so it is not reopened.
            if matches!(request.scope, KeyScope::Writes(_)) {
                continue;
            }
            match (request.credential_id, request.recipient_public) {
                (Some(access_key), Some(public)) => {
                    if !self.tokens.iter().any(|(k, _)| *k == access_key) {
                        self.tokens.push((access_key, public));
                    }
                }
                (Some(_), None) => {}
                // A saved scope is reopened as it was, so it is neither dropped nor broadened.
                (None, _) => {
                    let saved = (request.recipient_user, request.scope, request.restrictions);
                    if !self.users.contains(&saved) {
                        self.users.push(saved);
                    }
                }
            }
        }
        for (user_id, scope, path_restrictions) in std::mem::take(&mut self.users) {
            let auth = AuthContext {
                user_id,
                realm_id: self.auth.realm_id,
                path_restrictions,
                session: None,
            };
            let action = KeyAction::Request(scope);
            let run = KeyOperation::new(self.bucket.clone(), auth, self.node, action, self.now);
            self.runs.push(run.quiet());
        }
        if self.tokens.is_empty() {
            return self.run_next();
        }
        self.step = Step::Access;
        let reads = self
            .tokens
            .iter()
            .map(|(k, _)| {
                (
                    USER_ACCESS_KEYSPACE.to_string(),
                    k.as_bytes().to_vec().into(),
                )
            })
            .collect();
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None
        })]
    }
    /// Continuing token credentials get their requests under their owner, narrowed as before.
    fn access_read(&mut self, values: Vec<(Key, Option<Value>)>) -> Effects {
        for ((access_key, public_key), (_, value)) in
            std::mem::take(&mut self.tokens).into_iter().zip(values)
        {
            let Some(access) = value.and_then(|v| UserAccess::from_bytes(&v).ok()) else {
                continue;
            };
            let auth = AuthContext {
                user_id: access.user_identity,
                realm_id: self.auth.realm_id,
                path_restrictions: None,
                session: None,
            };
            let action = KeyAction::Token {
                access_key,
                public_key,
                restrictions: access.path_restrictions,
            };
            let run = KeyOperation::new(self.bucket.clone(), auth, self.node, action, self.now);
            self.runs.push(run.quiet());
        }
        self.run_next()
    }
    fn run_next(&mut self) -> Effects {
        let Some(run) = self.runs.pop() else {
            self.step = Step::Start;
            return smallvec![Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })];
        };
        self.step = Step::Run;
        // Only a recipient no longer eligible is skipped; other failures keep the page for a retry.
        let sub = boxed_suboperation(run, |result| match result {
            Ok(_)
            | Err(KeyError::Denied | KeyError::Missing)
            | Err(KeyError::Abe(AbeError::Stale | AbeError::Scope)) => {
                Event::SubOperation(SubOperationEvent::KeyRequestsOpened {
                    request_ids: Vec::new(),
                })
            }
            Err(error) => {
                tracing::warn!(event = "abe.reissue.failed", error = %error);
                Event::SubOperation(SubOperationEvent::KeyRequestsFailed)
            }
        });
        smallvec![Effect::SubOperation(sub)]
    }
    /// Advances the cursor only while the progress row is the one this page started from.
    fn check_read(&mut self, value: Option<Value>) -> Effects {
        let Some(txn_id) = self.txn else {
            return self.finish(Err(KeyError::Storage));
        };
        if value.as_deref() != Some(self.seen.as_slice()) {
            return self.finish(Ok(true));
        }
        let key = self.bucket_key();
        self.step = Step::Save;
        let effect = match self.next.clone() {
            Some(row) => StorageEffect::BatchWrite {
                writes: vec![(ABE_REISSUE_KEYSPACE.to_string(), key, row.into())],
                txn_id: Some(txn_id),
            },
            None => StorageEffect::BatchDelete {
                deletes: vec![(ABE_REISSUE_KEYSPACE.to_string(), key)],
                txn_id: Some(txn_id),
            },
        };
        smallvec![Effect::Storage(effect)]
    }
}
impl KeyOperation {
    /// After node issuance, runs the next page of a raise's saved reissue progress, if any.
    pub(super) fn walk(&mut self) -> Effects {
        let issuing = matches!(
            self.action,
            KeyAction::Request(_) | KeyAction::Member(_) | KeyAction::Token { .. }
        );
        if self.quiet || !issuing {
            self.state = State::Done;
            self.output = self.result.take().map(Ok);
            return smallvec![];
        }
        self.state = State::Reissue;
        let bucket = self.bucket.clone();
        let page =
            ReissueOperation::new(bucket, self.auth.clone(), self.node, self.now, MAX_REQUESTS);
        let sub = boxed_suboperation(page, |result| {
            let more = result.unwrap_or_else(|error| {
                tracing::warn!(event = "abe.reissue.failed", error = %error);
                false
            });
            Event::SubOperation(SubOperationEvent::ReissuePaged { more })
        });
        smallvec![Effect::SubOperation(sub)]
    }
}
impl Operation for ReissueOperation {
    type Output = bool;
    type Error = KeyError;
    fn start(&mut self) -> Effects {
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BUCKET_ENCRYPTION_KEYSPACE.to_string(),
            key: self.bucket.as_bytes().to_vec().into(),
            txn_id: None
        })]
    }
    fn step(&mut self, event: Event) -> Effects {
        match (self.step, event) {
            (Step::Settings, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                let settings = BucketEncryption::from_row(value.as_deref()).ok();
                let Some(key) = settings.and_then(|s| s.active_key()) else {
                    return self.finish(Ok(false));
                };
                self.bucket_id = Some(key.bucket_id);
                self.step = Step::Rows;
                let id = self.bucket_key();
                smallvec![Effect::Storage(StorageEffect::BatchRead {
                    reads: vec![
                        (ABE_EPOCH_KEYSPACE.to_string(), id.clone()),
                        (ABE_REISSUE_KEYSPACE.to_string(), id),
                    ],
                    txn_id: None
                })]
            }
            (Step::Rows, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.rows_read(values)
            }
            (Step::Page, Event::Storage(StorageEvent::IterResult { values, .. })) => {
                self.page_read(values)
            }
            (Step::Access, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.access_read(values)
            }
            (Step::Run, Event::SubOperation(SubOperationEvent::KeyRequestsOpened { .. })) => {
                self.run_next()
            }
            (Step::Run, Event::SubOperation(SubOperationEvent::KeyRequestsFailed)) => {
                self.finish(Err(KeyError::Storage))
            }
            (Step::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn = Some(txn_id);
                self.step = Step::Check;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: ABE_REISSUE_KEYSPACE.to_string(),
                    key: self.bucket_key(),
                    txn_id: Some(txn_id)
                })]
            }
            (Step::Check, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.check_read(value)
            }
            (
                Step::Save,
                Event::Storage(
                    StorageEvent::BatchWriteResult { .. } | StorageEvent::BatchDeleteResult { .. },
                ),
            ) => match self.txn {
                Some(txn_id) => {
                    self.step = Step::Commit;
                    smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
                }
                None => self.finish(Err(KeyError::Storage)),
            },
            (Step::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn = None;
                let more = self.next.is_some();
                self.finish(Ok(more))
            }
            _ => self.finish(Err(KeyError::Storage)),
        }
    }
    fn is_complete(&self) -> bool {
        self.step == Step::Done
    }
    fn finalize(self) -> Result<bool, KeyError> {
        self.output.unwrap_or(Err(KeyError::Storage))
    }
    fn abort(&mut self) -> Effects {
        self.txn.take().map_or_else(Effects::new, |txn_id| {
            smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reopens_saved_scopes() {
        let realm_id = aruna_core::structs::identity::realm::RealmId([1; 32]);
        let user = UserId::new(Ulid::from_bytes([5; 16]), realm_id);
        let node = iroh::SecretKey::from_bytes(&[7; 32]).public();
        let restricted = Some(vec![PathRestriction {
            pattern: "foo/**".into(),
            permission: aruna_core::structs::identity::auth::Permission::READ,
        }]);
        let saved = |id: u8, scope: &str, restrictions: Option<Vec<PathRestriction>>| {
            let request = KeyRequest {
                request_id: Ulid::from_bytes([id; 16]),
                requesting_user: user,
                recipient_user: user,
                recipient_record: None,
                recipient_public: None,
                recipient_fingerprint: None,
                bucket: "bucket".into(),
                parameters: AbeParameters {
                    realm_id,
                    node_id: node,
                    key: aruna_core::structs::storage::encryption::BucketKeyRef::new(
                        Ulid::from_bytes([4; 16]),
                        1,
                    ),
                    fingerprint: [7; 32],
                    parameters: vec![9; 3],
                },
                scope: KeyScope::Subtree(scope.into()),
                epochs: vec![1],
                credential_id: None,
                restrictions,
                revisions: Vec::new(),
                created_at_ms: 1,
            };
            (request.key().into(), request.to_bytes().unwrap().into())
        };
        let auth = AuthContext::anonymous(realm_id);
        let mut operation = ReissueOperation::new("bucket".into(), auth, node, 1, 64);
        operation.step = Step::Page;
        operation.next = Some(progress(2, true, &[]));
        let rows = vec![saved(1, "foo/", restricted.clone()), saved(2, "bar/", None)];
        let effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values: rows,
            next_start_after: None,
        }));
        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
        // The narrow scope keeps its restrictions instead of a scope derived from direct roles.
        let [waiting] = operation.runs.as_slice() else {
            panic!("one run waits");
        };
        let scope = KeyScope::Subtree("foo/".into());
        assert_eq!(waiting.action, KeyAction::Request(scope));
        assert_eq!(waiting.auth.user_id, user);
        assert_eq!(waiting.auth.path_restrictions, restricted);
    }

    #[test]
    fn failed_run_keeps_cursor() {
        let realm_id = aruna_core::structs::identity::realm::RealmId([1; 32]);
        let auth = AuthContext {
            user_id: UserId::nil(realm_id),
            realm_id,
            path_restrictions: None,
            session: None,
        };
        let node = iroh::SecretKey::from_bytes(&[7; 32]).public();
        let mut operation = ReissueOperation::new("bucket".into(), auth, node, 1, 1);
        operation.step = Step::Run;
        operation.next = Some(progress(2, false, b"next"));
        let failed = Event::SubOperation(SubOperationEvent::KeyRequestsFailed);
        // No progress write follows: a later run resumes from the saved cursor.
        assert!(operation.step(failed).is_empty());
        assert_eq!(operation.finalize(), Err(KeyError::Storage));
    }

    #[test]
    fn missing_user_retries() {
        // An absent recipient user row is unknown status: the run fails and the cursor stays.
        let realm_id = aruna_core::structs::identity::realm::RealmId([1; 32]);
        let auth = AuthContext::anonymous(realm_id);
        let node = iroh::SecretKey::from_bytes(&[7; 32]).public();
        let mut operation = ReissueOperation::new("bucket".into(), auth.clone(), node, 1, 1);
        operation.step = Step::Run;
        operation.next = Some(progress(2, false, b"next"));
        let action = KeyAction::Request(KeyScope::Subtree("foo/".into()));
        let mut run = KeyOperation::new("bucket".into(), auth, node, action, 1).quiet();
        run.state = State::Snapshot;
        operation.runs.push(run);
        let Some(Effect::SubOperation(mut sub)) = operation.run_next().pop() else {
            panic!("one run starts");
        };
        let values = vec![(Key::from(Vec::new()), None); 8];
        assert!(
            sub.step(Event::Storage(StorageEvent::BatchReadResult { values }))
                .is_empty()
        );
        assert!(sub.is_complete());
        // No progress write follows: a later run resumes from the saved cursor.
        assert!(operation.step(sub.finalize()).is_empty());
        assert_eq!(operation.finalize(), Err(KeyError::Storage));
    }
}
