//! Reopens key requests for remaining readers after an epoch raise, one bounded page at a time.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::epoch::{parse_progress, progress};
use super::*;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{done, values};
use aruna_core::structs::storage::blob::UserAccess;
use aruna_core::{NodeId, UserId};

#[derive(Clone, Copy, Debug, PartialEq)]
enum Step {
    Settings,
    Rows,
    Page,
    Access,
    Run,
    Save,
}

type ReissueTurn = Result<Next<bool>, KeyError>;

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
        }
    }
    fn bucket_key(&self) -> Key {
        self.bucket_id
            .unwrap_or_default()
            .to_bytes()
            .to_vec()
            .into()
    }
    fn settings_read(&mut self, value: Option<Value>) -> ReissueTurn {
        let settings = BucketEncryption::from_row(value.as_deref()).ok();
        let Some(key) = settings.and_then(|s| s.active_key()) else {
            return Ok(Next::Done(false));
        };
        self.bucket_id = Some(key.bucket_id);
        self.step = Step::Rows;
        let id = self.bucket_key();
        Ok(Next::Call(IoCall::Read(vec![
            (ABE_EPOCH_KEYSPACE.to_string(), id.clone()),
            (ABE_REISSUE_KEYSPACE.to_string(), id),
        ])))
    }
    fn rows_read(&mut self, epoch: Option<Value>, seen: Option<Value>) -> ReissueTurn {
        let epoch = epoch
            .as_deref()
            .and_then(|v| v.try_into().ok())
            .map(u64::from_be_bytes);
        let Some(seen) = seen else {
            return Ok(Next::Done(false));
        };
        let (Some(epoch), Some((at, requests, cursor))) = (epoch, parse_progress(&seen)) else {
            return Err(AbeError::Context.into());
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
        Ok(Next::Call(IoCall::Page {
            space: space.to_string(),
            prefix: self.bucket_key(),
            after: (!cursor.is_empty()).then(|| cursor.into()),
            limit: self.page,
        }))
    }
    fn page_read(&mut self, values: Vec<(Key, Value)>) -> ReissueTurn {
        let Some((epoch, requests, _)) = self.next.as_deref().and_then(parse_progress) else {
            return Err(AbeError::Context.into());
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
            return Ok(self.run_next());
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
        Ok(Next::Call(IoCall::Read(reads)))
    }
    /// Continuing token credentials get their requests under their owner, narrowed as before.
    fn access_read(&mut self, values: Vec<Option<Value>>) -> Next<bool> {
        for ((access_key, public_key), value) in
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
    fn run_next(&mut self) -> Next<bool> {
        let Some(run) = self.runs.pop() else {
            self.step = Step::Save;
            return Next::Call(IoCall::child(ProgressWrite {
                key: self.bucket_key(),
                seen: self.seen.clone(),
                next: self.next.clone(),
                saving: false,
            }));
        };
        self.step = Step::Run;
        Next::Call(IoCall::child(run))
    }
    /// Only a recipient no longer eligible is skipped; other failures keep the page for a retry.
    fn ran(&mut self, result: Result<KeyResult, KeyError>) -> ReissueTurn {
        match result {
            Ok(_)
            | Err(KeyError::Denied | KeyError::Missing)
            | Err(KeyError::Abe(AbeError::Stale | AbeError::Scope)) => Ok(self.run_next()),
            Err(error) => {
                tracing::warn!(event = "abe.reissue.failed", error = %error);
                Err(KeyError::Storage)
            }
        }
    }
}

/// Advances the cursor in one write transaction, only while the progress row is the one the
/// page started from; returns whether pages remain.
#[derive(Debug, PartialEq)]
struct ProgressWrite {
    key: Key,
    seen: Vec<u8>,
    next: Option<Vec<u8>>,
    saving: bool,
}

impl Operation for ProgressWrite {
    type Output = bool;
    type Error = KeyError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> ReissueTurn {
        let row = (ABE_REISSUE_KEYSPACE.to_string(), self.key.clone());
        Ok(Next::Call(IoCall::Read(vec![row])))
    }

    fn next(&mut self, result: IoResult) -> ReissueTurn {
        if self.saving {
            done(result)?;
            return Ok(Next::Done(self.next.is_some()));
        }
        let [value] = values::<1>(result)?;
        if value.as_deref() != Some(self.seen.as_slice()) {
            return Ok(Next::Done(true));
        }
        self.saving = true;
        let space = ABE_REISSUE_KEYSPACE.to_string();
        Ok(Next::Call(match self.next.clone() {
            Some(row) => IoCall::Write(vec![(space, self.key.clone(), row.into())]),
            None => IoCall::Delete(vec![(space, self.key.clone())]),
        }))
    }
}

impl KeyOperation {
    /// After node issuance, runs the next page of a raise's saved reissue progress, if any.
    pub(super) fn walk(&self) -> Option<IoCall> {
        let issuing = matches!(
            self.action,
            KeyAction::Request(_)
                | KeyAction::Member(_)
                | KeyAction::Token { .. }
                | KeyAction::Writes(_)
        );
        if self.quiet || !issuing {
            return None;
        }
        let bucket = self.bucket.clone();
        let page =
            ReissueOperation::new(bucket, self.auth.clone(), self.node, self.now, MAX_REQUESTS);
        Some(IoCall::child(page))
    }
}
impl Operation for ReissueOperation {
    type Output = bool;
    type Error = KeyError;

    fn first(&mut self) -> ReissueTurn {
        Ok(Next::Call(IoCall::Read(vec![(
            BUCKET_ENCRYPTION_KEYSPACE.to_string(),
            self.bucket.as_bytes().to_vec().into(),
        )])))
    }

    fn next(&mut self, result: IoResult) -> ReissueTurn {
        match self.step {
            Step::Settings => {
                let [value] = values::<1>(result)?;
                self.settings_read(value)
            }
            Step::Rows => {
                let [epoch, seen] = values::<2>(result)?;
                self.rows_read(epoch, seen)
            }
            Step::Page => {
                let (rows, _) = result.page()?;
                self.page_read(rows)
            }
            Step::Access => Ok(self.access_read(result.values()?)),
            Step::Run => {
                let ran = result.child::<KeyResult, KeyError>()?;
                self.ran(ran)
            }
            Step::Save => result.child::<bool, KeyError>()?.map(Next::Done),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;

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
        let turn = operation.next(IoResult::Page { rows, next: None });
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
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
    fn failure_keeps_cursor() {
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
        let failed = Err::<KeyResult, _>(KeyError::Conflict);
        // No progress write follows: a later run resumes from the saved cursor.
        let turn = operation.next(IoResult::Child(ChildOutcome::new(failed)));
        assert_eq!(turn, Err(KeyError::Storage));
    }

    #[test]
    fn progress_moves_once() {
        // The cursor moves only while the stored progress is the row this page started from.
        let key = Key::from(vec![1u8; 16]);
        let mut write = ProgressWrite {
            key: key.clone(),
            seen: b"seen".to_vec(),
            next: Some(b"next".to_vec()),
            saving: false,
        };
        assert_eq!(write.transaction(), TransactionMode::Write);
        write.first().unwrap();
        let moved = IoResult::Values(vec![Some(Value::from(b"other".to_vec()))]);
        assert_eq!(write.next(moved), Ok(Next::Done(true)));

        let mut write = ProgressWrite {
            key: key.clone(),
            seen: b"seen".to_vec(),
            next: None,
            saving: false,
        };
        write.first().unwrap();
        let same = IoResult::Values(vec![Some(Value::from(b"seen".to_vec()))]);
        let space = ABE_REISSUE_KEYSPACE.to_string();
        assert_eq!(
            write.next(same),
            Ok(Next::Call(IoCall::Delete(vec![(space, key)])))
        );
        assert_eq!(write.next(IoResult::Done), Ok(Next::Done(false)));
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
        let failed = run.next(IoResult::Values(vec![None; 8]));
        assert_eq!(failed, Err(KeyError::Storage));
        // No progress write follows: a later run resumes from the saved cursor.
        let failed = failed.map(|_| KeyResult::Opened(Vec::new()));
        let ran = IoResult::Child(ChildOutcome::new(failed));
        assert_eq!(operation.next(ran), Err(KeyError::Storage));
    }
}
