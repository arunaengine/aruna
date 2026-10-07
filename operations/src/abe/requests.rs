//! Rechecks request bindings before publishing recipient-sealed keys.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::notifications::outbox::new_outbox_record;
use aruna_core::storage_entries::outbox_write_entry;
use aruna_core::structs::execution::notification::{
    NotificationClass, NotificationKind, NotificationRecord,
};
use aruna_core::structs::storage::blob::UserAccess;
use aruna_core::structs::storage::encryption::{BucketHolder, HolderOrigin};
use std::time::{Duration, SystemTime};

impl KeyOperation {
    /// Checks the bucket-wide request bindings that do not depend on the recipient.
    fn current_bucket(&self, request: &KeyRequest) -> Result<(), KeyError> {
        let snapshot = self.snapshot.as_ref().ok_or(KeyError::Missing)?;
        if request.expired(self.now)
            || request.parameters != snapshot.parameters
            || request.epochs.iter().max() != Some(&snapshot.epoch)
            || request.revisions != snapshot.revisions
            || request.bucket != self.bucket
        {
            return Err(AbeError::Stale.into());
        }
        Ok(())
    }
    fn current_request(&self, request: &KeyRequest) -> Result<(), KeyError> {
        self.current_bucket(request)?;
        if request.recipient_user != self.recipient()
            || request.requesting_user != request.recipient_user
            || !self.recipient_current(request)
        {
            return Err(AbeError::Stale.into());
        }
        self.scope_allowed(&request.scope)
    }
    pub(super) fn grant_allowed(&self, request: &KeyRequest) -> Result<(), KeyError> {
        let snapshot = self.snapshot.as_ref().ok_or(KeyError::Missing)?;
        if request.parameters != snapshot.parameters
            || request.bucket != self.bucket
            || !self.recipient_current(request)
        {
            return Err(AbeError::Stale.into());
        }
        self.scope_allowed(&request.scope)
    }
    /// Whether `request` names the current recipient key: the newest user key, or a token key.
    fn recipient_current(&self, request: &KeyRequest) -> bool {
        let key = match (&self.action, &request.credential_id) {
            (_, None) => self
                .newest()
                .map(|k| (k.record_id, k.public_key, k.fingerprint)),
            (KeyAction::Token { access_key, .. }, Some(id)) if id != access_key => return false,
            (
                KeyAction::Token {
                    access_key,
                    public_key,
                    ..
                },
                _,
            ) => token_recipient(access_key, *public_key),
            // A stored token request keeps its key; revoking the credential deletes it.
            (_, Some(_)) => return request.recipient_public.is_some(),
        };
        request.recipient_record == key.map(|k| k.0)
            && request.recipient_public == key.map(|k| k.1)
            && request.recipient_fingerprint == key.map(|k| k.2)
    }
    /// The credential of a token action or of a published token request.
    pub(super) fn token_credential(&self) -> Option<String> {
        match &self.action {
            KeyAction::Token { access_key, .. } => Some(access_key.clone()),
            KeyAction::Publish(grant) => grant.context.request.credential_id.clone(),
            _ => None,
        }
    }
    /// Continues only while the token's credential is live, the recipient's, in the bucket's
    /// group and issued by this node; reading it here makes a revocation conflict.
    pub(super) fn credential_read(&mut self, value: Option<Value>) -> Effects {
        let now = SystemTime::UNIX_EPOCH + Duration::from_millis(self.now);
        let live = value
            .and_then(|v| UserAccess::from_bytes(&v).ok())
            .is_some_and(|access| {
                !access.is_expired(now)
                    && !access.is_revoked()
                    && access.user_identity == self.recipient()
                    && self.info.as_ref().map(|i| i.group_id) == Some(access.group_id)
                    && access.issued_by == *self.node.as_bytes()
            });
        if !live {
            return self.fail(AbeError::Stale);
        }
        // A token is its own recipient key, so the vault is not read.
        self.records()
    }
    /// The access key whose requests this action handles; user requests have none.
    fn credential(&self) -> Option<&str> {
        match &self.action {
            KeyAction::Token { access_key, .. } => Some(access_key),
            _ => None,
        }
    }
    pub(super) fn records_read(&mut self, event: Event) -> Effects {
        let action = self.action.clone();
        match (action, event) {
            (
                KeyAction::Publish(submitted),
                Event::Storage(StorageEvent::ReadResult { value, .. }),
            ) => {
                if !self.snapshot.as_ref().is_some_and(|s| s.holder)
                    || self.auth.path_restrictions.is_some()
                    || submitted.context.issuer != KeyIssuer::User(self.auth.user_id)
                {
                    return self.fail(KeyError::Denied);
                }
                let Some(value) = value else {
                    self.state = State::Existing;
                    return self.read(ABE_GRANT_KEYSPACE, submitted.context.request.key());
                };
                match KeyRequest::from_bytes(&value) {
                    Ok(request) if request == submitted.context.request => {
                        self.publish_grant(submitted)
                    }
                    Ok(_) => self.fail(AbeError::Stale),
                    Err(error) => self.fail(error),
                }
            }
            (
                KeyAction::Request(scope),
                Event::Storage(StorageEvent::IterResult { values, .. }),
            ) => self.request_read(scope, values),
            (
                KeyAction::Member(_) | KeyAction::Token { .. },
                Event::Storage(StorageEvent::IterResult { values, .. }),
            ) => match self.scopes.pop() {
                Some(scope) => self.request_read(scope, values),
                None => self.fail(AbeError::Context),
            },
            (KeyAction::Open(_), Event::Storage(StorageEvent::IterResult { values, .. })) => {
                let next = page_end(&values);
                let mut requests = Vec::new();
                for (key, value) in values.into_iter().take(MAX_REQUESTS) {
                    match KeyRequest::from_bytes(&value) {
                        // Stale requests would fail publication, so they are removed here.
                        Ok(r) if self.current_bucket(&r).is_err() => {
                            self.deletes.push((ABE_REQUEST_KEYSPACE.to_string(), key))
                        }
                        Ok(r) if r.recipient_public.is_some() => requests.push(r),
                        Ok(_) => {}
                        Err(error) => return self.fail(error),
                    }
                }
                self.result = Some(KeyResult::Requests(requests, next));
                self.flush()
            }
            (KeyAction::Grants(_), Event::Storage(StorageEvent::IterResult { values, .. })) => {
                let next = page_end(&values);
                let mut grants = Vec::new();
                for (key, value) in values.into_iter().take(MAX_REQUESTS) {
                    let grant = match KeyGrant::from_bytes(&value) {
                        Ok(g) if g.context.request.recipient_user == self.auth.user_id => g,
                        Ok(_) => return self.fail(KeyError::Denied),
                        Err(error) => return self.fail(error),
                    };
                    // Token grants are the credential's, listed with the bucket's tokens.
                    if grant.context.request.credential_id.is_some() {
                        continue;
                    }
                    if self.grant_allowed(&grant.context.request).is_err() {
                        self.deletes.push((ABE_GRANT_KEYSPACE.to_string(), key));
                    } else {
                        grants.push(grant);
                    }
                }
                self.result = Some(KeyResult::Grants(grants, next));
                self.flush()
            }
            _ => self.fail(AbeError::Context),
        }
    }
    fn request_read(&mut self, scope: KeyScope, values: Vec<(Key, Value)>) -> Effects {
        if let Err(error) = self.scope_allowed(&scope) {
            return self.fail(error);
        }
        let overflow = values.len() > MAX_REQUESTS;
        // A token's restrictions narrow its scopes instead.
        let restrictions = match self.action {
            KeyAction::Member(_) | KeyAction::Token { .. } => None,
            _ => self.auth.path_restrictions.clone(),
        };
        let mut open = Vec::new();
        for (key, value) in values {
            let request = match KeyRequest::from_bytes(&value) {
                Ok(r) => r,
                Err(error) => return self.fail(error),
            };
            if request.credential_id.as_deref() != self.credential() {
                continue;
            }
            // Other restrictions are judged only under their own rules, so they stay.
            if request.restrictions == restrictions && self.current_request(&request).is_err() {
                self.deletes.push((ABE_REQUEST_KEYSPACE.to_string(), key));
            } else {
                open.push(request);
            }
        }
        let same = open
            .iter()
            .position(|r| r.scope == scope && r.restrictions == restrictions);
        self.queue_full = overflow || open.len() >= MAX_REQUESTS;
        self.reused = same.is_some();
        let request = match (same, self.snapshot.as_ref()) {
            (Some(index), _) => open.swap_remove(index),
            (None, None) => return self.fail(KeyError::Missing),
            (None, Some(s)) => {
                let key = match &self.action {
                    KeyAction::Token {
                        access_key,
                        public_key,
                        ..
                    } => match token_recipient(access_key, *public_key) {
                        Some(key) => Some(key),
                        None => return self.fail(AbeError::Context),
                    },
                    _ => self
                        .newest()
                        .map(|k| (k.record_id, k.public_key, k.fingerprint)),
                };
                KeyRequest {
                    request_id: Ulid::generate(),
                    requesting_user: self.recipient(),
                    recipient_user: self.recipient(),
                    recipient_record: key.map(|k| k.0),
                    recipient_public: key.map(|k| k.1),
                    recipient_fingerprint: key.map(|k| k.2),
                    bucket: self.bucket.clone(),
                    parameters: s.parameters.clone(),
                    scope,
                    epochs: vec![s.epoch],
                    credential_id: self.credential().map(str::to_string),
                    restrictions,
                    revisions: s.revisions.clone(),
                    created_at_ms: self.now,
                }
            }
        };
        if same.is_none() {
            let prefix = request.prefix();
            self.request = Some(request);
            self.state = State::Reuse;
            return smallvec![Effect::Storage(StorageEffect::Iter {
                key_space: ABE_GRANT_KEYSPACE.to_string(),
                prefix: Some(prefix.into()),
                start: None,
                limit: MAX_REQUESTS + 1,
                txn_id: self.txn
            })];
        }
        self.issue(request)
    }
    pub(super) fn reuse_read(&mut self, values: Vec<(Key, Value)>) -> Effects {
        let (Some(mut request), Some(snapshot)) = (self.request.take(), self.snapshot.as_ref())
        else {
            return self.fail(KeyError::Missing);
        };
        // A continuation stops at the grant limit and keeps the batches it already wrote.
        if self.result.is_some() && values.len() >= MAX_REQUESTS {
            return self.flush();
        }
        let current = snapshot.epoch;
        let revisions = snapshot.revisions.clone();
        let mut covered = std::collections::BTreeSet::new();
        let mut held_grant = None;
        for (key, value) in values.into_iter().take(MAX_REQUESTS) {
            let grant = match KeyGrant::from_bytes(&value) {
                Ok(g) => g,
                Err(error) => return self.fail(error),
            };
            let held = &grant.context.request;
            // Other restrictions are judged only under their own rules, so they stay.
            if held.credential_id != request.credential_id
                || held.restrictions != request.restrictions
            {
                continue;
            }
            if self.grant_allowed(held).is_err() {
                // A holder's restrictions cannot judge the recipient's other grants.
                if !matches!(self.action, KeyAction::Publish(_)) {
                    self.deletes.push((ABE_GRANT_KEYSPACE.to_string(), key));
                }
                continue;
            }
            if held.scope == request.scope
                && held.restrictions == request.restrictions
                && held.revisions == revisions
            {
                covered.extend(held.epochs.iter().copied());
                if held.epochs.contains(&current) {
                    held_grant = Some(grant);
                }
            }
        }
        // Every epoch may hold data in scope: ask for the newest uncovered ones and the current one.
        let mut epochs: Vec<u64> = (1..=current)
            .rev()
            .filter(|e| !covered.contains(e))
            .take(MAX_EPOCHS + 1)
            .collect();
        if let (true, Some(grant)) = (epochs.is_empty(), held_grant) {
            self.result.get_or_insert(KeyResult::Grant(grant));
            return self.flush();
        }
        if epochs.first() != Some(&current) {
            epochs.insert(0, current);
        }
        self.more = epochs.len() > MAX_EPOCHS;
        epochs.truncate(MAX_EPOCHS);
        epochs.sort_unstable();
        request.epochs = epochs;
        // A full open-request queue refuses only a first batch without a reusable grant.
        if self.queue_full && self.result.is_none() {
            return self.fail(AbeError::Limit);
        }
        // After a holder's grant the next batch waits as one open request for a holder.
        if matches!(self.action, KeyAction::Publish(_)) {
            self.more = false;
            return self.write_request(request);
        }
        self.issue(request)
    }
    fn issue(&mut self, request: KeyRequest) -> Effects {
        if request.recipient_public.is_none() {
            self.result = Some(KeyResult::Request(request.clone()));
            return self.write_request(request);
        }
        self.request = Some(request.clone());
        self.state = State::Issue;
        let issuer = KeyIssuer::Node(self.node);
        let context = GrantContext { request, issuer };
        smallvec![Effect::Blob(BlobEffect::Abe(Box::new(AbeEffect::Issue(
            context
        ))))]
    }
    pub(super) fn publish_grant(&mut self, grant: KeyGrant) -> Effects {
        if let Err(error) = self.current_request(&grant.context.request) {
            return self.fail(error);
        }
        let prefix = grant.context.request.prefix();
        self.result = Some(KeyResult::Grant(grant));
        self.state = State::Count;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: ABE_GRANT_KEYSPACE.to_string(),
            prefix: Some(prefix.into()),
            start: None,
            limit: MAX_REQUESTS + 1,
            txn_id: self.txn
        })]
    }
    /// Writes the grant only while the recipient stores fewer than `MAX_REQUESTS` grants.
    pub(super) fn count_read(&mut self, values: Vec<(Key, Value)>) -> Effects {
        let Some(KeyResult::Grant(grant)) = self.result.take() else {
            return self.fail(AbeError::Context);
        };
        // Stored grants are only counted: this request's restrictions cannot judge other grants.
        if values.len() >= MAX_REQUESTS {
            return self.fail(AbeError::Limit);
        }
        let key: Key = grant.context.request.key().into();
        let bytes = match grant.to_bytes() {
            Ok(v) => v,
            Err(error) => return self.fail(error),
        };
        self.deletes
            .push((ABE_REQUEST_KEYSPACE.to_string(), key.clone()));
        self.writes
            .push((ABE_GRANT_KEYSPACE.to_string(), key, bytes.into()));
        self.index_token(&grant.context.request);
        self.result = Some(KeyResult::Grant(grant));
        // Any batch, even of a reused open request, may leave older epochs: rescan the coverage.
        self.more = true;
        self.flush()
    }
    /// Scans the recipient's grants again for the next batch of `request`'s scope.
    pub(super) fn next_batch(&mut self, mut request: KeyRequest) -> Effects {
        request.request_id = Ulid::generate();
        request.created_at_ms = self.now;
        let prefix = request.prefix();
        self.request = Some(request);
        self.state = State::Reuse;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: ABE_GRANT_KEYSPACE.to_string(),
            prefix: Some(prefix.into()),
            start: None,
            limit: MAX_REQUESTS + 1,
            txn_id: self.txn
        })]
    }
    pub(super) fn write_request(&mut self, request: KeyRequest) -> Effects {
        let bytes = match request.to_bytes() {
            Ok(v) => v,
            Err(error) => return self.fail(error),
        };
        self.writes.push((
            ABE_REQUEST_KEYSPACE.to_string(),
            request.key().into(),
            bytes.into(),
        ));
        self.index_token(&request);
        self.flush()
    }
    /// Lists a token's request or grant under its credential, so revocation finds it.
    fn index_token(&mut self, request: &KeyRequest) {
        if let Some(key) = request.token_key() {
            let row = (
                TOKEN_GRANT_KEYSPACE.to_string(),
                key.into(),
                Vec::new().into(),
            );
            self.writes.push(row);
        }
    }
}

impl KeyOperation {
    /// Records the finished scope, then starts the next or notifies holders of new requests.
    pub(super) fn next_scope(&mut self) -> Effects {
        if let Some(KeyResult::Request(request)) = self.result.take() {
            self.opened.push(request.request_id);
            self.fresh |= !self.reused;
        }
        if !self.scopes.is_empty() {
            return self.records();
        }
        let opened = std::mem::take(&mut self.opened);
        self.result = Some(KeyResult::Opened(opened));
        if self.fresh
            && !self.quiet
            && let Some(effects) = self.holders()
        {
            return effects;
        }
        self.flush()
    }
    /// Reads the bucket holders, including a caller who holds the bucket key.
    pub(super) fn holders(&mut self) -> Option<Effects> {
        let snapshot = self.snapshot.as_ref()?;
        let prefix = snapshot.parameters.key.bucket_id.to_bytes().to_vec();
        self.state = State::Holders;
        Some(smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: BUCKET_HOLDER_KEYSPACE.to_string(),
            prefix: Some(prefix.into()),
            start: None,
            limit: usize::MAX,
            txn_id: self.txn
        })])
    }
    /// Tells every current holder except the member that the member waits for keys.
    pub(super) fn notify_holders(&mut self, values: Vec<(Key, Value)>) -> Effects {
        let (Some(info), Some(snapshot)) = (&self.info, &self.snapshot) else {
            return self.fail(KeyError::Missing);
        };
        let mut holders = snapshot.holders.clone();
        for (_, value) in values {
            match BucketHolder::from_bytes(&value) {
                Ok(holder) if holder.origin == HolderOrigin::Explicit => {
                    holders.insert(holder.user_id);
                }
                Ok(_) => {}
                Err(_) => return self.fail(AbeError::Context),
            }
        }
        let member = self.recipient();
        holders.remove(&member);
        let kind = NotificationKind::BucketKeyPending {
            bucket: self.bucket.clone(),
            node_id: self.node,
            group_id: info.group_id,
            member_user_id: member,
        };
        for holder in holders {
            let class = NotificationClass::Direct;
            let record = NotificationRecord::new(holder, class, kind.clone(), self.now);
            match outbox_write_entry(&new_outbox_record(record)) {
                Ok(entry) => self.writes.push(entry),
                Err(_) => return self.fail(KeyError::Storage),
            }
        }
        self.notify = !self.writes.is_empty();
        self.flush()
    }
}

fn page_end(values: &[(Key, Value)]) -> Option<Vec<u8>> {
    (values.len() > MAX_REQUESTS).then(|| values[MAX_REQUESTS - 1].0.to_vec())
}
