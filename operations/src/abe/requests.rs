//! Rechecks request bindings before publishing recipient-sealed keys.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

impl KeyOperation {
    fn current_request(&self, request: &KeyRequest) -> Result<(), KeyError> {
        let snapshot = self.snapshot.as_ref().ok_or(KeyError::Missing)?;
        let key = self.newest();
        if request.expired(self.now)
            || request.parameters != snapshot.parameters
            || request.epochs != [snapshot.epoch]
            || request.revisions != snapshot.revisions
            || request.recipient_user != self.recipient()
            || request.requesting_user != request.recipient_user
            || request.recipient_record != key.map(|k| k.record_id)
            || request.recipient_public != key.map(|k| k.public_key)
            || request.recipient_fingerprint != key.map(|k| k.fingerprint)
            || request.bucket != self.bucket
        {
            return Err(AbeError::Stale.into());
        }
        self.scope_allowed(&request.scope)
    }
    pub(super) fn grant_allowed(&self, request: &KeyRequest) -> Result<(), KeyError> {
        let snapshot = self.snapshot.as_ref().ok_or(KeyError::Missing)?;
        let key = self.newest();
        if request.parameters != snapshot.parameters
            || request.bucket != self.bucket
            || request.recipient_record != key.map(|k| k.record_id)
            || request.recipient_public != key.map(|k| k.public_key)
            || request.recipient_fingerprint != key.map(|k| k.fingerprint)
        {
            return Err(AbeError::Stale.into());
        }
        self.scope_allowed(&request.scope)
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
            (KeyAction::Open(_), Event::Storage(StorageEvent::IterResult { values, .. })) => {
                let next = page_end(&values);
                let mut requests = Vec::new();
                for (_, value) in values.into_iter().take(MAX_REQUESTS) {
                    match KeyRequest::from_bytes(&value) {
                        Ok(r) if r.recipient_public.is_some() && !r.expired(self.now) => {
                            requests.push(r)
                        }
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
        let mut open = Vec::new();
        for (key, value) in values {
            let request = match KeyRequest::from_bytes(&value) {
                Ok(r) => r,
                Err(error) => return self.fail(error),
            };
            if self.current_request(&request).is_err() {
                self.deletes.push((ABE_REQUEST_KEYSPACE.to_string(), key));
            } else {
                open.push(request);
            }
        }
        let restrictions = self.auth.path_restrictions.clone();
        let same = open
            .iter()
            .position(|r| r.scope == scope && r.restrictions == restrictions);
        self.queue_full = overflow || open.len() >= MAX_REQUESTS;
        let request = match (same, self.snapshot.as_ref()) {
            (Some(index), _) => open.swap_remove(index),
            (None, None) => return self.fail(KeyError::Missing),
            (None, Some(s)) => {
                let key = self.newest();
                KeyRequest {
                    request_id: Ulid::generate(),
                    requesting_user: self.auth.user_id,
                    recipient_user: self.auth.user_id,
                    recipient_record: key.map(|k| k.record_id),
                    recipient_public: key.map(|k| k.public_key),
                    recipient_fingerprint: key.map(|k| k.fingerprint),
                    bucket: self.bucket.clone(),
                    parameters: s.parameters.clone(),
                    scope,
                    epochs: vec![s.epoch],
                    credential_id: None,
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
        let (Some(request), Some(snapshot)) = (self.request.take(), self.snapshot.as_ref()) else {
            return self.fail(KeyError::Missing);
        };
        let epochs = [snapshot.epoch];
        let revisions = snapshot.revisions.clone();
        for (key, value) in values.into_iter().take(MAX_REQUESTS) {
            let grant = match KeyGrant::from_bytes(&value) {
                Ok(g) => g,
                Err(error) => return self.fail(error),
            };
            let held = &grant.context.request;
            if self.grant_allowed(held).is_err() {
                self.deletes.push((ABE_GRANT_KEYSPACE.to_string(), key));
                continue;
            }
            if held.scope == request.scope
                && held.restrictions == request.restrictions
                && held.epochs == epochs
                && held.revisions == revisions
            {
                self.result = Some(KeyResult::Grant(grant));
                return self.flush();
            }
        }
        // A full open-request queue refuses only after no reusable grant is found.
        if self.queue_full {
            return self.fail(AbeError::Limit);
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
        self.result = Some(KeyResult::Grant(grant));
        self.flush()
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
        self.flush()
    }
}

fn page_end(values: &[(Key, Value)]) -> Option<Vec<u8>> {
    (values.len() > MAX_REQUESTS).then(|| values[MAX_REQUESTS - 1].0.to_vec())
}
