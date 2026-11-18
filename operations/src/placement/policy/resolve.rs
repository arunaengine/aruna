//! Resolves a policy ref to a verified policy: cache first, then a read, then a cache insert.
//! A cache miss costs latency only, and a missing policy is unavailable rather than denied.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::keyspaces::POLICY_CACHE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::{all_values, done, page, values};
use aruna_core::structs::identity::group::GroupAuthorizationDocument;
use aruna_core::structs::identity::realm::{
    RealmAuthorizationDocument, RealmConfigDocument, RealmId,
};
use aruna_core::structs::placement::policy::document::verify_policy_authority;
use aruna_core::structs::placement::policy::{PlacementPolicyRef, VerifiedPolicy};
use aruna_core::types::{Key, Value};
use byteview::ByteView;
use tracing::{debug, warn};

use crate::distribution::inventory::{StagedCall, outside_answer};

use super::cache::{
    CacheLookup, MAX_CACHE_ENTRIES, PolicyCacheEntry, PolicyCacheStats, cache_key, lookup,
    plan_eviction,
};
use super::read::{
    AuthenticPolicy, PolicySource, ReadPolicyConfig, ReadPolicyError, ReadPolicyOperation,
};

#[derive(Debug, Clone, PartialEq)]
pub struct ResolvePolicyConfig {
    pub realm_id: RealmId,
    pub policy_ref: PlacementPolicyRef,
    pub local_node_id: NodeId,
    pub now_ms: u64,
}

/// One resolved policy with the cache diagnostics of the run that produced it.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedPolicy {
    pub policy: VerifiedPolicy,
    pub source: PolicySource,
    pub stats: PolicyCacheStats,
}

#[derive(Debug, PartialEq)]
pub struct ResolvePolicyOperation {
    config: ResolvePolicyConfig,
    cached: Option<AuthenticPolicy>,
    pending: Option<PendingEntry>,
    stats: PolicyCacheStats,
    state: ResolveState,
    result: Option<Result<(VerifiedPolicy, PolicySource), ReadPolicyError>>,
}

#[derive(Debug, Clone, PartialEq)]
struct PendingEntry {
    key: Key,
    bytes: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum ResolveState {
    ReadCache,
    Revalidate,
    Resolve,
    Scan,
    Evict,
    Store,
}

type ResolveTurn = Result<Next<ResolvedPolicy>, ReadPolicyError>;

/// A cache access runs as its own child, so its failure only costs latency.
fn cache_call(call: IoCall) -> Next<ResolvedPolicy> {
    Next::Call(StagedCall::Outside(call).into_call())
}

impl ResolvePolicyOperation {
    pub fn new(config: ResolvePolicyConfig) -> Self {
        Self {
            config,
            cached: None,
            pending: None,
            stats: PolicyCacheStats::default(),
            state: ResolveState::ReadCache,
            result: None,
        }
    }

    fn start_read(&mut self) -> ResolveTurn {
        self.state = ResolveState::Resolve;
        Ok(Next::Call(IoCall::child(ReadPolicyOperation::new(
            ReadPolicyConfig {
                realm_id: self.config.realm_id,
                policy_ref: self.config.policy_ref,
                local_node_id: self.config.local_node_id,
            },
        ))))
    }

    fn cache_read(&mut self, value: Option<Value>) -> ResolveTurn {
        match lookup(
            value.as_ref(),
            self.config.realm_id,
            &self.config.policy_ref,
            self.config.now_ms,
        ) {
            CacheLookup::Hit { document, policy } => {
                self.stats.hits = 1;
                self.revalidate(AuthenticPolicy {
                    document: *document,
                    policy: *policy,
                })
            }
            CacheLookup::Negative => {
                self.stats.hits = 1;
                self.fail(ReadPolicyError::Unavailable(
                    "policy holders were recently unreachable".to_string(),
                ))
            }
            CacheLookup::Miss => {
                self.stats.misses = 1;
                self.start_read()
            }
        }
    }

    /// A cached entry is re-authenticated against the realm view, plus the
    /// owning group's roles when the rule names one.
    fn revalidate(&mut self, authentic: AuthenticPolicy) -> ResolveTurn {
        let owner = authentic.document.policy.owner_group_id;
        self.cached = Some(authentic);
        self.state = ResolveState::Revalidate;
        let config = DocumentTarget::RealmConfig {
            realm_id: self.config.realm_id,
        };
        let auth = DocumentTarget::RealmAuthorization {
            realm_id: self.config.realm_id,
        };
        let mut reads = vec![
            (config.storage_keyspace().to_string(), config.storage_key()),
            (auth.storage_keyspace().to_string(), auth.storage_key()),
        ];
        if let Some(group_id) = owner {
            let group = DocumentTarget::GroupAuthorization { group_id };
            reads.push((group.storage_keyspace().to_string(), group.storage_key()));
        }
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn accept_cached(&mut self, values: Vec<Option<Value>>) -> ResolveTurn {
        let ([config_value, auth_value], group_value) = match values.as_slice() {
            [config, auth] => ([config, auth], None),
            [config, auth, group] => ([config, auth], Some(group)),
            _ => {
                return self.unexpected_event(
                    "realm config and authorization",
                    "a batch of another length",
                );
            }
        };
        let Some(config_value) = config_value else {
            return self.fail(ReadPolicyError::RealmConfigMissing);
        };
        let Some(auth_value) = auth_value else {
            return self.fail(ReadPolicyError::Unavailable(
                "realm authorization document missing".to_string(),
            ));
        };
        let config = match RealmConfigDocument::from_bytes(config_value) {
            Ok(config) => config,
            Err(error) => return self.fail(error.into()),
        };
        let auth = match RealmAuthorizationDocument::from_bytes(auth_value) {
            Ok(auth) => auth,
            Err(error) => return self.fail(error.into()),
        };
        let group_auth = match group_value
            .and_then(|value| value.as_ref())
            .map(|value| GroupAuthorizationDocument::from_bytes(value))
            .transpose()
        {
            Ok(group_auth) => group_auth,
            Err(error) => return self.fail(error.into()),
        };
        let Some(authentic) = self.cached.take() else {
            return self.unexpected_event("cached policy", "nothing");
        };
        if let Err(error) =
            verify_policy_authority(&authentic.document, &config, &auth, group_auth.as_ref())
        {
            return self.fail(error.into());
        }
        self.result = Some(Ok((authentic.policy, PolicySource::Cached)));
        self.finish()
    }

    /// Keeps the resolved answer, then caches it. A positive entry is written
    /// only after the publication was authenticated, and it retains that
    /// provenance; only availability outcomes are cached negatively.
    fn after_read(
        &mut self,
        result: Result<(AuthenticPolicy, PolicySource), ReadPolicyError>,
    ) -> ResolveTurn {
        let entry = match &result {
            Ok((authentic, _)) => Some(PolicyCacheEntry::verified(
                &authentic.document,
                self.config.now_ms,
            )),
            Err(ReadPolicyError::NotFound { .. } | ReadPolicyError::Unavailable(_)) => {
                Some(PolicyCacheEntry::unavailable(self.config.now_ms))
            }
            Err(_) => None,
        };
        self.result = Some(result.map(|(authentic, source)| (authentic.policy, source)));
        let Some(entry) = entry else {
            return self.finish();
        };
        match entry.to_bytes() {
            Ok(bytes) => self.emit_scan(bytes),
            Err(error) => {
                warn!(policy_id = %self.config.policy_ref.policy_id, error = %error, "Policy cache entry not storable");
                self.finish()
            }
        }
    }

    fn emit_scan(&mut self, bytes: Vec<u8>) -> ResolveTurn {
        self.pending = Some(PendingEntry {
            key: cache_key(&self.config.policy_ref),
            bytes,
        });
        self.state = ResolveState::Scan;
        Ok(cache_call(IoCall::Page {
            space: POLICY_CACHE_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: None,
            limit: MAX_CACHE_ENTRIES + 1,
        }))
    }

    /// Evicts what the scan found over the cache bound, then stores the pending entry.
    fn scanned(&mut self, rows: Vec<(Key, Value)>) -> ResolveTurn {
        let Some(pending) = self.pending.as_ref() else {
            return self.finish();
        };
        let victims = plan_eviction(&rows, &pending.key, pending.bytes.len(), self.config.now_ms);
        if victims.is_empty() {
            return self.emit_store();
        }
        self.stats.evictions = victims.len() as u32;
        self.state = ResolveState::Evict;
        let deletes = victims
            .into_iter()
            .map(|key| (POLICY_CACHE_KEYSPACE.to_string(), key))
            .collect();
        Ok(cache_call(IoCall::Delete(deletes)))
    }

    fn emit_store(&mut self) -> ResolveTurn {
        let Some(pending) = self.pending.take() else {
            return self.finish();
        };
        self.state = ResolveState::Store;
        let row = (
            POLICY_CACHE_KEYSPACE.to_string(),
            pending.key,
            ByteView::from(pending.bytes),
        );
        Ok(cache_call(IoCall::Write(vec![row])))
    }

    fn finish(&mut self) -> ResolveTurn {
        debug!(
            policy_id = %self.config.policy_ref.policy_id,
            hits = self.stats.hits,
            misses = self.stats.misses,
            evictions = self.stats.evictions,
            resolved = self.result.as_ref().is_some_and(Result::is_ok),
            "Placement policy resolved"
        );
        let stats = self.stats;
        match self.result.take() {
            Some(Ok((policy, source))) => Ok(Next::Done(ResolvedPolicy {
                policy,
                source,
                stats,
            })),
            Some(Err(error)) => Err(error),
            None => Err(ReadPolicyError::NotFinished),
        }
    }

    fn fail(&mut self, error: ReadPolicyError) -> ResolveTurn {
        self.result = Some(Err(error));
        self.finish()
    }

    fn unexpected_event(&mut self, expected: &'static str, got: &'static str) -> ResolveTurn {
        let state = format!("{:?}", self.state);
        self.fail(ReadPolicyError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for ResolvePolicyOperation {
    type Output = ResolvedPolicy;
    type Error = ReadPolicyError;

    fn first(&mut self) -> ResolveTurn {
        let read = (
            POLICY_CACHE_KEYSPACE.to_string(),
            cache_key(&self.config.policy_ref),
        );
        Ok(cache_call(IoCall::Read(vec![read])))
    }

    fn next(&mut self, result: IoResult) -> ResolveTurn {
        match self.state {
            ResolveState::ReadCache => match outside_answer(result).and_then(values::<1>) {
                Ok([value]) => self.cache_read(value),
                Err(error) => {
                    warn!(error = %error, "Policy cache read failed");
                    self.stats.misses = 1;
                    self.start_read()
                }
            },
            ResolveState::Revalidate => self.accept_cached(all_values(result)?),
            ResolveState::Resolve => {
                let read = result.child::<(AuthenticPolicy, PolicySource), ReadPolicyError>()?;
                self.after_read(read)
            }
            ResolveState::Scan => match outside_answer(result).and_then(page) {
                Ok((rows, _)) => self.scanned(rows),
                Err(error) => {
                    warn!(error = %error, "Policy cache scan failed");
                    self.finish()
                }
            },
            ResolveState::Evict => match outside_answer(result).and_then(done) {
                Ok(()) => self.emit_store(),
                Err(error) => {
                    warn!(error = %error, "Policy cache eviction failed");
                    self.finish()
                }
            },
            ResolveState::Store => {
                if let Err(error) = outside_answer(result).and_then(done) {
                    warn!(error = %error, "Policy cache write failed");
                }
                self.finish()
            }
        }
    }

    fn expected_error(error: &Self::Error) -> bool {
        ReadPolicyOperation::expected_error(error)
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use aruna_core::calls::{ChildOutcome, IoError};
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::errors::StorageError;
    use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
    use aruna_core::structs::placement::policy::document::PlacementPolicyDocument;
    use aruna_core::structs::placement::policy::{PlacementPolicy, PlacementSelector};
    use ulid::Ulid;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn realm() -> RealmId {
        RealmId::from_bytes([3u8; 32])
    }

    fn policy(seed: u8, location: &str) -> VerifiedPolicy {
        let policy = PlacementPolicy::new(
            Ulid::from_bytes([seed; 16]),
            "residency".to_string(),
            vec![PlacementSelector {
                node_id: None,
                location: Some(location.to_string()),
                labels: Vec::new(),
                executor_kind: None,
            }],
        )
        .expect("policy is valid");
        VerifiedPolicy::verify(policy).expect("policy verifies")
    }

    fn document(policy: &VerifiedPolicy) -> PlacementPolicyDocument {
        crate::tests::policy::signed_document(realm(), policy, 1)
    }

    fn realm_values() -> (Value, Value) {
        let mut config = RealmConfigDocument::new(realm(), Vec::new(), 2);
        config.seed_default_placement();
        for seed in 1..=4u8 {
            config.ensure_node(node(seed), RealmNodeKind::Server);
        }
        crate::tests::policy::realm_view(&config, crate::tests::policy::admin_user(realm()))
    }

    fn authority() -> IoResult {
        let (config_value, auth_value) = realm_values();
        IoResult::Values(vec![Some(config_value), Some(auth_value)])
    }

    fn read_result(value: Option<Value>) -> IoResult {
        IoResult::Values(vec![value])
    }

    /// The outcome of the inner policy read child.
    fn read_child(result: Result<(AuthenticPolicy, PolicySource), ReadPolicyError>) -> IoResult {
        IoResult::Child(ChildOutcome::new(result))
    }

    fn fetched(policy: &VerifiedPolicy) -> IoResult {
        let authentic = AuthenticPolicy {
            document: document(policy),
            policy: policy.clone(),
        };
        read_child(Ok((authentic, PolicySource::Fetched)))
    }

    fn operation(policy_ref: PlacementPolicyRef) -> ResolvePolicyOperation {
        ResolvePolicyOperation::new(ResolvePolicyConfig {
            realm_id: realm(),
            policy_ref,
            local_node_id: node(9),
            now_ms: 1_000,
        })
    }

    /// Misses the cache, so the next call is the policy read child.
    fn missed(operation: &mut ResolvePolicyOperation) {
        operation.first().unwrap();
        let read = operation.next(read_result(None));
        assert!(matches!(read, Ok(Next::Call(IoCall::Child(_)))));
    }

    /// The storage effect a cache access child asks for.
    fn cache_effect(turn: ResolveTurn) -> Effect {
        let Ok(Next::Call(IoCall::Child(mut child))) = turn else {
            panic!("expected a cache access child");
        };
        child.start().into_iter().next().expect("one cache effect")
    }

    fn iter_result(rows: Vec<(Key, Value)>) -> IoResult {
        IoResult::Page { rows, next: None }
    }

    #[test]
    fn resolves_cold() {
        let policy = policy(1, "eu-west");
        let mut operation = operation(policy.policy_ref());
        missed(&mut operation);
        let scan = cache_effect(operation.next(fetched(&policy)));
        let Effect::Storage(StorageEffect::Iter { key_space, .. }) = scan else {
            panic!("expected a cache scan, got {scan:?}");
        };
        assert_eq!(key_space, POLICY_CACHE_KEYSPACE);

        let store = cache_effect(operation.next(iter_result(Vec::new())));
        let Effect::Storage(StorageEffect::BatchWrite { writes, .. }) = store else {
            panic!("expected a cache write, got {store:?}");
        };
        let [(_, key, value)] = writes.as_slice() else {
            panic!("expected one cache row");
        };
        assert_eq!(*key, cache_key(&policy.policy_ref()));
        assert_eq!(
            PolicyCacheEntry::from_bytes(value).expect("entry decodes"),
            PolicyCacheEntry::verified(&document(&policy), 1_000)
        );

        let Ok(Next::Done(resolved)) = operation.next(IoResult::Done) else {
            panic!("expected the resolved policy");
        };
        assert_eq!(resolved.policy, policy);
        assert_eq!(resolved.source, PolicySource::Fetched);
        assert_eq!(resolved.stats.misses, 1);
    }

    #[test]
    fn cache_failure_reads() {
        // A failed cache read only costs latency: the policy is read instead.
        let policy = policy(1, "eu-west");
        let mut operation = operation(policy.policy_ref());
        operation.first().unwrap();
        let failed: Result<IoResult, IoError> = Err(IoError::Storage(StorageError::Timeout));
        let read = operation.next(IoResult::Child(ChildOutcome::new(failed)));
        assert!(matches!(read, Ok(Next::Call(IoCall::Child(_)))));
    }

    #[test]
    fn revalidates_warm_entry() {
        // Cached publication bytes must still pass current realm authority.
        let policy = policy(1, "eu-west");
        let entry = PolicyCacheEntry::verified(&document(&policy), 10);
        let mut operation = operation(policy.policy_ref());
        operation.first().unwrap();
        let cached = read_result(Some(ByteView::from(
            entry.to_bytes().expect("entry encodes"),
        )));
        assert!(matches!(
            operation.next(cached),
            Ok(Next::Call(IoCall::Read(_)))
        ));
        let Ok(Next::Done(resolved)) = operation.next(authority()) else {
            panic!("expected the cached policy");
        };
        assert_eq!(resolved.source, PolicySource::Cached);
        assert_eq!(resolved.stats.hits, 1);
        assert_eq!(resolved.policy, policy);
    }

    #[test]
    fn refetches_evicted() {
        // Eviction only costs a round trip: the same ref resolves again from its
        // holders and is stored once the full cache has been trimmed.
        let policy = policy(1, "eu-west");
        let mut operation = operation(policy.policy_ref());
        missed(&mut operation);
        operation.next(fetched(&policy)).unwrap();

        let stale = PolicyCacheEntry::verified(&document(&policy), 1)
            .to_bytes()
            .expect("entry encodes");
        let rows: Vec<(Key, Value)> = (0..=MAX_CACHE_ENTRIES)
            .map(|index| {
                let mut key = vec![0u8; 48];
                key[..8].copy_from_slice(&(index as u64).to_be_bytes());
                (ByteView::from(key), ByteView::from(stale.clone()))
            })
            .collect();
        let evict = cache_effect(operation.next(iter_result(rows.clone())));
        let Effect::Storage(StorageEffect::BatchDelete { deletes, .. }) = evict else {
            panic!("expected an eviction, got {evict:?}");
        };
        assert_eq!(deletes.len(), 2);
        assert_eq!(deletes[0].1, rows[0].0);

        let store = cache_effect(operation.next(IoResult::Done));
        assert!(
            matches!(store, Effect::Storage(StorageEffect::BatchWrite { .. })),
            "the refetched policy must still be stored, got {store:?}"
        );
        let Ok(Next::Done(resolved)) = operation.next(IoResult::Done) else {
            panic!("expected the resolved policy");
        };
        assert_eq!(resolved.stats.evictions, 2);
    }

    #[test]
    fn mismatch_skips_cache() {
        // A substituted definition fails closed and must never be remembered.
        let requested = policy(1, "eu-west");
        let mut operation = operation(requested.policy_ref());
        missed(&mut operation);
        assert_eq!(
            operation.next(read_child(Err(ReadPolicyError::DigestMismatch))),
            Err(ReadPolicyError::DigestMismatch)
        );
    }

    #[test]
    fn caches_unavailable() {
        let policy = policy(1, "eu-west");
        let mut operation = operation(policy.policy_ref());
        missed(&mut operation);
        let unavailable = ReadPolicyError::Unavailable("no holder answered".to_string());
        let scan = cache_effect(operation.next(read_child(Err(unavailable))));
        assert!(
            matches!(scan, Effect::Storage(StorageEffect::Iter { .. })),
            "an availability miss is cached as a hint, got {scan:?}"
        );

        let store = cache_effect(operation.next(iter_result(Vec::new())));
        let Effect::Storage(StorageEffect::BatchWrite { writes, .. }) = store else {
            panic!("expected a negative cache write, got {store:?}");
        };
        assert_eq!(
            PolicyCacheEntry::from_bytes(&writes[0].2).expect("entry decodes"),
            PolicyCacheEntry::unavailable(1_000)
        );
    }

    #[test]
    fn negative_skips_fetch() {
        // A live hint suppresses the round trip and still reports availability,
        // never a denial.
        let policy = policy(1, "eu-west");
        let entry = PolicyCacheEntry::unavailable(1_000);
        let mut operation = operation(policy.policy_ref());
        operation.first().unwrap();
        let hint = read_result(Some(ByteView::from(
            entry.to_bytes().expect("entry encodes"),
        )));
        assert!(matches!(
            operation.next(hint),
            Err(ReadPolicyError::Unavailable(_))
        ));
    }
}
