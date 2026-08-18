//! Resolving one policy ref to a verified policy: durable cache first, then the
//! ordinary placement-resolved read, then a bounded cache insert.
//!
//! The cache is a latency device only. A failed cache read, eviction, or write
//! never changes the answer, and a missing policy is reported as unavailable,
//! never as a denial.

use aruna_core::NodeId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::PLACEMENT_POLICY_CACHE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::{PlacementPolicyRef, RealmId, VerifiedPolicy};
use aruna_core::types::{Effects, Key};
use byteview::ByteView;
use smallvec::smallvec;
use tracing::{debug, warn};

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
    reader: Option<ReadPolicyOperation>,
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
    Init,
    ReadCache,
    Resolve,
    Scan,
    Evict,
    Store,
    Finish,
    Error,
}

impl ResolvePolicyOperation {
    pub fn new(config: ResolvePolicyConfig) -> Self {
        Self {
            config,
            reader: None,
            pending: None,
            stats: PolicyCacheStats::default(),
            state: ResolveState::Init,
            result: None,
        }
    }

    fn start_read(&mut self) -> Effects {
        let mut reader = ReadPolicyOperation::new(ReadPolicyConfig {
            realm_id: self.config.realm_id,
            policy_ref: self.config.policy_ref,
            local_node_id: self.config.local_node_id,
        });
        let effects = reader.start();
        self.reader = Some(reader);
        self.state = ResolveState::Resolve;
        effects
    }

    /// Keeps the resolved answer, then caches it. A positive entry is written
    /// only after the publication was authenticated, and it retains that
    /// provenance; only availability outcomes are cached negatively.
    fn after_read(
        &mut self,
        result: Result<(AuthenticPolicy, PolicySource), ReadPolicyError>,
    ) -> Effects {
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

    fn emit_scan(&mut self, bytes: Vec<u8>) -> Effects {
        self.pending = Some(PendingEntry {
            key: cache_key(&self.config.policy_ref),
            bytes,
        });
        self.state = ResolveState::Scan;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: PLACEMENT_POLICY_CACHE_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: MAX_CACHE_ENTRIES + 1,
            txn_id: None,
        })]
    }

    fn emit_store(&mut self) -> Effects {
        let Some(pending) = self.pending.take() else {
            return self.finish();
        };
        self.state = ResolveState::Store;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: PLACEMENT_POLICY_CACHE_KEYSPACE.to_string(),
            key: pending.key,
            value: ByteView::from(pending.bytes),
            txn_id: None,
        })]
    }

    fn finish(&mut self) -> Effects {
        self.state = match &self.result {
            Some(Ok(_)) => ResolveState::Finish,
            _ => ResolveState::Error,
        };
        debug!(
            policy_id = %self.config.policy_ref.policy_id,
            hits = self.stats.hits,
            misses = self.stats.misses,
            evictions = self.stats.evictions,
            resolved = self.result.as_ref().is_some_and(Result::is_ok),
            "Placement policy resolved"
        );
        smallvec![]
    }

    fn fail(&mut self, error: ReadPolicyError) -> Effects {
        self.result = Some(Err(error));
        self.finish()
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
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

    fn start(&mut self) -> Effects {
        self.state = ResolveState::ReadCache;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: PLACEMENT_POLICY_CACHE_KEYSPACE.to_string(),
            key: cache_key(&self.config.policy_ref),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ResolveState::ReadCache => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    match lookup(
                        value.as_ref(),
                        self.config.realm_id,
                        &self.config.policy_ref,
                        self.config.now_ms,
                    ) {
                        CacheLookup::Hit(policy) => {
                            self.stats.hits = 1;
                            self.result = Some(Ok((*policy, PolicySource::Cached)));
                            self.finish()
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
                Event::Storage(StorageEvent::Error { error }) => {
                    warn!(error = %error, "Policy cache read failed");
                    self.stats.misses = 1;
                    self.start_read()
                }
                other => self.unexpected_event("policy cache read", format!("{other:?}")),
            },
            ResolveState::Resolve => {
                let Some(reader) = self.reader.as_mut() else {
                    return self.unexpected_event("active policy read", format!("{event:?}"));
                };
                let effects = reader.step(event);
                if !reader.is_complete() {
                    return effects;
                }
                let Some(reader) = self.reader.take() else {
                    return self.unexpected_event("active policy read", String::new());
                };
                self.after_read(reader.finalize())
            }
            ResolveState::Scan => match event {
                Event::Storage(StorageEvent::IterResult { values, .. }) => {
                    let Some(pending) = self.pending.as_ref() else {
                        return self.finish();
                    };
                    let victims = plan_eviction(
                        &values,
                        &pending.key,
                        pending.bytes.len(),
                        self.config.now_ms,
                    );
                    if victims.is_empty() {
                        return self.emit_store();
                    }
                    self.stats.evictions = victims.len() as u32;
                    self.state = ResolveState::Evict;
                    smallvec![Effect::Storage(StorageEffect::BatchDelete {
                        deletes: victims
                            .into_iter()
                            .map(|key| (PLACEMENT_POLICY_CACHE_KEYSPACE.to_string(), key))
                            .collect(),
                        txn_id: None,
                    })]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    warn!(error = %error, "Policy cache scan failed");
                    self.finish()
                }
                other => self.unexpected_event("policy cache scan", format!("{other:?}")),
            },
            ResolveState::Evict => match event {
                Event::Storage(StorageEvent::BatchDeleteResult { .. }) => self.emit_store(),
                Event::Storage(StorageEvent::Error { error }) => {
                    warn!(error = %error, "Policy cache eviction failed");
                    self.finish()
                }
                other => self.unexpected_event("policy cache eviction", format!("{other:?}")),
            },
            ResolveState::Store => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => self.finish(),
                Event::Storage(StorageEvent::Error { error }) => {
                    warn!(error = %error, "Policy cache write failed");
                    self.finish()
                }
                other => self.unexpected_event("policy cache write", format!("{other:?}")),
            },
            ResolveState::Init | ResolveState::Finish | ResolveState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ResolveState::Finish | ResolveState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        let stats = self.stats;
        match self.result {
            Some(Ok((policy, source))) => Ok(ResolvedPolicy {
                policy,
                source,
                stats,
            }),
            Some(Err(error)) => Err(error),
            None => Err(ReadPolicyError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        match self.reader.as_mut() {
            Some(reader) => reader.abort(),
            None => smallvec![],
        }
    }

    fn expected_error(error: &Self::Error) -> bool {
        ReadPolicyOperation::expected_error(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::effects::NetEffect;
    use aruna_core::events::{NetEvent, PolicyFetchEvent};
    use aruna_core::structs::{
        PlacementPolicy, PlacementPolicyDocument, PlacementSelector, RealmConfigDocument,
        RealmNodeKind,
    };
    use aruna_core::types::Value;
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
        super::super::tests::signed_document(realm(), policy, 1)
    }

    fn encoded(policy: &VerifiedPolicy) -> Value {
        ByteView::from(document(policy).to_bytes().expect("document encodes"))
    }

    /// The realm view and policy row the inner read starts with.
    fn opened(policy_row: Option<Value>) -> Event {
        let mut config = RealmConfigDocument::new(realm(), Vec::new(), 2);
        config.seed_default_placement();
        for seed in 1..=4u8 {
            config.ensure_node(node(seed), RealmNodeKind::Server);
        }
        let (config_value, auth_value) =
            super::super::tests::realm_view(&config, super::super::tests::admin_user(realm()));
        let key = ByteView::from(Vec::new());
        Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (key.clone(), policy_row),
                (key.clone(), Some(config_value)),
                (key, Some(auth_value)),
            ],
        })
    }

    fn read_result(value: Option<Value>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: ByteView::from(Vec::new()),
            value,
        })
    }

    fn operation(policy_ref: PlacementPolicyRef) -> ResolvePolicyOperation {
        ResolvePolicyOperation::new(ResolvePolicyConfig {
            realm_id: realm(),
            policy_ref,
            local_node_id: node(9),
            now_ms: 1_000,
        })
    }

    /// Drives a cold resolve up to the holder answer and returns the effects the
    /// cache insert produced.
    fn fetch_cold(operation: &mut ResolvePolicyOperation, policy: &VerifiedPolicy) -> Effects {
        operation.start();
        operation.step(read_result(None));
        let effects = operation.step(opened(None));
        let Some(Effect::Net(NetEffect::PolicyFetch(fetch))) = effects.first() else {
            panic!("a cache miss must resolve holders and fetch, got {effects:?}");
        };
        let holder = fetch.holders.as_slice()[0];
        operation.step(Event::Net(NetEvent::PolicyFetch(
            PolicyFetchEvent::Fetched {
                publisher: holder,
                document: Box::new(document(policy)),
            },
        )))
    }

    fn iter_result(values: Vec<(Key, Value)>) -> Event {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after: None,
        })
    }

    #[test]
    fn resolves_cold() {
        let policy = policy(1, "eu-west");
        let mut operation = operation(policy.policy_ref());
        let effects = fetch_cold(&mut operation, &policy);

        let Some(Effect::Storage(StorageEffect::Iter { key_space, .. })) = effects.first() else {
            panic!("expected a cache scan, got {effects:?}");
        };
        assert_eq!(key_space, PLACEMENT_POLICY_CACHE_KEYSPACE);

        let effects = operation.step(iter_result(Vec::new()));
        let Some(Effect::Storage(StorageEffect::Write { key, value, .. })) = effects.first() else {
            panic!("expected a cache write, got {effects:?}");
        };
        assert_eq!(*key, cache_key(&policy.policy_ref()));
        assert_eq!(
            PolicyCacheEntry::from_bytes(value).expect("entry decodes"),
            PolicyCacheEntry::verified(&document(&policy), 1_000)
        );

        operation.step(Event::Storage(StorageEvent::WriteResult {
            key: key.clone(),
        }));
        let resolved = operation.finalize().expect("policy resolves");
        assert_eq!(resolved.policy, policy);
        assert_eq!(resolved.source, PolicySource::Fetched);
        assert_eq!(resolved.stats.misses, 1);
    }

    #[test]
    fn serves_warm_entry() {
        // A durable positive entry must answer without any further effect, so a
        // warm resolve never touches the network.
        let policy = policy(1, "eu-west");
        let entry = PolicyCacheEntry::verified(&document(&policy), 10);
        let mut operation = operation(policy.policy_ref());
        operation.start();
        let effects = operation.step(read_result(Some(ByteView::from(
            entry.to_bytes().expect("entry encodes"),
        ))));

        assert!(effects.is_empty(), "a warm hit must emit no effect");
        assert!(operation.is_complete());
        let resolved = operation.finalize().expect("policy resolves");
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
        fetch_cold(&mut operation, &policy);

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
        let effects = operation.step(iter_result(rows.clone()));
        let Some(Effect::Storage(StorageEffect::BatchDelete { deletes, .. })) = effects.first()
        else {
            panic!("expected an eviction, got {effects:?}");
        };
        assert_eq!(deletes.len(), 2);
        assert_eq!(deletes[0].1, rows[0].0);

        let effects = operation.step(Event::Storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        }));
        assert!(
            matches!(
                effects.first(),
                Some(Effect::Storage(StorageEffect::Write { .. }))
            ),
            "the refetched policy must still be stored, got {effects:?}"
        );
        operation.step(Event::Storage(StorageEvent::WriteResult {
            key: cache_key(&policy.policy_ref()),
        }));
        assert_eq!(
            operation
                .finalize()
                .expect("policy resolves")
                .stats
                .evictions,
            2
        );
    }

    #[test]
    fn mismatch_skips_cache() {
        // A substituted definition fails closed and must never be remembered.
        let requested = policy(1, "eu-west");
        let mut operation = operation(requested.policy_ref());
        operation.start();
        let effects = operation.step(read_result(None));
        assert!(!effects.is_empty(), "a miss must start a read");
        let effects = operation.step(opened(Some(encoded(&policy(1, "us-east")))));

        assert!(
            effects.is_empty(),
            "a mismatch must not write a cache entry"
        );
        assert_eq!(operation.finalize(), Err(ReadPolicyError::DigestMismatch));
    }

    #[test]
    fn caches_unavailable() {
        let policy = policy(1, "eu-west");
        let mut operation = operation(policy.policy_ref());
        operation.start();
        operation.step(read_result(None));
        operation.step(opened(None));
        let effects = operation.step(Event::Net(NetEvent::PolicyFetch(
            PolicyFetchEvent::Unavailable("no holder answered".to_string()),
        )));
        assert!(
            matches!(
                effects.first(),
                Some(Effect::Storage(StorageEffect::Iter { .. }))
            ),
            "an availability miss is cached as a hint, got {effects:?}"
        );

        let effects = operation.step(iter_result(Vec::new()));
        let Some(Effect::Storage(StorageEffect::Write { value, .. })) = effects.first() else {
            panic!("expected a negative cache write, got {effects:?}");
        };
        assert_eq!(
            PolicyCacheEntry::from_bytes(value).expect("entry decodes"),
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
        operation.start();
        let effects = operation.step(read_result(Some(ByteView::from(
            entry.to_bytes().expect("entry encodes"),
        ))));

        assert!(effects.is_empty(), "a live hint must emit no effect");
        assert!(matches!(
            operation.finalize(),
            Err(ReadPolicyError::Unavailable(_))
        ));
    }
}
