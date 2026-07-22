use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use aruna_core::id::{DhtKeyId, NodeId};
use aruna_core::structs::RealmId;
use aruna_core::util::unix_timestamp_secs;
use aruna_core::{IdEnvironment, SystemEnvironment};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::constants::{
    DHT_ACTIVE_PREFIX, DHT_FLOOR_PREFIX, MAX_CLOCK_SKEW_SECS, MAX_ENTRIES_PER_KEY,
    MAX_STORED_VALUE_SIZE, MAX_TTL_SECS, MAX_VALUE_SIZE,
};
use super::rpc::verify_record;

pub(super) const CLEANUP_PAGE_SIZE: usize = 256;
const CLOCK_SAMPLE_WINDOW_MS: u64 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub(crate) enum DhtClockError {
    #[error("DHT wall clock diverged from monotonic time by {drift_ms} ms")]
    Diverged { drift_ms: u64 },
}

#[derive(Clone, Copy)]
struct ClockAnchor {
    wall_ms: u64,
    monotonic_ms: u64,
}

struct AnchorState {
    anchor: ClockAnchor,
    // Candidate anchor captured from the previous diverged sample. A re-anchor only
    // commits once a second sample agrees with it, so a lone spurious wall reading
    // fails one op but is discarded rather than adopted as the new anchor.
    pending: Option<ClockAnchor>,
}

#[derive(Clone)]
pub(crate) struct DhtClock<E = SystemEnvironment> {
    env: E,
    anchor: Arc<Mutex<AnchorState>>,
    last_ms: Arc<AtomicU64>,
}

impl DhtClock<SystemEnvironment> {
    pub(crate) fn new() -> Self {
        Self::with_env(SystemEnvironment::new())
    }

    pub(crate) fn from_secs(now_secs: u64) -> Self {
        let env = SystemEnvironment::new();
        let anchor = ClockAnchor {
            wall_ms: now_secs.saturating_mul(1_000),
            monotonic_ms: env.monotonic_ms(),
        };
        Self::from_anchor(env, anchor)
    }
}

impl<E: IdEnvironment> DhtClock<E> {
    fn with_env(env: E) -> Self {
        let anchor = Self::sample_time(&env);
        Self::from_anchor(env, anchor)
    }

    fn from_anchor(env: E, anchor: ClockAnchor) -> Self {
        Self {
            env,
            anchor: Arc::new(Mutex::new(AnchorState {
                anchor,
                pending: None,
            })),
            last_ms: Arc::new(AtomicU64::new(anchor.wall_ms)),
        }
    }

    fn sample_time(env: &E) -> ClockAnchor {
        loop {
            let monotonic_before = env.monotonic_ms();
            let wall_ms = env.now_ms();
            let monotonic_after = env.monotonic_ms();
            let Some(elapsed) = monotonic_after.checked_sub(monotonic_before) else {
                continue;
            };
            if elapsed <= CLOCK_SAMPLE_WINDOW_MS {
                return ClockAnchor {
                    wall_ms,
                    monotonic_ms: monotonic_before.saturating_add(elapsed / 2),
                };
            }
        }
    }

    pub(crate) fn current_secs(&self) -> u64 {
        self.last_ms.load(Ordering::Acquire) / 1_000
    }

    pub(crate) fn now_secs(&self) -> Result<u64, DhtClockError> {
        let sample = Self::sample_time(&self.env);
        let threshold_ms = MAX_CLOCK_SKEW_SECS.saturating_mul(1_000);
        {
            let mut state = self.anchor.lock();
            let anchor_drift = clock_drift_ms(&state.anchor, &sample);
            if anchor_drift.is_some_and(|drift| drift <= threshold_ms) {
                state.pending = None;
            } else if state
                .pending
                .and_then(|pending| clock_drift_ms(&pending, &sample))
                .is_some_and(|drift| drift <= threshold_ms)
            {
                // Two consecutive samples agree on the new offset: the wall clock has
                // settled, so re-anchor and resume service.
                state.anchor = sample;
                state.pending = None;
            } else {
                state.pending = Some(sample);
                return Err(DhtClockError::Diverged {
                    drift_ms: anchor_drift.unwrap_or(u64::MAX),
                });
            }
        }
        // The emitted seconds ratchet across re-anchoring: a backward wall step never
        // rolls the returned value back.
        let ratcheted = self
            .last_ms
            .fetch_max(sample.wall_ms, Ordering::AcqRel)
            .max(sample.wall_ms);
        Ok(ratcheted / 1_000)
    }
}

fn clock_drift_ms(anchor: &ClockAnchor, sample: &ClockAnchor) -> Option<u64> {
    let elapsed_ms = sample.monotonic_ms.checked_sub(anchor.monotonic_ms)?;
    let expected_ms = anchor.wall_ms.saturating_add(elapsed_ms);
    Some(sample.wall_ms.abs_diff(expected_ms))
}

pub fn now_unix_secs() -> u64 {
    unix_timestamp_secs()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredEntry {
    pub publisher: NodeId,
    pub realm_id: RealmId,
    pub value: Vec<u8>,
    pub expires_at: u64,
    pub revision: u64,
    pub signature: iroh::Signature,
    pub retain_until: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeadlineIndex {
    Active,
    Floor,
}

impl DeadlineIndex {
    pub(super) fn prefix(self) -> &'static [u8] {
        match self {
            Self::Active => DHT_ACTIVE_PREFIX,
            Self::Floor => DHT_FLOOR_PREFIX,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeadlineEntry {
    pub deadline: u64,
    pub key: DhtKeyId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum MergeError {
    #[error("record version is stale or conflicting")]
    Stale,
    #[error("record set exceeds the per-key capacity")]
    Capacity,
    #[error("record set cannot be encoded")]
    Encoding,
    #[error("stored record set is invalid")]
    Invalid,
}

pub fn decode_entries(bytes: &[u8]) -> Result<Vec<StoredEntry>, postcard::Error> {
    super::rpc::decode_exact(bytes)
}

pub fn encode_entries(entries: &[StoredEntry]) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(entries)
}

pub fn live_entries(entries: Vec<StoredEntry>, now: u64) -> Vec<StoredEntry> {
    entries
        .into_iter()
        .filter(|entry| entry_is_fresh(entry.expires_at, now))
        .collect()
}

pub fn retained_entries(entries: Vec<StoredEntry>, now: u64) -> Vec<StoredEntry> {
    entries
        .into_iter()
        .filter(|entry| entry.retain_until > now)
        .collect()
}

pub(super) fn retention_deadline(expires_at: u64, now: u64) -> u64 {
    expires_at
        .min(now)
        .saturating_add(MAX_TTL_SECS)
        .saturating_add(MAX_CLOCK_SKEW_SECS)
}

pub(super) fn active_deadline(key: DhtKeyId, entries: &[StoredEntry]) -> Option<DeadlineEntry> {
    entries
        .iter()
        .map(|entry| entry.expires_at)
        .max()
        .map(|deadline| DeadlineEntry { deadline, key })
}

pub(super) fn floor_deadline(key: DhtKeyId, entries: &[StoredEntry]) -> Option<DeadlineEntry> {
    entries
        .iter()
        .map(|entry| entry.retain_until)
        .max()
        .map(|deadline| DeadlineEntry { deadline, key })
}

pub(super) fn row_deadline(
    key: DhtKeyId,
    entries: &[StoredEntry],
    now: u64,
) -> Option<(DeadlineIndex, DeadlineEntry)> {
    if entries
        .iter()
        .any(|entry| entry_is_fresh(entry.expires_at, now))
    {
        active_deadline(key, entries).map(|entry| (DeadlineIndex::Active, entry))
    } else {
        floor_deadline(key, entries).map(|entry| (DeadlineIndex::Floor, entry))
    }
}

pub(super) fn deadline_key(index: DeadlineIndex, entry: DeadlineEntry) -> Vec<u8> {
    let prefix = index.prefix();
    let mut key = Vec::with_capacity(prefix.len() + 8 + entry.key.as_bytes().len());
    key.extend_from_slice(prefix);
    key.extend_from_slice(&entry.deadline.to_be_bytes());
    key.extend_from_slice(entry.key.as_bytes());
    key
}

pub(super) fn parse_deadline(index: DeadlineIndex, key: &[u8]) -> Option<DeadlineEntry> {
    let suffix = key.strip_prefix(index.prefix())?;
    let (deadline, raw_key) = suffix.split_at_checked(8)?;
    let raw_key: [u8; 32] = raw_key.try_into().ok()?;
    Some(DeadlineEntry {
        deadline: u64::from_be_bytes(deadline.try_into().ok()?),
        key: DhtKeyId::from_bytes(raw_key),
    })
}

pub fn merge_entry(
    key: &DhtKeyId,
    entries: Vec<StoredEntry>,
    mut new_entry: StoredEntry,
    now: u64,
) -> Result<Vec<StoredEntry>, MergeError> {
    validate_entries(key, &entries, now)?;
    if new_entry.expires_at
        > now
            .saturating_add(MAX_TTL_SECS)
            .saturating_add(MAX_CLOCK_SKEW_SECS)
    {
        return Err(MergeError::Invalid);
    }
    new_entry.retain_until = retention_deadline(new_entry.expires_at, now);
    if new_entry.retain_until <= now {
        return Err(MergeError::Stale);
    }
    validate_entries(key, std::slice::from_ref(&new_entry), now)?;

    let mut filtered = retained_entries(entries, now);

    if let Some(position) = filtered.iter().position(|entry| {
        entry.publisher == new_entry.publisher && entry.realm_id == new_entry.realm_id
    }) {
        let existing = &filtered[position];
        match new_entry.revision.cmp(&existing.revision) {
            std::cmp::Ordering::Less => return Err(MergeError::Stale),
            std::cmp::Ordering::Equal if !same_record(&new_entry, existing) => {
                return Err(MergeError::Stale);
            }
            std::cmp::Ordering::Equal => {}
            std::cmp::Ordering::Greater => filtered[position] = new_entry,
        }
    } else {
        if filtered.len() >= MAX_ENTRIES_PER_KEY {
            return Err(MergeError::Capacity);
        }
        filtered.push(new_entry);
    }

    filtered.sort_unstable_by(|left, right| {
        left.publisher
            .as_bytes()
            .cmp(right.publisher.as_bytes())
            .then_with(|| left.realm_id.as_bytes().cmp(right.realm_id.as_bytes()))
    });

    let encoded = encode_entries(&filtered).map_err(|_| MergeError::Encoding)?;
    if encoded.len() > MAX_STORED_VALUE_SIZE {
        return Err(MergeError::Capacity);
    }

    Ok(filtered)
}

pub fn validate_entries(
    key: &DhtKeyId,
    entries: &[StoredEntry],
    now: u64,
) -> Result<(), MergeError> {
    if entries.len() > MAX_ENTRIES_PER_KEY {
        return Err(MergeError::Capacity);
    }
    let encoded = encode_entries(entries).map_err(|_| MergeError::Encoding)?;
    if encoded.len() > MAX_STORED_VALUE_SIZE {
        return Err(MergeError::Capacity);
    }

    let latest_allowed = now
        .saturating_add(MAX_TTL_SECS)
        .saturating_add(MAX_CLOCK_SKEW_SECS)
        .saturating_add(MAX_CLOCK_SKEW_SECS);
    let mut identities = HashSet::with_capacity(entries.len());
    for entry in entries {
        if entry.revision == 0
            || entry.value.len() > MAX_VALUE_SIZE
            || entry.expires_at > latest_allowed
            || entry.retain_until < entry.expires_at
            || entry.retain_until.saturating_sub(entry.expires_at)
                > MAX_TTL_SECS.saturating_add(MAX_CLOCK_SKEW_SECS)
            || !identities.insert((entry.publisher, entry.realm_id))
            || !verify_record(
                key,
                &entry.publisher,
                &entry.realm_id,
                &entry.value,
                entry.expires_at,
                entry.revision,
                &entry.signature,
            )
        {
            return Err(MergeError::Invalid);
        }
    }
    Ok(())
}

fn same_record(left: &StoredEntry, right: &StoredEntry) -> bool {
    left.publisher == right.publisher
        && left.realm_id == right.realm_id
        && left.value == right.value
        && left.expires_at == right.expires_at
        && left.revision == right.revision
        && left.signature == right.signature
}

pub fn entry_is_fresh(expires_at: u64, now: u64) -> bool {
    expires_at > now
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;

    #[derive(Clone)]
    struct FakeEnv {
        wall_ms: Cell<u64>,
        monotonic_ms: Cell<u64>,
        suspend_ms: Cell<u64>,
    }

    impl FakeEnv {
        fn new(wall_ms: u64, monotonic_ms: u64) -> Self {
            Self {
                wall_ms: Cell::new(wall_ms),
                monotonic_ms: Cell::new(monotonic_ms),
                suspend_ms: Cell::new(0),
            }
        }

        fn set_time(&self, wall_ms: u64, monotonic_ms: u64) {
            self.wall_ms.set(wall_ms);
            self.monotonic_ms.set(monotonic_ms);
        }

        fn suspend_on_sample(&self, suspend_ms: u64) {
            self.suspend_ms.set(suspend_ms);
        }
    }

    impl IdEnvironment for FakeEnv {
        fn now_ms(&self) -> u64 {
            let suspend_ms = self.suspend_ms.replace(0);
            let wall_ms = self.wall_ms.get();
            self.wall_ms.set(wall_ms.saturating_add(suspend_ms));
            self.monotonic_ms
                .set(self.monotonic_ms.get().saturating_add(suspend_ms));
            wall_ms
        }

        fn monotonic_ms(&self) -> u64 {
            self.monotonic_ms.get()
        }

        fn random_nonce(&self) -> u64 {
            0
        }
    }

    fn make_entry(seed: u16, key: DhtKeyId, revision: u64, expires_at: u64) -> StoredEntry {
        let mut secret = [0u8; 32];
        secret[..2].copy_from_slice(&seed.to_le_bytes());
        let secret = iroh::SecretKey::from_bytes(&secret);
        let publisher = secret.public();
        let mut realm = [0u8; 32];
        realm[..2].copy_from_slice(&seed.to_le_bytes());
        let realm_id = RealmId::from_bytes(realm);
        let value = seed.to_le_bytes().to_vec();
        let signed = super::super::rpc::signed_record_bytes(
            &key, &publisher, &realm_id, &value, expires_at, revision,
        );
        StoredEntry {
            publisher,
            realm_id,
            value,
            expires_at,
            revision,
            signature: secret.sign(&signed),
            retain_until: expires_at,
        }
    }

    #[test]
    fn clock_retries_suspend() {
        let env = FakeEnv::new(100_000, 10_000);
        env.suspend_on_sample(1_000);
        let clock = DhtClock::with_env(env);

        assert_eq!(clock.current_secs(), 101);
    }

    #[test]
    fn clock_follows_corrections() {
        let env = FakeEnv::new(100_000, 10_000);
        let clock = DhtClock::with_env(env);
        let peer = clock.clone();
        clock.env.set_time(161_000, 11_000);
        assert_eq!(clock.now_secs(), Ok(161));

        peer.env.set_time(102_000, 12_000);
        assert_eq!(peer.now_secs(), Ok(161));
        clock.env.set_time(162_000, 72_000);
        assert_eq!(clock.now_secs(), Ok(162));
    }

    #[test]
    fn clock_rejects_jumps() {
        let max_skew_ms = MAX_CLOCK_SKEW_SECS * 1_000;
        let env = FakeEnv::new(100_000, 10_000);
        let clock = DhtClock::with_env(env);
        clock.env.set_time(101_000 + max_skew_ms, 11_000);
        assert_eq!(clock.now_secs(), Ok(401));
        clock.env.set_time(102_000 + max_skew_ms + 1, 12_000);
        assert_eq!(
            clock.now_secs(),
            Err(DhtClockError::Diverged {
                drift_ms: max_skew_ms + 1
            })
        );

        let env = FakeEnv::new(100_000 + max_skew_ms + 1, 10_000);
        let clock = DhtClock::with_env(env);
        clock.env.set_time(101_000, 11_000);
        assert_eq!(
            clock.now_secs(),
            Err(DhtClockError::Diverged {
                drift_ms: max_skew_ms + 1
            })
        );
    }

    #[test]
    fn clock_recovers_divergence() {
        // A divergence fails one sampling op, then a second agreeing sample re-anchors
        // so service resumes; a later backward wall step never rolls the value back.
        let max_skew_ms = MAX_CLOCK_SKEW_SECS * 1_000;
        let env = FakeEnv::new(100_000, 10_000);
        let clock = DhtClock::with_env(env);

        clock.env.set_time(101_000 + max_skew_ms + 1, 11_000);
        assert!(matches!(
            clock.now_secs(),
            Err(DhtClockError::Diverged { .. })
        ));
        assert_eq!(clock.current_secs(), 100);

        clock.env.set_time(102_000 + max_skew_ms + 1, 12_000);
        assert_eq!(clock.now_secs(), Ok(402));

        clock.env.set_time(403_001, 13_000);
        assert_eq!(clock.now_secs(), Ok(403));

        clock.env.set_time(402_501, 13_500);
        assert_eq!(clock.now_secs(), Ok(403));
    }

    #[test]
    fn clock_debounces_glitch() {
        // A single diverging sample fails one op but must not re-anchor; the next
        // healthy sample resumes on the original anchor.
        let max_skew_ms = MAX_CLOCK_SKEW_SECS * 1_000;
        let env = FakeEnv::new(100_000, 10_000);
        let clock = DhtClock::with_env(env);

        clock.env.set_time(101_000 + max_skew_ms + 1, 11_000);
        assert!(clock.now_secs().is_err());

        clock.env.set_time(102_000, 12_000);
        assert_eq!(clock.now_secs(), Ok(102));
    }

    #[test]
    fn corrupt_decode_fails() {
        assert!(decode_entries(b"not-postcard").is_err());
    }

    #[test]
    fn stale_replay_rejected() {
        let key = DhtKeyId::from_data(b"stale-replay");
        let current = make_entry(1, key, 2, 150);
        let stale = make_entry(1, key, 1, 200);

        assert_eq!(
            merge_entry(&key, vec![current], stale, 100),
            Err(MergeError::Stale)
        );
    }

    #[test]
    fn expired_highwater_blocks() {
        let key = DhtKeyId::from_data(b"expired-highwater");
        let older = make_entry(1, key, 1, 1_000);
        let newer = make_entry(1, key, 2, 500);

        let entries = merge_entry(&key, Vec::new(), older.clone(), 100).unwrap();
        let entries = merge_entry(&key, entries, newer, 110).unwrap();

        assert_eq!(
            merge_entry(&key, entries, older, 801),
            Err(MergeError::Stale)
        );
    }

    #[test]
    fn expired_floor_merges() {
        let key = DhtKeyId::from_data(b"expired-floor-merge");
        let floor = make_entry(1, key, 2, 90);
        let entries = merge_entry(&key, Vec::new(), floor, 100).expect("merge retained floor");
        assert_eq!(entries[0].retain_until, retention_deadline(90, 100));

        let replay = make_entry(1, key, 1, 200);
        assert_eq!(
            merge_entry(&key, entries, replay, 100),
            Err(MergeError::Stale)
        );
    }

    #[test]
    fn floor_window_expires() {
        let key = DhtKeyId::from_data(b"floor-window-expiry");
        let expires_at: u64 = 100;
        let now = expires_at
            .saturating_add(MAX_TTL_SECS)
            .saturating_add(MAX_CLOCK_SKEW_SECS);
        let floor = make_entry(1, key, 2, expires_at);

        assert_eq!(
            merge_entry(&key, Vec::new(), floor, now),
            Err(MergeError::Stale)
        );
    }

    #[test]
    fn clock_rollback_valid() {
        let key = DhtKeyId::from_data(b"clock-rollback");
        let entry = make_entry(1, key, 1, 200);
        let entries = merge_entry(&key, Vec::new(), entry, 100).unwrap();

        assert_eq!(validate_entries(&key, &entries, 99), Ok(()));
    }

    #[test]
    fn future_entry_rejected() {
        let key = DhtKeyId::from_data(b"future-entry");
        let expires_at = 100 + MAX_TTL_SECS + MAX_CLOCK_SKEW_SECS + 1;
        let entry = make_entry(1, key, 1, expires_at);

        assert_eq!(
            merge_entry(&key, Vec::new(), entry, 100),
            Err(MergeError::Invalid)
        );
    }

    #[test]
    fn duplicate_is_idempotent() {
        let key = DhtKeyId::from_data(b"duplicate-record");
        let entry = make_entry(1, key, 7, 200);

        assert_eq!(
            merge_entry(&key, vec![entry.clone()], entry.clone(), 100),
            Ok(vec![entry])
        );
    }

    #[test]
    fn record_limit_enforced() {
        let key = DhtKeyId::from_data(b"record-limit");
        let entries = (0..MAX_ENTRIES_PER_KEY)
            .map(|seed| make_entry(seed as u16, key, 1, 200))
            .collect::<Vec<_>>();

        assert_eq!(
            merge_entry(&key, entries, make_entry(u16::MAX, key, 1, 200), 100),
            Err(MergeError::Capacity)
        );
    }

    #[test]
    fn deadline_orders_keys() {
        let key = DhtKeyId::from_data(b"deadline-order");
        let early = deadline_key(DeadlineIndex::Active, DeadlineEntry { deadline: 9, key });
        let late = deadline_key(DeadlineIndex::Active, DeadlineEntry { deadline: 10, key });

        assert!(early < late);
        assert_eq!(
            parse_deadline(DeadlineIndex::Active, &late),
            Some(DeadlineEntry { deadline: 10, key })
        );
        assert_eq!(parse_deadline(DeadlineIndex::Floor, &late), None);
    }

    #[test]
    fn deadline_tracks_rows() {
        let key = DhtKeyId::from_data(b"deadline-row");
        let mut first = make_entry(1, key, 1, 150);
        first.retain_until = 300;
        let mut second = make_entry(2, key, 1, 200);
        second.retain_until = 350;
        let entries = vec![first, second];

        assert_eq!(
            row_deadline(key, &entries, 100),
            Some((DeadlineIndex::Active, DeadlineEntry { deadline: 200, key }))
        );
        assert_eq!(
            row_deadline(key, &entries, 200),
            Some((DeadlineIndex::Floor, DeadlineEntry { deadline: 350, key }))
        );
    }
}
