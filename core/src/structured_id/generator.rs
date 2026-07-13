use std::time::{Instant, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use ulid::Ulid;

use super::layout;
use super::{BucketId, PlacementHandle, StructuredId};

/// Realm skew bound default of five minutes (REQ-META-ID-TIME-001).
pub const DEFAULT_MAX_ID_CLOCK_SKEW_MS: u64 = 300_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ClockHealthError {
    #[error("wall clock jumped forward {jump_ms} ms, beyond max_id_clock_skew_ms {max_skew_ms}")]
    ForwardJump { jump_ms: u64, max_skew_ms: u64 },
}

/// Injectable time and entropy source. The production impl reads the system
/// clock and the OS CSPRNG; tests supply deterministic values.
pub trait IdEnvironment {
    /// Validated wall-clock time as Unix milliseconds.
    fn now_ms(&self) -> u64;
    /// Milliseconds from a monotonic clock with an arbitrary but fixed origin.
    fn monotonic_ms(&self) -> u64;
    /// A fresh 48-bit nonce drawn from a cryptographically secure source.
    fn random_nonce(&self) -> u64;
}

#[derive(Debug, Clone, Copy)]
pub struct SystemEnvironment {
    origin: Instant,
}

impl SystemEnvironment {
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl Default for SystemEnvironment {
    fn default() -> Self {
        Self::new()
    }
}

impl IdEnvironment for SystemEnvironment {
    fn now_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis() as u64)
            .unwrap_or(0)
    }

    fn monotonic_ms(&self) -> u64 {
        self.origin.elapsed().as_millis() as u64
    }

    fn random_nonce(&self) -> u64 {
        (Ulid::r#gen().random() as u64) & layout::NONCE_MASK
    }
}

#[derive(Clone, Copy)]
struct LastMint {
    timestamp_ms: u64,
    handle: u32,
    bucket: u16,
    nonce: u64,
}

/// Paired wall/monotonic reading from the most recent mint attempt. The wall
/// clock is expected to advance in step with the monotonic clock; divergence
/// beyond the skew bound is a forward jump.
#[derive(Clone, Copy)]
struct ClockAnchor {
    wall_ms: u64,
    monotonic_ms: u64,
}

/// Structured-ID generator: a monotonic ULID timestamp with a per
/// `(timestamp_ms, handle, bucket)` monotonic nonce and a forward-clock-jump
/// guard (REQ-META-ID-TIME-001, REQ-META-ID-NONCE-001). It never emits a
/// generic ULID, so the structured fields are always preserved.
pub struct StructuredIdGenerator<E: IdEnvironment = SystemEnvironment> {
    env: E,
    max_skew_ms: u64,
    anchor: Option<ClockAnchor>,
    last: Option<LastMint>,
}

impl StructuredIdGenerator<SystemEnvironment> {
    pub fn new() -> Self {
        Self::with_environment(SystemEnvironment::new(), DEFAULT_MAX_ID_CLOCK_SKEW_MS)
    }
}

impl Default for StructuredIdGenerator<SystemEnvironment> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E: IdEnvironment> StructuredIdGenerator<E> {
    pub fn with_environment(env: E, max_skew_ms: u64) -> Self {
        Self {
            env,
            max_skew_ms,
            anchor: None,
            last: None,
        }
    }

    /// Mints the next structured id for `(handle, bucket)`, or refuses with a
    /// clock-health error under a forward clock jump beyond the skew bound.
    pub fn mint<T: StructuredId>(
        &mut self,
        handle: PlacementHandle,
        bucket: BucketId,
    ) -> Result<T, ClockHealthError> {
        let value = self.next_value(handle.get(), bucket.get())?;
        Ok(T::from_ulid(Ulid(value)))
    }

    fn next_value(&mut self, handle: u32, bucket: u16) -> Result<u128, ClockHealthError> {
        let now = self.env.now_ms();
        let monotonic = self.env.monotonic_ms();

        // Re-anchoring on every attempt keeps the guard non-sticky: idle time
        // is covered by the monotonic delta, and a refused jump accepts the
        // new wall clock as reality on the next mint.
        if let Some(anchor) = self.anchor.replace(ClockAnchor {
            wall_ms: now,
            monotonic_ms: monotonic,
        }) {
            let expected = anchor
                .wall_ms
                .saturating_add(monotonic.saturating_sub(anchor.monotonic_ms));
            if now > expected && now - expected > self.max_skew_ms {
                return Err(ClockHealthError::ForwardJump {
                    jump_ms: now - expected,
                    max_skew_ms: self.max_skew_ms,
                });
            }
        }

        let mut timestamp_ms = match self.last {
            Some(last) => now.max(last.timestamp_ms),
            None => now,
        };

        let nonce = match self.last {
            Some(last)
                if last.timestamp_ms == timestamp_ms
                    && last.handle == handle
                    && last.bucket == bucket =>
            {
                if last.nonce >= layout::MAX_NONCE {
                    timestamp_ms += 1;
                    self.env.random_nonce() & layout::NONCE_MASK
                } else {
                    last.nonce + 1
                }
            }
            _ => self.env.random_nonce() & layout::NONCE_MASK,
        };

        self.last = Some(LastMint {
            timestamp_ms,
            handle,
            bucket,
            nonce,
        });
        Ok(layout::pack(timestamp_ms, handle, bucket, nonce))
    }
}

#[cfg(test)]
mod tests {
    use super::super::layout;
    use super::super::{BucketId, JobId, MetaResourceId, PlacementHandle, StructuredId};
    use super::*;
    use std::cell::{Cell, RefCell};
    use std::collections::VecDeque;

    struct MockEnv {
        now: Cell<u64>,
        monotonic: Cell<u64>,
        nonces: RefCell<VecDeque<u64>>,
    }

    impl MockEnv {
        fn new(now: u64, nonces: impl IntoIterator<Item = u64>) -> Self {
            Self {
                now: Cell::new(now),
                monotonic: Cell::new(0),
                nonces: RefCell::new(nonces.into_iter().collect()),
            }
        }

        fn set_now(&self, now: u64) {
            self.now.set(now);
        }

        fn advance(&self, wall_ms: u64, monotonic_ms: u64) {
            self.now.set(self.now.get() + wall_ms);
            self.monotonic.set(self.monotonic.get() + monotonic_ms);
        }
    }

    impl IdEnvironment for MockEnv {
        fn now_ms(&self) -> u64 {
            self.now.get()
        }

        fn monotonic_ms(&self) -> u64 {
            self.monotonic.get()
        }

        fn random_nonce(&self) -> u64 {
            self.nonces.borrow_mut().pop_front().unwrap_or(0)
        }
    }

    fn handle_bucket() -> (PlacementHandle, BucketId) {
        (PlacementHandle::new(7).unwrap(), BucketId::new(2).unwrap())
    }

    #[test]
    fn nonce_monotonic() {
        // Same millisecond and same (handle, bucket): the nonce increments by one.
        let mut generator =
            StructuredIdGenerator::with_environment(MockEnv::new(1000, [100, 200]), 300_000);
        let (handle, bucket) = handle_bucket();
        let first: MetaResourceId = generator.mint(handle, bucket).unwrap();
        let second: MetaResourceId = generator.mint(handle, bucket).unwrap();
        assert_eq!(first.timestamp_ms(), second.timestamp_ms());
        assert_eq!(first.nonce() + 1, second.nonce());
    }

    #[test]
    fn overflow_waits_ms() {
        // A fresh nonce already at the 48-bit max forces the next mint to the next ms.
        let mut generator = StructuredIdGenerator::with_environment(
            MockEnv::new(1000, [layout::MAX_NONCE, 42]),
            300_000,
        );
        let (handle, bucket) = handle_bucket();
        let first: MetaResourceId = generator.mint(handle, bucket).unwrap();
        let second: MetaResourceId = generator.mint(handle, bucket).unwrap();
        assert_eq!(first.timestamp_ms() + 1, second.timestamp_ms());
        assert_eq!(second.nonce(), 42);
    }

    #[test]
    fn forward_jump_blocks() {
        // The wall clock jumps past the bound while the monotonic clock
        // barely moves: a genuine discontinuity is refused.
        let mut generator =
            StructuredIdGenerator::with_environment(MockEnv::new(1000, [1, 2]), 300_000);
        let (handle, bucket) = handle_bucket();
        let _: MetaResourceId = generator.mint(handle, bucket).unwrap();
        generator.env.advance(300_002, 1);
        assert_eq!(
            generator
                .mint::<MetaResourceId>(handle, bucket)
                .unwrap_err(),
            ClockHealthError::ForwardJump {
                jump_ms: 300_001,
                max_skew_ms: 300_000,
            }
        );
    }

    #[test]
    fn jump_recovers() {
        // A refused jump re-anchors: the next mint accepts the new wall clock.
        let mut generator =
            StructuredIdGenerator::with_environment(MockEnv::new(1000, [1, 2]), 300_000);
        let (handle, bucket) = handle_bucket();
        let _: MetaResourceId = generator.mint(handle, bucket).unwrap();
        generator.env.advance(600_000, 0);
        assert!(generator.mint::<MetaResourceId>(handle, bucket).is_err());
        let id: MetaResourceId = generator.mint(handle, bucket).unwrap();
        assert_eq!(id.timestamp_ms(), 601_000);
    }

    #[test]
    fn idle_gap_mints() {
        // Wall and monotonic clocks advance together far beyond the bound:
        // healthy idle time never errors and never bricks the generator.
        let mut generator =
            StructuredIdGenerator::with_environment(MockEnv::new(1000, [1, 2, 3]), 300_000);
        let (handle, bucket) = handle_bucket();
        let _: MetaResourceId = generator.mint(handle, bucket).unwrap();
        generator.env.advance(3_600_000, 3_600_000);
        let id: MetaResourceId = generator.mint(handle, bucket).unwrap();
        assert_eq!(id.timestamp_ms(), 3_601_000);
        generator.env.advance(3_600_000, 3_600_000);
        let id: MetaResourceId = generator.mint(handle, bucket).unwrap();
        assert_eq!(id.timestamp_ms(), 7_201_000);
    }

    #[test]
    fn within_skew_ok() {
        // A wall-only divergence of exactly the bound is accepted.
        let mut generator =
            StructuredIdGenerator::with_environment(MockEnv::new(1000, [1, 2]), 300_000);
        let (handle, bucket) = handle_bucket();
        let _: MetaResourceId = generator.mint(handle, bucket).unwrap();
        generator.env.advance(300_000, 0);
        let id: MetaResourceId = generator.mint(handle, bucket).unwrap();
        assert_eq!(id.timestamp_ms(), 1000 + 300_000);
    }

    #[test]
    fn backward_clock_clamps() {
        // A backward clock does not regress the timestamp and stays monotonic.
        let mut generator =
            StructuredIdGenerator::with_environment(MockEnv::new(5000, [1, 2]), 300_000);
        let (handle, bucket) = handle_bucket();
        let first: MetaResourceId = generator.mint(handle, bucket).unwrap();
        generator.env.set_now(4000);
        let second: MetaResourceId = generator.mint(handle, bucket).unwrap();
        assert_eq!(first.timestamp_ms(), second.timestamp_ms());
        assert_eq!(first.nonce() + 1, second.nonce());
    }

    #[test]
    fn distinct_bucket_fresh() {
        // A different bucket in the same ms draws a fresh random nonce.
        let mut generator =
            StructuredIdGenerator::with_environment(MockEnv::new(1000, [10, 20]), 300_000);
        let handle = PlacementHandle::new(7).unwrap();
        let first: MetaResourceId = generator.mint(handle, BucketId::new(1).unwrap()).unwrap();
        let second: MetaResourceId = generator.mint(handle, BucketId::new(2).unwrap()).unwrap();
        assert_eq!(first.nonce(), 10);
        assert_eq!(second.nonce(), 20);
    }

    #[test]
    fn job_id_mint() {
        // The generator serves JobId through the same shared codec.
        let mut generator =
            StructuredIdGenerator::with_environment(MockEnv::new(1000, [5]), 300_000);
        let (handle, bucket) = handle_bucket();
        let job: JobId = generator.mint(handle, bucket).unwrap();
        assert_eq!(job.placement_handle().get(), 7);
        assert_eq!(job.nonce(), 5);
    }
}
