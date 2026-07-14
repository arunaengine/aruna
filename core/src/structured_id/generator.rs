#[cfg(any(target_os = "linux", target_os = "macos"))]
use nix::{sys::time::TimeValLike, time::ClockId};
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
use std::time::Instant;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
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
    #[error("timestamp {timestamp_ms} ms exceeds the 48-bit id timestamp range")]
    TimestampOverflow { timestamp_ms: u64 },
}

/// Injectable time and entropy source. The production impl reads the system
/// clock and the OS CSPRNG; tests supply deterministic values.
pub trait IdEnvironment {
    /// Validated wall-clock time as Unix milliseconds.
    fn now_ms(&self) -> u64;
    /// Milliseconds from a suspend-aware monotonic clock with an arbitrary but
    /// fixed origin.
    fn monotonic_ms(&self) -> u64;
    /// Waits until the wall clock advances beyond `timestamp_ms`.
    fn wait_next_ms(&self, timestamp_ms: u64) {
        while self.now_ms() <= timestamp_ms {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    /// A fresh 48-bit nonce drawn from a cryptographically secure source.
    fn random_nonce(&self) -> u64;
}

#[derive(Debug, Clone, Copy)]
pub struct SystemEnvironment {
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    origin: Instant,
}

impl SystemEnvironment {
    pub fn new() -> Self {
        Self {
            #[cfg(not(any(target_os = "linux", target_os = "macos")))]
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
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            #[cfg(target_os = "linux")]
            let clock = ClockId::CLOCK_BOOTTIME;
            #[cfg(target_os = "macos")]
            let clock = ClockId::from_raw(nix::libc::CLOCK_MONOTONIC_RAW);

            clock
                .now()
                .expect("suspend-aware monotonic clock is available")
                .num_milliseconds() as u64
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            self.origin.elapsed().as_millis() as u64
        }
    }

    fn random_nonce(&self) -> u64 {
        (Ulid::r#gen().random() as u64) & layout::NONCE_MASK
    }
}

#[derive(Clone, Copy)]
struct LastMint {
    timestamp_ms: u64,
    nonce: u64,
}

/// Paired wall/monotonic reading from the most recent validated clock sample.
/// The wall clock is expected to advance in step with the monotonic clock;
/// divergence beyond the skew bound is a forward jump.
#[derive(Clone, Copy)]
struct ClockAnchor {
    wall_ms: u64,
    monotonic_ms: u64,
}

/// Structured-ID generator: a monotonic ULID timestamp with a forward-clock-
/// jump guard (REQ-META-ID-TIME-001, REQ-META-ID-NONCE-001). The nonce
/// starts from a fresh random value for each timestamp and increments across
/// every subsequent mint at that timestamp, including interleaved handles and
/// buckets.
/// It never emits a generic ULID, so the structured fields are always
/// preserved.
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
        Ok(T::from_ulid(Ulid(value), super::sealed::new()))
    }

    fn validated_now_ms(&mut self) -> Result<u64, ClockHealthError> {
        let (now, monotonic) = loop {
            let monotonic_before = self.env.monotonic_ms();
            let now = self.env.now_ms();
            let monotonic = self.env.monotonic_ms();
            if monotonic.saturating_sub(monotonic_before) <= self.max_skew_ms {
                break (now, monotonic);
            }
        };

        if let Some(anchor) = self.anchor {
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

        self.anchor = Some(ClockAnchor {
            wall_ms: now,
            monotonic_ms: monotonic,
        });
        Ok(now)
    }

    fn next_value(&mut self, handle: u32, bucket: u16) -> Result<u128, ClockHealthError> {
        let now = self.validated_now_ms()?;

        let mut timestamp_ms = match self.last {
            Some(last) => now.max(last.timestamp_ms),
            None => now,
        };

        while let Some(last) = self.last
            && last.timestamp_ms == timestamp_ms
            && last.nonce >= layout::MAX_NONCE
        {
            self.env.wait_next_ms(last.timestamp_ms);
            timestamp_ms = self.validated_now_ms()?.max(last.timestamp_ms);
        }

        let nonce = match self.last {
            Some(last) if last.timestamp_ms == timestamp_ms => last.nonce + 1,
            _ => self.env.random_nonce() & layout::NONCE_MASK,
        };

        if timestamp_ms > layout::MAX_TIMESTAMP_MS {
            return Err(ClockHealthError::TimestampOverflow { timestamp_ms });
        }

        self.last = Some(LastMint {
            timestamp_ms,
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
        suspend_on_now: Cell<u64>,
        nonces: RefCell<VecDeque<u64>>,
    }

    impl MockEnv {
        fn new(now: u64, nonces: impl IntoIterator<Item = u64>) -> Self {
            Self {
                now: Cell::new(now),
                monotonic: Cell::new(0),
                suspend_on_now: Cell::new(0),
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

        fn suspend_on_next_now(&self, elapsed_ms: u64) {
            self.suspend_on_now.set(elapsed_ms);
        }
    }

    impl IdEnvironment for MockEnv {
        fn now_ms(&self) -> u64 {
            let suspended_ms = self.suspend_on_now.replace(0);
            self.advance(suspended_ms, suspended_ms);
            self.now.get()
        }

        fn monotonic_ms(&self) -> u64 {
            self.monotonic.get()
        }

        fn wait_next_ms(&self, timestamp_ms: u64) {
            self.now.set(timestamp_ms + 1);
            self.monotonic.set(self.monotonic.get() + 1);
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
        // Consecutive mints in the same millisecond increment the nonce by one.
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
        assert_eq!(generator.env.now.get(), second.timestamp_ms());
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
    fn jump_remains_blocked() {
        // A refused jump does not become the new trusted clock anchor.
        let mut generator =
            StructuredIdGenerator::with_environment(MockEnv::new(1000, [1, 2]), 300_000);
        let (handle, bucket) = handle_bucket();
        let _: MetaResourceId = generator.mint(handle, bucket).unwrap();
        generator.env.advance(600_000, 0);
        let expected = ClockHealthError::ForwardJump {
            jump_ms: 600_000,
            max_skew_ms: 300_000,
        };
        assert_eq!(
            generator
                .mint::<MetaResourceId>(handle, bucket)
                .unwrap_err(),
            expected
        );
        assert_eq!(
            generator
                .mint::<MetaResourceId>(handle, bucket)
                .unwrap_err(),
            expected
        );
        generator.env.set_now(1001);
        let id: MetaResourceId = generator.mint(handle, bucket).unwrap();
        assert_eq!(id.timestamp_ms(), 1001);
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
    fn suspend_during_clock_sample_retries() {
        let mut generator =
            StructuredIdGenerator::with_environment(MockEnv::new(1000, [1, 2]), 300_000);
        let (handle, bucket) = handle_bucket();
        let _: MetaResourceId = generator.mint(handle, bucket).unwrap();
        generator.env.suspend_on_next_now(600_000);
        let id: MetaResourceId = generator.mint(handle, bucket).unwrap();
        assert_eq!(id.timestamp_ms(), 601_000);
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
    fn interleaved_bucket_does_not_collide() {
        // Returning to a bucket in the same ms continues the shared nonce sequence.
        let mut generator =
            StructuredIdGenerator::with_environment(MockEnv::new(1000, [10, 20, 10]), 300_000);
        let handle = PlacementHandle::new(7).unwrap();
        let first: MetaResourceId = generator.mint(handle, BucketId::new(1).unwrap()).unwrap();
        let second: MetaResourceId = generator.mint(handle, BucketId::new(2).unwrap()).unwrap();
        let third: MetaResourceId = generator.mint(handle, BucketId::new(1).unwrap()).unwrap();
        assert_eq!(first.nonce(), 10);
        assert_eq!(second.nonce(), 11);
        assert_eq!(third.nonce(), 12);
        assert_ne!(first, third);
    }

    #[test]
    fn timestamp_overflow_errs() {
        // A wall clock past the 48-bit cap refuses instead of truncating.
        let mut generator = StructuredIdGenerator::with_environment(
            MockEnv::new(layout::MAX_TIMESTAMP_MS + 1, [1]),
            300_000,
        );
        let (handle, bucket) = handle_bucket();
        assert_eq!(
            generator
                .mint::<MetaResourceId>(handle, bucket)
                .unwrap_err(),
            ClockHealthError::TimestampOverflow {
                timestamp_ms: layout::MAX_TIMESTAMP_MS + 1,
            }
        );
    }

    #[test]
    fn overflow_bump_errs() {
        // Waiting past the timestamp cap refuses rather than truncating.
        let mut generator = StructuredIdGenerator::with_environment(
            MockEnv::new(layout::MAX_TIMESTAMP_MS, [layout::MAX_NONCE, 1]),
            300_000,
        );
        let (handle, bucket) = handle_bucket();
        let first: MetaResourceId = generator.mint(handle, bucket).unwrap();
        assert_eq!(first.timestamp_ms(), layout::MAX_TIMESTAMP_MS);
        assert_eq!(
            generator
                .mint::<MetaResourceId>(handle, bucket)
                .unwrap_err(),
            ClockHealthError::TimestampOverflow {
                timestamp_ms: layout::MAX_TIMESTAMP_MS + 1,
            }
        );
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
