//! The runner's process clock: stamp times that never go back and do not follow a forward jump
//! of the wall clock, and per-run entropy whose run slot no other run of the process shares.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::{LazyLock, Mutex, PoisonError};

use aruna_clock::SystemEnvironment;
use aruna_core::id_mint::run_entropy;
use aruna_core::structured_id::clock_health::{IdEnvironment, MAX_ID_SKEW};
use tracing::error;

/// The clock every drive of this process stamps from.
static RUN_CLOCK: LazyLock<RunClock<SystemEnvironment>> =
    LazyLock::new(|| RunClock::new(SystemEnvironment::new(), MAX_ID_SKEW));

/// The time the runner stamps on a run now.
pub fn stamp_time() -> u64 {
    RUN_CLOCK.now_ms()
}

/// The entropy of a new run.
pub fn next_entropy() -> u128 {
    RUN_CLOCK.entropy()
}

/// A checked clock and run sequence over an environment that tests pin.
pub struct RunClock<E: IdEnvironment> {
    env: E,
    max_skew_ms: u64,
    /// Random bits of this process, above the slot in every run's entropy.
    process: u32,
    /// Random start of the run slots, so another process rarely shares them.
    base: u32,
    state: Mutex<ClockState>,
}

#[derive(Default)]
struct ClockState {
    /// Wall and monotonic time of the last reading that passed the jump check.
    anchor: Option<(u64, u64)>,
    last_ms: u64,
    runs: u64,
    jumped: bool,
}

impl<E: IdEnvironment> RunClock<E> {
    pub fn new(env: E, max_skew_ms: u64) -> Self {
        let process = env.random_nonce() as u32;
        let base = env.random_nonce() as u32;
        Self {
            env,
            max_skew_ms,
            process,
            base,
            state: Mutex::new(ClockState::default()),
        }
    }

    /// The wall clock, never below a time handed out before. While the wall clock runs more than
    /// the skew bound ahead of the monotonic clock since the last good reading, the time follows
    /// the monotonic clock instead, as the old id generator refused such a jump.
    pub fn now_ms(&self) -> u64 {
        let (wall, monotonic) = loop {
            let before = self.env.monotonic_ms();
            let wall = self.env.now_ms();
            let monotonic = self.env.monotonic_ms();
            if monotonic.saturating_sub(before) <= self.max_skew_ms {
                break (wall, monotonic);
            }
        };
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let expected = (state.anchor).map(|(anchor_wall, anchor_monotonic)| {
            anchor_wall.saturating_add(monotonic.saturating_sub(anchor_monotonic))
        });
        let now = match expected {
            Some(expected) if wall.saturating_sub(expected) > self.max_skew_ms => {
                if !state.jumped {
                    let jump_ms = wall - expected;
                    let max_skew_ms = self.max_skew_ms;
                    error!(
                        jump_ms,
                        max_skew_ms, "Wall clock jumped forward; stamping past it"
                    );
                }
                state.jumped = true;
                expected
            }
            _ => {
                state.anchor = Some((wall, monotonic));
                state.jumped = false;
                wall
            }
        };
        state.last_ms = state.last_ms.max(now);
        state.last_ms
    }

    /// The entropy of the next run: this process's bits and the next run slot. Slots repeat only
    /// after 2^32 runs; when they wrap, later stamps move past every time handed out before.
    pub fn entropy(&self) -> u128 {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let slot = self.base.wrapping_add(state.runs as u32);
        if slot == 0 && state.runs > 0 {
            state.last_ms += 1;
        }
        state.runs += 1;
        run_entropy(self.process, slot)
    }
}

#[cfg(test)]
mod tests {
    use super::RunClock;
    use aruna_core::id_mint::{COUNTER_BITS, IdMint};
    use aruna_core::structured_id::clock_health::IdEnvironment;
    use std::cell::{Cell, RefCell};
    use std::collections::VecDeque;

    /// A pinned wall clock, monotonic clock and random source.
    struct Pinned {
        wall: Cell<u64>,
        monotonic: Cell<u64>,
        randoms: RefCell<VecDeque<u64>>,
    }

    impl IdEnvironment for Pinned {
        fn now_ms(&self) -> u64 {
            self.wall.get()
        }

        fn monotonic_ms(&self) -> u64 {
            self.monotonic.get()
        }

        fn random_nonce(&self) -> u64 {
            self.randoms.borrow_mut().pop_front().unwrap_or(0)
        }
    }

    fn pinned(wall: u64, randoms: [u64; 2]) -> RunClock<Pinned> {
        let env = Pinned {
            wall: Cell::new(wall),
            monotonic: Cell::new(0),
            randoms: RefCell::new(randoms.into()),
        };
        RunClock::new(env, 300_000)
    }

    #[test]
    fn stamps_never_fall() {
        let clock = pinned(1_000, [0, 0]);
        assert_eq!(clock.now_ms(), 1_000);
        clock.env.wall.set(900);
        clock.env.monotonic.set(5);
        assert_eq!(clock.now_ms(), 1_000);
    }

    // A wall clock far ahead of the monotonic clock is not followed until it comes back.
    #[test]
    fn forward_jump_held() {
        let clock = pinned(1_000, [0, 0]);
        assert_eq!(clock.now_ms(), 1_000);
        clock.env.monotonic.set(10);
        clock.env.wall.set(1_010 + 300_001);
        assert_eq!(clock.now_ms(), 1_010);
        clock.env.monotonic.set(20);
        clock.env.wall.set(1_020);
        assert_eq!(clock.now_ms(), 1_020);
    }

    // Runs in one millisecond get rising slots, so their ids never meet and keep rising.
    #[test]
    fn runs_never_share() {
        let clock = pinned(1_000, [9, 40]);
        let nonce = |entropy: u128| (entropy & ((1 << 48) - 1)) >> COUNTER_BITS;
        let (first, second) = (clock.entropy(), clock.entropy());
        assert_eq!((nonce(first), nonce(second)), (40, 41));
        assert_eq!(first >> 48, second >> 48);
        let now = clock.now_ms();
        let mut early = IdMint::new(first);
        let ids = [early.mint(now), early.mint(now)];
        let late = IdMint::new(second).mint(now);
        assert!(ids.iter().all(|id| *id < late));
    }

    #[test]
    fn slot_wrap_advances() {
        let clock = pinned(1_000, [0, u32::MAX.into()]);
        assert_eq!(clock.now_ms(), 1_000);
        clock.entropy();
        clock.entropy();
        assert_eq!(clock.now_ms(), 1_001);
    }
}
