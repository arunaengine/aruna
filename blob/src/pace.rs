//! Paces the source requests a task sends under a key, such as a read connector: one token
//! bucket per key on this node, taken by every HTTP request the adapter sends, retries included.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use tokio::time::Instant;
use ulid::Ulid;

tokio::task_local! {
    static PACE: Pace;
}

/// Requests per second for every source request sent under `key`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Pace {
    pub key: Ulid,
    pub rate: u32,
}

static BUCKETS: LazyLock<Mutex<HashMap<Ulid, Bucket>>> = LazyLock::new(Default::default);

/// Tokens refill at the rate up to one; a request may borrow ahead and waits for it.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Bucket {
    tokens: f64,
    at: Instant,
}

/// Takes one token at `now` and returns how long the request waits for it.
fn take(bucket: &mut Bucket, rate: u32, now: Instant) -> Duration {
    let rate = f64::from(rate.max(1));
    let elapsed = now.saturating_duration_since(bucket.at).as_secs_f64();
    bucket.tokens = (bucket.tokens + elapsed * rate).min(1.0);
    bucket.at = now;
    bucket.tokens -= 1.0;
    match bucket.tokens < 0.0 {
        true => Duration::from_secs_f64(-bucket.tokens / rate),
        false => Duration::ZERO,
    }
}

/// Runs `future` with every source request it sends paced by `pace`.
pub async fn paced<F: Future>(pace: Pace, future: F) -> F::Output {
    PACE.scope(pace, future).await
}

/// Waits for the next request's token when the running task is paced.
pub(crate) async fn turn() {
    let Ok(pace) = PACE.try_with(|pace| *pace) else {
        return;
    };
    let wait = {
        let mut buckets = BUCKETS.lock().unwrap_or_else(|poison| poison.into_inner());
        let now = Instant::now();
        let bucket = (buckets.entry(pace.key)).or_insert(Bucket {
            tokens: 1.0,
            at: now,
        });
        take(bucket, pace.rate, now)
    };
    if !wait.is_zero() {
        tokio::time::sleep(wait).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Requests are spaced by the rate, so no second holds more than the rate.
    #[test]
    fn bucket_paces_requests() {
        let start = Instant::now();
        let mut bucket = Bucket {
            tokens: 1.0,
            at: start,
        };
        let waits: Vec<Duration> = (0..30).map(|_| take(&mut bucket, 10, start)).collect();
        assert_eq!(waits[0], Duration::ZERO);
        assert_eq!(waits[1], Duration::from_millis(100));
        assert_eq!(waits[29], Duration::from_millis(2900));
        // An idle bucket holds one token, not the whole idle time's worth.
        let later = start + Duration::from_secs(60);
        let mut idle = Bucket {
            tokens: 0.0,
            at: start,
        };
        assert_eq!(take(&mut idle, 10, later), Duration::ZERO);
        assert_eq!(take(&mut idle, 10, later), Duration::from_millis(100));
    }

    // Only a paced task waits; one key's pace never slows another key or an unpaced task.
    #[tokio::test(start_paused = true)]
    async fn turns_follow_scope() {
        let key = Ulid::from_bytes([7; 16]);
        let start = Instant::now();
        for _ in 0..5 {
            turn().await;
        }
        assert_eq!(Instant::now(), start);
        let pace = Pace { key, rate: 10 };
        paced(pace, async {
            for _ in 0..5 {
                turn().await;
            }
        })
        .await;
        assert_eq!(Instant::now() - start, Duration::from_millis(400));
        let other = Pace {
            key: Ulid::from_bytes([8; 16]),
            rate: 10,
        };
        let before = Instant::now();
        paced(other, turn()).await;
        assert_eq!(Instant::now(), before);
    }
}
