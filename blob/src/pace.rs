//! Paces the source requests a task sends under a key, such as a read connector: one token
//! bucket per key on this node, taken by every HTTP request the adapter sends, retries included.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, SystemTime};

use http::{HeaderMap, StatusCode};
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
/// One sleep of a paced request at most; it then looks at the pause again.
const LONGEST_SLEEP: Duration = Duration::from_secs(60);

/// Tokens refill at the rate up to one; a request may borrow ahead and waits for it.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Bucket {
    tokens: f64,
    at: Instant,
    /// No request is sent before this, as a `Retry-After` answer asked.
    until: Option<Deadline>,
}

/// The end of a pause a source asked for.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Deadline {
    At(Instant),
    /// Later than any instant this clock holds.
    Never,
}

/// How long the bucket's pause still lasts at `now`, at most one sleep.
fn paused(bucket: &Bucket, now: Instant) -> Option<Duration> {
    match bucket.until? {
        Deadline::At(at) => (at > now).then(|| (at - now).min(LONGEST_SLEEP)),
        Deadline::Never => Some(LONGEST_SLEEP),
    }
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

/// The delay a `Retry-After` value asks for: seconds, or an HTTP date after `now`.
fn retry_after(value: &str, now: SystemTime) -> Option<Duration> {
    let value = value.trim();
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let date = chrono::DateTime::parse_from_rfc2822(value).ok()?;
    let date = SystemTime::from(date);
    Some(date.duration_since(now).unwrap_or_default())
}

/// Pauses the running task's key when an answer is throttled with `Retry-After`.
pub(crate) fn answered(status: StatusCode, headers: &HeaderMap) {
    let throttled = matches!(
        status,
        StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE
    );
    let Ok(pace) = PACE.try_with(|pace| *pace) else {
        return;
    };
    let value = headers.get(http::header::RETRY_AFTER);
    let Some(delay) = value
        .filter(|_| throttled)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| retry_after(value, SystemTime::now()))
    else {
        return;
    };
    let mut buckets = BUCKETS.lock().unwrap_or_else(|poison| poison.into_inner());
    let now = Instant::now();
    let bucket = (buckets.entry(pace.key)).or_insert(Bucket {
        tokens: 1.0,
        at: now,
        until: None,
    });
    let until = now.checked_add(delay).map_or(Deadline::Never, Deadline::At);
    bucket.until = Some(match (bucket.until, until) {
        (Some(Deadline::Never), _) | (_, Deadline::Never) => Deadline::Never,
        (Some(Deadline::At(known)), Deadline::At(at)) => Deadline::At(known.max(at)),
        (None, until) => until,
    });
}

/// Runs `future` with every source request it sends paced by `pace`.
pub async fn paced<F: Future>(pace: Pace, future: F) -> F::Output {
    PACE.scope(pace, future).await
}

/// Waits for the next request's token when the running task is paced. A pause found before
/// sending drops the request's slot, so requests leave a pause one slot apart.
pub(crate) async fn turn() {
    let Ok(pace) = PACE.try_with(|pace| *pace) else {
        return;
    };
    let mut slot: Option<Instant> = None;
    loop {
        let wait = {
            let mut buckets = BUCKETS.lock().unwrap_or_else(|poison| poison.into_inner());
            let now = Instant::now();
            let bucket = (buckets.entry(pace.key)).or_insert(Bucket {
                tokens: 1.0,
                at: now,
                until: None,
            });
            match paused(bucket, now) {
                Some(pause) => {
                    slot = None;
                    pause
                }
                None => {
                    let at = *slot.get_or_insert_with(|| now + take(bucket, pace.rate, now));
                    if at <= now {
                        return;
                    }
                    (at - now).min(LONGEST_SLEEP)
                }
            }
        };
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
            until: None,
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
            until: None,
        };
        assert_eq!(take(&mut idle, 10, later), Duration::ZERO);
        assert_eq!(take(&mut idle, 10, later), Duration::from_millis(100));
    }

    #[test]
    fn retry_after_forms() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_445_412_480);
        assert_eq!(retry_after(" 7 ", now), Some(Duration::from_secs(7)));
        let date = "Wed, 21 Oct 2015 07:28:30 GMT";
        assert_eq!(retry_after(date, now), Some(Duration::from_secs(30)));
        assert_eq!(
            retry_after("Wed, 21 Oct 2015 07:00:00 GMT", now),
            Some(Duration::ZERO)
        );
        assert_eq!(retry_after("soon", now), None);
    }

    // A throttled answer with `Retry-After` holds back the key's next request; other answers
    // and unpaced tasks do not.
    #[tokio::test(start_paused = true)]
    async fn throttled_answers_pause() {
        let pace = Pace {
            key: Ulid::from_bytes([9; 16]),
            rate: 100,
        };
        let mut headers = HeaderMap::new();
        headers.insert(http::header::RETRY_AFTER, "2".parse().unwrap());
        answered(StatusCode::TOO_MANY_REQUESTS, &headers);
        let start = Instant::now();
        paced(pace, async {
            turn().await;
            answered(StatusCode::OK, &headers);
            turn().await;
            assert!(Instant::now() - start < Duration::from_secs(1));
            answered(StatusCode::TOO_MANY_REQUESTS, &headers);
            turn().await;
        })
        .await;
        assert!(Instant::now() - start >= Duration::from_secs(2));
    }

    // Requests waiting out a pause leave it one slot apart, and a slot waits again when the
    // pause is extended before it is reached.
    #[tokio::test(start_paused = true)]
    async fn pauses_release_spaced() {
        let pace = Pace {
            key: Ulid::from_bytes([12; 16]),
            rate: 10,
        };
        let start = Instant::now();
        let mut headers = HeaderMap::new();
        headers.insert(http::header::RETRY_AFTER, "2".parse().unwrap());
        paced(pace, async {
            answered(StatusCode::TOO_MANY_REQUESTS, &headers)
        })
        .await;
        let waiter = || {
            paced(pace, async {
                turn().await;
                Instant::now() - start
            })
        };
        let (a, b, c) = tokio::join!(waiter(), waiter(), waiter());
        let mut sent = [a, b, c];
        sent.sort();
        assert!(sent[0] >= Duration::from_secs(2));
        assert!(sent[1] - sent[0] >= Duration::from_millis(100));
        assert!(sent[2] - sent[1] >= Duration::from_millis(100));

        let slow = Pace {
            key: Ulid::from_bytes([13; 16]),
            rate: 1,
        };
        headers.insert(http::header::RETRY_AFTER, "5".parse().unwrap());
        let start = Instant::now();
        let requests = paced(slow, async {
            turn().await;
            turn().await;
            Instant::now() - start
        });
        let extension = paced(slow, async {
            tokio::time::sleep(Duration::from_millis(500)).await;
            answered(StatusCode::TOO_MANY_REQUESTS, &headers);
        });
        let (second, ()) = tokio::join!(requests, extension);
        assert!(second >= Duration::from_millis(5500));
    }

    // A long `Retry-After` is kept whole, and one beyond any instant pauses for good.
    #[tokio::test(start_paused = true)]
    async fn long_pauses_kept() {
        let mut headers = HeaderMap::new();
        headers.insert(http::header::RETRY_AFTER, "3600".parse().unwrap());
        let pace = Pace {
            key: Ulid::from_bytes([10; 16]),
            rate: 10,
        };
        let start = Instant::now();
        paced(pace, async {
            answered(StatusCode::SERVICE_UNAVAILABLE, &headers);
            turn().await;
        })
        .await;
        assert!(Instant::now() - start >= Duration::from_secs(3600));
        let endless = Pace {
            key: Ulid::from_bytes([11; 16]),
            rate: 10,
        };
        headers.insert(
            http::header::RETRY_AFTER,
            u64::MAX.to_string().parse().unwrap(),
        );
        let waited = paced(endless, async {
            answered(StatusCode::TOO_MANY_REQUESTS, &headers);
            tokio::time::timeout(Duration::from_secs(30 * 86_400), turn()).await
        })
        .await;
        assert!(waited.is_err(), "the pause outlasts any wait");
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
