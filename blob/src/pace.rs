//! Paces the source requests a task sends under a key, such as a read connector: one token
//! bucket per key on this node, taken by every HTTP request the adapter sends, retries included.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, SystemTime};

use http::{HeaderMap, StatusCode};
use thiserror::Error;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

tokio::task_local! {
    static PACE: Pace;
}

/// Told the end of a pause a request waits for, and `None` once the wait is over.
pub type PauseNotice = Arc<dyn Fn(Option<SystemTime>) + Send + Sync>;

/// Requests per second for every source request sent under `key`; any of `stops` ends a wait.
#[derive(Clone)]
pub struct Pace {
    pub key: Ulid,
    pub rate: u32,
    pub stops: Vec<CancellationToken>,
    pub notice: Option<PauseNotice>,
}

impl Pace {
    pub fn new(key: Ulid, rate: u32) -> Self {
        Self {
            key,
            rate,
            stops: Vec::new(),
            notice: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum PaceError {
    #[error("the job stopped while its source request waited")]
    Stopped,
    #[error("the source asked to pause for longer than this node can represent")]
    Unrepresentable,
}

static BUCKETS: LazyLock<Mutex<HashMap<Ulid, Bucket>>> = LazyLock::new(Default::default);

/// Tokens refill at the rate up to one; a request may borrow ahead and waits for it.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Bucket {
    tokens: f64,
    at: Instant,
    /// No request is sent before this, as a `Retry-After` answer asked.
    until: Option<Instant>,
}

/// How long the bucket's pause still lasts at `now`.
fn paused(bucket: &Bucket, now: Instant) -> Option<Duration> {
    bucket
        .until
        .filter(|until| *until > now)
        .map(|until| until - now)
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
fn retry_after(value: &str, now: SystemTime) -> Result<Option<Duration>, PaceError> {
    let value = value.trim();
    if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) {
        let seconds = value.parse().map_err(|_| PaceError::Unrepresentable)?;
        return Ok(Some(Duration::from_secs(seconds)));
    }
    let Ok(date) = chrono::DateTime::parse_from_rfc2822(value) else {
        return Ok(None);
    };
    let date = SystemTime::from(date);
    Ok(Some(date.duration_since(now).unwrap_or_default()))
}

/// Pauses the running task's key when an answer is throttled with `Retry-After`; a pause this
/// node cannot represent fails the request instead of pausing the key for good.
pub(crate) fn answered(status: StatusCode, headers: &HeaderMap) -> Result<(), PaceError> {
    let throttled = matches!(
        status,
        StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE
    );
    let Ok(key) = PACE.try_with(|pace| pace.key) else {
        return Ok(());
    };
    let value = headers.get(http::header::RETRY_AFTER).filter(|_| throttled);
    let Some(value) = value.and_then(|value| value.to_str().ok()) else {
        return Ok(());
    };
    let Some(delay) = retry_after(value, SystemTime::now())? else {
        return Ok(());
    };
    let mut buckets = BUCKETS.lock().unwrap_or_else(|poison| poison.into_inner());
    let now = Instant::now();
    let until = now.checked_add(delay).ok_or(PaceError::Unrepresentable)?;
    let bucket = (buckets.entry(key)).or_insert(Bucket {
        tokens: 1.0,
        at: now,
        until: None,
    });
    bucket.until = Some(bucket.until.map_or(until, |known| known.max(until)));
    Ok(())
}

/// Runs `future` with every source request it sends paced by `pace`.
pub async fn paced<F: Future>(pace: Pace, future: F) -> F::Output {
    PACE.scope(pace, future).await
}

/// Sleeps for `wait` unless a stop token fires first.
async fn sleep_unless(wait: Duration, stops: &[CancellationToken]) -> Result<(), PaceError> {
    if stops.is_empty() {
        tokio::time::sleep(wait).await;
        return Ok(());
    }
    let stopped = futures::future::select_all(stops.iter().map(|stop| Box::pin(stop.cancelled())));
    tokio::select! {
        _ = tokio::time::sleep(wait) => Ok(()),
        _ = stopped => Err(PaceError::Stopped),
    }
}

/// Waits for the next request's token when the running task is paced. A pause found before
/// sending drops the request's slot, so requests leave a pause one slot apart. The notice
/// hears the pause's end while the request waits for it.
pub(crate) async fn turn() -> Result<(), PaceError> {
    let Ok(pace) = PACE.try_with(Pace::clone) else {
        return Ok(());
    };
    let notify = |until: Option<SystemTime>| {
        if let Some(notice) = &pace.notice {
            notice(until);
        }
    };
    let mut slot: Option<Instant> = None;
    let mut noticed = false;
    loop {
        let (wait, pause) = {
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
                    (pause, true)
                }
                None => {
                    let at = *slot.get_or_insert_with(|| now + take(bucket, pace.rate, now));
                    (at.saturating_duration_since(now), false)
                }
            }
        };
        if pause {
            notify(SystemTime::now().checked_add(wait));
            noticed = true;
        } else if noticed {
            notify(None);
            noticed = false;
        }
        if wait.is_zero() {
            return Ok(());
        }
        if let Err(stopped) = sleep_unless(wait, &pace.stops).await {
            if noticed {
                notify(None);
            }
            return Err(stopped);
        }
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
        assert_eq!(retry_after(" 7 ", now), Ok(Some(Duration::from_secs(7))));
        let date = "Wed, 21 Oct 2015 07:28:30 GMT";
        assert_eq!(retry_after(date, now), Ok(Some(Duration::from_secs(30))));
        assert_eq!(
            retry_after("Wed, 21 Oct 2015 07:00:00 GMT", now),
            Ok(Some(Duration::ZERO))
        );
        assert_eq!(retry_after("soon", now), Ok(None));
        let huge = "9".repeat(30);
        assert_eq!(retry_after(&huge, now), Err(PaceError::Unrepresentable));
    }

    // A throttled answer with `Retry-After` holds back the key's next request; other answers
    // and unpaced tasks do not.
    #[tokio::test(start_paused = true)]
    async fn throttled_answers_pause() {
        let pace = Pace::new(Ulid::from_bytes([9; 16]), 100);
        let mut headers = HeaderMap::new();
        headers.insert(http::header::RETRY_AFTER, "2".parse().unwrap());
        answered(StatusCode::TOO_MANY_REQUESTS, &headers).unwrap();
        let start = Instant::now();
        paced(pace, async {
            turn().await.unwrap();
            answered(StatusCode::OK, &headers).unwrap();
            turn().await.unwrap();
            assert!(Instant::now() - start < Duration::from_secs(1));
            answered(StatusCode::TOO_MANY_REQUESTS, &headers).unwrap();
            turn().await.unwrap();
        })
        .await;
        assert!(Instant::now() - start >= Duration::from_secs(2));
    }

    // Requests waiting out a pause leave it one slot apart, and a slot waits again when the
    // pause is extended before it is reached.
    #[tokio::test(start_paused = true)]
    async fn pauses_release_spaced() {
        let pace = Pace::new(Ulid::from_bytes([12; 16]), 10);
        let start = Instant::now();
        let mut headers = HeaderMap::new();
        headers.insert(http::header::RETRY_AFTER, "2".parse().unwrap());
        paced(pace.clone(), async {
            answered(StatusCode::TOO_MANY_REQUESTS, &headers)
        })
        .await
        .unwrap();
        let waiter = || {
            paced(pace.clone(), async {
                turn().await.unwrap();
                Instant::now() - start
            })
        };
        let (a, b, c) = tokio::join!(waiter(), waiter(), waiter());
        let mut sent = [a, b, c];
        sent.sort();
        assert!(sent[0] >= Duration::from_secs(2));
        assert!(sent[1] - sent[0] >= Duration::from_millis(100));
        assert!(sent[2] - sent[1] >= Duration::from_millis(100));

        let slow = Pace::new(Ulid::from_bytes([13; 16]), 1);
        headers.insert(http::header::RETRY_AFTER, "5".parse().unwrap());
        let start = Instant::now();
        let requests = paced(slow.clone(), async {
            turn().await.unwrap();
            turn().await.unwrap();
            Instant::now() - start
        });
        let extension = paced(slow, async {
            tokio::time::sleep(Duration::from_millis(500)).await;
            answered(StatusCode::TOO_MANY_REQUESTS, &headers).unwrap();
        });
        let (second, ()) = tokio::join!(requests, extension);
        assert!(second >= Duration::from_millis(5500));
    }

    // A long `Retry-After` is kept whole, and one beyond any instant pauses for good.
    #[tokio::test(start_paused = true)]
    async fn long_pauses_kept() {
        let mut headers = HeaderMap::new();
        headers.insert(http::header::RETRY_AFTER, "3600".parse().unwrap());
        let pace = Pace::new(Ulid::from_bytes([10; 16]), 10);
        let start = Instant::now();
        paced(pace, async {
            answered(StatusCode::SERVICE_UNAVAILABLE, &headers).unwrap();
            turn().await.unwrap();
        })
        .await;
        assert!(Instant::now() - start >= Duration::from_secs(3600));
        // A pause beyond any instant fails the request and pauses nothing.
        let endless = Pace::new(Ulid::from_bytes([11; 16]), 10);
        let start = Instant::now();
        for value in [u64::MAX.to_string(), "1".repeat(40)] {
            headers.insert(http::header::RETRY_AFTER, value.parse().unwrap());
            let answer = paced(endless.clone(), async {
                answered(StatusCode::TOO_MANY_REQUESTS, &headers)
            });
            assert_eq!(answer.await, Err(PaceError::Unrepresentable));
        }
        paced(endless, turn()).await.unwrap();
        assert_eq!(Instant::now(), start);
    }

    // A request waiting out a pause tells its notice the pause's end and ends when the job
    // stops; a finished wait tells it the pause is over.
    #[tokio::test(start_paused = true)]
    async fn pause_waits_stoppable() {
        let notices = Arc::new(Mutex::new(Vec::new()));
        let heard = notices.clone();
        let stop = CancellationToken::new();
        let pace = Pace {
            stops: vec![CancellationToken::new(), stop.clone()],
            notice: Some(Arc::new(move |until| heard.lock().unwrap().push(until))),
            ..Pace::new(Ulid::from_bytes([14; 16]), 10)
        };
        let mut headers = HeaderMap::new();
        headers.insert(http::header::RETRY_AFTER, "2".parse().unwrap());
        paced(pace.clone(), async {
            answered(StatusCode::TOO_MANY_REQUESTS, &headers).unwrap();
            turn().await.unwrap();
        })
        .await;
        assert!(notices.lock().unwrap()[0].is_some());
        assert_eq!(notices.lock().unwrap()[1..], [None]);
        headers.insert(http::header::RETRY_AFTER, "3600".parse().unwrap());
        let start = Instant::now();
        let waiting = paced(pace, async {
            answered(StatusCode::TOO_MANY_REQUESTS, &headers).unwrap();
            turn().await
        });
        let stopping = async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            stop.cancel();
        };
        let (waited, ()) = tokio::join!(waiting, stopping);
        assert_eq!(waited, Err(PaceError::Stopped));
        assert_eq!(Instant::now() - start, Duration::from_secs(1));
        assert_eq!(notices.lock().unwrap().last(), Some(&None));
    }

    // Only a paced task waits; one key's pace never slows another key or an unpaced task.
    #[tokio::test(start_paused = true)]
    async fn turns_follow_scope() {
        let key = Ulid::from_bytes([7; 16]);
        let start = Instant::now();
        for _ in 0..5 {
            turn().await.unwrap();
        }
        assert_eq!(Instant::now(), start);
        let pace = Pace::new(key, 10);
        paced(pace, async {
            for _ in 0..5 {
                turn().await.unwrap();
            }
        })
        .await;
        assert_eq!(Instant::now() - start, Duration::from_millis(400));
        let other = Pace::new(Ulid::from_bytes([8; 16]), 10);
        let before = Instant::now();
        paced(other, turn()).await.unwrap();
        assert_eq!(Instant::now(), before);
    }
}
