//! Paces the source requests of table imports: one token bucket per read connector on this
//! node, and backoff with `Retry-After` for transient answers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use aruna_core::structs::execution::job::StagingJobSpec;
use rand::RngExt;
use tokio::time::Instant;
use ulid::Ulid;

use tokio_util::sync::CancellationToken;

/// Attempts of one request while its answers stay transient.
const ATTEMPTS: u32 = 5;
/// The first backoff; each further attempt doubles it.
const BACKOFF: Duration = Duration::from_millis(500);
/// The longest backoff between two attempts.
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// The longest `Retry-After` a page waits for; a longer one retries the job later.
const MAX_RETRY_AFTER: Duration = Duration::from_secs(600);

static BUCKETS: LazyLock<Mutex<HashMap<Ulid, Bucket>>> = LazyLock::new(Default::default);

/// Tokens refill at `rate` per second up to one; a request may borrow ahead and waits for it.
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

/// Waits for `requests` tokens of `connector_id`, shared by every job of this node.
async fn acquire(connector_id: Ulid, rate: u32, requests: u32) {
    let wait = {
        let mut buckets = BUCKETS.lock().unwrap_or_else(|poison| poison.into_inner());
        let now = Instant::now();
        let bucket = (buckets.entry(connector_id)).or_insert(Bucket {
            tokens: 1.0,
            at: now,
        });
        (0..requests.max(1)).fold(Duration::ZERO, |_, _| take(bucket, rate, now))
    };
    if !wait.is_zero() {
        tokio::time::sleep(wait).await;
    }
}

/// The `Retry-After` seconds of an HTTP error answer; its headers are part of the message.
pub(super) fn retry_after(message: &str) -> Option<Duration> {
    let lower = message.to_ascii_lowercase();
    let at = lower.find("\"retry-after\": \"")? + "\"retry-after\": \"".len();
    let digits: String = lower[at..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    digits.parse().ok().map(Duration::from_secs)
}

fn backoff(attempt: u32) -> Duration {
    let base = BACKOFF
        .saturating_mul(1 << attempt.min(16))
        .min(MAX_BACKOFF);
    let jitter = rand::rng().random_range(0..=base.as_millis() as u64 / 2);
    base + Duration::from_millis(jitter)
}

/// Sends a source request of a table import, worth `requests` requests, within the node's rate,
/// again after a transient answer; `transient` gives a retryable failure's message.
/// Other jobs send once.
pub(super) async fn paced<T, E, R, F>(
    (cancel, shutdown): (&CancellationToken, &CancellationToken),
    (spec, requests): (&StagingJobSpec, u32),
    transient: impl Fn(&E) -> Option<String>,
    mut request: R,
) -> Result<T, E>
where
    R: FnMut() -> F,
    F: Future<Output = Result<T, E>>,
{
    let Some(table) = spec.table.as_ref() else {
        return request().await;
    };
    let mut attempt = 0;
    loop {
        acquire(spec.connector_id, table.rate(), requests).await;
        let result = request().await;
        let Err(error) = &result else {
            return result;
        };
        let Some(message) = transient(error) else {
            return result;
        };
        attempt += 1;
        let wait = retry_after(&message).unwrap_or_else(|| backoff(attempt));
        if attempt >= ATTEMPTS || wait > MAX_RETRY_AFTER {
            return result;
        }
        tokio::select! {
            _ = cancel.cancelled() => return result,
            _ = shutdown.cancelled() => return result,
            _ = tokio::time::sleep(wait) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::execution::table::{TableImport, TableSource};

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

    fn spec(rate: u32) -> StagingJobSpec {
        let mut spec = crate::jobs::staging::pure_tests::test_spec();
        spec.connector_id = Ulid::from_bytes(rand::random());
        let rows = serde_json::json!({ "kind": "file", "source": "{a}", "target": "{a}" });
        spec.table = Some(TableImport {
            source: TableSource::Connector("list.csv".to_string()),
            format: Default::default(),
            rows: serde_json::from_value(rows).unwrap(),
            rate: Some(rate),
        });
        spec
    }

    // A counting fake source sees at most the rate, retries included, and the retry waits
    // for the answer's `Retry-After`.
    #[tokio::test(start_paused = true)]
    async fn retries_count_too() {
        let spec = spec(10);
        let stop = (CancellationToken::new(), CancellationToken::new());
        let start = Instant::now();
        let sent = std::cell::RefCell::new(Vec::new());
        for index in 0..20 {
            let result: Result<(), String> = paced(
                (&stop.0, &stop.1),
                (&spec, 1),
                |e: &String| Some(e.clone()),
                || {
                    sent.borrow_mut().push(Instant::now() - start);
                    let throttled = index == 5 && sent.borrow().len() == 6;
                    let answer = match throttled {
                        true => Err("{\"retry-after\": \"3\"}".to_string()),
                        false => Ok(()),
                    };
                    std::future::ready(answer)
                },
            )
            .await;
            assert!(result.is_ok());
        }
        let sent = sent.into_inner();
        assert_eq!(sent.len(), 21);
        for window in sent.windows(11) {
            assert!(window[10] - window[0] >= Duration::from_secs(1));
        }
        assert!(sent[6] - sent[5] >= Duration::from_secs(3));
        // A lasting error is returned at once.
        let lasting: Result<(), String> = paced(
            (&stop.0, &stop.1),
            (&spec, 1),
            |_| None,
            || std::future::ready(Err("gone".to_string())),
        )
        .await;
        assert_eq!(lasting, Err("gone".to_string()));
    }

    #[test]
    fn retry_after_read() {
        let message = "Stat error: RateLimited at stat, context: { response: Parts { status: \
            429, headers: {\"content-type\": \"text/html\", \"retry-after\": \"7\"} } }";
        assert_eq!(retry_after(message), Some(Duration::from_secs(7)));
        assert_eq!(retry_after("Stat error: reset"), None);
    }
}
