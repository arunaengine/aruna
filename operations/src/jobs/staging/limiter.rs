//! Sends the source requests of table imports again after transient answers, with backoff. The
//! blob adapter paces every request it sends, these included, and honours `Retry-After`.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::structs::execution::job::StagingJobSpec;
use rand::RngExt;
use tokio_util::sync::CancellationToken;

/// Attempts of one request while its answers stay transient.
const ATTEMPTS: u32 = 5;
/// The first backoff; each further attempt doubles it.
const BACKOFF: Duration = Duration::from_millis(500);
/// The longest backoff between two attempts.
const MAX_BACKOFF: Duration = Duration::from_secs(30);

fn backoff(attempt: u32) -> Duration {
    let base = BACKOFF
        .saturating_mul(1 << attempt.min(16))
        .min(MAX_BACKOFF);
    let jitter = rand::rng().random_range(0..=base.as_millis() as u64 / 2);
    base + Duration::from_millis(jitter)
}

/// Sends a source request of a table import again after a transient answer; `transient` gives
/// a retryable failure's message. Other jobs send once.
pub(super) async fn retried<T, E, R, F>(
    (cancel, shutdown): (&CancellationToken, &CancellationToken),
    spec: &StagingJobSpec,
    transient: impl Fn(&E) -> Option<String>,
    mut request: R,
) -> Result<T, E>
where
    R: FnMut() -> F,
    F: Future<Output = Result<T, E>>,
{
    if spec.table.is_none() {
        return request().await;
    }
    let mut attempt = 0;
    loop {
        let result = request().await;
        let Err(error) = &result else {
            return result;
        };
        if transient(error).is_none() {
            return result;
        }
        attempt += 1;
        if attempt >= ATTEMPTS {
            return result;
        }
        let wait = backoff(attempt);
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
    use tokio::time::Instant;

    fn spec() -> StagingJobSpec {
        let mut spec = crate::jobs::staging::pure_tests::test_spec();
        let rows = serde_json::json!({ "kind": "file", "source": "{a}", "target": "{a}" });
        spec.table = Some(TableImport {
            source: TableSource::Connector("list.csv".to_string()),
            format: Default::default(),
            rows: serde_json::from_value(rows).unwrap(),
            rate: Some(10),
        });
        spec
    }

    // A transient answer is sent again after a backoff; a lasting one returns at once.
    #[tokio::test(start_paused = true)]
    async fn transient_answers_retried() {
        let spec = spec();
        let stop = (CancellationToken::new(), CancellationToken::new());
        let start = Instant::now();
        let sent = std::cell::RefCell::new(Vec::new());
        let transient = |error: &String| Some(error.clone());
        let result: Result<(), String> = retried((&stop.0, &stop.1), &spec, transient, || {
            sent.borrow_mut().push(Instant::now() - start);
            let answer = match sent.borrow().len() {
                1 => Err("reset".to_string()),
                _ => Ok(()),
            };
            std::future::ready(answer)
        })
        .await;
        assert!(result.is_ok());
        assert!(sent.borrow()[1] - sent.borrow()[0] >= BACKOFF);
        let lasting: Result<(), String> = retried(
            (&stop.0, &stop.1),
            &spec,
            |_| None,
            || std::future::ready(Err("gone".to_string())),
        )
        .await;
        assert_eq!(lasting, Err("gone".to_string()));
    }
}
