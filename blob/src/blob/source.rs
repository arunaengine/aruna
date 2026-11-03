//! Runs staging source effects: check, head, read and list against a resolved source access.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex as StdMutex};
use std::task::{Context, Poll, ready};
use std::time::{Duration, Instant};

use super::BlobHandler;
use crate::opendal::{
    check_staging_source, head_staging_source, list_staging_source, read_staging_source,
};
use aruna_core::errors::StagingSourceError;
use aruna_core::events::StagingSourceEvent;
use aruna_core::stream::{BackendStream, BoxStream, StreamError};
use aruna_core::structs::execution::source_access::{
    ListRequest, ResolvedSourceAccess, SOURCE_WEIGHT, SourceMetadata, SourceStats,
};
use bytes::Bytes;
use futures::Stream;

/// How long a source that failed counts as failing unless it answers again.
const FAILING_FOR: Duration = Duration::from_secs(60);
/// Weight of the newest sample in the decaying averages.
const SAMPLE_WEIGHT: f64 = 0.2;
/// The error rate halves over this time while no request fails.
const ERROR_HALF_LIFE: Duration = Duration::from_secs(60);
/// A source unused this long is forgotten, so it is measured afresh.
const FORGET_AFTER: Duration = Duration::from_secs(600);
/// Reads moving fewer bytes measure latency only, as their rate is mostly noise.
const RATE_MIN_BYTES: u64 = 256 * 1024;

/// This node's measurements of each source, named by a digest of its kind and settings.
#[derive(Debug, Default)]
pub(crate) struct SourceTable {
    sources: HashMap<[u8; 32], Measured>,
}

#[derive(Debug)]
struct Measured {
    first_byte_ms: Option<f64>,
    bytes_per_sec: Option<f64>,
    /// The error rate as of `rated_at`.
    error_rate: f64,
    rated_at: Instant,
    in_flight: u32,
    failed_at: Option<Instant>,
    used_at: Instant,
}

impl Measured {
    fn new(now: Instant) -> Self {
        Self {
            first_byte_ms: None,
            bytes_per_sec: None,
            error_rate: 0.0,
            rated_at: now,
            in_flight: 0,
            failed_at: None,
            used_at: now,
        }
    }

    fn error_rate(&self, now: Instant) -> f64 {
        let age = now.saturating_duration_since(self.rated_at).as_secs_f64();
        self.error_rate * 0.5f64.powf(age / ERROR_HALF_LIFE.as_secs_f64())
    }

    fn kept(&self, now: Instant) -> bool {
        self.in_flight > 0 || now.saturating_duration_since(self.used_at) < FORGET_AFTER
    }
}

fn blend(average: Option<f64>, sample: f64) -> f64 {
    average.map_or(sample, |average| {
        average + SAMPLE_WEIGHT * (sample - average)
    })
}

impl SourceTable {
    fn entry(&mut self, key: [u8; 32], now: Instant) -> &mut Measured {
        self.sources.retain(|_, measured| measured.kept(now));
        let measured = (self.sources.entry(key)).or_insert_with(|| Measured::new(now));
        measured.used_at = now;
        measured
    }

    /// Records whether the server answered; an error of one file or of the access is neither.
    fn answered(&mut self, key: [u8; 32], error: Option<&StagingSourceError>, now: Instant) {
        match error {
            None => self.outcome(key, false, now),
            Some(error) if unavailable(error) => self.outcome(key, true, now),
            Some(_) => {}
        }
    }

    fn outcome(&mut self, key: [u8; 32], failed: bool, now: Instant) {
        let measured = self.entry(key, now);
        let rate = measured.error_rate(now);
        measured.error_rate = rate + SAMPLE_WEIGHT * (f64::from(u8::from(failed)) - rate);
        measured.rated_at = now;
        measured.failed_at = failed.then_some(now);
    }

    fn first_byte(&mut self, key: [u8; 32], waited: Duration, now: Instant) {
        let measured = self.entry(key, now);
        let sample = waited.as_secs_f64() * 1000.0;
        measured.first_byte_ms = Some(blend(measured.first_byte_ms, sample));
    }

    fn started(&mut self, key: [u8; 32], now: Instant) {
        self.entry(key, now).in_flight += 1;
    }

    /// Ends a read that moved `bytes` while waiting `waited` on the source after its first byte.
    fn ended(&mut self, key: [u8; 32], (bytes, waited): (u64, Duration), now: Instant) {
        let measured = self.entry(key, now);
        measured.in_flight = measured.in_flight.saturating_sub(1);
        if bytes >= RATE_MIN_BYTES && !waited.is_zero() {
            let sample = bytes as f64 / waited.as_secs_f64();
            measured.bytes_per_sec = Some(blend(measured.bytes_per_sec, sample));
        }
    }

    fn stats(&self, key: &[u8; 32], now: Instant) -> SourceStats {
        let Some(measured) = self.sources.get(key).filter(|m| m.kept(now)) else {
            return SourceStats::default();
        };
        let failed_at = measured.failed_at;
        SourceStats {
            first_byte_ms: measured.first_byte_ms,
            bytes_per_sec: measured.bytes_per_sec,
            error_rate: measured.error_rate(now),
            in_flight: measured.in_flight,
            failing: failed_at.is_some_and(|at| now.saturating_duration_since(at) < FAILING_FOR),
        }
    }
}

/// One read in flight, counted from before its request until it ends, fails or is dropped, so
/// a read cancelled while waiting for its answer is no longer counted either.
struct ReadGuard {
    table: Arc<StdMutex<SourceTable>>,
    key: [u8; 32],
    bytes: u64,
    /// Time waited on the source after the first byte.
    waited: Duration,
    ended: bool,
}

impl ReadGuard {
    fn start(table: &Arc<StdMutex<SourceTable>>, key: [u8; 32], now: Instant) -> Self {
        if let Ok(mut table) = table.lock() {
            table.started(key, now);
        }
        Self {
            table: table.clone(),
            key,
            bytes: 0,
            waited: Duration::ZERO,
            ended: false,
        }
    }

    fn end(&mut self, failed: bool, now: Instant) {
        if std::mem::replace(&mut self.ended, true) {
            return;
        }
        if let Ok(mut table) = self.table.lock() {
            table.ended(self.key, (self.bytes, self.waited), now);
            if failed {
                table.outcome(self.key, true, now);
            }
        }
    }
}

impl Drop for ReadGuard {
    fn drop(&mut self) {
        self.end(false, Instant::now());
    }
}

/// Passes a read's bytes through and tells the table how long the source kept it waiting.
struct Metered {
    inner: BoxStream<'static, Result<Bytes, StreamError>>,
    guard: ReadGuard,
    started: Instant,
    /// Since when the reader waits on the source for the next chunk.
    polled_at: Option<Instant>,
    first: bool,
}

impl Stream for Metered {
    type Item = Result<Bytes, StreamError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        let polled_at = *this.polled_at.get_or_insert_with(Instant::now);
        let item = ready!(this.inner.as_mut().poll_next(cx));
        let now = Instant::now();
        this.polled_at = None;
        let guard = &mut this.guard;
        match &item {
            Some(Ok(chunk)) if this.first => {
                this.first = false;
                guard.bytes += chunk.len() as u64;
                let waited = now.saturating_duration_since(this.started);
                if let Ok(mut table) = guard.table.lock() {
                    table.first_byte(guard.key, waited, now);
                }
            }
            Some(Ok(chunk)) => {
                guard.bytes += chunk.len() as u64;
                guard.waited += now.saturating_duration_since(polled_at);
            }
            Some(Err(_)) => guard.end(true, now),
            None => guard.end(false, now),
        }
        Poll::Ready(item)
    }
}

/// The source without its path: one server and its settings, never logged.
fn source_key(access: &ResolvedSourceAccess) -> [u8; 32] {
    let ResolvedSourceAccess::OpenDal { kind, config, .. } = access;
    // The weight is an operator hint, not a setting of the server.
    let mut settings: Vec<_> = config.iter().filter(|(k, _)| *k != SOURCE_WEIGHT).collect();
    settings.sort();
    let mut hasher = blake3::Hasher::new();
    hasher.update(kind.to_string().as_bytes());
    for (key, value) in settings {
        for part in [key, value] {
            hasher.update(&(part.len() as u64).to_le_bytes());
            hasher.update(part.as_bytes());
        }
    }
    *hasher.finalize().as_bytes()
}

/// Errors of the server rather than of one file or of the caller's access.
fn unavailable(error: &StagingSourceError) -> bool {
    matches!(
        error,
        StagingSourceError::OperatorCreationFailed(_)
            | StagingSourceError::CheckError(_)
            | StagingSourceError::StatError(_)
            | StagingSourceError::ListError(_)
            | StagingSourceError::ReadError(_)
    )
}

impl BlobHandler {
    pub(crate) async fn check_staging_source(
        &self,
        access: ResolvedSourceAccess,
    ) -> StagingSourceEvent {
        match check_staging_source(&self.egress, &access).await {
            Ok(()) => StagingSourceEvent::CheckResult,
            Err(error) => StagingSourceEvent::Error { error },
        }
    }

    pub(crate) async fn head_staging_source(
        &self,
        access: ResolvedSourceAccess,
    ) -> StagingSourceEvent {
        let key = source_key(&access);
        let started = Instant::now();
        let result = head_staging_source(&self.egress, &access).await;
        let now = Instant::now();
        self.with_table(|table| {
            if result.is_ok() {
                table.first_byte(key, now.saturating_duration_since(started), now);
            }
            table.answered(key, result.as_ref().err(), now);
        });
        match result {
            Ok(metadata) => StagingSourceEvent::HeadResult { metadata },
            Err(error) => StagingSourceEvent::Error { error },
        }
    }

    pub(crate) async fn read_staging_source(
        &self,
        access: ResolvedSourceAccess,
        range: Option<std::ops::Range<u64>>,
        expected: Option<SourceMetadata>,
    ) -> StagingSourceEvent {
        let key = source_key(&access);
        let started = Instant::now();
        let guard = ReadGuard::start(&self.source_stats, key, started);
        let result = read_staging_source(&self.egress, &access, range, expected.as_ref()).await;
        let now = Instant::now();
        self.with_table(|table| table.answered(key, result.as_ref().err(), now));
        match result {
            Ok((metadata, stream)) => {
                let stream = BackendStream(Box::pin(Metered {
                    inner: stream.0,
                    guard,
                    started,
                    polled_at: None,
                    first: true,
                }));
                StagingSourceEvent::ReadResult { metadata, stream }
            }
            Err(error) => StagingSourceEvent::Error { error },
        }
    }

    /// This node's measurements of `accesses`, in their order, and a random draw.
    pub(crate) fn source_health(&self, accesses: &[ResolvedSourceAccess]) -> StagingSourceEvent {
        let now = Instant::now();
        let stats = match self.source_stats.lock() {
            Ok(table) => accesses
                .iter()
                .map(|access| table.stats(&source_key(access), now))
                .collect(),
            Err(_) => vec![SourceStats::default(); accesses.len()],
        };
        let draw = getrandom::u64().unwrap_or_default();
        StagingSourceEvent::HealthResult { stats, draw }
    }

    fn with_table(&self, update: impl FnOnce(&mut SourceTable)) {
        if let Ok(mut table) = self.source_stats.lock() {
            update(&mut table);
        }
    }

    pub(crate) async fn list_staging_source(
        &self,
        access: ResolvedSourceAccess,
        request: ListRequest,
    ) -> StagingSourceEvent {
        match list_staging_source(&self.egress, &access, &request).await {
            Ok((entries, next_cursor)) => StagingSourceEvent::ListResult {
                entries,
                next_cursor,
            },
            Err(error) => StagingSourceEvent::Error { error },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::execution::source_connector::SourceConnectorKind;

    fn access(endpoint: &str, path: &str) -> ResolvedSourceAccess {
        ResolvedSourceAccess::OpenDal {
            kind: SourceConnectorKind::Http,
            config: [("endpoint".to_string(), endpoint.to_string())].into(),
            path: path.to_string(),
            version: None,
        }
    }

    fn key(endpoint: &str) -> [u8; 32] {
        source_key(&access(endpoint, "x"))
    }

    #[test]
    fn failures_follow_servers() {
        // A server failure marks every path of that server; a missing file marks nothing.
        let mut table = SourceTable::default();
        let now = Instant::now();
        let error = StagingSourceError::StatError("timed out".to_string());
        table.answered(key("https://a"), Some(&error), now);
        assert!(
            table
                .stats(&source_key(&access("https://a", "y")), now)
                .failing
        );
        assert!(!table.stats(&key("https://b"), now).failing);
        table.answered(key("https://b"), Some(&StagingSourceError::NotFound), now);
        assert!(!table.stats(&key("https://b"), now).failing);
        table.answered(source_key(&access("https://a", "z")), None, now);
        assert!(!table.stats(&key("https://a"), now).failing);
    }

    #[test]
    fn weight_keeps_server() {
        let mut weighted = access("https://a", "x");
        let ResolvedSourceAccess::OpenDal { config, .. } = &mut weighted;
        config.insert(SOURCE_WEIGHT.to_string(), "3".to_string());
        assert_eq!(source_key(&weighted), key("https://a"));
    }

    #[test]
    fn errors_fade_out() {
        // Errors stop counting as failing after a minute and their rate halves each minute.
        let mut table = SourceTable::default();
        let start = Instant::now();
        let error = StagingSourceError::ReadError("reset".to_string());
        for _ in 0..5 {
            table.answered(key("https://a"), Some(&error), start);
        }
        let fresh = table.stats(&key("https://a"), start);
        assert!(fresh.failing && fresh.error_rate > 0.6);
        let later = table.stats(&key("https://a"), start + Duration::from_secs(120));
        assert!(!later.failing);
        assert!((later.error_rate - fresh.error_rate / 4.0).abs() < 1e-9);
        table.answered(key("https://a"), None, start + Duration::from_secs(120));
        let recovered = table.stats(&key("https://a"), start + Duration::from_secs(120));
        assert!(recovered.error_rate < later.error_rate);
    }

    #[test]
    fn averages_decay_out() {
        let mut table = SourceTable::default();
        let start = Instant::now();
        let a = key("https://a");
        table.first_byte(a, Duration::from_millis(100), start);
        table.first_byte(a, Duration::from_millis(200), start);
        table.started(a, start);
        table.ended(a, (RATE_MIN_BYTES, Duration::from_secs(1)), start);
        table.started(a, start);
        // A short read measures no rate.
        table.ended(a, (RATE_MIN_BYTES - 1, Duration::from_millis(1)), start);
        let stats = table.stats(&a, start);
        assert_eq!(stats.first_byte_ms, Some(120.0));
        assert_eq!(stats.bytes_per_sec, Some(RATE_MIN_BYTES as f64));
        assert_eq!(stats.in_flight, 0);
        let idle = start + FORGET_AFTER;
        assert_eq!(table.stats(&a, idle), SourceStats::default());
    }

    fn metered(
        table: &Arc<StdMutex<SourceTable>>,
        chunks: Vec<Result<Bytes, StreamError>>,
    ) -> Metered {
        Metered {
            inner: Box::pin(futures::stream::iter(chunks)),
            guard: ReadGuard::start(table, key("https://a"), Instant::now()),
            started: Instant::now(),
            polled_at: None,
            first: true,
        }
    }

    fn in_flight(table: &Arc<StdMutex<SourceTable>>) -> u32 {
        table
            .lock()
            .unwrap()
            .stats(&key("https://a"), Instant::now())
            .in_flight
    }

    #[tokio::test]
    async fn metered_reads_end() {
        use futures::StreamExt;
        let table = Arc::new(StdMutex::new(SourceTable::default()));
        let chunk = || Ok(Bytes::from_static(b"abc"));
        let mut whole = metered(&table, vec![chunk(), chunk()]);
        let mut cut = metered(&table, vec![chunk(), chunk()]);
        assert_eq!(in_flight(&table), 2);
        while whole.next().await.is_some() {}
        assert_eq!((whole.guard.bytes, in_flight(&table)), (6, 1));
        assert!(cut.next().await.is_some());
        // A reader that stops early ends the read without blaming the source.
        drop(cut);
        drop(whole);
        let stats = table
            .lock()
            .unwrap()
            .stats(&key("https://a"), Instant::now());
        assert_eq!((stats.in_flight, stats.failing), (0, false));
        assert!(stats.first_byte_ms.is_some());
        let broken = std::io::Error::other("reset");
        let mut failed = metered(&table, vec![chunk(), Err(StreamError(Box::new(broken)))]);
        while failed.next().await.is_some() {}
        assert!(
            table
                .lock()
                .unwrap()
                .stats(&key("https://a"), Instant::now())
                .failing
        );
    }

    #[tokio::test]
    async fn cancelled_read_ends() {
        // A read dropped while it still waits for the source's answer stops counting at once.
        let table = Arc::new(StdMutex::new(SourceTable::default()));
        let waiting = async {
            let _guard = ReadGuard::start(&table, key("https://a"), Instant::now());
            std::future::pending::<()>().await;
        };
        let mut waiting = Box::pin(waiting);
        assert!(futures::poll!(waiting.as_mut()).is_pending());
        assert_eq!(in_flight(&table), 1);
        drop(waiting);
        assert_eq!(in_flight(&table), 0);
    }
}
