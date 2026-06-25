//! Lightweight observability primitives shared across crates.
//!
//! - [`LatencyAggregator`]: unbiased per-key latency histograms flushed as
//!   rate-limited `latency.summary` INFO lines.
//! - [`RequestStages`]: a task-local per-request stage timing context used by
//!   the API middleware to emit `request.slow` WARN breakdowns.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tracing::{Span, info};

/// Default flush interval for latency summaries.
pub const LATENCY_SUMMARY_INTERVAL: Duration = Duration::from_secs(30);
/// Default tick interval for queue lag gauges.
pub const QUEUE_LAG_INTERVAL: Duration = Duration::from_secs(10);

// 1-2-5 series bucket upper bounds in microseconds (50us .. 10min) plus an
// implicit overflow bucket. Percentiles report a bucket upper bound capped by
// the exact observed maximum, so the error is bounded by the bucket width.
const BUCKET_BOUNDS_US: [u64; 22] = [
    50,
    100,
    200,
    500,
    1_000,
    2_000,
    5_000,
    10_000,
    20_000,
    50_000,
    100_000,
    200_000,
    500_000,
    1_000_000,
    2_000_000,
    5_000_000,
    10_000_000,
    30_000_000,
    60_000_000,
    120_000_000,
    300_000_000,
    600_000_000,
];
const BUCKET_COUNT: usize = BUCKET_BOUNDS_US.len() + 1;

pub fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

pub fn record_duration_ms(span: &Span, field: &'static str, duration: Duration) {
    span.record(field, duration_ms(duration));
}

pub fn record_elapsed_ms(span: &Span, field: &'static str, started: Instant) -> Duration {
    let duration = started.elapsed();
    record_duration_ms(span, field, duration);
    duration
}

fn us_to_ms(us: u64) -> f64 {
    us as f64 / 1_000.0
}

#[derive(Clone, Debug)]
pub struct LatencyHistogram {
    buckets: [u64; BUCKET_COUNT],
    count: u64,
    sum_us: u64,
    max_us: u64,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self {
            buckets: [0; BUCKET_COUNT],
            count: 0,
            sum_us: 0,
            max_us: 0,
        }
    }
}

impl LatencyHistogram {
    pub fn record(&mut self, duration: Duration) {
        let us = duration.as_micros().min(u128::from(u64::MAX)) as u64;
        let index = BUCKET_BOUNDS_US
            .iter()
            .position(|bound| us <= *bound)
            .unwrap_or(BUCKET_BOUNDS_US.len());
        self.buckets[index] += 1;
        self.count += 1;
        self.sum_us = self.sum_us.saturating_add(us);
        self.max_us = self.max_us.max(us);
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn max_ms(&self) -> f64 {
        us_to_ms(self.max_us)
    }

    pub fn mean_ms(&self) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        us_to_ms(self.sum_us) / self.count as f64
    }

    /// Approximate percentile in milliseconds: the upper bound of the bucket
    /// containing the requested rank, capped by the exact observed maximum.
    pub fn percentile_ms(&self, quantile: f64) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        let rank = ((quantile * self.count as f64).ceil() as u64).clamp(1, self.count);
        let mut seen = 0u64;
        for (index, bucket) in self.buckets.iter().enumerate() {
            seen += bucket;
            if seen >= rank {
                if index < BUCKET_BOUNDS_US.len() {
                    return us_to_ms(BUCKET_BOUNDS_US[index].min(self.max_us));
                }
                return us_to_ms(self.max_us);
            }
        }
        us_to_ms(self.max_us)
    }
}

#[derive(Clone, Debug)]
pub struct LatencySplitSummary {
    pub wait_p50_ms: f64,
    pub wait_p99_ms: f64,
    pub wait_max_ms: f64,
    pub service_p50_ms: f64,
    pub service_p99_ms: f64,
    pub service_max_ms: f64,
}

#[derive(Clone, Debug)]
pub struct LatencySummary {
    pub key: String,
    pub count: u64,
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
    pub mean_ms: f64,
    pub split: Option<LatencySplitSummary>,
    pub window_ms: u64,
}

#[derive(Clone, Debug, Default)]
struct LatencyEntry {
    total: LatencyHistogram,
    wait: LatencyHistogram,
    service: LatencyHistogram,
}

#[derive(Debug, Default)]
struct AggregatorInner {
    window_started: Option<Instant>,
    entries: HashMap<String, LatencyEntry>,
}

/// Records every observation into per-key histograms and emits one
/// `latency.summary` INFO line per key roughly every `interval`, then resets.
/// Recording is a mutex-guarded bucket increment; nothing runs while idle.
#[derive(Debug)]
pub struct LatencyAggregator {
    scope: &'static str,
    interval: Duration,
    inner: Mutex<AggregatorInner>,
}

impl LatencyAggregator {
    pub fn new(scope: &'static str) -> Self {
        Self::with_interval(scope, LATENCY_SUMMARY_INTERVAL)
    }

    pub fn with_interval(scope: &'static str, interval: Duration) -> Self {
        Self {
            scope,
            interval,
            inner: Mutex::new(AggregatorInner::default()),
        }
    }

    pub fn record(&self, key: &str, total: Duration) {
        self.observe(key, total, None);
    }

    pub fn record_split(&self, key: &str, queue_wait: Duration, service: Duration) {
        self.observe(
            key,
            queue_wait.saturating_add(service),
            Some((queue_wait, service)),
        );
    }

    fn observe(&self, key: &str, total: Duration, split: Option<(Duration, Duration)>) {
        let due = {
            let mut inner = self.inner.lock().unwrap_or_else(|lock| lock.into_inner());
            let now = Instant::now();
            let window_started = *inner.window_started.get_or_insert(now);
            let entry = match inner.entries.get_mut(key) {
                Some(entry) => entry,
                None => inner.entries.entry(key.to_string()).or_default(),
            };
            entry.total.record(total);
            if let Some((wait, service)) = split {
                entry.wait.record(wait);
                entry.service.record(service);
            }
            if now.duration_since(window_started) >= self.interval {
                Some(Self::drain_locked(&mut inner, now))
            } else {
                None
            }
        };
        if let Some(summaries) = due {
            self.emit(&summaries);
        }
    }

    /// Drains and emits the current window regardless of the interval.
    pub fn flush(&self) -> Vec<LatencySummary> {
        let summaries = {
            let mut inner = self.inner.lock().unwrap_or_else(|lock| lock.into_inner());
            Self::drain_locked(&mut inner, Instant::now())
        };
        self.emit(&summaries);
        summaries
    }

    fn drain_locked(inner: &mut AggregatorInner, now: Instant) -> Vec<LatencySummary> {
        let window_ms = inner
            .window_started
            .take()
            .map(|started| duration_ms(now.duration_since(started)))
            .unwrap_or(0);
        let mut summaries: Vec<LatencySummary> = inner
            .entries
            .drain()
            .filter(|(_, entry)| entry.total.count() > 0)
            .map(|(key, entry)| LatencySummary {
                key,
                count: entry.total.count(),
                p50_ms: entry.total.percentile_ms(0.50),
                p90_ms: entry.total.percentile_ms(0.90),
                p99_ms: entry.total.percentile_ms(0.99),
                max_ms: entry.total.max_ms(),
                mean_ms: entry.total.mean_ms(),
                split: (entry.wait.count() > 0).then(|| LatencySplitSummary {
                    wait_p50_ms: entry.wait.percentile_ms(0.50),
                    wait_p99_ms: entry.wait.percentile_ms(0.99),
                    wait_max_ms: entry.wait.max_ms(),
                    service_p50_ms: entry.service.percentile_ms(0.50),
                    service_p99_ms: entry.service.percentile_ms(0.99),
                    service_max_ms: entry.service.max_ms(),
                }),
                window_ms,
            })
            .collect();
        summaries.sort_by(|left, right| left.key.cmp(&right.key));
        summaries
    }

    fn emit(&self, summaries: &[LatencySummary]) {
        for summary in summaries {
            match &summary.split {
                Some(split) => info!(
                    event = "latency.summary",
                    scope = self.scope,
                    key = %summary.key,
                    count = summary.count,
                    p50_ms = summary.p50_ms,
                    p90_ms = summary.p90_ms,
                    p99_ms = summary.p99_ms,
                    max_ms = summary.max_ms,
                    mean_ms = summary.mean_ms,
                    wait_p50_ms = split.wait_p50_ms,
                    wait_p99_ms = split.wait_p99_ms,
                    wait_max_ms = split.wait_max_ms,
                    service_p50_ms = split.service_p50_ms,
                    service_p99_ms = split.service_p99_ms,
                    service_max_ms = split.service_max_ms,
                    window_ms = summary.window_ms,
                    "Latency summary"
                ),
                None => info!(
                    event = "latency.summary",
                    scope = self.scope,
                    key = %summary.key,
                    count = summary.count,
                    p50_ms = summary.p50_ms,
                    p90_ms = summary.p90_ms,
                    p99_ms = summary.p99_ms,
                    max_ms = summary.max_ms,
                    mean_ms = summary.mean_ms,
                    window_ms = summary.window_ms,
                    "Latency summary"
                ),
            }
        }
    }
}

tokio::task_local! {
    static REQUEST_STAGES: RequestStages;
}

const STAGE_DETAIL_LIMIT: usize = 32;

#[derive(Debug)]
struct StageEntry {
    name: &'static str,
    count: u64,
    total: Duration,
}

#[derive(Debug)]
struct StageDetail {
    name: &'static str,
    detail: String,
    elapsed: Duration,
}

#[derive(Debug, Default)]
struct StageData {
    stages: Vec<StageEntry>,
    details: Vec<StageDetail>,
}

/// Per-request stage timing context. Cloning shares the same data; the API
/// middleware installs one per request as a tokio task-local so any code on
/// the request task can attribute time via [`record_stage`] without plumbing.
#[derive(Clone, Debug, Default)]
pub struct RequestStages {
    inner: Arc<Mutex<StageData>>,
}

impl RequestStages {
    pub fn add(&self, name: &'static str, elapsed: Duration) {
        let mut data = self.inner.lock().unwrap_or_else(|lock| lock.into_inner());
        match data.stages.iter_mut().find(|stage| stage.name == name) {
            Some(stage) => {
                stage.count += 1;
                stage.total = stage.total.saturating_add(elapsed);
            }
            None => data.stages.push(StageEntry {
                name,
                count: 1,
                total: elapsed,
            }),
        }
    }

    pub fn add_detail(&self, name: &'static str, detail: String, elapsed: Duration) {
        let mut data = self.inner.lock().unwrap_or_else(|lock| lock.into_inner());
        if data.details.len() < STAGE_DETAIL_LIMIT {
            data.details.push(StageDetail {
                name,
                detail,
                elapsed,
            });
        }
    }

    pub fn is_empty(&self) -> bool {
        let data = self.inner.lock().unwrap_or_else(|lock| lock.into_inner());
        data.stages.is_empty() && data.details.is_empty()
    }

    /// Renders `stage=total_ms/count` pairs plus `stage[detail]=ms` entries
    /// into one grep-friendly field value. Stages may overlap (for example
    /// `craqle` time inside `execute`), so the sum can exceed the total.
    pub fn render(&self) -> String {
        let data = self.inner.lock().unwrap_or_else(|lock| lock.into_inner());
        let mut parts = Vec::with_capacity(data.stages.len() + data.details.len());
        for stage in &data.stages {
            let ms = stage.total.as_secs_f64() * 1_000.0;
            if stage.count == 1 {
                parts.push(format!("{}={ms:.1}ms", stage.name));
            } else {
                parts.push(format!("{}={ms:.1}ms/{}", stage.name, stage.count));
            }
        }
        for detail in &data.details {
            let ms = detail.elapsed.as_secs_f64() * 1_000.0;
            parts.push(format!("{}[{}]={ms:.1}ms", detail.name, detail.detail));
        }
        parts.join(";")
    }

    /// Runs `future` with this context installed as the task-local stage sink.
    pub async fn scope<F: Future>(self, future: F) -> F::Output {
        REQUEST_STAGES.scope(self, future).await
    }
}

/// Adds `elapsed` to the named stage of the current request, if any. Cheap
/// no-op outside a request scope.
pub fn record_stage(name: &'static str, elapsed: Duration) {
    let _ = REQUEST_STAGES.try_with(|stages| stages.add(name, elapsed));
}

/// Adds a per-item detail line (for example one fan-out peer); the detail
/// string is only built when a request scope is active.
pub fn record_stage_detail(name: &'static str, detail: impl FnOnce() -> String, elapsed: Duration) {
    let _ = REQUEST_STAGES.try_with(|stages| stages.add_detail(name, detail(), elapsed));
}

/// Awaits `future` and attributes its wall time to the named stage.
pub async fn time_stage<F: Future>(name: &'static str, future: F) -> F::Output {
    let started = Instant::now();
    let output = future.await;
    record_stage(name, started.elapsed());
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_ms_saturates_at_u64_max() {
        assert_eq!(duration_ms(Duration::from_millis(42)), 42);
        assert_eq!(duration_ms(Duration::from_secs(u64::MAX)), u64::MAX);
    }

    #[test]
    fn histogram_percentiles_use_bucket_bounds_capped_by_max() {
        let mut histogram = LatencyHistogram::default();
        for _ in 0..99 {
            histogram.record(Duration::from_micros(900));
        }
        histogram.record(Duration::from_millis(400));

        assert_eq!(histogram.count(), 100);
        // 900us falls into the 1ms bucket; p50/p90 report its upper bound.
        assert_eq!(histogram.percentile_ms(0.50), 1.0);
        assert_eq!(histogram.percentile_ms(0.90), 1.0);
        // Rank 100 lands on the 400ms sample in the 500ms bucket, capped by max.
        assert_eq!(histogram.percentile_ms(1.0), 400.0);
        assert_eq!(histogram.max_ms(), 400.0);
    }

    #[test]
    fn histogram_single_sample_percentile_is_capped_by_observed_max() {
        let mut histogram = LatencyHistogram::default();
        histogram.record(Duration::from_micros(300));
        assert_eq!(histogram.percentile_ms(0.50), 0.3);
        assert_eq!(histogram.percentile_ms(0.99), 0.3);
    }

    #[test]
    fn histogram_overflow_bucket_reports_exact_max() {
        let mut histogram = LatencyHistogram::default();
        histogram.record(Duration::from_secs(1_000));
        assert_eq!(histogram.percentile_ms(0.99), 1_000_000.0);
    }

    #[test]
    fn histogram_empty_reports_zero() {
        let histogram = LatencyHistogram::default();
        assert_eq!(histogram.count(), 0);
        assert_eq!(histogram.percentile_ms(0.5), 0.0);
        assert_eq!(histogram.mean_ms(), 0.0);
        assert_eq!(histogram.max_ms(), 0.0);
    }

    #[test]
    fn aggregator_flush_resets_window() {
        let aggregator = LatencyAggregator::new("test");
        aggregator.record("route_a", Duration::from_millis(5));
        aggregator.record("route_a", Duration::from_millis(5));
        aggregator.record("route_b", Duration::from_millis(50));

        let summaries = aggregator.flush();
        assert_eq!(summaries.len(), 2);
        let route_a = summaries
            .iter()
            .find(|summary| summary.key == "route_a")
            .expect("route_a summary");
        assert_eq!(route_a.count, 2);
        assert_eq!(route_a.p50_ms, 5.0);
        assert!(route_a.split.is_none());

        assert!(aggregator.flush().is_empty(), "window must reset on flush");
    }

    #[test]
    fn aggregator_split_reports_wait_and_service() {
        let aggregator = LatencyAggregator::new("test");
        aggregator.record_split("write", Duration::from_millis(40), Duration::from_millis(2));
        let summaries = aggregator.flush();
        let split = summaries[0].split.as_ref().expect("split summary");
        assert_eq!(split.wait_max_ms, 40.0);
        assert_eq!(split.service_max_ms, 2.0);
        assert_eq!(summaries[0].max_ms, 42.0);
    }

    #[test]
    fn aggregator_emits_inline_once_interval_elapses() {
        let aggregator = LatencyAggregator::with_interval("test", Duration::ZERO);
        // Window opens and immediately becomes due on the second record.
        aggregator.record("k", Duration::from_millis(1));
        aggregator.record("k", Duration::from_millis(1));
        assert!(aggregator.flush().is_empty());
    }

    #[test]
    fn request_stages_aggregate_and_render() {
        let stages = RequestStages::default();
        stages.add("storage", Duration::from_millis(10));
        stages.add("storage", Duration::from_millis(20));
        stages.add("auth", Duration::from_micros(1_500));
        stages.add_detail("fanout", "ab12cd34".to_string(), Duration::from_millis(873));

        let rendered = stages.render();
        assert!(rendered.contains("storage=30.0ms/2"), "{rendered}");
        assert!(rendered.contains("auth=1.5ms"), "{rendered}");
        assert!(rendered.contains("fanout[ab12cd34]=873.0ms"), "{rendered}");
    }

    #[tokio::test]
    async fn record_stage_is_noop_without_scope_and_records_in_scope() {
        record_stage("orphan", Duration::from_millis(1));

        let stages = RequestStages::default();
        stages
            .clone()
            .scope(async {
                time_stage("inner", async {}).await;
                record_stage("manual", Duration::from_millis(3));
            })
            .await;
        let rendered = stages.render();
        assert!(rendered.contains("inner="), "{rendered}");
        assert!(rendered.contains("manual=3.0ms"), "{rendered}");
        assert!(!rendered.contains("orphan"), "{rendered}");
    }
}
