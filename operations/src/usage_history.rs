use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{USAGE_HISTORY_KEYSPACE, USAGE_STATS_KEYSPACE};
use aruna_core::structs::{
    QuotaState, USAGE_HISTORY_GLOBAL_PREFIX, UsageHistorySample, usage_history_global_key,
    usage_history_group_key, usage_history_group_prefix,
};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::{GroupId, Key, Value};
use aruna_core::util::unix_timestamp_millis;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use tracing::warn;

use crate::driver::DriverContext;
use crate::usage_stats::{RealmUsageScope, iter_all, load_realm_usage, read_realm_config};

/// How often each node records a usage-history point.
pub const USAGE_HISTORY_SAMPLE_INTERVAL: Duration = Duration::from_secs(60 * 60);
/// How long history points are retained before the sampler prunes them.
pub const USAGE_HISTORY_RETENTION: Duration = Duration::from_secs(90 * 24 * 60 * 60);
/// Hard cap on points returned by a single history query.
pub const USAGE_HISTORY_QUERY_LIMIT: usize = 2000;

fn key_at_ms(key: &[u8]) -> Option<u64> {
    let tail = key.get(key.len().checked_sub(8)?..)?;
    Some(u64::from_be_bytes(tail.try_into().ok()?))
}

/// Samples the realm-wide usage for the global scope and every group with a local
/// counter, writing a history point when the reading changed, and prunes points
/// older than the retention window.
pub async fn record_usage_history(
    ctx: &DriverContext,
    local_node_id: NodeId,
) -> Result<(), String> {
    let now = unix_timestamp_millis();
    let cutoff = now.saturating_sub(USAGE_HISTORY_RETENTION.as_millis() as u64);
    let realm_config = read_realm_config(ctx).await?;

    let global = load_realm_usage(ctx, local_node_id, RealmUsageScope::Global).await?;
    sample_scope(
        ctx,
        USAGE_HISTORY_GLOBAL_PREFIX.to_vec(),
        usage_history_global_key(now),
        UsageHistorySample {
            at_ms: now,
            counters: global,
            quota_bytes: None,
            ceiling_bytes: None,
            state: QuotaState::Unlimited,
        },
        cutoff,
    )
    .await?;

    for group_id in local_group_counter_ids(ctx).await? {
        let counters =
            load_realm_usage(ctx, local_node_id, RealmUsageScope::Group(group_id)).await?;
        let (quota_bytes, ceiling_bytes, state) = match realm_config.as_ref() {
            Some(config) => (
                config.quota.effective_group_quota_bytes(&group_id),
                config.quota.effective_group_ceiling(&group_id),
                config
                    .quota
                    .group_quota_state(&group_id, counters.logical_bytes),
            ),
            None => (None, None, QuotaState::Unlimited),
        };
        sample_scope(
            ctx,
            usage_history_group_prefix(group_id),
            usage_history_group_key(group_id, now),
            UsageHistorySample {
                at_ms: now,
                counters,
                quota_bytes,
                ceiling_bytes,
                state,
            },
            cutoff,
        )
        .await?;
    }
    Ok(())
}

/// Writes `sample` under `key` when its reading differs from the latest point in
/// `prefix`, and deletes points older than `cutoff` in the same pass.
async fn sample_scope(
    ctx: &DriverContext,
    prefix: Vec<u8>,
    key: Vec<u8>,
    sample: UsageHistorySample,
    cutoff: u64,
) -> Result<(), String> {
    let points = iter_all(
        &ctx.storage_handle,
        USAGE_HISTORY_KEYSPACE,
        Some(Key::from(prefix)),
    )
    .await?;
    let mut deletes: Vec<(String, Key)> = Vec::new();
    let mut latest: Option<UsageHistorySample> = None;
    for (point_key, value) in &points {
        if key_at_ms(point_key.as_ref()).is_some_and(|at| at < cutoff) {
            deletes.push((USAGE_HISTORY_KEYSPACE.to_string(), point_key.clone()));
        }
        latest = Some(UsageHistorySample::from_bytes(value.as_ref()).map_err(|e| e.to_string())?);
    }

    let changed = latest
        .map(|last| !sample.same_reading(&last))
        .unwrap_or(true);
    if changed {
        let bytes = sample.to_bytes().map_err(|e| e.to_string())?;
        write_batch(
            &ctx.storage_handle,
            vec![(
                USAGE_HISTORY_KEYSPACE.to_string(),
                Key::from(key),
                Value::from(bytes),
            )],
        )
        .await?;
    }
    if !deletes.is_empty() {
        delete_batch(&ctx.storage_handle, deletes).await?;
    }
    Ok(())
}

/// Reads an ascending history slice for a scope within `[from_ms, to_ms]`, capped
/// at `limit` (itself capped at [`USAGE_HISTORY_QUERY_LIMIT`]).
pub async fn read_usage_history(
    ctx: &DriverContext,
    scope: RealmUsageScope,
    from_ms: u64,
    to_ms: u64,
    limit: usize,
) -> Result<Vec<UsageHistorySample>, String> {
    let prefix = match scope {
        RealmUsageScope::Global => USAGE_HISTORY_GLOBAL_PREFIX.to_vec(),
        RealmUsageScope::Group(group_id) => usage_history_group_prefix(group_id),
    };
    let limit = limit.min(USAGE_HISTORY_QUERY_LIMIT);
    let points = iter_all(
        &ctx.storage_handle,
        USAGE_HISTORY_KEYSPACE,
        Some(Key::from(prefix)),
    )
    .await?;
    let mut out = Vec::new();
    for (point_key, value) in points {
        let Some(at_ms) = key_at_ms(point_key.as_ref()) else {
            continue;
        };
        if at_ms < from_ms || at_ms > to_ms {
            continue;
        }
        out.push(UsageHistorySample::from_bytes(value.as_ref()).map_err(|e| e.to_string())?);
        if out.len() >= limit {
            break;
        }
    }
    Ok(out)
}

async fn local_group_counter_ids(ctx: &DriverContext) -> Result<Vec<GroupId>, String> {
    let mut ids = Vec::new();
    for (key, _) in iter_all(
        &ctx.storage_handle,
        USAGE_STATS_KEYSPACE,
        Some(Key::from(b"group/".to_vec())),
    )
    .await?
    {
        if let Some(rest) = key.as_ref().strip_prefix(b"group/")
            && let Ok(bytes) = <[u8; 16]>::try_from(rest)
        {
            ids.push(GroupId::from_bytes(bytes));
        }
    }
    Ok(ids)
}

async fn write_batch(
    storage: &StorageHandle,
    writes: Vec<(String, Key, Value)>,
) -> Result<(), String> {
    match storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected history write event: {other:?}")),
    }
}

async fn delete_batch(storage: &StorageHandle, deletes: Vec<(String, Key)>) -> Result<(), String> {
    match storage
        .send_storage_effect(StorageEffect::BatchDelete {
            deletes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected history delete event: {other:?}")),
    }
}

/// Arms the periodic history sampler. Uses `ShortenTimer` so the startup and
/// durable re-arm loops never push a handler-scheduled deadline forward.
pub async fn restore_usage_history_timer(_storage: &StorageHandle, task_handle: &TaskHandle) {
    if let Event::Task(TaskEvent::Error { message, .. }) = task_handle
        .send_effect(Effect::Task(TaskEffect::ShortenTimer {
            key: TaskKey::RecordUsageHistory,
            after: USAGE_HISTORY_SAMPLE_INTERVAL,
        }))
        .await
    {
        warn!(message = %message, "Failed to arm usage history sampler");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::handle::Handle;
    use aruna_core::structs::{UsageCounters, usage_group_key};
    use ulid::Ulid;

    fn test_ctx(root: &str) -> DriverContext {
        DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(root).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        }
    }

    fn node() -> NodeId {
        iroh::SecretKey::from_bytes(&[1u8; 32]).public()
    }

    async fn write_group_counter(ctx: &DriverContext, group: GroupId, logical_bytes: u64) {
        let counters = UsageCounters {
            logical_bytes,
            ..Default::default()
        };
        ctx.storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: USAGE_STATS_KEYSPACE.to_string(),
                key: Key::from(usage_group_key(group)),
                value: Value::from(counters.to_bytes().unwrap()),
                txn_id: None,
            })
            .await;
    }

    async fn group_points(ctx: &DriverContext, group: GroupId) -> Vec<UsageHistorySample> {
        read_usage_history(
            ctx,
            RealmUsageScope::Group(group),
            0,
            u64::MAX,
            USAGE_HISTORY_QUERY_LIMIT,
        )
        .await
        .unwrap()
    }

    // Unchanged readings dedup; a changed reading appends a point; queries filter.
    #[tokio::test]
    async fn history_dedup_query() {
        let temp = tempfile::tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let group = Ulid::from_bytes([2u8; 16]);
        write_group_counter(&ctx, group, 100).await;

        record_usage_history(&ctx, node()).await.unwrap();
        let points = group_points(&ctx, group).await;
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].counters.logical_bytes, 100);

        record_usage_history(&ctx, node()).await.unwrap();
        assert_eq!(group_points(&ctx, group).await.len(), 1);

        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        write_group_counter(&ctx, group, 200).await;
        record_usage_history(&ctx, node()).await.unwrap();
        let points = group_points(&ctx, group).await;
        assert_eq!(points.len(), 2);
        assert_eq!(points[1].counters.logical_bytes, 200);

        // Range excludes the first point.
        let recent = read_usage_history(
            &ctx,
            RealmUsageScope::Group(group),
            points[1].at_ms,
            u64::MAX,
            USAGE_HISTORY_QUERY_LIMIT,
        )
        .await
        .unwrap();
        assert_eq!(recent.len(), 1);
    }

    // Points older than the retention window are pruned on the next sample.
    #[tokio::test]
    async fn history_retention_prune() {
        let temp = tempfile::tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let group = Ulid::from_bytes([5u8; 16]);
        write_group_counter(&ctx, group, 10).await;

        let old_at = 1_000u64;
        let old = UsageHistorySample {
            at_ms: old_at,
            counters: UsageCounters {
                logical_bytes: 10,
                ..Default::default()
            },
            quota_bytes: None,
            ceiling_bytes: None,
            state: QuotaState::Unlimited,
        };
        ctx.storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: USAGE_HISTORY_KEYSPACE.to_string(),
                key: Key::from(usage_history_group_key(group, old_at)),
                value: Value::from(old.to_bytes().unwrap()),
                txn_id: None,
            })
            .await;

        record_usage_history(&ctx, node()).await.unwrap();
        let points = group_points(&ctx, group).await;
        assert!(points.iter().all(|point| point.at_ms != old_at));
    }
}
