use std::{collections::BTreeSet, hash::Hash, sync::Arc, time::Duration};

use anyhow::{bail, Result};
use async_channel::Receiver;
use chrono::{NaiveDateTime, Utc};
use diesel_ulid::DieselUlid;
use itertools::Itertools;
use log::error;
use postgres_from_row::FromRow;
use tokio_postgres::Client;

use crate::{
    caching::cache::Cache,
    database::connection::Database,
    notification::natsio_handler::{NatsIoHandler, ServerEvents},
    search::meilisearch_client::{MeilisearchClient, ObjectDocument},
    utils::search_utils,
};

#[derive(Copy, Clone, Debug, FromRow, Eq)]
pub struct ObjectStats {
    pub origin_pid: DieselUlid,
    pub count: i64,
    pub size: i64,
    pub last_refresh: NaiveDateTime,
}

impl Ord for ObjectStats {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Ignore timestamp in order
        (self.origin_pid, self.count, self.size).cmp(&(other.origin_pid, other.count, other.size))
    }
}

impl PartialOrd for ObjectStats {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ObjectStats {
    fn eq(&self, other: &Self) -> bool {
        // Ignore timestamp in equality comparison
        self.origin_pid == other.origin_pid && self.count == other.count && self.size == other.size
    }
}

impl Hash for ObjectStats {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Also ignore timestamp in hash to fulfill agreement with PartialEq
        self.origin_pid.hash(state);
        self.count.hash(state);
        self.size.hash(state);
    }
}

impl ObjectStats {
    pub async fn get_object_stats(id: &DieselUlid, client: &Client) -> Result<Self> {
        let query = "SELECT * FROM object_stats WHERE origin_pid = $1;";
        let prepared = client.prepare(query).await?;

        let stats = match client.query_opt(&prepared, &[&id]).await? {
            Some(row) => ObjectStats::from_row(&row),
            None => ObjectStats {
                origin_pid: *id,
                count: 1,
                size: 0,
                last_refresh: NaiveDateTime::default(),
            },
        };

        Ok(stats)
    }

    pub async fn get_all_stats(client: &Client) -> Result<Vec<Self>> {
        let query = "SELECT * FROM object_stats;";
        let prepared = client.prepare(query).await?;

        let rows = client.query(&prepared, &[]).await?;

        let stats = rows.iter().map(ObjectStats::from_row).collect_vec();

        Ok(stats)
    }
}

pub async fn refresh_stats_view(client: &Client) -> Result<()> {
    let query = "REFRESH MATERIALIZED VIEW object_stats;";
    let prepared = client.prepare(query).await?;

    client.execute(&prepared, &[]).await?;

    Ok(())
}

pub async fn get_last_refresh(client: &Client) -> Result<NaiveDateTime> {
    let query = "SELECT last_refresh FROM object_stats LIMIT 1;";
    let prepared = client.prepare(query).await?;

    let last_refreshed: NaiveDateTime = match client.query_opt(&prepared, &[]).await? {
        Some(row) => row.get("last_refresh"),
        None => bail!("Object stats table is empty"),
    };

    Ok(last_refreshed)
}

pub async fn start_refresh_loop(
    database: Arc<Database>,
    cache: Arc<Cache>,
    search_client: Arc<MeilisearchClient>,
    natsio_handler: Arc<NatsIoHandler>,
    refresh_receiver: Receiver<i64>,
    refresh_interval: i64,
) {
    // Start loop
    tokio::spawn(async move {
        let mut last_object_stats = BTreeSet::new();
        loop {
            // Try to get database connection
            let client = match database.get_client().await {
                Ok(client) => client,
                Err(err) => {
                    error!("Failed to get database client for MV refresh: {err}");
                    tokio::time::sleep(Duration::from_secs(15)).await; // Wait 15s and try again
                    continue;
                }
            };

            // Save current timestamp to check if refresh is necessary
            let mut current_timestamp = Utc::now().timestamp_millis();

            // Collect latest timestamp of all other started refreshs
            let mut latest_refresh = None;
            while let Ok(refresh_timestamp) = refresh_receiver.try_recv() {
                if let Some(latest_timestamp) = latest_refresh {
                    if refresh_timestamp > latest_timestamp {
                        latest_refresh = Some(refresh_timestamp)
                    }
                } else {
                    latest_refresh = Some(refresh_timestamp)
                }
            }

            // Evaluate if refresh is necessary
            let start_refresh = if let Some(refresh_timestamp) = latest_refresh {
                let start_refresh = (current_timestamp - refresh_timestamp) > refresh_interval;
                if !start_refresh {
                    current_timestamp = refresh_timestamp
                }
                start_refresh
            } else {
                true
            };

            // Start MV refresh if conditions are met
            if start_refresh {
                match refresh_stats_view(&client).await {
                    Ok(_) => {
                        // Send notification that MV refresh has been started
                        if let Err(err) = natsio_handler
                            .register_server_event(ServerEvents::MVREFRESH(current_timestamp))
                            .await
                        {
                            error!("Failed to send MV refresh notification: {err}")
                        }
                    }
                    Err(err) => {
                        error!("Start MV refresh failed: {err}");
                        // Sleep for refresh interval and try again
                        tokio::time::sleep(Duration::from_millis(
                            refresh_interval.try_into().unwrap_or(30000),
                        ))
                        .await;
                        continue;
                    }
                }
            }

            // Wait in every case for refresh to finish for cache update
            while let Ok(last_refresh) = get_last_refresh(&client).await {
                if last_refresh.and_utc().timestamp_millis() >= current_timestamp {
                    // Fetch all ObjectStats, create diff with last loop iteration and update only changed
                    let object_stats =
                        BTreeSet::from_iter(match ObjectStats::get_all_stats(&client).await {
                            Ok(stats) => stats,
                            Err(err) => {
                                error!("Failed to fetch all stats from database: {err}");
                                break;
                            }
                        });

                    let diff = object_stats
                        .difference(&last_object_stats)
                        .cloned()
                        .collect_vec();

                    // Save current stats for next iteration
                    last_object_stats = BTreeSet::from_iter(object_stats);

                    // Update changed stats in cache only if stats are available and anything has changed
                    if !diff.is_empty() {
                        if let Err(err) = cache.upsert_object_stats(diff.clone()).await {
                            error!("Object stats cache update failed: {err}")
                        } else {
                            // Update changes in search index
                            let ods: Result<Vec<ObjectDocument>> = diff
                                .iter()
                                .map(|os| -> Result<ObjectDocument> {
                                    cache.get_object_document(&os.origin_pid).ok_or_else(|| {
                                        anyhow::anyhow!(
                                            "Could not find object {} in cache",
                                            os.origin_pid
                                        )
                                    })
                                })
                                .collect();

                            match ods {
                                Ok(index_updates) => {
                                    search_utils::update_search_index(
                                        &search_client,
                                        &cache,
                                        index_updates,
                                    )
                                    .await;
                                }
                                Err(err) => {
                                    error!("Failed to fetch objects for search index update: {err}")
                                }
                            }
                        };
                    }
                    break;
                } else {
                    tokio::time::sleep(Duration::from_secs(1)).await // Wait 1 sec and try again
                }
            }

            // Sleep for refresh interval
            tokio::time::sleep(Duration::from_millis(
                refresh_interval.try_into().unwrap_or(30000),
            ))
            .await;
        }

        //Ok::<(), anyhow::Error>(())
    });
}
