use anyhow::Result;
use deadpool_postgres::{Client, Config, ManagerConfig, Pool, RecyclingMethod, Runtime};
use tokio_postgres::NoTls;

use crate::config::{Config as ProxyConfig, Persistence};

pub struct Database {
    connection_pool: Pool,
}

impl Database {
    #[tracing::instrument(level = "trace", skip())]
    pub async fn new(config: &ProxyConfig) -> Result<Self> {
        let Persistence::Postgres {
            host,
            port,
            user,
            password,
            database,
            schema,
        } = config
            .persistence
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("persistence configuration not found"))?;

        let mut cfg = Config::new();
        cfg.host = Some(host.to_string());
        cfg.port = Some(*port);
        cfg.user = Some(user.to_string());
        cfg.password = Some(
            password
                .clone()
                .ok_or_else(|| anyhow::anyhow!("password not found"))?,
        );
        cfg.dbname = Some(database.to_string());
        cfg.manager = Some(ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        });
        let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })?;

        Database::initialize_db(
            &pool.get().await.map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?,
            schema.to_string(),
        )
        .await?;

        Ok(Database {
            connection_pool: pool,
        })
    }

    #[tracing::instrument(level = "trace", skip(client))]
    pub async fn initialize_db(client: &Client, schema: String) -> Result<()> {
        let initial = tokio::fs::read_to_string(schema).await.map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })?;
        client.batch_execute(&initial).await.map_err(|e| {
            tracing::error!(error = ?e,msg = e.to_string());
            e
        })?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn get_client(&self) -> Result<Client> {
        Ok(self.connection_pool.get().await.map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })?)
    }
}
