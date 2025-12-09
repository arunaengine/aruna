use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod, Runtime};
use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use rand::Rng;
use std::path::PathBuf;
use testcontainers::core::logs::consumer::LogConsumer;
use testcontainers::core::logs::LogFrame;
use tokio::io::AsyncWriteExt;
use tokio_postgres::NoTls;

pub struct ContainerLogger {
    log_path: PathBuf,
}
impl ContainerLogger {
    pub fn new(log_name: &str) -> Self {
        ContainerLogger {
            log_path: PathBuf::from("/tmp").join(log_name),
        }
    }
}
impl LogConsumer for ContainerLogger {
    fn accept<'a>(&'a self, record: &'a LogFrame) -> BoxFuture<'a, ()> {
        async move {
            match record {
                LogFrame::StdOut(bytes) | LogFrame::StdErr(bytes) => {
                    match &self.log_path.try_exists() {
                        Ok(exists) => {
                            //tokio::io::stdout().write_all(bytes).await.unwrap();

                            if *exists {
                                tokio::fs::File::options()
                                    .append(true)
                                    .open(&self.log_path)
                                    .await
                                    .unwrap()
                                    .write_all(bytes)
                                    .await
                                    .unwrap()
                            } else {
                                tokio::fs::File::create(&self.log_path)
                                    .await
                                    .unwrap()
                                    .write_all(bytes)
                                    .await
                                    .unwrap()
                            }
                        }
                        Err(_) => tokio::io::stdout()
                            .write_all(
                                format!("Could not access log path: {:?}", &self.log_path)
                                    .as_bytes(),
                            )
                            .await
                            .unwrap(),
                    };
                }
            }
        }
        .boxed()
    }
}

pub async fn create_database(database_name: &str, port: u16) {
    // Init database connection
    let mut cfg = Config::new();
    cfg.host = Some("localhost".to_string());
    cfg.port = Some(port);
    cfg.user = Some("postgres".to_string());
    cfg.password = Some("postgres".to_string());
    cfg.dbname = Some("postgres".to_string());
    cfg.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });
    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap();
    let client = pool.get().await.unwrap();

    let query = format!("CREATE DATABASE {};", database_name);
    let prepared = client.prepare(&query).await.unwrap();
    client.execute(&prepared, &[]).await.unwrap();
}

pub async fn create_random_database() -> String {
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz";
    const DB_NAME_LEN: usize = 16;

    let mut rng = rand::rng();
    let database_name: String = (0..DB_NAME_LEN)
        .map(|_| {
            let idx = rng.random_range(0..CHARSET.len());
            char::from(CHARSET[idx])
        })
        .collect();

    // Init database connection
    let mut cfg = Config::new();
    cfg.host = Some("localhost".to_string());
    cfg.port = Some(5432);
    cfg.user = Some("postgres".to_string());
    cfg.password = Some("postgres".to_string());
    cfg.dbname = Some("postgres".to_string());
    cfg.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });
    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap();
    let client = pool.get().await.unwrap();

    let query = format!("CREATE DATABASE {};", database_name);
    let prepared = client.prepare(&query).await.unwrap();
    client.execute(&prepared, &[]).await.unwrap();

    database_name
}

pub async fn load_schema(database_name: impl Into<String>, schema_path: impl Into<String>) {
    // Read schema sql
    let schema = tokio::fs::read_to_string(schema_path.into()).await.unwrap();

    // Init database connection
    let mut cfg = Config::new();
    cfg.host = Some("localhost".to_string());
    cfg.port = Some(5432);
    cfg.user = Some("postgres".to_string());
    cfg.password = Some("postgres".to_string());
    cfg.dbname = Some(database_name.into());
    cfg.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });

    // Load schema sql into database
    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap();
    let mut client = pool.get().await.unwrap();
    let transaction = client.transaction().await.unwrap();
    transaction.batch_execute(&schema).await.unwrap();
    transaction.commit().await.unwrap();
}

pub async fn test_database(database_name: impl Into<String>) {
    // Init database connection
    let mut cfg = Config::new();
    cfg.host = Some("localhost".to_string());
    cfg.port = Some(5432);
    cfg.user = Some("postgres".to_string());
    cfg.password = Some("postgres".to_string());
    cfg.dbname = Some(database_name.into());
    cfg.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });

    // Load schema sql into database
    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap();
    let client = pool.get().await.unwrap();

    let query = "SELECT 1 FROM users";
    let prepared = client.prepare(query).await.unwrap();
    let rows = client.query(&prepared, &[]).await.unwrap();
    dbg!(rows);
}

pub async fn update_issuer(database_name: impl Into<String>) {
    // Init database connection
    let mut cfg = Config::new();
    cfg.host = Some("localhost".to_string());
    cfg.port = Some(5432);
    cfg.user = Some("postgres".to_string());
    cfg.password = Some("postgres".to_string());
    cfg.dbname = Some(database_name.into());
    cfg.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });

    // Load schema sql into database
    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap();
    let client = pool.get().await.unwrap();

    let query = "UPDATE identity_providers \
      SET jwks_endpoint = 'http://keycloak:8080/realms/test/protocol/openid-connect/certs' \
      WHERE issuer_name = 'http://localhost:1998/realms/test'\
      RETURNING *;";
    let prepared = client.prepare(query).await.unwrap();
    client.query_one(&prepared, &[]).await.unwrap();
}
