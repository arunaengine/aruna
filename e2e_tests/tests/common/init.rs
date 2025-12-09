use crate::common;
use crate::common::infra::{keycloak, meili, minio, nats, postgres};
use crate::common::init_server::Component;
use crate::common::util;
use crate::common::util::{create_database, ContainerLogger};
use aruna_server::database::connection::Database;
use maybe_once::tokio::Data;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use testcontainers::core::wait::HealthWaitStrategy;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, CopyDataSource, GenericImage, Healthcheck, ImageExt};

pub const NETWORK: &str = "aruna-test";

pub const MEILI_PORT: u16 = 7700;
pub static MEILI_OFFSET: AtomicU16 = AtomicU16::new(5); // Static Meili is already 0

pub const NATS_CLIENT_PORT: u16 = 4222;
pub const NATS_HTTP_PORT: u16 = 8222;
pub const MINIO_CLIENT_PORT: u16 = 9000;
pub const MINIO_CONSOLE_PORT: u16 = 9001;
pub const KEYCLOAK_PORT: u16 = 1998;
pub const POSTGRES_PORT: u16 = 5432;

pub struct TestContainers<'a> {
    pub keycloak: Data<'a, ContainerAsync<GenericImage>>,
    pub postgres: Data<'a, ContainerAsync<GenericImage>>,
    pub minio: Data<'a, ContainerAsync<GenericImage>>,
    pub nats: Data<'a, ContainerAsync<GenericImage>>,
    pub meili: Data<'a, ContainerAsync<GenericImage>>,
    pub meili_clean: Option<ContainerAsync<GenericImage>>,
}

pub async fn init_test_environment<'a>(static_meili_only: bool) -> TestContainers<'a> {
    let (keycloak, postgres, minio, nats, meili, meili_clean) = if static_meili_only {
        let (a, b, c, d, e) = tokio::join!(
            keycloak(false),
            postgres(false),
            minio(false),
            nats(false),
            meili(false),
        );
        (a, b, c, d, e, None)
    } else {
        let offset = MEILI_OFFSET.fetch_add(5, Ordering::SeqCst);
        let (a, b, c, d, e, f) = tokio::join!(
            keycloak(false),
            postgres(false),
            minio(false),
            nats(false),
            meili(false),
            init_meili_container(offset),
        );
        (a, b, c, d, e, Some(f))
    };

    TestContainers {
        keycloak,
        postgres,
        minio,
        nats,
        meili,
        meili_clean,
    }
}

pub async fn init_meili_container(offset: u16) -> ContainerAsync<GenericImage> {
    // Adapt config to offset
    let host_port = MEILI_PORT + offset;
    let log_file = format!("meili-{offset}.log");

    // Create Meilisearch container
    GenericImage::new("getmeili/meilisearch", "latest")
        .with_wait_for(WaitFor::Healthcheck(
            HealthWaitStrategy::new().with_poll_interval(std::time::Duration::from_millis(100)),
        ))
        .with_health_check(Healthcheck::cmd_shell(
            "curl 'http://localhost:7700/health'",
        ))
        //.with_health_check(Healthcheck::cmd_shell(format!("curl 'http://localhost:{host_port}/health'")))
        .with_startup_timeout(std::time::Duration::from_secs(600))
        .with_log_consumer(ContainerLogger::new(&log_file))
        .with_mapped_port(host_port, 7700.tcp())
        .with_network(NETWORK)
        .with_env_var("MEILI_MASTER_KEY", "MASTER_KEY")
        .start()
        .await
        .expect("Failed to start Meilisearch container")
}

pub async fn init_meili_container_static() -> ContainerAsync<GenericImage> {
    // Create Meilisearch container
    GenericImage::new("getmeili/meilisearch", "latest")
        .with_wait_for(WaitFor::Healthcheck(
            HealthWaitStrategy::new().with_poll_interval(std::time::Duration::from_millis(100)),
        ))
        .with_health_check(Healthcheck::cmd_shell(
            "curl 'http://localhost:7700/health'",
        ))
        //.with_health_check(Healthcheck::cmd_shell(format!("curl 'http://localhost:{host_port}/health'")))
        .with_startup_timeout(std::time::Duration::from_secs(600))
        .with_log_consumer(ContainerLogger::new("meili_static.log"))
        .with_mapped_port(MEILI_PORT, 7700.tcp())
        .with_network(NETWORK)
        .with_env_var("MEILI_MASTER_KEY", "MASTER_KEY")
        .start()
        .await
        .expect("Failed to start Meilisearch container")
}

pub async fn init_nats_container(offset: u16) -> ContainerAsync<GenericImage> {
    // Adapt config to offset
    let client_port = NATS_CLIENT_PORT + offset;
    let http_port = NATS_HTTP_PORT + offset;
    let log_file = format!("nats-{offset}.log");

    // Create Nats container
    GenericImage::new("nats", "alpine")
        .with_wait_for(WaitFor::Healthcheck(
            HealthWaitStrategy::new().with_poll_interval(std::time::Duration::from_millis(100)),
        ))
        .with_health_check(Healthcheck::cmd_shell(format!(
            "wget http://localhost:{http_port}/healthz -q -S -O -"
        )))
        .with_log_consumer(ContainerLogger::new(&log_file))
        .with_mapped_port(client_port, 4222.tcp())
        .with_mapped_port(http_port, 8222.tcp())
        .with_network(NETWORK)
        .with_cmd(["-m", &http_port.to_string(), "--js"])
        .start()
        .await
        .expect("Failed to start Nats container")
}

pub async fn init_nats_container_static() -> ContainerAsync<GenericImage> {
    // Create Nats container
    GenericImage::new("nats", "alpine")
        .with_wait_for(WaitFor::Healthcheck(
            HealthWaitStrategy::new().with_poll_interval(std::time::Duration::from_millis(100)),
        ))
        .with_health_check(Healthcheck::cmd_shell(
            "wget http://localhost:8222/healthz -q -S -O -",
        ))
        .with_log_consumer(ContainerLogger::new("nats_static.log"))
        .with_mapped_port(4222, 4222.tcp())
        .with_mapped_port(8222, 8222.tcp())
        .with_network(NETWORK)
        .with_cmd(["-m", "8222", "--js"])
        .start()
        .await
        .expect("Failed to start Nats container")
}

pub async fn init_minio_container(offset: u16) -> ContainerAsync<GenericImage> {
    // Adapt config to offset
    let client_port = MINIO_CLIENT_PORT + offset;
    let console_port = MINIO_CONSOLE_PORT + offset;
    let log_file = format!("minio-{offset}.log");

    // Create minio container
    GenericImage::new("minio/minio", "latest")
        .with_wait_for(WaitFor::Healthcheck(
            HealthWaitStrategy::new().with_poll_interval(std::time::Duration::from_millis(100)),
        ))
        .with_health_check(Healthcheck::cmd_shell("mc ready local"))
        .with_log_consumer(ContainerLogger::new(&log_file))
        .with_mapped_port(client_port, 9000.tcp())
        .with_mapped_port(console_port, 9001.tcp())
        .with_network(NETWORK)
        .with_env_var("MINIO_DOMAIN", &format!("minio:{client_port}"))
        .with_cmd([
            "server",
            "/data",
            "--console-address",
            &format!(":{console_port}"),
        ])
        .start()
        .await
        .expect("Failed to start Minio container")
}

pub async fn init_minio_container_static() -> ContainerAsync<GenericImage> {
    // Create minio container
    GenericImage::new("minio/minio", "latest")
        .with_wait_for(WaitFor::Healthcheck(
            HealthWaitStrategy::new().with_poll_interval(std::time::Duration::from_millis(100)),
        ))
        .with_health_check(Healthcheck::cmd_shell("mc ready local"))
        .with_log_consumer(ContainerLogger::new("minio_Static.log"))
        .with_mapped_port(9000, 9000.tcp())
        .with_mapped_port(9001, 9001.tcp())
        .with_network(NETWORK)
        .with_env_var("MINIO_DOMAIN", "minio:9001")
        .with_cmd(["server", "/data", "--console-address", ":9001"])
        .start()
        .await
        .expect("Failed to start Minio container")
}

pub async fn init_keycloak_container(offset: u16) -> ContainerAsync<GenericImage> {
    // Adapt config to offset
    let host_port = KEYCLOAK_PORT + offset;
    let log_file = format!("keycloak-{offset}.log");

    // Create keycloak container
    GenericImage::new("quay.io/keycloak/keycloak", "26.3.3")
        .with_wait_for(WaitFor::Healthcheck(
            HealthWaitStrategy::new().with_poll_interval(std::time::Duration::from_millis(100)),
        ))
        .with_health_check(Healthcheck::cmd_shell("[ -f /tmp/HealthCheck.java ] || echo \"public class HealthCheck { public static void main(String[] args) throws java.lang.Throwable { System.exit(java.net.HttpURLConnection.HTTP_OK == ((java.net.HttpURLConnection)new java.net.URL(args[0]).openConnection()).getResponseCode() ? 0 : 1); } }\" > /tmp/HealthCheck.java && java /tmp/HealthCheck.java http://localhost:9000/health/live"))
        .with_startup_timeout(std::time::Duration::from_secs(600))
        .with_log_consumer(ContainerLogger::new(&log_file))
        .with_mapped_port(host_port, 8080.tcp())
        .with_network(NETWORK)
        .with_env_var("KC_BOOTSTRAP_ADMIN_USERNAME", "admin")
        .with_env_var("KC_BOOTSTRAP_ADMIN_PASSWORD", "admin")
        .with_env_var("KC_HEALTH_ENABLED", "true")
        .with_copy_to(
            "/opt/keycloak/data/import/realm.json",
            CopyDataSource::File(PathBuf::from("../components/server/tests/common/keycloak/realm.json")))
        .with_cmd(["start-dev", "--import-realm"])
        .start()
        .await
        .expect("Failed to start Keycloak container")
}

pub async fn init_keycloak_container_static() -> ContainerAsync<GenericImage> {
    // Create keycloak container
    GenericImage::new("quay.io/keycloak/keycloak", "26.3.3")
        .with_wait_for(WaitFor::Healthcheck(
            HealthWaitStrategy::new().with_poll_interval(std::time::Duration::from_millis(100)),
        ))
        .with_health_check(Healthcheck::cmd_shell("[ -f /tmp/HealthCheck.java ] || echo \"public class HealthCheck { public static void main(String[] args) throws java.lang.Throwable { System.exit(java.net.HttpURLConnection.HTTP_OK == ((java.net.HttpURLConnection)new java.net.URL(args[0]).openConnection()).getResponseCode() ? 0 : 1); } }\" > /tmp/HealthCheck.java && java /tmp/HealthCheck.java http://localhost:9000/health/live"))
        .with_startup_timeout(std::time::Duration::from_secs(600))
        //.with_container_name("keycloak")
        .with_log_consumer(ContainerLogger::new("keycloak_static.log"))
        .with_mapped_port(1998, 8080.tcp())
        .with_network(NETWORK)
        .with_env_var("KC_BOOTSTRAP_ADMIN_USERNAME", "admin")
        .with_env_var("KC_BOOTSTRAP_ADMIN_PASSWORD", "admin")
        .with_env_var("KC_HEALTH_ENABLED", "true")
        .with_copy_to(
            "/opt/keycloak/data/import/realm.json",
            CopyDataSource::File(PathBuf::from("tests/common/data/realm.json")))
        .with_cmd(["start-dev", "--import-realm"])
        .start()
        .await
        .expect("Failed to start Keycloak container")
}

pub async fn init_postgres_container(offset: u16) -> ContainerAsync<GenericImage> {
    // Adapt config to offset
    let host_port = POSTGRES_PORT + offset;
    let log_file = format!("postgres-{offset}.log");

    // Create postgres container
    let container = GenericImage::new("postgres", "17.6")
        .with_wait_for(WaitFor::Healthcheck(
            HealthWaitStrategy::new().with_poll_interval(std::time::Duration::from_millis(100)),
        ))
        .with_health_check(Healthcheck::cmd_shell("pg_isready"))
        .with_log_consumer(ContainerLogger::new(&log_file))
        .with_mapped_port(host_port, 5432.tcp())
        .with_network(NETWORK)
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .start()
        .await
        .expect("Failed to start Postgres container");

    // Create databases
    create_database("server_test", host_port).await;
    create_database("proxy_test", host_port).await;

    // Load schema in database
    common::util::load_schema("server_test", "../server/src/database/schema.sql").await;
    common::util::load_schema("proxy_test", "src/database/schema.sql").await;

    container
}

pub async fn init_postgres_container_static() -> ContainerAsync<GenericImage> {
    // Create postgres container
    GenericImage::new("postgres", "17.6")
        .with_wait_for(WaitFor::Healthcheck(
            HealthWaitStrategy::new().with_poll_interval(std::time::Duration::from_millis(100)),
        ))
        .with_health_check(Healthcheck::cmd_shell("pg_isready"))
        .with_log_consumer(ContainerLogger::new("postgres_static.log"))
        .with_mapped_port(5432, 5432.tcp())
        .with_network(NETWORK)
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .start()
        .await
        .expect("Failed to start Postgres container")
}

/// Creates a database with a random name and
/// optionally imports the server schema and loads initial data
pub async fn init_database(
    postgres: &ContainerAsync<GenericImage>,
    component: Component,
) -> (String, Arc<Database>) {
    let host = postgres.get_bridge_ip_address().await.unwrap();
    let port = postgres.get_host_port_ipv4(5432.tcp()).await.unwrap();
    let db_name = util::create_random_database().await;

    // Init database connection
    let db = Database::new(
        host.to_string(),
        port,
        db_name.clone(),
        "postgres".to_string(),
        "postgres".to_string(),
    )
    .unwrap();

    // Load schema and data based on provided component
    match component {
        Component::Server => {
            util::load_schema(&db_name, "../components/server/src/database/schema.sql").await;
            util::load_schema(&db_name, "tests/common/data/initial_data.sql").await;
        }
        Component::Proxy => {
            util::load_schema(&db_name, "../components/data_proxy/src/database/schema.sql").await;
        }
    }

    (db_name, Arc::new(db))
}

/*
pub async fn init_aruna_server_container(
    offset: u16,
    db_name: &str,
    infra: &TestContainers<'_>,
) -> ContainerAsync<GenericImage> {
    // Adapt config to offset
    let host_port = SERVER_GRPC_PORT + offset;
    let log_file = format!("aruna-server-{offset}.log");

    // Service hosts
    let postgres_host = infra.postgres.get_bridge_ip_address().await.unwrap();
    let nats_host = format!("{}:4222", infra.nats.get_bridge_ip_address().await.unwrap(),);
    let meili_host = format!(
        "http://{}:{}",
        infra
            .meili
            .as_ref()
            //.unwrap()
            .get_bridge_ip_address()
            .await
            .unwrap(),
        infra
            .meili
            .as_ref()
            //.unwrap()
            .get_host_port_ipv4(7700.tcp())
            .await
            .unwrap()
    );

    /*
    println!(
        "[Aruna Server {host_port}] {keycloak_host}:{}, {postgres_host}, {nats_host}, {meili_host}",
        infra.keycloak.get_host_port_ipv4(8080.tcp()).await.unwrap()
    );
    */

    // Create Aruna Server container
    GenericImage::new(
        "harbor.computational.bio.uni-giessen.de/aruna/aruna-server",
        "2.0.8",
    )
    .with_wait_for(WaitFor::message_on_either_std("ArunaServer listening"))
    .with_startup_timeout(std::time::Duration::from_secs(600))
    .with_log_consumer(ContainerLogger::new(&log_file))
    .with_mapped_port(host_port, 50051.tcp())
    .with_network(NETWORK)
    .with_env_var("DATABASE_SCHEMA", "/run/schema.sql")
    .with_env_var("DATABASE_HOST", postgres_host.to_string())
    .with_env_var("DATABASE_PORT", "5432")
    .with_env_var("DATABASE_USER", "postgres")
    .with_env_var("DATABASE_PASSWORD", "postgres")
    .with_env_var("DATABASE_DB", db_name)
    .with_env_var("MEILISEARCH_HOST", meili_host)
    .with_env_var("NATS_HOST", nats_host)
    .with_copy_to(
        "/run/schema.sql",
        CopyDataSource::File(PathBuf::from("../server/src/database/schema.sql")),
    )
    .start()
    .await
    .expect(&format!(
        "[Aruna Server {offset}:{host_port}] Failed to start container"
    ))
}
*/
