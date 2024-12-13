use aruna_server::{config_from_env, start_server};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or("none".into())
        .add_directive("aruna_server=trace".parse().unwrap())
        .add_directive("tower_http=info".parse().unwrap())
        .add_directive("synevi_core=info".parse().unwrap());

    tracing_subscriber::fmt()
        .with_file(true)
        .with_line_number(true)
        .with_env_filter(filter)
        .init();

    let config = config_from_env();

    start_server(config, None).await.unwrap()
}