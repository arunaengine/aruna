use std::time::Duration;

use aruna_server::{config_from_env, start_server};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::TracerProvider;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    let config = config_from_env();

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(config.opentelemetry_endpoint.clone())
        .with_timeout(Duration::from_secs(3))
        .build()
        .unwrap();

    let provider = TracerProvider::builder()
        .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
        .build()
        .tracer(config.opentelemetry_name.clone());

    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or("none".into())
        .add_directive("aruna_server=trace".parse().unwrap())
        .add_directive("tower_http=info".parse().unwrap())
        .add_directive("synevi_core=info".parse().unwrap());

    let telemetry_layer = tracing_opentelemetry::layer().with_tracer(provider);

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_file(true)
        .with_line_number(true);

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .with(telemetry_layer)
        .init();

    start_server(config, None).await.unwrap()
}
