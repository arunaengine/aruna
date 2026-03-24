use std::sync::OnceLock;

use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

const DEFAULT_LOG_FILTER: &str = "info,iroh=error,iroh_gossip=error,iroh_quinn=error,quinn=error,pkarr=error,hickory_resolver=error";

static TRACER_PROVIDER: OnceLock<SdkTracerProvider> = OnceLock::new();

pub fn init_tracing() {
    global::set_text_map_propagator(TraceContextPropagator::new());

    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_LOG_FILTER));
    let fmt_layer = tracing_subscriber::fmt::layer().with_target(false);

    if let Some(provider) = build_tracer_provider() {
        let tracer = provider.tracer("aruna");
        let _ = TRACER_PROVIDER.set(provider.clone());
        global::set_tracer_provider(provider);

        let _ = tracing_subscriber::registry()
            .with(filter)
            .with(fmt_layer)
            .with(tracing_opentelemetry::layer().with_tracer(tracer))
            .try_init();
    } else {
        let _ = tracing_subscriber::registry()
            .with(filter)
            .with(fmt_layer)
            .try_init();
    }
}

pub fn shutdown_tracing() {
    if let Some(provider) = TRACER_PROVIDER.get() {
        let _ = provider.shutdown();
    }
}

fn build_tracer_provider() -> Option<SdkTracerProvider> {
    let otlp_configured = [
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
    ]
    .into_iter()
    .any(|key| std::env::var_os(key).is_some());
    if !otlp_configured {
        return None;
    }

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .build()
        .ok()?;

    Some(
        SdkTracerProvider::builder()
            .with_resource(
                Resource::builder_empty()
                    .with_service_name("aruna")
                    .with_attributes([KeyValue::new("service.version", env!("CARGO_PKG_VERSION"))])
                    .build(),
            )
            .with_batch_exporter(exporter)
            .build(),
    )
}
