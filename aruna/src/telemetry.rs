use std::sync::OnceLock;

use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

const DEFAULT_LOG_FILTER: &str = concat!(
    "error,",
    "aruna=debug,",
    "aruna_api=debug,",
    "aruna_blob=debug,",
    "aruna_core=debug,",
    "aruna_net=debug,",
    "aruna_operations=debug,",
    "aruna_search=debug,",
    "aruna_storage=debug,",
    "aruna_tasks=debug"
);

static TRACER_PROVIDER: OnceLock<SdkTracerProvider> = OnceLock::new();

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LogFormat {
    Text,
    Json,
}

fn parse_log_format(value: Option<&str>) -> LogFormat {
    match value.map(|raw| raw.trim().to_ascii_lowercase()).as_deref() {
        Some("json") => LogFormat::Json,
        _ => LogFormat::Text,
    }
}

pub fn init_tracing() {
    global::set_text_map_propagator(TraceContextPropagator::new());

    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_LOG_FILTER));
    let fmt_layer = match parse_log_format(std::env::var("LOG_FORMAT").ok().as_deref()) {
        LogFormat::Json => tracing_subscriber::fmt::layer()
            .json()
            .with_target(true)
            .with_file(true)
            .with_line_number(true)
            .boxed(),
        LogFormat::Text => tracing_subscriber::fmt::layer()
            .with_target(true)
            .with_file(true)
            .with_line_number(true)
            .boxed(),
    };

    let provider = build_tracer_provider();
    let tracer = provider.tracer("aruna");
    let _ = TRACER_PROVIDER.set(provider.clone());
    global::set_tracer_provider(provider);

    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .try_init();
}

pub fn shutdown_tracing() {
    if let Some(provider) = TRACER_PROVIDER.get() {
        let _ = provider.shutdown();
    }
}

fn build_tracer_provider() -> SdkTracerProvider {
    let otlp_configured = [
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
    ]
    .into_iter()
    .any(|key| std::env::var_os(key).is_some());
    let builder = SdkTracerProvider::builder().with_resource(
        Resource::builder_empty()
            .with_service_name("aruna")
            .with_attributes([KeyValue::new("service.version", env!("CARGO_PKG_VERSION"))])
            .build(),
    );

    if otlp_configured {
        return match opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .build()
        {
            Ok(exporter) => builder.with_batch_exporter(exporter).build(),
            Err(error) => {
                eprintln!("Failed to initialize OTLP span exporter: {error:?}");
                builder.build()
            }
        };
    }

    builder.build()
}

#[cfg(test)]
mod tests {
    use super::{LogFormat, parse_log_format};

    #[test]
    fn log_format_defaults_to_text() {
        assert_eq!(parse_log_format(None), LogFormat::Text);
        assert_eq!(parse_log_format(Some("text")), LogFormat::Text);
        assert_eq!(parse_log_format(Some("garbage")), LogFormat::Text);
    }

    #[test]
    fn log_format_parses_json_case_insensitively() {
        assert_eq!(parse_log_format(Some("json")), LogFormat::Json);
        assert_eq!(parse_log_format(Some("  JSON  ")), LogFormat::Json);
    }
}
