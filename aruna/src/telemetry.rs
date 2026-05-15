use tracing_subscriber::EnvFilter;
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

pub fn init_tracing() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_LOG_FILTER));
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_file(true)
        .with_line_number(true);

    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .try_init();
}

pub fn shutdown_tracing() {}
