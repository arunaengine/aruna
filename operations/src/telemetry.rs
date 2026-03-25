use std::collections::BTreeMap;

use opentelemetry::Context;
use opentelemetry::global;
use opentelemetry::propagation::{Extractor, Injector};
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

struct TraceContextInjector<'a>(&'a mut BTreeMap<String, String>);

impl Injector for TraceContextInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_string(), value);
    }
}

struct TraceContextExtractor<'a>(&'a BTreeMap<String, String>);

impl Extractor for TraceContextExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(String::as_str).collect()
    }
}

pub fn current_trace_context() -> BTreeMap<String, String> {
    let mut trace_context = BTreeMap::new();
    let context = Span::current().context();
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&context, &mut TraceContextInjector(&mut trace_context));
    });
    trace_context
}

pub fn extract_trace_context(trace_context: &BTreeMap<String, String>) -> Context {
    global::get_text_map_propagator(|propagator| {
        propagator.extract(&TraceContextExtractor(trace_context))
    })
}
