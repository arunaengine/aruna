use std::time::Instant;

use aruna_core::structs::AuthContext;
use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;
use http::Method;
use tracing::{Instrument, Span, error, field, info_span, trace, warn};
use ulid::Ulid;

pub async fn request_tracing_middleware(request: Request, next: Next) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let query = request.uri().query().map(str::to_string);
    let span = make_request_span("http", &method, &path);
    let started = Instant::now();

    {
        let _guard = span.enter();
        trace!(
            event = "request.received",
            protocol = "http",
            method = %method,
            path = %path,
            query = query.as_deref().unwrap_or(""),
            "Received HTTP request"
        );
    }

    let response = next.run(request).instrument(span.clone()).await;
    emit_request_completed(&span, "http", response.status().as_u16(), started);
    response
}

pub fn make_request_span(protocol: &'static str, method: &Method, path: &str) -> Span {
    let request_id = Ulid::new().to_string();
    info_span!(
        "request",
        protocol = protocol,
        request_id = %request_id,
        method = %method,
        path = %path,
        status_code = field::Empty,
        user_id = field::Empty,
        realm_id = field::Empty,
    )
}

pub fn record_auth_context(auth_ctx: Option<&AuthContext>) {
    let span = Span::current();
    if let Some(auth_ctx) = auth_ctx {
        span.record("user_id", field::display(auth_ctx.user_id));
        span.record("realm_id", field::display(&auth_ctx.realm_id));
        trace!(
            event = "request.authenticated",
            user_id = %auth_ctx.user_id,
            realm_id = %auth_ctx.realm_id,
            "Resolved request authentication"
        );
    } else {
        trace!(
            event = "request.authentication_missing",
            "Request did not include authentication"
        );
    }
}

pub fn emit_request_completed(
    span: &Span,
    protocol: &'static str,
    status_code: u16,
    started: Instant,
) {
    span.record("status_code", status_code);
    let _guard = span.enter();
    let latency_ms = started.elapsed().as_millis() as u64;
    match status_code {
        500.. => error!(
            event = "request.failed",
            protocol = protocol,
            status_code,
            latency_ms,
            "Request failed"
        ),
        400..=499 => warn!(
            event = "request.rejected",
            protocol = protocol,
            status_code,
            latency_ms,
            "Request completed with client error"
        ),
        _ => trace!(
            event = "request.completed",
            protocol = protocol,
            status_code,
            latency_ms,
            "Completed request"
        ),
    }
}
