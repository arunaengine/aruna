//! Builds the OpenAPI document from the route registration and adds shared responses.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde_json::json;
use utoipa::openapi::header::Header;
use utoipa::openapi::response::{Response, ResponseBuilder};
use utoipa::openapi::schema::{Object, Type};
use utoipa::openapi::security::{Http, HttpAuthScheme, SecurityScheme};
use utoipa::openapi::{Content, Ref, RefOr};
use utoipa::{Modify, OpenApi};

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Aruna Server API",
        version = env!("CARGO_PKG_VERSION"),
        description = r#"REST API for the Aruna federated data orchestration network.

**Authentication**: most operations take a realm bearer token; the GA4GH TES facade also accepts
HTTP Basic with an access key and secret issued by this node, and public routes carry an empty
security requirement.
Native Git/LFS also accepts HTTP Basic with an Aruna bearer token as the password.

**Conventions**
- Errors answer `application/json` with `ErrorResponse` (`error` plus an optional `code`); the
  GA4GH DRS and TES facades use their own error payloads.
- Every operation may answer 429 with a `Retry-After` header.
- An operation may answer 408 when the request exceeds the REST time limit; streaming RO-Crate
  uploads and Git/LFS transfers are exempt.
- An operation with a request body may answer 413 when the body exceeds the configured limit.
- An operation that reports errors as a body may answer 500 on an unexpected internal failure.
- Paths are relative to the `/api/v1` base path."#,
        license(name = "Apache-2.0", url = "https://www.apache.org/licenses/LICENSE-2.0"),
        contact(name = "Aruna Team", url = "https://github.com/arunaengine/aruna")
    ),
    servers(
        (url = "/api/v1", description = "REST API v1")
    ),
    modifiers(&SecurityAddon)
)]
struct BaseApiDoc;

pub struct ApiDoc;

impl ApiDoc {
    /// Serves the document built by the same registration that builds the
    /// runtime router, so a live route is never missing from it.
    pub fn openapi() -> utoipa::openapi::OpenApi {
        let mut openapi = BaseApiDoc::openapi();
        openapi.merge(crate::routes::rest_openapi());
        add_transport_responses(&mut openapi);
        openapi
    }
}

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "bearer_auth",
                SecurityScheme::Http(Http::new(HttpAuthScheme::Bearer)),
            );
            components.add_security_scheme(
                "basic_auth",
                SecurityScheme::Http(Http::new(HttpAuthScheme::Basic)),
            );
        }
    }
}

fn add_transport_responses(openapi: &mut utoipa::openapi::OpenApi) {
    for (path, item) in &mut openapi.paths.paths {
        for (method, operation) in [
            ("GET", item.get.as_mut()),
            ("PUT", item.put.as_mut()),
            ("POST", item.post.as_mut()),
            ("DELETE", item.delete.as_mut()),
            ("OPTIONS", item.options.as_mut()),
            ("HEAD", item.head.as_mut()),
            ("PATCH", item.patch.as_mut()),
            ("TRACE", item.trace.as_mut()),
        ] {
            let Some(operation) = operation else {
                continue;
            };
            operation
                .responses
                .responses
                .entry("429".to_string())
                .or_insert_with(|| rate_limit_response().into());
            if !crate::server::is_exempt(Some(path)) {
                operation
                    .responses
                    .responses
                    .entry("408".to_string())
                    .or_insert_with(|| timeout_response().into());
            }
            if operation.request_body.is_some() {
                let limit = operation
                    .responses
                    .responses
                    .entry("413".to_string())
                    .or_insert_with(|| body_limit_response().into());
                // A route with its own 413 still meets the transport body limit first.
                if let RefOr::T(response) = limit
                    && !response.content.contains_key("text/plain")
                {
                    response
                        .content
                        .insert("text/plain".to_string(), body_limit_content());
                }
            }
            if needs_internal(path, method, operation) {
                operation
                    .responses
                    .responses
                    .entry("500".to_string())
                    .or_insert_with(|| internal_response(path).into());
            }
        }
    }
}

fn needs_internal(path: &str, method: &str, operation: &utoipa::openapi::path::Operation) -> bool {
    if operation.responses.responses.contains_key("500") {
        return false;
    }
    if path.starts_with("/ga4gh/drs/") {
        return !matches!(
            (path, method),
            ("/ga4gh/drs/v1/service-info", "GET")
                | ("/ga4gh/drs/v1/objects/{object_id}", "OPTIONS")
        );
    }
    if path.starts_with("/ga4gh/tes/") {
        return !matches!((path, method), ("/ga4gh/tes/v1/service-info", "GET"));
    }
    operation.responses.responses.values().any(|response| {
        let RefOr::T(response) = response else {
            return false;
        };
        response.content.values().any(|content| {
            matches!(
                content.schema.as_ref(),
                Some(RefOr::Ref(reference))
                    if reference.ref_location.ends_with("/ErrorResponse")
            )
        })
    })
}

fn error_body(schema: &str, example: serde_json::Value) -> Content {
    let mut content = Content::new(Some(Ref::from_schema_name(schema)));
    content.example = Some(example);
    content
}

fn rate_limit_response() -> Response {
    ResponseBuilder::new()
        .description("Request rate exceeded; retry after the number of seconds in `Retry-After`")
        .content(
            "application/json",
            error_body(
                "ErrorResponse",
                json!({"error": "too many requests", "code": "rate_limited"}),
            ),
        )
        .header("Retry-After", Header::new(Object::with_type(Type::String)))
        .build()
}

fn timeout_response() -> Response {
    ResponseBuilder::new()
        .description("The request exceeded the REST request time limit; the response body is empty")
        .build()
}

fn body_limit_response() -> Response {
    ResponseBuilder::new()
        .description("The request body exceeded the configured limit")
        .content("text/plain", body_limit_content())
        .build()
}

fn body_limit_content() -> Content {
    let mut text = Content::new(Some(Object::with_type(Type::String)));
    text.example = Some(json!("Failed to buffer the request body"));
    text
}

fn internal_response(path: &str) -> Response {
    let (schema, example) = if path.starts_with("/ga4gh/drs/") {
        (
            "DrsErrorPayload",
            json!({"status_code": 500, "msg": "internal server error"}),
        )
    } else if path.starts_with("/ga4gh/tes/") {
        (
            "TesErrorPayload",
            json!({"status_code": 500, "msg": "internal server error"}),
        )
    } else {
        (
            "ErrorResponse",
            json!({"error": "Internal server error", "code": "Internal error"}),
        )
    };
    ResponseBuilder::new()
        .description("Unexpected internal failure")
        .content("application/json", error_body(schema, example))
        .build()
}

#[cfg(test)]
#[path = "openapi_tests.rs"]
mod tests;
