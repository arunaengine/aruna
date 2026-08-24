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

**Conventions**
- Errors answer `application/json` with `ErrorResponse` (`error` plus an optional `code`); the
  GA4GH DRS and TES facades use their own error payloads.
- Every operation may answer 429 with a `Retry-After` header.
- An operation may answer 408 when the request exceeds the REST time limit; the streaming RO-Crate
  upload does not.
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
            if path != "/metadata/rocrate/uploads" {
                operation
                    .responses
                    .responses
                    .entry("408".to_string())
                    .or_insert_with(|| timeout_response().into());
            }
            if operation.request_body.is_some() {
                operation
                    .responses
                    .responses
                    .entry("413".to_string())
                    .or_insert_with(|| body_limit_response().into());
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
    let mut response =
        ResponseBuilder::new().description("The request body exceeded the configured limit");
    let mut text = Content::new(Some(Object::with_type(Type::String)));
    text.example = Some(json!("Failed to buffer the request body"));
    response = response.content("text/plain", text);
    response.build()
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
mod tests {
    use super::ApiDoc;
    use serde_json::{Value, json};
    use std::collections::BTreeSet;

    const METHODS: &[&str] = &[
        "get", "put", "post", "delete", "options", "head", "patch", "trace",
    ];

    /// Section labels a description may carry, in their only allowed order.
    const SECTIONS: &[&str] = &[
        "**Authentication**",
        "**Behavior**",
        "**Limits**",
        "**Errors**",
    ];

    const MAX_SUMMARY_WORDS: usize = 8;
    const MAX_LEAD: usize = 200;
    const MAX_PARAGRAPH: usize = 400;

    /// Example values that would mean a real credential leaked into the document.
    const FORBIDDEN: &[&str] = &[
        "-----BEGIN",
        "eyJhbGciOi",
        "AKIA",
        "aruna_secret",
        "onboarding_secret_",
    ];

    /// Operations that serve anonymous callers but return more to an authenticated one.
    /// They pair an empty requirement with `bearer_auth` so clients send an available
    /// token without presenting the operation as authentication-only.
    const OPTIONAL_AUTH: &[(&str, &str)] = &[
        ("/metadata", "get"),
        ("/groups/{group_id}/metadata", "get"),
        ("/metadata/{document_id}", "get"),
        ("/metadata/{document_id}/rocrate", "get"),
        ("/metadata/{document_id}/sparql/query", "post"),
        ("/metadata/sparql/query", "post"),
        ("/metadata/search", "get"),
        ("/info", "get"),
        ("/info/realm", "get"),
        ("/ga4gh/drs/v1/objects", "post"),
        ("/ga4gh/drs/v1/objects/{object_id}", "get"),
        ("/ga4gh/drs/v1/download", "get"),
    ];

    /// Operations that reject an anonymous caller.
    const REQUIRED_AUTH: &[(&str, &str)] = &[
        ("/users/register", "post"),
        ("/metadata", "post"),
        ("/metadata/{document_id}", "delete"),
        ("/metadata/references", "get"),
        ("/audit", "get"),
        ("/admin/compute/config", "get"),
        ("/admin/compute/config", "put"),
        ("/admin/compute/drain", "post"),
        ("/admin/compute/snapshots", "get"),
        ("/admin/placement-quarantine", "post"),
        ("/admin/placement-policies", "post"),
        ("/admin/placement-policies", "get"),
        ("/admin/placement-policies/{policy_id}", "get"),
        ("/admin/placement-diagnostics", "get"),
        ("/buckets/{bucket}/placement", "get"),
        ("/buckets/{bucket}/placement", "put"),
        ("/buckets/{bucket}/placement/objects", "post"),
        ("/buckets/{bucket}/placement/runs", "post"),
        ("/buckets/{bucket}/placement/coverage", "get"),
        ("/jobs/{job_id}/audit", "get"),
    ];

    fn operation_security(doc: &Value, path: &str, method: &str) -> Vec<Value> {
        doc["paths"][path][method]["security"]
            .as_array()
            .unwrap_or_else(|| panic!("{method} {path} declares no security requirement"))
            .clone()
    }

    /// Follows `$ref` chains through the document. A cycle resolves to nothing,
    /// so a self-referential `$ref` can never pass as documentation.
    fn resolve<'a>(doc: &'a Value, node: &'a Value) -> Option<&'a Value> {
        let mut visited = BTreeSet::new();
        let mut current = node;
        while let Some(reference) = current.get("$ref").and_then(Value::as_str) {
            if !visited.insert(reference.to_owned()) {
                return None;
            }
            let mut target = doc;
            for segment in reference.trim_start_matches("#/").split('/') {
                target = target.get(segment)?;
            }
            current = target;
        }
        Some(current)
    }

    fn operations(doc: &Value) -> Vec<(String, &Value)> {
        let mut operations = Vec::new();
        let Some(paths) = doc.get("paths").and_then(Value::as_object) else {
            return operations;
        };
        for (path, item) in paths {
            for method in METHODS {
                if let Some(operation) = item.get(method) {
                    operations.push((format!("{} {path}", method.to_uppercase()), operation));
                }
            }
        }
        operations
    }

    fn filled(value: Option<&Value>) -> bool {
        value
            .and_then(Value::as_str)
            .is_some_and(|text| !text.trim().is_empty())
    }

    /// JSON media entries of a request body or response. Other media types are
    /// fully described but carry no JSON body example.
    fn json_media(carrier: &Value) -> Vec<(&String, &Value)> {
        carrier
            .get("content")
            .and_then(Value::as_object)
            .map(|content| {
                content
                    .iter()
                    .filter(|(media_type, _)| media_type.contains("json"))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn example_values(doc: &Value, media: &Value) -> Vec<Value> {
        let mut values = Vec::new();
        if let Some(example) = media.get("example") {
            values.push(example.clone());
        }
        if let Some(examples) = media.get("examples").and_then(Value::as_object) {
            for example in examples.values() {
                if let Some(value) = resolve(doc, example).and_then(|item| item.get("value")) {
                    values.push(value.clone());
                }
            }
        }
        if let Some(schema) = media.get("schema").and_then(|schema| resolve(doc, schema))
            && let Some(example) = schema.get("example")
        {
            values.push(example.clone());
        }
        values
    }

    fn schema_matches(doc: &Value, schema: &Value, value: &Value) -> bool {
        let Some(schema) = resolve(doc, schema) else {
            return false;
        };
        if let Some(values) = schema.get("enum").and_then(Value::as_array)
            && !values.iter().any(|item| item == value)
        {
            return false;
        }
        if let Some(value_type) = schema.get("type") {
            let matches = match value_type {
                Value::String(value_type) => type_matches(value_type, value),
                Value::Array(types) => types
                    .iter()
                    .filter_map(Value::as_str)
                    .any(|value_type| type_matches(value_type, value)),
                _ => true,
            };
            if !matches {
                return false;
            }
        }
        composition_matches(doc, schema, value)
            && object_matches(doc, schema, value)
            && array_matches(doc, schema, value)
    }

    fn composition_matches(doc: &Value, schema: &Value, value: &Value) -> bool {
        if let Some(one_of) = schema.get("oneOf").and_then(Value::as_array)
            && one_of
                .iter()
                .filter(|item| schema_matches(doc, item, value))
                .count()
                != 1
        {
            return false;
        }
        if let Some(any_of) = schema.get("anyOf").and_then(Value::as_array)
            && !any_of.iter().any(|item| schema_matches(doc, item, value))
        {
            return false;
        }
        if let Some(all_of) = schema.get("allOf").and_then(Value::as_array)
            && !all_of.iter().all(|item| schema_matches(doc, item, value))
        {
            return false;
        }
        true
    }

    fn object_matches(doc: &Value, schema: &Value, value: &Value) -> bool {
        let (Some(properties), Some(object)) = (
            schema.get("properties").and_then(Value::as_object),
            value.as_object(),
        ) else {
            return true;
        };
        if schema
            .get("required")
            .and_then(Value::as_array)
            .is_some_and(|required| {
                required
                    .iter()
                    .any(|name| name.as_str().is_none_or(|name| !object.contains_key(name)))
            })
        {
            return false;
        }
        if properties.iter().any(|(name, property)| {
            object
                .get(name)
                .is_some_and(|value| !schema_matches(doc, property, value))
        }) {
            return false;
        }
        schema.get("additionalProperties") != Some(&Value::Bool(false))
            || object.keys().all(|name| properties.contains_key(name))
    }

    fn array_matches(doc: &Value, schema: &Value, value: &Value) -> bool {
        let (Some(items), Some(array)) = (schema.get("items"), value.as_array()) else {
            return true;
        };
        array.iter().all(|value| schema_matches(doc, items, value))
    }

    fn type_matches(value_type: &str, value: &Value) -> bool {
        match value_type {
            "object" => value.is_object(),
            "array" => value.is_array(),
            "string" => value.is_string(),
            "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
            "number" => value.is_number(),
            "boolean" => value.is_boolean(),
            "null" => value.is_null(),
            _ => true,
        }
    }

    fn has_example(doc: &Value, media: &Value) -> bool {
        let values = example_values(doc, media);
        if values.is_empty() {
            return false;
        }
        let Some(schema) = media.get("schema") else {
            return values.iter().all(|value| {
                !value.is_null() && !value.as_str().is_some_and(|text| text.is_empty())
            });
        };
        values.iter().all(|value| {
            !value.is_null()
                && !value.as_str().is_some_and(|text| text.is_empty())
                && schema_matches(doc, schema, value)
                && non_empty(doc, schema, value)
        })
    }

    /// An example that shows nothing: `{}` where the schema promises fields, `[]`
    /// for an array. A free-form object body may legitimately carry `{}`.
    fn non_empty(doc: &Value, schema: &Value, value: &Value) -> bool {
        let Some(schema) = resolve(doc, schema) else {
            return true;
        };
        let promises_fields = schema
            .get("properties")
            .and_then(Value::as_object)
            .is_some_and(|properties| !properties.is_empty())
            || schema.get("required").is_some();
        if promises_fields && value.as_object().is_some_and(|fields| fields.is_empty()) {
            return false;
        }
        schema.get("type") != Some(&json!("array"))
            || !value.as_array().is_some_and(|items| items.is_empty())
    }

    /// Operations missing a summary, description, parameter or response text.
    fn text_gaps(doc: &Value) -> Vec<String> {
        let mut gaps = Vec::new();
        for (name, operation) in operations(doc) {
            if !filled(operation.get("summary")) {
                gaps.push(format!("{name}: missing summary"));
            }
            if !filled(operation.get("description")) {
                gaps.push(format!("{name}: missing description"));
            }
            for parameter in operation
                .get("parameters")
                .and_then(Value::as_array)
                .map(Vec::as_slice)
                .unwrap_or_default()
            {
                if !resolve(doc, parameter)
                    .is_some_and(|parameter| filled(parameter.get("description")))
                {
                    let named = parameter
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or("unnamed");
                    gaps.push(format!("{name}: parameter {named} without description"));
                }
            }
            let Some(responses) = operation.get("responses").and_then(Value::as_object) else {
                gaps.push(format!("{name}: declares no response"));
                continue;
            };
            for (status, response) in responses {
                if !resolve(doc, response)
                    .is_some_and(|response| filled(response.get("description")))
                {
                    gaps.push(format!("{name}: response {status} without description"));
                }
            }
        }
        gaps
    }

    /// Tag names the document describes, so an operation cannot reference a bare tag.
    fn described_tags(doc: &Value) -> BTreeSet<&str> {
        doc.get("tags")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default()
            .iter()
            .filter(|tag| filled(tag.get("description")))
            .filter_map(|tag| tag.get("name").and_then(Value::as_str))
            .collect()
    }

    /// Blank-line separated paragraphs, keeping each paragraph's own line breaks.
    fn split_paragraphs(text: &str) -> Vec<String> {
        let mut paragraphs = Vec::new();
        let mut current: Vec<&str> = Vec::new();
        for line in text.lines() {
            if line.trim().is_empty() {
                if !current.is_empty() {
                    paragraphs.push(current.join("\n"));
                    current.clear();
                }
            } else {
                current.push(line);
            }
        }
        if !current.is_empty() {
            paragraphs.push(current.join("\n"));
        }
        paragraphs
    }

    fn snippet(text: &str) -> String {
        text.chars().take(40).collect()
    }

    /// A long paragraph is only allowed as a bullet list under its label.
    fn bulleted(paragraph: &str) -> bool {
        paragraph.lines().all(|line| {
            let body = line.trim_start();
            body.starts_with("- ") || body.starts_with("**") || line.starts_with("  ")
        })
    }

    fn prose_gaps(name: &str, field: &str, text: &str, gaps: &mut Vec<String>) {
        if text.contains('\u{2014}') {
            gaps.push(format!("{name}: {field} contains an em dash"));
        }
        if text.contains(['\u{2018}', '\u{2019}', '\u{201c}', '\u{201d}']) {
            gaps.push(format!("{name}: {field} contains curly quotes"));
        }
    }

    fn summary_gaps(name: &str, summary: &str, gaps: &mut Vec<String>) {
        let words = summary.split_whitespace().count();
        if !(1..=MAX_SUMMARY_WORDS).contains(&words) {
            gaps.push(format!(
                "{name}: summary has {words} words (allowed 1 to {MAX_SUMMARY_WORDS})"
            ));
        }
        if summary.trim_end().ends_with('.') {
            gaps.push(format!("{name}: summary ends with a period"));
        }
        prose_gaps(name, "summary", summary, gaps);
    }

    fn section_gaps(name: &str, paragraphs: &[String], gaps: &mut Vec<String>) {
        let mut found = Vec::new();
        for paragraph in paragraphs {
            let paragraph = paragraph.trim_start();
            if !paragraph.starts_with("**") {
                continue;
            }
            match SECTIONS
                .iter()
                .position(|label| paragraph.starts_with(*label))
            {
                Some(label) => found.push(label),
                None => gaps.push(format!(
                    "{name}: unknown section label: {}",
                    snippet(paragraph)
                )),
            }
        }
        if !found.contains(&0) {
            gaps.push(format!("{name}: missing **Authentication** section"));
        }
        if found.windows(2).any(|pair| pair[0] >= pair[1]) {
            let labels: Vec<&str> = found.iter().map(|label| SECTIONS[*label]).collect();
            gaps.push(format!(
                "{name}: sections out of order or repeated: {}",
                labels.join(" ")
            ));
        }
    }

    fn description_gaps(name: &str, description: &str, gaps: &mut Vec<String>) {
        prose_gaps(name, "description", description, gaps);
        let paragraphs = split_paragraphs(description);
        let Some(lead) = paragraphs.first().map(|lead| lead.trim()) else {
            gaps.push(format!("{name}: description is empty"));
            return;
        };
        let length = lead.chars().count();
        if lead.contains('\n')
            || !lead.ends_with('.')
            || length > MAX_LEAD
            || lead.starts_with("**")
        {
            gaps.push(format!(
                "{name}: lead must be one sentence of at most {MAX_LEAD} chars ending with '.' \
                 ({length} chars)"
            ));
        }
        for paragraph in &paragraphs {
            if paragraph.chars().count() > MAX_PARAGRAPH && !bulleted(paragraph) {
                gaps.push(format!(
                    "{name}: paragraph over {MAX_PARAGRAPH} chars is not a bullet list: {}",
                    snippet(paragraph)
                ));
            }
        }
        section_gaps(name, &paragraphs[1..], gaps);
    }

    /// Operation prose that does not follow the structured documentation convention.
    fn structure_gaps(doc: &Value) -> Vec<String> {
        let described = described_tags(doc);
        let mut gaps = Vec::new();
        for (name, operation) in operations(doc) {
            if let Some(summary) = operation.get("summary").and_then(Value::as_str) {
                summary_gaps(&name, summary, &mut gaps);
            }
            if let Some(description) = operation.get("description").and_then(Value::as_str) {
                description_gaps(&name, description, &mut gaps);
            }
            for tag in operation
                .get("tags")
                .and_then(Value::as_array)
                .map(Vec::as_slice)
                .unwrap_or_default()
                .iter()
                .filter_map(Value::as_str)
            {
                if !described.contains(tag) {
                    gaps.push(format!("{name}: tag {tag} without description"));
                }
            }
        }
        gaps
    }

    /// JSON bodies without a usable example. Exemptions follow from the status and
    /// the media type, never from a list of endpoints.
    fn example_gaps(doc: &Value) -> Vec<String> {
        let mut gaps = Vec::new();
        for (name, operation) in operations(doc) {
            if let Some(body) = operation
                .get("requestBody")
                .and_then(|body| resolve(doc, body))
            {
                for (media_type, media) in json_media(body) {
                    if !has_example(doc, media) {
                        gaps.push(format!(
                            "{name}: request {media_type} without a usable example"
                        ));
                    }
                }
            }
            for (status, response) in operation
                .get("responses")
                .and_then(Value::as_object)
                .into_iter()
                .flatten()
            {
                if status == "204" || status.starts_with('3') {
                    continue;
                }
                let Some(response) = resolve(doc, response) else {
                    continue;
                };
                for (media_type, media) in json_media(response) {
                    if !has_example(doc, media) {
                        gaps.push(format!(
                            "{name}: {status} {media_type} without a usable example"
                        ));
                    }
                }
            }
        }
        gaps
    }

    fn collect_examples(node: &Value, found: &mut Vec<String>) {
        match node {
            Value::Object(fields) => {
                for (key, value) in fields {
                    if key == "example" || key == "examples" {
                        found.push(value.to_string());
                    }
                    collect_examples(value, found);
                }
            }
            Value::Array(items) => {
                for item in items {
                    collect_examples(item, found);
                }
            }
            _ => {}
        }
    }

    #[test]
    fn pins_optional_security() {
        let doc = serde_json::to_value(ApiDoc::openapi()).unwrap();
        for (path, method) in OPTIONAL_AUTH {
            let security = operation_security(&doc, path, method);
            assert!(
                security.contains(&json!({ "bearer_auth": [] })),
                "{method} {path} must offer bearer_auth so clients send a held token"
            );
            assert!(
                security.contains(&json!({})),
                "{method} {path} must keep the empty requirement that marks auth optional"
            );
        }
    }

    #[test]
    fn pins_required_security() {
        let doc = serde_json::to_value(ApiDoc::openapi()).unwrap();
        for (path, method) in REQUIRED_AUTH {
            let security = operation_security(&doc, path, method);
            assert!(
                security.contains(&json!({ "bearer_auth": [] })),
                "{method} {path} must require bearer_auth"
            );
            assert!(
                !security.contains(&json!({})),
                "{method} {path} rejects anonymous callers, so auth must not read as optional"
            );
        }
    }

    #[test]
    fn declares_security_schemes() {
        let doc = serde_json::to_value(ApiDoc::openapi()).unwrap();
        let schemes = &doc["components"]["securitySchemes"];
        assert_eq!(schemes["bearer_auth"]["scheme"], json!("bearer"));
        assert_eq!(schemes["basic_auth"]["scheme"], json!("basic"));
    }

    #[test]
    fn requires_operation_docs() {
        let doc = serde_json::to_value(ApiDoc::openapi()).unwrap();
        let gaps = text_gaps(&doc);
        assert!(
            gaps.is_empty(),
            "undocumented operations:\n{}",
            gaps.join("\n")
        );
        let gaps = structure_gaps(&doc);
        assert!(
            gaps.is_empty(),
            "operation docs off the convention:\n{}",
            gaps.join("\n")
        );
    }

    #[test]
    fn requires_body_examples() {
        let doc = serde_json::to_value(ApiDoc::openapi()).unwrap();
        let gaps = example_gaps(&doc);
        assert!(
            gaps.is_empty(),
            "bodies without a usable example:\n{}",
            gaps.join("\n")
        );
    }

    #[test]
    fn pins_identity() {
        let doc = serde_json::to_value(ApiDoc::openapi()).unwrap();
        assert_eq!(doc["info"]["version"], json!(env!("CARGO_PKG_VERSION")));
        assert_eq!(
            doc["servers"],
            json!([{ "url": "/api/v1", "description": "REST API v1" }])
        );
    }

    #[test]
    fn pins_transport_docs() {
        let doc = serde_json::to_value(ApiDoc::openapi()).unwrap();
        for (path, item) in doc["paths"].as_object().unwrap() {
            for method in METHODS {
                let Some(operation) = item.get(*method) else {
                    continue;
                };
                let responses = operation["responses"].as_object().unwrap();
                let limited = &responses["429"];
                assert!(limited["content"]["application/json"].is_object());
                assert!(limited["headers"]["Retry-After"].is_object());
                if path != "/metadata/rocrate/uploads" {
                    assert!(responses["408"]["content"].is_null());
                } else {
                    assert!(responses.get("408").is_none());
                }
                if operation.get("requestBody").is_some() {
                    let media_type = if path == "/metadata/rocrate/uploads" {
                        "application/json"
                    } else {
                        "text/plain"
                    };
                    assert!(responses["413"]["content"][media_type].is_object());
                }
            }
        }
        assert!(
            doc["paths"]["/users/register"]["post"]["responses"]["500"]["content"][
                "application/json"
            ]
            .is_object()
        );
        assert!(
            doc["paths"]["/ga4gh/tes/v1/tasks/{id}:cancel"]["post"]["responses"]["500"]["content"]
                ["application/json"]
                .is_object()
        );
    }

    fn endpoint_addr() -> iroh::EndpointAddr {
        let id = iroh::PublicKey::from_bytes(&[
            0x2b, 0x3c, 0x4d, 0x5e, 0x6f, 0x70, 0x81, 0x92, 0xa3, 0xb4, 0xc5, 0xd6, 0xe7, 0xf8,
            0x09, 0x1a, 0x2b, 0x3c, 0x4d, 0x5e, 0x6f, 0x70, 0x81, 0x92, 0xa3, 0xb4, 0xc5, 0xd6,
            0xe7, 0xf8, 0x09, 0x1a,
        ])
        .unwrap();
        iroh::EndpointAddr::from_parts(
            id,
            [
                iroh::TransportAddr::Relay("https://relay.example.test/".parse().unwrap()),
                iroh::TransportAddr::Ip("192.0.2.10:4433".parse().unwrap()),
            ],
        )
    }

    async fn response_value(response: axum::response::Response) -> Value {
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    fn accepts_example(doc: &Value, path: &str, method: &str, status: &str, runtime: &Value) {
        let media = &doc["paths"][path][method]["responses"][status]["content"]["application/json"];
        assert_eq!(media["example"], *runtime);
        assert!(schema_matches(doc, &media["schema"], runtime));
    }

    fn normalize_addrs(value: &Value) -> Value {
        let mut value = value.clone();
        // Iroh stores addresses in a set; their JSON array order is not contractual.
        if let Some(addrs) = value["temporary_bootstrap_endpoint"]["addrs"].as_array_mut() {
            addrs.sort_by_key(|item| item.to_string());
        }
        value
    }

    fn align_policy_ids(mut value: Value, example: &Value) -> Value {
        // Policy ids are generated at runtime; the documentation uses stable examples.
        let Some(actual) = value["trace"].as_array_mut() else {
            return value;
        };
        let Some(expected) = example["trace"].as_array() else {
            return value;
        };
        for (actual, expected) in actual.iter_mut().zip(expected) {
            if let Some(policy_id) = expected.get("policy_id") {
                actual["policy_id"] = policy_id.clone();
            }
        }
        value
    }

    fn policy_response() -> crate::routes::policies::DryRunResponse {
        use crate::routes::policies::{DryRunResponse, ScopedTraceEntry};
        use aruna_core::request_policy::{PolicyKind, PolicyResult, PolicyTraceEntry};

        DryRunResponse {
            denied: true,
            matched_scope: Some("group(01JABCDEF0123456789ABCDEFG)".to_string()),
            policy_name: Some("read-only-group".to_string()),
            reason: Some("policy matched".to_string()),
            trace: vec![
                ScopedTraceEntry {
                    scope: "realm".to_string(),
                    entry: PolicyTraceEntry {
                        policy_id: ulid::Ulid::from_bytes([7; 16]),
                        name: "no-admin-writes".to_string(),
                        kind: PolicyKind::Deny,
                        applicable: true,
                        result: PolicyResult::Passed,
                        detail: None,
                    },
                },
                ScopedTraceEntry {
                    scope: "group(01JABCDEF0123456789ABCDEFG)".to_string(),
                    entry: PolicyTraceEntry {
                        policy_id: ulid::Ulid::from_bytes([8; 16]),
                        name: "read-only-group".to_string(),
                        kind: PolicyKind::Deny,
                        applicable: true,
                        result: PolicyResult::Denied,
                        detail: Some("policy matched".to_string()),
                    },
                },
            ],
        }
    }

    #[test]
    fn accepts_typed_examples() {
        let doc = serde_json::to_value(ApiDoc::openapi()).unwrap();
        let error = serde_json::to_value(crate::error::ErrorResponse::new("synthetic")).unwrap();
        assert!(schema_matches(
            &doc,
            &doc["components"]["schemas"]["ErrorResponse"],
            &error
        ));
        let endpoint = crate::routes::onboarding::BootstrapEndpointDoc {
            id: iroh::SecretKey::from_bytes(&[8; 32]).public().to_string(),
            addrs: vec![
                crate::routes::onboarding::TransportAddressDoc::Relay(
                    "https://relay.example.test/".to_string(),
                ),
                crate::routes::onboarding::TransportAddressDoc::Ip("192.0.2.10:4433".to_string()),
            ],
        };
        let endpoint = serde_json::to_value(endpoint).unwrap();
        let actual = iroh::EndpointAddr::from_parts(
            iroh::SecretKey::from_bytes(&[8; 32]).public(),
            [
                iroh::TransportAddr::Relay("https://relay.example.test/".parse().unwrap()),
                iroh::TransportAddr::Ip("192.0.2.10:4433".parse().unwrap()),
            ],
        );
        assert_eq!(serde_json::to_value(actual).unwrap(), endpoint);
        assert!(schema_matches(
            &doc,
            &doc["components"]["schemas"]["BootstrapEndpointDoc"],
            &endpoint
        ));
        let trace = crate::routes::policies::ScopedTraceEntry {
            scope: "realm".to_string(),
            entry: aruna_core::request_policy::PolicyTraceEntry {
                policy_id: ulid::Ulid::from_bytes([7; 16]),
                name: "synthetic".to_string(),
                kind: aruna_core::request_policy::PolicyKind::Deny,
                applicable: true,
                result: aruna_core::request_policy::PolicyResult::Denied,
                detail: Some("matched".to_string()),
            },
        };
        let trace = serde_json::to_value(trace).unwrap();
        assert_eq!(trace["kind"], json!("Deny"));
        assert!(schema_matches(
            &doc,
            &doc["components"]["schemas"]["ScopedTraceEntry"],
            &trace
        ));
    }

    #[tokio::test]
    async fn accepts_runtime_examples() {
        let doc = serde_json::to_value(ApiDoc::openapi()).unwrap();
        let cancel = response_value(crate::routes::tes::cancel_response()).await;
        accepts_example(
            &doc,
            "/ga4gh/tes/v1/tasks/{id}:cancel",
            "post",
            "200",
            &cancel,
        );

        let drs = response_value(crate::routes::drs::authorizations_response()).await;
        accepts_example(
            &doc,
            "/ga4gh/drs/v1/objects/{object_id}",
            "options",
            "200",
            &drs,
        );

        let onboarding =
            serde_json::to_value(aruna_core::onboarding::BootstrapOnboardingResponse {
                realm_id: "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8".to_string(),
                mode: aruna_core::onboarding::OnboardingMode::Server,
                temporary_bootstrap_endpoint: endpoint_addr(),
                wrapped_realm_private_key: None,
                wrapped_realm_private_key_nonce: None,
                wrapping_public_key: None,
                delegation_signature: Some("<realm-delegation-signature>".to_string()),
                onboarding_sync_ticket: "<one-time-onboarding-sync-ticket>".to_string(),
                realm_endpoints: vec![aruna_core::structs::StaticRealmEndpoint {
                    node_id: endpoint_addr().id.to_string(),
                    endpoint_addr: format!("{};ip:192.0.2.10:4433", endpoint_addr().id),
                }],
            })
            .unwrap();
        let onboarding_media = &doc["paths"]["/onboarding/bootstrap"]["post"]["responses"]["200"]["content"]
            ["application/json"];
        assert_eq!(
            normalize_addrs(&onboarding_media["example"]),
            normalize_addrs(&onboarding)
        );
        assert!(schema_matches(
            &doc,
            &onboarding_media["schema"],
            &onboarding
        ));

        let policy = serde_json::to_value(policy_response()).unwrap();
        let policy_media = &doc["paths"]["/policies/dry-run"]["post"]["responses"]["200"]["content"]
            ["application/json"];
        assert_eq!(
            align_policy_ids(policy.clone(), &policy_media["example"]),
            policy_media["example"]
        );
        assert!(schema_matches(&doc, &policy_media["schema"], &policy));
    }

    #[test]
    fn rejects_bad_examples() {
        let doc = json!({
            "paths": {
                "/thing": {
                    "post": {
                        "summary": "Create thing",
                        "description": "Creates a thing.",
                        "requestBody": {
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "required": ["id"],
                                        "properties": {"id": {"type": "string"}}
                                    },
                                    "example": {"id": 7}
                                }
                            }
                        },
                        "responses": {"200": {"description": "Created"}}
                    }
                }
            }
        });
        let media = &doc["paths"]["/thing"]["post"]["requestBody"]["content"]["application/json"];
        assert!(!has_example(&doc, media));
        assert!(
            example_gaps(&doc)
                .iter()
                .any(|gap| gap.contains("without a usable example"))
        );
        let null_media = json!({"example": null});
        assert!(!has_example(&doc, &null_media));
    }

    #[test]
    fn rejects_missing_docs() {
        // The gates must fail a document that omits the contract, otherwise they
        // would pass an undocumented operation unnoticed.
        let doc = json!({
            "paths": {
                "/thing": {
                    "get": {
                        "parameters": [{ "name": "id", "in": "path" }],
                        "responses": {
                            "200": {
                                "content": {
                                    "application/json": { "schema": { "type": "object" } }
                                }
                            }
                        }
                    }
                }
            },
            "components": { "schemas": {} }
        });

        let gaps = text_gaps(&doc);
        assert!(gaps.iter().any(|gap| gap.contains("missing summary")));
        assert!(gaps.iter().any(|gap| gap.contains("missing description")));
        assert!(gaps.iter().any(|gap| gap.contains("parameter id")));
        assert!(
            gaps.iter()
                .any(|gap| gap.contains("response 200 without description"))
        );
        assert!(
            example_gaps(&doc)
                .iter()
                .any(|gap| gap.contains("without a usable example"))
        );

        let cyclic = json!({
            "paths": { "/thing": { "get": { "summary": "s", "description": "d",
                "responses": { "200": { "$ref": "#/components/responses/Loop" } } } } },
            "components": { "responses": { "Loop": { "$ref": "#/components/responses/Loop" } } }
        });
        assert!(
            text_gaps(&cyclic)
                .iter()
                .any(|gap| gap.contains("response 200 without description")),
            "a $ref cycle must not count as documentation"
        );
    }

    #[test]
    fn rejects_flat_docs() {
        // The structure gate must fail an unwrapped description, a mis-ordered
        // section list, an overlong summary, a bare tag and an empty example.
        let lead = "Creates a thing and inlines every rule, limit and status in one paragraph. "
            .repeat(12);
        let description = format!(
            "{lead}Refusals are listed above \u{2014} not below.\n\n\
             **Limits**: at most one thing.\n\n**Authentication**: bearer token."
        );
        let doc = json!({
            "paths": {
                "/thing": {
                    "post": {
                        "tags": ["thing"],
                        "summary": "Create a thing and also explain the whole limit story in here.",
                        "description": description,
                        "responses": {
                            "200": {
                                "description": "Created",
                                "content": {
                                    "application/json": {
                                        "schema": {
                                            "type": "object",
                                            "properties": {"id": {"type": "string"}}
                                        },
                                        "example": {}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        let gaps = structure_gaps(&doc);
        assert!(gaps.iter().any(|gap| gap.contains("summary has 12 words")));
        assert!(
            gaps.iter()
                .any(|gap| gap.contains("summary ends with a period"))
        );
        assert!(gaps.iter().any(|gap| gap.contains("lead must be one")));
        assert!(gaps.iter().any(|gap| gap.contains("not a bullet list")));
        assert!(gaps.iter().any(|gap| gap.contains("out of order")));
        assert!(gaps.iter().any(|gap| gap.contains("contains an em dash")));
        assert!(
            gaps.iter()
                .any(|gap| gap.contains("tag thing without description"))
        );
        assert!(
            example_gaps(&doc)
                .iter()
                .any(|gap| gap.contains("without a usable example")),
            "an empty object documents no body"
        );
    }

    #[test]
    fn serializes_api_doc() {
        let doc = serde_json::to_value(ApiDoc::openapi()).unwrap();
        assert!(
            doc["openapi"]
                .as_str()
                .is_some_and(|version| version.starts_with('3'))
        );
        assert!(!operations(&doc).is_empty());
    }

    #[test]
    fn examples_omit_secrets() {
        let doc = serde_json::to_value(ApiDoc::openapi()).unwrap();
        let mut examples = Vec::new();
        collect_examples(&doc, &mut examples);
        assert!(!examples.is_empty());
        for example in examples {
            for marker in FORBIDDEN {
                assert!(
                    !example.contains(marker),
                    "example leaks a credential-shaped value: {example}"
                );
            }
        }
    }

    #[test]
    fn shares_route_builder() {
        // Swagger UI, the served document and the runtime router must come from
        // the one co-registered builder.
        let served = serde_json::to_value(ApiDoc::openapi()).unwrap();
        let registered = serde_json::to_value(crate::routes::rest_openapi()).unwrap();
        let names = |doc: &Value| {
            operations(doc)
                .into_iter()
                .map(|(name, _)| name)
                .collect::<BTreeSet<_>>()
        };
        let served_names = names(&served);
        assert!(!served_names.is_empty());
        assert_eq!(served_names, names(&registered));
    }

    #[test]
    fn documents_oai_contract() {
        // OAI-PMH answers XML for every verb, and its POST form is url-encoded.
        let doc = serde_json::to_value(ApiDoc::openapi()).unwrap();
        let read = &doc["paths"]["/oai"]["get"];
        assert!(read["responses"]["200"]["content"]["text/xml"].is_object());
        assert!(
            read["responses"]["200"]["content"]
                .get("application/json")
                .is_none()
        );
        let submit = &doc["paths"]["/oai"]["post"];
        assert!(submit["requestBody"]["content"]["application/x-www-form-urlencoded"].is_object());
        assert!(submit["responses"]["200"]["content"]["text/xml"].is_object());
    }

    #[test]
    fn documents_pid_contract() {
        // The landing route is public and redirects; withdrawal answers 204.
        let doc = serde_json::to_value(ApiDoc::openapi()).unwrap();
        let landing = &doc["paths"]["/pid/{document_id}"]["get"];
        assert!(landing["responses"]["302"]["content"].is_null());
        assert!(landing["responses"]["404"]["content"].is_null());
        assert!(landing["responses"]["410"]["content"]["application/json"].is_object());
        assert!(
            landing["security"]
                .as_array()
                .is_some_and(|security| security.contains(&json!({})))
        );
        assert!(doc["paths"]["/pid/{document_id}"]["post"].is_null());
        let withdraw = &doc["paths"]["/pid/{document_id}"]["delete"];
        assert!(withdraw["responses"]["204"]["content"].is_null());
    }
}
