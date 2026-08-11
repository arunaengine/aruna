use utoipa::openapi::security::{Http, HttpAuthScheme, SecurityScheme};
use utoipa::{Modify, OpenApi};

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Aruna Server API",
        version = "3.0.0-alpha.1",
        description = "REST API for the Aruna federated data orchestration network",
        license(name = "Apache-2.0", url = "https://www.apache.org/licenses/LICENSE-2.0"),
        contact(name = "Aruna Team", url = "https://github.com/arunaengine/aruna")
    ),
    servers(
        (url = "/api/v1", description = "REST API v1"),
        (url = "/", description = "Admin API")
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

#[cfg(test)]
mod tests {
    use super::ApiDoc;
    use serde_json::{Value, json};
    use std::collections::BTreeSet;

    const METHODS: &[&str] = &[
        "get", "put", "post", "delete", "options", "head", "patch", "trace",
    ];

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

    fn has_example(doc: &Value, media: &Value) -> bool {
        if media.get("example").is_some() {
            return true;
        }
        if media
            .get("examples")
            .and_then(Value::as_object)
            .is_some_and(|examples| !examples.is_empty())
        {
            return true;
        }
        media
            .get("schema")
            .and_then(|schema| resolve(doc, schema))
            .is_some_and(|schema| schema.get("example").is_some())
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

    /// JSON bodies without an example. Exemptions follow from the status and the
    /// media type, never from a list of endpoints.
    fn example_gaps(doc: &Value) -> Vec<String> {
        let mut gaps = Vec::new();
        for (name, operation) in operations(doc) {
            if let Some(body) = operation
                .get("requestBody")
                .and_then(|body| resolve(doc, body))
            {
                for (media_type, media) in json_media(body) {
                    if !has_example(doc, media) {
                        gaps.push(format!("{name}: request {media_type} without example"));
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
                        gaps.push(format!("{name}: {status} {media_type} without example"));
                    }
                }
            }
        }
        gaps
    }

    fn example_values(node: &Value, found: &mut Vec<String>) {
        match node {
            Value::Object(fields) => {
                for (key, value) in fields {
                    if key == "example" || key == "examples" {
                        found.push(value.to_string());
                    }
                    example_values(value, found);
                }
            }
            Value::Array(items) => {
                for item in items {
                    example_values(item, found);
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
    }

    #[test]
    fn requires_body_examples() {
        let doc = serde_json::to_value(ApiDoc::openapi()).unwrap();
        let gaps = example_gaps(&doc);
        assert!(
            gaps.is_empty(),
            "bodies without examples:\n{}",
            gaps.join("\n")
        );
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
                .any(|gap| gap.contains("without example"))
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
        example_values(&doc, &mut examples);
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
        let mint = &doc["paths"]["/pid/{document_id}"]["post"];
        assert!(mint["responses"]["202"]["content"]["application/json"].is_object());
        assert_eq!(
            operation_security(&doc, "/pid/{document_id}", "post"),
            vec![json!({ "bearer_auth": [] })]
        );
        let withdraw = &doc["paths"]["/pid/{document_id}"]["delete"];
        assert!(withdraw["responses"]["204"]["content"].is_null());
    }
}
