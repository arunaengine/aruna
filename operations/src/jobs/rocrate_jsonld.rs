use std::collections::HashMap;

use serde_json::{Map, Value};

pub(super) const RDF_TYPE_IRI: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
pub(super) const SCHEMA_MEDIA_IRI: &str = "http://schema.org/MediaObject";
pub(super) const SCHEMA_MEDIA_HTTPS_IRI: &str = "https://schema.org/MediaObject";

pub(super) fn is_file_type(value: &str) -> bool {
    matches!(value, SCHEMA_MEDIA_IRI | SCHEMA_MEDIA_HTTPS_IRI)
}

pub(super) struct JsonLdKeywords {
    terms: HashMap<String, Option<String>>,
}

impl JsonLdKeywords {
    pub(super) fn new(document: &Value) -> Self {
        let mut terms = HashMap::new();
        if let Some(context) = document.get("@context") {
            collect_terms(context, &mut terms);
        }
        Self { terms }
    }

    pub(super) fn is_id(&self, key: &str) -> bool {
        key == "@id"
            || self
                .terms
                .get(key)
                .is_some_and(|iri| iri.as_deref() == Some("@id"))
    }

    pub(super) fn is_graph(&self, key: &str) -> bool {
        key == "@graph"
            || self
                .terms
                .get(key)
                .is_some_and(|iri| iri.as_deref() == Some("@graph"))
    }

    pub(super) fn expands_to(&self, key: &str, values: &[&str]) -> bool {
        match self.terms.get(key) {
            Some(Some(iri)) => values.contains(&iri.as_str()),
            Some(None) => false,
            None => values.contains(&key),
        }
    }

    pub(super) fn term_matches(&self, term: &str, values: &[&str]) -> bool {
        match self.terms.get(term) {
            Some(Some(iri)) => values.contains(&iri.as_str()),
            Some(None) => false,
            None => true,
        }
    }

    pub(super) fn object_id<'a>(
        &self,
        object: &'a Map<String, Value>,
    ) -> Option<(&'a str, &'a str)> {
        object.iter().find_map(|(key, value)| {
            self.is_id(key)
                .then(|| value.as_str().map(|value| (key.as_str(), value)))
                .flatten()
        })
    }

    pub(super) fn graph_mut<'a>(&self, document: &'a mut Value) -> Option<&'a mut Vec<Value>> {
        document
            .as_object_mut()?
            .iter_mut()
            .find_map(|(key, value)| self.is_graph(key).then(|| value.as_array_mut()).flatten())
    }

    pub(super) fn graph<'a>(&self, document: &'a Value) -> Option<&'a Vec<Value>> {
        document
            .as_object()?
            .iter()
            .find_map(|(key, value)| self.is_graph(key).then(|| value.as_array()).flatten())
    }
}

fn collect_terms(context: &Value, terms: &mut HashMap<String, Option<String>>) {
    match context {
        Value::Array(values) => {
            for value in values {
                collect_terms(value, terms);
            }
        }
        Value::Object(values) => {
            for (term, definition) in values {
                let iri = match definition {
                    Value::String(iri) => Some(iri.as_str()),
                    Value::Object(definition) => definition.get("@id").and_then(Value::as_str),
                    _ => None,
                };
                terms.insert(term.clone(), iri.map(str::to_string));
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_file_iris() {
        assert!(is_file_type(SCHEMA_MEDIA_IRI));
        assert!(is_file_type(SCHEMA_MEDIA_HTTPS_IRI));
        assert!(!is_file_type("https://schema.org/Dataset"));
    }
}
