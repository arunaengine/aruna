use serde_json::{Map, Value};

use crate::harvest::oai::parse::OaiRecord;

/// RO-Crate context the metadata registry validates against.
const ROCRATE_CONTEXT: &str = "https://w3id.org/ro/crate/1.2/context";
/// DCMI Elements 1.1 namespace; each element becomes `{DC_ELEMENTS}{element}`.
const DC_ELEMENTS: &str = "http://purl.org/dc/elements/1.1/";

/// Map one `oai_dc` record to an RO-Crate JSON-LD document.
///
/// DEFAULT mapping (not locked, swappable): the record is one `./` Dataset
/// entity whose properties are the Dublin Core elements keyed by their DCMI term
/// IRI. Repeated elements become arrays; `name` mirrors the first title so the
/// document is human-labelled.
pub fn oai_dc_to_jsonld(record: &OaiRecord) -> String {
    let mut entity = Map::new();
    entity.insert("@id".to_string(), Value::String("./".to_string()));
    entity.insert("@type".to_string(), Value::String("Dataset".to_string()));

    let name = record
        .dc
        .iter()
        .find(|(element, _)| element == "title")
        .map(|(_, value)| value.clone())
        .unwrap_or_else(|| record.header.identifier.clone());
    entity.insert("name".to_string(), Value::String(name));

    for element in distinct_elements(record) {
        let values: Vec<Value> = record
            .dc
            .iter()
            .filter(|(name, _)| name == &element)
            .map(|(_, value)| Value::String(value.clone()))
            .collect();
        let key = format!("{DC_ELEMENTS}{element}");
        let value = if values.len() == 1 {
            values.into_iter().next().expect("length checked")
        } else {
            Value::Array(values)
        };
        entity.insert(key, value);
    }

    let document = serde_json::json!({
        "@context": ROCRATE_CONTEXT,
        "@graph": [Value::Object(entity)],
    });
    document.to_string()
}

/// Element local names in first-seen order, without duplicates.
fn distinct_elements(record: &OaiRecord) -> Vec<String> {
    let mut seen: Vec<String> = Vec::new();
    for (element, _) in &record.dc {
        if !seen.contains(element) {
            seen.push(element.clone());
        }
    }
    seen
}

/// Canonical Dublin Core element order for a stable oai_dc rendering.
const DC_ELEMENT_ORDER: [&str; 15] = [
    "title",
    "creator",
    "subject",
    "description",
    "publisher",
    "contributor",
    "date",
    "type",
    "format",
    "identifier",
    "source",
    "language",
    "relation",
    "coverage",
    "rights",
];

/// schema.org property (local name) -> Dublin Core element crosswalk.
const SCHEMA_CROSSWALK: [(&str, &str); 14] = [
    ("name", "title"),
    ("description", "description"),
    ("author", "creator"),
    ("creator", "creator"),
    ("datePublished", "date"),
    ("dateCreated", "date"),
    ("license", "rights"),
    ("keywords", "subject"),
    ("identifier", "identifier"),
    ("publisher", "publisher"),
    ("inLanguage", "language"),
    ("encodingFormat", "format"),
    ("isPartOf", "relation"),
    ("hasPart", "relation"),
];

/// Map a stored RO-Crate JSON-LD document to `oai_dc` element pairs, in canonical
/// Dublin Core order.
///
/// DEFAULT crosswalk (not locked, swappable), mirroring `oai_dc_to_jsonld`: a
/// harvested document carrying explicit DCMI-IRI properties round-trips
/// losslessly; a native schema.org document is down-projected with the standard
/// crosswalk. `oai_dc` is the lowest-common-denominator format, so a lossy
/// down-mapping is expected. Always yields at least a `title`.
pub fn jsonld_to_oai_dc(jsonld: &str, fallback_title: &str) -> Vec<(String, String)> {
    let mut collected: Vec<(&'static str, Vec<String>)> = DC_ELEMENT_ORDER
        .iter()
        .map(|element| (*element, Vec::new()))
        .collect();

    if let Ok(value) = serde_json::from_str::<Value>(jsonld)
        && let Some(entity) = main_entity(&value)
    {
        collect_dc_elements(entity, &mut collected);
    }

    if title_slot(&collected).is_empty() {
        push_element(&mut collected, "title", fallback_title.to_string());
    }

    let mut pairs = Vec::new();
    for (element, values) in &collected {
        for value in values {
            if !value.is_empty() {
                pairs.push((element.to_string(), value.clone()));
            }
        }
    }
    pairs
}

fn title_slot<'a>(collected: &'a [(&'static str, Vec<String>)]) -> &'a [String] {
    collected
        .iter()
        .find(|(element, _)| *element == "title")
        .map(|(_, values)| values.as_slice())
        .unwrap_or(&[])
}

fn push_element(collected: &mut [(&'static str, Vec<String>)], element: &str, value: String) {
    if let Some((_, values)) = collected.iter_mut().find(|(name, _)| *name == element) {
        values.push(value);
    }
}

/// The primary node: the RO-Crate root `./` Dataset, else the first non-descriptor
/// entity, else the value itself when it is a single node.
fn main_entity(value: &Value) -> Option<&Value> {
    let Some(graph) = value.get("@graph").and_then(Value::as_array) else {
        return value.is_object().then_some(value);
    };
    graph
        .iter()
        .find(|entity| entity.get("@id").and_then(Value::as_str) == Some("./"))
        .or_else(|| {
            graph.iter().find(|entity| {
                entity.get("@id").and_then(Value::as_str) != Some("ro-crate-metadata.json")
            })
        })
        .or_else(|| graph.first())
}

fn collect_dc_elements(entity: &Value, collected: &mut [(&'static str, Vec<String>)]) {
    let dcmi_present = DC_ELEMENT_ORDER
        .iter()
        .any(|element| entity.get(format!("{DC_ELEMENTS}{element}")).is_some());

    if dcmi_present {
        for element in DC_ELEMENT_ORDER {
            if let Some(value) = entity.get(format!("{DC_ELEMENTS}{element}")) {
                for extracted in extract_values(value) {
                    push_element(collected, element, extracted);
                }
            }
        }
        return;
    }

    for (schema_local, element) in SCHEMA_CROSSWALK {
        if let Some(value) = schema_property(entity, schema_local) {
            for extracted in extract_values(value) {
                push_element(collected, element, extracted);
            }
        }
    }
    if let Some(value) = entity.get("@type") {
        for extracted in extract_values(value) {
            push_element(collected, "type", extracted);
        }
    }
}

/// Look up a schema.org property by bare, prefixed, or full-IRI key form.
fn schema_property<'a>(entity: &'a Value, local: &str) -> Option<&'a Value> {
    for key in [
        local.to_string(),
        format!("schema:{local}"),
        format!("http://schema.org/{local}"),
        format!("https://schema.org/{local}"),
    ] {
        if let Some(value) = entity.get(&key) {
            return Some(value);
        }
    }
    None
}

/// Flatten a JSON-LD value into scalar strings, following `name`/`@value`/`@id`
/// for object references and expanding arrays.
fn extract_values(value: &Value) -> Vec<String> {
    match value {
        Value::String(text) => vec![text.clone()],
        Value::Bool(flag) => vec![flag.to_string()],
        Value::Number(number) => vec![number.to_string()],
        Value::Array(items) => items.iter().flat_map(extract_values).collect(),
        Value::Object(object) => object
            .get("name")
            .or_else(|| object.get("@value"))
            .or_else(|| object.get("@id"))
            .map(extract_values)
            .unwrap_or_default(),
        Value::Null => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::harvest::oai::parse::OaiHeader;

    fn record() -> OaiRecord {
        OaiRecord {
            header: OaiHeader {
                identifier: "oai:example.org:1".to_string(),
                datestamp: "2026-01-02".to_string(),
                deleted: false,
                sets: Vec::new(),
            },
            dc: vec![
                ("title".to_string(), "A dataset".to_string()),
                ("creator".to_string(), "Alice".to_string()),
                ("creator".to_string(), "Bob".to_string()),
            ],
        }
    }

    #[test]
    fn maps_elements_to_dcmi_iris_and_arrays_repeats() {
        let jsonld = oai_dc_to_jsonld(&record());
        let value: Value = serde_json::from_str(&jsonld).unwrap();
        let entity = &value["@graph"][0];

        assert_eq!(entity["@id"], "./");
        assert_eq!(entity["@type"], "Dataset");
        assert_eq!(entity["name"], "A dataset");
        assert_eq!(entity["http://purl.org/dc/elements/1.1/title"], "A dataset");
        assert_eq!(
            entity["http://purl.org/dc/elements/1.1/creator"],
            serde_json::json!(["Alice", "Bob"])
        );
    }

    #[test]
    fn name_falls_back_to_identifier() {
        let mut record = record();
        record.dc.clear();
        let value: Value = serde_json::from_str(&oai_dc_to_jsonld(&record)).unwrap();
        assert_eq!(value["@graph"][0]["name"], "oai:example.org:1");
    }

    #[test]
    fn harvested_dcmi_document_round_trips() {
        let jsonld = oai_dc_to_jsonld(&record());
        let pairs = jsonld_to_oai_dc(&jsonld, "urn:fallback");
        assert!(pairs.contains(&("title".to_string(), "A dataset".to_string())));
        assert_eq!(
            pairs
                .iter()
                .filter(|(element, _)| element == "creator")
                .count(),
            2
        );
    }

    #[test]
    fn native_schema_document_projects_to_dublin_core() {
        let jsonld = serde_json::json!({
            "@context": "https://w3id.org/ro/crate/1.2/context",
            "@graph": [{
                "@id": "./",
                "@type": "Dataset",
                "name": "Native crate",
                "author": [{ "@id": "#alice", "name": "Alice" }, "Bob"],
                "datePublished": "2026-01-02",
                "license": { "@id": "https://spdx.org/licenses/CC0-1.0" },
                "keywords": "genomics"
            }]
        })
        .to_string();

        let pairs = jsonld_to_oai_dc(&jsonld, "urn:fallback");
        assert!(pairs.contains(&("title".to_string(), "Native crate".to_string())));
        assert!(pairs.contains(&("creator".to_string(), "Alice".to_string())));
        assert!(pairs.contains(&("creator".to_string(), "Bob".to_string())));
        assert!(pairs.contains(&("date".to_string(), "2026-01-02".to_string())));
        assert!(pairs.contains(&(
            "rights".to_string(),
            "https://spdx.org/licenses/CC0-1.0".to_string()
        )));
        assert!(pairs.contains(&("subject".to_string(), "genomics".to_string())));
        assert!(pairs.contains(&("type".to_string(), "Dataset".to_string())));
    }

    #[test]
    fn malformed_jsonld_still_yields_title() {
        let pairs = jsonld_to_oai_dc("not json", "urn:fallback");
        assert_eq!(
            pairs,
            vec![("title".to_string(), "urn:fallback".to_string())]
        );
    }

    #[test]
    fn canonical_order_is_stable() {
        let jsonld = serde_json::json!({
            "@graph": [{ "@id": "./", "@type": "Dataset", "license": "CC0", "name": "N" }]
        })
        .to_string();
        let pairs = jsonld_to_oai_dc(&jsonld, "urn:fallback");
        let elements: Vec<&str> = pairs.iter().map(|(element, _)| element.as_str()).collect();
        // title precedes type precedes rights in canonical DC order.
        assert_eq!(elements, vec!["title", "type", "rights"]);
    }
}
