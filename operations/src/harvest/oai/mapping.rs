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
}
