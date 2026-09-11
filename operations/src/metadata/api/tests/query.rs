use super::*;

#[test]
fn deduplicates_select_rows_from_multiple_nodes() {
    let results = aggregate_query_results(
        vec![
            MetadataQueryResults::Solutions(vec![
                BTreeMap::from([(String::from("s"), String::from("<urn:a>"))]),
                BTreeMap::from([(String::from("s"), String::from("<urn:b>"))]),
            ]),
            MetadataQueryResults::Solutions(vec![BTreeMap::from([(
                String::from("s"),
                String::from("<urn:a>"),
            )])]),
        ],
        MetadataQueryForm::Select,
        None,
    )
    .unwrap();

    let MetadataQueryResults::Solutions(rows) = results else {
        panic!("expected solutions");
    };
    assert_eq!(rows.len(), 2);
}

#[test]
fn reapplies_select_limit_after_distributed_merge() {
    let results = aggregate_query_results(
        vec![
            MetadataQueryResults::Solutions(vec![
                BTreeMap::from([(String::from("s"), String::from("<urn:a>"))]),
                BTreeMap::from([(String::from("s"), String::from("<urn:b>"))]),
            ]),
            MetadataQueryResults::Solutions(vec![
                BTreeMap::from([(String::from("s"), String::from("<urn:c>"))]),
                BTreeMap::from([(String::from("s"), String::from("<urn:d>"))]),
            ]),
        ],
        MetadataQueryForm::Select,
        Some(3),
    )
    .unwrap();

    let MetadataQueryResults::Solutions(rows) = results else {
        panic!("expected solutions");
    };
    assert_eq!(rows.len(), 3);
}

#[test]
fn query_select_limit_reads_outermost_limit_only() {
    assert_eq!(
        query_select_limit("SELECT ?s WHERE { ?s ?p ?o } LIMIT 5"),
        Some(5)
    );
    assert_eq!(
        query_select_limit("SELECT ?s WHERE { ?s ?p ?o } LIMIT 7 OFFSET 3"),
        Some(7)
    );
    assert_eq!(query_select_limit("SELECT ?s WHERE { ?s ?p ?o }"), None);
    assert_eq!(
        query_select_limit("SELECT ?s WHERE { { SELECT ?s WHERE { ?s ?p ?o } LIMIT 5 } ?s ?p ?o }"),
        None
    );
    assert_eq!(query_select_limit("ASK WHERE { ?s ?p ?o }"), None);
    assert_eq!(query_select_limit("not sparql"), None);
}

#[test]
fn query_form_accepts_single_line_declarations() {
    assert_eq!(
        query_form("PREFIX ex: <https://example.org/> SELECT ?s WHERE { ?s ?p ?o }").unwrap(),
        MetadataQueryForm::Select
    );
    assert_eq!(
        query_form("BASE <https://example.org/> ASK WHERE { ?s ?p ?o }").unwrap(),
        MetadataQueryForm::Ask
    );
    assert_eq!(query_form("CONSTRUCT WHERE { ?s ?p ?o }"), None);
}

#[test]
fn query_validation_rejects_updates_and_service() {
    assert!(ensure_supported_query_form("SELECT ?s WHERE { ?s ?p ?o }").is_ok());
    assert!(ensure_supported_query_form("ASK WHERE { ?s ?p ?o }").is_ok());
    assert!(ensure_supported_query_form("INSERT DATA { <urn:s> <urn:p> <urn:o> }").is_err());
    assert!(
        ensure_supported_query_form(
            "SELECT ?s WHERE { SERVICE <https://example.org/sparql> { ?s ?p ?o } }"
        )
        .is_err()
    );
    assert!(
        ensure_supported_query_form(
            "ASK WHERE { FILTER EXISTS { SERVICE SILENT ?endpoint { ?s ?p ?o } } }"
        )
        .is_err()
    );
}

#[test]
fn distributed_query_validation_accepts_only_union_safe_forms() {
    assert!(distributed_query_is_union_safe("ASK WHERE { ?s ?p ?o }"));
    assert!(!distributed_query_is_union_safe(
        "ASK WHERE { ?s ?p ?o . ?s ?p2 ?o2 }"
    ));
    assert!(distributed_query_is_union_safe(
        "SELECT DISTINCT ?s WHERE { ?s ?p ?o } LIMIT 10"
    ));
    assert!(!distributed_query_is_union_safe(
        "SELECT ?s WHERE { ?s ?p ?o }"
    ));
    assert!(!distributed_query_is_union_safe(
        "SELECT DISTINCT ?s WHERE { ?s ?p ?o . ?s ?p2 ?o2 }"
    ));
    assert!(!distributed_query_is_union_safe(
        "SELECT DISTINCT ?s WHERE { ?s ?p ?o } OFFSET 1"
    ));
    assert!(!distributed_query_is_union_safe(
        "SELECT (COUNT(*) AS ?count) WHERE { ?s ?p ?o }"
    ));
}

#[test]
fn query_validation_enforces_byte_and_row_bounds() {
    assert!(ensure_supported_query_form(&" ".repeat(METADATA_QUERY_MAX_BYTES + 1)).is_err());
    assert!(
        ensure_supported_query_form(&format!(
            "SELECT ?s WHERE {{ ?s ?p ?o }} LIMIT {}",
            METADATA_QUERY_MAX_ROWS + 1
        ))
        .is_err()
    );
}
