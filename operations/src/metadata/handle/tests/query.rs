use super::super::query::parse_metadata_query;
use super::super::*;
#[test]
fn query_validation_allows() {
    parse_metadata_query("SELECT ?s WHERE { ?s a schema:Dataset }")
        .expect("common metadata prefixes are available");
    parse_metadata_query("ASK WHERE { ?s ?p ?o }").expect("ASK is supported");

    for query in [
        "CONSTRUCT WHERE { ?s ?p ?o }",
        "INSERT DATA { <urn:s> <urn:p> <urn:o> }",
        "SELECT * WHERE { SERVICE <https://example.com/sparql> { ?s ?p ?o } }",
        "SELECT * WHERE { FILTER EXISTS { SERVICE <https://example.com/sparql> { ?s ?p ?o } } }",
    ] {
        assert!(
            matches!(
                parse_metadata_query(query),
                Err(MetadataError::InvalidInput(_))
            ),
            "query should be rejected: {query}"
        );
    }
}

#[test]
fn query_validation_rejects() {
    assert!(matches!(
        parse_metadata_query(&" ".repeat(METADATA_QUERY_MAX_BYTES + 1)),
        Err(MetadataError::InvalidInput(_))
    ));
}
