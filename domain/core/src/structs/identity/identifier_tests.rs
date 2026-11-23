//! Tests identifier parsers: presets on real keys, match rules and refused rules.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

fn rule(name: &str, pattern: &str, target: IdentifierTarget, url: &str) -> IdentifierRule {
    IdentifierRule {
        name: name.to_string(),
        pattern: pattern.to_string(),
        target,
        url_template: url.to_string(),
    }
}

fn presets() -> IdentifierParser {
    use IdentifierTarget::{Key, SourceUrl};
    IdentifierParser {
        rules: vec![
            rule(
                "BioSample",
                r"(?P<id>SAM(?:N|EA|D)\d+)",
                Key,
                "https://www.ebi.ac.uk/biosamples/samples/{id}",
            ),
            rule(
                "NCBI assembly",
                r"(?P<id>GC[AF]_\d{9}\.\d+)",
                Key,
                "https://www.ncbi.nlm.nih.gov/datasets/genome/{id}/",
            ),
            rule(
                "RefSeq sequence",
                r"(?P<id>N[CZ]_[A-Z]*\d+\.\d+)[^/]*$",
                Key,
                "https://www.ncbi.nlm.nih.gov/nuccore/{id}",
            ),
            rule(
                "ENA run",
                r"(?P<id>[EDS]RR\d+)[^/]*$",
                Key,
                "https://www.ebi.ac.uk/ena/browser/view/{id}",
            ),
            rule(
                "Zenodo record",
                r"zenodo\.org/(?:api/)?records?/(?P<id>\d+)",
                SourceUrl,
                "https://doi.org/10.5281/zenodo.{id}",
            ),
        ],
    }
}

fn values(key: &str, source: Option<&str>) -> Vec<(String, String)> {
    let parser = CompiledParser::compile(&presets()).unwrap();
    (parser.find(key, source).into_iter())
        .map(|found| (found.name, found.value))
        .collect()
}

fn pair(name: &str, value: &str) -> (String, String) {
    (name.to_string(), value.to_string())
}

#[test]
fn presets_find_samples() {
    assert_eq!(
        values("SAMN02138552/contigs.fna.gz", None),
        [pair("BioSample", "SAMN02138552")]
    );
    assert_eq!(
        values("genes/SAMN15803895.psa_megahit.prodigal.faa.gz", None),
        [pair("BioSample", "SAMN15803895")]
    );
    assert_eq!(
        values("GCF/000/001/405/GCF_000001405.40_GRCh38.p14/x.fna", None),
        [pair("NCBI assembly", "GCF_000001405.40")]
    );
    assert_eq!(
        values("NC_000913.3.fna", None),
        [pair("RefSeq sequence", "NC_000913.3")]
    );
    assert_eq!(
        values("NZ_CP012345.1.gbk", None),
        [pair("RefSeq sequence", "NZ_CP012345.1")]
    );
    assert_eq!(
        values("ERR1234567_1.fastq.gz", None),
        [pair("ENA run", "ERR1234567")]
    );
    assert_eq!(
        values(
            "db.tar.gz",
            Some("https://zenodo.org/records/1234567/files/db.tar.gz")
        ),
        [pair("Zenodo record", "1234567")]
    );
    // A RefSeq id in a folder name is not the file's own id.
    assert!(values("NC_000913.3/readme.txt", None).is_empty());
}

#[test]
fn links_and_duplicates() {
    let parser = CompiledParser::compile(&IdentifierParser {
        rules: vec![
            rule(
                "Path",
                r"^(?P<id>[a-z]+/[a-z]+)",
                IdentifierTarget::Key,
                "https://x.org/{id}",
            ),
            rule(
                "Path",
                r"(?P<id>[a-z]+/[a-z]+)",
                IdentifierTarget::Key,
                "https://y.org/{id}",
            ),
            rule(
                "Word",
                r"(?P<id>[a-z]+)",
                IdentifierTarget::Key,
                "https://z.org/{id}",
            ),
            rule(
                "Url",
                r"(?P<id>.+)",
                IdentifierTarget::SourceUrl,
                "https://u.org/{id}",
            ),
        ],
    })
    .unwrap();
    let found = parser.find("ab/cd/ef", None);
    // The value is encoded as one path segment; a repeated (name, value) is dropped.
    assert_eq!(found.len(), 2);
    assert_eq!(found[0].value, "ab/cd");
    assert_eq!(found[0].url, "https://x.org/ab%2Fcd");
    // First match per rule only.
    assert_eq!(found[1].value, "ab");
}

#[test]
fn refuses_bad_rules() {
    let check = |pattern: &str, url: &str| {
        validate_parser(&IdentifierParser {
            rules: vec![rule("A", pattern, IdentifierTarget::Key, url)],
        })
    };
    assert!(matches!(
        check("(?P<id>[", "https://a.org/{id}"),
        Err(IdentifierError::Pattern { index: 0, .. })
    ));
    assert!(matches!(
        check("(?P<id>a{4000}){4000}", "https://a.org/{id}"),
        Err(IdentifierError::Pattern { .. })
    ));
    assert_eq!(
        check("(?P<name>a)", "https://a.org/{id}"),
        Err(IdentifierError::MissingGroup { index: 0 })
    );
    assert_eq!(
        check("(?P<id>a)", "https://a.org/"),
        Err(IdentifierError::MissingPlaceholder { index: 0 })
    );
    assert_eq!(
        check("(?P<id>a)", "javascript:alert({id})"),
        Err(IdentifierError::InvalidTemplate { index: 0 })
    );
    assert_eq!(
        check("(?P<id>a)", "{id}"),
        Err(IdentifierError::InvalidTemplate { index: 0 })
    );
    assert_eq!(validate_parser(&presets()), Ok(()));
}
