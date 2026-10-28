// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

const SUMMARY: &str = include_str!("../../../tests/fixtures/assembly_summary.tsv");
const BASE: &str = "https://ftp.ncbi.nlm.nih.gov/genomes/";

fn ncbi() -> TableImport {
    serde_json::from_value(serde_json::json!({
        "source": { "connector": "ASSEMBLY_REPORTS/assembly_summary_refseq.txt" },
        "format": { "delimiter": "\t", "header": "last_comment", "comment": "#",
                    "nulls": ["na", ""] },
        "rows": {
            "kind": "folder",
            "source": "{ftp_path}",
            "target": "{ftp_path|basename}",
            "filter": [{ "column": "assembly_accession",
                         "any_of": ["GCF_000005845.2", "GCF_000001405.40"] }],
            "text": ["assembly_accession", "organism_name", "biosample"],
            "exact": ["taxid", "species_taxid", "assembly_level", "group"]
        }
    }))
    .unwrap()
}

fn files() -> TableImport {
    serde_json::from_value(serde_json::json!({
        "source": { "object": { "bucket": "lists", "key": "samples.csv" } },
        "rows": { "kind": "file", "source": "{url}", "target": "samples/{url|basename}",
                  "keep": ["sample", "description"], "text": ["description"],
                  "exact": ["sample"] }
    }))
    .unwrap()
}

fn imported(outcome: RowOutcome) -> TableEntry {
    match outcome {
        RowOutcome::Import(entry) => entry,
        other => panic!("expected an imported row, got {other:?}"),
    }
}

// The NCBI descriptor reads the last comment line as header and maps two folders.
#[test]
fn summary_maps_folders() {
    let import = ncbi();
    import.validate().unwrap();
    let bytes = SUMMARY.as_bytes();
    let (columns, used) = read_header(&import.format, bytes, true).unwrap();
    assert_eq!(columns.len(), 38);
    assert_eq!(columns[0], "assembly_accession");
    let mapper = RowMapper::new(&import.rows, &columns).unwrap();
    let (rows, _) = parse_rows(&import.format, Some(38), &bytes[used..], true).unwrap();
    let outcomes: Vec<RowOutcome> = (rows.into_iter())
        .map(|(row, _)| mapper.map(&row.unwrap(), Some(BASE)))
        .collect();
    assert_eq!(outcomes.len(), 4);
    let human = imported(outcomes[0].clone());
    assert_eq!(
        human.source_path,
        "all/GCF/000/001/405/GCF_000001405.40_GRCh38.p14/"
    );
    assert_eq!(human.target, "GCF_000001405.40_GRCh38.p14");
    let coli = imported(outcomes[1].clone());
    assert_eq!(coli.metadata["taxid"], "511145");
    assert_eq!(coli.metadata["species_taxid"], "562");
    assert_eq!(coli.metadata["assembly_level"], "Complete Genome");
    // Every column is kept except null values, plus the two selection entries.
    assert!(!coli.metadata.contains_key("wgs_master"));
    assert_eq!(
        coli.metadata[SEARCH_TEXT_KEY],
        "assembly_accession,organism_name,biosample"
    );
    assert_eq!(
        coli.metadata[SEARCH_EXACT_KEY],
        "taxid,species_taxid,assembly_level,group"
    );
    assert_eq!(outcomes[2], RowOutcome::Filtered);
    // A row without folder is skipped once the filter lets it through.
    let mut all = import.rows.clone();
    all.filter.clear();
    let mapper = RowMapper::new(&all, &columns).unwrap();
    let (rows, _) = parse_rows(&import.format, Some(38), &bytes[used..], true).unwrap();
    let last = rows.into_iter().last().unwrap().0.unwrap();
    assert_eq!(mapper.map(&last, Some(BASE)), RowOutcome::Skipped);
}

// URLs outside the connector base and non-canonical paths fail their row only.
#[test]
fn foreign_paths_fail() {
    let import = files();
    let columns = vec![
        "url".to_string(),
        "sample".to_string(),
        "description".to_string(),
    ];
    let mapper = RowMapper::new(&import.rows, &columns).unwrap();
    let row = |url: &str| vec![Some(url.to_string()), Some("s1".to_string()), None];
    let base = Some("https://data.example.org/files/");
    let entry = imported(mapper.map(&row("https://data.example.org/files/a/b.fq"), base));
    assert_eq!(
        (entry.source_path.as_str(), entry.target.as_str()),
        ("a/b.fq", "samples/b.fq")
    );
    assert_eq!(entry.metadata["sample"], "s1");
    assert!(!entry.metadata.contains_key("description"));
    for bad in [
        "https://other.example.org/files/a.fq",
        "https://data.example.org/filesx/a.fq",
        "https://data.example.org/files/../a.fq",
        "a//b.fq",
    ] {
        assert!(
            matches!(mapper.map(&row(bad), base), RowOutcome::Failed(_)),
            "{bad}"
        );
    }
    assert!(matches!(
        mapper.map(&row("https://data.example.org/files/a.fq"), None),
        RowOutcome::Failed(_)
    ));
}

// Quoted fields may hold delimiters and newlines; a row cut by the chunk end waits.
#[test]
fn quoted_rows_resume() {
    let format = TableFormat::default();
    let table =
        "url,sample,description\na.fq,s1,\"one, two\"\nb.fq,s2,\"line\nbreak\"\nc.fq,s3,x\n";
    let bytes = table.as_bytes();
    let (columns, used) = read_header(&format, bytes, false).unwrap();
    assert_eq!(columns, ["url", "sample", "description"]);
    let cut = table.find("break").unwrap();
    let (rows, taken) = parse_rows(&format, Some(3), &bytes[used..cut], false).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0.as_ref().unwrap()[2].as_deref(), Some("one, two"));
    let rest = &bytes[used + taken..];
    let (rows, _) = parse_rows(&format, Some(3), rest, true).unwrap();
    let values: Vec<_> = rows.into_iter().map(|(row, _)| row.unwrap()).collect();
    assert_eq!(values[0][2].as_deref(), Some("line\nbreak"));
    assert_eq!(values[1][0].as_deref(), Some("c.fq"));
}

// A row with too few columns fails alone; a row longer than a chunk ends the read.
#[test]
fn bad_rows_reported() {
    let format = TableFormat::default();
    let (rows, _) = parse_rows(&format, Some(2), b"a,b\nc\nd,e\n", true).unwrap();
    assert!(rows[0].0.is_ok() && rows[1].0.is_err() && rows[2].0.is_ok());
    let long = vec![b'x'; TABLE_CHUNK];
    assert_eq!(
        parse_rows(&format, Some(1), &long, false),
        Err(TableError::RowTooLong)
    );
    let (rows, _) = parse_rows(&format, Some(1), b"\xff\n", true).unwrap();
    assert!(rows[0].0.is_err());
}

#[test]
fn headers_by_row() {
    let mut format = TableFormat {
        delimiter: ';',
        header: TableHeader::Row(2),
        ..TableFormat::default()
    };
    let bytes = "\u{feff}title line\nid;path\n1;a\n".as_bytes();
    let (columns, used) = read_header(&format, bytes, true).unwrap();
    assert_eq!(columns, ["id", "path"]);
    assert_eq!(&bytes[used..], b"1;a\n");
    format.header = TableHeader::Columns(vec!["id".to_string(), "path".to_string()]);
    let (columns, used) = read_header(&format, b"1;a\n", true).unwrap();
    assert_eq!((columns.len(), used), (2, 0));
    let repeated = TableHeader::Columns(vec!["id".to_string(), "id".to_string()]);
    format.header = repeated;
    assert!(read_header(&format, b"1;a\n", true).is_err());
}

#[test]
fn descriptors_validated() {
    let mut import = files();
    import.rows.target = "{url|upper}".to_string();
    assert!(import.validate().is_err());
    let mut import = files();
    import.rows.target = "fixed".to_string();
    assert!(import.validate().is_err());
    let mut import = files();
    import.rate = Some(0);
    assert!(import.validate().is_err());
    let mut import = files();
    import.format.header = TableHeader::LastComment;
    assert!(import.validate().is_err());
    let mut import = files();
    import.format.delimiter = 'ä';
    assert!(import.validate().is_err());
}

// Kept columns must make safe metadata keys and fit the per-object bounds.
#[test]
fn metadata_bounds_hold() {
    let rows = files().rows;
    let spaced = vec![
        "url".to_string(),
        "sample".to_string(),
        "Sample Id".to_string(),
    ];
    let mut keep_all = rows.clone();
    keep_all.keep = None;
    assert!(RowMapper::new(&keep_all, &spaced).is_err());
    let spoofed = vec![
        "url".into(),
        "sample".into(),
        "description".into(),
        "aruna-x".into(),
    ];
    assert!(RowMapper::new(&keep_all, &spoofed).is_err());
    let wide: Vec<String> = (0..128).map(|index| format!("c{index}")).collect();
    let mut wide_rows = rows.clone();
    wide_rows.source = "{c0}".to_string();
    wide_rows.target = "{c1}".to_string();
    wide_rows.keep = None;
    wide_rows.text = vec!["c2".to_string()];
    wide_rows.exact = Vec::new();
    assert!(RowMapper::new(&wide_rows, &wide).is_err());
    let columns = vec![
        "url".to_string(),
        "sample".to_string(),
        "description".to_string(),
    ];
    let mapper = RowMapper::new(&rows, &columns).unwrap();
    let long = "x".repeat(MAX_METADATA_VALUE + 1);
    let row = vec![Some("a.fq".to_string()), Some("s".to_string()), Some(long)];
    assert!(matches!(mapper.map(&row, None), RowOutcome::Failed(_)));
}

// A gzip table read in windows yields the same bytes as the plain table.
#[test]
fn gzip_windows_resume() {
    use flate2::Compression;
    use flate2::write::GzEncoder;
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(SUMMARY.as_bytes()).unwrap();
    let compressed = encoder.finish().unwrap();
    assert!(is_gzip(&TableFormat::default(), "summary.txt", &compressed));
    assert!(!is_gzip(
        &TableFormat::default(),
        "summary.txt",
        SUMMARY.as_bytes()
    ));
    let mut read = Vec::new();
    loop {
        let mut window = GzipWindow::new(read.len() as u64, 1000);
        for chunk in compressed.chunks(97) {
            if window.push(chunk).unwrap() {
                break;
            }
        }
        let (bytes, complete) = window.finish().unwrap();
        read.extend_from_slice(&bytes);
        if complete {
            break;
        }
    }
    assert_eq!(read, SUMMARY.as_bytes());
    let mut truncated = GzipWindow::new(0, TABLE_CHUNK);
    truncated.push(&compressed[..compressed.len() / 2]).unwrap();
    assert!(truncated.finish().is_err());
}

// Search reads the version's own selections: only selected columns match, tax ids exactly.
#[test]
fn metadata_search_terms() {
    let import = ncbi();
    let bytes = SUMMARY.as_bytes();
    let (columns, used) = read_header(&import.format, bytes, true).unwrap();
    let mapper = RowMapper::new(&import.rows, &columns).unwrap();
    let (rows, _) = parse_rows(&import.format, Some(38), &bytes[used..], true).unwrap();
    let coli = imported(mapper.map(&rows[1].0.clone().unwrap(), Some(BASE)));
    let metadata: std::collections::HashMap<_, _> = coli.metadata.into_iter().collect();
    let taxid = ["taxid=562".to_string()];
    assert!(!metadata_matches(&metadata, "", &taxid));
    let species = ["species_taxid=562".to_string()];
    assert!(metadata_matches(&metadata, "Escherichia K-12", &species));
    assert!(metadata_matches(&metadata, "samn02604091", &[]));
    // `asm_submitter` is stored but not selected for search.
    assert!(!metadata_matches(&metadata, "Wisconsin", &[]));
    assert!(!metadata_matches(&metadata, "562", &[]));
    let mut plain = metadata.clone();
    plain.remove(SEARCH_TEXT_KEY);
    plain.remove(SEARCH_EXACT_KEY);
    assert!(!metadata_matches(&plain, "", &[]));
}

#[test]
fn queries_split_terms() {
    let (text, exact) = query_terms(r#"coli  taxid=562 assembly_level="Complete Genome" k-12"#);
    assert_eq!(text, "coli k-12");
    assert_eq!(exact, ["taxid=562", "assembly_level=Complete Genome"]);
    assert_eq!(query_terms("=x a"), ("=x a".to_string(), Vec::new()));
}
