// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use std::io::Write;

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

// One file per assembly comes straight from the summary: a suffix after the folder's basename.
#[test]
fn summary_maps_genomes() {
    let mut import = ncbi();
    import.rows.kind = RowKind::File;
    import.rows.source = "{ftp_path}/{ftp_path|basename}_genomic.fna.gz".to_string();
    import.rows.target = "{ftp_path|basename}/{ftp_path|basename}_genomic.fna.gz".to_string();
    import.rows.filter.clear();
    import.validate().unwrap();
    let bytes = SUMMARY.as_bytes();
    let (columns, used) = read_header(&import.format, bytes, true).unwrap();
    let mapper = RowMapper::new(&import.rows, &columns).unwrap();
    let (rows, _) = parse_rows(&import.format, Some(38), &bytes[used..], true).unwrap();
    let outcomes: Vec<RowOutcome> = (rows.into_iter())
        .map(|(row, _)| mapper.map(&row.unwrap(), Some(BASE)))
        .collect();
    let human = imported(outcomes[0].clone());
    let folder = "GCF_000001405.40_GRCh38.p14";
    assert_eq!(
        human.source_path,
        format!("all/GCF/000/001/405/{folder}/{folder}_genomic.fna.gz")
    );
    assert_eq!(human.target, format!("{folder}/{folder}_genomic.fna.gz"));
    // A row without a size column is headed once at import; an `na` folder is skipped.
    assert_eq!(human.size, None);
    assert_eq!(outcomes.last(), Some(&RowOutcome::Skipped));
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
    // URL segments are decoded once, so the connector does not encode them a second time.
    let spaced = imported(mapper.map(&row("https://data.example.org/files/a%20b.fq"), base));
    assert_eq!(spaced.source_path, "a b.fq");
    for bad in [
        "https://other.example.org/files/a.fq",
        "https://data.example.org/filesx/a.fq",
        "https://data.example.org/files/../a.fq",
        "https://data.example.org/files/%2e%2e/a.fq",
        "https://data.example.org/files/a%2Fb.fq",
        "https://data.example.org/files/%ff.fq",
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
    // A failed row is still named by the key it would map to.
    let failed = row("https://other.example.org/files/a.fq");
    assert_eq!(mapper.target_key(&failed).as_deref(), Some("samples/a.fq"));
}

// Each mirror template renders another source of the row's file; a null value gives none.
#[test]
fn rows_render_mirrors() {
    let mut import = files();
    let connector_id = Ulid::from_bytes([7; 16]);
    import.rows.mirrors = vec![MirrorColumn {
        connector_id,
        source: "{sample}/{url|basename}".to_string(),
    }];
    import.validate().unwrap();
    let columns = ["url", "sample", "description"].map(str::to_string);
    let mapper = RowMapper::new(&import.rows, &columns).unwrap();
    let row = |sample: Option<&str>| {
        let url = Some("https://data.example.org/files/a/b.fq".to_string());
        vec![url, sample.map(str::to_string), None]
    };
    let base = Some("https://data.example.org/files/");
    let entry = imported(mapper.map(&row(Some("s1")), base));
    assert_eq!(entry.mirrors, [(connector_id, "s1/b.fq".to_string())]);
    assert!(imported(mapper.map(&row(None), base)).mirrors.is_empty());
    import.rows.kind = RowKind::Folder;
    assert!(import.validate().is_err());
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

// A stretch of comment lines longer than a chunk is consumed; only a cut line waits.
#[test]
fn comment_stretch_consumed() {
    let format = TableFormat {
        comment: Some('#'),
        ..TableFormat::default()
    };
    let mut bytes = b"a\n".to_vec();
    while bytes.len() < TABLE_CHUNK {
        bytes.extend_from_slice(b"#note\n");
    }
    bytes.truncate(TABLE_CHUNK);
    let (rows, used) = parse_rows(&format, Some(1), &bytes, false).unwrap();
    assert_eq!(rows.len(), 1);
    assert!(used > TABLE_CHUNK - 6 && bytes[..used].ends_with(b"\n"));
    let (rows, used) = parse_rows(&format, Some(1), &bytes[2..], false).unwrap();
    assert!(rows.is_empty() && used > TABLE_CHUNK - 8);
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
    // Each step continues the decoder of the step before, after the bytes that step used.
    let mut cursor = GzipCursor::new(0, 1000);
    let mut pieces = compressed.chunks(97);
    let mut read = Vec::new();
    loop {
        while !cursor.fill().unwrap() {
            match pieces.next() {
                Some(piece) => cursor.push(piece),
                None => cursor.finish(),
            }
        }
        let (bytes, complete) = cursor.chunk();
        let used = bytes.len().min(700);
        read.extend_from_slice(&bytes[..used]);
        assert!(cursor.advance(read.len() as u64));
        if complete && used == bytes.len() {
            break;
        }
    }
    assert_eq!(read, SUMMARY.as_bytes());
    assert!(!cursor.advance(0), "a cursor never goes back");
    // A cursor opened at a later position skips to it.
    let mut later = GzipCursor::new(5, 4);
    later.push(&compressed);
    later.finish();
    assert!(later.fill().unwrap());
    assert_eq!(later.chunk().0, &SUMMARY.as_bytes()[5..9]);
    let mut truncated = GzipCursor::new(0, TABLE_CHUNK);
    truncated.push(&compressed[..compressed.len() / 2]);
    truncated.finish();
    assert!(truncated.fill().is_err());
}

fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

// A small compressed input that expands enormously is decoded only as far as a chunk needs,
// also while skipping to a late position.
#[test]
fn gzip_memory_bounded() {
    let zeros = gzip(&vec![0u8; 16 << 20]);
    assert!(zeros.len() < 64 * 1024);
    let mut cursor = GzipCursor::new(0, 1000);
    cursor.push(&zeros);
    assert!(cursor.fill().unwrap());
    assert!(cursor.kept.len() <= 1001 && cursor.kept.capacity() <= 4 * 1024);
    assert!(
        cursor.decoded <= 1001,
        "decoding stops after the chunk and one byte"
    );
    let (bytes, complete) = cursor.chunk();
    assert_eq!((bytes.len(), complete), (1000, false));

    let mut late = GzipCursor::new(15 << 20, 1000);
    late.push(&zeros);
    assert!(late.fill().unwrap());
    assert!(late.kept.len() <= 1001 && late.kept.capacity() <= 4 * 1024);
    assert!(late.decoded <= (15 << 20) + 1001);
}

// A table whose last chunk is exactly the chunk size is complete only once its end is known;
// until then the cursor waits instead of offering a chunk that cuts the last row off.
#[test]
fn gzip_exact_chunk() {
    let table = b"a\tb\n1\t2\n";
    let mut cursor = GzipCursor::new(0, table.len());
    cursor.push(&gzip(table));
    assert!(
        !cursor.fill().unwrap(),
        "the end of the input is not known yet"
    );
    cursor.finish();
    assert!(cursor.fill().unwrap());
    assert_eq!(cursor.chunk(), (table.to_vec(), true));
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

// Size, checksum and time columns are checked per row; a bad value fails only its row.
#[test]
fn file_columns_checked() {
    let rows: TableRows = serde_json::from_value(serde_json::json!({
        "kind": "file", "source": "{url}", "target": "{url|basename}",
        "size": "bytes", "modified": "time",
        "checksums": [{ "algorithm": "md5", "column": "md5" },
                      { "algorithm": "sha256", "column": "sha256" }]
    }))
    .unwrap();
    let columns: Vec<String> = ["url", "bytes", "md5", "sha256", "time"]
        .map(String::from)
        .into();
    let mapper = RowMapper::new(&rows, &columns).unwrap();
    let md5 = "9e107d9d372bb6826bd81d3542a419d6";
    let sha = "d7a8fbb307d7809469ca9abcb0082e4f8d5651e46d3cdb762d02d0bf37c9e592";
    let row = |bytes: &str, md5: &str, sha: &str, time: &str| {
        let value = |text: &str| (!text.is_empty()).then(|| text.to_string());
        vec![
            Some("a.fq".to_string()),
            value(bytes),
            value(md5),
            value(sha),
            value(time),
        ]
    };
    let entry = imported(mapper.map(&row("42", md5, sha, "2025-11-17T17:38:21Z"), None));
    assert_eq!(entry.size, Some(42));
    assert_eq!(
        entry.digests[0],
        (ChecksumAlgorithm::Md5, hex::decode(md5).unwrap())
    );
    assert_eq!(entry.digests[1].0, ChecksumAlgorithm::Sha256);
    let at = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_763_401_101);
    assert_eq!(entry.modified, Some(at));
    // Null values give nothing; a date is checked but names no second.
    let bare = imported(mapper.map(&row("", "", "", "2025-11-17"), None));
    assert_eq!(
        (bare.size, bare.digests.len(), bare.modified),
        (None, 0, None)
    );
    for bad in [
        row("-1", md5, sha, ""),
        row("4.2", md5, sha, ""),
        row("42", &md5[..30], sha, ""),
        row("42", sha, "", ""),
        row("42", "zz107d9d372bb6826bd81d3542a419d6", "", ""),
        row("42", md5, sha, "yesterday"),
    ] {
        assert!(
            matches!(mapper.map(&bad, None), RowOutcome::Failed(_)),
            "{bad:?}"
        );
    }
    let mut folder = rows.clone();
    folder.kind = RowKind::Folder;
    let import = |rows: TableRows| TableImport {
        source: TableSource::Connector("list.csv".to_string()),
        format: TableFormat::default(),
        rows,
        rate: None,
    };
    assert!(import(folder).validate().is_err());
    let mut twice = rows.clone();
    twice.checksums[1].algorithm = "md5".to_string();
    assert!(import(twice).validate().is_err());
    let mut unknown = rows;
    unknown.checksums[0].algorithm = "crc32".to_string();
    assert!(import(unknown).validate().is_err());
}

#[test]
fn record_rows_trusted() {
    let rows: TableRows = serde_json::from_value(serde_json::json!({
        "kind": "record", "source": "{source}", "target": "{key}", "keep": [],
        "size": "size", "modified": "modified",
        "checksums": [{ "algorithm": "md5", "column": "md5" }]
    }))
    .unwrap();
    let columns: Vec<String> = [
        "key",
        "source",
        "size",
        "modified",
        "etag",
        "content_type",
        "md5",
        "metadata",
    ]
    .map(String::from)
    .into();
    let mapper = RowMapper::new(&rows, &columns).unwrap();
    let md5 = "9e107d9d372bb6826bd81d3542a419d6";
    let row = |key: &str, source: &str, size: &str, metadata: &str| {
        let value = |text: &str| (!text.is_empty()).then(|| text.to_string());
        vec![
            value(key),
            value(source),
            value(size),
            Some("2025-11-17T17:38:21Z".to_string()),
            Some(format!("\"{md5}\"")),
            Some("text/plain".to_string()),
            Some(md5.to_string()),
            value(metadata),
        ]
    };
    let meta = r#"{"taxid": "562", "aruna-search-exact": "taxid"}"#;
    let entry = imported(mapper.map(&row("g/a.txt", "all/g/a.txt", "42", meta), None));
    assert_eq!(
        (entry.source_path.as_str(), entry.target.as_str()),
        ("all/g/a.txt", "g/a.txt")
    );
    assert_eq!(entry.size, Some(42));
    assert_eq!(
        entry.etag.as_deref(),
        Some("\"9e107d9d372bb6826bd81d3542a419d6\"")
    );
    assert_eq!(entry.content_type.as_deref(), Some("text/plain"));
    assert_eq!(entry.digests.len(), 1);
    assert_eq!(entry.metadata.get("taxid").map(String::as_str), Some("562"));
    assert_eq!(
        entry.metadata.get(SEARCH_EXACT_KEY).map(String::as_str),
        Some("taxid")
    );
    // A row without a source is an empty object; a folder marker keeps its slash.
    let folder = imported(mapper.map(&row("g/", "", "0", meta), None));
    assert_eq!(
        (folder.source_path.as_str(), folder.target.as_str()),
        ("", "g/")
    );
    for bad in [
        row("g/b", "", "3", ""),
        row("g/b", "all/g/b", "3", "[1]"),
        row("g/b", "all/g/b", "3", r#"{"aruna-owner": "x"}"#),
        row("g/b", "all/g/b", "3", r#"{"bad key": "x"}"#),
    ] {
        assert!(
            matches!(mapper.map(&bad, None), RowOutcome::Failed(_)),
            "{bad:?}"
        );
    }
    // File rows ignore the record columns.
    let mut file = rows.clone();
    file.kind = RowKind::File;
    let plain = imported(
        RowMapper::new(&file, &columns)
            .unwrap()
            .map(&row("g/a", "all/g/a", "1", meta), None),
    );
    assert_eq!(
        (plain.etag, plain.content_type, plain.metadata.len()),
        (None, None, 0)
    );
    let import = |rows: TableRows| TableImport {
        source: TableSource::Connector("records.tsv".to_string()),
        format: TableFormat::default(),
        rows,
        rate: None,
    };
    let mut sizeless = rows;
    sizeless.size = None;
    assert!(import(sizeless).validate().is_err());
}

// The record kind is a new last variant, so stored descriptors of the other kinds still decode.
#[test]
fn record_kind_appended() {
    assert_eq!(postcard::to_allocvec(&RowKind::File).unwrap(), [0]);
    assert_eq!(postcard::to_allocvec(&RowKind::Folder).unwrap(), [1]);
    assert_eq!(postcard::to_allocvec(&RowKind::Record).unwrap(), [2]);
}

// A further source's path names exactly the file its READ check names: a segment a server
// would trim or a repeated separator is refused.
#[test]
fn mirror_paths_canonical() {
    assert_eq!(
        mirror_path("/refseq/a.fna", None).as_deref(),
        Ok("refseq/a.fna")
    );
    for bad in [" private/file", "private /file", "private//file", "a/../b"] {
        assert!(mirror_path(bad, None).is_err(), "{bad}");
    }
}

// Free text words of at least three characters match by their start; exact terms never do.
#[test]
fn metadata_prefixes_match() {
    let metadata = std::collections::HashMap::from([
        (SEARCH_TEXT_KEY.to_string(), "biosample".to_string()),
        ("biosample".to_string(), "SAMD00012345".to_string()),
        (SEARCH_EXACT_KEY.to_string(), "taxid".to_string()),
        ("taxid".to_string(), "562".to_string()),
    ]);
    assert!(metadata_matches(&metadata, "samd", &[]));
    assert!(metadata_matches(&metadata, "SAMD0001", &[]));
    assert!(!metadata_matches(&metadata, "sa", &[]));
    assert!(!metadata_matches(&metadata, "amd0", &[]));
    assert!(!metadata_matches(&metadata, "", &["taxid=56".to_string()]));
}
