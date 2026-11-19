//! Tests table staging jobs that pace, pause and resume, run under the node's job host.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::lane_jobs::run_staging_job;
use aruna_operations::tests::staging::{
    COLI, Fixture, HUMAN, STRUCTURE, SUMMARY, Site, serve, summary_table, write_object,
};
use std::collections::BTreeMap;
use std::sync::Arc;

use aruna_core::structs::execution::job::JobResultPayload;
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

use aruna_core::structs::execution::job::StagingJobSpec;
use aruna_operations::driver::{drive, node_routing};
use aruna_operations::jobs::context::JobContext;
use aruna_operations::jobs::executor::JobRunOutcome;
use aruna_operations::jobs::staging::table::*;

// A job cancelled or shut down while its source's `Retry-After` pauses it ends with that stop,
// not with a failed attempt.
#[tokio::test]
async fn paused_job_stops() {
    let throttled = |endpoint: &str| Site {
        throttled: true,
        ..Site::ncbi(endpoint)
    };
    let (endpoint, _site, server) = serve(throttled).await;
    let fixture = Fixture::new(&endpoint, summary_table()).await;
    for (seed, cancel) in [(20u8, true), (21u8, false)] {
        let ctx = fixture.context(seed).await;
        let stop = match cancel {
            true => ctx.cancel.clone(),
            false => ctx.shutdown.clone(),
        };
        let stopping = async {
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            stop.cancel();
        };
        let run = run_staging_job(&ctx, &fixture.spec);
        let (outcome, ()) = tokio::join!(run, stopping);
        match cancel {
            true => assert!(matches!(outcome, JobRunOutcome::Cancelled)),
            false => assert!(matches!(outcome, JobRunOutcome::Interrupted)),
        }
    }
    server.abort();
}

/// Rows per second of a trusted record import into a searched distributed bucket:
/// `ARUNA_BENCH_ROWS` source rows and `ARUNA_BENCH_EMPTY` empty rows, spread over
/// `ARUNA_BENCH_JOBS` tables imported at once. Run with `--ignored --nocapture`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark"]
async fn record_import_rate() {
    let count = |name: &str, default: usize| {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(default)
    };
    let (rows, empty) = (
        count("ARUNA_BENCH_ROWS", 100_000),
        count("ARUNA_BENCH_EMPTY", 0),
    );
    let jobs = count("ARUNA_BENCH_JOBS", 1).max(1);
    let background = count("ARUNA_BENCH_BACKGROUND", 1) > 0;
    // Contiguous key blocks per job, as a split export gives, instead of interleaved rows.
    let blocks = count("ARUNA_BENCH_BLOCKS", 0) > 0;
    let ((completed, failed, conflicts), elapsed, index) =
        import_records(rows, empty, (jobs, blocks), background).await;
    println!(
        "record import: {rows} rows, {empty} empty, {jobs} jobs, {completed} completed, \
         {failed} failed, {conflicts} conflicts, {elapsed:.1} s, {:.0} rows/s, \
         index caught up {index:.1} s later",
        (rows + empty) as f64 / elapsed
    );
}

// With the drain and the feed beside the import, the catch-up leaves no feed row unapplied.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn background_import_settles() {
    let ((completed, failed, _), ..) = import_records(300, 0, (2, true), true).await;
    assert_eq!((completed, failed), (300, 0));
}

/// The search feed rows still waiting for the feed.
async fn feed_rows(fixture: &Fixture) -> Vec<(aruna_core::types::Key, aruna_core::types::Value)> {
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::SEARCH_FEED_KEYSPACE;
    let read = StorageEffect::Iter {
        key_space: SEARCH_FEED_KEYSPACE.to_string(),
        prefix: None,
        start: None,
        limit: 1,
        txn_id: None,
    };
    match fixture
        .driver
        .storage_handle
        .send_storage_effect(read)
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => values,
        event => panic!("the feed must be readable: {event:?}"),
    }
}

// Empty rows of one job and record rows of another write one range in turns, not in conflict.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_rows_alternate() {
    assert_eq!(
        import_records(400, 100, (2, false), false).await.0,
        (500, 0, 0)
    );
}

/// The table line of record row `row`: key `samn<row>/assembly.fna.gz`, size `row`.
fn record_line(row: usize, meta: &str) -> String {
    let md5 = format!("{:x}", md5::compute(row.to_le_bytes()));
    format!(
        "samn{row:08}/assembly.fna.gz\t{COLI}{row:08}.fna.gz\t{row}\t2025-11-17T17:38:21Z\t\
         \"{md5}\"\tapplication/gzip\t{md5}\t{meta}\n"
    )
}

// A record import of several full pages stopped after its first page resumes there, beside a
// second job with empty rows on the same bucket: every key once, counters and search right.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn record_pages_resume() {
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::key_index::KeyQuery;
    use aruna_core::keyspaces::USAGE_STATS_KEYSPACE;
    use aruna_core::structs::storage::usage::{BucketCounters, usage_bucket_key};
    use aruna_operations::distribution::feed_walks::FeedLimits;
    use aruna_operations::distribution::test_store::Feeder;
    use aruna_operations::staging::page::rows::MAX_PAGE_ENTRIES;
    use aruna_storage::key_index::RangeIndexes;
    use std::ops::Bound;

    let (endpoint, site, server) = serve(Site::ncbi).await;
    let table = serde_json::json!({
        "source": { "object": { "bucket": "lists", "key": "first.tsv" } },
        "format": { "delimiter": "\t", "quote": null, "nulls": [""] },
        "rows": { "kind": "record", "source": "{source}", "target": "{key}", "keep": [],
                  "size": "size", "modified": "modified",
                  "checksums": [{ "algorithm": "md5", "column": "md5" }] }
    });
    let fixture = Fixture::new(&endpoint, table).await;
    let header = "key\tsource\tsize\tmodified\tetag\tcontent_type\tmd5\tmetadata\n";
    let meta = r#"{"note":"sample"}"#;
    let rows = 2 * MAX_PAGE_ENTRIES + 500;
    let first: String = (0..rows).map(|row| record_line(row, meta)).collect();
    let second: String = (rows..rows + 300)
        .map(|row| record_line(row, meta))
        .chain((0..3).map(|row| format!("samn{row:08}/\t\t0\t\t\t\t\t{meta}\n")))
        .collect();
    let ctx = fixture.context(50).await;
    let (set_id, range_id) = searched_set(&ctx, &fixture.spec).await;
    let lists = drive(
        GetBucketOperation::new("lists".to_string()),
        &fixture.driver,
    )
    .await
    .unwrap();
    let mut specs = Vec::new();
    for (key, rows) in [("first.tsv", first), ("second.tsv", second)] {
        let body = (Bytes::from(format!("{header}{rows}")), &BTreeMap::new());
        write_object(&ctx, &fixture.spec, ("lists", &lists), key, body)
            .await
            .ok()
            .unwrap();
        let mut spec = fixture.spec.clone();
        let import = spec.table.as_mut().unwrap();
        import.source = serde_json::from_value(serde_json::json!({
            "object": { "bucket": "lists", "key": key }
        }))
        .unwrap();
        specs.push(spec);
    }

    // The shutdown comes once the first page is committed, so the job stops after its next one.
    let (progress, stop) = (ctx.progress.clone(), ctx.shutdown.clone());
    let watch = async move {
        while progress.snapshot().current < MAX_PAGE_ENTRIES as u64 {
            progress.flush_requested().await;
        }
        stop.cancel();
    };
    let run = run_staging_job(&ctx, &specs[0]);
    let (outcome, ()) = tokio::join!(run, watch);
    assert!(matches!(outcome, JobRunOutcome::Interrupted));
    let resumed = JobContext {
        shutdown: CancellationToken::new(),
        ..ctx
    };
    let mut other = fixture.context_for(&specs[1], 51).await;
    other.range_turns = resumed.range_turns.clone();
    let (first, second) = tokio::join!(
        run_staging_job(&resumed, &specs[0]),
        run_staging_job(&other, &specs[1]),
    );
    let counts = |outcome| match outcome {
        JobRunOutcome::Succeeded(JobResultPayload::Staging {
            completed_items,
            failed_items,
        }) => (completed_items, failed_items),
        JobRunOutcome::Failed(error) | JobRunOutcome::Deferred(error) => {
            panic!("the import stopped: {error:?}")
        }
        _ => panic!("the import did not finish"),
    };
    assert_eq!(counts(first), (rows as u64, 0));
    assert_eq!(counts(second), (303, 0));
    assert!(site.requests.lock().unwrap().is_empty());

    // One version per key: a page committed twice would add versions, not objects.
    let read = StorageEffect::Read {
        key_space: USAGE_STATS_KEYSPACE.to_string(),
        key: usage_bucket_key(&fixture.spec.bucket).into(),
        txn_id: None,
    };
    let Event::Storage(StorageEvent::ReadResult {
        value: Some(value), ..
    }) = fixture
        .driver
        .storage_handle
        .send_storage_effect(read)
        .await
    else {
        panic!("the bucket's counters must exist");
    };
    let counters = BucketCounters::from_bytes(&value).unwrap();
    let keys = rows as u64 + 303;
    assert_eq!((counters.objects, counters.versions), (keys, keys));
    let bytes = (0..rows + 300).map(|row| row as u64).sum::<u64>();
    assert_eq!(counters.referenced_bytes, bytes);
    for row in [
        0,
        MAX_PAGE_ENTRIES - 1,
        MAX_PAGE_ENTRIES,
        rows - 1,
        rows + 299,
    ] {
        let key = format!("samn{row:08}/assembly.fna.gz");
        assert_eq!(fixture.observation(&key).await.content_length, row as u64);
    }

    // The feed finds no index at first; the first index generation is built from the heads.
    let indexes = RangeIndexes::new(fixture._dir.path().join("range-indexes"));
    let indexes = fixture.driver.storage_handle.install_range_indexes(indexes);
    let mut feeder = Feeder::new(FeedLimits::default());
    while feeder.pass(&fixture.driver).await {}
    for row in [0, MAX_PAGE_ENTRIES, rows - 1, rows + 299] {
        let query = KeyQuery {
            start: Bound::Unbounded,
            end: Bound::Unbounded,
            names: Some(format!("samn{row:08}")),
            meta: None,
            limit: 10,
        };
        let found = indexes.search(set_id, range_id, 1, query).await.unwrap();
        let mut expected = vec![format!("samn{row:08}/assembly.fna.gz")];
        // The empty rows are the folders of the first rows.
        if row < 3 {
            expected.insert(0, format!("samn{row:08}/"));
        }
        assert_eq!(found.keys, expected);
    }
    server.abort();
}

/// Imports `rows` record and `empty` empty rows over `jobs` tables at once, in key blocks or
/// interleaved, with drain and search feed beside them when `background`. Returns completed and
/// failed rows, storage conflicts, the jobs' seconds and the search index's catch-up seconds.
async fn import_records(
    rows: usize,
    empty: usize,
    (jobs, blocks): (usize, bool),
    background: bool,
) -> ((u64, u64, u64), f64, f64) {
    let (endpoint, _site, server) = serve(Site::ncbi).await;
    let table = serde_json::json!({
        "source": { "object": { "bucket": "lists", "key": "records-0.tsv" } },
        "format": { "delimiter": "\t", "quote": null, "nulls": [""] },
        "rows": { "kind": "record", "source": "{source}", "target": "{key}", "keep": [],
                  "size": "size", "modified": "modified",
                  "checksums": [{ "algorithm": "md5", "column": "md5" }] }
    });
    let fixture = Fixture::new(&endpoint, table.clone()).await;
    let meta = r#"{"note":"sample"}"#;
    let header = "key\tsource\tsize\tmodified\tetag\tcontent_type\tmd5\tmetadata\n";
    let mut tables = vec![String::from(header); jobs];
    for row in 0..rows {
        let table = match blocks {
            true => row * jobs / rows,
            false => row % jobs,
        };
        tables[table].push_str(&record_line(row, meta));
    }
    for row in 0..empty {
        tables[row % jobs].push_str(&format!("samn{row:08}/\t\t0\t\t\t\t\t{meta}\n"));
    }
    let ctx = fixture.context(40).await;
    searched_set(&ctx, &fixture.spec).await;
    let lists = drive(
        GetBucketOperation::new("lists".to_string()),
        &fixture.driver,
    )
    .await;
    let lists = ("lists", &lists.unwrap());
    let mut runs = Vec::new();
    for (index, tsv) in tables.into_iter().enumerate() {
        let key = format!("records-{index}.tsv");
        let body = (Bytes::from(tsv), &BTreeMap::new());
        write_object(&ctx, &fixture.spec, lists, &key, body)
            .await
            .ok()
            .unwrap();
        let mut spec = fixture.spec.clone();
        let mut import = table.clone();
        import["source"]["object"]["key"] = key.into();
        spec.table = Some(serde_json::from_value(import).unwrap());
        let mut job = fixture.context_for(&spec, 41 + index as u8).await;
        // Jobs of one node share their range turns.
        job.range_turns = ctx.range_turns.clone();
        runs.push((job, spec));
    }

    // The replication drain and the search feed run beside the import, as on a node, where each
    // committed page schedules them: here every page a job commits wakes both.
    let (pages, _) = tokio::sync::watch::channel(0u64);
    let pages = Arc::new(pages);
    let watchers: Vec<_> = (runs.iter())
        .map(|(job, _)| {
            let (progress, pages) = (job.progress.clone(), pages.clone());
            tokio::spawn(async move {
                loop {
                    progress.flush_requested().await;
                    pages.send_modify(|count| *count += 1);
                }
            })
        })
        .collect();
    let drain = fixture.driver.clone();
    let mut woken = pages.subscribe();
    let drain = tokio::spawn(async move {
        if !background {
            return;
        }
        loop {
            let drained = aruna_operations::driver::drive(
                aruna_operations::replication::drain::ProcessBatchOperation::new(
                    aruna_operations::replication::page::deliver::MetadataPages,
                    aruna_operations::driver::node_routing(&drain),
                    (&drain).node_and_realm(),
                ),
                &drain,
            )
            .await;
            if !drained.is_ok_and(|drained| drained.has_more_due) && woken.changed().await.is_err()
            {
                return;
            }
        }
    });
    let feed = fixture.driver.clone();
    let indexes = fixture._dir.path().join("range-indexes");
    let stop = CancellationToken::new();
    let stopping = stop.clone();
    let mut woken = pages.subscribe();
    let feed = tokio::spawn(async move {
        use aruna_operations::distribution::feed_walks::FeedLimits;
        use aruna_operations::distribution::test_store::Feeder;
        use aruna_storage::key_index::RangeIndexes;
        feed.storage_handle
            .install_range_indexes(RangeIndexes::new(indexes));
        let mut feeder = Feeder::new(FeedLimits::default());
        while background && !stopping.is_cancelled() {
            if !feeder.pass(&feed).await {
                tokio::select! {
                    _ = stopping.cancelled() => {}
                    _ = woken.changed() => {}
                }
            }
        }
        feeder
    });
    let storage = &fixture.driver.storage_handle;
    let conflicts = storage.snapshot_metrics().conflicts_total;
    let started = std::time::Instant::now();
    // Each job runs as its own task, as on a node.
    let runs = runs
        .into_iter()
        .map(|(job, spec)| tokio::spawn(async move { run_staging_job(&job, &spec).await }));
    let outcomes = futures_util::future::join_all(runs).await;
    let elapsed = started.elapsed().as_secs_f64();
    // The catch-up counts from the jobs' end, the feed's pass running then included.
    let ended = std::time::Instant::now();
    let conflicts = storage.snapshot_metrics().conflicts_total - conflicts;
    for watcher in watchers {
        watcher.abort();
    }
    drain.abort();
    stop.cancel();
    let mut feeder = feed.await.unwrap();
    server.abort();
    // The index catches up with what the jobs wrote.
    while background && feeder.pass(&fixture.driver).await {}
    let index = ended.elapsed().as_secs_f64();
    if background {
        assert!(
            feed_rows(&fixture).await.is_empty(),
            "the catch-up applied every feed row"
        );
    }
    let (mut completed, mut failed) = (0, 0);
    for outcome in outcomes {
        let (completed_items, failed_items) = match outcome.unwrap() {
            JobRunOutcome::Succeeded(JobResultPayload::Staging {
                completed_items,
                failed_items,
            }) => (completed_items, failed_items),
            JobRunOutcome::Failed(error) | JobRunOutcome::Deferred(error) => {
                panic!("the table import stopped: {error:?}")
            }
            _ => panic!("the table import did not finish"),
        };
        (completed, failed) = (completed + completed_items, failed + failed_items);
    }
    ((completed, failed, conflicts), elapsed, index)
}

/// Makes the job's bucket a searched distributed set whose upper range, holding keys from `m`,
/// this member coordinates, with a full key sample like a large bucket; returns set and range.
async fn searched_set(ctx: &JobContext, spec: &StagingJobSpec) -> (Ulid, Ulid) {
    use aruna_core::effects::StorageEffect;
    use aruna_core::keyspaces::{KEY_SAMPLE_KEYSPACE, RANGE_COUNTER_KEYSPACE, S3_BUCKET_KEYSPACE};
    use aruna_core::structs::storage::distribution::KeyRange;
    use aruna_core::structs::storage::distribution::accounting::{
        RangeCounters, SAMPLE_SIZE, SampleEntry, sample_count_key,
    };
    use aruna_operations::distribution::test_record::{binding, searched, store_record};
    use aruna_operations::distribution::test_store::{begin, commit, install, range};
    let handle = &ctx.driver.storage_handle;
    let (snapshot, bytes) = searched();
    let set_id = snapshot.set_id();
    store_record(handle, set_id, bytes).await;
    let found = GetBucketOperation::new(spec.bucket.clone());
    let mut bucket = aruna_operations::driver::drive(found, &ctx.driver)
        .await
        .unwrap();
    bucket.distribution = Some(binding(&snapshot, 2, 5));
    let range_id = snapshot.range_for("samn").unwrap().range_id;
    let ranges = [KeyRange {
        range_id,
        ..range(1, "m", None)
    }];
    install(handle, set_id, &ranges).await;
    let full = RangeCounters {
        sampled: i128::from(SAMPLE_SIZE),
        ..RangeCounters::default()
    };
    let largest = SampleEntry {
        priority: u64::MAX,
        key: "largest".to_string(),
    };
    let txn_id = begin(handle).await;
    let effects = [
        StorageEffect::Write {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: spec.bucket.as_bytes().to_vec().into(),
            value: bucket.to_bytes().unwrap().into(),
            txn_id: Some(txn_id),
        },
        StorageEffect::AddCounters {
            key_space: RANGE_COUNTER_KEYSPACE.to_string(),
            deltas: vec![(sample_count_key(set_id, range_id).into(), full)],
            txn_id,
        },
        StorageEffect::Write {
            key_space: KEY_SAMPLE_KEYSPACE.to_string(),
            key: largest.row_key(set_id, range_id).into(),
            value: Vec::new().into(),
            txn_id: Some(txn_id),
        },
    ];
    for effect in effects {
        handle.send_storage_effect(effect).await;
    }
    assert!(commit(handle, txn_id).await);
    (set_id, range_id)
}

// The two fixture assemblies become folder objects with their summary columns; their files,
// subfolders included, become references with one HEAD each; a re-run writes nothing.
#[tokio::test]
async fn summary_imports_folders() {
    let (endpoint, site, server) = serve(Site::ncbi).await;
    let fixture = Fixture::new(&endpoint, summary_table()).await;

    assert_eq!(run_fixture(&fixture, 7).await, (4, 0));
    let bucket = fixture.spec.bucket.clone();
    let folder = "GCF_000005845.2_ASM584v2/";
    let (folder_version, metadata) = fixture.head(&bucket, folder).await.unwrap();
    assert_eq!(metadata["taxid"], "511145");
    let job_id = Some(JobId::from_bytes([7; 16]).as_ulid());
    let origin = fixture.origin(folder).await;
    assert_eq!(origin, Some(VersionOrigin::Source { job_id }));
    assert_eq!(
        metadata["organism_name"],
        "Escherichia coli str. K-12 substr. MG1655"
    );
    assert!(!metadata.contains_key("wgs_master"));
    assert_eq!(
        metadata[SEARCH_EXACT_KEY],
        "taxid,species_taxid,assembly_level,group"
    );
    let genome = format!("{folder}GCF_000005845.2_ASM584v2_genomic.fna.gz");
    let (genome_version, genome_metadata) = fixture.head(&bucket, &genome).await.unwrap();
    assert!(genome_metadata.is_empty());
    let nested = format!("GCF_000001405.40_GRCh38.p14/{STRUCTURE}chr1.fna");
    assert!(fixture.head(&bucket, &nested).await.is_some());
    assert!(
        fixture
            .head(&bucket, "GCF_000006945.2_ASM694v2/")
            .await
            .is_none()
    );
    // Index pages round sizes, so every file got its observation from a HEAD.
    let file = format!("{COLI}GCF_000005845.2_ASM584v2_genomic.fna.gz");
    assert_eq!(site.count(&Method::HEAD, &file), 1);

    assert_eq!(run_fixture(&fixture, 8).await, (4, 0));
    assert_eq!(
        fixture.head(&bucket, folder).await.unwrap().0,
        folder_version
    );
    assert_eq!(
        fixture.head(&bucket, &genome).await.unwrap().0,
        genome_version
    );

    // An ordinary bucket matches metadata during its head scan; tax ids match only exactly.
    assert_eq!(
        fixture.search("escherichia species_taxid=562").await,
        [folder]
    );
    assert_eq!(fixture.search("coli taxid=562").await, Vec::<String>::new());
    let complete = r#"assembly_level="Complete Genome""#;
    assert_eq!(fixture.search(complete).await, [folder]);
    assert_eq!(
        fixture.search("sapiens").await,
        ["GCF_000001405.40_GRCh38.p14/"]
    );
    server.abort();
}

// A gzip table spanning many chunks is fetched once per run: each chunk continues the decoder
// of the one before instead of decompressing the table from its start again.
#[tokio::test]
async fn gzip_table_streams() {
    use std::io::Write;
    const GZIP: &str = "ASSEMBLY_REPORTS/assembly_summary_refseq.txt.gz";
    let (endpoint, site, server) = serve(|endpoint| {
        let mut site = Site::ncbi(endpoint);
        let summary = String::from_utf8(site.files[GZIP.trim_end_matches(".gz")].clone()).unwrap();
        let (header, rows) = summary.split_at(summary.find("\nGCF_").unwrap() + 1);
        // A filtered row repeated until the real rows lie several chunks into the table.
        let filler = rows.lines().nth(2).unwrap();
        let mut table = header.to_string();
        while table.len() < 4 * aruna_core::structs::execution::table::TABLE_CHUNK {
            table.push_str(filler);
            table.push('\n');
        }
        table.push_str(rows);
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), Default::default());
        encoder.write_all(table.as_bytes()).unwrap();
        site.files
            .insert(GZIP.to_string(), encoder.finish().unwrap());
        site
    })
    .await;
    let mut table = summary_table();
    table["source"]["connector"] = GZIP.into();
    let fixture = Fixture::new(&endpoint, table).await;

    assert_eq!(run_fixture(&fixture, 9).await, (4, 0));
    assert_eq!(
        site.count(&Method::GET, GZIP),
        1,
        "one stream for the whole table"
    );
    server.abort();
}

// An uploaded CSV of file URLs imports one reference per row with its kept columns, read as
// the submitter.
#[tokio::test]
async fn uploaded_list_imports() {
    let (endpoint, _site, server) = serve(Site::ncbi).await;
    let table = serde_json::json!({
        "source": { "object": { "bucket": "lists", "key": "samples.csv" } },
        "rows": { "kind": "file", "source": "{url}", "target": "samples/{url|basename}",
                  "keep": ["sample", "description"], "text": ["description"],
                  "exact": ["sample"] }
    });
    let fixture = Fixture::new(&endpoint, table).await;
    let csv = format!(
        "url,sample,description\n\
         {endpoint}/genomes/{COLI}md5checksums.txt,s1,\"checksums, coli\"\n\
         {HUMAN}README.txt,s2,human readme\n\
         {HUMAN}README.txt,s2,human readme\n\
         {HUMAN}README.txt,s4,other readme\n\
         https://elsewhere.example.org/x.txt,s3,outside\n"
    );
    let ctx = fixture.context(9).await;
    let lists = drive(
        GetBucketOperation::new("lists".to_string()),
        &fixture.driver,
    )
    .await
    .unwrap();
    let body = (Bytes::from(csv), &BTreeMap::new());
    write_object(&ctx, &fixture.spec, ("lists", &lists), "samples.csv", body)
        .await
        .ok()
        .unwrap();

    // A repeated row imports once; a row mapping its key differently fails.
    assert_eq!(run_fixture(&fixture, 10).await, (2, 2));
    let bucket = fixture.spec.bucket.clone();
    let (_, metadata) = fixture
        .head(&bucket, "samples/md5checksums.txt")
        .await
        .unwrap();
    assert_eq!(metadata["sample"], "s1");
    assert_eq!(metadata["description"], "checksums, coli");
    let (_, readme) = fixture.head(&bucket, "samples/README.txt").await.unwrap();
    assert_eq!(readme["sample"], "s2");
    assert_eq!(
        fixture.search("readme sample=s2").await,
        ["samples/README.txt"]
    );
    server.abort();
}

// List columns give size, checksum and time: such rows are registered without a HEAD, reads
// check the bytes against the list's checksum, and metadata hits carry the kept columns.
#[tokio::test]
async fn list_columns_recorded() {
    let (endpoint, site, server) = serve(Site::ncbi).await;
    let table = serde_json::json!({
        "source": { "object": { "bucket": "lists", "key": "files.csv" } },
        "rows": { "kind": "file", "source": "{url}", "target": "files/{name}",
                  "size": "bytes", "modified": "time",
                  "checksums": [{ "algorithm": "md5", "column": "md5" }],
                  "keep": ["note"], "text": ["note"] }
    });
    let fixture = Fixture::new(&endpoint, table).await;
    let good = format!("{:x}", md5::compute(b"sums"));
    let wrong = format!("{:x}", md5::compute(b"other"));
    let csv = format!(
        "url,name,bytes,md5,time,note\n\
         {COLI}md5checksums.txt,good.txt,4,{good},2025-11-17T17:38:21Z,coli sums\n\
         {HUMAN}README.txt,wrong.txt,6,{wrong},,human readme\n\
         {HUMAN}README.txt,bad.txt,six,{good},,broken size\n\
         {COLI}md5checksums.txt,drift.txt,5,{good},,size off\n\
         {COLI}GCF_000005845.2_ASM584v2_genomic.fna.gz,timed.fna,,,2020-01-02T03:04:05Z,headed\n"
    );
    let ctx = fixture.context(30).await;
    let lists = drive(
        GetBucketOperation::new("lists".to_string()),
        &fixture.driver,
    )
    .await;
    let body = (Bytes::from(csv), &BTreeMap::new());
    let lists = ("lists", &lists.unwrap());
    write_object(&ctx, &fixture.spec, lists, "files.csv", body)
        .await
        .ok()
        .unwrap();

    assert_eq!(run_fixture(&fixture, 31).await, (4, 1));
    assert_eq!(
        site.count(&Method::HEAD, &format!("{COLI}md5checksums.txt")),
        0
    );
    assert_eq!(site.count(&Method::HEAD, &format!("{HUMAN}README.txt")), 0);
    // A row without a size is headed, and keeps the time the list gives.
    let headed = format!("{COLI}GCF_000005845.2_ASM584v2_genomic.fna.gz");
    assert_eq!(site.count(&Method::HEAD, &headed), 1);
    let timed = fixture.observation("files/timed.fna").await;
    let at = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_577_934_245);
    assert_eq!((timed.content_length, timed.last_modified), (4, Some(at)));
    assert_eq!(fixture.read("files/good.txt").await.unwrap(), b"sums");
    assert!(fixture.read("files/wrong.txt").await.is_err());
    // A source whose size differs is refused; no successor drops the list's checksum.
    let bucket = fixture.spec.bucket.clone();
    let before = fixture.head(&bucket, "files/drift.txt").await.unwrap().0;
    assert!(fixture.read("files/drift.txt").await.is_err());
    let after = fixture.head(&bucket, "files/drift.txt").await.unwrap().0;
    assert_eq!(before, after);

    let hits = fixture.hits("sums").await;
    assert_eq!(hits.len(), 1);
    let kept = &hits[0].metadata;
    assert_eq!(kept.get("note").map(String::as_str), Some("coli sums"));
    assert!(kept.keys().all(|key| !key.starts_with("aruna-")));
    server.abort();
}

// Record rows are trusted as they are: the source sees no request, the references keep the
// row's ETag, type, time and metadata, and a row without a source writes an empty object.
#[tokio::test]
async fn record_rows_trusted() {
    let (endpoint, site, server) = serve(Site::ncbi).await;
    let table = serde_json::json!({
        "source": { "object": { "bucket": "lists", "key": "records.tsv" } },
        "format": { "delimiter": "\t", "quote": null },
        "rows": { "kind": "record", "source": "{source}", "target": "{key}", "keep": [],
                  "size": "size", "modified": "modified",
                  "checksums": [{ "algorithm": "md5", "column": "md5" }] }
    });
    let fixture = Fixture::new(&endpoint, table).await;
    let md5 = format!("{:x}", md5::compute(b"sums"));
    let meta = r#"{"note":"coli sums","aruna-search-text":"note"}"#;
    let tsv = format!(
        "key\tsource\tsize\tmodified\tetag\tcontent_type\tmd5\tmetadata\n\
         coli/sums.txt\t{COLI}md5checksums.txt\t4\t2025-11-17T17:38:21Z\t\"e1\"\ttext/plain\t{md5}\t{meta}\n\
         coli/\t\t0\t\t\t\t\t{meta}\n"
    );
    let ctx = fixture.context(40).await;
    let lists = drive(
        GetBucketOperation::new("lists".to_string()),
        &fixture.driver,
    )
    .await;
    let body = (Bytes::from(tsv), &BTreeMap::new());
    let lists = ("lists", &lists.unwrap());
    write_object(&ctx, &fixture.spec, lists, "records.tsv", body)
        .await
        .ok()
        .unwrap();

    // The empty row counts as imported, like the row with a source.
    assert_eq!(run_fixture(&fixture, 41).await, (2, 0));
    assert!(site.requests.lock().unwrap().is_empty());
    let observation = fixture.observation("coli/sums.txt").await;
    let at = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_763_401_101);
    assert_eq!(observation.content_length, 4);
    assert_eq!(observation.etag.as_deref(), Some("\"e1\""));
    assert_eq!(observation.content_type.as_deref(), Some("text/plain"));
    assert_eq!(observation.last_modified, Some(at));
    let bucket = fixture.spec.bucket.clone();
    let (_, folder) = fixture.head(&bucket, "coli/").await.unwrap();
    assert_eq!(folder["note"], "coli sums");
    // The empty object is the import's like the reference, not an upload.
    let job_id = Some(JobId::from_bytes([41; 16]).as_ulid());
    for key in ["coli/sums.txt", "coli/"] {
        let origin = fixture.origin(key).await;
        assert_eq!(origin, Some(VersionOrigin::Source { job_id }));
    }
    let mut found = fixture.search("sums").await;
    found.sort();
    assert_eq!(found, ["coli/", "coli/sums.txt"]);
    server.abort();
}

/// Runs the fixture's staging job under the node's job host and counts what it imported.
async fn run_fixture(fixture: &Fixture, seed: u8) -> (u64, u64) {
    let ctx = fixture.context(seed).await;
    match run_staging_job(&ctx, &fixture.spec).await {
        JobRunOutcome::Succeeded(JobResultPayload::Staging {
            completed_items,
            failed_items,
        }) => (completed_items, failed_items),
        JobRunOutcome::Failed(error) => panic!("the table import failed: {error:?}"),
        _ => panic!("the table import did not finish"),
    }
}
