use super::*;

pub(super) fn object_search_fingerprint(
    realm_id: RealmId,
    query: &str,
    key_match: ObjectKeyMatch,
    bucket: Option<&str>,
    mode: ObjectSearchQueryMode,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"aruna.object.search.v1\0");
    hasher.update(realm_id.as_bytes());
    hasher.update(query.as_bytes());
    hasher.update(&[0]);
    hasher.update(&[match key_match {
        ObjectKeyMatch::Substring => 1,
        ObjectKeyMatch::Prefix => 2,
    }]);
    match bucket {
        Some(bucket) => {
            hasher.update(&[1]);
            hasher.update(bucket.as_bytes());
        }
        None => {
            hasher.update(&[0]);
        }
    }
    hasher.update(&[match mode {
        ObjectSearchQueryMode::Local => 1,
        ObjectSearchQueryMode::DistributedBestEffort => 2,
        ObjectSearchQueryMode::DistributedStrict => 3,
    }]);
    *hasher.finalize().as_bytes()
}

pub(super) fn record_object_result(
    span: &Span,
    result: &Result<ObjectSearchNodePage, MetadataReadError>,
) {
    match result {
        Ok(page) => {
            span.record("result", "ok");
            span.record("hit_count", page.hits.len() as u64);
        }
        Err(_) => {
            span.record("result", "error");
        }
    }
}

pub(super) fn record_bucket_result(
    span: &Span,
    result: &Result<Vec<BucketSearchHit>, MetadataReadError>,
) {
    match result {
        Ok(hits) => {
            span.record("result", "ok");
            span.record("hit_count", hits.len() as u64);
        }
        Err(_) => {
            span.record("result", "error");
        }
    }
}

pub(super) fn record_query_result(
    span: &Span,
    result: &Result<MetadataQueryResults, MetadataReadError>,
) {
    match result {
        Ok(result) => {
            span.record("result", result.kind());
        }
        Err(_) => {
            span.record("result", "error");
        }
    }
}

pub(super) fn record_search_node_result(
    span: &Span,
    result: &Result<(Vec<MetadataSearchHit>, usize), MetadataReadError>,
) {
    match result {
        Ok((hits, _)) => {
            span.record("result", "ok");
            span.record("hit_count", hits.len() as u64);
        }
        Err(_) => {
            span.record("result", "error");
        }
    }
}

pub(super) fn record_preflight_node_result(
    span: &Span,
    result: &Result<MetadataReferencePreflightNodeExecution, MetadataReadError>,
) {
    match result {
        Ok(result) => {
            span.record("result", "ok");
            span.record("hit_count", result.visible_references.len() as u64);
        }
        Err(_) => {
            span.record("result", "error");
        }
    }
}
