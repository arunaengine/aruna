use super::*;

pub(super) fn plan_export(
    spec: &ExportRoCrateSpec,
    checkpoint: &mut ExportCheckpoint,
    opened: &[ProbedEntry],
) -> Result<(), ExportFailure> {
    let sources = opened
        .iter()
        .map(|entry| {
            source_key(
                &checkpoint.entities[entry.entity_index],
                entry.candidate_index,
            )
        })
        .collect::<Vec<_>>();
    let layout = KeyLayout::new(&sources);
    let mut paths = HashSet::new();
    for (entry, source) in opened.iter().zip(&sources) {
        let entity = &mut checkpoint.entities[entry.entity_index];
        entity.report_source = Some(entry.report_source);
        entity.resolved_version = entry.resolved_version;
        let reserved = |path: &String| path == METADATA_PATH || path == REPORT_PATH;
        let explicit = entity
            .local_path
            .as_deref()
            .and_then(safe_zip_path)
            .filter(|path| !reserved(path));
        let derived = source
            .as_ref()
            .and_then(|source| layout.path(source))
            .filter(|path| !reserved(path));
        let path = match explicit {
            Some(path) => path,
            None => {
                entity.path_synthesized = true;
                derived.unwrap_or_else(|| synthesized_path(entry.hash, &entity.entity_id))
            }
        };
        if path.len() as u64 > spec.limits.key_bytes {
            return Err(ExportFailure::Permanent(format!(
                "ZIP path exceeds the {} byte limit",
                spec.limits.key_bytes
            )));
        }
        if !paths.insert(path.clone()) {
            return Err(ExportFailure::Permanent(format!(
                "multiple File entities resolve to ZIP path `{path}`"
            )));
        }
        entity.zip_path = Some(path);
    }

    let raw = checkpoint
        .raw_jsonld
        .as_deref()
        .ok_or_else(|| ExportFailure::Permanent("raw RO-Crate snapshot is missing".to_string()))?;
    let mut document: JsonValue =
        serde_json::from_str(raw).map_err(|error| ExportFailure::Permanent(error.to_string()))?;
    let replacements = checkpoint
        .entities
        .iter()
        .filter_map(|entity| {
            entity
                .zip_path
                .as_ref()
                .map(|path| (entity.entity_id.clone(), jsonld_path(path)))
        })
        .collect::<BTreeMap<_, _>>();
    let unrewritten = scan_unrewritten(&document, &replacements);
    rewrite_ids(&mut document, &replacements);
    checkpoint.report = build_rows(&checkpoint.entities, &unrewritten);
    let has_omissions = checkpoint.report.iter().any(|row| {
        matches!(
            row.code,
            ReasonCode::External
                | ReasonCode::Denied
                | ReasonCode::Missing
                | ReasonCode::Offline
                | ReasonCode::Unsupported
        )
    });
    checkpoint.report_json = if has_omissions {
        let report = build_report(checkpoint)?;
        add_report(&mut document)?;
        Some(
            serde_json::to_vec(&report)
                .map_err(|error| ExportFailure::Permanent(error.to_string()))?,
        )
    } else {
        None
    };
    let rewritten = serde_json::to_vec(&document)
        .map_err(|error| ExportFailure::Permanent(error.to_string()))?;
    if rewritten.len() as u64 > spec.limits.metadata_bytes {
        return Err(ExportFailure::Permanent(format!(
            "rewritten RO-Crate metadata exceeds the {} byte limit",
            spec.limits.metadata_bytes
        )));
    }
    craqle::validate_rocrate_jsonld(
        std::str::from_utf8(&rewritten)
            .map_err(|error| ExportFailure::Permanent(error.to_string()))?,
    )
    .map_err(map_crate_error)?;
    precheck_size(
        spec,
        &rewritten,
        checkpoint.report_json.as_deref(),
        opened,
        &checkpoint.entities,
    )?;
    checkpoint.rewritten_jsonld = Some(rewritten);
    checkpoint.phase = ExportPhase::Assemble;
    Ok(())
}

pub(super) fn recognize_entities(
    document: &JsonValue,
    nquads: &str,
    realm_id: RealmId,
) -> Result<Vec<ExportEntity>, ExportFailure> {
    let keywords = JsonLdKeywords::new(document);
    let raw_ids = raw_entity_ids(document, &keywords)?;
    let mut files = BTreeSet::new();
    let mut content_urls = BTreeMap::<String, Vec<String>>::new();
    let mut local_paths = BTreeMap::<String, Vec<String>>::new();
    for quad in NQuadsParser::new().for_slice(nquads) {
        let quad = quad.map_err(|error| ExportFailure::Permanent(error.to_string()))?;
        let NamedOrBlankNode::NamedNode(subject) = quad.subject else {
            continue;
        };
        let subject = subject.as_str().to_string();
        match quad.predicate.as_str() {
            RDF_TYPE_IRI
                if matches!(
                    &quad.object,
                    Term::NamedNode(node)
                        if is_file_type(node.as_str())
                ) =>
            {
                files.insert(subject);
            }
            SCHEMA_CONTENT_IRI | SCHEMA_CONTENT_HTTPS_IRI => {
                if let Some(value) = term_value(&quad.object) {
                    content_urls.entry(subject).or_default().push(value);
                }
            }
            LOCAL_PATH_IRI | LOCAL_PATH_HTTP_IRI => {
                if let Some(value) = term_value(&quad.object) {
                    local_paths.entry(subject).or_default().push(value);
                }
            }
            _ => {}
        }
    }

    let mut entities = Vec::new();
    for (subject, entity_id, raw_path) in raw_ids {
        if !files.remove(&subject) {
            continue;
        }
        let urls = content_urls.get(&subject).map_or(&[][..], Vec::as_slice);
        let identity = entity_identity(&entity_id, urls);
        let storage_key = identity
            .exact
            .as_ref()
            .filter(|exact| exact.realm_id == realm_id)
            .map(|exact| StorageKey {
                bucket: exact.bucket.clone(),
                key: exact.key.clone(),
            })
            .or_else(|| {
                std::iter::once(entity_id.as_str())
                    .chain(urls.iter().map(String::as_str))
                    .find_map(object_location)
            });
        let external = identity.exact.is_none() && identity.hash.is_none();
        let hash_realm = identity.hash_realm;
        let supported_exact = identity
            .exact
            .as_ref()
            .is_some_and(|exact| exact.realm_id == realm_id);
        let supported_hash =
            identity.hash.is_some() && hash_realm.is_none_or(|hash_realm| hash_realm == realm_id);
        let unsupported_realm = !external && !supported_exact && !supported_hash;
        let paths = local_paths.remove(&subject).unwrap_or_default();
        let local_path = raw_path
            .filter(|raw_path| paths.contains(raw_path))
            .or_else(|| paths.into_iter().next());
        entities.push(ExportEntity {
            entity_id,
            local_path,
            storage_key,
            exact: identity.exact,
            hash: identity.hash,
            hash_realm,
            candidates: Vec::new(),
            omission: if external {
                Some(ReasonCode::External)
            } else if unsupported_realm {
                Some(ReasonCode::Unsupported)
            } else {
                None
            },
            message: if external {
                Some("external File entity was not fetched".to_string())
            } else if unsupported_realm {
                Some("Aruna identifier belongs to another realm".to_string())
            } else {
                None
            },
            zip_path: None,
            report_source: None,
            resolved_version: None,
            path_synthesized: false,
        });
    }
    if let Some(subject) = files.into_iter().next() {
        return Err(ExportFailure::Permanent(format!(
            "expanded File entity `{subject}` has no raw JSON-LD definition"
        )));
    }
    Ok(entities)
}

pub(super) fn raw_entity_ids(
    document: &JsonValue,
    keywords: &JsonLdKeywords,
) -> Result<Vec<(String, String, Option<String>)>, ExportFailure> {
    fn collect(
        value: &JsonValue,
        keywords: &JsonLdKeywords,
        entities: &mut Vec<(String, String, Option<String>)>,
    ) -> Result<(), ExportFailure> {
        match value {
            JsonValue::Array(values) => {
                for value in values {
                    collect(value, keywords, entities)?;
                }
            }
            JsonValue::Object(object) => {
                if object.len() > 1
                    && let Some((_, id)) = keywords.object_id(object)
                {
                    let expanded = expanded_id(id)?;
                    if let Some((_, existing_id, _)) = entities
                        .iter()
                        .find(|(existing, _, _)| existing == &expanded)
                    {
                        if existing_id != id {
                            return Err(ExportFailure::Permanent(format!(
                                "JSON-LD entity `{expanded}` uses ambiguous identifiers"
                            )));
                        }
                    } else {
                        entities.push((expanded, id.to_string(), raw_local_path(object, keywords)));
                    }
                }
                for value in object.values() {
                    collect(value, keywords, entities)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    let mut entities = Vec::new();
    collect(document, keywords, &mut entities)?;
    Ok(entities)
}

pub(super) fn raw_local_path(
    object: &serde_json::Map<String, JsonValue>,
    keywords: &JsonLdKeywords,
) -> Option<String> {
    object.iter().find_map(|(key, value)| {
        keywords
            .expands_to(key, &["localPath", LOCAL_PATH_IRI, LOCAL_PATH_HTTP_IRI])
            .then(|| match value {
                JsonValue::String(value) => Some(value.clone()),
                JsonValue::Array(values) => values
                    .iter()
                    .find_map(JsonValue::as_str)
                    .map(str::to_string),
                _ => None,
            })
            .flatten()
    })
}

pub(super) fn expanded_id(id: &str) -> Result<String, ExportFailure> {
    if let Ok(url) = Url::parse(id) {
        return Ok(url.to_string());
    }
    Url::parse(JSONLD_BASE_IRI)
        .expect("static JSON-LD base is valid")
        .join(id)
        .map(String::from)
        .map_err(|error| ExportFailure::Permanent(error.to_string()))
}

pub(super) fn term_value(term: &Term) -> Option<String> {
    match term {
        Term::NamedNode(value) => Some(value.as_str().to_string()),
        Term::Literal(value) => Some(value.value().to_string()),
        _ => None,
    }
}

/// Reads an Aruna object identity out of a data entity's `@id` and
/// `contentUrl` values: a versioned ARN, or a content hash W3ID or ARN.
pub(crate) fn entity_identity(entity_id: &str, content_urls: &[String]) -> EntityIdentity {
    let mut exact = None;
    let mut hash = None;
    let mut hash_realm = None;
    for value in std::iter::once(entity_id).chain(content_urls.iter().map(String::as_str)) {
        if let Ok(identifier) = W3idDataIdentifier::parse(value) {
            match identifier {
                W3idDataIdentifier::ContentHash(value) => hash = Some(value),
                W3idDataIdentifier::VersionedObject(value) => exact = Some(value),
            }
            continue;
        }
        if let Ok(value) = VersionedObjectArn::parse(value) {
            exact = Some(value);
            continue;
        }
        if let Ok(value) = ArunaArn::parse(value)
            && value.resource_type == ArunaArnType::ContentHash
            && let Some(value_hash) = parse_hash(&value.path)
        {
            hash = Some(value_hash);
            hash_realm = Some(value.realm_id);
        }
    }
    EntityIdentity {
        exact,
        hash,
        hash_realm,
    }
}

pub(super) fn parse_hash(value: &str) -> Option<[u8; 32]> {
    let value = value.strip_prefix("blake3/").unwrap_or(value);
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return None;
    }
    let mut hash = [0; 32];
    hex::decode_to_slice(value, &mut hash).ok()?;
    Some(hash)
}

pub(super) fn safe_zip_path(value: &str) -> Option<String> {
    let mut value = value;
    while let Some(stripped) = value.strip_prefix("./") {
        value = stripped;
    }
    let normalized = value.nfc().collect::<String>();
    let lower = normalized.to_ascii_lowercase();
    if normalized.is_empty()
        || normalized.ends_with('/')
        || normalized.contains('\\')
        || lower.contains("%2f")
        || lower.contains("%5c")
        || normalized
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
        || ensure_confined_path(Path::new(&normalized)).is_err()
        || Path::new(&normalized)
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return None;
    }
    Some(normalized)
}

pub(super) fn jsonld_path(path: &str) -> String {
    let mut url = Url::parse(JSONLD_BASE_IRI).expect("static JSON-LD base is valid");
    {
        let mut segments = url
            .path_segments_mut()
            .expect("static JSON-LD base supports path segments");
        segments.clear();
        for segment in path.split('/') {
            segments.push(segment);
        }
    }
    url.path().trim_start_matches('/').to_string()
}

pub(super) fn synthesized_path(hash: [u8; 32], entity_id: &str) -> String {
    let suffix = blake3::hash(entity_id.as_bytes()).to_hex();
    format!("data/{}-{}", hex::encode(hash), &suffix[..12])
}

pub(super) fn object_location(value: &str) -> Option<StorageKey> {
    let (bucket, key) = value.strip_prefix("s3://")?.split_once('/')?;
    (!bucket.is_empty() && !key.is_empty()).then(|| StorageKey {
        bucket: bucket.to_string(),
        key: key.to_string(),
    })
}

/// The authored location wins over the resolved candidate: a content hash may
/// be served from any alias, and only the authored key matches the crate.
pub(super) fn source_key(entity: &ExportEntity, candidate_index: usize) -> Option<StorageKey> {
    if let Some(storage_key) = entity.storage_key.clone() {
        return Some(storage_key);
    }
    match &entity.candidates.get(candidate_index)?.source {
        CandidateSource::Local { bucket, key, .. } => Some(StorageKey {
            bucket: bucket.clone(),
            key: key.clone(),
        }),
        CandidateSource::RemoteExact { target, .. } => Some(StorageKey {
            bucket: target.bucket.clone(),
            key: target.key.clone(),
        }),
        CandidateSource::RemoteHash { .. } => None,
    }
}

/// How keys become archive paths: one bucket drops the directory prefix every
/// payload shares, several buckets keep the whole key under the bucket name.
struct KeyLayout {
    dropped: usize,
    with_bucket: bool,
}

impl KeyLayout {
    fn new(sources: &[Option<StorageKey>]) -> Self {
        let mut buckets = BTreeSet::new();
        let mut shared: Option<Vec<&str>> = None;
        for source in sources.iter().flatten() {
            buckets.insert(source.bucket.as_str());
            let parents = key_parents(&source.key);
            shared = Some(match shared {
                Some(shared) => common_prefix(shared, &parents),
                None => parents,
            });
        }
        let single = buckets.len() == 1;
        Self {
            dropped: if single {
                shared.map_or(0, |shared| shared.len())
            } else {
                0
            },
            with_bucket: !single,
        }
    }

    fn path(&self, source: &StorageKey) -> Option<String> {
        let relative = source
            .key
            .split('/')
            .skip(self.dropped)
            .collect::<Vec<_>>()
            .join("/");
        let candidate = if self.with_bucket {
            format!("{}/{relative}", source.bucket)
        } else {
            relative
        };
        safe_zip_path(&candidate)
    }
}

pub(super) fn key_parents(key: &str) -> Vec<&str> {
    let mut parents = key.split('/').collect::<Vec<_>>();
    parents.pop();
    parents
}

pub(super) fn common_prefix<'a>(mut shared: Vec<&'a str>, parents: &[&str]) -> Vec<&'a str> {
    let common = shared
        .iter()
        .zip(parents)
        .take_while(|(left, right)| left == right)
        .count();
    shared.truncate(common);
    shared
}

pub(super) fn scan_unrewritten(
    document: &JsonValue,
    replacements: &BTreeMap<String, String>,
) -> BTreeSet<String> {
    fn scan(
        value: &JsonValue,
        key: Option<&str>,
        replacements: &BTreeMap<String, String>,
        keywords: &JsonLdKeywords,
        found: &mut BTreeSet<String>,
    ) {
        match value {
            JsonValue::String(value)
                if !key.is_some_and(|key| keywords.is_id(key))
                    && replacements.contains_key(value.as_str()) =>
            {
                found.insert(value.clone());
            }
            JsonValue::Array(values) => {
                for value in values {
                    scan(value, key, replacements, keywords, found);
                }
            }
            JsonValue::Object(values) => {
                for (key, value) in values {
                    scan(value, Some(key), replacements, keywords, found);
                }
            }
            _ => {}
        }
    }
    let keywords = JsonLdKeywords::new(document);
    let mut found = BTreeSet::new();
    scan(document, None, replacements, &keywords, &mut found);
    found
}

pub(super) fn rewrite_ids(value: &mut JsonValue, replacements: &BTreeMap<String, String>) {
    let keywords = JsonLdKeywords::new(value);
    rewrite_id_values(value, replacements, &keywords);
}

pub(super) fn rewrite_id_values(
    value: &mut JsonValue,
    replacements: &BTreeMap<String, String>,
    keywords: &JsonLdKeywords,
) {
    match value {
        JsonValue::Array(values) => {
            for value in values {
                rewrite_id_values(value, replacements, keywords);
            }
        }
        JsonValue::Object(values) => {
            let id_key = keywords.object_id(values).map(|(key, _)| key.to_string());
            if let Some(JsonValue::String(value)) =
                id_key.as_deref().and_then(|key| values.get_mut(key))
                && let Some(replacement) = replacements.get(value)
            {
                *value = replacement.clone();
            }
            for value in values.values_mut() {
                rewrite_id_values(value, replacements, keywords);
            }
        }
        _ => {}
    }
}

pub(super) fn build_rows(
    entities: &[ExportEntity],
    unrewritten: &BTreeSet<String>,
) -> Vec<ExportReportRow> {
    let mut rows = Vec::new();
    for (index, entity) in entities.iter().enumerate() {
        let main_code = entity.omission.unwrap_or(ReasonCode::Included);
        rows.push(ExportReportRow {
            entry_key: format!("{index:016x}:main"),
            code: main_code,
            message: entity.message.clone(),
            detail: ExportReportDetail {
                entity_id: entity.entity_id.clone(),
                zip_path: entity.zip_path.clone(),
                source: entity.report_source,
                resolved_version: entity.resolved_version,
                validation: None,
            },
        });
        if entity.path_synthesized {
            rows.push(ExportReportRow {
                entry_key: format!("{index:016x}:path"),
                code: ReasonCode::PathSynthesized,
                message: Some("unsafe, absent, or reserved localPath was synthesized".to_string()),
                detail: ExportReportDetail {
                    entity_id: entity.entity_id.clone(),
                    zip_path: entity.zip_path.clone(),
                    source: entity.report_source,
                    resolved_version: entity.resolved_version,
                    validation: None,
                },
            });
        }
        if unrewritten.contains(&entity.entity_id) {
            rows.push(ExportReportRow {
                entry_key: format!("{index:016x}:reference"),
                code: ReasonCode::UnrewrittenReference,
                message: Some(
                    "a string-form reference outside an @id field was preserved".to_string(),
                ),
                detail: ExportReportDetail {
                    entity_id: entity.entity_id.clone(),
                    zip_path: entity.zip_path.clone(),
                    source: entity.report_source,
                    resolved_version: entity.resolved_version,
                    validation: None,
                },
            });
        }
    }
    rows
}

pub(super) fn build_report(checkpoint: &ExportCheckpoint) -> Result<JsonValue, ExportFailure> {
    let event_id = checkpoint
        .winning_event_id
        .ok_or_else(|| ExportFailure::Permanent("snapshot event cursor is missing".to_string()))?;
    let context_digest = checkpoint.context_digest.ok_or_else(|| {
        ExportFailure::Permanent("snapshot context digest is missing".to_string())
    })?;
    let dataset_digest = checkpoint.dataset_digest.ok_or_else(|| {
        ExportFailure::Permanent("snapshot dataset digest is missing".to_string())
    })?;
    let omissions = checkpoint
        .report
        .iter()
        .filter(|row| {
            matches!(
                row.code,
                ReasonCode::External
                    | ReasonCode::Denied
                    | ReasonCode::Missing
                    | ReasonCode::Offline
                    | ReasonCode::Unsupported
            )
        })
        .map(|row| {
            json!({
                "entity_id": &row.detail.entity_id,
                "code": row.code,
                "message": &row.message,
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "winning_event_id": event_id,
        "context_digest": hex::encode(context_digest),
        "dataset_digest": hex::encode(dataset_digest),
        "omissions": omissions,
    }))
}

pub(super) fn add_report(document: &mut JsonValue) -> Result<(), ExportFailure> {
    let keywords = JsonLdKeywords::new(document);
    let graph = keywords
        .graph(document)
        .ok_or_else(|| ExportFailure::Permanent("RO-Crate @graph is missing".to_string()))?;
    if graph.iter().any(|entity| {
        entity
            .as_object()
            .and_then(|entity| keywords.object_id(entity))
            .is_some_and(|(_, id)| id == REPORT_PATH || id == "#aruna-export-report")
    }) {
        return Err(ExportFailure::Permanent(
            "RO-Crate uses a reserved export report identifier".to_string(),
        ));
    }
    let root_id = report_root_id(graph, &keywords).ok_or_else(|| {
        ExportFailure::Permanent("RO-Crate metadata descriptor has no root".to_string())
    })?;
    let graph = keywords
        .graph_mut(document)
        .ok_or_else(|| ExportFailure::Permanent("RO-Crate @graph is missing".to_string()))?;
    let root = graph
        .iter_mut()
        .find(|entity| {
            entity
                .as_object()
                .and_then(|entity| keywords.object_id(entity))
                .is_some_and(|(_, id)| id == root_id)
        })
        .and_then(JsonValue::as_object_mut)
        .ok_or_else(|| ExportFailure::Permanent("RO-Crate root Dataset is missing".to_string()))?;
    let subject_key = property_key(
        root,
        &keywords,
        &[
            "subjectOf",
            "schema:subjectOf",
            SCHEMA_SUBJECT_IRI,
            SCHEMA_SUBJECT_HTTPS_IRI,
        ],
        "subjectOf",
        SCHEMA_SUBJECT_HTTPS_IRI,
    );
    match root.get_mut(&subject_key) {
        Some(JsonValue::Array(values)) => values.push(json!({"@id": "#aruna-export-report"})),
        Some(value) => {
            let previous = std::mem::take(value);
            *value = json!([previous, {"@id": "#aruna-export-report"}]);
        }
        None => {
            root.insert(subject_key, json!({"@id": "#aruna-export-report"}));
        }
    }
    let part_key = property_key(
        root,
        &keywords,
        &[
            "hasPart",
            "schema:hasPart",
            SCHEMA_HAS_PART_IRI,
            SCHEMA_HAS_PART_HTTPS_IRI,
        ],
        "hasPart",
        SCHEMA_HAS_PART_HTTPS_IRI,
    );
    match root.get_mut(&part_key) {
        Some(JsonValue::Array(values)) => values.push(json!({"@id": REPORT_PATH})),
        Some(value) => {
            let previous = std::mem::take(value);
            *value = json!([previous, {"@id": REPORT_PATH}]);
        }
        None => {
            root.insert(part_key, json!({"@id": REPORT_PATH}));
        }
    }
    let encoding_key = safe_term(
        &keywords,
        "encodingFormat",
        &[
            SCHEMA_ENCODING_IRI,
            SCHEMA_ENCODING_HTTPS_IRI,
            "schema:encodingFormat",
        ],
        SCHEMA_ENCODING_HTTPS_IRI,
    );
    let about_key = safe_term(
        &keywords,
        "about",
        &[SCHEMA_ABOUT_IRI, SCHEMA_ABOUT_HTTPS_IRI, "schema:about"],
        SCHEMA_ABOUT_HTTPS_IRI,
    );
    let name_key = safe_term(
        &keywords,
        "name",
        &[SCHEMA_NAME_IRI, SCHEMA_NAME_HTTPS_IRI, "schema:name"],
        SCHEMA_NAME_HTTPS_IRI,
    );
    let file_type = if keywords.term_matches(
        "File",
        &[
            SCHEMA_MEDIA_IRI,
            SCHEMA_MEDIA_HTTPS_IRI,
            "schema:MediaObject",
        ],
    ) {
        "File"
    } else {
        SCHEMA_MEDIA_HTTPS_IRI
    };
    graph.push(JsonValue::Object(serde_json::Map::from_iter([
        ("@id".to_string(), json!(REPORT_PATH)),
        ("@type".to_string(), json!(file_type)),
        (encoding_key, json!("application/json")),
        (about_key.clone(), json!({"@id": "#aruna-export-report"})),
    ])));
    graph.push(JsonValue::Object(serde_json::Map::from_iter([
        ("@id".to_string(), json!("#aruna-export-report")),
        ("@type".to_string(), json!("http://schema.org/CreativeWork")),
        (name_key, json!("Aruna RO-Crate export completeness report")),
        (about_key, json!({"@id": root_id})),
    ])));
    Ok(())
}

pub(super) fn report_root_id(graph: &[JsonValue], keywords: &JsonLdKeywords) -> Option<String> {
    graph.iter().find_map(|entity| {
        let entity = entity.as_object()?;
        let (_, id) = keywords.object_id(entity)?;
        if id.trim_start_matches("./") != METADATA_PATH {
            return None;
        }
        entity.iter().find_map(|(key, value)| {
            keywords
                .expands_to(
                    key,
                    &[
                        "about",
                        "schema:about",
                        SCHEMA_ABOUT_IRI,
                        SCHEMA_ABOUT_HTTPS_IRI,
                    ],
                )
                .then(|| reference_id(value, keywords).map(str::to_string))
                .flatten()
        })
    })
}

pub(super) fn reference_id<'a>(value: &'a JsonValue, keywords: &JsonLdKeywords) -> Option<&'a str> {
    match value {
        JsonValue::String(value) => Some(value),
        JsonValue::Object(value) => keywords.object_id(value).map(|(_, id)| id),
        JsonValue::Array(values) => values
            .iter()
            .find_map(|value| reference_id(value, keywords)),
        _ => None,
    }
}

pub(super) fn property_key(
    object: &serde_json::Map<String, JsonValue>,
    keywords: &JsonLdKeywords,
    values: &[&str],
    compact: &str,
    absolute: &str,
) -> String {
    object
        .keys()
        .find(|key| keywords.expands_to(key, values))
        .cloned()
        .unwrap_or_else(|| safe_term(keywords, compact, values, absolute))
}

pub(super) fn safe_term(
    keywords: &JsonLdKeywords,
    compact: &str,
    values: &[&str],
    absolute: &str,
) -> String {
    if keywords.term_matches(compact, values) {
        compact.to_string()
    } else {
        absolute.to_string()
    }
}

pub(super) fn precheck_size(
    spec: &ExportRoCrateSpec,
    metadata: &[u8],
    report: Option<&[u8]>,
    opened: &[ProbedEntry],
    entities: &[ExportEntity],
) -> Result<(), ExportFailure> {
    let mut size = (metadata.len() as u64)
        .checked_add(256 + 2 * METADATA_PATH.len() as u64)
        .ok_or_else(|| ExportFailure::Permanent("export size overflow".to_string()))?;
    let mut entries = 1u64;
    if let Some(report) = report {
        size = size
            .checked_add(report.len() as u64)
            .and_then(|size| size.checked_add(256 + 2 * REPORT_PATH.len() as u64))
            .ok_or_else(|| ExportFailure::Permanent("export size overflow".to_string()))?;
        entries += 1;
    }
    for entry in opened {
        let path = entities
            .get(entry.entity_index)
            .and_then(|entity| entity.zip_path.as_deref())
            .ok_or_else(|| ExportFailure::Permanent("planned ZIP path is missing".to_string()))?;
        size = size
            .checked_add(entry.size)
            .and_then(|size| size.checked_add(256 + 2 * path.len() as u64))
            .ok_or_else(|| ExportFailure::Permanent("export size overflow".to_string()))?;
        entries += 1;
    }
    size = size
        .checked_add(256)
        .ok_or_else(|| ExportFailure::Permanent("export size overflow".to_string()))?;
    if entries > spec.limits.max_entries.saturating_add(2) {
        return Err(ExportFailure::Permanent(format!(
            "export has more than {} payload entries",
            spec.limits.max_entries
        )));
    }
    if size > spec.limits.export_artifact_bytes {
        return Err(ExportFailure::Permanent(format!(
            "planned ZIP exceeds the {} byte artifact limit",
            spec.limits.export_artifact_bytes
        )));
    }
    Ok(())
}

pub(super) async fn assemble_export(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    checkpoint: &ExportCheckpoint,
    opened: Vec<ProbedEntry>,
    policies: &BTreeMap<GroupId, std::sync::Arc<PolicyEvaluator>>,
) -> Result<ArtifactRef, ExportFailure> {
    let metadata = checkpoint
        .rewritten_jsonld
        .clone()
        .ok_or_else(|| ExportFailure::Permanent("rewritten metadata is missing".to_string()))?;
    let mut entries = Vec::with_capacity(opened.len());
    let source_spec = std::sync::Arc::new(spec.clone());
    let job_ms = unix_timestamp_millis();
    for entry in opened {
        let entity = &checkpoint.entities[entry.entity_index];
        let path = entity
            .zip_path
            .clone()
            .ok_or_else(|| ExportFailure::Permanent("planned ZIP path is missing".to_string()))?;
        let mut candidate = entity
            .candidates
            .get(entry.candidate_index)
            .cloned()
            .ok_or_else(|| {
                ExportFailure::Permanent("planned export candidate is missing".to_string())
            })?;
        candidate.expected_blake3 = Some(entry.hash);
        entries.push(PlannedEntry {
            entity_index: entry.entity_index,
            candidate_index: entry.candidate_index,
            path,
            source: PlannedSource::Candidate {
                driver: ctx.driver.clone(),
                spec: source_spec.clone(),
                candidate,
            },
            expected_blake3: entry.hash,
            modified_ms: entry
                .resolved_version
                .map_or(job_ms, |version| version.timestamp_ms()),
        });
    }
    let report = checkpoint.report_json.clone();
    let policies = std::sync::Arc::new(policies.clone());
    let Some(blob_handle) = ctx.driver.blob_handle.as_ref() else {
        return Err(ExportFailure::Retryable(
            "blob handle unavailable".to_string(),
        ));
    };
    let (writer, reader) = tokio::io::duplex(128 * 1024);
    let cancel = ctx.cancel.clone();
    let shutdown = ctx.shutdown.clone();
    let writer_task = tokio::spawn(Box::pin(write_archive_checked(
        writer, metadata, entries, report, policies, cancel, shutdown, job_ms,
    )));
    let event = blob_handle
        .send_blob_effect(BlobEffect::SpoolHidden {
            namespace: ctx.job_id.as_ulid(),
            name: "rocrate.zip".to_string(),
            created_by: spec.auth_context.user_id,
            max_bytes: Some(spec.limits.export_artifact_bytes),
            deadline: None,
            blob: BackendStream::new(tokio_util::io::ReaderStream::new(reader)),
        })
        .await;
    let write_result = match writer_task.await {
        Ok(result) => result,
        Err(error) => Err(ExportFailure::Retryable(error.to_string())),
    };
    match (event, write_result) {
        (
            Event::Blob(BlobEvent::HiddenSpooled {
                location,
                blake3,
                size,
            }),
            Ok(()),
        ) => Ok(ArtifactRef {
            location,
            blake3,
            size,
            expires_at_ms: unix_timestamp_millis()
                .saturating_add(spec.limits.artifact_retention_ms),
        }),
        (Event::Blob(BlobEvent::HiddenSpooled { location, .. }), Err(error)) => {
            let _ = delete_hidden(&ctx.driver, &location).await;
            Err(error)
        }
        (Event::Blob(BlobEvent::Error(BlobError::SizeLimitExceeded { limit })), _) => {
            Err(ExportFailure::Permanent(format!(
                "assembled ZIP exceeds the {limit} byte artifact limit"
            )))
        }
        (Event::Blob(BlobEvent::Error(_)), Err(ExportFailure::Cancelled)) => {
            Err(ExportFailure::Cancelled)
        }
        (Event::Blob(BlobEvent::Error(_)), Err(ExportFailure::Interrupted)) => {
            Err(ExportFailure::Interrupted)
        }
        (Event::Blob(BlobEvent::Error(error)), _) => {
            Err(ExportFailure::Retryable(error.to_string()))
        }
        (event, _) => Err(ExportFailure::Retryable(format!(
            "unexpected hidden artifact event: {event:?}"
        ))),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn write_archive_checked(
    writer: tokio::io::DuplexStream,
    metadata: Vec<u8>,
    mut entries: Vec<PlannedEntry>,
    report: Option<Vec<u8>>,
    policies: std::sync::Arc<BTreeMap<GroupId, std::sync::Arc<PolicyEvaluator>>>,
    cancel: tokio_util::sync::CancellationToken,
    shutdown: tokio_util::sync::CancellationToken,
    job_ms: u64,
) -> Result<(), ExportFailure> {
    entries.sort_by(|left, right| left.path.cmp(&right.path));
    let mut archive = async_zip::base::write::ZipFileWriter::with_tokio(writer);
    archive
        .write_entry_whole(zip_entry(METADATA_PATH, job_ms), &metadata)
        .await
        .map_err(|error| ExportFailure::Retryable(error.to_string()))?;
    for entry in entries {
        let PlannedEntry {
            entity_index,
            candidate_index,
            path,
            source,
            expected_blake3,
            modified_ms,
        } = entry;
        let opened = match source {
            PlannedSource::Candidate {
                driver,
                spec,
                candidate,
            } => {
                Box::pin(open_candidate_checked(
                    &driver, &spec, &policies, &candidate, false,
                ))
                .await?
            }
            #[cfg(test)]
            PlannedSource::Ready(blob) => CandidateOpen::Opened(BaoReadOutput::Stream {
                blob,
                size: 0,
                blake3: expected_blake3,
                etag: None,
                hashes: HashMap::new(),
            }),
        };
        let mut blob = match opened {
            CandidateOpen::Opened(BaoReadOutput::Stream { blob, blake3, .. })
                if blake3 == expected_blake3 =>
            {
                blob
            }
            CandidateOpen::Opened(BaoReadOutput::Stream { .. })
            | CandidateOpen::Status(OpenStatus::Corrupt) => {
                return Err(ExportFailure::Candidate {
                    entity_index,
                    candidate_index,
                    status: OpenStatus::Corrupt,
                    message: format!("payload integrity check failed for `{path}`"),
                });
            }
            CandidateOpen::Opened(BaoReadOutput::Metadata { .. }) => {
                return Err(ExportFailure::Retryable(
                    "source open unexpectedly returned metadata".to_string(),
                ));
            }
            CandidateOpen::Status(status) => {
                return Err(ExportFailure::Candidate {
                    entity_index,
                    candidate_index,
                    status,
                    message: format!("payload source became unavailable for `{path}`"),
                });
            }
        };
        let mut writer = archive
            .write_entry_stream(zip_entry(&path, modified_ms))
            .await
            .map_err(|error| ExportFailure::Retryable(error.to_string()))?;
        let mut hasher = blake3::Hasher::new();
        loop {
            let next = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(ExportFailure::Cancelled),
                _ = shutdown.cancelled() => return Err(ExportFailure::Interrupted),
                next = blob.next() => next,
            };
            match next {
                Some(Ok(bytes)) => {
                    hasher.update(&bytes);
                    writer
                        .write_all(&bytes)
                        .await
                        .map_err(|error| ExportFailure::Retryable(error.to_string()))?;
                }
                Some(Err(error)) => {
                    return Err(ExportFailure::Candidate {
                        entity_index,
                        candidate_index,
                        status: stream_status(&error),
                        message: error.to_string(),
                    });
                }
                None => break,
            }
        }
        if hasher.finalize().as_bytes() != &expected_blake3 {
            return Err(ExportFailure::Candidate {
                entity_index,
                candidate_index,
                status: OpenStatus::Corrupt,
                message: format!("payload integrity check failed for `{path}`"),
            });
        }
        writer
            .close()
            .await
            .map_err(|error| ExportFailure::Retryable(error.to_string()))?;
    }
    if let Some(report) = report {
        archive
            .write_entry_whole(zip_entry(REPORT_PATH, job_ms), &report)
            .await
            .map_err(|error| ExportFailure::Retryable(error.to_string()))?;
    }
    archive
        .close()
        .await
        .map_err(|error| ExportFailure::Retryable(error.to_string()))?;
    Ok(())
}

pub(super) fn zip_entry(path: &str, modified_ms: u64) -> ZipEntryBuilder {
    ZipEntryBuilder::new(path.to_string().into(), Compression::Stored)
        .last_modification_date(zip_date(modified_ms))
}

/// MS-DOS ZIP timestamps only cover 1980 through 2107, so a moment outside
/// that window keeps its month and day but clamps to the nearest year.
pub(super) fn zip_date(modified_ms: u64) -> ZipDateTime {
    let Some(moment) = i64::try_from(modified_ms)
        .ok()
        .and_then(DateTime::<Utc>::from_timestamp_millis)
    else {
        return ZipDateTime::default();
    };
    ZipDateTimeBuilder::new()
        .year(moment.year().clamp(1980, 2107))
        .month(moment.month())
        .day(moment.day())
        .hour(moment.hour())
        .minute(moment.minute())
        .second(moment.second())
        .build()
}

pub(super) fn stream_status(error: &StreamError) -> OpenStatus {
    if matches!(
        error.0.downcast_ref::<BlobError>(),
        Some(BlobError::IntegrityCheckFailed(_))
    ) {
        OpenStatus::Corrupt
    } else {
        OpenStatus::Offline
    }
}

pub(super) async fn publish_export(
    ctx: &JobContext,
    checkpoint: &ExportCheckpoint,
) -> JobRunOutcome {
    let Some(artifact) = checkpoint.artifact.clone() else {
        return permanent("export artifact is missing");
    };
    for row in &checkpoint.report {
        if ctx.cancel.is_cancelled() {
            return JobRunOutcome::Cancelled;
        }
        if ctx.shutdown.is_cancelled() {
            return JobRunOutcome::Interrupted;
        }
        if let Err(error) = put_job_entry(
            &ctx.driver.storage_handle,
            ctx.job_id,
            ctx.claim_token,
            row.entry_key.as_bytes(),
            row,
        )
        .await
        {
            return retryable(error.to_string());
        }
    }
    let (included, omitted) = report_counts(&checkpoint.report);
    JobRunOutcome::Succeeded(JobResultPayload::ExportRoCrate(ExportRoCrateResult {
        artifact: Some(artifact),
        included,
        omitted,
        report_digest: [0; 32],
    }))
}

pub(super) fn report_counts(rows: &[ExportReportRow]) -> (u64, ExportOmissionCounts) {
    let mut included = 0u64;
    let mut omitted = ExportOmissionCounts::default();
    for row in rows {
        match row.code {
            ReasonCode::Included => included = included.saturating_add(1),
            ReasonCode::External => omitted.external = omitted.external.saturating_add(1),
            ReasonCode::Denied => omitted.denied = omitted.denied.saturating_add(1),
            ReasonCode::Missing => omitted.missing = omitted.missing.saturating_add(1),
            ReasonCode::Offline => omitted.offline = omitted.offline.saturating_add(1),
            ReasonCode::Unsupported => omitted.unsupported = omitted.unsupported.saturating_add(1),
            _ => {}
        }
    }
    (included, omitted)
}

pub(super) async fn discard_artifact(
    ctx: &JobContext,
    checkpoint: &mut ExportCheckpoint,
    persist: bool,
) {
    if let Some(artifact) = checkpoint.artifact.as_ref() {
        let _ = delete_hidden(&ctx.driver, &artifact.location).await;
    }
    checkpoint.artifact = None;
    checkpoint.refs.hidden_locations.clear();
    if persist {
        let _ = persist_checkpoint(ctx, checkpoint).await;
    }
}

pub(super) async fn read_export_checkpoint(
    ctx: &JobContext,
    job_id: JobId,
) -> Result<Option<ExportCheckpoint>, String> {
    read_state(
        &ctx.driver.storage_handle,
        ROCRATE_JOB_STATE_KEYSPACE,
        ByteView::from(job_id.to_bytes().to_vec()),
        "export checkpoint",
    )
    .await
}

pub(super) async fn persist_checkpoint(
    ctx: &JobContext,
    checkpoint: &ExportCheckpoint,
) -> Result<(), String> {
    put_state(
        &ctx.driver.storage_handle,
        ctx.job_id,
        ctx.claim_token,
        ROCRATE_JOB_STATE_KEYSPACE,
        ByteView::from(ctx.job_id.to_bytes().to_vec()),
        checkpoint,
    )
    .await
    .map_err(|error| error.to_string())
}

pub(super) fn map_crate_error(error: craqle::RoCrateError) -> ExportFailure {
    match error {
        craqle::RoCrateError::Update(craqle::UpdateError::ValidationFailed(violations)) => {
            ExportFailure::Validation(
                violations
                    .into_iter()
                    .map(|violation| MetadataValidationViolation {
                        code: violation.code.to_string(),
                        message: violation.message,
                        pointer: violation.pointer,
                        entity_id: violation.entity_id,
                    })
                    .collect(),
            )
        }
        error => ExportFailure::Permanent(error.to_string()),
    }
}

pub(super) async fn finish_export(
    ctx: &JobContext,
    checkpoint: &mut ExportCheckpoint,
    error: ExportFailure,
) -> JobRunOutcome {
    discard_artifact(ctx, checkpoint, false).await;
    if let ExportFailure::Validation(violations) = &error
        && let Err(message) = write_validation_rows(ctx, violations).await
    {
        return retryable(message);
    }
    failure_outcome(error)
}

pub(super) async fn write_validation_rows(
    ctx: &JobContext,
    violations: &[MetadataValidationViolation],
) -> Result<(), String> {
    for (index, violation) in violations.iter().enumerate() {
        let row = ExportReportRow {
            entry_key: format!("validation/{index:08}"),
            code: if violation.code == "unsupported_crate_version" {
                ReasonCode::UnsupportedCrateVersion
            } else {
                ReasonCode::Failed
            },
            message: Some(violation.message.clone()),
            detail: ExportReportDetail {
                entity_id: violation.entity_id.clone().unwrap_or_default(),
                zip_path: None,
                source: None,
                resolved_version: None,
                validation: Some(violation.clone()),
            },
        };
        put_job_entry(
            &ctx.driver.storage_handle,
            ctx.job_id,
            ctx.claim_token,
            row.entry_key.as_bytes(),
            &row,
        )
        .await
        .map_err(|error| error.to_string())?;
    }
    Ok(())
}

pub(super) fn failure_outcome(error: ExportFailure) -> JobRunOutcome {
    match error {
        ExportFailure::Permanent(message) => permanent(message),
        ExportFailure::Retryable(message) => retryable(message),
        ExportFailure::Validation(violations) => permanent(validation_message(&violations)),
        ExportFailure::Candidate { message, .. } => retryable(message),
        ExportFailure::Cancelled => JobRunOutcome::Cancelled,
        ExportFailure::Interrupted => JobRunOutcome::Interrupted,
    }
}

pub(super) fn validation_message(violations: &[MetadataValidationViolation]) -> String {
    violations
        .iter()
        .map(|violation| {
            format!(
                "{} at {}: {}",
                violation.code, violation.pointer, violation.message
            )
        })
        .collect::<Vec<_>>()
        .join("; ")
}

pub(super) fn retryable(message: impl Into<String>) -> JobRunOutcome {
    JobRunOutcome::Failed(JobError::retryable(message.into()))
}

pub(super) fn permanent(message: impl Into<String>) -> JobRunOutcome {
    JobRunOutcome::Failed(JobError::permanent(message.into()))
}

/// 2026-02-03T04:05:06Z: a fixed moment keeps fixture archives byte-identical.
#[cfg(test)]
pub(super) const FIXTURE_MOMENT_MS: u64 = 1_770_091_506_000;
