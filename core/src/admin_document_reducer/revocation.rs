use super::*;

pub(super) fn expiry_threshold(expires_at: u64) -> u64 {
    expires_at.saturating_add(REVOCATION_GRACE_SECS)
}

pub(super) fn candidate_cmp(left: &RevocationCandidate, right: &RevocationCandidate) -> Ordering {
    left.expires_at
        .cmp(&right.expires_at)
        .then_with(|| right.dot.cmp(&left.dot))
        .then_with(|| right.token_owner.cmp(&left.token_owner))
}

pub(super) fn value_matches(version: &AdminDocumentAttributeVersion, expires_at: u64) -> bool {
    version
        .value
        .as_deref()
        .and_then(|value| value.parse::<u64>().ok())
        == Some(expires_at)
}

fn add_paths<T>(
    paths: &BTreeMap<String, T>,
    indexed: &mut BTreeMap<String, Option<RevocationPath>>,
) {
    let prefix = format!("{REALM_CONFIG_REVOKED_TOKENS_PATH}.");
    for path in paths
        .range((Included(prefix.clone()), Unbounded))
        .map(|(path, _)| path)
    {
        if !path.starts_with(&prefix) {
            break;
        }
        indexed.entry(path.clone()).or_insert_with(|| {
            revoked_token_entry(path).map(|(hash, expires_at, token_owner)| RevocationPath {
                hash: hash.to_string(),
                expires_at,
                token_owner,
            })
        });
    }
}
impl RevocationIndex {
    pub(super) fn build(state: &AdminDocumentReducerState, now: u64) -> Self {
        if !matches!(&state.target, AdminDocumentTarget::RealmConfig { .. }) {
            return Self {
                now,
                groups: BTreeMap::new(),
                retained: BTreeMap::new(),
                live: BTreeMap::new(),
                origin_counts: BTreeMap::new(),
                owner_counts: BTreeMap::new(),
                next_expiry: None,
            };
        }

        let mut indexed = BTreeMap::new();
        add_paths(&state.user_subject_ids, &mut indexed);
        add_paths(&state.equivalent_value_dots, &mut indexed);
        add_paths(&state.conflicts, &mut indexed);

        let mut groups = BTreeMap::new();
        for (path, entry) in indexed {
            let Some(entry) = entry else {
                continue;
            };
            let group = groups
                .entry(entry.hash.clone())
                .or_insert_with(RevocationGroup::default);
            group.paths.insert(path.clone());

            if let Some(version) = state.user_subject_ids.get(&path) {
                group.event_ids.insert(version.dot.event_id);
                if value_matches(version, entry.expires_at) {
                    group.candidates.push(RevocationCandidate {
                        path: path.clone(),
                        expires_at: entry.expires_at,
                        token_owner: entry.token_owner,
                        dot: version.dot,
                    });
                }
            }
            if let Some(dots) = state.equivalent_value_dots.get(&path) {
                group.event_ids.extend(dots.iter().map(|dot| dot.event_id));
                group
                    .candidates
                    .extend(dots.iter().copied().map(|dot| RevocationCandidate {
                        path: path.clone(),
                        expires_at: entry.expires_at,
                        token_owner: entry.token_owner,
                        dot,
                    }));
            }
            if let Some(conflict) = state.conflicts.get(&path) {
                group
                    .event_ids
                    .extend(conflict.values.iter().map(|value| value.dot.event_id));
                group.candidates.extend(
                    conflict
                        .values
                        .iter()
                        .filter(|value| {
                            value.value.as_deref() == Some(entry.expires_at.to_string().as_str())
                        })
                        .map(|value| RevocationCandidate {
                            path: path.clone(),
                            expires_at: entry.expires_at,
                            token_owner: entry.token_owner,
                            dot: value.dot,
                        }),
                );
            }
        }

        let mut retained = BTreeMap::new();
        let mut live = BTreeMap::new();
        let mut origin_counts = BTreeMap::new();
        let mut owner_counts = BTreeMap::new();
        for (hash, group) in &groups {
            let Some(winner) = group
                .candidates
                .iter()
                .max_by(|left, right| candidate_cmp(left, right))
                .cloned()
            else {
                continue;
            };
            if revocation_retained(winner.expires_at, now) {
                *origin_counts.entry(winner.dot.origin_node_id).or_insert(0) += 1;
                *owner_counts
                    .entry((winner.dot.origin_node_id, winner.token_owner))
                    .or_insert(0) += 1;
                retained.insert(hash.clone(), winner.clone());
            }
            if revocation_live(winner.expires_at, now) {
                live.insert(hash.clone(), winner);
            }
        }
        let next_expiry = groups
            .values()
            .flat_map(|group| group.paths.iter())
            .filter_map(|path| revoked_token_entry(path))
            .map(|(_, expires_at, _)| expiry_threshold(expires_at))
            .min();

        Self {
            now,
            groups,
            retained,
            live,
            origin_counts,
            owner_counts,
            next_expiry,
        }
    }
}
