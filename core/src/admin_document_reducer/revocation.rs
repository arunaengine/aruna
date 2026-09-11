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

impl RevocationIndex {

    pub fn origin(&self, token_hash: &str) -> Option<NodeId> {
        self.retained
            .get(token_hash)
            .map(|candidate| candidate.dot.origin_node_id)
    }

    pub fn owner(&self, token_hash: &str) -> Option<UserId> {
        self.retained
            .get(token_hash)
            .map(|candidate| candidate.token_owner)
    }

    pub fn count(&self, origin_node_id: &NodeId) -> usize {
        self.origin_counts
            .get(origin_node_id)
            .copied()
            .unwrap_or_default()
    }

    pub fn owner_count(&self, origin_node_id: &NodeId, token_owner: &UserId) -> usize {
        self.owner_counts
            .get(&(*origin_node_id, *token_owner))
            .copied()
            .unwrap_or_default()
    }

    pub fn materialized(&self) -> BTreeMap<String, u64> {
        self.live
            .iter()
            .map(|(hash, candidate)| (hash.clone(), candidate.expires_at))
            .collect()
    }

    pub(crate) fn watermark(&self) -> u64 {
        self.now
    }

    pub(super) fn next_expiry(&self) -> Option<u64> {
        self.next_expiry
    }

    fn refresh_expiry(&mut self) {
        self.next_expiry = self
            .groups
            .values()
            .flat_map(|group| group.paths.iter())
            .filter_map(|path| revoked_token_entry(path))
            .map(|(_, expires_at, _)| expiry_threshold(expires_at))
            .min();
    }

    fn clear_hash(&mut self, hash: &str) {
        if let Some(candidate) = self.retained.remove(hash) {
            let origin = candidate.dot.origin_node_id;
            let owner = (origin, candidate.token_owner);
            if let Some(count) = self.origin_counts.get_mut(&origin) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    self.origin_counts.remove(&origin);
                }
            }
            if let Some(count) = self.owner_counts.get_mut(&owner) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    self.owner_counts.remove(&owner);
                }
            }
        }
        self.live.remove(hash);
    }

    fn set_hash(&mut self, hash: &str, winner: RevocationCandidate) {
        self.clear_hash(hash);
        if revocation_retained(winner.expires_at, self.now) {
            *self
                .origin_counts
                .entry(winner.dot.origin_node_id)
                .or_insert(0) += 1;
            *self
                .owner_counts
                .entry((winner.dot.origin_node_id, winner.token_owner))
                .or_insert(0) += 1;
            self.retained.insert(hash.to_string(), winner.clone());
        }
        if revocation_live(winner.expires_at, self.now) {
            self.live.insert(hash.to_string(), winner);
        }
    }

    fn canonical_group(winner: &RevocationCandidate) -> RevocationGroup {
        RevocationGroup {
            paths: BTreeSet::from([winner.path.clone()]),
            candidates: vec![winner.clone()],
            event_ids: BTreeSet::from([winner.dot.event_id]),
        }
    }

    pub(super) fn apply(
        &mut self,
        state: &mut AdminDocumentReducerState,
        event: &AdminDocumentEvent,
        token_hash: &str,
        expires_at: u64,
        token_owner: UserId,
    ) -> AdminDocumentApplyStatus {
        self.clear_hash(token_hash);
        let group = self.groups.remove(token_hash).unwrap_or_default();
        let winner = state.canonicalize_group(
            token_hash,
            group,
            Some((expires_at, token_owner, event.dot())),
        );
        let status = winner
            .as_ref()
            .filter(|winner| winner.dot == event.dot())
            .map_or(AdminDocumentApplyStatus::Redundant, |_| {
                AdminDocumentApplyStatus::Applied
            });
        if let Some(winner) = winner {
            self.groups
                .insert(token_hash.to_string(), Self::canonical_group(&winner));
            self.set_hash(token_hash, winner);
        }
        self.refresh_expiry();
        state.revocation_next_expiry = self.next_expiry();
        state.clock.advance(event.origin_node_id, event.origin_seq);
        if status != AdminDocumentApplyStatus::Redundant {
            state.applied_event_ids.insert(event.event_id);
        }
        status
    }

    pub fn compact(&mut self, state: &mut AdminDocumentReducerState) {
        let groups = std::mem::take(&mut self.groups);
        let retained = std::mem::take(&mut self.retained);
        self.live.clear();
        self.origin_counts.clear();
        self.owner_counts.clear();
        state.revocation_floor = state.revocation_floor.max(self.now);
        for (hash, group) in groups {
            let Some(winner) = retained.get(&hash) else {
                state.remove_revocation_group(&group);
                continue;
            };
            let unchanged = group.paths.len() == 1
                && group.paths.contains(&winner.path)
                && state
                    .user_subject_ids
                    .get(&winner.path)
                    .is_some_and(|version| {
                        value_matches(version, winner.expires_at) && version.dot == winner.dot
                    })
                && !state.equivalent_value_dots.contains_key(&winner.path)
                && !state.conflicts.contains_key(&winner.path);
            if !unchanged {
                state.remove_revocation_group(&group);
                state.user_subject_ids.insert(
                    winner.path.clone(),
                    AdminDocumentAttributeVersion {
                        value: Some(winner.expires_at.to_string()),
                        dot: winner.dot,
                    },
                );
            }
            state.applied_event_ids.insert(winner.dot.event_id);
            self.groups
                .insert(hash.clone(), Self::canonical_group(winner));
            self.set_hash(&hash, winner.clone());
        }
        self.refresh_expiry();
        state.revocation_next_expiry = self.next_expiry();
    }
}
