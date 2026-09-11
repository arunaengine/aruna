use super::*;

impl MetadataVisibilityCache {
    pub(super) fn new() -> Self {
        Self {
            registry: Mutex::new(None),
            registry_fill: Arc::new(tokio::sync::Mutex::new(())),
            lifecycle_deleted: Mutex::new(HashMap::new()),
            generation: AtomicU64::new(0),
        }
    }

    pub(super) fn current_generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub(super) fn advance_generation(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
    }

    #[cfg(test)]
    pub(super) fn registry_records(&self) -> Option<Arc<Vec<MetadataRegistryRecord>>> {
        match self.registry_records_any() {
            Some((records, true)) => Some(records),
            _ => None,
        }
    }

    // Expired entries are kept so readers can be served stale data while a
    // background refill replaces the entry; the bool flags freshness.
    pub(super) fn registry_records_any(&self) -> Option<(Arc<Vec<MetadataRegistryRecord>>, bool)> {
        let now = Instant::now();
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        registry.as_mut().and_then(|entry| {
            entry
                .snapshot()
                .map(|records| (records, entry.expires_at > now))
        })
    }

    pub(super) fn registry_records_for_group_any(
        &self,
        group_id: GroupId,
    ) -> Option<(Arc<Vec<MetadataRegistryRecord>>, bool)> {
        let now = Instant::now();
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        registry.as_mut().and_then(|entry| {
            entry
                .group_snapshot(group_id)
                .map(|records| (records, entry.expires_at > now))
        })
    }

    #[cfg(test)]
    pub(super) fn store_registry_records(&self, records: Arc<Vec<MetadataRegistryRecord>>) {
        let map: BTreeMap<_, _> = records
            .iter()
            .map(|record| (record.document_id, record.clone()))
            .collect();
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        *registry = Some(RegistryCacheEntry {
            records: map,
            snapshot: Some(records),
            group_snapshots: HashMap::new(),
            expires_at: Instant::now() + METADATA_VISIBILITY_CACHE_TTL,
        });
    }

    pub(super) fn store_visibility_fill(
        &self,
        records: Arc<Vec<MetadataRegistryRecord>>,
        lifecycle_entries: Vec<(String, bool)>,
        fill_generation: u64,
    ) -> bool {
        if records.len() > METADATA_REGISTRY_CANDIDATE_LIMIT
            || lifecycle_entries.len() > METADATA_REGISTRY_CANDIDATE_LIMIT
        {
            return false;
        }
        if self.current_generation() != fill_generation {
            return false;
        }
        let map: BTreeMap<_, _> = records
            .iter()
            .map(|record| (record.document_id, record.clone()))
            .collect();
        let now = Instant::now();
        let expires_at = now + METADATA_VISIBILITY_CACHE_TTL;
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        let mut lifecycle = self
            .lifecycle_deleted
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        if self.current_generation() != fill_generation {
            return false;
        }
        let protected = lifecycle_entries
            .iter()
            .map(|(graph_iri, _)| graph_iri.clone())
            .collect::<HashSet<_>>();
        Self::trim_lifecycle_deleted(&mut lifecycle, &protected, now);
        for (graph_iri, deleted) in lifecycle_entries {
            lifecycle.insert(
                graph_iri,
                LifecycleDeletedCacheEntry {
                    deleted,
                    expires_at,
                },
            );
        }
        lifecycle.retain(|_, entry| entry.expires_at > now);
        *registry = Some(RegistryCacheEntry {
            records: map,
            snapshot: Some(records),
            group_snapshots: HashMap::new(),
            expires_at,
        });
        true
    }
    #[cfg(test)]
    pub(super) fn lifecycle_deleted(&self, graph_iri: &str) -> Option<bool> {
        match self.lifecycle_deleted_any(graph_iri) {
            Some((deleted, true)) => Some(deleted),
            _ => None,
        }
    }

    pub(super) fn lifecycle_deleted_any(&self, graph_iri: &str) -> Option<(bool, bool)> {
        let now = Instant::now();
        let lifecycle = self
            .lifecycle_deleted
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        lifecycle
            .get(graph_iri)
            .map(|entry| (entry.deleted, entry.expires_at > now))
    }

    pub(super) fn store_lifecycle_deleted(&self, graph_iri: String, deleted: bool) {
        let mut lifecycle = self
            .lifecycle_deleted
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        self.advance_generation();
        let now = Instant::now();
        let protected = HashSet::from([graph_iri.clone()]);
        Self::trim_lifecycle_deleted(&mut lifecycle, &protected, now);
        lifecycle.insert(
            graph_iri,
            LifecycleDeletedCacheEntry {
                deleted,
                expires_at: now + METADATA_VISIBILITY_CACHE_TTL,
            },
        );
    }

    // Bulk refresh after a registry fill: re-stamps every supplied graph and
    // drops expired leftovers (graphs no longer in the registry) so the map
    // stays bounded.
    #[cfg(test)]
    pub(super) fn refresh_lifecycle_deleted(
        &self,
        entries: impl IntoIterator<Item = (String, bool)>,
    ) {
        let entries = entries.into_iter().collect::<Vec<_>>();
        if entries.len() > METADATA_REGISTRY_CANDIDATE_LIMIT {
            return;
        }
        let now = Instant::now();
        let expires_at = now + METADATA_VISIBILITY_CACHE_TTL;
        let mut lifecycle = self
            .lifecycle_deleted
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        let protected = entries
            .iter()
            .map(|(graph_iri, _)| graph_iri.clone())
            .collect::<HashSet<_>>();
        Self::trim_lifecycle_deleted(&mut lifecycle, &protected, now);
        for (graph_iri, deleted) in entries {
            lifecycle.insert(
                graph_iri,
                LifecycleDeletedCacheEntry {
                    deleted,
                    expires_at,
                },
            );
        }
        lifecycle.retain(|_, entry| entry.expires_at > now);
    }

    pub(super) fn trim_lifecycle_deleted(
        lifecycle: &mut HashMap<String, LifecycleDeletedCacheEntry>,
        protected: &HashSet<String>,
        now: Instant,
    ) {
        lifecycle.retain(|_, entry| entry.expires_at > now);
        let protected_count = lifecycle
            .keys()
            .filter(|graph_iri| protected.contains(*graph_iri))
            .count();
        let available = METADATA_REGISTRY_CANDIDATE_LIMIT.saturating_sub(protected.len());
        let remove_count = lifecycle
            .len()
            .saturating_sub(protected_count)
            .saturating_sub(available);
        let evicted = lifecycle
            .keys()
            .filter(|graph_iri| !protected.contains(*graph_iri))
            .take(remove_count)
            .cloned()
            .collect::<Vec<_>>();
        for graph_iri in evicted {
            lifecycle.remove(&graph_iri);
        }
    }

    pub(super) fn refresh_lifecycle_deleted_if_current(
        &self,
        entries: impl IntoIterator<Item = (String, bool)>,
        fill_generation: u64,
    ) -> bool {
        if self.current_generation() != fill_generation {
            return false;
        }
        let entries = entries.into_iter().collect::<Vec<_>>();
        if entries.len() > METADATA_REGISTRY_CANDIDATE_LIMIT {
            return false;
        }
        let now = Instant::now();
        let expires_at = now + METADATA_VISIBILITY_CACHE_TTL;
        let mut lifecycle = self
            .lifecycle_deleted
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        if self.current_generation() != fill_generation {
            return false;
        }
        let protected = entries
            .iter()
            .map(|(graph_iri, _)| graph_iri.clone())
            .collect::<HashSet<_>>();
        Self::trim_lifecycle_deleted(&mut lifecycle, &protected, now);
        for (graph_iri, deleted) in entries {
            lifecycle.insert(
                graph_iri,
                LifecycleDeletedCacheEntry {
                    deleted,
                    expires_at,
                },
            );
        }
        lifecycle.retain(|_, entry| entry.expires_at > now);
        true
    }
}
