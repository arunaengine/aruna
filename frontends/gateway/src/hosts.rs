//! Caches host decisions for at most their `max_age_s`, and forgets them on node notices.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use lru::LruCache;

use crate::upstream::Answer;

/// The longest a decision stays cached, whatever the node allows.
const MAX_AGE: Duration = Duration::from_secs(30);
/// How long an unknown host stays unknown, so random host names cannot flood the node.
const MISSING_AGE: Duration = Duration::from_secs(5);

struct Entry {
    answer: Option<Arc<Answer>>,
    expires: Instant,
}

/// Decisions by normalized host; `None` remembers that no node serves the host.
pub struct HostCache {
    entries: Mutex<LruCache<String, Entry>>,
}

impl HostCache {
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self {
            entries: Mutex::new(LruCache::new(capacity)),
        }
    }

    /// The cached answer at `now`; the outer `None` means the node must be asked.
    pub fn get(&self, host: &str, now: Instant) -> Option<Option<Arc<Answer>>> {
        let mut entries = self.entries.lock().ok()?;
        match entries.get(host) {
            Some(entry) if entry.expires > now => Some(entry.answer.clone()),
            Some(_) => {
                entries.pop(host);
                None
            }
            None => None,
        }
    }

    pub fn put(&self, host: &str, answer: Option<Arc<Answer>>, now: Instant) {
        let age = match &answer {
            Some(answer) => MAX_AGE.min(Duration::from_secs(answer.decision.max_age_s.into())),
            None => MISSING_AGE,
        };
        if let Ok(mut entries) = self.entries.lock() {
            let expires = now + age;
            entries.put(host.to_string(), Entry { answer, expires });
        }
    }

    /// Drops every decision for `applet_id` and every named host.
    pub fn forget(&self, applet_id: &str, hosts: &[String]) {
        let Ok(mut entries) = self.entries.lock() else {
            return;
        };
        let stale: Vec<String> = (entries.iter())
            .filter(|(host, entry)| {
                hosts.contains(host)
                    || (entry.answer.as_ref()).is_some_and(|a| a.decision.applet_id == applet_id)
            })
            .map(|(host, _)| host.clone())
            .collect();
        for host in stale {
            entries.pop(&host);
        }
    }

    pub fn clear(&self) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.clear();
        }
    }
}

/// The host of a `Host` header in lowercase without its port, or `None` when it is no DNS name.
pub fn normalize(raw: &str) -> Option<String> {
    let host = match raw.rsplit_once(':') {
        Some((name, port)) if port.bytes().all(|b| b.is_ascii_digit()) => name,
        _ => raw,
    };
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    let valid = !host.is_empty()
        && host.len() <= 253
        && host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        });
    valid.then_some(host)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upstream::{Access, Decision, Serving};

    fn answer(applet_id: &str, max_age_s: u32) -> Arc<Answer> {
        Arc::new(Answer {
            decision: Decision {
                applet_id: applet_id.to_string(),
                custom_domain: false,
                serving: Serving::Live,
                version: Some(1),
                access: Access::Public,
                frame_ancestors: Vec::new(),
                guests: false,
                guests_in_frames: false,
                portal_origin: None,
                max_age_s,
            },
            node: 0,
        })
    }

    // The node's max age and the 30 second bound both end an entry; unknown hosts end sooner.
    #[test]
    fn entries_expire_bounded() {
        let cache = HostCache::new(NonZeroUsize::new(4).expect("non-zero"));
        let start = Instant::now();
        cache.put("short.apps.test", Some(answer("a", 10)), start);
        cache.put("long.apps.test", Some(answer("b", 3600)), start);
        cache.put("nobody.apps.test", None, start);
        let later = |seconds| start + Duration::from_secs(seconds);
        assert!(matches!(
            cache.get("short.apps.test", later(9)),
            Some(Some(_))
        ));
        assert!(cache.get("short.apps.test", later(10)).is_none());
        assert!(matches!(
            cache.get("long.apps.test", later(29)),
            Some(Some(_))
        ));
        assert!(cache.get("long.apps.test", later(30)).is_none());
        assert!(matches!(
            cache.get("nobody.apps.test", later(4)),
            Some(None)
        ));
        assert!(cache.get("nobody.apps.test", later(5)).is_none());
    }

    #[test]
    fn forget_drops_applet() {
        let cache = HostCache::new(NonZeroUsize::new(4).expect("non-zero"));
        let now = Instant::now();
        cache.put("one.apps.test", Some(answer("a", 30)), now);
        cache.put("one.example.org", Some(answer("a", 30)), now);
        cache.put("two.apps.test", Some(answer("b", 30)), now);
        cache.forget("a", &[]);
        assert!(cache.get("one.apps.test", now).is_none());
        assert!(cache.get("one.example.org", now).is_none());
        assert!(cache.get("two.apps.test", now).is_some());
        cache.forget("unknown", &["two.apps.test".to_string()]);
        assert!(cache.get("two.apps.test", now).is_none());
    }

    #[test]
    fn normalize_strips_port() {
        assert_eq!(
            normalize("Bakta.Apps.Test:443").as_deref(),
            Some("bakta.apps.test")
        );
        assert_eq!(
            normalize("bakta.apps.test.").as_deref(),
            Some("bakta.apps.test")
        );
        assert!(normalize("").is_none());
        assert!(normalize("bad host").is_none());
        assert!(normalize("a..b").is_none());
        assert!(normalize("evil/../x").is_none());
    }
}
