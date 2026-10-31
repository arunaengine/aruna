//! Runs staging source effects: check, head, read and list against a resolved source access.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::time::{Duration, Instant};

use super::BlobHandler;
use crate::opendal::{
    check_staging_source, head_staging_source, list_staging_source, read_staging_source,
};
use aruna_core::errors::StagingSourceError;
use aruna_core::events::StagingSourceEvent;
use aruna_core::structs::execution::source_access::{
    ListRequest, ResolvedSourceAccess, SourceMetadata,
};

/// How long a source that failed counts as failing unless it answers again.
const FAILING_FOR: Duration = Duration::from_secs(60);

/// When each source, named by a digest of its kind and settings, last failed.
#[derive(Debug, Default)]
pub(crate) struct SourceFailures {
    failed: HashMap<[u8; 32], Instant>,
}

impl SourceFailures {
    fn record(&mut self, access: &ResolvedSourceAccess, error: Option<&StagingSourceError>) {
        let now = Instant::now();
        self.failed
            .retain(|_, at| now.duration_since(*at) < FAILING_FOR);
        match error {
            None => {
                self.failed.remove(&source_key(access));
            }
            Some(error) if unavailable(error) => {
                self.failed.insert(source_key(access), now);
            }
            Some(_) => {}
        }
    }

    fn failing(&self, access: &ResolvedSourceAccess) -> bool {
        (self.failed.get(&source_key(access))).is_some_and(|at| at.elapsed() < FAILING_FOR)
    }
}

/// The source without its path: one server and its settings, never logged.
fn source_key(access: &ResolvedSourceAccess) -> [u8; 32] {
    let ResolvedSourceAccess::OpenDal { kind, config, .. } = access;
    let mut settings: Vec<_> = config.iter().collect();
    settings.sort();
    let mut hasher = blake3::Hasher::new();
    hasher.update(kind.to_string().as_bytes());
    for (key, value) in settings {
        for part in [key, value] {
            hasher.update(&(part.len() as u64).to_le_bytes());
            hasher.update(part.as_bytes());
        }
    }
    *hasher.finalize().as_bytes()
}

/// Errors of the server rather than of one file or of the caller's access.
fn unavailable(error: &StagingSourceError) -> bool {
    matches!(
        error,
        StagingSourceError::OperatorCreationFailed(_)
            | StagingSourceError::CheckError(_)
            | StagingSourceError::StatError(_)
            | StagingSourceError::ListError(_)
            | StagingSourceError::ReadError(_)
    )
}

impl BlobHandler {
    pub(crate) async fn check_staging_source(
        &self,
        access: ResolvedSourceAccess,
    ) -> StagingSourceEvent {
        match check_staging_source(&self.egress, &access).await {
            Ok(()) => StagingSourceEvent::CheckResult,
            Err(error) => StagingSourceEvent::Error { error },
        }
    }

    pub(crate) async fn head_staging_source(
        &self,
        access: ResolvedSourceAccess,
    ) -> StagingSourceEvent {
        let result = head_staging_source(&self.egress, &access).await;
        self.record_source(&access, result.as_ref().err());
        match result {
            Ok(metadata) => StagingSourceEvent::HeadResult { metadata },
            Err(error) => StagingSourceEvent::Error { error },
        }
    }

    pub(crate) async fn read_staging_source(
        &self,
        access: ResolvedSourceAccess,
        range: Option<std::ops::Range<u64>>,
        expected: Option<SourceMetadata>,
    ) -> StagingSourceEvent {
        let result = read_staging_source(&self.egress, &access, range, expected.as_ref()).await;
        self.record_source(&access, result.as_ref().err());
        match result {
            Ok((metadata, stream)) => StagingSourceEvent::ReadResult { metadata, stream },
            Err(error) => StagingSourceEvent::Error { error },
        }
    }

    /// Which of `accesses` failed lately without answering since.
    pub(crate) fn source_health(&self, accesses: &[ResolvedSourceAccess]) -> StagingSourceEvent {
        let failing = match self.source_failures.lock() {
            Ok(failures) => accesses.iter().map(|a| failures.failing(a)).collect(),
            Err(_) => vec![false; accesses.len()],
        };
        StagingSourceEvent::HealthResult { failing }
    }

    fn record_source(&self, access: &ResolvedSourceAccess, error: Option<&StagingSourceError>) {
        if let Ok(mut failures) = self.source_failures.lock() {
            failures.record(access, error);
        }
    }

    pub(crate) async fn list_staging_source(
        &self,
        access: ResolvedSourceAccess,
        request: ListRequest,
    ) -> StagingSourceEvent {
        match list_staging_source(&self.egress, &access, &request).await {
            Ok((entries, next_cursor)) => StagingSourceEvent::ListResult {
                entries,
                next_cursor,
            },
            Err(error) => StagingSourceEvent::Error { error },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::execution::source_connector::SourceConnectorKind;

    fn access(endpoint: &str, path: &str) -> ResolvedSourceAccess {
        ResolvedSourceAccess::OpenDal {
            kind: SourceConnectorKind::Http,
            config: [("endpoint".to_string(), endpoint.to_string())].into(),
            path: path.to_string(),
            version: None,
        }
    }

    #[test]
    fn failures_follow_servers() {
        // A server failure marks every path of that server; a missing file marks nothing.
        let mut failures = SourceFailures::default();
        let error = StagingSourceError::StatError("timed out".to_string());
        failures.record(&access("https://a", "x"), Some(&error));
        assert!(failures.failing(&access("https://a", "y")));
        assert!(!failures.failing(&access("https://b", "x")));
        failures.record(
            &access("https://b", "x"),
            Some(&StagingSourceError::NotFound),
        );
        assert!(!failures.failing(&access("https://b", "x")));
        failures.record(&access("https://a", "z"), None);
        assert!(!failures.failing(&access("https://a", "x")));
    }
}
