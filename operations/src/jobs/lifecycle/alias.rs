//! Reads the request family one accepted job alias belongs to.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::FAMILY_ALIAS_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::record_keys::{
    MAX_ALIAS_FAMILIES, alias_family, alias_prefix,
};
use aruna_core::structs::execution::job::{JobFamilyId, JobId};

use crate::jobs::route::JobRouteError;

/// The family of one alias, ordered by key, so two families claiming one alias
/// resolve identically on every replica. `None` names no family here.
#[derive(Debug, PartialEq)]
pub struct AliasFamilyOperation {
    job_id: JobId,
}

impl AliasFamilyOperation {
    pub fn new(job_id: JobId) -> Self {
        Self { job_id }
    }
}

impl Operation for AliasFamilyOperation {
    type Output = Option<JobFamilyId>;
    type Error = JobRouteError;

    fn first(&mut self) -> Result<Next<Self::Output>, JobRouteError> {
        Ok(Next::Call(IoCall::Page {
            space: FAMILY_ALIAS_KEYSPACE.to_string(),
            prefix: alias_prefix(self.job_id),
            after: None,
            limit: MAX_ALIAS_FAMILIES,
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, JobRouteError> {
        let IoResult::Page { rows, .. } = result else {
            return Err(JobRouteError::Unavailable(
                "unexpected answer to the alias page".to_string(),
            ));
        };
        Ok(Next::Done(
            rows.iter().filter_map(|(key, _)| alias_family(key)).min(),
        ))
    }
}

#[cfg(test)]
mod state_machine_tests {
    use aruna_core::calls::{IoCall, IoError, IoResult, Next};
    use aruna_core::errors::StorageError;
    use aruna_core::keyspaces::FAMILY_ALIAS_KEYSPACE;
    use aruna_core::operation::Operation;
    use aruna_core::structs::execution::job::record_keys::{
        MAX_ALIAS_FAMILIES, alias_key, alias_prefix,
    };
    use aruna_core::structs::execution::job::{JobFamilyId, JobId, SubmissionId};
    use aruna_core::types::{Key, Value};

    use super::AliasFamilyOperation;
    use crate::jobs::route::JobRouteError;

    fn job() -> JobId {
        JobId::from_bytes([4u8; 16])
    }

    fn family(seed: u8) -> JobFamilyId {
        JobFamilyId {
            submission_id: SubmissionId([seed; 32]),
            request_digest: [seed; 32],
        }
    }

    fn rows(families: &[JobFamilyId]) -> IoResult {
        IoResult::Page {
            rows: families
                .iter()
                .map(|family| (alias_key(job(), family), Value::from(&[0u8; 0][..])))
                .collect(),
            next: None,
        }
    }

    #[test]
    fn picks_lowest_family() {
        let mut alias = AliasFamilyOperation::new(job());
        assert_eq!(
            alias.first(),
            Ok(Next::Call(IoCall::Page {
                space: FAMILY_ALIAS_KEYSPACE.to_string(),
                prefix: alias_prefix(job()),
                after: None,
                limit: MAX_ALIAS_FAMILIES,
            }))
        );
        assert_eq!(
            alias.next(rows(&[family(9), family(2)])),
            Ok(Next::Done(Some(family(2))))
        );
    }

    #[test]
    fn names_no_family() {
        let mut alias = AliasFamilyOperation::new(job());
        assert_eq!(alias.next(rows(&[])), Ok(Next::Done(None)));
        // A row whose key names no family is not one.
        let stray = IoResult::Page {
            rows: vec![(Key::from(&[1u8][..]), Value::from(&[0u8; 0][..]))],
            next: None,
        };
        assert_eq!(alias.next(stray), Ok(Next::Done(None)));
    }

    #[test]
    fn reports_storage_error() {
        let error = StorageError::Timeout;
        assert_eq!(
            JobRouteError::from(IoError::Storage(error.clone())),
            JobRouteError::Unavailable(error.to_string())
        );
    }

    #[test]
    fn rejects_unfit_result() {
        let mut alias = AliasFamilyOperation::new(job());
        assert!(matches!(
            alias.next(IoResult::Done),
            Err(JobRouteError::Unavailable(_))
        ));
    }
}
