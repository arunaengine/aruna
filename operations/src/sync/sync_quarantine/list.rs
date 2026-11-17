//! Reads one bounded page of quarantine rows together with the usage counters.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::rows::values;
use aruna_core::structs::SyncQuarantineRecord;

use super::{
    QuarantineAdminError, QuarantinePage, QuarantinePageRequest, decode_usage, page_call,
    page_rows, usage_read,
};

/// One page of quarantine evidence in key order, with the usage read after the page.
#[derive(Debug, PartialEq)]
pub struct ListQuarantineOperation {
    request: QuarantinePageRequest,
    /// The page read so far: its records and the key to continue after.
    page: Option<(Vec<SyncQuarantineRecord>, Option<Vec<u8>>)>,
}

impl ListQuarantineOperation {
    pub fn new(request: QuarantinePageRequest) -> Self {
        Self {
            request,
            page: None,
        }
    }
}

impl Operation for ListQuarantineOperation {
    type Output = QuarantinePage;
    type Error = QuarantineAdminError;

    fn first(&mut self) -> Result<Next<QuarantinePage>, QuarantineAdminError> {
        Ok(Next::Call(page_call(&self.request)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<QuarantinePage>, QuarantineAdminError> {
        let Some((records, next_start_after)) = self.page.take() else {
            let (rows, next) = page_rows(result)?;
            let records = rows
                .into_iter()
                .map(|(_, value)| SyncQuarantineRecord::from_bytes(value.as_ref()))
                .collect::<Result<Vec<_>, _>>()?;
            self.page = Some((records, next));
            return Ok(Next::Call(usage_read()));
        };
        let [value] = values::<1>(result)?;
        Ok(Next::Done(QuarantinePage {
            records,
            next_start_after,
            usage: decode_usage(value)?,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{record, record_bytes, usage_bytes};
    use super::*;
    use aruna_core::calls::{IoCall, IoError};
    use aruna_core::structs::SyncQuarantineUsage;

    fn request() -> QuarantinePageRequest {
        QuarantinePageRequest {
            start_after: Some(vec![1]),
            topic: Some(vec![2]),
            limit: Some(500),
        }
    }

    fn page(next: Option<Vec<u8>>) -> IoResult {
        IoResult::Page {
            rows: vec![(vec![7].into(), record_bytes(&record(0, false)))],
            next: next.map(Into::into),
        }
    }

    #[test]
    fn lists_page_usage() {
        let usage = SyncQuarantineUsage {
            records: 3,
            bytes: 90,
        };
        let mut operation = ListQuarantineOperation::new(request());
        let scan = IoCall::Page {
            space: aruna_core::keyspaces::SYNC_QUARANTINE_KEYSPACE.to_string(),
            prefix: vec![2].into(),
            after: Some(vec![1].into()),
            limit: super::super::QUARANTINE_PAGE_MAX,
        };
        assert_eq!(operation.first(), Ok(Next::Call(scan)));
        assert_eq!(
            operation.next(page(Some(vec![7]))),
            Ok(Next::Call(usage_read()))
        );
        let read = IoResult::Values(vec![Some(usage_bytes(usage))]);
        assert_eq!(
            operation.next(read),
            Ok(Next::Done(QuarantinePage {
                records: vec![record(0, false)],
                next_start_after: Some(vec![7]),
                usage,
            }))
        );
    }

    #[test]
    fn refuses_bad_results() {
        let corrupt = IoResult::Page {
            rows: vec![(vec![7].into(), vec![0xff].into())],
            next: None,
        };
        assert!(matches!(
            ListQuarantineOperation::new(request()).next(corrupt),
            Err(QuarantineAdminError::Conversion(_))
        ));
        let mut operation = ListQuarantineOperation::new(request());
        assert!(matches!(
            operation.next(IoResult::Done),
            Err(QuarantineAdminError::Io(IoError::Unexpected { .. }))
        ));
        operation.next(page(None)).unwrap();
        assert!(matches!(
            operation.next(page(None)),
            Err(QuarantineAdminError::Io(IoError::Unexpected { .. }))
        ));
    }
}
