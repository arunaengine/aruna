//! Reads one stored S3 session by its access key.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::S3SessionError;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::S3_SESSION_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::s3_session::S3Session;

#[derive(Debug, PartialEq)]
pub struct GetS3Operation {
    access_key: String,
}

impl GetS3Operation {
    pub fn new(access_key: String) -> Self {
        Self { access_key }
    }
}

impl Operation for GetS3Operation {
    type Output = Option<S3Session>;
    type Error = S3SessionError;

    fn first(&mut self) -> Result<Next<Self::Output>, S3SessionError> {
        if !S3Session::valid_access_key(&self.access_key) {
            return Err(S3SessionError::InvalidAccessKey);
        }
        let key = self.access_key.as_bytes().into();
        Ok(Next::Call(IoCall::Read(vec![(
            S3_SESSION_KEYSPACE.to_string(),
            key,
        )])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, S3SessionError> {
        let Some(value) = result.value()? else {
            return Ok(Next::Done(None));
        };
        let session = S3Session::from_bytes(value.as_ref())?;
        if session.access_key != self.access_key {
            return Err(S3SessionError::IndexInconsistent);
        }
        Ok(Next::Done(Some(session)))
    }

    fn expected_error(error: &Self::Error) -> bool {
        error.expected()
    }
}

#[cfg(test)]
mod tests {
    use aruna_core::calls::IoError;

    use super::*;

    fn read(key: &str) -> Next<Option<S3Session>> {
        Next::Call(IoCall::Read(vec![(
            S3_SESSION_KEYSPACE.to_string(),
            key.as_bytes().into(),
        )]))
    }

    #[test]
    fn reads_session_row() {
        let prefix = aruna_core::structs::identity::s3_session::SESSION_ACCESS_PREFIX;
        let key = format!("{prefix}{}", ulid::Ulid::from_parts(1, 1));
        let mut operation = GetS3Operation::new(key.clone());
        assert_eq!(operation.first(), Ok(read(&key)));
        let missing = operation.next(IoResult::Values(vec![None]));
        assert_eq!(missing, Ok(Next::Done(None)));
    }

    #[test]
    fn refuses_bad_answers() {
        let mut invalid = GetS3Operation::new("bad key".to_string());
        assert_eq!(invalid.first(), Err(S3SessionError::InvalidAccessKey));
        let mut operation = GetS3Operation::new("unused".to_string());
        let corrupt = IoResult::Values(vec![Some(vec![0xff].into())]);
        assert!(matches!(
            operation.next(corrupt),
            Err(S3SessionError::Conversion(_))
        ));
        assert!(matches!(
            operation.next(IoResult::Done),
            Err(S3SessionError::Io(IoError::Unexpected { .. }))
        ));
    }
}
