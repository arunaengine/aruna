//! Tests central provider writes and daily usage charges as calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::credential_encryption::EncryptedS3Secret;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::{
    AssistantProvider, AssistantProviderKind, AssistantProviderStatus, SESSION_HEADER, USER_HEADER,
    utc_day,
};
use ulid::Ulid;

fn user() -> UserId {
    UserId::local(Ulid::from_bytes([7; 16]), RealmId::from_bytes([3; 32]))
}

fn central(label: &str) -> CentralAssistant {
    CentralAssistant {
        provider: AssistantProvider {
            provider_id: CENTRAL_PROVIDER_ID.to_string(),
            user_id: user(),
            kind: AssistantProviderKind::OpenaiCompatible,
            label: label.to_string(),
            base_url: "https://llm.example.org".to_string(),
            headers: EncryptedS3Secret::empty(),
            secret: EncryptedS3Secret::empty(),
            models: vec!["deepseek-v4.1".to_string()],
            default_model: None,
            created_at: 1,
            status: AssistantProviderStatus::Ready,
            token_obtained_at: None,
            login_expires_at: None,
            login_interval_seconds: None,
        },
        daily_limit: Some(2),
        user_header: USER_HEADER.to_string(),
        session_header: SESSION_HEADER.to_string(),
        changed_at: 1,
    }
}

fn value(bytes: Option<Vec<u8>>) -> IoResult {
    IoResult::Values(vec![bytes.map(Into::into)])
}

fn usage_value(usage: Option<AssistantUsage>) -> IoResult {
    value(usage.map(|usage| usage.to_bytes().unwrap()))
}

#[test]
fn sets_when_unchanged() {
    let mut operation = SetCentralOperation::new(Some(central("Realm")), None);
    assert!(operation.transaction());
    assert_eq!(
        operation.first().unwrap(),
        Next::Call(IoCall::Read(vec![central_row()]))
    );
    let next = operation.next(value(None)).unwrap();
    assert!(matches!(next, Next::Call(IoCall::Write(rows)) if rows.len() == 1));
    assert_eq!(operation.next(IoResult::Done).unwrap(), Next::Done(()));
}

#[test]
fn concurrent_set_conflicts() {
    let mut operation = SetCentralOperation::new(Some(central("Mine")), None);
    operation.first().unwrap();
    let other = central("Theirs").to_bytes().unwrap();
    assert_eq!(
        operation.next(value(Some(other))),
        Err(CentralStoreError::Stale)
    );
}

#[test]
fn removes_provider() {
    let stored = central("Realm");
    let mut operation = SetCentralOperation::new(None, Some(stored.clone()));
    operation.first().unwrap();
    let next = operation.next(value(Some(stored.to_bytes().unwrap())));
    assert_eq!(next, Ok(Next::Call(IoCall::Delete(vec![central_row()]))));
}

#[test]
fn wrong_result_fails() {
    let mut operation = SetCentralOperation::new(Some(central("Realm")), None);
    operation.first().unwrap();
    assert!(matches!(
        operation.next(IoResult::Done),
        Err(CentralStoreError::Storage(StorageError::ReadError(_)))
    ));
}

#[test]
fn charge_writes_count() {
    let today = utc_day(86_400 * 3 + 5);
    let mut operation = ChargeUsageOperation::new(user(), today, Some(2));
    assert!(operation.transaction());
    operation.first().unwrap();
    let next = operation.next(usage_value(Some(AssistantUsage {
        day: today,
        count: 1,
    })));
    let Ok(Next::Call(IoCall::Write(rows))) = next else {
        panic!("expected one usage write, got {next:?}");
    };
    let written = AssistantUsage::from_bytes(rows[0].2.as_ref()).unwrap();
    assert_eq!(
        written,
        AssistantUsage {
            day: today,
            count: 2
        }
    );
    assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(written)));
}

#[test]
fn refused_charge_fails() {
    let mut operation = ChargeUsageOperation::new(user(), 9, Some(2));
    operation.first().unwrap();
    let next = operation.next(usage_value(Some(AssistantUsage { day: 9, count: 2 })));
    assert_eq!(
        next,
        Err(CentralStoreError::QuotaReached(QuotaReached {
            used: 2,
            limit: 2
        }))
    );
}

#[test]
fn charge_resets_daily() {
    // The last second of day 9 is refused; the first second of day 10 counts from zero.
    let last = 86_400 * 10 - 1;
    let full = Some(AssistantUsage { day: 9, count: 2 });
    let mut operation = ChargeUsageOperation::new(user(), utc_day(last), Some(2));
    operation.first().unwrap();
    assert!(operation.next(usage_value(full)).is_err());
    let mut operation = ChargeUsageOperation::new(user(), utc_day(last + 1), Some(2));
    operation.first().unwrap();
    let next = operation.next(usage_value(full));
    assert!(matches!(next, Ok(Next::Call(IoCall::Write(_)))));
}

#[test]
fn reads_central_usage() {
    let mut operation = GetCentralOperation::new(user());
    assert_eq!(
        operation.first().unwrap(),
        Next::Call(IoCall::Read(vec![central_row(), usage_row(user())]))
    );
    let usage = AssistantUsage { day: 4, count: 1 };
    let next = operation.next(IoResult::Values(vec![
        Some(central("Realm").to_bytes().unwrap().into()),
        Some(usage.to_bytes().unwrap().into()),
    ]));
    let Ok(Next::Done((stored, read_usage))) = next else {
        panic!("expected the central read, got {next:?}");
    };
    assert_eq!(stored.unwrap().provider.label, "Realm");
    assert_eq!(read_usage, Some(usage));
}
