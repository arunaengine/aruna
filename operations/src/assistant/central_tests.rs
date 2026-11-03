//! Tests central provider writes and daily usage charges without I/O.
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

fn txn() -> TxnId {
    Ulid::from_bytes([6; 16])
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

fn started() -> Event {
    Event::Storage(StorageEvent::TransactionStarted { txn_id: txn() })
}

fn read(value: Option<Vec<u8>>) -> Event {
    Event::Storage(StorageEvent::ReadResult {
        key: central_key(),
        value: value.map(Into::into),
    })
}

fn committed() -> Event {
    Event::Storage(StorageEvent::TransactionCommitted { txn_id: txn() })
}

fn aborts(effects: &Effects) -> bool {
    matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction { .. })]
    )
}

#[test]
fn sets_when_unchanged() {
    let mut operation = SetCentralOperation::new(Some(central("Realm")), None);
    operation.start();
    operation.step(started());
    let effects = operation.step(read(None));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Write { .. })]
    ));
    let key = central_key();
    operation.step(Event::Storage(StorageEvent::WriteResult { key }));
    operation.step(committed());
    assert!(operation.is_complete());
    operation.finalize().unwrap();
}

#[test]
fn concurrent_set_conflicts() {
    let mut operation = SetCentralOperation::new(Some(central("Mine")), None);
    operation.start();
    operation.step(started());
    let other = central("Theirs").to_bytes().unwrap();
    let effects = operation.step(read(Some(other)));
    assert!(aborts(&effects));
    assert!(matches!(
        operation.finalize(),
        Err(CentralStoreError::Stale)
    ));
}

#[test]
fn removes_provider() {
    let stored = central("Realm");
    let mut operation = SetCentralOperation::new(None, Some(stored.clone()));
    operation.start();
    operation.step(started());
    let effects = operation.step(read(Some(stored.to_bytes().unwrap())));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Delete { .. })]
    ));
}

#[test]
fn wrong_event_fails() {
    let mut operation = SetCentralOperation::new(Some(central("Realm")), None);
    operation.start();
    operation.step(started());
    let key = central_key();
    let effects = operation.step(Event::Storage(StorageEvent::WriteResult { key }));
    assert!(aborts(&effects));
    assert!(matches!(
        operation.finalize(),
        Err(CentralStoreError::UnexpectedEvent { .. })
    ));
}

fn usage_read(usage: Option<AssistantUsage>) -> Event {
    Event::Storage(StorageEvent::ReadResult {
        key: usage_key(user()),
        value: usage.map(|usage| usage.to_bytes().unwrap().into()),
    })
}

#[test]
fn charge_writes_count() {
    let today = utc_day(86_400 * 3 + 5);
    let mut operation = ChargeUsageOperation::new(user(), today, Some(2));
    operation.start();
    operation.step(started());
    let effects = operation.step(usage_read(Some(AssistantUsage {
        day: today,
        count: 1,
    })));
    let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
        panic!("expected one usage write, got {effects:?}");
    };
    let written = AssistantUsage::from_bytes(value.as_ref()).unwrap();
    assert_eq!(
        written,
        AssistantUsage {
            day: today,
            count: 2
        }
    );
    let key = usage_key(user());
    operation.step(Event::Storage(StorageEvent::WriteResult { key }));
    operation.step(committed());
    assert_eq!(operation.finalize().unwrap(), written);
}

#[test]
fn refused_charge_aborts() {
    let mut operation = ChargeUsageOperation::new(user(), 9, Some(2));
    operation.start();
    operation.step(started());
    let effects = operation.step(usage_read(Some(AssistantUsage { day: 9, count: 2 })));
    assert!(aborts(&effects));
    assert!(matches!(
        operation.finalize(),
        Err(CentralStoreError::QuotaReached(QuotaReached {
            used: 2,
            limit: 2
        }))
    ));
}

#[test]
fn charge_resets_daily() {
    // The last second of day 9 is refused; the first second of day 10 counts from zero.
    let last = 86_400 * 10 - 1;
    let mut operation = ChargeUsageOperation::new(user(), utc_day(last), Some(2));
    operation.start();
    operation.step(started());
    let full = Some(AssistantUsage { day: 9, count: 2 });
    assert!(aborts(&operation.step(usage_read(full))));
    let mut operation = ChargeUsageOperation::new(user(), utc_day(last + 1), Some(2));
    operation.start();
    operation.step(started());
    let effects = operation.step(usage_read(full));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Write { .. })]
    ));
}

#[test]
fn reads_central_usage() {
    let mut operation = GetCentralOperation::new(user());
    let effects = operation.start();
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::BatchRead { reads, txn_id: None })] if reads.len() == 2
    ));
    let usage = AssistantUsage { day: 4, count: 1 };
    operation.step(Event::Storage(StorageEvent::BatchReadResult {
        values: vec![
            (
                central_key(),
                Some(central("Realm").to_bytes().unwrap().into()),
            ),
            (usage_key(user()), Some(usage.to_bytes().unwrap().into())),
        ],
    }));
    let (stored, read_usage) = operation.finalize().unwrap();
    assert_eq!(stored.unwrap().provider.label, "Realm");
    assert_eq!(read_usage, Some(usage));
}
