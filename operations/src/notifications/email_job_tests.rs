//! Tests the email job as calls: the realm profile, the delivery decision, the cached sender and
//! a sender built with the opened SMTP password.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::notifications::user;
use aruna_core::calls::ChildOutcome;
use aruna_core::realm_vault::{RealmSecretRef, RealmVaultEntry, RealmVaultPurpose};
use aruna_core::register::RegisterStamp;
use aruna_core::structs::execution::email::{RealmEmailProfile, SmtpTls};
use aruna_core::structs::execution::job::JobErrorKind;
use aruna_core::structs::execution::notification::{
    NotificationClass, NotificationKind, NotificationRecord,
};
use std::time::Duration;
use ulid::Ulid;

const ADDRESS: &str = "ada@example.org";
const NOW_MS: u64 = 1_000;

fn node() -> NodeId {
    iroh::SecretKey::from_bytes(&[8; 32]).public()
}

fn spec() -> EmailDeliverySpec {
    let kind = NotificationKind::ShareUpload {
        share_id: Ulid::from_parts(4, 4),
        node_id: node(),
        group_id: Ulid::from_parts(5, 5),
        bucket: "inbox".to_string(),
        key: "a.csv".to_string(),
        size_bytes: 9,
    };
    let mut notification = NotificationRecord::new(user(1, 2), NotificationClass::Direct, kind, 42);
    notification.notification_id = Ulid::from_parts(7, 7);
    EmailDeliverySpec {
        preference_id: Ulid::nil(),
        notification,
    }
}

fn profile(password: bool) -> RealmEmailProfile {
    let entry = RealmVaultEntry::new(RealmVaultPurpose::SmtpPassword, Ulid::from_parts(3, 3));
    RealmEmailProfile {
        revision: Ulid::from_parts(9, 1),
        host: "smtp.example.org".to_string(),
        port: Some(587),
        tls: SmtpTls::Starttls,
        username: password.then(|| "aruna".to_string()),
        from: "aruna@example.org".to_string(),
        password: password.then(|| RealmSecretRef {
            entry,
            revision: Ulid::from_parts(4, 1),
        }),
        changed_by: user(1, 2),
        changed_at_ms: 1,
    }
}

fn config(profile: Option<RealmEmailProfile>) -> RealmConfigDocument {
    let mut config = RealmConfigDocument::new(user(1, 2).realm_id, Vec::new(), 3);
    config.email = profile;
    config
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

/// An email setting with these categories and the opened address.
fn winner(categories: &[&str]) -> Delivery {
    let id = Ulid::from_parts(6, 6);
    let preference = EmailPreference {
        preference_id: id,
        user_id: user(1, 2),
        stamp: RegisterStamp {
            counter: 1,
            node_id: node(),
            write_id: id,
        },
        categories: categories
            .iter()
            .map(|category| category.to_string())
            .collect(),
        sealed: Some(vec![1]),
        created_at_ms: 1,
    };
    Delivery::Send(SettingWinner {
        bytes: preference.to_bytes().unwrap(),
        secret: Some(SecretBytes::new(ADDRESS.as_bytes().to_vec())),
    })
}

/// A job that read `profile` and decided `delivery`.
fn decided(profile: Option<RealmEmailProfile>, delivery: Delivery) -> (EmailJobOperation, Turn) {
    let mut job = EmailJobOperation::new(spec(), node(), NOW_MS);
    assert!(matches!(job.first(), Ok(Next::Call(IoCall::Child(_)))));
    let read = job.next(child::<_, GetConfigError>(Ok(config(profile))));
    assert!(matches!(read, Ok(Next::Call(IoCall::Child(_)))));
    let turn = job.next(child::<_, IoError>(Ok(delivery)));
    (job, turn)
}

fn unsent_turn() -> Turn {
    Ok(Next::Done(DeliveryOutcome::Succeeded(
        JobResultPayload::Email { sent: false },
    )))
}

fn sent_turn() -> Turn {
    Ok(Next::Done(DeliveryOutcome::Succeeded(
        JobResultPayload::Email { sent: true },
    )))
}

#[test]
fn expired_notice_finishes() {
    let expired = spec().notification.expires_at_ms();
    let mut job = EmailJobOperation::new(spec(), node(), expired);
    assert_eq!(job.first(), unsent_turn());
}

#[test]
fn unconfigured_realm_fails() {
    let mut job = EmailJobOperation::new(spec(), node(), NOW_MS);
    job.first().unwrap();
    let Ok(Next::Done(DeliveryOutcome::Failed(error))) =
        job.next(child::<_, GetConfigError>(Ok(config(None))))
    else {
        panic!("a realm without a profile sends nothing");
    };
    assert_eq!(error.kind, JobErrorKind::Permanent);
}

/// An unreadable config still decides, with no manager to ask; a wait defers the job.
#[test]
fn unreadable_config_defers() {
    let mut job = EmailJobOperation::new(spec(), node(), NOW_MS);
    job.first().unwrap();
    let unread = job.next(child::<RealmConfigDocument, _>(Err(
        GetConfigError::DocumentNotFound,
    )));
    assert!(matches!(unread, Ok(Next::Call(IoCall::Child(_)))));
    let error = JobError::retryable("the realm configuration cannot be read now");
    let wait = Delivery::Wait(error.clone(), Duration::from_secs(30));
    assert_eq!(
        job.next(child::<_, IoError>(Ok(wait))),
        Ok(Next::Done(DeliveryOutcome::DeferredFor(
            error,
            Duration::from_secs(30)
        )))
    );
}

#[test]
fn unwanted_skips() {
    assert_eq!(
        decided(Some(profile(false)), Delivery::Skip).1,
        unsent_turn()
    );
    let excluded = winner(&["group.membership"]);
    assert_eq!(decided(Some(profile(false)), excluded).1, unsent_turn());
}

/// The cached sender mails the notice to the opened address; refusals keep their kind.
#[test]
fn cached_sender_sends() {
    let (mut job, turn) = decided(Some(profile(false)), winner(&["share.upload"]));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let mail = job.mail.clone().unwrap();
    assert_eq!(
        (mail.to.as_str(), mail.id),
        (ADDRESS, Ulid::from_parts(7, 7))
    );
    assert!(
        !format!("{mail:?}").contains(ADDRESS),
        "Debug hides the recipient"
    );
    assert_eq!(
        job.next(child::<Option<u16>, SendError>(Ok(None))),
        sent_turn()
    );

    for (refusal, kind) in [
        (
            SendError::Failed("SMTP status 451".into()),
            JobErrorKind::Retryable,
        ),
        (
            SendError::Refused("SMTP status 550".into()),
            JobErrorKind::Permanent,
        ),
    ] {
        let (mut job, _) = decided(Some(profile(false)), winner(&["share.upload"]));
        let Ok(Next::Done(DeliveryOutcome::Failed(error))) =
            job.next(child::<Option<u16>, _>(Err(refusal)))
        else {
            panic!("a refusal fails the attempt");
        };
        assert_eq!(error.kind, kind);
    }
}

/// Without a cached sender, the SMTP password is opened and a sender built from the profile.
#[test]
fn sender_built_once() {
    let (mut job, _) = decided(Some(profile(true)), winner(&["share.upload"]));
    let no_sender = job.next(child::<Option<u16>, _>(Err(SendError::NoSender)));
    assert!(matches!(no_sender, Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(job.state, EmailState::Password);
    let password = SecretBytes::new(b"canary-smtp-31".to_vec());
    let built = job.next(child::<_, RealmSecretError>(Ok(password)));
    assert!(matches!(built, Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(job.state, EmailState::Built);
    assert_eq!(
        job.next(child::<Option<u16>, SendError>(Ok(None))),
        sent_turn()
    );

    let (mut open, _) = decided(Some(profile(false)), winner(&["share.upload"]));
    open.next(child::<Option<u16>, _>(Err(SendError::NoSender)))
        .unwrap();
    assert_eq!(
        open.state,
        EmailState::Built,
        "a profile without password builds at once"
    );

    let (mut locked, _) = decided(Some(profile(true)), winner(&["share.upload"]));
    locked
        .next(child::<Option<u16>, _>(Err(SendError::NoSender)))
        .unwrap();
    let unavailable = RealmSecretError::Unavailable("no manager".into());
    let Ok(Next::Done(DeliveryOutcome::Failed(error))) =
        locked.next(child::<SecretBytes, _>(Err(unavailable)))
    else {
        panic!("an unopened password fails the attempt");
    };
    assert_eq!(error.kind, JobErrorKind::Retryable);
}

#[test]
fn unexpected_results_fail() {
    let mut job = EmailJobOperation::new(spec(), node(), NOW_MS);
    job.first().unwrap();
    assert!(matches!(
        job.next(IoResult::Done),
        Err(IoError::Unexpected { .. })
    ));
}
