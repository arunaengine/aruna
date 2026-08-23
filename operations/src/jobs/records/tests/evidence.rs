//! Evidence is read by exact key or by paging one whole kind, never from a
//! bounded prefix of the family.

use aruna_core::effects::{Effect, JobRecordFrame, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::JOB_FAMILY_RECORD_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::{
    JobFamilyRecord, JobRecordBody, JobRecordEnvelope, JobRecordKind, LaunchIntent, LogicalJobSpec,
    PhysicalExecutionState, RealmNodeKind,
};
use aruna_core::types::{Key, Value};
use ulid::Ulid;

use super::fixture::context as fixture;
use super::fixture::{Family, REALM, actor, node, secret};
use crate::driver::{DriverContext, drive};
use crate::jobs::records::admit::Admission;
use crate::jobs::records::keys::record_key;
use crate::jobs::records::rows::{PendingNeed, to_bytes};
use crate::jobs::records::verify::EvidencePlan;
use crate::jobs::records::{
    AppendOutcome, AppendRecordConfig, AppendRecordOperation, RecordOrigin, RecordStoreError,
};

/// Rows past the 256-record prefix the append used to read.
const OVERFLOW: u16 = 260;

async fn append(
    context: &DriverContext,
    family: &Family,
    envelope: JobRecordEnvelope,
) -> AppendOutcome {
    let record = JobRecordFrame::new(envelope).expect("bounded record");
    drive(
        AppendRecordOperation::new(AppendRecordConfig {
            realm_id: REALM,
            local_node_id: family.holder.public(),
            record,
            local: None,
            origin: RecordOrigin::Local,
            now_ms: 3_000,
        }),
        context,
    )
    .await
    .expect("append completes")
}

/// Writes authentic rows directly, so a family large enough to overflow one
/// bounded scan does not need one append per record.
async fn seed(context: &DriverContext, records: &[JobRecordEnvelope]) {
    let writes: Vec<(String, Key, Value)> = records
        .iter()
        .map(|envelope| {
            (
                JOB_FAMILY_RECORD_KEYSPACE.to_string(),
                record_key(&envelope.key()),
                Value::from(to_bytes(envelope).expect("record encodes").as_slice()),
            )
        })
        .collect();
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));
}

/// A subject that sorts before every identity built from a high byte.
fn low_id(index: u16) -> [u8; 16] {
    let mut bytes = [0u8; 16];
    bytes[..2].copy_from_slice(&index.to_be_bytes());
    bytes
}

fn filler_launches(family: &Family, spec: &LogicalJobSpec, count: u16) -> Vec<JobRecordEnvelope> {
    (0..count)
        .map(|index| {
            let mut launch = family.launch(spec, family.holder.public(), 0);
            launch.launch_id = Ulid::from_bytes(low_id(index));
            family.sign(&family.holder, JobFamilyRecord::Launch(Box::new(launch)))
        })
        .collect()
}

fn filler_receipts(family: &Family, launch: &LaunchIntent, count: u16) -> Vec<JobRecordEnvelope> {
    (0..count)
        .map(|index| {
            let mut receipt = family.receipt(launch, 1);
            receipt.execution_id = Ulid::from_bytes(low_id(index));
            family.sign(&family.target, JobFamilyRecord::Receipt(Box::new(receipt)))
        })
        .collect()
}

#[tokio::test]
async fn admits_late_launch() {
    // The launch a receipt names sorts after the whole filler run, so only an
    // exact predecessor read finds it.
    let family = Family::new([21u8; 32]);
    let (_dir, context) = fixture(&family.config, family.holder.public()).await;
    let spec = family.spec();
    let mut launch = family.launch(&spec, family.holder.public(), 0);
    launch.launch_id = Ulid::from_bytes([0xffu8; 16]);
    let receipt = family.receipt(&launch, 1);

    let mut seeded = filler_launches(&family, &spec, OVERFLOW);
    seeded.push(family.sign(&family.holder, JobFamilyRecord::Launch(Box::new(launch))));
    seed(&context, &seeded).await;

    let outcome = append(
        &context,
        &family,
        family.sign(&family.target, JobFamilyRecord::Receipt(Box::new(receipt))),
    )
    .await;
    assert_eq!(outcome.admission, Admission::Authentic);
}

#[tokio::test]
async fn admits_late_receipt() {
    // The receipt a first update is chained to sorts after the filler run.
    let family = Family::new([22u8; 32]);
    let (_dir, context) = fixture(&family.config, family.holder.public()).await;
    let spec = family.spec();
    let launch = family.launch(&spec, family.holder.public(), 0);
    let mut receipt = family.receipt(&launch, 1);
    receipt.execution_id = Ulid::from_bytes([0xffu8; 16]);
    let update = family.update(
        &receipt,
        0,
        receipt.digest().expect("receipt digest"),
        PhysicalExecutionState::Running,
        None,
    );

    let mut seeded = filler_receipts(&family, &launch, OVERFLOW);
    seeded.push(family.sign(&family.target, JobFamilyRecord::Receipt(Box::new(receipt))));
    seed(&context, &seeded).await;

    let outcome = append(
        &context,
        &family,
        family.sign(&family.target, JobFamilyRecord::Update(Box::new(update))),
    )
    .await;
    assert_eq!(outcome.admission, Admission::Authentic);
}

#[tokio::test]
async fn admits_late_update() {
    // Both predecessors of a later update, its receipt and the update before
    // it, sort after the filler run.
    let family = Family::new([23u8; 32]);
    let (_dir, context) = fixture(&family.config, family.holder.public()).await;
    let spec = family.spec();
    let launch = family.launch(&spec, family.holder.public(), 0);
    let mut receipt = family.receipt(&launch, 1);
    receipt.execution_id = Ulid::from_bytes([0xffu8; 16]);
    let first = family.update(
        &receipt,
        0,
        receipt.digest().expect("receipt digest"),
        PhysicalExecutionState::Running,
        None,
    );
    let second = family.update(
        &receipt,
        1,
        first.digest().expect("update digest"),
        PhysicalExecutionState::Running,
        None,
    );

    let mut seeded = filler_receipts(&family, &launch, OVERFLOW);
    seeded.push(family.sign(&family.target, JobFamilyRecord::Receipt(Box::new(receipt))));
    seeded.push(family.sign(&family.target, JobFamilyRecord::Update(Box::new(first))));
    seed(&context, &seeded).await;

    let outcome = append(
        &context,
        &family,
        family.sign(&family.target, JobFamilyRecord::Update(Box::new(second))),
    )
    .await;
    assert_eq!(outcome.admission, Admission::Authentic);
}

#[tokio::test]
async fn admits_late_output() {
    // The receipt an output is sealed against sorts after the filler run.
    let family = Family::new([24u8; 32]);
    let (_dir, context) = fixture(&family.config, family.holder.public()).await;
    let spec = family.spec();
    let launch = family.launch(&spec, family.holder.public(), 0);
    let mut receipt = family.receipt(&launch, 1);
    receipt.execution_id = Ulid::from_bytes([0xffu8; 16]);
    let output = family.output(&receipt);

    let mut seeded = filler_receipts(&family, &launch, OVERFLOW);
    seeded.push(family.sign(&family.target, JobFamilyRecord::Receipt(Box::new(receipt))));
    seed(&context, &seeded).await;

    let outcome = append(
        &context,
        &family,
        family.sign(&family.target, JobFamilyRecord::Output(Box::new(output))),
    )
    .await;
    assert_eq!(outcome.admission, Admission::Authentic);
}

#[tokio::test]
async fn pages_receipt_kind() {
    // A launch whose scheduler this view no longer ranks as a holder is
    // authentic only through the receipt that sealed it, and that receipt lies
    // on a later page of its own kind.
    let mut family = Family::new([25u8; 32]);
    family.config.ensure_node(node(7), RealmNodeKind::Server);
    let (_dir, context) = fixture(&family.config, family.holder.public()).await;
    let scheduler = secret(7);
    let spec = family.spec();
    let mut launch = family.launch(&spec, scheduler.public(), 0);
    launch.launch_id = Ulid::from_bytes([0x77u8; 16]);
    let mut receipt = family.receipt(&launch, 1);
    receipt.execution_id = Ulid::from_bytes([0xffu8; 16]);

    // The fillers seal a different launch, so only the receipt seeded later
    // can authenticate the offered one.
    let mut other = family.launch(&spec, scheduler.public(), 1);
    other.launch_id = Ulid::from_bytes([0x66u8; 16]);
    let mut seeded = vec![
        family.sign(
            &family.holder,
            JobFamilyRecord::Spec(Box::new(spec.clone())),
        ),
        family.sign(
            &scheduler,
            JobFamilyRecord::Budget(family.budget(&spec, scheduler.public())),
        ),
    ];
    seeded.extend(filler_receipts(&family, &other, 100));
    seed(&context, &seeded).await;

    let offered = family.sign(&scheduler, JobFamilyRecord::Launch(Box::new(launch)));
    let deferred = append(&context, &family, offered.clone()).await;
    assert_eq!(
        deferred.admission,
        Admission::Pending(PendingNeed::HolderView)
    );

    seed(
        &context,
        &[family.sign(&family.target, JobFamilyRecord::Receipt(Box::new(receipt)))],
    )
    .await;
    assert_eq!(
        append(&context, &family, offered).await.admission,
        Admission::Authentic
    );
}

#[tokio::test]
async fn admits_retained_pending() {
    // A retained record is re-judged against evidence read for it, not only
    // against the record whose arrival triggered the append.
    let family = Family::new([26u8; 32]);
    let (_dir, context) = fixture(&family.config, family.holder.public()).await;
    let spec = family.spec();
    let launch = family.launch(&spec, family.holder.public(), 0);
    let mut receipt = family.receipt(&launch, 1);
    receipt.execution_id = Ulid::from_bytes([0xffu8; 16]);
    let first = family.update(
        &receipt,
        0,
        receipt.digest().expect("receipt digest"),
        PhysicalExecutionState::Running,
        None,
    );
    let second = family.update(
        &receipt,
        1,
        first.digest().expect("update digest"),
        PhysicalExecutionState::Running,
        None,
    );

    let retained = append(
        &context,
        &family,
        family.sign(&family.target, JobFamilyRecord::Update(Box::new(second))),
    )
    .await;
    assert_eq!(
        retained.admission,
        Admission::Pending(PendingNeed::Evidence(JobRecordKind::Receipt))
    );

    let mut seeded = filler_receipts(&family, &launch, OVERFLOW);
    seeded.push(family.sign(&family.target, JobFamilyRecord::Receipt(Box::new(receipt))));
    seeded.push(family.sign(&family.target, JobFamilyRecord::Update(Box::new(first))));
    seed(&context, &seeded).await;

    let outcome = append(
        &context,
        &family,
        family.sign(&family.holder, JobFamilyRecord::Spec(Box::new(spec))),
    )
    .await;
    assert_eq!(outcome.admission, Admission::Authentic);
    assert_eq!(outcome.admitted, 2);
}

#[test]
fn derives_exact_predecessors() {
    // Every derived key must equal the predecessor record's own key; only a
    // predecessor selected by digest or by a foreign id needs a kind scan.
    let family = Family::new([28u8; 32]);
    let spec = family.spec();
    let launch = family.launch(&spec, family.holder.public(), 0);
    let receipt = family.receipt(&launch, 1);
    let previous = family.update(
        &receipt,
        0,
        [0u8; 32],
        PhysicalExecutionState::Running,
        None,
    );
    let update = family.update(
        &receipt,
        1,
        [0u8; 32],
        PhysicalExecutionState::Running,
        None,
    );

    let mut plan = EvidencePlan::default();
    plan.extend(&JobFamilyRecord::Claim(family.claim(&spec)));
    assert_eq!(
        plan.keys.iter().copied().collect::<Vec<_>>(),
        vec![JobFamilyRecord::Spec(Box::new(spec.clone())).key()]
    );
    assert!(plan.kinds.is_empty());

    let mut plan = EvidencePlan::default();
    plan.extend(&JobFamilyRecord::Update(Box::new(update)));
    assert!(
        plan.keys
            .contains(&JobFamilyRecord::Receipt(Box::new(receipt.clone())).key())
    );
    assert!(
        plan.keys
            .contains(&JobFamilyRecord::Update(Box::new(previous)).key())
    );
    assert!(plan.kinds.is_empty());

    let mut plan = EvidencePlan::default();
    plan.extend(&JobFamilyRecord::Receipt(Box::new(receipt)));
    assert!(
        plan.keys
            .contains(&JobFamilyRecord::Launch(Box::new(launch.clone())).key())
    );

    let mut plan = EvidencePlan::default();
    plan.extend(&JobFamilyRecord::Launch(Box::new(launch)));
    assert_eq!(
        plan.keys.iter().copied().collect::<Vec<_>>(),
        vec![JobFamilyRecord::Budget(family.budget(&spec, family.holder.public())).key()]
    );
    assert_eq!(
        plan.kinds.iter().copied().collect::<Vec<_>>(),
        vec![JobRecordKind::Spec, JobRecordKind::Receipt]
    );
}

#[test]
fn refuses_partial_scan() {
    // A failed evidence page must not admit the candidate from the rows that
    // did load: the append fails and writes nothing.
    let family = Family::new([27u8; 32]);
    let spec = family.spec();
    let launch = family.launch(&spec, family.holder.public(), 0);
    let record =
        JobRecordFrame::new(family.sign(&family.holder, JobFamilyRecord::Launch(Box::new(launch))))
            .expect("bounded record");
    let mut operation = AppendRecordOperation::new(AppendRecordConfig {
        realm_id: REALM,
        local_node_id: family.holder.public(),
        record,
        local: None,
        origin: RecordOrigin::Local,
        now_ms: 3_000,
    });
    let config = family
        .config
        .to_bytes(&actor(family.holder.public()))
        .expect("config encodes");
    let txn_id = Ulid::from_bytes([1u8; 16]);
    let empty = Key::from([0u8].as_slice());

    let mut effects: Vec<Effect> = operation.start().into_iter().collect();
    effects.extend(operation.step(Event::Storage(StorageEvent::ReadResult {
        key: empty.clone(),
        value: Some(Value::from(config.as_slice())),
    })));
    effects.extend(operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id })));
    effects.extend(
        operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![(empty.clone(), None), (empty.clone(), None)],
        })),
    );
    effects.extend(operation.step(Event::Storage(StorageEvent::IterResult {
        values: Vec::new(),
        next_start_after: None,
    })));
    effects.extend(
        operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![(empty, None)],
        })),
    );
    assert!(matches!(
        effects.last(),
        Some(Effect::Storage(StorageEffect::Iter { .. }))
    ));

    effects.extend(operation.step(Event::Storage(StorageEvent::Error {
        error: StorageError::ReadError("boom".to_string()),
    })));
    assert!(operation.is_complete());
    assert!(
        !effects
            .iter()
            .any(|effect| matches!(effect, Effect::Storage(StorageEffect::BatchWrite { .. })))
    );
    assert_eq!(
        operation.finalize(),
        Err(RecordStoreError::Storage(StorageError::ReadError(
            "boom".to_string()
        )))
    );
}
