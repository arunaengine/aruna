//! Checks that the record append refuses an event before it starts.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::JobRecordFrame;
use aruna_core::operation_check::{rejects_events, sample_events};
use aruna_core::structs::execution::job::JobFamilyRecord;

use crate::jobs::records::append::{AppendRecordConfig, AppendRecordOperation, RecordOrigin};
use crate::tests::records::{Family, REALM};

fn append() -> AppendRecordOperation {
    let family = Family::new([31u8; 32]);
    let spec = family.spec();
    let record =
        JobRecordFrame::new(family.sign(&family.holder, JobFamilyRecord::Spec(Box::new(spec))))
            .expect("bounded record");
    AppendRecordOperation::new(AppendRecordConfig {
        realm_id: REALM,
        local_node_id: family.holder.public(),
        record,
        local: None,
        origin: RecordOrigin::Local,
        now_ms: 3_000,
    })
}

#[test]
fn rejects_unstarted_events() {
    let events = sample_events().len();
    assert_eq!(
        rejects_events(append, sample_events, |_| false).len(),
        events
    );
}
