//! Intent-first `notify` stage sending plain-text notices to group admins or members.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::automation::firing::Firing;
use aruna_core::automation::record::{Automation, StageKind};
use aruna_core::automation::scope::Audience;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id_mint::IdMint;
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, AUTOMATION_INTENTS_KEYSPACE, NOTIFICATION_OUTBOX_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::plugin_runs::CallIntent;
use aruna_core::structs::execution::notification::{
    NotificationClass, NotificationKind, NotificationRecord, new_outbox_record,
    notification_outbox_key, schedule_drain_effect,
};
use aruna_core::structs::identity::group::GroupAuthorizationDocument;
use aruna_core::structs::identity::realm::RealmId;
use byteview::ByteView;
use ulid::Ulid;

use super::AutoError;
use super::copy_stage::{StageOutcome, call_intent, stage_intent_id};
use super::run_stage::render_text;
use crate::plugins::intents::{Begun, begin_in, settled_row_in};
use crate::plugins::rows::StoreError;

#[derive(Debug, PartialEq)]
enum NotifyStep {
    Begin,
    AuthDoc,
    Write(u32),
}

/// Sends a plain-text notification to `GroupAdmins` or `GroupMembers` via the notification outbox.
#[derive(Debug, PartialEq)]
pub struct NotifyStageOp {
    auto: Automation,
    firing_id: Ulid,
    stage_name: String,
    stage_id: Ulid,
    audience: Audience,
    message: String,
    realm_id: RealmId,
    intent: CallIntent,
    ids: IdMint,
    now_ms: u64,
    step: NotifyStep,
}

impl NotifyStageOp {
    pub fn new(
        auto: Automation,
        firing: &Firing,
        stage_idx: usize,
        realm_id: RealmId,
    ) -> Result<Self, AutoError> {
        let (stage_name, audience, message) = match firing.spec.stages.get(stage_idx) {
            Some(stage) => match &stage.kind {
                StageKind::Notify { audience, message } => {
                    (stage.name.clone(), *audience, message.clone())
                }
                _ => return Err(AutoError::Invalid("stage is not a notify stage".into())),
            },
            None => return Err(AutoError::Invalid("missing notify stage".into())),
        };
        let rendered = render_text(&message, firing);
        let stage_id = stage_intent_id(firing.firing_id, &stage_name);
        let intent = call_intent(0, &[audience.as_str().as_bytes(), rendered.as_bytes()]);
        Ok(Self {
            auto,
            firing_id: firing.firing_id,
            stage_name,
            stage_id,
            audience,
            message: rendered,
            realm_id,
            intent,
            ids: IdMint::default(),
            now_ms: 0,
            step: NotifyStep::Begin,
        })
    }
}

impl Operation for NotifyStageOp {
    type Output = StageOutcome;
    type Error = AutoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn seed(&mut self, seed: u128) {
        self.ids = IdMint::new(seed);
    }

    fn first(&mut self) -> Result<Next<StageOutcome>, AutoError> {
        let begin = begin_in(
            AUTOMATION_INTENTS_KEYSPACE,
            self.stage_id,
            self.intent.clone(),
        );
        Ok(Next::Call(IoCall::child(begin)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<StageOutcome>, AutoError> {
        match std::mem::replace(&mut self.step, NotifyStep::Begin) {
            NotifyStep::Begin => match result
                .child::<Begun, StoreError>()?
                .map_err(|e| AutoError::Storage(e.detail()))?
            {
                Begun::Fresh => {
                    self.step = NotifyStep::AuthDoc;
                    Ok(Next::Call(IoCall::Read(vec![(
                        AUTH_KEYSPACE.to_string(),
                        ByteView::from(self.auto.group_id.to_bytes()),
                    )])))
                }
                Begun::Settled(reply) => {
                    let count = reply.parse::<u32>().unwrap_or(0);
                    Ok(Next::Done(StageOutcome::Notified(count)))
                }
                Begun::Changed | Begun::Unknown(_) => Ok(Next::Done(StageOutcome::NeedsAttention(
                    "notification delivery outcome unknown".into(),
                    None,
                ))),
            },
            NotifyStep::AuthDoc => {
                let mut recipients = BTreeSet::new();
                if let Some(bytes) = result.value()? {
                    let doc = GroupAuthorizationDocument::from_bytes(&bytes)?;
                    for role in doc.roles.values() {
                        for &user in &role.assigned_users {
                            if user.is_nil() || self.auto.account == Some(user) {
                                continue;
                            }
                            let keep = match self.audience {
                                Audience::GroupAdmins => doc.is_admin(self.realm_id, user),
                                Audience::GroupMembers => true,
                            };
                            if keep {
                                recipients.insert(user);
                            }
                        }
                    }
                }
                let count = recipients.len() as u32;
                let mut writes = Vec::with_capacity(recipients.len() + 1);
                for recipient in recipients {
                    let notice_id = self.ids.mint(self.now_ms);
                    let outbox_id = self.ids.mint(self.now_ms);
                    let kind = NotificationKind::AutomationNotice {
                        group_id: self.auto.group_id,
                        automation_id: self.auto.automation_id,
                        firing_id: self.firing_id,
                        stage: self.stage_name.clone(),
                        message: self.message.clone(),
                        run_link: format!("/compute/automation-runs/{}", self.firing_id),
                    };
                    let rec = NotificationRecord::new(
                        notice_id,
                        recipient,
                        NotificationClass::Direct,
                        kind,
                        self.now_ms,
                    );
                    let outbox = new_outbox_record(outbox_id, rec);
                    let encoded = postcard::to_allocvec(&outbox)
                        .map_err(|e| AutoError::Storage(e.to_string()))?;
                    writes.push((
                        NOTIFICATION_OUTBOX_KEYSPACE.to_string(),
                        notification_outbox_key(outbox_id),
                        ByteView::from(encoded),
                    ));
                }
                writes.push(
                    settled_row_in(
                        AUTOMATION_INTENTS_KEYSPACE,
                        self.stage_id,
                        &self.intent,
                        count.to_string(),
                    )
                    .map_err(|e| AutoError::Storage(e.detail()))?,
                );
                self.step = NotifyStep::Write(count);
                Ok(Next::Call(IoCall::Write(writes)))
            }
            NotifyStep::Write(count) => {
                result.done()?;
                Ok(Next::Then {
                    output: StageOutcome::Notified(count),
                    calls: vec![IoCall::from(schedule_drain_effect())],
                })
            }
        }
    }
}
