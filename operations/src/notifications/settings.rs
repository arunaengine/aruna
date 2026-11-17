//! Decides deliveries from this node's view of a user's notification settings and asks a
//! management node to open the sealed secret.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::SecretBytes;
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, MetadataRequestEffect, NetEffect};
use aruna_core::events::{Event, MetadataAnswer, NetEvent};
use aruna_core::operation::Operation;
use aruna_core::outbound::{
    BackoffChange, BackoffRequest, OutboundEffect, OutboundEvent, StoredBackoff,
};
use aruna_core::structs::execution::job::{JobError, JobResultPayload};
use aruna_core::structs::execution::notification::NotificationRecord;
use aruna_core::structs::execution::setting::SettingChannel;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use tracing::warn;

use crate::metadata::protocol::{
    MetadataReadError, MetadataTransportMessage, decode_message, encode_message,
};
use crate::notifications::setting_rows::{ReadSettingOperation, SettingError};
use crate::notifications::unexpected_event;
use crate::realm_vault::{SecretAnswer, ServeRequest, ServeSecretOperation};

/// The first delay after no management node opened a secret; it doubles up to `BACKOFF_CAP_MS`.
pub(crate) const BACKOFF_START_MS: u64 = 30_000;
const BACKOFF_CAP_MS: u64 = 30 * 60_000;
/// How long one management node may take to open a setting secret.
const OPEN_TIMEOUT: Duration = Duration::from_secs(30);

/// The current setting with its secret opened when it is on.
#[derive(Clone, PartialEq, Eq)]
pub struct SettingWinner {
    pub bytes: Vec<u8>,
    pub secret: Option<SecretBytes>,
}

impl std::fmt::Debug for SettingWinner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SettingWinner")
            .field("bytes_len", &self.bytes.len())
            .field("has_secret", &self.secret.is_some())
            .finish()
    }
}

/// The management nodes of `config`, in config order.
pub fn managers(config: &RealmConfigDocument) -> Vec<NodeId> {
    (config.nodes.iter())
        .filter(|node| node.kind == RealmNodeKind::Management)
        .filter_map(|node| node.node_id.parse::<NodeId>().ok())
        .collect()
}

/// The next delay while no management node opens a secret: it starts at `BACKOFF_START_MS` and
/// doubles up to `BACKOFF_CAP_MS`.
fn next_delay(previous_ms: Option<u64>) -> u64 {
    previous_ms.map_or(BACKOFF_START_MS, |delay| {
        delay.saturating_mul(2).min(BACKOFF_CAP_MS)
    })
}

/// What a delivery attempt does.
#[derive(Debug, PartialEq)]
pub enum Delivery {
    /// The notice expired, or the current setting on this node is off or missing.
    Skip,
    /// Try again after the delay without spending an attempt.
    Wait(JobError, Duration),
    /// Send with the current setting and its opened secret.
    Send(SettingWinner),
}

/// How a delivery job ends, as the job runtime takes it.
#[derive(Debug, PartialEq)]
pub enum DeliveryOutcome {
    Succeeded(JobResultPayload),
    Failed(JobError),
    /// Runs again after the delay without spending an attempt.
    DeferredFor(JobError, Duration),
}

/// Where the delivery decision waits.
#[derive(Clone, Debug, Default, PartialEq)]
enum DeliveryState {
    #[default]
    Setting,
    /// Reads the stored delay; `failed` names a failure that sets the next one.
    Backoff { failed: Option<&'static str> },
    /// Stores the next delay, then waits with `reason`.
    Fail { until_ms: u64, reason: &'static str },
    /// Asks `managers[asked]` to open the secret of `target`.
    Open {
        target: DocumentTarget,
        asked: usize,
    },
    /// Clears the delay, then sends with `secret`.
    Clear { secret: SecretBytes },
}

/// Decides one delivery from this node's current setting. When it is on, the management nodes
/// are asked in config order to open its secret and the first answer is used. `managers` is
/// `None` when the realm config could not be read, which counts as no answer. While no
/// management node answers, the delay of the outbound backend grows per owner and channel.
#[derive(Clone, Debug, PartialEq)]
pub struct DeliveryOperation {
    record: NotificationRecord,
    channel: SettingChannel,
    managers: Option<Vec<NodeId>>,
    /// This node and its realm, which serve the request when this node is a manager.
    local: NodeId,
    local_realm: RealmId,
    now_ms: u64,
    state: DeliveryState,
    current: Vec<u8>,
    /// The delay stored before this attempt.
    stored: Option<StoredBackoff>,
}

type Turn = Result<Next<Delivery>, IoError>;

impl DeliveryOperation {
    pub fn new(
        record: NotificationRecord,
        channel: SettingChannel,
        managers: Option<Vec<NodeId>>,
        (local, local_realm): (NodeId, RealmId),
        now_ms: u64,
    ) -> Self {
        Self {
            record,
            channel,
            managers,
            local,
            local_realm,
            now_ms,
            state: DeliveryState::Setting,
            current: Vec::new(),
            stored: None,
        }
    }

    fn backoff(&self, change: BackoffChange) -> IoCall {
        IoCall::from(Effect::Outbound(OutboundEffect::Backoff(BackoffRequest {
            owner: self.record.recipient,
            channel: self.channel,
            change,
        })))
    }

    /// Waits until `until_ms`, at the latest until the notice expires.
    fn wait(&self, until_ms: u64, reason: &str) -> Turn {
        let expires_ms = self.record.expires_at_ms();
        let delay = until_ms.min(expires_ms).saturating_sub(self.now_ms);
        let wait = Delivery::Wait(JobError::retryable(reason), Duration::from_millis(delay));
        Ok(Next::Done(wait))
    }

    /// Stores the next delay after a failed attempt.
    fn fail(&mut self, reason: &'static str) -> Turn {
        let delay_ms = next_delay(self.stored.map(|stored| stored.delay_ms));
        let until_ms = self.now_ms.saturating_add(delay_ms);
        self.state = DeliveryState::Fail { until_ms, reason };
        Ok(Next::Call(
            self.backoff(BackoffChange::Set { until_ms, delay_ms }),
        ))
    }

    fn backed_off(&mut self, failed: Option<&'static str>) -> Turn {
        if let Some(reason) = failed {
            return self.fail(reason);
        }
        if let Some(stored) = self.stored.filter(|stored| stored.until_ms > self.now_ms) {
            return self.wait(stored.until_ms, "waiting for a management node");
        }
        let head = self.channel.head(&self.current);
        let (Ok(head), Some(_)) = (head, self.managers.as_ref()) else {
            return self.fail("the realm configuration cannot be read now");
        };
        let target = self.channel.target(self.record.recipient, head.id);
        self.ask(target, 0)
    }

    /// Asks the manager at `asked`, or stores the next delay once none is left.
    fn ask(&mut self, target: DocumentTarget, asked: usize) -> Turn {
        let managers = self.managers.as_deref().unwrap_or_default();
        let Some(&node) = managers.get(asked) else {
            return self.fail("no management node opened the setting secret");
        };
        let bytes = self.current.clone();
        self.state = DeliveryState::Open {
            target: target.clone(),
            asked,
        };
        if node == self.local {
            let request = ServeRequest::Setting { target, bytes };
            let serve = ServeSecretOperation::new(self.local_realm, node, node, request);
            return Ok(Next::Call(IoCall::child(serve)));
        }
        let message = MetadataTransportMessage::OpenSetting { target, bytes };
        let frame = match encode_message(&message) {
            Ok(frame) => frame,
            Err(error) => return self.refused(node, &error),
        };
        let request = MetadataRequestEffect {
            peer: node,
            frame,
            timeout: OPEN_TIMEOUT,
        };
        Ok(Next::Call(IoCall::from(Effect::Net(
            NetEffect::MetadataRequest(Box::new(request)),
        ))))
    }

    /// Logs a manager that did not open the secret and asks the next one.
    fn refused(&mut self, node: NodeId, error: &dyn std::fmt::Debug) -> Turn {
        warn!(%node, ?error, "A management node did not open a setting");
        let DeliveryState::Open { target, asked } = std::mem::take(&mut self.state) else {
            return Err(IoError::Unfinished);
        };
        self.ask(target, asked + 1)
    }

    fn opened(&mut self, node: NodeId, answer: SecretAnswer) -> Turn {
        match answer {
            Ok(secret) => {
                self.state = DeliveryState::Clear { secret };
                Ok(Next::Call(self.backoff(BackoffChange::Clear)))
            }
            Err(error) => self.refused(node, &error),
        }
    }

    fn asked_node(&self) -> Option<NodeId> {
        let DeliveryState::Open { asked, .. } = self.state else {
            return None;
        };
        self.managers.as_deref()?.get(asked).copied()
    }
}

/// The secret in a manager's encoded answer.
fn remote_secret(answer: MetadataAnswer) -> SecretAnswer {
    let bytes = answer.result.map_err(|_| MetadataReadError::Unavailable)?;
    match decode_message(&bytes) {
        Ok(MetadataTransportMessage::OpenSettingResult { result }) => result,
        _ => Err(MetadataReadError::Unavailable),
    }
}

/// The delay the outbound backend reports.
fn stored_backoff(result: IoResult) -> Result<Option<StoredBackoff>, IoError> {
    match result.event()? {
        Event::Outbound(OutboundEvent::Backoff { stored }) => Ok(stored),
        other => Err(unexpected_event("OutboundEvent::Backoff", &other)),
    }
}

impl Operation for DeliveryOperation {
    type Output = Delivery;
    type Error = IoError;

    fn first(&mut self) -> Turn {
        if self.now_ms >= self.record.expires_at_ms() {
            return Ok(Next::Done(Delivery::Skip));
        }
        let read = ReadSettingOperation::new(self.channel, self.record.recipient);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match self.state.clone() {
            DeliveryState::Setting => {
                let failed = match result.child::<Option<Vec<u8>>, SettingError>()? {
                    Ok(None) => return Ok(Next::Done(Delivery::Skip)),
                    Ok(Some(bytes)) => {
                        self.current = bytes;
                        None
                    }
                    Err(error) => {
                        warn!(%error, "Reading the notification setting failed");
                        Some("the setting read failed")
                    }
                };
                self.state = DeliveryState::Backoff { failed };
                Ok(Next::Call(self.backoff(BackoffChange::Read)))
            }
            DeliveryState::Backoff { failed } => {
                self.stored = stored_backoff(result)?;
                self.backed_off(failed)
            }
            DeliveryState::Fail { until_ms, reason } => {
                stored_backoff(result)?;
                self.wait(until_ms, reason)
            }
            DeliveryState::Open { .. } => {
                let node = self.asked_node().ok_or(IoError::Unfinished)?;
                let answer = match result {
                    IoResult::Child(_) => {
                        let served = result.child::<SecretAnswer, MetadataReadError>()?;
                        served.and_then(|answer| answer)
                    }
                    result => match result.event()? {
                        Event::Net(NetEvent::MetadataAnswer(answer)) => remote_secret(*answer),
                        other => return Err(unexpected_event("a metadata answer", &other)),
                    },
                };
                self.opened(node, answer)
            }
            DeliveryState::Clear { secret } => {
                stored_backoff(result)?;
                Ok(Next::Done(Delivery::Send(SettingWinner {
                    bytes: std::mem::take(&mut self.current),
                    secret: Some(secret),
                })))
            }
        }
    }
}

#[cfg(test)]
#[path = "settings_tests.rs"]
mod tests;
