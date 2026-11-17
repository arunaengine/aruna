//! Intents of mutating plugin calls, written before their effects. A settled call answers its
//! stored reply again; a call whose effect may have happened unrecorded is unknown, never resent.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::keyspaces::PLUGIN_INTENTS_KEYSPACE;
use aruna_core::plugin_runs::CallIntent;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::rows::{
    Answer, Ask, Plan, Row, Rows, RowsOperation, Scan, StoreError, Turn, decode, encode, row,
    unexpected, values,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IntentState {
    /// Written before the effect; found again, the effect may have happened.
    Pending,
    Settled {
        reply: String,
    },
    Unknown {
        reason: String,
    },
    /// A person decided whether the effect happened; nothing waits on the call any more.
    Resolved {
        done: bool,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntentRecord {
    pub digest: [u8; 32],
    pub state: IntentState,
}

/// What a call does after its intent was looked up.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Begun {
    /// No earlier call: the effect may be dispatched now.
    Fresh,
    /// The same call settled before; its stored reply answers it.
    Settled(String),
    /// The call at this ordinal had other arguments: the run changed.
    Changed,
    /// An earlier attempt may have had the effect; it is not dispatched again.
    Unknown(String),
}

pub fn intent_key(run_id: Ulid, ordinal: u32) -> Vec<u8> {
    let mut key = run_id.to_bytes().to_vec();
    key.extend_from_slice(&ordinal.to_be_bytes());
    key
}

/// Looks up the call's intent and writes it as pending when it is new. A pending intent found
/// here belongs to an earlier attempt, so it becomes unknown.
pub fn begin(run_id: Ulid, intent: CallIntent) -> RowsOperation<BeginIntent> {
    RowsOperation::transaction(BeginIntent {
        run_id,
        intent,
        begun: None,
    })
}

#[derive(Debug, PartialEq)]
pub struct BeginIntent {
    run_id: Ulid,
    intent: CallIntent,
    /// The outcome once its intent row is sent.
    begun: Option<Begun>,
}

impl Plan for BeginIntent {
    type Output = Begun;
    type Error = StoreError;

    fn first(&mut self) -> Result<Turn<Begun>, StoreError> {
        let key = intent_key(self.run_id, self.intent.ordinal);
        Ok(Turn::Ask(Ask::Read(vec![(PLUGIN_INTENTS_KEYSPACE, key)])))
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<Begun>, StoreError> {
        if let Some(begun) = self.begun.take() {
            return match answer {
                Answer::Done => Ok(Turn::Finish(begun)),
                other => Err(unexpected(other)),
            };
        }
        let [found] = values::<1>(answer)?;
        let found = found
            .map(|bytes| decode::<IntentRecord>(&bytes))
            .transpose()?;
        let (begun, state) = match found {
            None => (Begun::Fresh, IntentState::Pending),
            Some(record) if record.digest != self.intent.digest => {
                return Ok(Turn::Finish(Begun::Changed));
            }
            Some(record) => match record.state {
                IntentState::Settled { reply } => return Ok(Turn::Finish(Begun::Settled(reply))),
                IntentState::Unknown { reason } => {
                    return Ok(Turn::Finish(Begun::Unknown(reason)));
                }
                IntentState::Resolved { .. } => {
                    let reason = "a person resolved this call".into();
                    return Ok(Turn::Finish(Begun::Unknown(reason)));
                }
                IntentState::Pending => {
                    let reason = "an earlier attempt of this call did not finish".to_string();
                    (
                        Begun::Unknown(reason.clone()),
                        IntentState::Unknown { reason },
                    )
                }
            },
        };
        self.begun = Some(begun);
        let row = intent_row(self.run_id, &self.intent, state)?;
        Ok(Turn::Ask(Ask::Write(vec![row])))
    }
}

/// Whether an earlier attempt of the run already wrote this call's intent.
pub fn written(run_id: Ulid, intent: &CallIntent) -> RowsOperation<IntentWritten> {
    RowsOperation::direct(IntentWritten(intent_key(run_id, intent.ordinal)))
}

#[derive(Debug, PartialEq)]
pub struct IntentWritten(Vec<u8>);

impl Plan for IntentWritten {
    type Output = bool;
    type Error = StoreError;

    fn first(&mut self) -> Result<Turn<bool>, StoreError> {
        let read = vec![(PLUGIN_INTENTS_KEYSPACE, self.0.clone())];
        Ok(Turn::Ask(Ask::Read(read)))
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<bool>, StoreError> {
        let [row] = values::<1>(answer)?;
        Ok(Turn::Finish(row.is_some()))
    }
}

/// The intent row that records the call's reply, written with the call's effect where it has one.
pub(crate) fn settled_row(
    run_id: Ulid,
    intent: &CallIntent,
    reply: String,
) -> Result<Row, StoreError> {
    intent_row(run_id, intent, IntentState::Settled { reply })
}

/// The intent row of a call whose effect may have happened without a recorded reply.
pub(crate) fn unknown_row(
    run_id: Ulid,
    intent: &CallIntent,
    reason: &str,
) -> Result<Row, StoreError> {
    let reason = reason.to_string();
    intent_row(run_id, intent, IntentState::Unknown { reason })
}

/// The key of a call's intent, removed when the call ended without an effect so it may be made
/// again.
pub(crate) fn intent_row_key(run_id: Ulid, intent: &CallIntent) -> (&'static str, Vec<u8>) {
    let key = intent_key(run_id, intent.ordinal);
    (PLUGIN_INTENTS_KEYSPACE, key)
}

fn intent_row(run_id: Ulid, intent: &CallIntent, state: IntentState) -> Result<Row, StoreError> {
    let record = IntentRecord {
        digest: intent.digest,
        state,
    };
    let key = intent_key(run_id, intent.ordinal);
    Ok(row(PLUGIN_INTENTS_KEYSPACE, key, encode(&record)?))
}

/// Reads every intent of a run.
pub(crate) fn intent_scan(run_id: Ulid) -> Scan {
    Scan::new(PLUGIN_INTENTS_KEYSPACE, run_id.to_bytes().to_vec())
}

/// The first intent of the run that is pending or unknown, as the reason the run needs
/// attention; every intent is inspected before a run ends.
pub(crate) fn open_intent(run_id: Ulid) -> RowsOperation<OpenIntent> {
    RowsOperation::direct(OpenIntent(intent_scan(run_id)))
}

#[derive(Debug, PartialEq)]
pub struct OpenIntent(Scan);

impl Plan for OpenIntent {
    type Output = Option<String>;
    type Error = StoreError;

    fn first(&mut self) -> Result<Turn<Self::Output>, StoreError> {
        Ok(Turn::Ask(self.0.ask()))
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<Self::Output>, StoreError> {
        match self.0.take(answer)? {
            None => Ok(Turn::Ask(self.0.ask())),
            Some(rows) => Ok(Turn::Finish(first_open(&rows)?)),
        }
    }
}

/// The reason of the first pending or unknown intent among `rows`.
pub(crate) fn first_open(rows: &Rows) -> Result<Option<String>, StoreError> {
    for (key, value) in rows {
        let ordinal = key.get(16..).map(<[u8]>::to_vec).unwrap_or_default();
        let ordinal = <[u8; 4]>::try_from(ordinal.as_slice()).map(u32::from_be_bytes);
        let ordinal = ordinal.map_or_else(|_| "?".to_string(), |ordinal| ordinal.to_string());
        match decode::<IntentRecord>(value)?.state {
            IntentState::Settled { .. } | IntentState::Resolved { .. } => {}
            IntentState::Pending => {
                return Ok(Some(format!("call {ordinal} may have had its effect")));
            }
            IntentState::Unknown { reason } => {
                return Ok(Some(format!(
                    "call {ordinal} has an unknown outcome: {reason}"
                )));
            }
        }
    }
    Ok(None)
}

/// A person's decision on every pending or unknown intent among a run's `rows`.
pub(crate) fn resolved_rows(rows: Rows, done: bool) -> Result<Vec<Row>, StoreError> {
    let mut resolved = Vec::new();
    for (key, value) in rows {
        let mut record = decode::<IntentRecord>(&value)?;
        if matches!(
            record.state,
            IntentState::Pending | IntentState::Unknown { .. }
        ) {
            record.state = IntentState::Resolved { done };
            resolved.push(row(PLUGIN_INTENTS_KEYSPACE, key, encode(&record)?));
        }
    }
    Ok(resolved)
}

/// The intents among a run's `rows` a person resolved as not done, removed so a retry makes
/// those calls again; the other intents, settled replies among them, stay.
pub(crate) fn undone_keys(rows: Rows) -> Result<Vec<(&'static str, Vec<u8>)>, StoreError> {
    let mut undone = Vec::new();
    for (key, value) in rows {
        let record = decode::<IntentRecord>(&value)?;
        if record.state == (IntentState::Resolved { done: false }) {
            undone.push((PLUGIN_INTENTS_KEYSPACE, key));
        }
    }
    Ok(undone)
}

/// The keys of every intent among a run's `rows`.
pub(crate) fn intent_keys(rows: Rows) -> Vec<(&'static str, Vec<u8>)> {
    let keys = rows
        .into_iter()
        .map(|(key, _)| (PLUGIN_INTENTS_KEYSPACE, key));
    keys.collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::drive;
    use crate::plugins::broker::call_intent;
    use crate::plugins::rows::{delete_rows, scan_rows, write_rows};
    use crate::plugins::runs::tests::opened;
    use aruna_plugins::broker::Intent;

    #[tokio::test]
    async fn intent_outcomes() {
        let (_dir, ctx) = opened();
        let run = Ulid::from_parts(5, 1);
        let first = Intent::new(0, &[b"patch a"]);
        let second = Intent::new(1, &[b"patch b"]);
        let begun = async |intent: &Intent| drive(begin(run, call_intent(intent)), &ctx).await;
        let open = async || drive(open_intent(run), &ctx).await.unwrap();
        assert_eq!(begun(&first).await.unwrap(), Begun::Fresh);
        let settled = settled_row(run, &call_intent(&first), "r2".into()).unwrap();
        drive(write_rows(vec![settled]), &ctx).await.unwrap();
        assert_eq!(open().await, None);
        assert_eq!(begun(&first).await.unwrap(), Begun::Settled("r2".into()));
        let other = Intent::new(0, &[b"patch c"]);
        assert_eq!(begun(&other).await.unwrap(), Begun::Changed);

        // A pending intent found by a later attempt is unknown and stays so.
        assert_eq!(begun(&second).await.unwrap(), Begun::Fresh);
        assert!(open().await.is_some());
        assert!(matches!(begun(&second).await, Ok(Begun::Unknown(_))));
        assert!(matches!(begun(&second).await, Ok(Begun::Unknown(_))));
        let third = Intent::new(2, &[b"put"]);
        let unknown = unknown_row(run, &call_intent(&third), "timed out").unwrap();
        drive(write_rows(vec![unknown]), &ctx).await.unwrap();
        assert!(matches!(begun(&third).await, Ok(Begun::Unknown(_))));

        let rows = vec![(b"k".to_vec(), Vec::new())];
        assert_eq!(
            intent_keys(rows),
            [(PLUGIN_INTENTS_KEYSPACE, b"k".to_vec())]
        );
        let all = drive(scan_rows(intent_scan(run)), &ctx);
        let keys = intent_keys(all.await.unwrap());
        assert_eq!(keys.len(), 3);
        drive(delete_rows(keys), &ctx).await.unwrap();
        assert_eq!(open().await, None);
        assert_eq!(begun(&first).await.unwrap(), Begun::Fresh);
    }

    #[test]
    fn begin_decisions() {
        let run = Ulid::from_parts(5, 1);
        let intent = call_intent(&Intent::new(0, &[b"patch a"]));
        let record = |digest, state| {
            let record = IntentRecord { digest, state };
            Answer::Values(vec![Some(encode(&record).unwrap())])
        };
        let mut plan = BeginIntent {
            run_id: run,
            intent: intent.clone(),
            begun: None,
        };
        let key = intent_key(run, 0);
        let read = Ask::Read(vec![(PLUGIN_INTENTS_KEYSPACE, key)]);
        assert_eq!(plan.first().unwrap(), Turn::Ask(read));
        let pending = intent_row(run, &intent, IntentState::Pending).unwrap();
        assert_eq!(
            plan.next(Answer::Values(vec![None])).unwrap(),
            Turn::Ask(Ask::Write(vec![pending]))
        );
        assert_eq!(plan.next(Answer::Done).unwrap(), Turn::Finish(Begun::Fresh));
        let settled = IntentState::Settled { reply: "r".into() };
        let changed = plan.next(record([9; 32], settled.clone())).unwrap();
        assert_eq!(changed, Turn::Finish(Begun::Changed));
        let again = plan.next(record(intent.digest, settled)).unwrap();
        assert_eq!(again, Turn::Finish(Begun::Settled("r".into())));
        let earlier = plan
            .next(record(intent.digest, IntentState::Pending))
            .unwrap();
        assert!(matches!(earlier, Turn::Ask(Ask::Write(_))));
        assert!(matches!(
            plan.next(Answer::Done),
            Ok(Turn::Finish(Begun::Unknown(_)))
        ));
        assert!(plan.next(Answer::Page(Vec::new(), None)).is_err());
    }

    #[test]
    fn resolutions() {
        let run = Ulid::from_parts(5, 1);
        let row = |ordinal, state| {
            let (_, key, value) =
                intent_row(run, &call_intent(&Intent::new(ordinal, &[])), state).unwrap();
            (key.to_vec(), value.to_vec())
        };
        let rows = vec![
            row(0, IntentState::Pending),
            row(
                1,
                IntentState::Settled {
                    reply: String::new(),
                },
            ),
            row(2, IntentState::Resolved { done: false }),
        ];
        let reason = first_open(&rows).unwrap().unwrap();
        assert_eq!(reason, "call 0 may have had its effect");
        assert_eq!(resolved_rows(rows.clone(), true).unwrap().len(), 1);
        let undone = undone_keys(rows.clone()).unwrap();
        assert_eq!(undone, [(PLUGIN_INTENTS_KEYSPACE, rows[2].0.clone())]);
        assert_eq!(first_open(&rows[1..].to_vec()).unwrap(), None);
    }
}
