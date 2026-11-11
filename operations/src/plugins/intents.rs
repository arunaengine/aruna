//! Intents of mutating plugin calls, written before their effects. A settled call answers its
//! stored reply again; a call whose effect may have happened unrecorded is unknown, never resent.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::keyspaces::PLUGIN_INTENTS_KEYSPACE;
use aruna_plugins::broker::Intent;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::store::{StoreError, Txn, decode, encode, transact};
use crate::driver::DriverContext;

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
pub async fn begin(
    ctx: &DriverContext,
    run_id: Ulid,
    intent: &Intent,
) -> Result<Begun, StoreError> {
    let key = intent_key(run_id, intent.ordinal);
    transact(ctx, async |txn: &Txn<'_>| {
        let found = match txn.read(PLUGIN_INTENTS_KEYSPACE, &key).await? {
            Some(bytes) => Some(decode::<IntentRecord>(&bytes)?),
            None => None,
        };
        let (begun, state) = match found {
            None => (Begun::Fresh, IntentState::Pending),
            Some(record) if record.digest != intent.digest => return Ok(Begun::Changed),
            Some(record) => match record.state {
                IntentState::Settled { reply } => return Ok(Begun::Settled(reply)),
                IntentState::Unknown { reason } => return Ok(Begun::Unknown(reason)),
                IntentState::Pending => {
                    let reason = "an earlier attempt of this call did not finish".to_string();
                    (
                        Begun::Unknown(reason.clone()),
                        IntentState::Unknown { reason },
                    )
                }
            },
        };
        let record = IntentRecord {
            digest: intent.digest,
            state,
        };
        txn.write(
            PLUGIN_INTENTS_KEYSPACE,
            vec![(key.clone(), encode(&record)?)],
        )
        .await?;
        Ok(begun)
    })
    .await
}

/// Whether an earlier attempt of the run already wrote this call's intent.
pub async fn written(
    ctx: &DriverContext,
    run_id: Ulid,
    intent: &Intent,
) -> Result<bool, StoreError> {
    let key = intent_key(run_id, intent.ordinal);
    let row = Txn::direct(ctx).read(PLUGIN_INTENTS_KEYSPACE, &key).await?;
    Ok(row.is_some())
}

/// Records the call's reply, in the transaction that commits its effect where it has one.
pub async fn settle_in(
    txn: &Txn<'_>,
    run_id: Ulid,
    intent: &Intent,
    reply: String,
) -> Result<(), StoreError> {
    let record = IntentRecord {
        digest: intent.digest,
        state: IntentState::Settled { reply },
    };
    let key = intent_key(run_id, intent.ordinal);
    txn.write(PLUGIN_INTENTS_KEYSPACE, vec![(key, encode(&record)?)])
        .await
}

/// Removes the intent of a call that ended without an effect, so it may be made again.
pub async fn clear_in(txn: &Txn<'_>, run_id: Ulid, intent: &Intent) -> Result<(), StoreError> {
    let key = intent_key(run_id, intent.ordinal);
    txn.delete(PLUGIN_INTENTS_KEYSPACE, vec![key]).await
}

/// Marks a call whose effect may have happened without a recorded reply.
pub async fn mark_unknown(
    ctx: &DriverContext,
    run_id: Ulid,
    intent: &Intent,
    reason: &str,
) -> Result<(), StoreError> {
    let record = IntentRecord {
        digest: intent.digest,
        state: IntentState::Unknown {
            reason: reason.to_string(),
        },
    };
    let key = intent_key(run_id, intent.ordinal);
    (Txn::direct(ctx))
        .write(PLUGIN_INTENTS_KEYSPACE, vec![(key, encode(&record)?)])
        .await
}

/// The first intent of the run that is pending or unknown, as the reason the run needs
/// attention; every intent is inspected before a run ends.
pub async fn open_intent(ctx: &DriverContext, run_id: Ulid) -> Result<Option<String>, StoreError> {
    let rows = (Txn::direct(ctx))
        .scan(PLUGIN_INTENTS_KEYSPACE, &run_id.to_bytes())
        .await?;
    for (key, value) in rows {
        let ordinal = key.get(16..).map(<[u8]>::to_vec).unwrap_or_default();
        let ordinal = <[u8; 4]>::try_from(ordinal.as_slice()).map(u32::from_be_bytes);
        let ordinal = ordinal.map_or_else(|_| "?".to_string(), |ordinal| ordinal.to_string());
        match decode::<IntentRecord>(&value)?.state {
            IntentState::Settled { .. } => {}
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

/// Removes every intent of a run.
pub async fn drop_intents(txn: &Txn<'_>, run_id: Ulid) -> Result<(), StoreError> {
    let rows = txn
        .scan(PLUGIN_INTENTS_KEYSPACE, &run_id.to_bytes())
        .await?;
    let keys = rows.into_iter().map(|(key, _)| key).collect();
    txn.delete(PLUGIN_INTENTS_KEYSPACE, keys).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugins::runs::tests::opened;

    #[tokio::test]
    async fn intent_outcomes() {
        let (_dir, ctx) = opened();
        let run = Ulid::from_parts(5, 1);
        let first = Intent::new(0, None, &[b"patch a"]);
        let second = Intent::new(1, None, &[b"patch b"]);
        assert_eq!(begin(&ctx, run, &first).await.unwrap(), Begun::Fresh);
        let settle = async |txn: &Txn<'_>| settle_in(txn, run, &first, "r2".into()).await;
        transact(&ctx, settle).await.unwrap();
        assert_eq!(open_intent(&ctx, run).await.unwrap(), None);
        assert_eq!(
            begin(&ctx, run, &first).await.unwrap(),
            Begun::Settled("r2".into())
        );
        let other = Intent::new(0, None, &[b"patch c"]);
        assert_eq!(begin(&ctx, run, &other).await.unwrap(), Begun::Changed);

        // A pending intent found by a later attempt is unknown and stays so.
        assert_eq!(begin(&ctx, run, &second).await.unwrap(), Begun::Fresh);
        assert!(open_intent(&ctx, run).await.unwrap().is_some());
        assert!(matches!(
            begin(&ctx, run, &second).await,
            Ok(Begun::Unknown(_))
        ));
        assert!(matches!(
            begin(&ctx, run, &second).await,
            Ok(Begun::Unknown(_))
        ));
        let third = Intent::new(2, None, &[b"put"]);
        mark_unknown(&ctx, run, &third, "timed out").await.unwrap();
        assert!(matches!(
            begin(&ctx, run, &third).await,
            Ok(Begun::Unknown(_))
        ));

        transact(&ctx, async |txn: &Txn<'_>| drop_intents(txn, run).await)
            .await
            .unwrap();
        assert_eq!(open_intent(&ctx, run).await.unwrap(), None);
        assert_eq!(begin(&ctx, run, &first).await.unwrap(), Begun::Fresh);
    }
}
