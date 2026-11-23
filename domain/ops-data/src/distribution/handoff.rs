//! Plans the end of a counter rebuild: a range moves its charge to the new generation, and
//! a split hands both child totals over once both child rebuilds reached the end.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::{RANGE_ACCOUNT_KEYSPACE, RANGE_COUNTER_KEYSPACE};
use aruna_core::structs::storage::distribution::OriginalEpoch;
use aruna_core::structs::storage::distribution::accounting::{
    Charge, RangeAccount, RangeCounters, Rebuild, account_key, generation_key, pending_key,
};
use aruna_core::types::{Key, Value};
use thiserror::Error;
use ulid::Ulid;

#[derive(Debug, Error, PartialEq)]
pub enum HandoffError {
    #[error("range {0} has no account row")]
    RangeMissing(Ulid),
    #[error("range {0} was split; its children are rebuilt instead")]
    RangeRetired(Ulid),
    #[error("range {range_id} has an inconsistent account: {reason}")]
    Invalid {
        range_id: Ulid,
        reason: &'static str,
    },
    #[error(transparent)]
    Conversion(#[from] ConversionError),
}

/// The rows one activation transaction writes and deletes, and the epoch maximum it carries
/// into each new generation row.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Activation {
    pub writes: Vec<(String, Key, Value)>,
    pub deletes: Vec<(String, Key)>,
    pub counters: Vec<(Key, RangeCounters)>,
}

pub fn decode_account(value: Option<&Value>, range_id: Ulid) -> Result<RangeAccount, HandoffError> {
    let value = value.ok_or(HandoffError::RangeMissing(range_id))?;
    Ok(RangeAccount::from_bytes(value)?)
}

pub fn account_row(
    set_id: Ulid,
    range_id: Ulid,
    account: &RangeAccount,
) -> Result<(String, Key, Value), ConversionError> {
    Ok((
        RANGE_ACCOUNT_KEYSPACE.to_string(),
        account_key(set_id, range_id).into(),
        account.to_bytes()?.into(),
    ))
}

/// The rows an activation reads: the range's account and its old counter row, or for a
/// split child the sibling and parent accounts, the parent's frozen row and both pending rows.
pub fn activation_reads(
    set_id: Ulid,
    range_id: Ulid,
    account: &RangeAccount,
) -> Vec<(String, Key)> {
    let row = |key_space: &str, key: Vec<u8>| (key_space.to_string(), Key::from(key));
    let accounts = |range| row(RANGE_ACCOUNT_KEYSPACE, account_key(set_id, range));
    let counters = |key| row(RANGE_COUNTER_KEYSPACE, key);
    match account.charge {
        Charge::Active(old) => vec![
            accounts(range_id),
            counters(generation_key(set_id, range_id, old)),
        ],
        Charge::Child {
            parent,
            sibling,
            frozen,
        } => vec![
            accounts(range_id),
            accounts(sibling),
            accounts(parent),
            counters(generation_key(set_id, parent, frozen)),
            counters(pending_key(set_id, range_id)),
            counters(pending_key(set_id, sibling)),
        ],
        Charge::Retired { .. } => vec![accounts(range_id)],
    }
}

fn epoch_of(value: Option<Value>) -> Result<OriginalEpoch, HandoffError> {
    let counters = value
        .map(|value| RangeCounters::from_bytes(&value))
        .transpose()?;
    Ok(counters.unwrap_or_default().max_epoch)
}

/// A zero delta that only raises a new row's epoch maximum to `epoch`.
fn carried(key: Vec<u8>, epoch: OriginalEpoch) -> (Key, RangeCounters) {
    let counters = RangeCounters {
        max_epoch: epoch,
        ..RangeCounters::default()
    };
    (key.into(), counters)
}

fn activated(account: RangeAccount, rebuild: &Rebuild) -> RangeAccount {
    RangeAccount {
        charge: Charge::Active(rebuild.generation),
        rebuild: None,
        ..account
    }
}

/// Plans the activation from the rows [`activation_reads`] named, in that order. `None`
/// while a split child's sibling still walks; the sibling's activation does the handoff.
pub fn plan_activation(
    set_id: Ulid,
    range_id: Ulid,
    values: Vec<Option<Value>>,
) -> Result<Option<Activation>, HandoffError> {
    let invalid = |reason| HandoffError::Invalid { range_id, reason };
    let mut values = values.into_iter();
    let account = decode_account(values.next().flatten().as_ref(), range_id)?;
    let rebuild = account
        .rebuild
        .clone()
        .ok_or(invalid("no rebuild to activate"))?;
    if !rebuild.is_done() {
        return Err(invalid("the rebuild has not reached the end"));
    }
    let counters = |key: Vec<u8>| (RANGE_COUNTER_KEYSPACE.to_string(), Key::from(key));
    match account.charge {
        Charge::Active(old) => {
            let epoch = epoch_of(values.next().flatten())?;
            let new = generation_key(set_id, range_id, rebuild.generation);
            Ok(Some(Activation {
                writes: vec![account_row(
                    set_id,
                    range_id,
                    &activated(account, &rebuild),
                )?],
                deletes: vec![counters(generation_key(set_id, range_id, old))],
                counters: vec![carried(new, epoch)],
            }))
        }
        Charge::Child {
            parent, sibling, ..
        } => {
            let sibling_account = decode_account(values.next().flatten().as_ref(), sibling)?;
            let parent_account = decode_account(values.next().flatten().as_ref(), parent)?;
            let frozen = epoch_of(values.next().flatten())?;
            let own = frozen.max(epoch_of(values.next().flatten())?);
            let other = frozen.max(epoch_of(values.next().flatten())?);
            let sibling_rebuild = match (&sibling_account.charge, &sibling_account.rebuild) {
                (Charge::Child { parent: other, .. }, Some(done)) if *other == parent => {
                    done.clone()
                }
                _ => return Err(invalid("the sibling is no child of the same parent")),
            };
            if !sibling_rebuild.is_done() {
                return Ok(None);
            }
            let Charge::Retired { generation, .. } = parent_account.charge else {
                return Err(invalid("the parent is not retired"));
            };
            let parent_row = (
                RANGE_ACCOUNT_KEYSPACE.to_string(),
                account_key(set_id, parent).into(),
            );
            let carry = vec![
                carried(generation_key(set_id, range_id, rebuild.generation), own),
                carried(
                    generation_key(set_id, sibling, sibling_rebuild.generation),
                    other,
                ),
            ];
            Ok(Some(Activation {
                counters: carry,
                writes: vec![
                    account_row(set_id, range_id, &activated(account, &rebuild))?,
                    account_row(
                        set_id,
                        sibling,
                        &activated(sibling_account, &sibling_rebuild),
                    )?,
                ],
                deletes: vec![
                    parent_row,
                    counters(generation_key(set_id, parent, generation)),
                    counters(pending_key(set_id, range_id)),
                    counters(pending_key(set_id, sibling)),
                ],
            }))
        }
        Charge::Retired { .. } => Err(HandoffError::RangeRetired(range_id)),
    }
}
