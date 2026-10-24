//! Plans the end of a counter rebuild: a range moves its charge to the new generation, and
//! a split hands both child totals over once both child rebuilds reached the end.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::{RANGE_ACCOUNT_KEYSPACE, RANGE_COUNTER_KEYSPACE};
use aruna_core::structs::storage::distribution::accounting::{
    Charge, RangeAccount, Rebuild, account_key, generation_key, pending_key,
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

/// The rows one activation transaction writes and deletes.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Activation {
    pub writes: Vec<(String, Key, Value)>,
    pub deletes: Vec<(String, Key)>,
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

/// The account rows an activation reads: the range's own, then a child's sibling and parent.
pub fn activation_reads(
    set_id: Ulid,
    range_id: Ulid,
    account: &RangeAccount,
) -> Vec<(String, Key)> {
    let mut ranges = vec![range_id];
    if let Charge::Child { parent, sibling } = account.charge {
        ranges.extend([sibling, parent]);
    }
    ranges
        .into_iter()
        .map(|range| {
            (
                RANGE_ACCOUNT_KEYSPACE.to_string(),
                account_key(set_id, range).into(),
            )
        })
        .collect()
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
        Charge::Active(old) => Ok(Some(Activation {
            writes: vec![account_row(
                set_id,
                range_id,
                &activated(account, &rebuild),
            )?],
            deletes: vec![counters(generation_key(set_id, range_id, old))],
        })),
        Charge::Child { parent, sibling } => {
            let sibling_account = decode_account(values.next().flatten().as_ref(), sibling)?;
            let parent_account = decode_account(values.next().flatten().as_ref(), parent)?;
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
            Ok(Some(Activation {
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
