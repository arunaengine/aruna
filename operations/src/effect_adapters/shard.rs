//! Carries out shard topic effects with the net handle.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::document::DocumentNetEvent;
use aruna_core::effects::{ShardMembership, ShardNetEffect};
use aruna_core::events::{Event, GenesisProbe, NetEvent, ShardNetEvent};
use aruna_net::NetHandle;
use tracing::debug;

use crate::driver::{DriverContext, drive};
use crate::sync::reconcile::SyncTopicsOperation;

pub(super) async fn dispatch_shard(effect: ShardNetEffect, context: &DriverContext) -> Event {
    let answer = match &context.net_handle {
        None => ShardNetEvent::NoNetwork,
        Some(net) => run_effect(effect, net, context).await,
    };
    Event::Net(NetEvent::Shard(Box::new(answer)))
}

fn outcome<E: std::fmt::Display>(result: Result<(), E>) -> ShardNetEvent {
    ShardNetEvent::Outcome(result.map_err(|error| error.to_string()))
}

async fn run_effect(
    effect: ShardNetEffect,
    net: &NetHandle,
    context: &DriverContext,
) -> ShardNetEvent {
    match effect {
        ShardNetEffect::Existing(topics) => ShardNetEvent::Existing(
            (topics.into_iter())
                .filter(|topic| net.sync_topic_exists(*topic).unwrap_or(false))
                .collect(),
        ),
        ShardNetEffect::Reopen(topics) => {
            let mut failed = 0;
            for topic in topics {
                if let Err(error) = net.reopen_sync_topic(topic) {
                    debug!(error = %error, "Failed to reopen a current shard holder topic");
                    failed += 1;
                }
            }
            ShardNetEvent::Reopened { failed }
        }
        ShardNetEffect::Close(topic) => ShardNetEvent::Closed(
            net.close_sync_topic(topic)
                .map_err(|error| error.to_string()),
        ),
        ShardNetEffect::Membership(ShardMembership {
            topics,
            members,
            publishers,
            retained,
            verified,
        }) => outcome(
            net.reconcile_shard_membership(&topics, members, publishers, &retained, &verified)
                .await,
        ),
        ShardNetEffect::Probe { topics, peers } => {
            let probe = net.probe_shard_geneses(topics, peers).await;
            ShardNetEvent::Probed(GenesisProbe {
                known_co_holder: probe.known_co_holder,
                unconfirmed: probe.unconfirmed,
                unreachable: probe.unreachable,
            })
        }
        ShardNetEffect::Pull { topics, peers } => {
            let sync = SyncTopicsOperation::new(*net.realm_id(), topics, peers);
            let synced = Box::pin(drive(sync, context)).await;
            ShardNetEvent::Pulled {
                node_id: net.node_id(),
                realm_id: *net.realm_id(),
                sync: Box::new(DocumentNetEvent::reconciled(synced)),
            }
        }
        ShardNetEffect::EnsureLocal(topics) => outcome(net.ensure_local_topics(&topics).await),
        ShardNetEffect::EnsureShared { topics, peers } => {
            outcome(net.ensure_sync_topics(&topics, peers).await)
        }
        ShardNetEffect::EvictionPending(placement) => {
            ShardNetEvent::Flag(net.eviction_pending(&placement))
        }
        ShardNetEffect::TopicFacts(topic) => {
            let (digest, cursor) = net.shard_topic_facts(topic);
            ShardNetEvent::Facts {
                holder: net.node_id(),
                digest,
                cursor,
            }
        }
        ShardNetEffect::FrontierRoot { topic, frontier } => {
            ShardNetEvent::Root(net.shard_frontier_root(topic, &frontier))
        }
        ShardNetEffect::SignProof(claim) => {
            ShardNetEvent::Proof(claim.signed_with(|message| net.sign(message)))
        }
        ShardNetEffect::FetchManifest {
            peer,
            realm_id,
            placement,
        } => ShardNetEvent::Manifest(
            net.fetch_shard_manifest(peer, realm_id, placement)
                .await
                .map(Box::new),
        ),
    }
}
