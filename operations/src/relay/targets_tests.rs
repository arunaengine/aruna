//! Tests which nodes a relayed call goes to, call by call.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;
use aruna_core::structs::identity::realm::RealmNode;

use super::*;

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn nodes(urls: &[(u8, &str)], managers: &[u8]) -> RealmNodes {
    let mut config = RealmConfigDocument::default_for_realm(realm(), Vec::new());
    config.nodes = (managers.iter())
        .map(|seed| RealmNode {
            node_id: node(*seed).to_string(),
            kind: RealmNodeKind::Management,
        })
        .collect();
    RealmNodes {
        config,
        urls: (urls.iter())
            .map(|(seed, url)| (node(*seed), url.to_string()))
            .collect(),
    }
}

fn found(nodes: RealmNodes) -> IoResult {
    IoResult::Child(ChildOutcome::new(Ok::<_, TargetError>(nodes)))
}

fn installed(urls: &[&str]) -> IoResult {
    let urls: Vec<String> = urls.iter().map(|url| url.to_string()).collect();
    IoResult::Child(ChildOutcome::new(Ok::<_, IoError>(urls)))
}

#[test]
fn uses_installed_urls() {
    // A device resolves no peer from node-info documents it never holds.
    let installed = vec!["https://mgmt.example.test/api/v1".to_string()];
    assert_eq!(relay_targets(Vec::new(), installed.clone()), installed);
    let peers = vec!["https://peer.example.test/api/v1".to_string()];
    assert_eq!(relay_targets(peers.clone(), installed), peers);
    assert!(relay_targets(Vec::new(), Vec::new()).is_empty());
}

#[test]
fn peers_skip_self() {
    let known = nodes(
        &[(1, "http://one"), (2, "http://two"), (3, "http://two")],
        &[1, 2, 3, 4],
    );
    assert_eq!(peer_management_urls(node(1), &known), ["http://two"]);
}

#[test]
fn peers_beat_installed() {
    let mut peers = PeerTargetsOperation::new(realm(), node(1));
    peers.first().unwrap();
    let known = nodes(&[(2, "http://two")], &[1, 2]);
    assert!(matches!(peers.next(found(known)), Ok(Next::Call(_))));
    let answer = peers.next(installed(&["http://installed"]));
    assert_eq!(answer, Ok(Next::Done(vec!["http://two".to_string()])));
}

#[test]
fn holder_is_first() {
    let mut here = HolderTargetOperation::new(realm(), node(1));
    here.first().unwrap();
    let known = nodes(&[(2, "http://two")], &[1, 2]);
    assert_eq!(here.next(found(known)), Ok(Next::Done(None)));
    let mut peer = HolderTargetOperation::new(realm(), node(2));
    peer.first().unwrap();
    let known = nodes(&[(1, "http://one")], &[1, 2]);
    let target = Some((Some(node(1)), "http://one".to_string()));
    assert_eq!(peer.next(found(known)), Ok(Next::Done(target)));
}

#[test]
fn device_asks_installed() {
    let mut device = HolderTargetOperation::new(realm(), node(9));
    device.first().unwrap();
    assert!(matches!(
        device.next(found(nodes(&[], &[]))),
        Ok(Next::Call(_))
    ));
    let target = Some((None, "http://installed".to_string()));
    let answer = device.next(installed(&["http://installed"]));
    assert_eq!(answer, Ok(Next::Done(target)));
    let mut none = HolderTargetOperation::new(realm(), node(9));
    none.first().unwrap();
    none.next(found(nodes(&[], &[]))).unwrap();
    assert_eq!(none.next(installed(&[])), Err(TargetError::NoNode));
}

#[test]
fn unpinned_coordinator_managers() {
    let config = |seed| {
        IoResult::Child(ChildOutcome::new(Ok::<_, GetConfigError>(
            nodes(&[], &[seed]).config,
        )))
    };
    let mut manager = CoordinatorTargetOperation::new(realm(), node(1), true);
    manager.first().unwrap();
    assert!(matches!(manager.next(config(1)), Ok(Next::Call(_))));
    let empty = IoResult::Values(vec![None]);
    assert_eq!(manager.next(empty), Ok(Next::Done(CoordinatorTarget::Here)));
    let mut server = CoordinatorTargetOperation::new(realm(), node(2), false);
    server.first().unwrap();
    server.next(config(1)).unwrap();
    let empty = IoResult::Values(vec![None]);
    let answer = server.next(empty);
    assert_eq!(answer, Ok(Next::Done(CoordinatorTarget::Managers)));
}
