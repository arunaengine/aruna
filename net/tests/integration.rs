use std::time::Duration;

use aruna_core::TopicId;
use aruna_core::effects::{DhtEffect, Effect, GossipEffect, NetEffect};
use aruna_core::events::{DhtEvent, Event, GossipEvent, NetEvent};
use aruna_core::handle::Handle;
use aruna_core::id::DhtKeyId;
use aruna_net::{NetConfig, NetHandle};
use aruna_storage::FjallStorage;
use tempfile::tempdir;
use ulid::Ulid;

#[tokio::test]
async fn test_dht_put_get() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let storage = FjallStorage::open(temp_dir.path().to_str().unwrap())?;
    let handle = NetHandle::new(NetConfig::default(), storage).await?;

    let key = [42u8; 32];
    let value = b"test-value".to_vec();

    // Put
    let effect = Effect::Net(NetEffect::Dht(DhtEffect::Put {
        key,
        value: value.clone(),
        ttl: Duration::from_secs(3600),
    }));

    let event = handle.send_effect(effect).await;
    assert!(matches!(
        event,
        Event::Net(NetEvent::Dht(DhtEvent::PutComplete { .. }))
    ));

    // Get
    let effect = Effect::Net(NetEffect::Dht(DhtEffect::Get { key }));
    let event = handle.send_effect(effect).await;

    if let Event::Net(NetEvent::Dht(DhtEvent::GetResult { values, .. })) = event {
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].value, value);
    } else {
        panic!("Expected GetResult, got {:?}", event);
    }

    Ok(())
}

#[tokio::test]
async fn test_gossip_subscribe_unsubscribe() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let storage = FjallStorage::open(temp_dir.path().to_str().unwrap())?;
    let handle = NetHandle::new(NetConfig::default(), storage).await?;

    let topic = TopicId::realm(Ulid::new());

    // Subscribe
    let effect = Effect::Net(NetEffect::Gossip(GossipEffect::Subscribe {
        topic: topic.clone(),
    }));
    let event = handle.send_effect(effect).await;
    assert!(matches!(
        event,
        Event::Net(NetEvent::Gossip(GossipEvent::Subscribed { .. }))
    ));

    // Unsubscribe
    let effect = Effect::Net(NetEffect::Gossip(GossipEffect::Unsubscribe { topic }));
    let event = handle.send_effect(effect).await;
    assert!(matches!(
        event,
        Event::Net(NetEvent::Gossip(GossipEvent::Unsubscribed { .. }))
    ));

    Ok(())
}

#[tokio::test]
async fn test_node_id() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let storage = FjallStorage::open(temp_dir.path().to_str().unwrap())?;
    let handle = NetHandle::new(NetConfig::default(), storage).await?;

    let node_id = handle.node_id();
    // Node ID should be non-zero (it's derived from the secret key)
    assert!(node_id.as_bytes().iter().any(|&b| b != 0));

    Ok(())
}

#[test]
fn test_dht_key_id_hashing() {
    // Test that DhtKeyId::from_data produces consistent hashes
    let key1 = DhtKeyId::from_data(b"test-key");
    let key2 = DhtKeyId::from_data(b"test-key");
    let key3 = DhtKeyId::from_data(b"different-key");

    // Same input produces same hash
    assert_eq!(key1, key2);
    assert_eq!(key1.as_bytes(), key2.as_bytes());

    // Different input produces different hash
    assert_ne!(key1, key3);

    // Hash is non-zero
    assert!(key1.as_bytes().iter().any(|&b| b != 0));

    // Test from_bytes round-trip
    let bytes = *key1.as_bytes();
    let key4 = DhtKeyId::from_bytes(bytes);
    assert_eq!(key1, key4);
}

#[test]
fn test_topic_id_creation() {
    // Test TopicId creation via enum variants
    let realm_id = Ulid::new();
    let topic1 = TopicId::realm(realm_id);

    // Test roundtrip via to_bytes/from_bytes
    let bytes = topic1.to_bytes();
    let topic2 = TopicId::from_bytes(&bytes).unwrap();
    assert_eq!(topic1, topic2);

    // Test custom topics
    let topic3 = TopicId::custom(b"my-topic".to_vec());
    let topic4 = TopicId::custom(b"my-topic".to_vec());
    let topic5 = TopicId::custom(b"other-topic".to_vec());

    // Same input produces same topic
    assert_eq!(topic3, topic4);

    // Different input produces different topic
    assert_ne!(topic3, topic5);

    // Test conversion to iroh topic (hashes to 32 bytes)
    let iroh_topic = topic1.to_iroh_topic();
    assert!(iroh_topic.as_bytes().iter().any(|&b| b != 0));
}
