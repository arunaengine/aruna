use std::{cmp::Reverse, collections::BinaryHeap, time::SystemTime};

use iroh::NodeId;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Key {
    // TODO: Use chrono::UTC instead of SystemTime for better time handling
    created: SystemTime,
    key: [u8; 32],
    node_id: Option<NodeId>,
}

impl Key {
    pub fn new(key: [u8; 32], node_id: Option<NodeId>) -> Self {
        Self {
            created: SystemTime::now(),
            key,
            node_id,
        }
    }

    #[allow(dead_code)]
    fn with_timestamp(key: [u8; 32], node_id: Option<NodeId>, timestamp: SystemTime) -> Self {
        Self {
            created: timestamp,
            key,
            node_id,
        }
    }

    pub fn key(&self) -> [u8; 32] {
        self.key
    }

    pub fn node_id(&self) -> Option<NodeId> {
        self.node_id
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Primary sort by creation time (oldest first)
        self.created
            .cmp(&other.created)
            // Secondary sort by key bytes for consistency
            .then_with(|| self.key.cmp(&other.key))
            .then_with(|| self.node_id.cmp(&other.node_id))
    }
}

#[derive(Debug)]
pub struct TimeHandler {
    // Using Reverse<Key> to make it a min-heap (oldest first)
    heap: BinaryHeap<Reverse<Key>>,
}

impl TimeHandler {
    pub fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
        }
    }

    // Insert a new key with current timestamp
    pub fn insert(&mut self, key: [u8; 32], node_id: Option<NodeId>) {
        let key_obj = Key::new(key, node_id);
        self.heap.push(Reverse(key_obj));
    }

    // Insert a key with a specific timestamp
    #[allow(dead_code)]
    pub fn insert_with_timestamp(
        &mut self,
        key: [u8; 32],
        node_id: Option<NodeId>,
        timestamp: SystemTime,
    ) {
        let key_obj = Key::with_timestamp(key, node_id, timestamp);
        self.heap.push(Reverse(key_obj));
    }

    // Remove and return all keys older than the threshold
    pub fn remove_older_than(&mut self, threshold: SystemTime) -> Vec<Key> {
        let mut result = Vec::new();

        // Keep popping while the oldest key is older than the threshold
        while let Some(key) = self.peek_oldest() {
            if key.created < threshold {
                // If it's older, pop it and add to results
                if let Some(popped) = self.pop_oldest() {
                    result.push(popped);
                }
            } else {
                // As soon as we encounter a key that's not older, we can stop
                break;
            }
        }

        result
    }

    // Peek at the oldest key
    pub fn peek_oldest(&self) -> Option<&Key> {
        self.heap.peek().map(|Reverse(key)| key)
    }

    // Remove and return the oldest key
    pub fn pop_oldest(&mut self) -> Option<Key> {
        self.heap.pop().map(|Reverse(key)| key)
    }

    // Get number of keys
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.heap.len()
    }

    // Check if empty
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{thread::sleep, time::Duration};

    fn create_test_key(value: u8) -> [u8; 32] {
        let mut key = [0u8; 32];
        key[0] = value;
        key
    }

    #[test]
    fn test_new() {
        let handler = TimeHandler::new();
        assert!(handler.is_empty());
        assert_eq!(handler.len(), 0);
        assert!(handler.peek_oldest().is_none());
    }

    #[test]
    fn test_insert_and_peek() {
        let mut handler = TimeHandler::new();
        let key = create_test_key(1);

        handler.insert(key, None);
        assert_eq!(handler.len(), 1);
        assert!(!handler.is_empty());

        let peeked = handler.peek_oldest().unwrap();
        assert_eq!(peeked.key, key);

        // Peek doesn't remove
        assert_eq!(handler.len(), 1);
    }

    #[test]
    fn test_pop_oldest() {
        let mut handler = TimeHandler::new();

        // Insert multiple keys
        let key1 = create_test_key(1);
        let key2 = create_test_key(2);

        // Insert key2 first (older)
        handler.insert(key2, None);
        sleep(Duration::from_millis(10)); // Ensure different timestamps
        handler.insert(key1, None);

        // Should pop key2 first (the oldest)
        let popped = handler.pop_oldest().unwrap();
        assert_eq!(popped.key, key2);

        // Should have key1 left
        assert_eq!(handler.len(), 1);

        // Pop again to get key1
        let popped = handler.pop_oldest().unwrap();
        assert_eq!(popped.key, key1);

        // Now empty
        assert!(handler.is_empty());
        assert!(handler.pop_oldest().is_none());
    }

    #[test]
    fn test_remove_older_than() {
        let mut handler = TimeHandler::new();

        let key1 = create_test_key(1);
        let key2 = create_test_key(2);
        let key3 = create_test_key(3);

        let now = SystemTime::now();
        let past1 = now - Duration::from_secs(10);
        let past2 = now - Duration::from_secs(20);
        let future = now + Duration::from_secs(10);

        // Insert with different timestamps
        handler.insert_with_timestamp(key1, None, past1);
        handler.insert_with_timestamp(key2, None, past2); // oldest
        handler.insert_with_timestamp(key3, None, now);

        // Remove older than 'past1'
        let removed = handler.remove_older_than(past1);
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].key, key2);

        // Should have key1 and key3 remaining
        assert_eq!(handler.len(), 2);

        // Remove older than 'now'
        let removed = handler.remove_older_than(now);
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].key, key1);

        // Should have only key3 remaining
        assert_eq!(handler.len(), 1);

        // Nothing older than past2
        let removed = handler.remove_older_than(past2);
        assert_eq!(removed.len(), 0);
        assert_eq!(handler.len(), 1);

        // Everything older than future
        let removed = handler.remove_older_than(future);
        assert_eq!(removed.len(), 1);
        assert!(handler.is_empty());
    }

    #[test]
    fn test_reinsert_with_fresh_timestamp() {
        let mut handler = TimeHandler::new();

        let key1 = create_test_key(1);
        let key2 = create_test_key(2);

        let old_time = SystemTime::now() - Duration::from_secs(100);

        // Insert with old timestamp
        handler.insert_with_timestamp(key1, None, old_time);
        handler.insert_with_timestamp(key2, None, old_time);

        // Remove old keys
        let threshold = SystemTime::now() - Duration::from_secs(50);
        let removed = handler.remove_older_than(threshold);
        assert_eq!(removed.len(), 2);
        assert!(handler.is_empty());

        // Reinsert with fresh timestamp
        assert_eq!(handler.len(), 2);

        // Now they shouldn't be removed by the same threshold
        let newly_removed = handler.remove_older_than(threshold);
        assert_eq!(newly_removed.len(), 0);
        assert_eq!(handler.len(), 2);
    }

    #[test]
    fn test_multiple_keys_same_timestamp() {
        let mut handler = TimeHandler::new();

        let timestamp = SystemTime::now();

        // Insert multiple keys with the same timestamp
        let key1 = create_test_key(1);
        let key2 = create_test_key(2);
        let key3 = create_test_key(3);

        handler.insert_with_timestamp(key1, None, timestamp);
        handler.insert_with_timestamp(key2, None, timestamp);
        handler.insert_with_timestamp(key3, None, timestamp);

        // They should come out in deterministic order based on key bytes and is_node
        let popped1 = handler.pop_oldest().unwrap();
        let popped2 = handler.pop_oldest().unwrap();
        let popped3 = handler.pop_oldest().unwrap();

        // Should be ordered by key bytes since timestamps are equal
        assert_eq!(popped1.key[0], 1);
        assert_eq!(popped2.key[0], 2);
        assert_eq!(popped3.key[0], 3);
    }

    #[test]
    fn test_ordering() {
        let mut handler = TimeHandler::new();

        let now = SystemTime::now();
        let older = now - Duration::from_secs(10);
        let oldest = now - Duration::from_secs(20);

        let key1 = create_test_key(1);
        let key2 = create_test_key(2);
        let key3 = create_test_key(3);

        // Insert in reverse chronological order
        handler.insert_with_timestamp(key1, None, now);
        handler.insert_with_timestamp(key2, None, older);
        handler.insert_with_timestamp(key3, None, oldest);

        // Should pop in chronological order (oldest first)
        assert_eq!(handler.pop_oldest().unwrap().key[0], 3);
        assert_eq!(handler.pop_oldest().unwrap().key[0], 2);
        assert_eq!(handler.pop_oldest().unwrap().key[0], 1);
    }
}
