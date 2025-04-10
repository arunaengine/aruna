/// XOR distance calculation for Kademlia
///
/// Calculates the XOR distance between two 32-byte IDs
pub fn calculate_distance(a: &[u8; 32], b: &[u8; 32]) -> [u8; 32] {
    let mut result = [0u8; 32];
    for i in 0..32 {
        result[i] = a[i] ^ b[i]; // XOR for distance in Kademlia
    }
    result
}

/// Find the position of the first set bit in a distance
///
/// Returns the index of the first non-zero bit (0-255)
pub fn get_bucket_index(distance: &[u8; 32]) -> usize {
    // Find the index of the first non-zero bit in the distance
    for (i, _) in distance.iter().enumerate() {
        let byte = distance[i];
        if byte != 0 {
            // Find the position of the first set bit in this byte
            for j in 0..8 {
                if (byte & (1 << (7 - j))) != 0 {
                    return i * 8 + j;
                }
            }
        }
    }

    // If distance is 0 (same node), use last bucket
    255
}
