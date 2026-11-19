//! Builds storage-only driver contexts, a local net config, a frozen clock and an RO-Crate import
//! job record for the driver, job runtime and node tests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::id::NodeId;
use aruna_core::structs::execution::job::{
    ImportMetadataTarget, ImportRoCrateSource, ImportRoCrateSpec, ImportRoCrateTarget, JobId,
    JobPayload, JobRecord, RoCrateLimits,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_net::{DiscoveryMethod, NetConfig, RelayMethod};
use aruna_storage::StorageHandle;
use ulid::Ulid;

use crate::driver::DriverContext;

/// An upload import into bucket `target` under `crate`, owned by a fixed user.
pub fn import_record(job_id: JobId, node: NodeId) -> JobRecord {
    let owner = UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32]));
    JobRecord::new(
        job_id,
        JobPayload::ImportRoCrate(ImportRoCrateSpec {
            auth_context: AuthContext {
                user_id: owner,
                realm_id: RealmId([1u8; 32]),
                path_restrictions: None,
                session: None,
            },
            source: ImportRoCrateSource::Upload {
                upload_id: Ulid::from_bytes([3u8; 16]),
            },
            target: ImportRoCrateTarget {
                bucket: "target".to_string(),
                prefix: "crate".to_string(),
            },
            metadata: ImportMetadataTarget {
                group_id: Ulid::from_bytes([4u8; 16]),
                path: "crate".to_string(),
                public: false,
            },
            limits: RoCrateLimits::default(),
            document_id: Ulid::from_bytes([5u8; 16]),
        }),
        owner,
        node,
        1,
        1,
        None,
    )
}

/// A driver context with storage and no other plane.
pub fn test_context(storage: StorageHandle) -> DriverContext {
    DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    }
}

/// A net on loopback without relays or discovery, so a test reaches no outside service. A relay
/// connection's timers also read the real clock and spin forever under paused time.
pub fn local_net() -> NetConfig {
    NetConfig {
        bind_addr: "127.0.0.1:0".parse().expect("loopback address"),
        discovery_method: DiscoveryMethod::None,
        relay_method: RelayMethod::None,
        ..NetConfig::default()
    }
}

/// Tokio inhibits paused-clock auto-advance while a blocking task is alive.
/// Storage and net answer from their own threads, so without this the clock
/// races ahead of every round trip. `tokio::time::advance` still applies.
pub struct ClockGuard {
    _stop: std::sync::mpsc::Sender<()>,
}

pub fn freeze_clock() -> ClockGuard {
    let (stop, wait) = std::sync::mpsc::channel::<()>();
    tokio::task::spawn_blocking(move || {
        let _ = wait.recv();
    });
    ClockGuard { _stop: stop }
}
