use aruna_core::structs::{ComputeResources, ExecutionSpec};
use aruna_core::types::NodeId;
use ulid::Ulid;

pub(crate) fn node_id(seed: u8) -> NodeId {
    let mut bytes = [0u8; 32];
    bytes[0] = seed;
    iroh::SecretKey::from_bytes(&bytes).public()
}

pub(crate) fn execution_spec() -> ExecutionSpec {
    ExecutionSpec {
        group_id: Ulid::from_bytes([3u8; 16]),
        name: None,
        description: None,
        tags: Default::default(),
        image: "alpine:3".to_string(),
        entrypoint: None,
        command: vec!["true".to_string()],
        workdir: None,
        env: Default::default(),
        resources: ComputeResources {
            cpu_cores: None,
            ram_bytes: None,
            disk_bytes: None,
            max_walltime_ms: None,
            preemptible: false,
        },
        executor_constraint: None,
        inputs: Vec::new(),
        file_outputs: Vec::new(),
        workspace_outputs: Vec::new(),
        output_prefixes: Vec::new(),
        collision_policy: Default::default(),
    }
}
