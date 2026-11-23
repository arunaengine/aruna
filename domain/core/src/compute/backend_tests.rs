//! Tests the compute entries of the backends file: parsing, checks and runtime destinations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

const STORAGE: &str = r#"
[backend.default]
type = "filesystem"
root = "/data/blob"
multipart_bucket = "parts"
default = true

[import_root.nfs-share]
path = "/shared"
groups = ["01JC0000000000000000000000"]
"#;

const VALID: &str = r#"
[compute.hpc-a]
kind = "slurm"
class = "hpc"
optional = true
execution_location = "dc-b"
execution_labels = { gpu = "a100" }
data_reach = ["nfs-share", "default"]
max_cpu_cores = 512
max_walltime_ms = 172800000
[compute.hpc-a.capabilities]
gpus = 4
architectures = ["x86_64"]
[compute.hpc-a.suitability]
batch = "best"
[compute.hpc-a.slurm]
state_root = "/shared/aruna/hpc-a"
partition = "long"

[compute.lab-docker]
kind = "docker"
[compute.lab-docker.docker]
docker_host = "tcp://10.0.0.5:2376"
state_root = "/var/lib/aruna/lab-docker"

[compute.local]
kind = "docker"
[compute.local.docker]
state_root = "/var/lib/aruna/local"

[compute.cluster]
kind = "kubernetes"
[compute.cluster.kubernetes]
kubeconfig = "/etc/aruna/kubeconfig"
context = "prod"
storage_class = "standard"
helper_image = "helper:1"
"#;

fn server(entry: &KubernetesEntry) -> Result<String, String> {
    entry
        .context
        .clone()
        .map(|context| format!("https://{context}.example:6443"))
        .ok_or_else(|| "no context".to_string())
}

fn check(compute: &str) -> Result<Vec<ComputeBackend>, ConversionError> {
    let file = BackendsFile::parse(&format!("{STORAGE}\n{compute}"))?;
    let inputs = DestinationInputs {
        docker_host: None,
        kube_server: &server,
    };
    resolve(&file, &inputs)
}

fn error(compute: &str) -> String {
    check(compute).unwrap_err().to_string()
}

#[test]
fn resolves_valid_file() {
    let backends = check(VALID).unwrap();

    let names: Vec<&str> = backends.iter().map(|b| b.name.as_str()).collect();
    assert_eq!(names, ["cluster", "hpc-a", "lab-docker", "local"]);
    let by_name = |name: &str| backends.iter().find(|b| b.name == name).unwrap();
    let hpc = by_name("hpc-a");
    assert_eq!(hpc.kind, ExecutorKind::Slurm);
    assert_eq!(hpc.entry.capabilities.gpus, 4);
    assert_eq!(hpc.suitability.batch, Suitability::Best);
    assert_eq!(hpc.suitability.sessions, Suitability::Refused);
    assert_eq!(
        by_name("cluster").destination,
        RuntimeDestination::Kubernetes {
            server: "https://prod.example:6443".to_string(),
            namespace: "default".to_string(),
        }
    );
    // A remote daemon cannot host sessions; the local one keeps the kind default.
    assert_eq!(
        by_name("lab-docker").suitability.sessions,
        Suitability::Refused
    );
    assert!(!by_name("lab-docker").destination.is_local());
    assert_eq!(by_name("local").suitability.sessions, Suitability::Good);
    assert_eq!(
        by_name("local").destination,
        RuntimeDestination::Docker {
            host: DOCKER_DEFAULT_HOST.to_string(),
            state_root: "/var/lib/aruna/local".to_string(),
        }
    );
}

#[test]
fn docker_host_fallback() {
    let file = BackendsFile::parse(&format!(
        "{STORAGE}\n[compute.a]\nkind = \"docker\"\n[compute.a.docker]\nstate_root = \"/a\"\n"
    ))
    .unwrap();
    let inputs = DestinationInputs {
        docker_host: Some("ssh://builder@remote"),
        kube_server: &server,
    };

    let backend = resolve(&file, &inputs).unwrap().remove(0);

    assert!(!backend.destination.is_local());
    assert_eq!(backend.suitability.sessions, Suitability::Refused);
}

#[test]
fn rejects_bad_names() {
    assert!(
        error(
            "[compute.default]\nkind = \"slurm\"\n[compute.default.slurm]\nstate_root = \"/s\"\n"
        )
        .contains("reserved")
    );
    let long = "a".repeat(MAX_KIND_LEN + 1);
    assert!(
        check(&format!(
            "[compute.{long}]\nkind = \"slurm\"\n[compute.{long}.slurm]\nstate_root = \"/s\"\n"
        ))
        .is_err()
    );
    // TOML refuses a table defined twice, so names stay unique.
    let twice = "[compute.a]\nkind = \"slurm\"\n[compute.a.slurm]\nstate_root = \"/s\"\n";
    assert!(check(&format!("{twice}{twice}")).is_err());
}

#[test]
fn rejects_kind_mismatch() {
    for compute in [
        "[compute.a]\nkind = \"slurm\"\n",
        "[compute.a]\nkind = \"slurm\"\n[compute.a.apptainer]\nstate_root = \"/s\"\ncgroup_root = \"/c\"\n",
        "[compute.a]\nkind = \"galaxy\"\n[compute.a.slurm]\nstate_root = \"/s\"\n",
        "[compute.a]\nkind = \"slurm\"\n[compute.a.slurm]\nstate_root = \"/s\"\n[compute.a.docker]\nstate_root = \"/d\"\n",
    ] {
        assert!(check(compute).is_err(), "{compute}");
    }
}

#[test]
fn rejects_shared_destination() {
    let message = error(
        "[compute.a]\nkind = \"slurm\"\n[compute.a.slurm]\nstate_root = \"/s\"\n\
         [compute.b]\nkind = \"slurm\"\n[compute.b.slurm]\nstate_root = \"/s\"\npartition = \"x\"\n",
    );
    assert!(message.contains("runtime destination"), "{message}");
}

#[test]
fn rejects_equivalent_roots() {
    // Two spellings of one folder are one runtime destination; `..` is refused.
    let pair = |first: &str, second: &str| {
        format!(
            "[compute.a]\nkind = \"slurm\"\n[compute.a.slurm]\nstate_root = \"{first}\"\n\
             [compute.b]\nkind = \"slurm\"\n[compute.b.slurm]\nstate_root = \"{second}\"\n"
        )
    };
    for second in ["/shared/aruna/", "/shared/./aruna", "/shared//aruna/."] {
        let message = error(&pair("/shared/aruna", second));
        assert!(
            message.contains("runtime destination"),
            "{second}: {message}"
        );
    }
    let message = error(&pair("/shared/aruna", "/shared/other/../aruna"));
    assert!(message.contains("state_root"), "{message}");
    assert!(check(&pair("/shared/aruna", "/shared/aruna-b")).is_ok());
}

#[test]
fn rejects_session_hosts() {
    let two = "[compute.a]\nkind = \"docker\"\n[compute.a.docker]\nstate_root = \"/a\"\n\
               [compute.b]\nkind = \"docker\"\n[compute.b.docker]\nstate_root = \"/b\"\n";
    assert!(error(two).contains("sessions"));
    let one = "[compute.a]\nkind = \"docker\"\n[compute.a.docker]\nstate_root = \"/a\"\n\
               [compute.b]\nkind = \"docker\"\nsuitability = { sessions = \"refused\" }\n\
               [compute.b.docker]\nstate_root = \"/b\"\n";
    assert!(check(one).is_ok());
    let remote = "[compute.a]\nkind = \"docker\"\nsuitability = { sessions = \"good\" }\n\
                  [compute.a.docker]\ndocker_host = \"tcp://10.0.0.5:2376\"\nstate_root = \"/a\"\n";
    assert!(error(remote).contains("cannot host sessions"));
}

#[test]
fn rejects_unknown_reach() {
    let message = error(
        "[compute.a]\nkind = \"slurm\"\ndata_reach = [\"tape\"]\n[compute.a.slurm]\nstate_root = \"/s\"\n",
    );
    assert!(message.contains("tape"), "{message}");
}

#[test]
fn rejects_bad_values() {
    for compute in [
        "[compute.a]\nkind = \"slurm\"\nmax_concurrent = 0\n[compute.a.slurm]\nstate_root = \"/s\"\n",
        "[compute.a]\nkind = \"slurm\"\n[compute.a.slurm]\nstate_root = \"relative\"\n",
        "[compute.a]\nkind = \"slurm\"\nsite = \"trusted\"\n[compute.a.slurm]\nstate_root = \"/s\"\n",
        "[compute.a]\nkind = \"slurm\"\ntoken = \"leaked\"\n[compute.a.slurm]\nstate_root = \"/s\"\n",
        "[compute.a]\nkind = \"slurm\"\n[compute.a.slurm]\nstate_root = \"/s\"\nqueue = \"x\"\n",
        "[compute.a]\nkind = \"kubernetes\"\n[compute.a.kubernetes]\nstorage_class = \"s\"\nhelper_image = \"h\"\n",
    ] {
        assert!(check(compute).is_err(), "{compute}");
    }
}

#[test]
fn rejects_long_allowance() {
    let groups = |count: u128| {
        let ids: Vec<String> = (1..=count)
            .map(|id| format!("\"{}\"", ulid::Ulid::from(id)))
            .collect();
        format!(
            "[compute.a]\nkind = \"slurm\"\ngroups = [{}]\n[compute.a.slurm]\nstate_root = \"/s\"\n",
            ids.join(", ")
        )
    };
    assert!(check(&groups(MAX_BACKEND_GROUPS as u128)).is_ok());
    let message = error(&groups(MAX_BACKEND_GROUPS as u128 + 1));
    assert!(message.contains("groups"), "{message}");
}

#[test]
fn rejects_ninth_backend() {
    let entry = |index: usize| {
        format!(
            "[compute.s{index}]\nkind = \"slurm\"\n[compute.s{index}.slurm]\nstate_root = \"/s{index}\"\n"
        )
    };
    let eight: String = (0..MAX_ADVERTISED_EXECUTORS).map(entry).collect();
    assert!(check(&eight).is_ok());
    assert!(check(&format!("{eight}{}", entry(MAX_ADVERTISED_EXECUTORS))).is_err());
}

#[test]
fn parses_compute_tables() {
    let file = BackendsFile::parse(
        r#"
[compute.hpc-a]
kind = "slurm"
groups = ["01JC0000000000000000000000"]
[compute.hpc-a.capabilities]
gpus = 4
multi_container = true
[compute.hpc-a.suitability]
batch = "best"
[compute.hpc-a.slurm]
state_root = "/shared/aruna/hpc-a"
"#,
    )
    .unwrap();

    let entry = &file.compute["hpc-a"];
    assert_eq!(entry.capabilities.gpus, 4);
    assert!(entry.capabilities.multi_container);
    assert_eq!(entry.suitability.batch, Some(Suitability::Best));
    assert_eq!(entry.suitability.sessions, None);
    assert!(entry.slurm.is_some() && entry.docker.is_none());
    for unknown in ["token = \"leaked\"", "site = \"verified\""] {
        let text = format!("[compute.a]\nkind = \"slurm\"\n{unknown}\n");
        assert!(BackendsFile::parse(&text).is_err(), "{unknown}");
    }
}

#[test]
fn classifies_daemon_addresses() {
    assert!(local_daemon("unix:///var/run/docker.sock"));
    assert!(local_daemon("/run/user/1000/docker.sock"));
    assert!(local_daemon("npipe:////./pipe/docker_engine"));
    for remote in [
        "tcp://10.0.0.5:2376",
        "http://docker.example:2375",
        "https://docker.example:2376",
        "ssh://builder@remote",
        "fd://",
        "docker.example:2375",
    ] {
        assert!(!local_daemon(remote), "{remote}");
    }
}
