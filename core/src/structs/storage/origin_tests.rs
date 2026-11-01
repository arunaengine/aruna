//! Tests version origins: what a dataset may name, bounds and public source URLs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::structs::identity::realm::RealmId;

fn global(seed: u8) -> OriginVersion {
    OriginVersion::Global(GlobalId::Version {
        realm_id: RealmId::from_bytes([1; 32]),
        version_id: Ulid::from_bytes([seed; 16]),
    })
}

fn node() -> NodeId {
    iroh::SecretKey::from_bytes(&[2; 32]).public()
}

#[test]
fn uploads_imports_private() {
    // Uploader, device and storage paths are never projected into datasets.
    let upload = VersionOrigin::Upload {
        node_id: node(),
        device: Some("laptop".to_string()),
        client: Some(UploadClient::Device),
    };
    assert!(project(Some(&upload), None, None).is_empty());
    let storage = VersionOrigin::Storage {
        backend_id: Ulid::nil(),
        key: "share/a.txt".to_string(),
        job_id: None,
    };
    assert!(project(Some(&storage), None, None).is_empty());
}

#[test]
fn derived_name_original() {
    let copy = VersionOrigin::Copy {
        from: global(3),
        job_id: None,
    };
    assert_eq!(
        project(Some(&copy), None, None),
        vec![OriginProjection::BasedOn {
            id: global(3).to_id()
        }]
    );
    let hop = SyncHop {
        relationship_id: Ulid::nil(),
        from: global(4),
    };
    let upload = VersionOrigin::Upload {
        node_id: node(),
        device: None,
        client: None,
    };
    assert_eq!(
        project(Some(&upload), Some(&hop), None),
        vec![OriginProjection::BasedOn {
            id: global(4).to_id()
        }]
    );
}

#[test]
fn node_sources_unpublished() {
    // A node ARN names a bucket and key, so neither a copy nor a hop projects it.
    let arn = VersionedObjectArn::new(
        RealmId::from_bytes([1; 32]),
        node(),
        "private",
        "a.txt",
        Ulid::from_bytes([6; 16]),
    )
    .unwrap();
    let copy = VersionOrigin::Copy {
        from: OriginVersion::Node(arn.clone()),
        job_id: None,
    };
    let hop = SyncHop {
        relationship_id: Ulid::nil(),
        from: OriginVersion::Node(arn),
    };
    assert!(project(Some(&copy), None, None).is_empty());
    assert!(project(None, Some(&hop), None).is_empty());
}

#[test]
fn sources_jobs_project() {
    let source = VersionOrigin::Source { job_id: None };
    let url = "https://s3.example.org/reads/a.fastq".to_string();
    assert_eq!(
        project(Some(&source), None, Some(url.clone())),
        vec![OriginProjection::BasedOn { id: url }]
    );
    let job = VersionOrigin::Job {
        run_id: Ulid::from_bytes([5; 16]),
        started_by: None,
    };
    assert_eq!(
        project(Some(&job), None, None),
        vec![OriginProjection::CreatedBy {
            run_id: Ulid::from_bytes([5; 16])
        }]
    );
}

// Every kind keeps its client, job or starter through the stored encoding.
#[test]
fn origin_fields_stored() {
    let job_id = Some(Ulid::from_bytes([7; 16]));
    let user = UserId::new(Ulid::from_bytes([8; 16]), RealmId::from_bytes([1; 32]));
    let origins = [
        VersionOrigin::Upload {
            node_id: node(),
            device: None,
            client: Some(UploadClient::Portal),
        },
        VersionOrigin::Source { job_id },
        VersionOrigin::Copy {
            from: global(3),
            job_id,
        },
        VersionOrigin::Storage {
            backend_id: Ulid::nil(),
            key: "share/a.txt".to_string(),
            job_id,
        },
        VersionOrigin::Job {
            run_id: Ulid::from_bytes([5; 16]),
            started_by: Some(user),
        },
    ];
    for origin in origins {
        let bytes = postcard::to_allocvec(&origin).unwrap();
        assert_eq!(
            postcard::from_bytes::<VersionOrigin>(&bytes).unwrap(),
            origin
        );
    }
}

// Only a session credential, which the portal requests, marks an upload as the portal's.
#[test]
fn session_key_portal() {
    let session = S3Session::build_access_key(&Ulid::nil().to_string()).unwrap();
    assert_eq!(
        UploadClient::from_access_key(&session),
        UploadClient::Portal
    );
    assert_eq!(
        UploadClient::from_access_key("01ARZ3NDEKTSV4RRFFQ69G5FAV"),
        UploadClient::S3
    );
}

#[test]
fn oversized_device_unbounded() {
    let upload = VersionOrigin::Upload {
        node_id: node(),
        device: Some("d".repeat(MAX_DEVICE_BYTES + 1)),
        client: None,
    };
    assert!(!upload.bounded());
}

#[test]
fn source_url_joins() {
    use crate::structs::execution::source_connector::SourceConnectorKind;
    use std::collections::HashMap;
    let descriptor = PortableSourceDescriptor {
        kind: SourceConnectorKind::S3,
        public_config: HashMap::from([
            (
                "endpoint".to_string(),
                "https://s3.gi.denbi.de/".to_string(),
            ),
            ("bucket".to_string(), "bakrep-v2-data-007".to_string()),
        ]),
        source_path: "/genomes/a.gbff".to_string(),
        version_selector: None,
        namespace: None,
        capabilities: Vec::new(),
        origin_node_id: None,
        root: None,
    };
    assert_eq!(
        public_source_url(&descriptor).as_deref(),
        Some("https://s3.gi.denbi.de/bakrep-v2-data-007/genomes/a.gbff")
    );
    let device = PortableSourceDescriptor {
        kind: SourceConnectorKind::LocalDirectory,
        ..descriptor
    };
    assert_eq!(public_source_url(&device), None);
}

// Only a source read without credentials may be named in a dataset.
#[test]
fn signed_source_unpublished() {
    use crate::structs::execution::source_connector::SourceConnectorKind;
    use std::collections::HashMap;
    let mut descriptor = PortableSourceDescriptor {
        kind: SourceConnectorKind::S3,
        public_config: HashMap::from([
            ("endpoint".to_string(), "https://s3.example.org".to_string()),
            ("bucket".to_string(), "lab".to_string()),
        ]),
        source_path: "a.txt".to_string(),
        version_selector: None,
        namespace: None,
        capabilities: Vec::new(),
        origin_node_id: None,
        root: None,
    };
    assert!(!publishable(&descriptor));
    (descriptor.public_config).insert("skip_signature".to_string(), "true".to_string());
    assert!(publishable(&descriptor));
    descriptor.namespace = Some("login".to_string());
    assert!(!publishable(&descriptor));
    descriptor.namespace = None;
    descriptor.kind = SourceConnectorKind::Http;
    assert!(!publishable(&descriptor));
}

// An HTTP source that served a read is named by its server alone.
#[test]
fn served_http_server() {
    use crate::structs::execution::source_connector::SourceConnectorKind;
    use std::collections::HashMap;
    let endpoint = "https://user:secret@ftp.ncbi.nlm.nih.gov:443/private/token?sig=abc";
    let mut descriptor = PortableSourceDescriptor {
        kind: SourceConnectorKind::Http,
        public_config: HashMap::from([("endpoint".to_string(), endpoint.to_string())]),
        source_path: "genomes/a.fna".to_string(),
        version_selector: Some("v1".to_string()),
        namespace: None,
        capabilities: Vec::new(),
        origin_node_id: None,
        root: None,
    };
    let pin = SourcePin::served(&descriptor);
    assert_eq!(pin.url.as_deref(), Some("https://ftp.ncbi.nlm.nih.gov"));
    assert_eq!(pin.path, "genomes/a.fna");
    // The pin recorded at submission keeps naming no URL for it.
    assert_eq!(SourcePin::of(&descriptor).url, None);
    // A public S3 source keeps its full URL; a signed one and other kinds stay unnamed.
    descriptor.kind = SourceConnectorKind::S3;
    assert_eq!(SourcePin::served(&descriptor).url, None);
    descriptor.public_config = HashMap::from([
        ("endpoint".to_string(), "https://s3.example.org".to_string()),
        ("skip_signature".to_string(), "true".to_string()),
    ]);
    let url = SourcePin::served(&descriptor).url;
    assert_eq!(url.as_deref(), Some("https://s3.example.org/genomes/a.fna"));
    descriptor.kind = SourceConnectorKind::LocalDirectory;
    assert_eq!(SourcePin::served(&descriptor).url, None);
}

// User info, query and fragment of an endpoint never reach a source URL or pin.
#[test]
fn credentials_stripped() {
    use crate::structs::execution::source_connector::SourceConnectorKind;
    use std::collections::HashMap;
    let endpoint = "https://user:pass@s3.example.org/store?token=abc&X-Amz-Signature=def#sig=x";
    let descriptor = PortableSourceDescriptor {
        kind: SourceConnectorKind::S3,
        public_config: HashMap::from([
            ("endpoint".to_string(), endpoint.to_string()),
            ("bucket".to_string(), "lab".to_string()),
            ("skip_signature".to_string(), "true".to_string()),
        ]),
        source_path: "a.txt".to_string(),
        version_selector: None,
        namespace: None,
        capabilities: Vec::new(),
        origin_node_id: None,
        root: None,
    };
    let bare = "https://s3.example.org/store/lab/a.txt";
    assert_eq!(public_source_url(&descriptor).as_deref(), Some(bare));
    assert_eq!(SourcePin::of(&descriptor).url.as_deref(), Some(bare));
    assert_eq!(SourcePin::served(&descriptor).url.as_deref(), Some(bare));
}
