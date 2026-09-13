use super::*;

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn label(key: &str, value: &str) -> LabelMatch {
    LabelMatch {
        key: key.to_string(),
        value: value.to_string(),
    }
}

fn selector() -> PlacementSelector {
    PlacementSelector {
        node_id: None,
        location: None,
        labels: Vec::new(),
        executor_kind: None,
    }
}

fn policy(allowed: Vec<PlacementSelector>) -> PlacementPolicy {
    PlacementPolicy {
        policy_id: Ulid::generate(),
        name: "eu-only".to_string(),
        owner_group_id: None,
        allowed,
    }
}

fn subject() -> PlacementSubject {
    PlacementSubject {
        node_id: node(1),
        generation: 1,
        location: "eu-west".to_string(),
        labels: BTreeMap::from([
            ("zone".to_string(), "a".to_string()),
            ("tier".to_string(), "gold".to_string()),
        ]),
        executor_kind: Some("docker".to_string()),
        local_to_controller: true,
    }
}

fn resolved(policies: &[&PlacementPolicy]) -> BTreeMap<Ulid, PolicyResolution> {
    policies
        .iter()
        .map(|policy| {
            let verified = VerifiedPolicy::verify((*policy).clone()).expect("valid policy");
            (policy.policy_id, PolicyResolution::Known(verified))
        })
        .collect()
}

/// A cache entry that never passed verification, as a corrupt store or a
/// hostile holder could supply.
fn forged(policy: &PlacementPolicy) -> BTreeMap<Ulid, PolicyResolution> {
    BTreeMap::from([(
        policy.policy_id,
        PolicyResolution::Known(VerifiedPolicy(policy.clone())),
    )])
}

/// The ref a document mints from its own bytes, valid or not.
fn self_ref(policy: &PlacementPolicy) -> PlacementPolicyRef {
    PlacementPolicyRef {
        policy_id: policy.policy_id,
        digest: policy.digest(),
    }
}

fn distinct_refs(count: usize) -> Vec<PlacementPolicyRef> {
    (0..count)
        .map(|index| PlacementPolicyRef {
            policy_id: Ulid::generate(),
            digest: [index as u8; 32],
        })
        .collect()
}

#[test]
fn empty_refs_allow() {
    assert_eq!(
        evaluate_placement(&[], &BTreeMap::new(), &subject()),
        PlacementDecision::Allowed
    );
}

#[test]
fn exact_node_matches() {
    let allowed = policy(vec![PlacementSelector {
        node_id: Some(node(1)),
        ..selector()
    }]);
    let other = policy(vec![PlacementSelector {
        node_id: Some(node(2)),
        ..selector()
    }]);
    assert_eq!(
        evaluate_placement(&[self_ref(&allowed)], &resolved(&[&allowed]), &subject()),
        PlacementDecision::Allowed
    );
    assert_eq!(
        evaluate_placement(&[self_ref(&other)], &resolved(&[&other]), &subject()),
        PlacementDecision::Denied {
            policy_ids: vec![other.policy_id]
        }
    );
}

#[test]
fn location_gates_subject() {
    // An unset subject location is the default location, not an unknown one.
    let default = policy(vec![PlacementSelector {
        location: Some(DEFAULT_LOCATION.to_string()),
        ..selector()
    }]);
    let west = policy(vec![PlacementSelector {
        location: Some("eu-west".to_string()),
        ..selector()
    }]);
    let mut unset = subject();
    unset.location = "  ".to_string();
    assert_eq!(
        evaluate_placement(&[self_ref(&west)], &resolved(&[&west]), &subject()),
        PlacementDecision::Allowed
    );
    assert_eq!(
        evaluate_placement(&[self_ref(&west)], &resolved(&[&west]), &unset),
        PlacementDecision::Denied {
            policy_ids: vec![west.policy_id]
        }
    );
    assert_eq!(
        evaluate_placement(&[self_ref(&default)], &resolved(&[&default]), &unset),
        PlacementDecision::Allowed
    );
}

#[test]
fn labels_and_together() {
    let both = policy(vec![PlacementSelector {
        labels: vec![label("tier", "gold"), label("zone", "a")],
        ..selector()
    }]);
    let missing = policy(vec![PlacementSelector {
        labels: vec![label("tier", "platinum"), label("zone", "a")],
        ..selector()
    }]);
    assert_eq!(
        evaluate_placement(&[self_ref(&both)], &resolved(&[&both]), &subject()),
        PlacementDecision::Allowed
    );
    assert_eq!(
        evaluate_placement(&[self_ref(&missing)], &resolved(&[&missing]), &subject()),
        PlacementDecision::Denied {
            policy_ids: vec![missing.policy_id]
        }
    );
}

#[test]
fn executor_kind_matches() {
    // An unknown required attribute must deny instead of being skipped.
    let docker = policy(vec![PlacementSelector {
        executor_kind: Some("docker".to_string()),
        ..selector()
    }]);
    let mut unknown = subject();
    unknown.executor_kind = None;
    assert_eq!(
        evaluate_placement(&[self_ref(&docker)], &resolved(&[&docker]), &subject()),
        PlacementDecision::Allowed
    );
    assert_eq!(
        evaluate_placement(&[self_ref(&docker)], &resolved(&[&docker]), &unknown),
        PlacementDecision::Denied {
            policy_ids: vec![docker.policy_id]
        }
    );
}

#[test]
fn permuted_selectors_match() {
    let first = PlacementSelector {
        location: Some("eu-west".to_string()),
        labels: vec![label("tier", "gold"), label("zone", "a")],
        ..selector()
    };
    let second = PlacementSelector {
        node_id: Some(node(9)),
        ..selector()
    };
    let policy_id = Ulid::generate();
    let forward = PlacementPolicy::new(
        policy_id,
        "eu-only".to_string(),
        vec![first.clone(), second.clone()],
    )
    .expect("valid policy");
    let reverse = PlacementPolicy::new(
        policy_id,
        "eu-only".to_string(),
        vec![
            second,
            PlacementSelector {
                labels: vec![label("zone", "a"), label("tier", "gold")],
                ..first
            },
        ],
    )
    .expect("valid policy");
    assert_eq!(forward.digest(), reverse.digest());
    assert_eq!(forward.canonical_bytes(), reverse.canonical_bytes());
    assert_eq!(
        evaluate_placement(&[self_ref(&forward)], &resolved(&[&forward]), &subject()),
        evaluate_placement(&[self_ref(&reverse)], &resolved(&[&reverse]), &subject())
    );
}

#[test]
fn permuted_refs_match() {
    let west = policy(vec![PlacementSelector {
        location: Some("eu-west".to_string()),
        ..selector()
    }]);
    let gold = policy(vec![PlacementSelector {
        labels: vec![label("tier", "gold")],
        ..selector()
    }]);
    let store = resolved(&[&west, &gold]);
    let forward = evaluate_placement(&[self_ref(&west), self_ref(&gold)], &store, &subject());
    let reverse = evaluate_placement(&[self_ref(&gold), self_ref(&west)], &store, &subject());
    assert_eq!(forward, PlacementDecision::Allowed);
    assert_eq!(forward, reverse);
}

#[test]
fn refs_intersect_policies() {
    let west = policy(vec![PlacementSelector {
        location: Some("eu-west".to_string()),
        ..selector()
    }]);
    let east = policy(vec![PlacementSelector {
        location: Some("eu-east".to_string()),
        ..selector()
    }]);
    let store = resolved(&[&west, &east]);
    assert_eq!(
        evaluate_placement(&[self_ref(&east), self_ref(&west)], &store, &subject()),
        PlacementDecision::Denied {
            policy_ids: vec![east.policy_id]
        }
    );
}

#[test]
fn missing_ref_requires() {
    let west = policy(vec![PlacementSelector {
        location: Some("eu-west".to_string()),
        ..selector()
    }]);
    assert_eq!(
        evaluate_placement(&[self_ref(&west)], &BTreeMap::new(), &subject()),
        PlacementDecision::Required {
            refs: vec![self_ref(&west)]
        }
    );
}

#[test]
fn unresolved_ref_unavailable() {
    let west = policy(vec![PlacementSelector {
        location: Some("eu-west".to_string()),
        ..selector()
    }]);
    let store = BTreeMap::from([(west.policy_id, PolicyResolution::Unresolved)]);
    assert_eq!(
        evaluate_placement(&[self_ref(&west)], &store, &subject()),
        PlacementDecision::Unavailable {
            policy_ids: vec![west.policy_id]
        }
    );
}

#[test]
fn digest_mismatch_detected() {
    let west = policy(vec![PlacementSelector {
        location: Some("eu-west".to_string()),
        ..selector()
    }]);
    let tampered = PlacementPolicyRef {
        policy_id: west.policy_id,
        digest: [7; 32],
    };
    assert_eq!(
        evaluate_placement(&[tampered], &resolved(&[&west]), &subject()),
        PlacementDecision::DigestMismatch {
            refs: vec![tampered]
        }
    );
}

#[test]
fn empty_selector_blocks() {
    // A constraint-free selector would otherwise allow every subject.
    let open = policy(vec![selector()]);
    assert_eq!(open.validate(), Err(PlacementPolicyError::EmptySelector));
    assert_eq!(
        VerifiedPolicy::verify(open.clone()),
        Err(PlacementPolicyError::EmptySelector)
    );
    assert_eq!(
        evaluate_placement(&[self_ref(&open)], &forged(&open), &subject()),
        PlacementDecision::Invalid {
            policy_ids: vec![open.policy_id]
        }
    );
    let none = PlacementPolicy {
        allowed: Vec::new(),
        ..open
    };
    assert!(!none.allows(&subject().normalized().expect("valid subject")));
}

#[test]
fn validate_bounds_inputs() {
    let valid = PlacementSelector {
        location: Some("eu-west".to_string()),
        ..selector()
    };
    let mut named = policy(vec![valid.clone()]);
    named.name = "n".repeat(MAX_POLICY_NAME_LEN + 1);
    assert_eq!(named.validate(), Err(PlacementPolicyError::InvalidName));
    named.name = "  ".to_string();
    assert_eq!(named.validate(), Err(PlacementPolicyError::InvalidName));

    let nil = PlacementPolicy {
        policy_id: Ulid::nil(),
        ..policy(vec![valid.clone()])
    };
    assert_eq!(nil.validate(), Err(PlacementPolicyError::NilPolicyId));

    let many = policy(vec![valid.clone(); MAX_POLICY_SELECTORS + 1]);
    assert_eq!(many.validate(), Err(PlacementPolicyError::SelectorCount));
    assert_eq!(
        policy(Vec::new()).validate(),
        Err(PlacementPolicyError::SelectorCount)
    );

    let long_location = policy(vec![PlacementSelector {
        location: Some("l".repeat(MAX_NODE_LOCATION_LEN + 1)),
        ..selector()
    }]);
    assert_eq!(
        long_location.validate(),
        Err(PlacementPolicyError::InvalidLocation)
    );

    let long_kind = policy(vec![PlacementSelector {
        executor_kind: Some("k".repeat(MAX_EXECUTOR_KIND_LEN + 1)),
        ..selector()
    }]);
    assert_eq!(
        long_kind.validate(),
        Err(PlacementPolicyError::InvalidExecutorKind)
    );

    let many_labels = policy(vec![PlacementSelector {
        labels: vec![label("zone", "a"); MAX_SELECTOR_LABELS + 1],
        ..selector()
    }]);
    assert_eq!(
        many_labels.validate(),
        Err(PlacementPolicyError::LabelCount)
    );

    let long_key = policy(vec![PlacementSelector {
        labels: vec![label(&"k".repeat(MAX_LABEL_KEY_LEN + 1), "a")],
        ..selector()
    }]);
    assert_eq!(long_key.validate(), Err(PlacementPolicyError::InvalidLabel));

    let empty_key = policy(vec![PlacementSelector {
        labels: vec![label(" ", "a")],
        ..selector()
    }]);
    assert_eq!(
        empty_key.validate(),
        Err(PlacementPolicyError::InvalidLabel)
    );

    let authored = PlacementPolicy::new(Ulid::generate(), " ok ".to_string(), vec![valid])
        .expect("valid policy");
    assert!(VerifiedPolicy::verify(authored).is_ok());
}

#[test]
fn canonical_dedupes_selectors() {
    let untrimmed = PlacementSelector {
        location: Some(" eu-west ".to_string()),
        labels: vec![label(" zone ", " a ")],
        ..selector()
    };
    let clean = PlacementSelector {
        location: Some("eu-west".to_string()),
        labels: vec![label("zone", "a")],
        ..selector()
    };
    let duplicated = policy(vec![untrimmed, clean.clone()]);
    let single = PlacementPolicy {
        allowed: vec![clean],
        ..duplicated.clone()
    };
    assert_eq!(duplicated.canonical().allowed.len(), 1);
    assert_eq!(duplicated.digest(), single.digest());
}

#[test]
fn digest_binds_definition() {
    let base = policy(vec![PlacementSelector {
        location: Some("eu-west".to_string()),
        ..selector()
    }]);
    let renamed = PlacementPolicy {
        name: "renamed".to_string(),
        ..base.clone()
    };
    let widened = PlacementPolicy {
        allowed: vec![
            PlacementSelector {
                location: Some("eu-west".to_string()),
                ..selector()
            },
            PlacementSelector {
                location: Some("eu-east".to_string()),
                ..selector()
            },
        ],
        ..base.clone()
    };
    let reidentified = PlacementPolicy {
        policy_id: Ulid::generate(),
        ..base.clone()
    };
    assert_ne!(base.digest(), renamed.digest());
    assert_ne!(base.digest(), widened.digest());
    assert_ne!(base.digest(), reidentified.digest());
    assert_eq!(base.digest(), base.canonical().digest());
}

#[test]
fn digest_binds_generation() {
    // A stored receipt must detect a subject that changed underneath it,
    // while matching itself must never depend on the generation.
    let base = subject();
    let advanced = PlacementSubject {
        generation: base.generation + 1,
        ..base.clone()
    };
    let moved = PlacementSubject {
        location: "eu-east".to_string(),
        ..base.clone()
    };
    assert_ne!(base.digest(), advanced.digest());
    assert_ne!(base.digest(), moved.digest());
    assert_eq!(
        base.normalized().expect("valid subject"),
        advanced.normalized().expect("valid subject")
    );
}

#[test]
fn digest_binds_locality() {
    // A receipt must prove which execution-site model was accepted.
    let local = subject();
    let remote = PlacementSubject {
        local_to_controller: false,
        ..local.clone()
    };
    assert_ne!(
        local.digest().expect("valid subject"),
        remote.digest().expect("valid subject")
    );
    assert_eq!(
        local.normalized().expect("valid subject"),
        remote.normalized().expect("valid subject")
    );
}

#[test]
fn refs_reject_conflicts() {
    let policy_id = Ulid::generate();
    let first = PlacementPolicyRef {
        policy_id,
        digest: [1; 32],
    };
    let second = PlacementPolicyRef {
        policy_id,
        digest: [2; 32],
    };
    assert_eq!(
        PlacementPolicyRef::canonical_set(&[first, second]),
        Err(PlacementPolicyError::ConflictingRefs { policy_id })
    );
    assert_eq!(
        PlacementPolicyRef::canonical_set(&[first, first]),
        Ok(vec![first])
    );
    assert_eq!(
        PlacementPolicyRef::canonical_set(&distinct_refs(MAX_POLICY_REFS + 1)),
        Err(PlacementPolicyError::RefCount)
    );
    // The raw input is bounded before it is copied and deduplicated.
    assert_eq!(
        PlacementPolicyRef::canonical_set(&distinct_refs(MAX_POLICY_REF_INPUT + 1)),
        Err(PlacementPolicyError::RefCount)
    );
    assert!(PlacementPolicyRef::canonical_set(&distinct_refs(MAX_POLICY_REFS)).is_ok());
}

#[test]
fn policy_round_trips() {
    let policy = policy(vec![PlacementSelector {
        node_id: Some(node(3)),
        location: Some("eu-west".to_string()),
        labels: vec![label("zone", "a")],
        executor_kind: Some("docker".to_string()),
    }]);
    let bytes = postcard::to_allocvec(&policy).unwrap();
    let decoded: PlacementPolicy = postcard::from_bytes(&bytes).unwrap();
    assert_eq!(decoded, policy);
    assert_eq!(decoded.digest(), policy.digest());
    assert!(VerifiedPolicy::verify(decoded).is_ok());
}

#[test]
fn invalid_known_blocks() {
    // A self-consistent digest must never turn a malformed document into a
    // grant, whichever holder cached it.
    let valid = PlacementSelector {
        location: Some("eu-west".to_string()),
        ..selector()
    };
    let base = policy(vec![valid.clone()]);
    let documents = vec![
        PlacementPolicy {
            policy_id: Ulid::nil(),
            ..base.clone()
        },
        PlacementPolicy {
            name: "n".repeat(MAX_POLICY_NAME_LEN + 1),
            ..base.clone()
        },
        PlacementPolicy {
            allowed: vec![valid.clone(); MAX_POLICY_SELECTORS + 1],
            ..base.clone()
        },
        PlacementPolicy {
            allowed: vec![PlacementSelector {
                labels: vec![label("zone", "a"); MAX_SELECTOR_LABELS + 1],
                ..valid.clone()
            }],
            ..base.clone()
        },
        PlacementPolicy {
            allowed: vec![PlacementSelector {
                location: Some("l".repeat(MAX_NODE_LOCATION_LEN + 1)),
                ..selector()
            }],
            ..base.clone()
        },
        PlacementPolicy {
            allowed: vec![PlacementSelector {
                executor_kind: Some("k".repeat(MAX_EXECUTOR_KIND_LEN + 1)),
                ..valid
            }],
            ..base.clone()
        },
        PlacementPolicy {
            allowed: Vec::new(),
            ..base
        },
    ];
    for document in documents {
        assert!(VerifiedPolicy::verify(document.clone()).is_err());
        assert_eq!(
            evaluate_placement(&[self_ref(&document)], &forged(&document), &subject()),
            PlacementDecision::Invalid {
                policy_ids: vec![document.policy_id]
            }
        );
    }
}

#[test]
fn noncanonical_document_rejected() {
    // Untrimmed bytes hash to the canonical digest, so only the exact
    // canonical encoding may resolve a ref.
    let west = policy(vec![PlacementSelector {
        location: Some("eu-west".to_string()),
        ..selector()
    }]);
    let untrimmed = PlacementPolicy {
        name: " eu-only ".to_string(),
        ..west.clone()
    };
    assert_eq!(self_ref(&untrimmed), self_ref(&west));
    assert_eq!(untrimmed.validate(), Ok(()));
    assert_eq!(
        VerifiedPolicy::verify(untrimmed.clone()),
        Err(PlacementPolicyError::NotCanonical)
    );
    assert_eq!(
        evaluate_placement(&[self_ref(&untrimmed)], &forged(&untrimmed), &subject()),
        PlacementDecision::Invalid {
            policy_ids: vec![untrimmed.policy_id]
        }
    );
    assert_eq!(
        evaluate_placement(&[self_ref(&west)], &resolved(&[&west]), &subject()),
        PlacementDecision::Allowed
    );
}

#[test]
fn oversized_refs_rejected() {
    let west = policy(vec![PlacementSelector {
        location: Some("eu-west".to_string()),
        ..selector()
    }]);
    let store = resolved(&[&west]);
    assert_eq!(
        evaluate_placement(&distinct_refs(MAX_POLICY_REFS + 1), &store, &subject()),
        PlacementDecision::InvalidInput {
            reason: PlacementPolicyError::RefCount
        }
    );
    assert_eq!(
        evaluate_placement(&distinct_refs(MAX_POLICY_REF_INPUT + 1), &store, &subject()),
        PlacementDecision::InvalidInput {
            reason: PlacementPolicyError::RefCount
        }
    );
    let conflicting = [
        self_ref(&west),
        PlacementPolicyRef {
            policy_id: west.policy_id,
            digest: [9; 32],
        },
    ];
    assert_eq!(
        evaluate_placement(&conflicting, &store, &subject()),
        PlacementDecision::InvalidInput {
            reason: PlacementPolicyError::ConflictingRefs {
                policy_id: west.policy_id
            }
        }
    );
}

#[test]
fn oversized_resolution_rejected() {
    // An empty ref set must not shortcut a malformed resolution map.
    let store = (0..=MAX_POLICY_REF_INPUT)
        .map(|_| (Ulid::generate(), PolicyResolution::Unresolved))
        .collect();
    assert_eq!(
        evaluate_placement(&[], &store, &subject()),
        PlacementDecision::InvalidInput {
            reason: PlacementPolicyError::ResolutionCount
        }
    );
}

#[test]
fn subject_bounds_rejected() {
    let cases = vec![
        (
            PlacementSubject {
                location: "l".repeat(MAX_NODE_LOCATION_LEN + 1),
                ..subject()
            },
            PlacementPolicyError::InvalidLocation,
        ),
        (
            PlacementSubject {
                labels: (0..=MAX_SUBJECT_LABELS)
                    .map(|index| (format!("key{index}"), "a".to_string()))
                    .collect(),
                ..subject()
            },
            PlacementPolicyError::SubjectLabelCount,
        ),
        (
            PlacementSubject {
                labels: BTreeMap::from([("k".repeat(MAX_LABEL_KEY_LEN + 1), "a".to_string())]),
                ..subject()
            },
            PlacementPolicyError::InvalidLabel,
        ),
        (
            PlacementSubject {
                labels: BTreeMap::from([("zone".to_string(), "v".repeat(MAX_LABEL_VALUE_LEN + 1))]),
                ..subject()
            },
            PlacementPolicyError::InvalidLabel,
        ),
        (
            PlacementSubject {
                labels: BTreeMap::from([(" ".to_string(), "a".to_string())]),
                ..subject()
            },
            PlacementPolicyError::InvalidLabel,
        ),
        (
            PlacementSubject {
                executor_kind: Some("k".repeat(MAX_EXECUTOR_KIND_LEN + 1)),
                ..subject()
            },
            PlacementPolicyError::InvalidExecutorKind,
        ),
        (
            PlacementSubject {
                executor_kind: Some("  ".to_string()),
                ..subject()
            },
            PlacementPolicyError::InvalidExecutorKind,
        ),
    ];
    for (subject, reason) in cases {
        assert_eq!(subject.validate(), Err(reason.clone()));
        assert_eq!(subject.digest(), Err(reason.clone()));
        assert_eq!(
            evaluate_placement(&[], &BTreeMap::new(), &subject),
            PlacementDecision::InvalidInput { reason }
        );
    }
}

#[test]
fn ambiguous_labels_rejected() {
    // Two spellings of one key must not silently collapse into one label.
    let ambiguous = PlacementSubject {
        labels: BTreeMap::from([
            ("zone".to_string(), "a".to_string()),
            (" zone ".to_string(), "b".to_string()),
        ]),
        ..subject()
    };
    let reason = PlacementPolicyError::AmbiguousLabel {
        key: "zone".to_string(),
    };
    assert_eq!(ambiguous.validate(), Err(reason.clone()));
    assert_eq!(
        evaluate_placement(&[], &BTreeMap::new(), &ambiguous),
        PlacementDecision::InvalidInput { reason }
    );
}
