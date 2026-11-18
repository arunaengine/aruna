//! Resolves every input of one submission at the node that holds it, once, at ingress. The
//! values are stored in the family spec and stay valid when admission is forwarded.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, VecDeque};

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{
    CapturedInput, ExecutionSpec, InputSelection, SubmissionId,
};
use aruna_core::structs::placement::policy::PlacementPolicyRef;
use aruna_core::structs::storage::replication::VersionedObjectArn;

use super::admit_here::placement_failure;
use super::input_capture::{CaptureCaller, CaptureInputsOperation};
use super::input_walk::{
    ExpandDirectoryOperation, FollowLinkOperation, InputCaller, RangeHolderOperation,
};
use super::output_policies::OutputPoliciesOperation;
use super::remote_inputs::{RemoteInputsOperation, ResolveCaller};
use super::submission_rules::{exact_version, is_directory};
use crate::jobs::submit::SubmitJobError;

/// The caller of a resolve, and until when its grants hold.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct ResolveScope {
    pub(super) caller: InputCaller,
    pub(super) submission_id: SubmissionId,
    pub(super) expires_at: u64,
    pub(super) token: Option<AuthToken>,
}

impl ResolveScope {
    /// The caller for captures here; the bearer lets coordinators answer release membership.
    pub(super) fn capture_caller(&self) -> CaptureCaller {
        let bearer = match &self.token {
            Some(AuthToken::Bearer(token)) => Some(token.as_str().to_string()),
            _ => None,
        };
        CaptureCaller {
            input: self.caller,
            submission_id: self.submission_id,
            expires_at: self.expires_at,
            bearer,
        }
    }
}

/// The inputs after links and directories, their captures and the output policies.
#[derive(Debug, PartialEq)]
pub(super) struct ResolvedInputs {
    pub(super) inputs: Vec<InputSelection>,
    pub(super) captured: Vec<CapturedInput>,
    pub(super) policies: Vec<PlacementPolicyRef>,
}

#[derive(Debug, PartialEq)]
enum ResolveStep {
    Expand,
    Follow,
    Range,
    Capture,
    Remote,
    Policies,
}

/// Expands directories, follows links, finds each input's holder, captures what is held here
/// and asks the other holders for the rest.
#[derive(Debug, PartialEq)]
pub(super) struct ResolveInputsOperation {
    scope: ResolveScope,
    spec: ExecutionSpec,
    step: ResolveStep,
    pending: VecDeque<InputSelection>,
    files: Vec<InputSelection>,
    here: Vec<InputSelection>,
    remote: BTreeMap<NodeId, Vec<InputSelection>>,
    links: BTreeMap<String, VersionedObjectArn>,
    captured: Vec<CapturedInput>,
    answered: Vec<InputSelection>,
}

type ResolveNext = Result<Next<ResolvedInputs>, SubmitJobError>;
type Followed = (InputSelection, Option<VersionedObjectArn>);
type Answers = Vec<(InputSelection, CapturedInput)>;

impl ResolveInputsOperation {
    pub(super) fn new(spec: &ExecutionSpec, scope: ResolveScope) -> Self {
        Self {
            scope,
            spec: spec.clone(),
            step: ResolveStep::Expand,
            pending: VecDeque::new(),
            files: Vec::new(),
            here: Vec::new(),
            remote: BTreeMap::new(),
            links: BTreeMap::new(),
            captured: Vec::new(),
            answered: Vec::new(),
        }
    }

    fn local(&self) -> NodeId {
        self.scope.caller.local
    }

    fn place(&mut self, input: InputSelection) {
        match input.source_node_id.filter(|node| *node != self.local()) {
            Some(node) => self.remote.entry(node).or_default().push(input),
            None => self.here.push(input),
        }
    }

    fn expand_next(&mut self) -> ResolveNext {
        while let Some(input) = self.pending.pop_front() {
            if is_directory(&input) {
                self.step = ResolveStep::Expand;
                let expand = ExpandDirectoryOperation::new(self.scope.caller, input);
                return Ok(Next::Call(IoCall::child(expand)));
            }
            self.files.push(input);
        }
        self.pending = std::mem::take(&mut self.files).into();
        self.follow_next()
    }

    /// A link resolves to its target; the target may live on another node.
    fn follow_next(&mut self) -> ResolveNext {
        let Some(input) = self.pending.pop_front() else {
            return self.capture();
        };
        self.step = ResolveStep::Follow;
        let follow = FollowLinkOperation::new(self.scope.caller, input);
        Ok(Next::Call(IoCall::child(follow)))
    }

    fn capture(&mut self) -> ResolveNext {
        self.step = ResolveStep::Capture;
        let caller = self.scope.capture_caller();
        let capture = CaptureInputsOperation::new(caller, self.here.clone());
        Ok(Next::Call(IoCall::child(capture)))
    }

    fn remote_next(&mut self) -> ResolveNext {
        let Some((node, inputs)) = self.remote.pop_first() else {
            self.step = ResolveStep::Policies;
            let policies = OutputPoliciesOperation::new(&self.spec, self.local());
            return Ok(Next::Call(IoCall::child(policies)));
        };
        self.step = ResolveStep::Remote;
        let caller = ResolveCaller {
            created_by: self.scope.caller.created_by,
            submission_id: self.scope.submission_id,
            expires_at: self.scope.expires_at,
        };
        let token = self.scope.token.clone();
        let ask = RemoteInputsOperation::new(node, inputs, caller, token);
        Ok(Next::Call(IoCall::child(ask)))
    }

    /// The captures in input order, each with the first link its walk met, here or remotely.
    fn finish(&mut self, policies: Vec<PlacementPolicyRef>) -> ResolvedInputs {
        let mut by_key: BTreeMap<String, CapturedInput> = std::mem::take(&mut self.captured)
            .into_iter()
            .map(|captured| (captured.destination_key.clone(), captured))
            .collect();
        let mut inputs = std::mem::take(&mut self.here);
        inputs.append(&mut self.answered);
        let captured = inputs
            .iter()
            .filter_map(|input| by_key.remove(&input.dest_key))
            .map(|mut captured| {
                let link = self.links.remove(&captured.destination_key);
                captured.link = link.or(captured.link);
                captured
            })
            .collect();
        ResolvedInputs {
            inputs,
            captured,
            policies,
        }
    }
}

impl Operation for ResolveInputsOperation {
    type Output = ResolvedInputs;
    type Error = SubmitJobError;

    fn first(&mut self) -> ResolveNext {
        for input in std::mem::take(&mut self.spec.inputs) {
            match input.source_node_id.filter(|node| *node != self.local()) {
                Some(node) => {
                    exact_version(&input)?;
                    self.remote.entry(node).or_default().push(input);
                }
                None => self.pending.push_back(input),
            }
        }
        self.expand_next()
    }

    fn next(&mut self, result: IoResult) -> ResolveNext {
        match self.step {
            ResolveStep::Expand => {
                let files = result.child::<Vec<InputSelection>, SubmitJobError>()?;
                self.files.extend(files.map_err(placement_failure)?);
                self.expand_next()
            }
            ResolveStep::Follow => {
                let followed = result.child::<Followed, SubmitJobError>()?;
                let (input, link) = followed.map_err(placement_failure)?;
                if let Some(link) = link {
                    self.links.insert(input.dest_key.clone(), link);
                }
                self.step = ResolveStep::Range;
                let holder = RangeHolderOperation::new(input, self.local());
                Ok(Next::Call(IoCall::child(holder)))
            }
            ResolveStep::Range => {
                let input = result.child::<InputSelection, SubmitJobError>()?;
                self.place(input.map_err(placement_failure)?);
                self.follow_next()
            }
            ResolveStep::Capture => {
                let captured = result.child::<Vec<CapturedInput>, SubmitJobError>()?;
                self.captured = captured.map_err(placement_failure)?;
                self.remote_next()
            }
            ResolveStep::Remote => {
                let answers = result.child::<Answers, SubmitJobError>()?;
                for (input, capture) in answers.map_err(placement_failure)? {
                    self.answered.push(input);
                    self.captured.push(capture);
                }
                self.remote_next()
            }
            ResolveStep::Policies => {
                let policies = result.child::<Vec<PlacementPolicyRef>, SubmitJobError>()?;
                let policies = policies.map_err(placement_failure)?;
                Ok(Next::Done(self.finish(policies)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::records::{REALM, captured_input, node, payload, user};
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::execution::job::{InputMode, InputSource};
    use ulid::Ulid;

    fn input(key: &str, version: Option<&str>, holder: Option<NodeId>) -> InputSelection {
        InputSelection {
            source: InputSource::S3 {
                bucket: "b".to_string(),
                key: key.to_string(),
                version_id: version.map(str::to_string),
            },
            source_node_id: holder,
            dest_key: key.to_string(),
            mode: InputMode::Snapshot,
            container_path: Some(format!("/in/{key}")),
            name: None,
            description: None,
        }
    }

    fn operation(inputs: Vec<InputSelection>) -> ResolveInputsOperation {
        let mut spec = payload();
        spec.inputs = inputs;
        let scope = ResolveScope {
            caller: InputCaller {
                created_by: user(),
                realm_id: REALM,
                local: node(1),
            },
            submission_id: SubmissionId([4; 32]),
            expires_at: 10,
            token: None,
        };
        ResolveInputsOperation::new(&spec, scope)
    }

    fn child<T>(output: T) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(Ok::<_, SubmitJobError>(output)))
    }

    fn arn(key: &str) -> VersionedObjectArn {
        VersionedObjectArn {
            realm_id: REALM,
            node_id: node(1),
            bucket: "b".to_string(),
            key: key.to_string(),
            version: Ulid::from_parts(1, 1),
        }
    }

    fn calls_child(next: ResolveNext) {
        assert!(matches!(next, Ok(Next::Call(IoCall::Child(_)))), "{next:?}");
    }

    // An input named on another node must pin its version before anything is read.
    #[test]
    fn remote_needs_version() {
        let unpinned = input("r", None, Some(node(2)));
        assert!(matches!(
            operation(vec![unpinned]).first(),
            Err(SubmitJobError::InvalidWorkspace(_))
        ));
    }

    // Local inputs walk and capture here, the others are asked of their holder, local first.
    #[test]
    fn resolves_both_sides() {
        let version = Ulid::from_parts(1, 1).to_string();
        let local = input("l", None, None);
        let remote = input("r", Some(&version), Some(node(2)));
        let mut resolve = operation(vec![remote.clone(), local.clone()]);
        calls_child(resolve.first());
        assert_eq!(resolve.step, ResolveStep::Follow);
        calls_child(resolve.next(child((local.clone(), Some(arn("target"))))));
        assert_eq!(resolve.step, ResolveStep::Range);
        calls_child(resolve.next(child(local.clone())));
        assert_eq!(resolve.step, ResolveStep::Capture);
        let here = captured_input(&local, node(1));
        calls_child(resolve.next(child(vec![here])));
        assert_eq!(resolve.step, ResolveStep::Remote);
        let there = captured_input(&remote, node(2));
        calls_child(resolve.next(child(vec![(remote.clone(), there)])));
        assert_eq!(resolve.step, ResolveStep::Policies);
        let done = resolve.next(child(Vec::<PlacementPolicyRef>::new()));
        let Ok(Next::Done(resolved)) = done else {
            panic!("every input resolved: {done:?}");
        };
        assert_eq!(resolved.inputs, vec![local, remote]);
        assert_eq!(resolved.captured[0].link, Some(arn("target")));
        assert_eq!(resolved.captured[1].source_node_id, node(2));
    }

    // A directory is replaced by the files it expands to, which then walk like any input.
    #[test]
    fn directory_expands_first() {
        let directory = input("d/", None, None);
        let mut resolve = operation(vec![directory]);
        calls_child(resolve.first());
        assert_eq!(resolve.step, ResolveStep::Expand);
        calls_child(resolve.next(child(vec![input("d/a", None, None)])));
        assert_eq!(resolve.step, ResolveStep::Follow);
    }

    // A storage failure of a walk step reads as unavailable, so the caller may retry.
    #[test]
    fn storage_failure_unavailable() {
        let mut resolve = operation(vec![input("l", None, None)]);
        resolve.first().expect("the walk starts");
        let failed = SubmitJobError::UnexpectedEvent("lost".to_string());
        let answer = IoResult::Child(ChildOutcome::new(Err::<Followed, _>(failed)));
        assert!(matches!(
            resolve.next(answer),
            Err(SubmitJobError::PlacementUnavailable(_))
        ));
    }
}
