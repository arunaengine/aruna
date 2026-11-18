//! Answers Git store calls with the store the blob handle owns.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::{Duration, Instant};

use aruna_core::effects::Effect;
use aruna_core::git::{GitAnswer, GitCall, GitEffect, record_writes};
use aruna_core::telemetry::{RequestStages, record_stage};
use futures_util::StreamExt;

use super::{dispatch_effect_until, start_background};
use crate::driver::DriverContext;

pub(super) async fn dispatch_git(
    call: GitCall,
    context: &DriverContext,
    depth: usize,
    deadline: Option<tokio::time::Instant>,
) -> GitAnswer {
    let call = match call {
        GitCall::Stages(stages) => {
            for (name, elapsed_ms) in stages {
                record_stage(name, Duration::from_millis(elapsed_ms));
            }
            return GitAnswer::Noted;
        }
        GitCall::Canonicalize(jsonld) => {
            let started = Instant::now();
            let canonical = craqle::canonicalize_jsonld(&jsonld);
            record_stage("git_canonicalize", started.elapsed());
            return GitAnswer::Canonical(canonical.ok().map(|canonical| canonical.digest));
        }
        GitCall::RecordWrites(document_id) => {
            return GitAnswer::RecordWrites(record_writes(document_id));
        }
        GitCall::Timed(child) => {
            let stages = RequestStages::default();
            let run = dispatch_effect_until(Effect::SubOperation(child), context, depth, deadline);
            let event = stages.clone().scope(run).await;
            return GitAnswer::Timed {
                event: Box::new(event),
                stages: stages.render(),
            };
        }
        call => call,
    };
    let store = context.blob_handle.as_ref().and_then(|handle| handle.git());
    let Some(store) = store else {
        return GitAnswer::Failed("this node has no Git store".to_string());
    };
    let caches = store.caches();
    let ran = |result: std::io::Result<_>| match result {
        Ok(event) => GitAnswer::Ran(event),
        Err(error) => GitAnswer::Failed(error.to_string()),
    };
    match call {
        GitCall::Run { effect, actor } => ran(store.execute(effect, actor).await),
        GitCall::Push { mut request, actor } => {
            let value = hex::encode(rand::random::<[u8; 32]>());
            let key = caches.open_push(request.repository.document_id, value);
            request.push_key = key.value().to_string();
            ran(store.execute(GitEffect::Http(request), actor).await)
        }
        GitCall::PushKeyValid { document_id, proof } => {
            GitAnswer::PushKeyValid(caches.push_key_valid(document_id, &proof.0))
        }
        GitCall::Configured(document_id) => {
            GitAnswer::Configured(store.configured(document_id).await)
        }
        GitCall::Busy(document_id) => GitAnswer::Busy(store.try_lock(document_id).is_none()),
        GitCall::Collect { mut blob, limit } => {
            let mut bytes = Vec::new();
            while let Some(chunk) = blob.next().await {
                match chunk {
                    Ok(chunk) => bytes.extend_from_slice(&chunk),
                    Err(error) => return GitAnswer::Failed(error.to_string()),
                }
                if bytes.len() as u64 > limit {
                    break;
                }
            }
            GitAnswer::Collected(bytes.into())
        }
        GitCall::Recent {
            document_id,
            inputs,
        } => GitAnswer::Projection(caches.remembered(document_id, &inputs).map(Box::new)),
        GitCall::Last(document_id) => GitAnswer::Projection(caches.last(document_id).map(Box::new)),
        GitCall::Remember {
            document_id,
            inputs,
            projection,
        } => {
            caches.remember(document_id, inputs, *projection);
            GitAnswer::Noted
        }
        GitCall::Forget(document_id) => {
            caches.forget(document_id);
            GitAnswer::Noted
        }
        GitCall::KnownAncestry(document_id) => GitAnswer::Ancestry(caches.answered(document_id)),
        GitCall::Projected {
            document_id,
            ancestry,
            refs,
        } => {
            caches.learn(document_id, &ancestry);
            caches.serve(document_id, refs);
            GitAnswer::Noted
        }
        GitCall::ServedRefs(document_id) => GitAnswer::ServedRefs(caches.served(document_id)),
        GitCall::NextSequence => match caches.next_sequence() {
            Some(sequence) => GitAnswer::Sequence(sequence),
            None => GitAnswer::Failed("the merge sequence is exhausted".to_string()),
        },
        GitCall::RefreshOnce { document_id, child } => {
            let Some(claim) = caches.claim_refresh(document_id) else {
                return GitAnswer::RefreshStarted(false);
            };
            GitAnswer::RefreshStarted(start_background(child, context, claim))
        }
        GitCall::Stages(_)
        | GitCall::Canonicalize(_)
        | GitCall::RecordWrites(_)
        | GitCall::Timed(_) => GitAnswer::Failed("the call was already answered".to_string()),
    }
}
