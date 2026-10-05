//! The rows a key or settings change writes: settings, key records and the transition.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::s3::bucket::key::rows::audit_row;
use aruna_core::structs::storage::key_audit::{AuditAction, AuditOutcome, BucketAuditRecord};

impl ChangeEncryptionOperation {
    pub(super) fn rows(
        &mut self,
        secret: Option<SharedSecret>,
        copies: Vec<SealedCopy>,
    ) -> Result<Vec<Row>, ChangeError> {
        let (mode, cipher, block_keys) = self.wanted();
        let mut old = self.active.clone().ok_or(ChangeError::NotFinished)?;
        let info = self.info.as_ref().ok_or(ChangeError::NotFinished)?;
        let previous = self.settings.clone();
        let mut settings = BucketEncryption {
            mode,
            cipher,
            block_keys,
            ..previous.clone()
        };
        if let Some(max) = self.input.max_unlock_ms {
            settings.max_unlock_ms = max;
        }
        let format_changed = previous.cipher != cipher
            || previous.block_keys != block_keys
            || mode == EncryptionMode::Off;
        let mut rows = Vec::new();
        let target_key = match self.new_key.as_ref() {
            Some((record, _)) => {
                let report = self.report(&copies);
                if mode == EncryptionMode::VaultLocked
                    && report.recovery.state != RecoveryState::Met
                {
                    return Err(ChangeError::RecoveryUnmet);
                }
                settings.key_generation = record.key.generation;
                settings.storage_generation += 1;
                rows.extend(generation_rows(
                    &self.input.bucket,
                    &settings,
                    record,
                    &copies,
                    &report,
                )?);
                Some(record.clone())
            }
            None => {
                if format_changed {
                    settings.storage_generation += 1;
                }
                if mode == EncryptionMode::NodeManaged && secret.is_some() {
                    old.vault_entry = Some(Ulid::generate());
                }
                rows.push((
                    aruna_core::keyspaces::BUCKET_ENCRYPTION_KEYSPACE.to_string(),
                    self.input.bucket.as_bytes().to_vec().into(),
                    settings.to_bytes()?.into(),
                ));
                (mode != EncryptionMode::Off).then(|| old.clone())
            }
        };
        let moves = target_key.as_ref().map(|key| key.key) != Some(old.key) || format_changed;
        let transition = match moves {
            true => {
                let kind = match (&target_key, format_changed) {
                    (None, _) => TransitionKind::Decrypt,
                    (Some(_), true) => TransitionKind::Reencode,
                    (Some(_), false) => TransitionKind::Rotate,
                };
                let plan = target_key.as_ref().map(|key| SealPlan {
                    key: key.key,
                    public_key: key.public_key,
                    cipher,
                    block_keys,
                    storage_generation: settings.storage_generation,
                });
                let target = TransitionTarget {
                    compression: info.compression,
                    plan,
                };
                let generation = settings.storage_generation;
                let now = self.input.now_ms;
                let mut transition =
                    EncryptionTransition::new(kind, Some(old.key), target, generation, now);
                transition.source_vault = old.vault_entry;
                Some(transition)
            }
            false => None,
        };
        if let Some(transition) = transition.as_ref() {
            if transition.target.plan.map(|plan| plan.key) != Some(old.key) {
                old.state = KeyState::Retiring;
            }
            let bucket: Key = self.input.bucket.as_bytes().to_vec().into();
            rows.push((
                TRANSITION_KEYSPACE.to_string(),
                bucket.clone(),
                transition.to_bytes()?.into(),
            ));
            rows.push((
                TRANSITION_QUEUE_KEYSPACE.to_string(),
                bucket,
                Vec::new().into(),
            ));
        }
        rows.push((
            BUCKET_KEY_KEYSPACE.to_string(),
            old.key.key().into(),
            old.to_bytes()?.into(),
        ));
        let key = self.new_key.take();
        self.vault = match (&secret, old.vault_entry, &key) {
            (Some(secret), Some(entry), _) => Some((entry, secret.clone())),
            (None, _, Some((record, secret))) => {
                record.vault_entry.map(|entry| (entry, secret.clone()))
            }
            _ => None,
        };
        // The change and its audit record commit together.
        let action = match self.input.change {
            KeyChange::Rotate => AuditAction::Rotation,
            KeyChange::Settings { .. } => AuditAction::ModeChange,
        };
        let generation = key
            .as_ref()
            .map_or(old.key.generation, |(record, _)| record.key.generation);
        rows.push(audit_row(&BucketAuditRecord {
            event_id: Ulid::generate(),
            bucket_id: old.key.bucket_id,
            at_ms: self.input.now_ms,
            action,
            actor: Some(self.input.caller),
            node_id: self.input.node_id,
            generation: Some(generation),
            session_id: None,
            intent_id: None,
            sequence: None,
            deadline_ms: None,
            reason: None,
            outcome: AuditOutcome::Applied,
        })?);
        self.result = Some(ChangeResult {
            settings,
            transition,
            key,
        });
        Ok(rows)
    }
}
