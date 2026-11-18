//! Steps an operation through the runner on the events a test feeds it, children included.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::operation::{Runnable, SubOperation, boxed_suboperation};
use aruna_core::types::Effects;
use std::fmt::Debug;
use std::marker::PhantomData;

/// A run whose effects the test answers; a child the run starts takes the events until it ends.
pub(crate) struct Stepper<T, E> {
    machine: Box<dyn SubOperation>,
    child: Option<Box<dyn SubOperation>>,
    outcome: PhantomData<fn() -> Result<T, E>>,
}

impl<T, E> Stepper<T, E>
where
    T: Send + Sync + Debug + PartialEq + 'static,
    E: Send + Sync + Debug + PartialEq + 'static,
{
    /// Starts `operation` and answers its first effects.
    pub(crate) fn new<O, K>(operation: O) -> (Self, Effects)
    where
        O: Runnable<K, Output = T, Error = E> + 'static,
        K: 'static,
    {
        let mut machine =
            boxed_suboperation(operation, |result| Event::Child(ChildOutcome::new(result)));
        let effects = machine.start();
        let mut stepper = Self {
            machine,
            child: None,
            outcome: PhantomData,
        };
        let effects = stepper.enter(effects);
        (stepper, effects)
    }

    pub(crate) fn step(&mut self, event: Event) -> Effects {
        let Some(child) = self.child.as_mut() else {
            let effects = self.machine.step(event);
            return self.enter(effects);
        };
        let mut effects = child.step(event);
        if !child.is_complete() {
            return effects;
        }
        if let Some(child) = self.child.take() {
            let next = self.machine.step(child.finalize());
            effects.extend(self.enter(next));
        }
        effects
    }

    /// Starts a child the run asked for and gives the child's first effects.
    fn enter(&mut self, mut effects: Effects) -> Effects {
        loop {
            match effects.pop() {
                Some(Effect::SubOperation(mut child)) if effects.is_empty() => {
                    let started = child.start();
                    if !child.is_complete() {
                        self.child = Some(child);
                        return started;
                    }
                    effects = self.machine.step(child.finalize());
                }
                Some(effect) => {
                    effects.push(effect);
                    return effects;
                }
                None => return effects,
            }
        }
    }

    pub(crate) fn is_complete(&self) -> bool {
        self.child.is_none() && self.machine.is_complete()
    }

    pub(crate) fn finish(self) -> Result<T, E> {
        let Event::Child(outcome) = self.machine.finalize() else {
            panic!("the run ended without its outcome")
        };
        outcome.take().expect("the run's outcome")
    }
}
