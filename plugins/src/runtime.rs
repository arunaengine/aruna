//! Compiles, caches and runs plugin components under node admission and per-run budgets.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use lru::LruCache;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use wasmtime::component::types::ComponentItem;
use wasmtime::component::{Component, HasSelf, InstancePre, Linker};
use wasmtime::{Config, Engine, Store, Trap, UpdateDeadline};

use crate::artifact::{ArtifactDigest, SectionError, read_manifest};
use crate::broker::{Broker, CallContext};
use crate::host::wit::aruna::plugin::types::{Failure, Field, Invocation as GuestCall, Trigger};
use crate::host::{Exceeded, Grants, Limit, Session, wit};
use crate::limits::{Limits, RuntimeConfig};
use crate::manifest::{Manifest, ManifestError};
use crate::permission::Permission;

/// Time between two epoch ticks; a running guest yields to the executor on each tick.
const TICK: std::time::Duration = std::time::Duration::from_millis(10);
/// Live component handles per store, including async subtasks and waitables.
const MAX_HANDLES: usize = 1_024;
const MAX_TRAP_TEXT: usize = 512;
/// The one export every plugin provides; handlers are dispatched by name inside the guest.
const HANDLE_EXPORT: &str = "handle";

type HandleParams = (String, GuestCall);
type HandleResult = (Result<Option<String>, Failure>,);

/// One handler call with its pinned identity, settings, grants and budget.
#[derive(Clone, Debug)]
pub struct Invocation {
    pub run: String,
    pub install: String,
    pub group: String,
    pub handler: String,
    /// The install's settings as a JSON object.
    pub settings: String,
    pub trigger: Trigger,
    /// Permissions granted at all; the broker checks each resource.
    pub permissions: BTreeSet<Permission>,
    /// Connections granted to send through.
    pub connections: BTreeSet<String>,
    pub limits: Limits,
    pub cancel: CancellationToken,
}

/// Why a run produced no result. Guest failures carry the guest's class.
#[derive(Debug, thiserror::Error)]
pub enum InvokeError {
    #[error("plugin reported a {:?} failure: {}", .0.kind, .0.message)]
    Guest(Failure),
    #[error("plugin trapped: {0}")]
    Trap(String),
    #[error("plugin exceeded its {0} limit")]
    Limit(Limit),
    #[error("plugin exceeded its time limit")]
    Timeout,
    #[error("plugin run was cancelled")]
    Cancelled,
    #[error("plugin runtime has no free run slot")]
    Overloaded,
    #[error("plugin runtime is closed")]
    Closed,
    #[error("`{0}` is not a handler of this plugin")]
    Handler(String),
}

/// Why an artifact cannot be compiled.
#[derive(Debug, thiserror::Error)]
pub enum CompileError {
    #[error("artifact has {size} bytes, more than the limit of {limit}")]
    TooLarge { size: usize, limit: usize },
    #[error(transparent)]
    Section(#[from] SectionError),
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    #[error("the `handle` export is missing or has the wrong type: {0}")]
    Export(String),
    #[error("artifact does not compile: {0}")]
    Wasm(String),
    #[error("plugin runtime is closed")]
    Closed,
}

/// A compiled, checked artifact ready for independent runs.
pub struct Compiled {
    pub digest: ArtifactDigest,
    pub manifest: Manifest,
    pre: InstancePre<Session>,
}

/// The node plugin runtime. Cloning shares the engine, cache and admission slots.
#[derive(Clone)]
pub struct Runtime {
    inner: Arc<Inner>,
}

struct Inner {
    engine: Engine,
    linker: Linker<Session>,
    config: RuntimeConfig,
    cache: Mutex<LruCache<ArtifactDigest, Arc<Compiled>>>,
    runs: Arc<Semaphore>,
    compiles: Arc<Semaphore>,
    tracker: TaskTracker,
}

impl Runtime {
    /// Builds the engine and host linker: the plugin imports, and WASI for toolchains that need
    /// it, with no files, environment, arguments or network.
    pub fn new(config: RuntimeConfig) -> Result<Self, CompileError> {
        let mut engine_config = Config::new();
        engine_config.consume_fuel(true);
        engine_config.epoch_interruption(true);
        let engine = Engine::new(&engine_config).map_err(wasm_error)?;
        spawn_ticker(&engine);
        let mut linker = Linker::new(&engine);
        wit::Plugin::add_to_linker::<Session, HasSelf<Session>>(&mut linker, |session| session)
            .map_err(wasm_error)?;
        wasmtime_wasi::p2::add_to_linker_async(&mut linker).map_err(wasm_error)?;
        crate::host::bounded_wasi(&mut linker).map_err(wasm_error)?;
        let cache = NonZeroUsize::new(config.cache).unwrap_or(NonZeroUsize::MIN);
        Ok(Self {
            inner: Arc::new(Inner {
                engine,
                linker,
                runs: Arc::new(Semaphore::new(config.invocations)),
                compiles: Arc::new(Semaphore::new(config.compiles.max(1))),
                cache: Mutex::new(LruCache::new(cache)),
                tracker: TaskTracker::new(),
                config,
            }),
        })
    }

    /// Checks and compiles `bytes` on a blocking thread, at most `compiles` at a time
    /// on this node. Never runs guest code.
    pub async fn prepare(&self, bytes: Vec<u8>) -> Result<Arc<Compiled>, CompileError> {
        let digest = ArtifactDigest::of(&bytes);
        if let Some(compiled) = self.inner.cached(&digest) {
            return Ok(compiled);
        }
        let slot = self
            .inner
            .compiles
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| CompileError::Closed)?;
        let runtime = self.clone();
        tokio::task::spawn_blocking(move || {
            let _slot = slot;
            runtime.compile(&bytes)
        })
        .await
        .map_err(|err| CompileError::Wasm(err.to_string()))?
    }

    /// Checks and compiles `bytes` on the calling thread, without node admission.
    pub fn compile(&self, bytes: &[u8]) -> Result<Arc<Compiled>, CompileError> {
        let inner = &self.inner;
        let limit = inner.config.max_artifact;
        if bytes.len() > limit {
            return Err(CompileError::TooLarge {
                size: bytes.len(),
                limit,
            });
        }
        let digest = ArtifactDigest::of(bytes);
        if let Some(compiled) = inner.cached(&digest) {
            return Ok(compiled);
        }
        let manifest = read_manifest(bytes)?;
        let component = Component::new(&inner.engine, bytes).map_err(wasm_error)?;
        let ty = component.component_type();
        let instances = ty.imports(&inner.engine).filter_map(|(name, item)| {
            matches!(item.ty, ComponentItem::ComponentInstance(_)).then_some(name)
        });
        manifest.check_imports(instances)?;
        let Some(ComponentItem::ComponentFunc(func)) = ty
            .get_export(&inner.engine, HANDLE_EXPORT)
            .map(|export| export.ty)
        else {
            return Err(CompileError::Export("no function export".into()));
        };
        func.typecheck::<HandleParams, HandleResult>(&ty.instance_type())
            .map_err(|err| CompileError::Export(err.to_string()))?;
        let pre = inner
            .linker
            .instantiate_pre(&component)
            .map_err(wasm_error)?;
        let compiled = Arc::new(Compiled {
            digest: digest.clone(),
            manifest,
            pre,
        });
        inner.lock_cache().put(digest, compiled.clone());
        Ok(compiled)
    }

    /// Runs one handler in a fresh instance and store. Any trap, budget or deadline ends it
    /// without a result, but only after its started writes recorded their outcome.
    pub async fn invoke(
        &self,
        compiled: &Compiled,
        call: Invocation,
        broker: Arc<dyn Broker>,
    ) -> Result<Option<String>, InvokeError> {
        if !compiled.manifest.handlers.contains(&call.handler) {
            return Err(InvokeError::Handler(call.handler));
        }
        let limits = call.limits.within(&self.inner.config.ceiling);
        let input = call.run.len() + call.install.len() + call.group.len() + call.settings.len();
        if input + trigger_size(&call.trigger) > limits.input {
            return Err(InvokeError::Limit(Limit::Input));
        }
        let _permit = acquire(&self.inner.runs)?;
        let _token = self.inner.tracker.token();
        let cx = CallContext {
            run: call.run.clone(),
            install: call.install.clone(),
            max_bytes: limits.call_bytes,
        };
        let writes = TaskTracker::new();
        let declared = &compiled.manifest.connections;
        let connections = (call.connections.into_iter())
            .filter_map(|name| {
                Some((
                    name.clone(),
                    declared.get(&name)?.response.fields().to_vec(),
                ))
            })
            .collect();
        let grants = Grants {
            permissions: call.permissions,
            connections,
        };
        let trackers = (self.inner.tracker.clone(), writes.clone());
        let session = Session::new(broker, cx, grants, trackers, limits);
        let params = (
            call.handler,
            GuestCall {
                run: call.run,
                install: call.install,
                group: call.group,
                settings: call.settings,
                trigger: call.trigger,
            },
        );
        let ended = self.run(compiled, session, params, &call.cancel).await;
        // Started writes record their outcome before any end of this run is reported.
        writes.close();
        writes.wait().await;
        match ended?.0 {
            Ok(Some(output)) if output.len() > limits.output => {
                Err(InvokeError::Limit(Limit::Output))
            }
            Ok(output) => Ok(output),
            Err(failure) if failure.message.len() > limits.output => {
                Err(InvokeError::Limit(Limit::Output))
            }
            Err(failure) => Err(InvokeError::Guest(failure)),
        }
    }

    /// Stops admitting new runs and compilations; running ones continue.
    pub fn close(&self) {
        self.inner.runs.close();
        self.inner.compiles.close();
        self.inner.tracker.close();
    }

    /// Waits until every admitted run and its writes have finished, after [`Runtime::close`].
    pub async fn drain(&self) {
        self.inner.tracker.wait().await;
    }

    async fn run(
        &self,
        compiled: &Compiled,
        session: Session,
        params: HandleParams,
        cancel: &CancellationToken,
    ) -> Result<HandleResult, InvokeError> {
        let limits = session.limits();
        let mut store = Store::new(&self.inner.engine, session);
        store.limiter(|session| session);
        // Each epoch tick yields to the executor, so deadlines and cancellation are observed.
        store.set_epoch_deadline(1);
        store.epoch_deadline_callback(|_| Ok(UpdateDeadline::Yield(1)));
        store.set_hostcall_fuel(limits.call_bytes);
        store.set_max_component_handles(MAX_HANDLES);
        store
            .set_fuel(limits.fuel)
            .map_err(|err| InvokeError::Trap(err.to_string()))?;
        let work = async {
            let instance = compiled.pre.instantiate_async(&mut store).await?;
            let func =
                instance.get_typed_func::<HandleParams, HandleResult>(&mut store, HANDLE_EXPORT)?;
            func.call_async(&mut store, params).await
        };
        let outcome = tokio::select! {
            biased;
            () = cancel.cancelled() => return Err(InvokeError::Cancelled),
            result = tokio::time::timeout(limits.wall, work) => result,
        };
        match outcome {
            Err(_) => Err(InvokeError::Timeout),
            Ok(Ok(value)) => Ok(value),
            Ok(Err(err)) => Err(classify(&err, store.data())),
        }
    }
}

impl Inner {
    fn lock_cache(&self) -> std::sync::MutexGuard<'_, LruCache<ArtifactDigest, Arc<Compiled>>> {
        self.cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn cached(&self, digest: &ArtifactDigest) -> Option<Arc<Compiled>> {
        self.lock_cache().get(digest).cloned()
    }
}

fn acquire(slots: &Arc<Semaphore>) -> Result<OwnedSemaphorePermit, InvokeError> {
    slots.clone().try_acquire_owned().map_err(|err| match err {
        TryAcquireError::Closed => InvokeError::Closed,
        TryAcquireError::NoPermits => InvokeError::Overloaded,
    })
}

/// Advances the engine's epoch every [`TICK`] on its own thread until the engine is dropped.
fn spawn_ticker(engine: &Engine) {
    let weak = engine.weak();
    std::thread::spawn(move || {
        while let Some(engine) = weak.upgrade() {
            engine.increment_epoch();
            drop(engine);
            std::thread::sleep(TICK);
        }
    });
}

fn classify(err: &wasmtime::Error, session: &Session) -> InvokeError {
    if let Some(limit) = session.exceeded {
        return InvokeError::Limit(limit);
    }
    if let Some(limit) = session.stream_limit() {
        return InvokeError::Limit(limit);
    }
    if let Some(Exceeded(limit)) = err.downcast_ref::<Exceeded>() {
        return InvokeError::Limit(*limit);
    }
    if let Some(Trap::OutOfFuel) = err.downcast_ref::<Trap>() {
        return InvokeError::Limit(Limit::Fuel);
    }
    let mut text = err.root_cause().to_string();
    if text.len() > MAX_TRAP_TEXT {
        let mut end = MAX_TRAP_TEXT;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
    }
    InvokeError::Trap(text)
}

fn wasm_error(err: wasmtime::Error) -> CompileError {
    CompileError::Wasm(format!("{err:#}"))
}

fn fields_size(fields: &[Field]) -> usize {
    fields.iter().map(|f| f.name.len() + f.value.len()).sum()
}

fn trigger_size(trigger: &Trigger) -> usize {
    match trigger {
        Trigger::Manual(input) => input.len(),
        Trigger::Webhook(delivery) => {
            let request = &delivery.request;
            delivery.delivery_id.as_ref().map_or(0, String::len)
                + request.method.len()
                + request.query.len()
                + request.body.len()
                + fields_size(&request.headers)
        }
        Trigger::Event(event) => {
            event.kind.len() + event.path.len() + event.resource.len() + event.revision.len()
        }
        Trigger::Timer(_) => 8,
    }
}
