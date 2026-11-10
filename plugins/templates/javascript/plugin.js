// Plugin handlers: each trigger in plugin.toml calls one of these by name.
const handlers = {
  run(ctx, event) {
    return `hello from ${ctx.install}`;
  },
};

// Calls the named handler with a context and a plain event and awaits it; a thrown error fails
// the run with the kind the Rust SDK would give it.
export async function handle(name, invocation) {
  const handler = handlers[name];
  if (!handler) {
    throw { kind: "invalid-input", message: `${name} is not a handler of this plugin` };
  }
  const ctx = {
    run: invocation.run,
    install: invocation.install,
    group: invocation.group,
    settings: JSON.parse(invocation.settings),
  };
  let result;
  try {
    result = await handler(ctx, toEvent(invocation.trigger));
  } catch (error) {
    throw failure(error);
  }
  return result === undefined ? undefined : typeof result === "string" ? result : JSON.stringify(result);
}

// The plain event of a trigger: manual input as JSON, the timer tick, or the record's fields.
function toEvent({ tag, val }) {
  if (tag === "manual") return { kind: tag, input: val ? JSON.parse(val) : null };
  if (tag === "timer") return { kind: tag, tick: val };
  return { kind: tag, ...val };
}

const FAILURE_KINDS = ["permanent", "retryable", "invalid-input"];

// A handler's own failure stays; an unavailable host is retryable, any other error permanent.
function failure(error) {
  if (FAILURE_KINDS.includes(error?.kind) && typeof error.message === "string") return error;
  const host = error?.payload;
  if (typeof host?.kind === "string") {
    const kind = host.kind === "unavailable" ? "retryable" : "permanent";
    return { kind, message: `host call failed (${host.kind}): ${host.message}` };
  }
  return { kind: "permanent", message: error?.message ?? String(error) };
}
