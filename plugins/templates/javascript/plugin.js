// Plugin handlers: each trigger in plugin.toml calls one of these by name.
const handlers = {
  run(ctx, event) {
    return `hello from ${ctx.install}`;
  },
};

// Calls the named handler with a context and a plain event; a thrown error fails the run.
export function handle(name, invocation) {
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
  const { tag, val } = invocation.trigger;
  const event = tag === "manual" ? { kind: tag, input: val ? JSON.parse(val) : null } : { kind: tag, ...val };
  try {
    const result = handler(ctx, event);
    return result === undefined ? undefined : typeof result === "string" ? result : JSON.stringify(result);
  } catch (error) {
    const message = error?.payload?.message ?? error?.message ?? String(error);
    throw { kind: "permanent", message };
  }
}
