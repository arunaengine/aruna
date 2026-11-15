// Plugin handlers: each trigger in plugin.toml runs the function registered for it.
import aruna from "aruna";

aruna.on("manual", () => `hello from ${aruna.run.install}`);
