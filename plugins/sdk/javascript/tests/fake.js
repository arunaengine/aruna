// A fake host for the library's tests: datasets, objects, service answers, secrets and state live
// in memory, and it keeps the log and every call it got.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

import { handle, use } from "../aruna.js";

const text = (value) => JSON.stringify(value);

export class Fake {
  constructor(types = {}) {
    Object.assign(this, { lines: [], calls: [], settings: {} });
    use(this, types);
  }

  /** Runs the handler of `tag` with `val`, as the node would. */
  invoke(tag, val) {
    const invocation = { run: "r-1", install: "i-1", group: "g-1", settings: text(this.settings) };
    return handle({ ...invocation, trigger: { tag, val } });
  }

  log(level, message) {
    this.lines.push([level, message]);
  }
}
