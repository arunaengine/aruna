// Types of the JavaScript library for Aruna plugins, for editors and type checks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

export type Outcome = "permanent" | "retryable" | "invalid-input";

/** Every error the library throws; uncaught, `outcome` says how the run ends. */
export declare class ArunaError extends Error {
  outcome: Outcome;
  constructor(message: string);
}
export declare class NotAllowed extends ArunaError {}
export declare class NotFound extends ArunaError {}
export declare class Conflict extends ArunaError {}
export declare class LimitReached extends ArunaError {}
export declare class InvalidCall extends ArunaError {}
export declare class Unavailable extends ArunaError {}
export declare class UnknownOutcome extends ArunaError {}
export declare class ServiceError extends ArunaError {
  status: number;
  constructor(status: number, message: string);
}
export declare class Fail extends ArunaError {}
export declare class Retry extends ArunaError {}
export declare class InvalidInput extends ArunaError {}

/** The `[manual]` fields a person gave, defaults filled in; `dataset` and `object` fields loaded. */
export type Form = { [field: string]: any };
type Handler<T> = ((subject: T) => unknown) | (() => unknown);

export declare function on(trigger: "manual", handler: Handler<Form>): typeof handler;
export declare function on(trigger: "timer", handler: Handler<Date>): typeof handler;
export declare function on(trigger: "webhook", handler: Handler<Request>): typeof handler;
export declare function on(trigger: "sync_completed" | "sync_failed", handler: Handler<SyncEvent>): typeof handler;
export declare function on(trigger: string | string[], handler: Handler<any>): typeof handler;

export interface WatchEvent {
  kind: string;
  path: string;
  resource: string;
  revision: string;
}

/** Who runs the handler, and the raw event of an event trigger. */
export declare const run: { id: string; install: string; group: string; event: WatchEvent | null };
/** The install's settings by name. */
export declare const settings: { [name: string]: any };

export declare const log: {
  debug(message: unknown): void;
  info(message: unknown): void;
  warn(message: unknown): void;
  error(message: unknown): void;
};

/** A verified webhook request; authentication headers are already removed. */
export declare class Request {
  method: string;
  query: string;
  headers: { [name: string]: string };
  body: Uint8Array;
  deliveryId: string | null;
  readonly text: string;
  json(): any;
}

/** A sync relationship's job that completed or failed, below the source path. */
export declare class SyncEvent {
  ok: boolean;
  path: string;
  relationship: string;
  job: string;
}

/** Puts a host in place of the component's imports; for the build's entry and tests. */
export declare function use(host: object, types?: { [field: string]: string }): void;

declare const aruna: {
  on: typeof on;
  run: typeof run;
  settings: typeof settings;
  log: typeof log;
  Request: typeof Request;
  SyncEvent: typeof SyncEvent;
  ArunaError: typeof ArunaError;
  Conflict: typeof Conflict;
  Fail: typeof Fail;
  InvalidCall: typeof InvalidCall;
  InvalidInput: typeof InvalidInput;
  LimitReached: typeof LimitReached;
  NotAllowed: typeof NotAllowed;
  NotFound: typeof NotFound;
  Retry: typeof Retry;
  ServiceError: typeof ServiceError;
  Unavailable: typeof Unavailable;
  UnknownOutcome: typeof UnknownOutcome;
};
export default aruna;
